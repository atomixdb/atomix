use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use crate::for_testing::in_memory_wal::InMemIterator;

use super::*;
use async_trait::async_trait;
use flatbuffers::FlatBufferBuilder;
use prost::Message;
use scylla::batch::Batch;
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use scylla::Session;
use scylla::SessionBuilder;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct CassandraWal {
    session: Session,
    wal_id: Uuid,
    state: RwLock<State>,
}

enum State {
    NotSynced,
    Synced(LogState),
}

struct LogState {
    first_offset: Option<i64>,
    next_offset: i64,
    flatbuf_builder: FlatBufferBuilder<'static>,
}

fn scylla_query_error_to_wal_error(qe: QueryError) -> Error {
    match qe {
        QueryError::TimeoutError | QueryError::DbError(DbError::WriteTimeout { .. }, _) => {
            Error::Timeout
        }
        _ => Error::Internal(Arc::new(qe)),
    }
}

static SYNC_WAL_QUERY: &str = r#"
  SELECT first_offset, next_offset FROM atomix.wal
    WHERE wal_id = ? and offset = ?;
"#;

static UPDATE_FIRST_OFFSET_QUERY: &str = r#"
    UPDATE atomix.wal SET first_offset = ?
      WHERE wal_id = ? and offset = ?
      IF first_offset = ?
"#;

static TRIM_LOG_QUERY: &str = r#"
    DELETE FROM atomix.wal
      WHERE wal_id = ? and offset < ?
"#;

static UPDATE_METADATA_QUERY: &str = r#"
    UPDATE atomix.wal SET next_offset = ?, first_offset = ?
      WHERE wal_id = ? and offset = ?
      IF next_offset = ?
"#;

static APPEND_ENTRY_QUERY: &str = r#"
    INSERT INTO atomix.wal (wal_id, offset, content, write_id)
    VALUES (?, ?, ?, ?)
"#;

static GET_ENTRY_AT_OFFSET_QUERY: &str = r#"
    SELECT content FROM atomix.wal
      WHERE wal_id = ? and offset = ?
"#;

static RETRIEVE_LOG_ENTRY: &str = r#"
    SELECT * FROM atomix.wal
      WHERE wal_id = ? and offset = ? and write_id = ?
      ALLOW FILTERING
"#;

static METADATA_OFFSET: i64 = i64::MAX;

impl CassandraWal {
    pub async fn new(known_node: String, wal_id: Uuid) -> CassandraWal {
        let session = SessionBuilder::new()
            .known_node(known_node)
            .build()
            .await
            .unwrap();
        CassandraWal {
            session,
            wal_id,
            state: RwLock::new(State::NotSynced),
        }
    }

    async fn append_entry(&self, entry_type: Entry, entry: &[u8]) -> Result<u64, Error> {
        let mut state = self.state.write().await;
        match state.deref_mut() {
            State::NotSynced => Err(Error::NotSynced),
            State::Synced(log_state) => {
                let bytes = log_state.flatbuf_builder.create_vector(entry);
                let fb_root = LogEntry::create(
                    &mut log_state.flatbuf_builder,
                    &LogEntryArgs {
                        entry: entry_type,
                        bytes: Some(bytes),
                    },
                );
                log_state.flatbuf_builder.finish(fb_root, None);
                let content = Vec::from(log_state.flatbuf_builder.finished_data());
                log_state.flatbuf_builder.reset();

                let write_id = Uuid::new_v4();
                let offset: i64 = log_state.next_offset;
                log_state.next_offset += 1;
                log_state.first_offset = Some(log_state.first_offset.unwrap_or(offset));

                let mut batch: Batch = Default::default();
                batch.set_serial_consistency(Some(SerialConsistency::Serial));
                batch.append_statement(Query::new(UPDATE_METADATA_QUERY));
                batch.append_statement(Query::new(APPEND_ENTRY_QUERY));
                let batch_args = (
                    (
                        log_state.next_offset,
                        log_state.first_offset,
                        self.wal_id,
                        METADATA_OFFSET,
                        offset,
                    ),
                    (self.wal_id, offset, content, write_id),
                );
                self.session
                    .batch(&batch, batch_args)
                    .await
                    .map_err(scylla_query_error_to_wal_error)?;

                // Unfortunately scylladb driver does not tell us if the
                // conditional checks passed or not, so we lookup the entry
                // to match the write_id to verify that the write made it.
                let mut post_write_check_query = Query::new(RETRIEVE_LOG_ENTRY);
                post_write_check_query.set_serial_consistency(Some(SerialConsistency::Serial));
                let rows = self
                    .session
                    .query(post_write_check_query, (self.wal_id, offset, write_id))
                    .await
                    .map_err(scylla_query_error_to_wal_error)?
                    .rows;
                if rows.is_none() || rows.unwrap().len() != 1 {
                    *state = State::NotSynced;
                    return Err(Error::NotSynced);
                }
                Ok(offset as u64)
            }
        }
    }
}

#[async_trait]
impl Wal for CassandraWal {
    async fn sync(&self) -> Result<(), Error> {
        let mut state = self.state.write().await;
        (*state) = State::NotSynced;
        let mut query = Query::new(SYNC_WAL_QUERY);
        query.set_serial_consistency(Some(SerialConsistency::Serial));
        let rows = self
            .session
            .query(query, (self.wal_id, METADATA_OFFSET))
            .await
            .map_err(scylla_query_error_to_wal_error)?
            .rows;
        // TODO(tamer): initialize the log if it's never been created.
        let res = match rows {
            None => Err(Error::NotSynced),
            Some(mut rows) => {
                if rows.len() != 1 {
                    panic!("found multiple rows for the same WAL metadata!");
                } else {
                    let row = rows.pop().unwrap();
                    let (first_offset, next_offset) =
                        row.into_typed::<(Option<i64>, i64)>().unwrap();
                    (*state) = State::Synced(LogState {
                        first_offset,
                        next_offset,
                        flatbuf_builder: FlatBufferBuilder::new(),
                    });
                    Ok(())
                }
            }
        };
        res
    }

    async fn first_offset(&self) -> Result<Option<u64>, Error> {
        let state = self.state.read().await;
        match state.deref() {
            State::NotSynced => Err(Error::NotSynced),
            State::Synced(log) => Ok(log.first_offset.map(|o| o as u64)),
        }
    }

    async fn last_offset(&self) -> Result<Option<u64>, Error> {
        let state = self.state.read().await;
        match state.deref() {
            State::NotSynced => Err(Error::NotSynced),
            State::Synced(log) => match log.first_offset {
                None => Ok(None),
                Some(_) => {
                    let last_offset = log.next_offset - 1;
                    Ok(Some(last_offset as u64))
                }
            },
        }
    }

    async fn next_offset(&self) -> Result<u64, Error> {
        let state = self.state.read().await;
        match state.deref() {
            State::NotSynced => Err(Error::NotSynced),
            State::Synced(log) => Ok(log.next_offset as u64),
        }
    }

    async fn get_entry_at_offset(&self, offset: u64) -> Result<Vec<u8>, Error> {
        let state = self.state.read().await;
        match state.deref() {
            State::NotSynced => Err(Error::NotSynced),
            State::Synced(log) => {
                let mut query = Query::new(GET_ENTRY_AT_OFFSET_QUERY);
                query.set_serial_consistency(Some(SerialConsistency::Serial));
                let rows = self
                    .session
                    .query(query, (self.wal_id, offset as i64))
                    .await
                    .map_err(scylla_query_error_to_wal_error)?
                    .rows;
                match rows {
                    None => Err(Error::EntryNotFound),
                    Some(mut rows) => {
                        if rows.len() == 0 {
                            return Err(Error::EntryNotFound);
                        } else if rows.len() != 1 {
                            panic!("found multiple rows for the same WAL metadata!");
                        } else {
                            let row = rows.pop().unwrap();
                            let (content,) = row.into_typed::<(Vec<u8>,)>().unwrap();
                            Ok(content)
                        }
                    }
                }
            }
        }
    }

    async fn trim_before_offset(&self, offset: u64) -> Result<(), Error> {
        let first_offset = self.first_offset().await?;
        let first_offset = match first_offset {
            None => return Ok(()),
            Some(first_offset) => {
                if offset < first_offset {
                    return Ok(());
                };
                first_offset
            }
        };
        let mut batch: Batch = Default::default();
        batch.set_serial_consistency(Some(SerialConsistency::Serial));
        batch.append_statement(Query::new(UPDATE_FIRST_OFFSET_QUERY));
        batch.append_statement(Query::new(TRIM_LOG_QUERY));

        let batch_args = (
            (
                offset as i64,
                self.wal_id,
                METADATA_OFFSET,
                first_offset as i64,
            ),
            (self.wal_id, offset as i64),
        );
        self.session
            .batch(&batch, batch_args)
            .await
            .map_err(scylla_query_error_to_wal_error)?;

        // Unfortunately, the scylladb driver does not tell us whether the
        // conditional check passed or not, so as a hack we just re-sync here.
        self.sync().await
    }

    async fn append_prepare(&self, entry: PrepareRequest<'_>) -> Result<u64, Error> {
        self.append_entry(Entry::Prepare, entry._tab.buf()).await
    }

    async fn append_abort(&self, entry: AbortRequest<'_>) -> Result<u64, Error> {
        self.append_entry(Entry::Abort, entry._tab.buf()).await
    }

    async fn append_commit(&self, entry: CommitRequest<'_>) -> Result<u64, Error> {
        self.append_entry(Entry::Commit, entry._tab.buf()).await
    }

    async fn append_replicated_commit(&self, entry: ReplicateDataRequest) -> Result<u64, Error> {
        self.append_entry(Entry::ReplicatedCommit, entry.encode_to_vec().as_slice())
            .await
    }

    fn iterator(self: &Arc<Self>, start_after_offset: Option<u64>) -> InMemIterator {
        todo!()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use common::util;

    impl CassandraWal {
        async fn create_test() -> CassandraWal {
            let cassandra = CassandraWal::new("127.0.0.1:9042".to_string(), Uuid::new_v4()).await;
            let mut query =
                Query::new("INSERT INTO atomix.wal (wal_id, offset, next_offset) VALUES (?, ?, ?)");
            query.set_serial_consistency(Some(SerialConsistency::Serial));
            cassandra
                .session
                .query(query, (cassandra.wal_id, METADATA_OFFSET, 0_i64))
                .await
                .unwrap();
            cassandra
        }

        async fn cleanup(&self) {
            let mut query = Query::new("DELETE FROM atomix.wal WHERE wal_id = ?");
            query.set_serial_consistency(Some(SerialConsistency::Serial));
            let _ = self.session.query(query, (self.wal_id,)).await;
        }
    }

    #[tokio::test]
    async fn initial_sync() {
        let cassandra = CassandraWal::create_test().await;
        cassandra.sync().await.unwrap();
        let first_offset = cassandra.first_offset().await.unwrap();
        assert!(first_offset.is_none());
        let next_offset = cassandra.next_offset().await.unwrap();
        assert!(next_offset == 0);
        cassandra.cleanup().await;
    }

    #[tokio::test]
    async fn basic_append_and_trim() {
        let cassandra = CassandraWal::create_test().await;
        cassandra.sync().await.unwrap();
        let mut fbb = FlatBufferBuilder::new();
        let transaction_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(Uuid::new_v4()),
        ));
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(Uuid::new_v4()),
        ));
        let fbb_root = AbortRequest::create(
            &mut fbb,
            &AbortRequestArgs {
                request_id,
                transaction_id,
                range_id: None,
            },
        );
        fbb.finish(fbb_root, None);
        let abort_record_bytes = fbb.finished_data();
        let abort_record = flatbuffers::root::<AbortRequest>(abort_record_bytes).unwrap();

        cassandra.append_abort(abort_record).await.unwrap();
        let first_offset = cassandra.first_offset().await.unwrap().unwrap();
        assert!(first_offset == 0);
        let next_offset = cassandra.next_offset().await.unwrap();
        assert!(next_offset == 1);

        cassandra.append_abort(abort_record).await.unwrap();
        let first_offset = cassandra.first_offset().await.unwrap().unwrap();
        assert!(first_offset == 0);
        let next_offset = cassandra.next_offset().await.unwrap();
        assert!(next_offset == 2);

        cassandra.trim_before_offset(1).await.unwrap();
        let first_offset = cassandra.first_offset().await.unwrap().unwrap();
        assert!(first_offset == 1);
        let next_offset = cassandra.next_offset().await.unwrap();
        assert!(next_offset == 2);

        cassandra.cleanup().await;
    }
}
