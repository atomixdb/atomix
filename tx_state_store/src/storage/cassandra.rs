use super::*;
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use scylla::transport::PagingState;
use scylla::Session;
use scylla::SessionBuilder;

pub struct Cassandra {
    session: Session,
}

static START_TRANSACTION_QUERY: &str = r#"
  INSERT INTO atomix.transactions (transaction_id, status) 
    VALUES (?, 'started') 
    IF NOT EXISTS
"#;

static COMMIT_TRANSACTION_QUERY: &str = r#"
  UPDATE atomix.transactions SET status = 'committed', epoch = ?
    WHERE transaction_id = ? 
"#;

static ABORT_TRANSACTION_QUERY: &str = r#"
  UPDATE atomix.transactions SET status = 'aborted'
    WHERE transaction_id = ? 
    IF status IN ('started', 'aborted')
"#;

static GET_COMMIT_EPOCH_QUERY: &str = r#"
  SELECT epoch from atomix.transactions
    WHERE transaction_id = ? 
    AND status = 'committed'
    ALLOW FILTERING
"#;

fn scylla_query_error_to_storage_error(qe: QueryError) -> Error {
    match qe {
        QueryError::TimeoutError | QueryError::DbError(DbError::WriteTimeout { .. }, _) => {
            Error::Timeout
        }
        _ => Error::InternalError(Arc::new(qe)),
    }
}

fn get_serial_query(query_text: impl Into<String>) -> Query {
    let mut query = Query::new(query_text);
    query.set_serial_consistency(Some(SerialConsistency::Serial));
    query
}

impl Cassandra {
    pub async fn new(known_node: String) -> Cassandra {
        let session = SessionBuilder::new()
            .known_node(known_node)
            .build()
            .await
            .unwrap();
        Cassandra { session }
    }

    async fn maybe_get_commit_epoch(&self, transaction_id: Uuid) -> Result<OpResult, Error> {
        let query = get_serial_query(GET_COMMIT_EPOCH_QUERY);
        let query_result = self
            .session
            .query_single_page(query, (transaction_id,), PagingState::start())
            .await
            .map_err(scylla_query_error_to_storage_error)?
            .0
            .rows;
        let res = match query_result {
            None => Ok(OpResult::TransactionIsAborted),
            Some(mut rows) => {
                if rows.len() > 1 {
                    panic!("found multiple results for a transaction record");
                } else {
                    let row = rows.pop();
                    match row {
                        None => Ok(OpResult::TransactionIsAborted),
                        Some(row) => {
                            let epoch = row.columns[0].as_ref().unwrap().as_bigint().unwrap();
                            Ok(OpResult::TransactionIsCommitted(CommitInfo {
                                epoch: epoch as u64,
                            }))
                        }
                    }
                }
            }
        };
        res
    }
}

impl Storage for Cassandra {
    async fn start_transaction(&self, transaction_id: Uuid) -> Result<(), Error> {
        let query = get_serial_query(START_TRANSACTION_QUERY);
        let _ = self
            .session
            .query_single_page(query, (transaction_id,), PagingState::start())
            .await
            .map_err(scylla_query_error_to_storage_error)?;
        Ok(())
    }

    async fn commit_transaction(
        &self,
        transaction_id: Uuid,
        epoch: u64,
    ) -> Result<OpResult, Error> {
        let query = get_serial_query(COMMIT_TRANSACTION_QUERY);
        let query_result = self
            .session
            .query_single_page(query, (epoch as i64, transaction_id), PagingState::start())
            .await
            .map_err(scylla_query_error_to_storage_error)?
            .0
            .rows;
        // let res = match query_result {
        //     None => panic!("no results results from a LWT"),
        //     Some(mut rows) => {
        //         if rows.len() != 1 {
        //             panic!("found multiple results from a LWT");
        //         } else {
        //             let row = rows.pop().unwrap();
        //             let applied = row.columns[0].as_ref().unwrap().as_boolean().unwrap();
        //             if applied {
        //                 Ok(OpResult::TransactionIsCommitted(CommitInfo { epoch }))
        //             } else {
        //                 Ok(OpResult::TransactionIsAborted)
        //             }
        //         }
        //     }
        // };
        // res
        Ok(OpResult::TransactionIsCommitted(CommitInfo { epoch }))
    }

    async fn abort_transaction(&self, transaction_id: Uuid) -> Result<OpResult, Error> {
        let query = get_serial_query(ABORT_TRANSACTION_QUERY);
        let query_result = self
            .session
            .query_single_page(query, (transaction_id,), PagingState::start())
            .await
            .map_err(scylla_query_error_to_storage_error)?
            .0
            .rows;
        let res = match query_result {
            None => panic!("no results results from a LWT"),
            Some(mut rows) => {
                if rows.len() != 1 {
                    panic!("found multiple results from a LWT");
                } else {
                    let row = rows.pop().unwrap();
                    let applied = row.columns[0].as_ref().unwrap().as_boolean().unwrap();
                    if applied {
                        Ok(OpResult::TransactionIsAborted)
                    } else {
                        // This could be either because the transaction is committed, or the
                        // row has been deleted (presumed abort).
                        // Let's check which, and fetch the epoch in case of commit.
                        self.maybe_get_commit_epoch(transaction_id).await
                    }
                }
            }
        };
        res
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;

    impl Cassandra {
        async fn create_test() -> Cassandra {
            Cassandra::new("127.0.0.1:9042".to_string()).await
        }
    }

    #[tokio::test]
    async fn start_commit() {
        let cassandra = Cassandra::create_test().await;
        let tx_id = Uuid::new_v4();
        cassandra.start_transaction(tx_id).await.unwrap();
        match cassandra.commit_transaction(tx_id, 5).await.unwrap() {
            OpResult::TransactionIsAborted => panic!("expected transaction to commit"),
            OpResult::TransactionIsCommitted(c) => assert!(c.epoch == 5),
        }
    }

    #[tokio::test]
    async fn start_abort() {
        let cassandra = Cassandra::create_test().await;
        let tx_id = Uuid::new_v4();
        cassandra.start_transaction(tx_id).await.unwrap();
        match cassandra.abort_transaction(tx_id).await.unwrap() {
            OpResult::TransactionIsCommitted(_) => panic!("expected transaction to abort"),
            OpResult::TransactionIsAborted => (),
        }
    }

    #[tokio::test]
    async fn abort_committed() {
        let cassandra = Cassandra::create_test().await;
        let tx_id = Uuid::new_v4();
        cassandra.start_transaction(tx_id).await.unwrap();
        cassandra.commit_transaction(tx_id, 5).await.unwrap();
        match cassandra.abort_transaction(tx_id).await.unwrap() {
            OpResult::TransactionIsAborted => panic!("expected transaction to commit"),
            OpResult::TransactionIsCommitted(c) => assert!(c.epoch == 5),
        }
    }

    #[tokio::test]
    async fn commit_aborted() {
        let cassandra = Cassandra::create_test().await;
        let tx_id = Uuid::new_v4();
        cassandra.start_transaction(tx_id).await.unwrap();
        cassandra.abort_transaction(tx_id).await.unwrap();
        match cassandra.commit_transaction(tx_id, 6).await.unwrap() {
            OpResult::TransactionIsCommitted(_) => panic!("expected transaction to abort"),
            OpResult::TransactionIsAborted => (),
        }
    }

    #[tokio::test]
    async fn presumed_abort() {
        let cassandra = Cassandra::create_test().await;
        let tx_id = Uuid::new_v4();
        match cassandra.commit_transaction(tx_id, 19).await.unwrap() {
            OpResult::TransactionIsCommitted(_) => panic!("expected transaction to abort"),
            OpResult::TransactionIsAborted => (),
        };
        match cassandra.abort_transaction(tx_id).await.unwrap() {
            OpResult::TransactionIsCommitted(_) => panic!("expected transaction to abort"),
            OpResult::TransactionIsAborted => (),
        }
    }
}
