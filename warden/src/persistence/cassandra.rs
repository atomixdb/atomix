use super::*;
use std::sync::Arc;

use common::keyspace_id::KeyspaceId;
use scylla::{
    query::Query, statement::SerialConsistency, FromRow, FromUserType, IntoUserType, SerializeCql,
    SerializeRow, Session, SessionBuilder,
};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, FromUserType, IntoUserType, SerializeCql)]
struct CqlEpochRange {
    lower_bound_inclusive: i64,
    upper_bound_inclusive: i64,
}

#[derive(Debug, FromRow, SerializeRow)]
struct SerializedRangeAssignment {
    keyspace_id: Uuid,
    range_id: Uuid,
    key_lower_bound_inclusive: Option<Vec<u8>>,
    key_upper_bound_exclusive: Option<Vec<u8>>,
    assignee: String,
}

pub struct Cassandra {
    session: Arc<Session>,
}

impl Cassandra {
    pub async fn new(known_node: String) -> Self {
        let session = Arc::new(
            SessionBuilder::new()
                .known_node(known_node)
                .build()
                .await
                .unwrap(),
        );
        Self { session }
    }
}

static INSERT_INTO_RANGE_LEASE_QUERY: &str = r#"
  INSERT INTO atomix.range_leases(range_id, key_lower_bound_inclusive, key_upper_bound_exclusive, leader_sequence_number, epoch_lease, safe_snapshot_epochs)
    VALUES (?, ?, ?, ?, ?, ?)
    IF NOT EXISTS
"#;

// TODO(purujit): To prevent writes from different Warden servers from clobbering each other,
// we will ultimately want the Warden to acquire a leader lease,
// we can then wrap this write into a Cassandra Lightweight Transaction that checks against
// the lease's sequence number.
static INSERT_OR_UPDATE_RANGE_ASSIGNMENT_QUERY: &str = r#"
  INSERT INTO atomix.range_map(keyspace_id, range_id, key_lower_bound_inclusive, key_upper_bound_exclusive, assignee)
  VALUES (?, ?, ?, ?, ?)
  "#;

#[async_trait::async_trait]
impl Persistence for Cassandra {
    async fn get_keyspace_range_map(
        &self,
        keyspace_id: &KeyspaceId,
    ) -> Result<Vec<RangeAssignment>, Error> {
        todo!();
    }

    async fn update_range_assignments(
        &self,
        version: i64,
        assignments: Vec<RangeAssignment>,
    ) -> Result<(), Error> {
        let prepared = self
            .session
            .prepare(INSERT_OR_UPDATE_RANGE_ASSIGNMENT_QUERY)
            .await
            .map_err(|op| Error::InternalError(Arc::new(op)))?;
        info!("Writing assignments for version: {}", version);
        for assignment in assignments {
            let assignment = SerializedRangeAssignment {
                keyspace_id: assignment.range.keyspace_id.id,
                range_id: assignment.range.id,
                key_lower_bound_inclusive: assignment
                    .range
                    .key_range
                    .lower_bound_inclusive
                    .map(|b| b.to_vec()),
                key_upper_bound_exclusive: assignment
                    .range
                    .key_range
                    .upper_bound_exclusive
                    .map(|b| b.to_vec()),
                assignee: assignment.assignee,
            };
            self.session
                .execute(&prepared, assignment)
                .await
                .map_err(|op| Error::InternalError(Arc::new(op)))?;
        }
        info!("Finished writing assignments for version: {}", version);
        Ok(())
    }

    async fn insert_new_ranges(&self, ranges: &Vec<RangeInfo>) -> Result<(), Error> {
        for range in ranges {
            let mut query = Query::new(INSERT_INTO_RANGE_LEASE_QUERY);
            query.set_serial_consistency(Some(SerialConsistency::Serial));
            if let Err(err) = self
                .session
                .query(
                    query,
                    (
                        range.id,
                        range
                            .key_range
                            .lower_bound_inclusive
                            .clone()
                            .map(|v| v.to_vec()),
                        range
                            .key_range
                            .upper_bound_exclusive
                            .clone()
                            .map(|v| v.to_vec()),
                        0 as i64,
                        CqlEpochRange {
                            lower_bound_inclusive: 0,
                            upper_bound_inclusive: 0,
                        },
                        CqlEpochRange {
                            lower_bound_inclusive: 0,
                            upper_bound_inclusive: 0,
                        },
                    ),
                )
                .await
            {
                return Err(Error::InternalError(Arc::new(err)));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::keyspace_id::KeyspaceId;
    use rangeserver::storage::{cassandra::Cassandra as RangeServerCassandra, Storage};

    #[tokio::test]
    async fn test_insert_range_with_none_bounds() {
        // TODO(yanniszark): This feels like more of an integration test,
        // should we move it elsewhere?
        let persistence = Cassandra::new("127.0.0.1:9042".to_string()).await;
        let rangeserver_persistence = RangeServerCassandra::new("127.0.0.1:9042".to_string()).await;

        let range = RangeInfo {
            keyspace_id: KeyspaceId::new(Uuid::new_v4()),
            id: Uuid::new_v4(),
            key_range: KeyRange {
                lower_bound_inclusive: None,
                upper_bound_exclusive: None,
            },
        };

        persistence
            .insert_new_ranges(&vec![range.clone()])
            .await
            .unwrap();

        // Query back the inserted range to verify serialization
        let lease = rangeserver_persistence
            .take_ownership_and_load_range(FullRangeId {
                keyspace_id: range.keyspace_id,
                range_id: range.id,
            })
            .await
            .unwrap();

        assert_eq!(lease.id, range.id);
        assert_eq!(
            lease.key_range.lower_bound_inclusive,
            range.key_range.lower_bound_inclusive
        );
        assert_eq!(
            lease.key_range.upper_bound_exclusive,
            range.key_range.upper_bound_exclusive
        );
    }
}
