pub mod cassandra;

use std::sync::Arc;

use common::{full_range_id::FullRangeId, key_range::KeyRange, keyspace_id::KeyspaceId};
use scylla::{
    query::Query, statement::SerialConsistency, FromRow, SerializeRow, Session, SessionBuilder,
};
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct RangeInfo {
    pub keyspace_id: KeyspaceId,
    pub id: Uuid,
    pub key_range: KeyRange,
}

#[derive(Debug)]
pub struct RangeAssignment {
    pub range: RangeInfo,
    pub assignee: String,
}

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Persistence Layer error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

#[async_trait::async_trait]
pub trait Persistence: Send + Sync + 'static {
    async fn get_keyspace_range_map(
        &self,
        keyspace_id: &KeyspaceId,
    ) -> Result<Vec<RangeAssignment>, Error>;

    async fn update_range_assignments(
        &self,
        version: i64,
        assignments: Vec<RangeAssignment>,
    ) -> Result<(), Error>;

    async fn insert_new_ranges(&self, ranges: &Vec<RangeInfo>) -> Result<(), Error>;
}
