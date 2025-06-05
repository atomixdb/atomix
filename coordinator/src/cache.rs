use common::keyspace_id::KeyspaceId;
use proto::universe::KeyspaceInfo;
use tonic::async_trait;

use crate::error::Error;

pub mod in_memory;
pub mod no_op;

/// Caches the following information for the coordinator:
/// - Keyspace name, namespace -> keyspace id
/// - Keyspace id -> keyspace info (ranges, etc)
#[async_trait]
pub trait KeyspaceCache: Send + Sync + 'static {
    async fn get_keyspace_id(&self, name: &str, namespace: &str) -> Result<KeyspaceId, Error>;
    async fn get_keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo, Error>;
}
