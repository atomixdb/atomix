use crate::{full_range_id::FullRangeId, host_info::HostInfo, keyspace::Keyspace};
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait RangeAssignmentOracle: Send + Sync + 'static {
    async fn full_range_id_of_key(&self, keyspace: &Keyspace, key: Bytes) -> Option<FullRangeId>;
    async fn host_of_range(&self, range_id: &FullRangeId) -> Option<HostInfo>;
    /// Requests refreshing the assignment.
    /// Should be used whenever a host says it does not own the range.
    fn maybe_refresh_host_of_range(&self, range_id: &FullRangeId);
    // TODO: provide APIs for key ranges / scans
}
