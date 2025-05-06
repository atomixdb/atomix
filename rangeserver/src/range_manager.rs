pub mod r#impl;
mod lock_table;
mod replication_client;
pub mod r#secondary;

use crate::error::Error;
use bytes::Bytes;
use common::replication_mapping::ReplicationMapping;
use common::transaction_info::TransactionInfo;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use proto::rangeserver::{ReplicateRequest, ReplicateResponse};
use secondary::ReplicationError;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tonic::Status as TStatus;
use tonic::{async_trait, Streaming};
use uuid::Uuid;

pub struct GetResult {
    pub val: Option<Bytes>,
    pub leader_sequence_number: i64,
}

pub struct PrepareResult {
    pub highest_known_epoch: u64,
    pub epoch_lease: (u64, u64),
}

#[async_trait]
pub trait LoadableRange {
    /// Load and manage the range.
    async fn load(&self) -> Result<(), Error>;
    /// unload the range.
    /// If the range is ever been unloaded, the same RangeManager cannot be
    /// reused again, and a new one should be created for the same range.
    async fn unload(&self);
    /// Returns true if the range is ever been unloaded, false otherwise.
    async fn is_unloaded(&self) -> bool;
}

#[async_trait]
pub trait RangeManager: LoadableRange {
    /// Request prefetching a key from storage and pinning to memory.
    async fn prefetch(&self, transaction_id: Uuid, key: Bytes) -> Result<(), Error>;
    /// Get the value associated with a key.
    async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<GetResult, Error>;
    /// Run the prepare phase of two-phase commit.
    /// If prepare ever returns success, the implementation must be able to
    /// (eventually) commit the transaction no matter what, unless we get an
    /// abort call from the coordinator or know for certain that the
    /// transaction aborted.
    // It is possible that prepare gets called multiple times due to retransmits
    /// etc., so the implementation must be able to handle that.
    async fn prepare(
        &self,
        tx: Arc<TransactionInfo>,
        prepare: PrepareRequest<'_>,
    ) -> Result<PrepareResult, Error>;
    /// Abort the transaction.
    async fn abort(&self, tx: Arc<TransactionInfo>, abort: AbortRequest<'_>) -> Result<(), Error>;
    /// Run the commit phase of two-phase commit.
    /// Commit *informs* the range manager of a transaction commit, it does not
    /// decide the transaction outcome.
    /// A call to commit can fail only for intermittent reasons, and must be
    /// idempotent and safe to retry any number of times.
    async fn commit(
        &self,
        tx: Arc<TransactionInfo>,
        commit: CommitRequest<'_>,
    ) -> Result<(), Error>;
    /// Starts replicating the range to a secondary range.
    async fn start_replication(&self, replication_mapping: ReplicationMapping)
        -> Result<(), Error>;
}

/// A trait for secondary range managers.
/// TODO: We don't need all the methods of RangeManager here. Introduce a new
/// trait that includes the common methods, which are mainly about range
/// management. Basically, the lifecycle of both primary and secondary ranges
/// should be similar in terms of getting / renewing leases.
#[async_trait]

pub trait SecondaryRangeManager: LoadableRange {
    /// Get the value associated with a key.
    async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<GetResult, Error>;
    /// Sets the replication stream for this range.
    async fn start_replication(
        &self,
        recv_stream: Pin<Box<dyn Stream<Item = Result<ReplicateRequest, TStatus>> + Send>>,
        send_stream: tokio::sync::mpsc::Sender<Result<ReplicateResponse, TStatus>>,
    ) -> Result<(), ReplicationError>;
}
