pub mod cassandra;

use std::sync::Arc;

use async_trait::async_trait;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use proto::rangeserver::ReplicateDataRequest;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("WAL needs to be sync'd")]
    NotSynced,
    #[error("Timeout")]
    Timeout,
    #[error("WAL Layer error: {0}")]
    Internal(Arc<dyn std::error::Error + Send + Sync>),
}

pub trait Iterator<'a> {
    fn next(&mut self) -> impl std::future::Future<Output = Option<LogEntry<'_>>> + Send;
    fn next_offset(&self) -> impl std::future::Future<Output = Result<u64, Error>> + Send;
}

#[async_trait]
pub trait Wal: Send + Sync + 'static {
    /// Must be called first before calling any other function on the WAL,
    /// when the WAL is first created or whenever any function returns a
    /// NotSynced error.
    async fn sync(&self) -> Result<(), Error>;
    /// Returns the offset of the first entry in the log (if any). Entries below
    /// the returned value would have been removed.
    async fn first_offset(&self) -> Result<Option<u64>, Error>;
    /// Returns the offset at which a new entry in the log would be inserted.
    async fn next_offset(&self) -> Result<u64, Error>;

    async fn append_prepare(&self, entry: PrepareRequest<'_>) -> Result<(), Error>;
    async fn append_commit(&self, entry: CommitRequest<'_>) -> Result<(), Error>;
    async fn append_abort(&self, entry: AbortRequest<'_>) -> Result<(), Error>;
    async fn append_replicated_commit(&self, entry: ReplicateDataRequest) -> Result<(), Error>;
    async fn trim_before_offset(&self, offset: u64) -> Result<(), Error>;

    fn iterator(&self) -> impl Iterator + Send;
}
