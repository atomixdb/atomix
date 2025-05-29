pub mod r#impl;
mod log_applicator;
mod replication_server;

use crate::error::Error;
use crate::range_manager::GetResult;
use crate::range_manager::LoadableRange;
use bytes::Bytes;
use common::transaction_info::TransactionInfo;
use proto::rangeserver::{ReplicateRequest, ReplicateResponse};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tonic::async_trait;
use tonic::Status as TStatus;

/// A trait for secondary range managers.
#[async_trait]
pub trait SecondaryRangeManager: LoadableRange {
    /// Get the value associated with a key.
    async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<GetResult, Error>;
    /// Sets the replication stream for this range.
    async fn start_replication(
        &self,
        recv_stream: Pin<Box<dyn Stream<Item = Result<ReplicateRequest, TStatus>> + Send>>,
        send_stream: tokio::sync::mpsc::Sender<Result<ReplicateResponse, TStatus>>,
    ) -> Result<(), Error>;
    async fn set_desired_applied_epoch(&self, epoch: u64) -> Result<(), Error>;
    /// Get the status of the range.
    async fn status(&self) -> Result<proto::warden::SecondaryRangeStatus, Error>;
}
