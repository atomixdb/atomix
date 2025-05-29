use super::r#impl::AtomicEpoch;
use super::r#impl::ReplicationError;
use super::SecondaryRangeManager as SecondaryRangeManagerTrait;
use crate::storage::Storage;
use crate::wal::Wal;
use common::full_range_id::FullRangeId;
use proto::rangeserver::{
    replicate_request, replicate_response, ReplicateDataResponse, ReplicateRequest,
    ReplicateResponse,
};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::Status as TStatus;
use tracing::error;
use tracing::info;

pub struct ReplicationServer<S, W>
where
    S: Storage,
    W: Wal,
{
    range_id: FullRangeId,
    receiver: Pin<Box<dyn Stream<Item = Result<ReplicateRequest, TStatus>> + Send>>,
    sender: tokio::sync::mpsc::Sender<Result<ReplicateResponse, TStatus>>,
    // The offset of the last primary WAL entry that has been persisted.
    last_acked_wal_offset: Option<u64>,
    // The corresponding epoch of the last primary WAL entry that has been persisted.
    wal_epoch: Arc<AtomicEpoch>,
    ack_send_frequency: u64,
    cancellation_token: CancellationToken,
    wal: Arc<W>,
    storage: Arc<S>,
}

pub struct ReplicationServerHandle {
    /// The epoch of the last WAL entry that has been persisted in the secondary.
    cancellation_token: CancellationToken,
    task: Option<tokio::task::JoinHandle<Result<(), ReplicationError>>>,
}

impl ReplicationServerHandle {
    pub async fn stop(&mut self) {
        // Signal the cancellation token and join the task.
        self.cancellation_token.cancel();
        let task = self.task.take();
        if let Some(task) = task {
            if let Err(e) = task.await {
                error!("Replication server task failed: {}", e);
            }
        }
    }
}

impl<S, W> ReplicationServer<S, W>
where
    S: Storage,
    W: Wal,
{
    pub fn new(
        range_id: FullRangeId,
        wal_epoch: Arc<AtomicEpoch>,
        receiver: Pin<Box<dyn Stream<Item = Result<ReplicateRequest, TStatus>> + Send>>,
        sender: tokio::sync::mpsc::Sender<Result<ReplicateResponse, TStatus>>,
        storage: Arc<S>,
        wal: Arc<W>,
    ) -> Self {
        Self {
            range_id,
            receiver,
            sender,
            wal,
            storage,
            ack_send_frequency: 1,
            last_acked_wal_offset: None,
            wal_epoch,
            cancellation_token: CancellationToken::new(),
        }
    }

    pub fn serve(mut self, runtime: &tokio::runtime::Handle) -> ReplicationServerHandle {
        // TODO(yanniszark): Set the desired applied epoch dynamically.
        let cancellation_token_clone = self.cancellation_token.clone();

        let task = runtime.spawn(async move {
            let cancellation_token = self.cancellation_token.clone();
            let result = tokio::select! {
                result = self.serve_inner() => result,
                _ = cancellation_token.cancelled() => {
                    info!("Replication server cancelled for range {}", self.range_id.range_id);
                    Ok(())
                }
            };
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    error!(
                        "Replication server error for range {}: {}",
                        self.range_id.range_id, e
                    );
                    Err(e)
                }
            }
        });

        ReplicationServerHandle {
            cancellation_token: cancellation_token_clone,
            task: Some(task),
        }
    }

    async fn serve_inner(&mut self) -> Result<(), ReplicationError> {
        // TODO(yanniszark): Add a cancellation token for the replication server.
        info!(
            "Starting replication server for range {}",
            self.range_id.range_id
        );
        let mut message_counter = 0;
        loop {
            // Get the next message from the stream.
            let message = match self.receiver.next().await {
                Some(Ok(msg)) => msg,
                None => return Err(ReplicationError::StreamDropped),
                Some(Err(e)) => return Err(ReplicationError::InternalError(Arc::new(e))),
            };
            // Process the message.
            match message.request.unwrap() {
                replicate_request::Request::Init(_) => {
                    return Err(ReplicationError::InitReceivedOnExistingStream);
                }
                replicate_request::Request::Data(data) => {
                    // Persist to WAL
                    info!("Received data for range {}", self.range_id.range_id);
                    let wal_offset = data.primary_wal_offset;
                    let epoch = data.epoch;
                    self.wal.append_replicated_commit(data).await.unwrap();
                    self.last_acked_wal_offset = Some(wal_offset);
                    // The last completely replicated epoch is the epoch of the
                    // last WAL entry minus 1. This is because epochs in the
                    // log are monotonically increasing.
                    match epoch {
                        0 => {}
                        epoch => {
                            self.wal_epoch.set(epoch - 1);
                        }
                    }
                }
            }
            message_counter += 1;
            if message_counter % self.ack_send_frequency == 0 {
                // Send an ack back to the client.
                let response = ReplicateResponse {
                    response: Some(replicate_response::Response::Data(ReplicateDataResponse {
                        acked_wal_offset: self.last_acked_wal_offset.unwrap(),
                    })),
                };
                // TODO(yanniszark): Should we block here?
                self.sender
                    .send(Ok(response))
                    .await
                    .map_err(|e| ReplicationError::InternalError(Arc::new(e)))?;
            }
        }
        Ok(())
    }
}
