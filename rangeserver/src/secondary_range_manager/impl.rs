use crate::epoch_supplier::EpochSupplier;
use crate::error::Error;
use crate::range_manager::r#impl::HighestKnownEpoch;
use crate::storage::RangeInfo;
use crate::storage::Storage;
use crate::wal::Wal;
use bytes::Bytes;
use common::config::Config;
use common::full_range_id::FullRangeId;
use common::transaction_info::TransactionInfo;
use proto::rangeserver::{
    replicate_request, replicate_response, ReplicateDataResponse, ReplicateRequest,
    ReplicateResponse,
};
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::async_trait;
use tonic::Status as TStatus;
use tracing::error;
use tracing::info;

use super::log_applicator::LogApplicator;
use super::log_applicator::LogApplicatorHandle;
use super::GetResult;
use super::LoadableRange;
use super::SecondaryRangeManager as SecondaryRangeManagerTrait;

struct LoadedState {
    range_info: RangeInfo,
    highest_known_epoch: HighestKnownEpoch,
}

enum State {
    NotLoaded,
    Loading(tokio::sync::broadcast::Sender<Result<(), Error>>),
    Loaded(LoadedState),
    Unloaded,
}

pub struct ReplicationServer<S, W>
where
    S: Storage,
    W: Wal,
{
    receiver: Pin<Box<dyn Stream<Item = Result<ReplicateRequest, TStatus>> + Send>>,
    sender: tokio::sync::mpsc::Sender<Result<ReplicateResponse, TStatus>>,
    log_applicator: LogApplicatorHandle,
    wal: Arc<W>,
    storage: Arc<S>,
    ack_send_frequency: u64,
    last_acked_wal_offset: Option<u64>,
}

impl<S, W> ReplicationServer<S, W>
where
    S: Storage,
    W: Wal,
{
    pub fn new(
        range_id: FullRangeId,
        receiver: Pin<Box<dyn Stream<Item = Result<ReplicateRequest, TStatus>> + Send>>,
        sender: tokio::sync::mpsc::Sender<Result<ReplicateResponse, TStatus>>,
        storage: Arc<S>,
        wal: Arc<W>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (mut log_applicator, log_applicator_handle) =
            LogApplicator::<S, W>::new(range_id, wal.clone(), storage.clone());

        runtime.spawn(async move {
            log_applicator.apply_loop().await;
        });

        Self {
            receiver,
            sender,
            log_applicator: log_applicator_handle,
            wal,
            storage,
            ack_send_frequency: 1,
            last_acked_wal_offset: None,
        }
    }

    pub async fn serve(&mut self) -> Result<(), ReplicationError> {
        // TODO(yanniszark): Set the desired applied epoch dynamically.
        self.log_applicator.set_desired_applied_epoch(0);
        let result = self.serve_inner().await;
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Replication server failed: {:?}", e);
                Err(e)
            }
        }
    }

    async fn serve_inner(&mut self) -> Result<(), ReplicationError> {
        // TODO: Maybe add a stop channel to stop the server gracefully.
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
                    let wal_offset = data.primary_wal_offset;
                    self.wal.append_replicated_commit(data).await.unwrap();
                    self.last_acked_wal_offset = Some(wal_offset);
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
                self.sender
                    .send(Ok(response))
                    .await
                    .map_err(|e| ReplicationError::InternalError(Arc::new(e)))?;
            }
        }
        Ok(())
    }
}

pub struct SecondaryRangeManager<S, W>
where
    S: Storage,
    W: Wal,
{
    range_id: FullRangeId,
    config: Config,
    storage: Arc<S>,
    epoch_supplier: Arc<dyn EpochSupplier>,
    wal: Arc<W>,
    state: Arc<RwLock<State>>,
    bg_runtime: tokio::runtime::Handle,
    replication_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Clone, Debug, Error)]
pub enum ReplicationError {
    #[error("Replication stream dropped")]
    StreamDropped,
    #[error("Replication stream already exists")]
    StreamAlreadyExists,
    #[error("Replication stream received init on existing stream")]
    InitReceivedOnExistingStream,
    #[error("Replication stream internal error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
impl<S, W> LoadableRange for SecondaryRangeManager<S, W>
where
    S: Storage,
    W: Wal,
{
    async fn load(&self) -> Result<(), Error> {
        let sender = {
            let mut state = self.state.write().await;
            match state.deref_mut() {
                State::Loaded(_) => return Ok(()),
                State::Loading(sender) => {
                    let mut receiver = sender.subscribe();
                    drop(state);
                    return receiver.recv().await.unwrap();
                }
                State::NotLoaded => {
                    let (sender, _) = tokio::sync::broadcast::channel(1);
                    *state = State::Loading(sender.clone());
                    sender
                }
                State::Unloaded => return Err(Error::RangeIsNotLoaded),
            }
        };

        let load_result = self.load_inner().await;

        let mut state = self.state.write().await;
        match load_result {
            Err(e) => {
                *state = State::Unloaded;
                sender.send(Err(e.clone())).unwrap();
                Err(e)
            }
            Ok(loaded_state) => {
                *state = State::Loaded(loaded_state);
                // TODO(tamer): Ignoring the error here seems kind of sketchy.
                let _ = sender.send(Ok(()));
                Ok(())
            }
        }
    }

    async fn unload(&self) {
        let mut state = self.state.write().await;
        *state = State::Unloaded;
    }

    async fn is_unloaded(&self) -> bool {
        let state = self.state.read().await;
        match state.deref() {
            State::Unloaded => true,
            State::NotLoaded | State::Loading(_) | State::Loaded(_) => false,
        }
    }
}

#[async_trait]
impl<S, W> SecondaryRangeManagerTrait for SecondaryRangeManager<S, W>
where
    S: Storage,
    W: Wal,
{
    /// Get the value associated with a key.
    async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<GetResult, Error> {
        todo!("implement stale reads");
    }

    /// Sets the replication stream for this range.
    async fn start_replication(
        &self,
        recv_stream: Pin<Box<dyn Stream<Item = Result<ReplicateRequest, TStatus>> + Send>>,
        send_stream: tokio::sync::mpsc::Sender<Result<ReplicateResponse, TStatus>>,
    ) -> Result<(), ReplicationError> {
        // Is there a replication task already?
        // If there is one but it's finished, we can start a new one.
        let mut replication_task = self.replication_task.lock().await;
        match replication_task.deref() {
            Some(handle) => {
                if handle.is_finished() {
                    // Extract the result
                    // TODO: Handle this more systematically.
                    // The task is finished, we can start a new one.
                    *replication_task = None;
                } else {
                    // The task is still running, we cannot start a new one.
                    return Err(ReplicationError::StreamAlreadyExists);
                }
            }
            None => {}
        }
        // Start the replication task.
        // Create a new task to handle replication
        let storage_clone = self.storage.clone();
        let wal_clone = self.wal.clone();
        let range_id = self.range_id;
        let bg_runtime = self.bg_runtime.clone();
        *replication_task = Some(self.bg_runtime.spawn(async move {
            let mut replication_server = ReplicationServer::new(
                range_id,
                recv_stream,
                send_stream,
                storage_clone,
                wal_clone,
                bg_runtime,
            );
            let _ = replication_server.serve().await;
        }));

        Ok(())
    }
}

impl<S, W> SecondaryRangeManager<S, W>
where
    S: Storage,
    W: Wal,
{
    pub fn new(
        range_id: FullRangeId,
        config: Config,
        storage: Arc<S>,
        epoch_supplier: Arc<dyn EpochSupplier>,
        wal: W,
        bg_runtime: tokio::runtime::Handle,
    ) -> Arc<Self> {
        Arc::new(Self {
            range_id,
            config,
            storage,
            epoch_supplier,
            wal: Arc::new(wal),
            state: Arc::new(RwLock::new(State::NotLoaded)),
            bg_runtime,
            replication_task: Mutex::new(None),
        })
    }

    async fn load_inner(&self) -> Result<LoadedState, Error> {
        let epoch_supplier = self.epoch_supplier.clone();
        let storage = self.storage.clone();
        let wal = self.wal.clone();
        let range_id = self.range_id;
        let bg_runtime = self.bg_runtime.clone();
        let state = self.state.clone();
        let lease_renewal_interval = self.config.range_server.range_maintenance_duration;
        let epoch_duration = self.config.epoch.epoch_duration;
        // TODO(yanniszark): Put this in the config.
        let intended_lease_duration = Duration::from_secs(2);
        let num_epochs_per_lease = intended_lease_duration
            .as_nanos()
            .checked_div(epoch_duration.as_nanos())
            .and_then(|n| u64::try_from(n).ok())
            .unwrap();

        self.bg_runtime
            .spawn(async move {
                // TODO: handle all errors instead of panicking.
                let epoch = epoch_supplier
                    .read_epoch()
                    .await
                    .map_err(Error::from_epoch_supplier_error)?;
                let range_info = storage
                    .take_ownership_and_load_range(range_id)
                    .await
                    .map_err(Error::from_storage_error)?;
                info!("Loaded range: {:?}", range_info.id);
                // Epoch read from the provider can be 1 less than the true epoch. The highest known epoch
                // of a range cannot move backward even across range load/unloads, so to maintain that guarantee
                // we just wait for the epoch to advance once.
                info!("Read epoch: {}", epoch);
                epoch_supplier
                    .wait_until_epoch(epoch + 1, chrono::Duration::seconds(10))
                    .await
                    .map_err(|e| {
                        Error::internal_error_from_string(&format!(
                            "Failed to wait for epoch to advance: {e}"
                        ))
                    })?;

                // Get a new epoch lease.
                // Calculate how many epochs we need for a ~10 second lease
                let highest_known_epoch = epoch + 1;
                let new_epoch_lease_lower_bound =
                    std::cmp::max(highest_known_epoch, range_info.epoch_lease.1 + 1);
                let new_epoch_lease_upper_bound =
                    new_epoch_lease_lower_bound + num_epochs_per_lease;
                storage
                    .renew_epoch_lease(
                        range_id,
                        (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound),
                        range_info.leader_sequence_number,
                    )
                    .await
                    .map_err(Error::from_storage_error)?;
                wal.sync().await.map_err(Error::from_wal_error)?;
                // Create a recurrent task to renew.
                bg_runtime.spawn(async move {
                    Self::renew_epoch_lease_task(
                        range_id,
                        epoch_supplier,
                        storage,
                        state,
                        lease_renewal_interval,
                        num_epochs_per_lease,
                    )
                    .await
                });
                // TODO: apply WAL here!
                Ok(LoadedState {
                    range_info,
                    highest_known_epoch: HighestKnownEpoch::new(highest_known_epoch),
                })
            })
            .await
            .unwrap()
    }

    async fn renew_epoch_lease_task(
        range_id: FullRangeId,
        epoch_supplier: Arc<dyn EpochSupplier>,
        storage: Arc<S>,
        state: Arc<RwLock<State>>,
        lease_renewal_interval: std::time::Duration,
        num_epochs_per_lease: u64,
    ) -> Result<(), Error> {
        loop {
            let leader_sequence_number: u64;
            let old_lease: (u64, u64);
            let epoch = epoch_supplier
                .read_epoch()
                .await
                .map_err(Error::from_epoch_supplier_error)?;
            let highest_known_epoch = epoch + 1;
            if let State::Loaded(state) = state.read().await.deref() {
                old_lease = state.range_info.epoch_lease;
                leader_sequence_number = state.range_info.leader_sequence_number;
            } else {
                tokio::time::sleep(lease_renewal_interval).await;
                continue;
            }
            // How far are we from the current lease expiring? Check so we don't
            // end up taking the lease for an unbounded amount of epochs.
            let num_epochs_left = old_lease.1.saturating_sub(epoch);
            if num_epochs_left > 2 * num_epochs_per_lease {
                tokio::time::sleep(lease_renewal_interval).await;
                continue;
            }
            // TODO: If we renew too often, this could get out of hand.
            // We should probably limit the max number of epochs in the future
            // we can request a lease for.
            let new_epoch_lease_lower_bound = std::cmp::max(highest_known_epoch, old_lease.1 + 1);
            let new_epoch_lease_upper_bound = new_epoch_lease_lower_bound + num_epochs_per_lease;
            // Update the state.
            // If our new lease continues from our old lease, merge the ranges.
            let mut new_lease = (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound);
            if (new_epoch_lease_lower_bound - old_lease.1) == 1 {
                new_lease = (old_lease.0, new_epoch_lease_upper_bound);
            }
            // TODO: We should handle some errors here. For example:
            // - If the error seems transient (e.g., a timeout), we should retry.
            // - If the error is something like RangeOwnershipLost, we should unload the range.
            storage
                .renew_epoch_lease(range_id, new_lease, leader_sequence_number)
                .await
                .map_err(Error::from_storage_error)?;

            if let State::Loaded(state) = state.write().await.deref_mut() {
                // This should never happen as only this task changes the epoch lease.
                assert_eq!(
                    state.range_info.epoch_lease, old_lease,
                    "Epoch lease changed by someone else, but only this task should be changing it!"
                );
                state.range_info.epoch_lease = new_lease;
                state
                    .highest_known_epoch
                    .maybe_update(highest_known_epoch)
                    .await;
            } else {
                return Err(Error::RangeIsNotLoaded);
            }
            // Sleep for a while before renewing the lease again.
            tokio::time::sleep(lease_renewal_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use proto::rangeserver::{replicate_response, Record, ReplicateDataRequest};
    use tokio_stream::wrappers::ReceiverStream;
    use tracing::info;

    use crate::{
        for_testing::{epoch_supplier::EpochSupplier, in_memory_wal::InMemoryWal},
        storage::cassandra::{for_testing, Cassandra},
    };

    use super::*;
    use std::sync::Arc;

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
    }

    async fn init_secondary_rangemanager() -> Arc<SecondaryRangeManager<Cassandra, InMemoryWal>> {
        let mock_wal = InMemoryWal::new();
        let mock_epoch_supplier = Arc::new(EpochSupplier::new());
        let mock_runtime = tokio::runtime::Handle::current();
        let test_context = for_testing::init().await;

        let mut config: Config = Default::default();
        let protobuf_port = 50054;
        config.range_server.proto_server_addr.port = protobuf_port;

        info!("Creating secondary range manager");
        let secondary_range_manager = SecondaryRangeManager::new(
            FullRangeId {
                keyspace_id: test_context.keyspace_id.clone(),
                range_id: test_context.range_id.clone(),
            },
            config,
            test_context.cassandra.clone(),
            mock_epoch_supplier.clone(),
            mock_wal,
            mock_runtime,
        );

        info!("Loading secondary range manager");
        let srm_copy = secondary_range_manager.clone();
        let init_handle = tokio::spawn(async move { srm_copy.load().await.unwrap() });
        // Give some delay so the RM can see the epoch advancing.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        mock_epoch_supplier.set_epoch(1).await;
        init_handle.await.unwrap();
        return secondary_range_manager;
    }

    #[tokio::test]
    async fn test_start_replication() {
        init_tracing();
        let secondary_range_manager = init_secondary_rangemanager().await;
        // TODO: Create replication mapping

        info!("Starting replication");
        let (recv_stream_tx, recv_stream_rx) =
            tokio::sync::mpsc::channel::<Result<ReplicateRequest, TStatus>>(8);
        let (send_stream_tx, mut send_stream_rx) = tokio::sync::mpsc::channel(8);
        let recv_stream = Box::pin(ReceiverStream::new(recv_stream_rx))
            as Pin<Box<dyn Stream<Item = Result<ReplicateRequest, TStatus>> + Send>>;
        secondary_range_manager
            .start_replication(recv_stream, send_stream_tx)
            .await
            .unwrap();

        // Check that replication task is running
        {
            info!("Checking replication task status");
            let replication_task = secondary_range_manager.replication_task.lock().await;
            assert!(
                replication_task.is_some(),
                "Replication task should be running"
            );
        }

        // Send some data
        info!("Sending data to replication stream");
        let wal_offset = 10;
        let puts = vec![Record {
            key: vec![1, 2, 3, 4],
            value: vec![5, 6, 7, 8],
        }];
        let deletes = vec![
            vec![9, 10, 11, 12], // Random key to delete
        ];
        let data_req = ReplicateRequest {
            request: Some(replicate_request::Request::Data(ReplicateDataRequest {
                deletes: deletes,
                puts: puts,
                has_reads: false,
                transaction_id: uuid::Uuid::new_v4().to_string(),
                primary_wal_offset: wal_offset,
                epoch: 1,
            })),
        };
        recv_stream_tx.send(Ok(data_req)).await.unwrap();

        // Get back the ack
        info!("Waiting for response from replication stream");
        let response = send_stream_rx.recv().await;
        assert!(response.is_some(), "Response should be received");
        match response {
            Some(Ok(data_resp)) => {
                // Response received successfully
                if let Some(replicate_response::Response::Data(data_resp)) = data_resp.response {
                    assert_eq!(
                        data_resp.acked_wal_offset, 10,
                        "Expected DataResponse with wal_offset {wal_offset}"
                    );
                } else {
                    panic!("Expected DataResponse but got something else");
                }
            }
            Some(Err(e)) => {
                panic!("Unexpected error in replication response: {:?}", e);
            }
            None => {
                panic!("No response received from replication stream");
            }
        }
    }
}
