use crate::epoch_supplier::EpochSupplier;
use crate::error::Error;
use crate::prefetching_buffer::PrefetchingBuffer;
use crate::storage::RangeInfo;
use crate::storage::Storage;
use crate::wal::Wal;
use bytes::Bytes;
use common::config::Config;
use common::full_range_id::FullRangeId;
use common::transaction_info::TransactionInfo;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use proto::rangeserver::ReplicatedCommitRequest;
use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tonic::async_trait;
use uuid::Uuid;

use super::GetResult;
use super::LoadableRange;
use super::PrepareResult;
use super::{
    RangeManager as RangeManagerTrait, SecondaryRangeManager as SecondaryRangeManagerTrait,
};

struct LoadedState {
    range_info: RangeInfo,
    highest_known_epoch: u64,
    highest_replicated_epoch: u64,
}

enum State {
    NotLoaded,
    Loading(tokio::sync::broadcast::Sender<Result<(), Error>>),
    Loaded(LoadedState),
    Unloaded,
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
    /// Append to the secondary commit log.
    /// Will be applied later in a coordinated manner.
    async fn commit_replicated(&self, commit: ReplicatedCommitRequest) -> Result<(), Error> {
        self.wal.append_replicated_commit(commit).await.unwrap();
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
                // Epoch read from the provider can be 1 less than the true epoch. The highest known epoch
                // of a range cannot move backward even across range load/unloads, so to maintain that guarantee
                // we just wait for the epoch to advance once.
                epoch_supplier
                    .wait_until_epoch(epoch + 1, chrono::Duration::seconds(10))
                    .await
                    .map_err(Error::from_epoch_supplier_error)?;
                // Get a new epoch lease.
                let highest_known_epoch = epoch + 1;
                let new_epoch_lease_lower_bound =
                    std::cmp::max(highest_known_epoch, range_info.epoch_lease.1 + 1);
                let new_epoch_lease_upper_bound = new_epoch_lease_lower_bound + 10;
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
                    )
                    .await
                });
                // TODO: apply WAL here!
                Ok(LoadedState {
                    range_info,
                    highest_known_epoch,
                    // TODO: this should be set to the highest replicated epoch.
                    highest_replicated_epoch: 0,
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
            // TODO: If we renew too often, this could get out of hand.
            // We should probably limit the max number of epochs in the future
            // we can request a lease for.
            let new_epoch_lease_lower_bound = std::cmp::max(highest_known_epoch, old_lease.1 + 1);
            let new_epoch_lease_upper_bound = new_epoch_lease_lower_bound + 10;
            // TODO: We should handle some errors here. For example:
            // - If the error seems transient (e.g., a timeout), we should retry.
            // - If the error is something like RangeOwnershipLost, we should unload the range.
            storage
                .renew_epoch_lease(
                    range_id,
                    (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound),
                    leader_sequence_number,
                )
                .await
                .map_err(Error::from_storage_error)?;

            // Update the state.
            // If our new lease continues from our old lease, merge the ranges.
            let mut new_lease = (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound);
            if (new_epoch_lease_lower_bound - old_lease.1) == 1 {
                new_lease = (old_lease.0, new_epoch_lease_upper_bound);
            }
            if let State::Loaded(state) = state.write().await.deref_mut() {
                // This should never happen as only this task changes the epoch lease.
                assert_eq!(
                    state.range_info.epoch_lease, old_lease,
                    "Epoch lease changed by someone else, but only this task should be changing it!"
                );
                state.range_info.epoch_lease = new_lease;
                state.highest_known_epoch =
                    std::cmp::max(state.highest_known_epoch, highest_known_epoch);
            } else {
                return Err(Error::RangeIsNotLoaded);
            }
            // Sleep for a while before renewing the lease again.
            tokio::time::sleep(lease_renewal_interval).await;
        }
    }
}
