use super::{GetResult, PrepareResult, RangeManager as Trait};

use crate::{
    epoch_supplier::EpochSupplier, error::Error, key_version::KeyVersion,
    range_manager::lock_table, storage::RangeInfo, storage::Storage,
    transaction_abort_reason::TransactionAbortReason, wal::Wal,
};
use bytes::Bytes;
use common::config::Config;
use common::full_range_id::FullRangeId;
use common::transaction_info::TransactionInfo;

use uuid::Uuid;

use crate::prefetching_buffer::KeyState;
use crate::prefetching_buffer::PrefetchingBuffer;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tonic::async_trait;

struct LoadedState {
    range_info: RangeInfo,
    highest_known_epoch: HighestKnownEpoch,
    lock_table: lock_table::LockTable,
    // TODO: need more efficient representation of prepares than raw bytes.
    pending_prepare_records: Mutex<HashMap<Uuid, Bytes>>,
}

enum State {
    NotLoaded,
    Loading(tokio::sync::broadcast::Sender<Result<(), Error>>),
    Loaded(LoadedState),
    Unloaded,
}

struct HighestKnownEpoch {
    val: RwLock<u64>,
}

impl HighestKnownEpoch {
    fn new(e: u64) -> HighestKnownEpoch {
        HighestKnownEpoch {
            val: RwLock::new(e),
        }
    }

    async fn read(&self) -> u64 {
        *self.val.read().await
    }

    async fn maybe_update(&self, new_epoch: u64) {
        let mut v = self.val.write().await;
        *v = std::cmp::max(*v, new_epoch)
    }
}

pub struct RangeManager<S, W>
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
    prefetching_buffer: Arc<PrefetchingBuffer>,
    bg_runtime: tokio::runtime::Handle,
}

#[async_trait]
impl<S, W> Trait for RangeManager<S, W>
where
    S: Storage,
    W: Wal,
{
    async fn load(&self) -> Result<(), Error> {
        if let State::Loaded(_) = self.state.read().await.deref() {
            return Ok(());
        }
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

    async fn prefetch(&self, transaction_id: Uuid, key: Bytes) -> Result<(), Error> {
        // Request prefetch from the prefetching buffer
        let keystate = self
            .prefetching_buffer
            .process_prefetch_request(transaction_id, key.clone())
            .await;

        match keystate {
            KeyState::Fetched => Ok(()), // key has previously been fetched
            KeyState::Loading(_) => Err(Error::PrefetchError), // Something is wrong if loading was returned
            KeyState::Requested(fetch_sequence_number) =>
            // key has just been requested - start fetch
            {
                // Fetch from database
                let val = match self.prefetch_get(key.clone()).await {
                    Ok(value) => value,
                    Err(_) => {
                        self.prefetching_buffer
                            .fetch_failed(key.clone(), fetch_sequence_number)
                            .await;
                        return Err(Error::PrefetchError);
                    }
                };
                // Successfully fetched from database -> add to buffer and update records
                self.prefetching_buffer
                    .fetch_complete(key.clone(), val, fetch_sequence_number)
                    .await;
                Ok(())
            }
        }
    }

    async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<GetResult, Error> {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                if !state.range_info.key_range.includes(key.clone()) {
                    return Err(Error::KeyIsOutOfRange);
                };
                self.acquire_range_lock(state, tx.clone()).await?;

                let mut get_result = GetResult {
                    val: None,
                    leader_sequence_number: state.range_info.leader_sequence_number as i64,
                };

                // check prefetch buffer
                let value = self
                    .prefetching_buffer
                    .get_from_buffer(key.clone())
                    .await
                    .map_err(|_| Error::PrefetchError)?;
                if let Some(val) = value {
                    get_result.val = Some(val);
                } else {
                    let val = self
                        .storage
                        .get(self.range_id, key.clone())
                        .await
                        .map_err(Error::from_storage_error)?;

                    get_result.val = val.clone();
                }

                Ok(get_result)
            }
        }
    }

    async fn prepare(
        &self,
        tx: Arc<TransactionInfo>,
        prepare: PrepareRequest<'_>,
    ) -> Result<PrepareResult, Error> {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => {
                return Err(Error::RangeIsNotLoaded)
            }
            State::Loaded(state) => {
                // Sanity check that the written keys are all within this range.
                // TODO: check delete and write sets are non-overlapping.
                for put in prepare.puts().iter() {
                    for put in put.iter() {
                        // TODO: too much copying :(
                        let key = Bytes::copy_from_slice(put.key().unwrap().k().unwrap().bytes());
                        if !state.range_info.key_range.includes(key) {
                            return Err(Error::KeyIsOutOfRange);
                        }
                    }
                }
                for del in prepare.deletes().iter() {
                    for del in del.iter() {
                        let key = Bytes::copy_from_slice(del.k().unwrap().bytes());
                        if !state.range_info.key_range.includes(key) {
                            return Err(Error::KeyIsOutOfRange);
                        }
                    }
                }
                // Validate the transaction lock is not lost, this is essential to ensure 2PL
                // invariants still hold.

                if prepare.has_reads() && !state.lock_table.is_currently_holding(tx.clone()).await {
                    return Err(Error::TransactionAborted(
                        TransactionAbortReason::TransactionLockLost,
                    ));
                }

                self.acquire_range_lock(state, tx.clone()).await?;
                {
                    // TODO: probably don't need holding that latch while writing to the WAL.
                    // but needs careful thinking.
                    let mut pending_prepare_records = state.pending_prepare_records.lock().await;
                    self.wal
                        .append_prepare(prepare)
                        .await
                        .map_err(Error::from_wal_error)?;

                    pending_prepare_records
                        .insert(tx.id, Bytes::copy_from_slice(prepare._tab.buf()));
                }

                let highest_known_epoch = state.highest_known_epoch.read().await;

                Ok(PrepareResult {
                    highest_known_epoch,
                    epoch_lease: state.range_info.epoch_lease,
                })
            }
        }
    }

    async fn abort(&self, tx: Arc<TransactionInfo>, abort: AbortRequest<'_>) -> Result<(), Error> {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => {
                return Err(Error::RangeIsNotLoaded)
            }
            State::Loaded(state) => {
                if !state.lock_table.is_currently_holding(tx.clone()).await {
                    return Ok(());
                }
                {
                    // TODO: We can skip aborting to the log if we never appended a prepare record.
                    // TODO: It's possible the WAL already contains this record in case this is a retry
                    // so avoid re-inserting in that case.
                    self.wal
                        .append_abort(abort)
                        .await
                        .map_err(Error::from_wal_error)?;
                }
                state.lock_table.release().await;

                let _ = self
                    .prefetching_buffer
                    .process_transaction_complete(tx.id)
                    .await;
                Ok(())
            }
        }
    }

    async fn commit(
        &self,
        tx: Arc<TransactionInfo>,
        commit: CommitRequest<'_>,
    ) -> Result<(), Error> {
        let s = self.state.read().await;
        match s.deref() {
            State::NotLoaded | State::Unloaded | State::Loading(_) => {
                return Err(Error::RangeIsNotLoaded)
            }
            State::Loaded(state) => {
                if !state.lock_table.is_currently_holding(tx.clone()).await {
                    // it must be that we already finished committing, but perhaps the coordinator didn't
                    // realize that, so we just return success.
                    return Ok(());
                }
                state.highest_known_epoch.maybe_update(commit.epoch()).await;

                // TODO: handle potential duplicates here.
                self.wal
                    .append_commit(commit)
                    .await
                    .map_err(Error::from_wal_error)?;
                let prepare_record_bytes = {
                    let mut pending_prepare_records = state.pending_prepare_records.lock().await;
                    // TODO: handle prior removals.
                    pending_prepare_records.remove(&tx.id).unwrap().clone()
                };

                let prepare_record =
                    flatbuffers::root::<PrepareRequest>(&prepare_record_bytes).unwrap();
                let version = KeyVersion {
                    epoch: commit.epoch(),
                    // TODO: version counter should be an internal counter per range.
                    // Remove from the commit message.
                    version_counter: commit.vid() as u64,
                };
                // TODO: we shouldn't be doing a storage operation per individual key put or delete.
                // Instead we should write them in batches, and whenever we do multiple operations they
                // should go in parallel not sequentially.
                // We should also add retries in case of intermittent failures. Note that all our
                // storage operations here are idempotent and safe to retry any number of times.
                // We also don't need to be holding the state latch for that long.
                for put in prepare_record.puts().iter() {
                    for put in put.iter() {
                        // TODO: too much copying :(
                        let key = Bytes::copy_from_slice(put.key().unwrap().k().unwrap().bytes());
                        let val = Bytes::copy_from_slice(put.value().unwrap().bytes());

                        // TODO: we should do the storage writes lazily in the background
                        self.storage
                            .upsert(self.range_id, key.clone(), val.clone(), version)
                            .await
                            .map_err(Error::from_storage_error)?;

                        // Update the prefetch buffer if this key has been requested by a prefetch call
                        self.prefetching_buffer.upsert(key, val).await;
                    }
                }
                for del in prepare_record.deletes().iter() {
                    for del in del.iter() {
                        let key = Bytes::copy_from_slice(del.k().unwrap().bytes());

                        // TODO: we should do the storage writes lazily in the background
                        self.storage
                            .delete(self.range_id, key.clone(), version)
                            .await
                            .map_err(Error::from_storage_error)?;

                        // Delete the key from the prefetch buffer if this key has been requested by a prefetch call
                        self.prefetching_buffer.delete(key).await;
                    }
                }

                // We apply the writes to storage before releasing the lock since we send all
                // gets to storage directly. We should implement a memtable to allow us to release
                // the lock sooner.
                state.lock_table.release().await;
                // Process transaction complete and remove the requests from the logs
                self.prefetching_buffer
                    .process_transaction_complete(tx.id)
                    .await;
                Ok(())
            }
        }
    }
}

impl<S, W> RangeManager<S, W>
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
        prefetching_buffer: Arc<PrefetchingBuffer>,
        bg_runtime: tokio::runtime::Handle,
    ) -> Arc<Self> {
        Arc::new(RangeManager {
            range_id,
            config,
            storage,
            epoch_supplier,
            wal: Arc::new(wal),
            state: Arc::new(RwLock::new(State::NotLoaded)),
            prefetching_buffer,
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
        let epoch_duration = self.config.epoch.epoch_duration;
        // Calculate how many epochs we need for the desired lease duration.
        // TODO(yanniszark): Put this in the config.
        let intended_lease_duration = Duration::from_secs(2);
        let num_epochs_per_lease = intended_lease_duration
            .as_nanos()
            .checked_div(epoch_duration.as_nanos())
            .and_then(|n| u64::try_from(n).ok())
            .unwrap();
        // Ensure that we have at least one epoch per lease.
        let num_epochs_per_lease = std::cmp::max(1, num_epochs_per_lease);

        self.bg_runtime
            .spawn(async move {
                // TODO: handle all errors instead of panicking.
                let epoch = epoch_supplier
                    .read_epoch()
                    .await
                    .map_err(Error::from_epoch_supplier_error)?;
                let mut range_info = storage
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
                range_info.epoch_lease = (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound);
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
                    lock_table: lock_table::LockTable::new(),
                    pending_prepare_records: Mutex::new(HashMap::new()),
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

    async fn acquire_range_lock(
        &self,
        state: &LoadedState,
        tx: Arc<TransactionInfo>,
    ) -> Result<(), Error> {
        let receiver = state.lock_table.acquire(tx.clone()).await?;
        // TODO: allow timing out locks when transaction timeouts are implemented.
        receiver.await.unwrap();
        Ok(())
    }

    /// Get from database without acquiring any locks
    /// This is a very basic copy of the 'get' function
    pub async fn prefetch_get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        let val = self
            .storage
            .get(self.range_id, key.clone())
            .await
            .map_err(Error::from_storage_error)?;
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use common::config::{
        CassandraConfig, EpochConfig, FrontendConfig, HostPort, RangeServerConfig, UniverseConfig,
    };
    use common::transaction_info::TransactionInfo;
    use common::util;
    use core::time;
    use flatbuffers::FlatBufferBuilder;
    use std::str::FromStr;
    use uuid::Uuid;

    use super::*;
    use crate::for_testing::epoch_supplier::EpochSupplier;
    use crate::for_testing::in_memory_wal::InMemoryWal;
    use crate::storage::cassandra::Cassandra;

    type RM = RangeManager<Cassandra, InMemoryWal>;

    impl RM {
        async fn abort_transaction(&self, tx: Arc<TransactionInfo>) {
            let mut fbb = FlatBufferBuilder::new();
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let fbb_root = AbortRequest::create(
                &mut fbb,
                &AbortRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                },
            );
            fbb.finish(fbb_root, None);
            let abort_record_bytes = fbb.finished_data();
            let abort_record = flatbuffers::root::<AbortRequest>(abort_record_bytes).unwrap();
            self.abort(tx.clone(), abort_record).await.unwrap()
        }

        async fn prepare_transaction(
            &self,
            tx: Arc<TransactionInfo>,
            writes: Vec<(Bytes, Bytes)>,
            deletes: Vec<Bytes>,
            has_reads: bool,
        ) -> Result<(), Error> {
            let mut fbb = FlatBufferBuilder::new();
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let mut puts_vector = Vec::new();
            for (k, v) in writes {
                let k = Some(fbb.create_vector(k.to_vec().as_slice()));
                let key = Key::create(&mut fbb, &KeyArgs { k });
                let value = fbb.create_vector(v.to_vec().as_slice());
                puts_vector.push(Record::create(
                    &mut fbb,
                    &RecordArgs {
                        key: Some(key),
                        value: Some(value),
                    },
                ));
            }
            let puts = Some(fbb.create_vector(&puts_vector));
            let mut del_vector = Vec::new();
            for k in deletes {
                let k = Some(fbb.create_vector(k.to_vec().as_slice()));
                let key = Key::create(&mut fbb, &KeyArgs { k });
                del_vector.push(key);
            }
            let deletes = Some(fbb.create_vector(&del_vector));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let fbb_root = PrepareRequest::create(
                &mut fbb,
                &PrepareRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                    has_reads,
                    puts,
                    deletes,
                },
            );
            fbb.finish(fbb_root, None);
            let prepare_record_bytes = fbb.finished_data();
            let prepare_record = flatbuffers::root::<PrepareRequest>(prepare_record_bytes).unwrap();
            self.prepare(tx.clone(), prepare_record).await.map(|_| ())
        }

        async fn commit_transaction(&self, tx: Arc<TransactionInfo>) -> Result<(), Error> {
            let epoch = self.epoch_supplier.read_epoch().await.unwrap();
            let mut fbb = FlatBufferBuilder::new();
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let fbb_root = CommitRequest::create(
                &mut fbb,
                &CommitRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                    epoch,
                    vid: 0,
                },
            );
            fbb.finish(fbb_root, None);
            let commit_record_bytes = fbb.finished_data();
            let commit_record = flatbuffers::root::<CommitRequest>(commit_record_bytes).unwrap();
            self.commit(tx.clone(), commit_record).await
        }
    }

    struct TestContext {
        rm: Arc<RM>,
        storage_context: crate::storage::cassandra::for_testing::TestContext,
    }

    async fn init() -> TestContext {
        let epoch_supplier = Arc::new(EpochSupplier::new());
        let storage_context: crate::storage::cassandra::for_testing::TestContext =
            crate::storage::cassandra::for_testing::init().await;
        let cassandra = storage_context.cassandra.clone();
        let prefetching_buffer = Arc::new(PrefetchingBuffer::new());
        let range_id = FullRangeId {
            keyspace_id: storage_context.keyspace_id,
            range_id: storage_context.range_id,
        };
        let epoch_config = EpochConfig {
            // Not used in these tests.
            proto_server_addr: "127.0.0.1:50052".parse().unwrap(),
            epoch_duration: time::Duration::from_millis(10),
        };
        let config = Config {
            range_server: RangeServerConfig {
                range_maintenance_duration: time::Duration::from_secs(1),
                proto_server_addr: HostPort::from_str("127.0.0.1:50054").unwrap(),
                fast_network_addr: HostPort::from_str("127.0.0.1:50055").unwrap(),
            },
            universe: UniverseConfig {
                proto_server_addr: "127.0.0.1:123".parse().unwrap(),
            },
            frontend: FrontendConfig {
                proto_server_addr: "127.0.0.1:124".parse().unwrap(),
                fast_network_addr: HostPort::from_str("127.0.0.1:125").unwrap(),
                transaction_overall_timeout: time::Duration::from_secs(10),
            },
            cassandra: CassandraConfig {
                cql_addr: HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 9042,
                },
            },
            regions: std::collections::HashMap::new(),
            epoch: epoch_config,
        };
        let rm = Arc::new(RM {
            range_id,
            config,
            storage: cassandra,
            wal: Arc::new(InMemoryWal::new()),
            epoch_supplier: epoch_supplier.clone(),
            state: Arc::new(RwLock::new(State::NotLoaded)),
            prefetching_buffer,
            bg_runtime: tokio::runtime::Handle::current().clone(),
        });
        let rm_copy = rm.clone();
        let init_handle = tokio::spawn(async move { rm_copy.load().await.unwrap() });
        // Give some delay so the RM can see the epoch advancing.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        epoch_supplier.set_epoch(1).await;
        init_handle.await.unwrap();
        TestContext {
            rm,
            storage_context,
        }
    }

    fn start_transaction() -> Arc<TransactionInfo> {
        Arc::new(TransactionInfo {
            id: Uuid::new_v4(),
            started: chrono::Utc::now(),
            overall_timeout: time::Duration::from_secs(10),
        })
    }

    #[tokio::test]
    async fn basic_get_put() {
        let context = init().await;
        let rm = context.rm.clone();
        let key = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
        let tx1 = start_transaction();
        assert!(rm
            .get(tx1.clone(), key.clone())
            .await
            .unwrap()
            .val
            .is_none());
        rm.abort_transaction(tx1.clone()).await;
        let tx2 = start_transaction();
        let val = Bytes::from_static(b"I have a value!");
        rm.prepare_transaction(
            tx2.clone(),
            Vec::from([(key.clone(), val.clone())]),
            Vec::new(),
            false,
        )
        .await
        .unwrap();
        rm.commit_transaction(tx2.clone()).await.unwrap();
        let tx3 = start_transaction();
        let val_after_commit = rm.get(tx3.clone(), key.clone()).await.unwrap().val.unwrap();
        assert!(val_after_commit == val);
    }

    #[tokio::test]
    async fn test_recurring_lease_renewal() {
        let context = init().await;
        let rm = context.rm.clone();
        // Get the current lease bounds.
        let initial_lease = match rm.state.read().await.deref() {
            State::Loaded(state) => state.range_info.epoch_lease,
            _ => panic!("Range is not loaded"),
        };
        // Sleep for 2 seconds to allow the lease renewal task to run at least once.
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let final_lease = match rm.state.read().await.deref() {
            State::Loaded(state) => state.range_info.epoch_lease,
            _ => panic!("Range is not loaded"),
        };
        // Check that the upper bound has increased.
        assert!(
            final_lease.1 > initial_lease.1,
            "Lease upper bound did not increase"
        );
    }
}
