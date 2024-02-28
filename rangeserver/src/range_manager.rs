use crate::{
    epoch_provider::EpochProvider, epoch_provider::Error as EpochProviderError,
    key_version::KeyVersion, persistence::Error as PersistenceError, persistence::Persistence,
    persistence::RangeInfo, transaction_abort_reason::TransactionAbortReason,
    transaction_info::TransactionInfo, wal::Error as WalError, wal::Iterator, wal::Wal,
};
use bytes::Bytes;
use chrono::DateTime;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Debug)]
enum Error {
    RangeDoesNotExist,
    RangeIsNotLoaded,
    KeyIsOutOfRange,
    RangeOwnershipLost,
    Timeout,
    TransactionAborted(TransactionAbortReason),
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

impl Error {
    fn from_persistence_error(e: PersistenceError) -> Self {
        match e {
            PersistenceError::RangeDoesNotExist => Self::RangeDoesNotExist,
            PersistenceError::RangeOwnershipLost => Self::RangeOwnershipLost,
            PersistenceError::Timeout => Self::Timeout,
            PersistenceError::InternalError(_) => Self::InternalError(Arc::new(e)),
        }
    }

    fn from_wal_error(e: WalError) -> Self {
        match e {
            WalError::Unknown => Self::InternalError(Arc::new(e)),
        }
    }

    fn from_epoch_provider_error(e: EpochProviderError) -> Self {
        match e {
            EpochProviderError::Unknown => Self::InternalError(Arc::new(e)),
        }
    }
}

type UtcDateTime = DateTime<chrono::Utc>;
struct CurrentLockHolder {
    transaction: Arc<TransactionInfo>,
    when_acquired: UtcDateTime,
    when_requested: UtcDateTime,
}

struct LockRequest {
    transaction: Arc<TransactionInfo>,
    sender: oneshot::Sender<()>,
    when_requested: UtcDateTime,
}

// Implements transaction lock table for the range.
// Currently there is just a single lock for the entire range despite having
// "Table" in the name, but we might partition the lock to allow for more
// concurrency down the line.
struct LockTable {
    current_holder: Option<CurrentLockHolder>,
    waiting_for_release: VecDeque<LockRequest>,
    waiting_to_acquire: VecDeque<LockRequest>,
}

impl LockTable {
    pub fn new() -> LockTable {
        LockTable {
            current_holder: None,
            waiting_for_release: VecDeque::new(),
            waiting_to_acquire: VecDeque::new(),
        }
    }
    pub fn maybe_wait_for_current_holder(
        &mut self,
        tx: Arc<TransactionInfo>,
    ) -> oneshot::Receiver<()> {
        let (s, r) = oneshot::channel();
        match &self.current_holder {
            None => s.send(()).unwrap(),
            Some(_) => {
                let req = LockRequest {
                    transaction: tx.clone(),
                    sender: s,
                    when_requested: chrono::Utc::now(),
                };
                self.waiting_for_release.push_back(req);
            }
        };
        r
    }

    pub fn acquire(&mut self, tx: Arc<TransactionInfo>) -> Result<oneshot::Receiver<()>, Error> {
        let when_requested = chrono::Utc::now();
        let (s, r) = oneshot::channel();
        match &self.current_holder {
            None => {
                let holder = CurrentLockHolder {
                    transaction: tx.clone(),
                    when_requested,
                    when_acquired: when_requested,
                };
                self.current_holder = Some(holder);
                s.send(()).unwrap();
                Ok(r)
            }
            Some(current_holder) => {
                if current_holder.transaction.id == tx.id {
                    s.send(()).unwrap();
                    Ok(r)
                } else {
                    let highest_waiter = self
                        .waiting_to_acquire
                        .back()
                        .map_or(current_holder.transaction.id, |r| r.transaction.id);
                    if highest_waiter > tx.id {
                        // TODO: allow for skipping these checks if locks are ordered!
                        Err(Error::TransactionAborted(TransactionAbortReason::WaitDie))
                    } else {
                        let req = LockRequest {
                            transaction: tx.clone(),
                            sender: s,
                            when_requested: chrono::Utc::now(),
                        };
                        self.waiting_to_acquire.push_back(req);
                        Ok(r)
                    }
                }
            }
        }
    }

    pub fn release(&mut self) {
        self.current_holder = None;
        while !self.waiting_for_release.is_empty() {
            let req = self.waiting_for_release.pop_front().unwrap();
            req.sender.send(()).unwrap();
        }
        match self.waiting_to_acquire.pop_front() {
            None => (),
            Some(req) => {
                let when_acquired = chrono::Utc::now();
                let new_holder = CurrentLockHolder {
                    transaction: req.transaction.clone(),
                    when_requested: req.when_requested,
                    when_acquired,
                };
                self.current_holder = Some(new_holder);
                req.sender.send(()).unwrap();
            }
        }
    }

    pub fn is_currently_holding(&self, tx: Arc<TransactionInfo>) -> bool {
        match &self.current_holder {
            None => false,
            Some(current) => current.transaction.id == tx.id,
        }
    }
}

struct LoadedState {
    range_info: RangeInfo,
    highest_known_epoch: u64,
    lock_table: Mutex<LockTable>,
}

enum State {
    Unloaded,
    Loading,
    Loaded(LoadedState),
}

struct RangeManager<P, E, W>
where
    P: Persistence,
    E: EpochProvider,
    W: Wal,
{
    persistence: Arc<P>,
    epoch_provider: Arc<E>,
    wal: Mutex<W>,
    range_id: Uuid,
    state: RwLock<State>,
}

impl<P, E, W> RangeManager<P, E, W>
where
    P: Persistence,
    E: EpochProvider,
    W: Wal,
{
    pub fn new(range_id: Uuid, persistence: Arc<P>, epoch_provider: Arc<E>, wal: W) -> Box<Self> {
        Box::new(RangeManager {
            range_id,
            persistence,
            epoch_provider,
            wal: Mutex::new(wal),
            state: RwLock::new(State::Unloaded),
        })
    }

    async fn load_inner(&self) -> Result<LoadedState, Error> {
        // TODO: handle all errors instead of panicing.
        let epoch = self
            .epoch_provider
            .read_epoch()
            .await
            .map_err(Error::from_epoch_provider_error)?;
        let range_info = self
            .persistence
            .take_ownership_and_load_range(self.range_id)
            .await
            .map_err(Error::from_persistence_error)?;
        // Epoch read from the provider can be 1 less than the true epoch. The highest known epoch
        // of a range cannot move backward even across range load/unloads, so to maintain that guarantee
        // we just wait for the epoch to advance once.
        self.epoch_provider
            .wait_until_epoch(epoch + 1)
            .await
            .map_err(Error::from_epoch_provider_error)?;
        // Get a new epoch lease.
        // TODO: Create a recurrent task to renew.
        let highest_known_epoch = epoch + 1;
        let new_epoch_lease_lower_bound =
            std::cmp::max(highest_known_epoch, range_info.epoch_lease.1 + 1);
        let new_epoch_lease_upper_bound = new_epoch_lease_lower_bound + 10;
        self.persistence
            .renew_epoch_lease(
                self.range_id,
                (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound),
                range_info.leader_sequence_number,
            )
            .await
            .map_err(Error::from_persistence_error)?;
        // TODO: apply WAL here!
        Ok(LoadedState {
            range_info,
            highest_known_epoch,
            lock_table: Mutex::new(LockTable::new()),
        })
    }

    pub async fn load(&self) -> Result<(), Error> {
        {
            let mut state = self.state.write().await;
            match *state {
                State::Loaded(_) => return Ok(()),
                State::Loading => todo!(),
                State::Unloaded => {
                    *state = State::Loading;
                    true
                }
            }
        };
        let load_result: Result<LoadedState, Error> = self.load_inner().await;
        let mut state = self.state.write().await;
        match load_result {
            Err(e) => {
                *state = State::Unloaded;
                Err(e)
            }
            Ok(loaded_state) => {
                *state = State::Loaded(loaded_state);
                Ok(())
            }
        }
    }

    async fn acquire_range_lock(
        &self,
        state: &LoadedState,
        tx: Arc<TransactionInfo>,
    ) -> Result<(), Error> {
        let mut lock_table = state.lock_table.lock().await;
        let receiver = lock_table.acquire(tx.clone())?;
        // TODO: allow timing out locks when transaction timeouts are implemented.
        receiver.await.unwrap();
        Ok(())
    }

    pub async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<Option<Bytes>, Error> {
        let s = self.state.write().await;
        match s.deref() {
            State::Unloaded | State::Loading => Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                self.acquire_range_lock(state, tx.clone()).await?;
                let val = self
                    .persistence
                    .get(self.range_id, key.clone())
                    .await
                    .map_err(Error::from_persistence_error)?;
                Ok(val.clone())
            }
        }
    }

    // If prepare ever returns success, we must be able to (eventually) commit the
    // transaction no matter what, unless we get an abort call from the coordinator
    // or know for certain that the transaction aborted.
    // It is possible that prepare gets called multiple times due to retries from the
    // coordinator, so we must be able to handle that.
    pub async fn prepare(
        &self,
        tx: Arc<TransactionInfo>,
        prepare: PrepareRecord<'_>,
    ) -> Result<(), Error> {
        let s = self.state.write().await;
        match s.deref() {
            State::Unloaded | State::Loading => return Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                {
                    let lock_table = state.lock_table.lock().await;
                    if prepare.has_reads() && !lock_table.is_currently_holding(tx.clone()) {
                        return Err(Error::TransactionAborted(
                            TransactionAbortReason::TransactionLockLost,
                        ));
                    }
                };
                self.acquire_range_lock(state, tx.clone()).await?;
                {
                    let mut wal = self.wal.lock().await;
                    wal.append_prepare(prepare)
                        .await
                        .map_err(Error::from_wal_error)?;
                }

                Ok(())
            }
        }
    }

    pub async fn abort(
        &self,
        tx: Arc<TransactionInfo>,
        abort: AbortRecord<'_>,
    ) -> Result<(), Error> {
        let s = self.state.write().await;
        match s.deref() {
            State::Unloaded | State::Loading => return Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                let mut lock_table = state.lock_table.lock().await;
                if !lock_table.is_currently_holding(tx.clone()) {
                    return Ok(());
                }
                {
                    // TODO: We can skip aborting to the log if we never appended a prepare record.
                    let mut wal = self.wal.lock().await;
                    // TODO: It's possible the WAL already contains this record in case this is a retry
                    // so avoid re-inserting in that case.
                    wal.append_abort(abort)
                        .await
                        .map_err(Error::from_wal_error)?;
                }
                lock_table.release();
                Ok(())
            }
        }
    }

    // Commit *informs* the range manager of a transaction commit, it does not
    // decide the transaction outcome.
    // A call to commit can fail only for intermittent reasons, and must be
    // idempotent and safe to retry any number of times.
    pub async fn commit(
        &self,
        tx: Arc<TransactionInfo>,
        commit: CommitRecord<'_>,
    ) -> Result<(), Error> {
        let s = self.state.write().await;
        match s.deref() {
            State::Unloaded | State::Loading => return Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                let mut lock_table = state.lock_table.lock().await;
                if !lock_table.is_currently_holding(tx.clone()) {
                    // it must be that we already finished committing, but perhaps the coordinator didn't
                    // realize that, so we just return success.
                    return Ok(());
                }
                {
                    // TODO: update highest known epoch!
                    let mut wal = self.wal.lock().await;
                    wal.append_commit(commit)
                        .await
                        .map_err(Error::from_wal_error)?;
                    // Find the corresponding prepare entry in the WAL to get the writes.
                    // This is quite inefficient, we should cache a copy in memory instead, but for now
                    // it's convenient to also test WAL iteration.
                    let mut wal_iterator = wal.iterator();
                    let prepare_record = {
                        loop {
                            let next = wal_iterator.next().await;
                            match next {
                                None => break None,
                                Some(entry) => match entry.entry() {
                                    Entry::Prepare => {
                                        let bytes = entry.bytes().unwrap().bytes();
                                        let flatbuf =
                                            flatbuffers::root::<PrepareRecord>(bytes).unwrap();
                                        let tid =
                                            Uuid::parse_str(flatbuf.transaction_id().unwrap())
                                                .unwrap();
                                        if tid == tx.id {
                                            break (Some(flatbuf));
                                        }
                                    }
                                    _ => (),
                                },
                            }
                        }
                    }
                    .unwrap();
                    let version = KeyVersion {
                        epoch: commit.epoch() as u64,
                        // TODO: version counter should be an internal counter per range.
                        // Remove from the commit message.
                        version_counter: commit.vid() as u64,
                    };
                    // TODO: we shouldn't be doing a persistence operation per individual key put or delete.
                    // Instead we should write them in batches, and whenever we do multiple operations they
                    // should go in parallel not sequentially.
                    // We should also add retries in case of intermittent failures. Note that all our
                    // persistence operations here are idempotent and safe to retry any number of times.
                    for put in prepare_record.puts().iter() {
                        for put in put.iter() {
                            // TODO: too much copying :(
                            let key =
                                Bytes::copy_from_slice(put.key().unwrap().k().unwrap().bytes());
                            let val = Bytes::copy_from_slice(put.value().unwrap().bytes());

                            self.persistence
                                .upsert(self.range_id, key, val, version)
                                .await
                                .map_err(Error::from_persistence_error)?;
                        }
                    }
                    for del in prepare_record.deletes().iter() {
                        for del in del.iter() {
                            let key = Bytes::copy_from_slice(del.k().unwrap().bytes());
                            self.persistence
                                .delete(self.range_id, key, version)
                                .await
                                .map_err(Error::from_persistence_error)?;
                        }
                    }
                }

                // We apply the writes to persistence before releasing the lock since we send all
                // gets to persistence directly. We should implement a memtable to allow us to release
                // the lock sooner.
                lock_table.release();
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use flatbuffers::FlatBufferBuilder;

    use super::*;
    use crate::epoch_provider::EpochProvider as EpochProviderTrait;
    use crate::for_testing::epoch_provider::EpochProvider;
    use crate::for_testing::in_memory_wal::InMemoryWal;
    use crate::persistence::cassandra::tests::TEST_RANGE_UUID;
    use crate::persistence::cassandra::Cassandra;

    type RM = RangeManager<Cassandra, EpochProvider, InMemoryWal>;

    impl RM {
        async fn abort_transaction(&self, tx: Arc<TransactionInfo>) {
            let mut fbb = FlatBufferBuilder::new();
            let tx_id_string = tx.id.to_string();
            let transaction_id = Some(fbb.create_string(&tx_id_string));
            let fbb_root = AbortRecord::create(&mut fbb, &AbortRecordArgs { transaction_id });
            fbb.finish(fbb_root, None);
            let abort_record_bytes = fbb.finished_data();
            let abort_record = flatbuffers::root::<AbortRecord>(abort_record_bytes).unwrap();
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
            let tx_id_string = tx.id.to_string();
            let transaction_id = Some(fbb.create_string(&tx_id_string));
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
            let range_id_string = self.range_id.to_string();
            let range_id = Some(fbb.create_string(&range_id_string));
            let fbb_root = PrepareRecord::create(
                &mut fbb,
                &PrepareRecordArgs {
                    transaction_id,
                    range_id,
                    has_reads,
                    puts,
                    deletes,
                },
            );
            fbb.finish(fbb_root, None);
            let prepare_record_bytes = fbb.finished_data();
            let prepare_record = flatbuffers::root::<PrepareRecord>(prepare_record_bytes).unwrap();
            self.prepare(tx.clone(), prepare_record).await
        }

        async fn commit_transaction(&self, tx: Arc<TransactionInfo>) -> Result<(), Error> {
            let epoch = self.epoch_provider.read_epoch().await.unwrap();
            let mut fbb = FlatBufferBuilder::new();
            let tx_id_string = tx.id.to_string();
            let transaction_id = Some(fbb.create_string(&tx_id_string));
            let fbb_root = CommitRecord::create(
                &mut fbb,
                &CommitRecordArgs {
                    transaction_id,
                    epoch: epoch as i64,
                    vid: 0,
                },
            );
            fbb.finish(fbb_root, None);
            let commit_record_bytes = fbb.finished_data();
            let commit_record = flatbuffers::root::<CommitRecord>(commit_record_bytes).unwrap();
            self.commit(tx.clone(), commit_record).await
        }
    }

    async fn init() -> Arc<RM> {
        let epoch_provider = Arc::new(EpochProvider::new());
        let wal = Mutex::new(InMemoryWal::new());
        let cassandra = Arc::new(crate::persistence::cassandra::tests::init().await);
        let range_id = Uuid::parse_str(TEST_RANGE_UUID).unwrap();
        let rm = Arc::new(RM {
            range_id,
            persistence: cassandra,
            wal,
            epoch_provider,
            state: RwLock::new(State::Unloaded),
        });
        let rm_copy = rm.clone();
        let init_handle = tokio::spawn(async move { rm_copy.load().await.unwrap() });
        let epoch_provider = rm.epoch_provider.clone();
        // Give some delay so the RM can see the epoch advancing.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        epoch_provider.set_epoch(1).await;
        init_handle.await.unwrap();
        rm
    }

    fn start_transaction() -> Arc<TransactionInfo> {
        Arc::new(TransactionInfo { id: Uuid::new_v4() })
    }

    #[tokio::test]
    async fn basic_get_put() {
        let rm = init().await;
        let key = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
        let tx1 = start_transaction();
        assert!(rm.get(tx1.clone(), key.clone()).await.unwrap().is_none());
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
        let val_after_commit = rm.get(tx3.clone(), key.clone()).await.unwrap().unwrap();
        assert!(val_after_commit == val);
    }
}
