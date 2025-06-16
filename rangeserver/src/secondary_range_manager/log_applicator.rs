use bytes::Bytes;
use common::full_range_id::FullRangeId;
use prost::Message;
use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

use flatbuf::rangeserver_flatbuffers::range_server::Entry;
use proto::rangeserver::ReplicateDataRequest;

use crate::{
    key_version::KeyVersion,
    storage::Storage,
    wal::{Iterator, Wal},
};

use super::r#impl::{AtomicEpoch, AtomicWalOffset};

pub struct DesiredAppliedEpoch {
    val: AtomicU64,
    notify: Notify,
}

impl DesiredAppliedEpoch {
    pub fn new(epoch: u64) -> Self {
        Self {
            val: AtomicU64::new(epoch),
            notify: Notify::new(),
        }
    }

    pub fn set(&self, epoch: u64) {
        let prev = self.val.swap(epoch, Ordering::SeqCst);
        if epoch < prev {
            panic!("Epoch {} is less than previous epoch {}", epoch, prev);
        }
        self.notify.notify_one();
    }

    pub fn get(&self) -> u64 {
        self.val.load(Ordering::SeqCst)
    }

    async fn wait_for_updates(&self) {
        self.notify.notified().await;
    }
}

pub struct LogApplicator<S, W>
where
    S: Storage,
    W: Wal,
{
    desired_applied_epoch: Arc<DesiredAppliedEpoch>,
    actual_applied_epoch: Arc<AtomicEpoch>,
    applied_secondary_wal_offset: Arc<AtomicWalOffset>,
    range_id: FullRangeId,
    wal: Arc<W>,
    storage: Arc<S>,
    runtime: tokio::runtime::Handle,
    cancellation_token: CancellationToken,
}

pub struct LogApplicatorHandle {
    pub desired_applied_epoch: Arc<DesiredAppliedEpoch>,
    applied_secondary_wal_offset: Arc<AtomicWalOffset>,
    actual_applied_epoch: Arc<AtomicEpoch>,
    cancellation_token: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl LogApplicatorHandle {
    pub fn get_actual_applied_epoch(&self) -> Option<u64> {
        self.actual_applied_epoch.get()
    }

    pub fn get_applied_secondary_wal_offset(&self) -> Option<u64> {
        self.applied_secondary_wal_offset.get()
    }

    pub async fn stop(&mut self) {
        self.cancellation_token.cancel();
        info!("Log applicator stopping...");
        if let Some(task) = self.task.take() {
            task.await
                .unwrap_or_else(|e| error!("Log applicator task failed: {}", e));
        }
        info!("Log applicator stopped.");
    }
}

impl<S, W> LogApplicator<S, W>
where
    S: Storage,
    W: Wal,
{
    pub fn new(
        range_id: FullRangeId,
        applied_secondary_wal_offset: Arc<AtomicWalOffset>,
        actual_applied_epoch: Arc<AtomicEpoch>,
        desired_applied_epoch: Arc<DesiredAppliedEpoch>,
        wal: Arc<W>,
        storage: Arc<S>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let child_token = cancellation_token.child_token();
        Self {
            wal,
            storage,
            runtime,
            applied_secondary_wal_offset,
            actual_applied_epoch,
            desired_applied_epoch,
            range_id,
            cancellation_token: child_token,
        }
    }

    pub fn serve(mut self) -> LogApplicatorHandle {
        info!("Starting log applicator loop");
        let cancellation_token = self.cancellation_token.clone();
        let desired_applied_epoch = self.desired_applied_epoch.clone();
        let actual_applied_epoch = self.actual_applied_epoch.clone();
        let applied_secondary_wal_offset = self.applied_secondary_wal_offset.clone();
        let runtime = self.runtime.clone();
        let task = runtime.spawn(async move {
            self.serve_inner().await;
            self.cancellation_token.cancel();
        });

        LogApplicatorHandle {
            desired_applied_epoch,
            actual_applied_epoch,
            applied_secondary_wal_offset,
            cancellation_token,
            task: Some(task),
        }
    }

    async fn serve_inner(&mut self) {
        // 1. Iterate over the WAL, starting after the applied offset.
        // 2. For each entry, check if the epoch is less or equal than the desired applied epoch.
        // 3. If it is, apply the entry to the storage.
        // 4. If it is not, break the loop.

        let _ = self.runtime.spawn(Self::update_secondary_status_task(
            self.range_id,
            self.applied_secondary_wal_offset.clone(),
            self.storage.clone(),
            self.cancellation_token.clone(),
        ));

        let mut wal_iterator = self.wal.iterator(self.applied_secondary_wal_offset.get());
        let mut greatest_seen_epoch: Option<u64> = None;
        loop {
            if self.cancellation_token.is_cancelled() {
                info!("Log applicator cancelled.");
                return;
            }
            let entry_offset = wal_iterator.next_offset().await.unwrap();
            let entry = match wal_iterator.next().await {
                Some(entry) => entry,
                None => {
                    // Reached the end of the WAL.
                    trace!("Reached the end of the WAL.");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            if entry.entry() != Entry::ReplicatedCommit {
                panic!("Expected a ReplicatedCommit entry. Got {:?}", entry.entry());
            }
            let bytes = entry.bytes().unwrap();
            let replicated_commit =
                ReplicateDataRequest::decode(bytes.bytes()).unwrap_or_else(|e| {
                    panic!(
                        "Failed to decode ReplicateDataRequest on secondary wal: {}",
                        e
                    );
                });

            // TODO(yanniszark): This code assumes that the epoch appears
            // monotonically increasing in the WAL. For now, we use a sanity
            // check here to guarantee the invariant.
            if greatest_seen_epoch.is_none() {
                greatest_seen_epoch = Some(replicated_commit.epoch);
            }
            if replicated_commit.epoch < greatest_seen_epoch.unwrap() {
                panic!("Epoch is not monotonically increasing in the WAL.");
            }
            if replicated_commit.epoch > greatest_seen_epoch.unwrap() {
                self.actual_applied_epoch.set(greatest_seen_epoch.unwrap());
            }
            greatest_seen_epoch = Some(replicated_commit.epoch);

            // Wait until the epoch is greater than the desired applied epoch.
            loop {
                if replicated_commit.epoch > self.desired_applied_epoch.get() {
                    info!(
                        "Reached end of desired epoch. Desired epoch: {}, actual epoch: {}",
                        self.desired_applied_epoch.get(),
                        replicated_commit.epoch
                    );
                    self.desired_applied_epoch.wait_for_updates().await;
                    continue;
                }
                break;
            }

            // TODO(yanniszark): Parallelize log application using something
            // like the C5 algorithm.

            // Apply to storage
            for put in replicated_commit.puts {
                self.storage
                    .upsert(
                        self.range_id,
                        Bytes::from(put.key.clone()),
                        Bytes::from(put.value),
                        KeyVersion {
                            epoch: replicated_commit.epoch,
                            // TODO(yanniszark): Do we need to set this?
                            version_counter: 0,
                        },
                    )
                    .await
                    .unwrap_or_else(|e| {
                        error!(
                            "Failed to upsert key {:?} on secondary wal: {}",
                            Bytes::from(put.key),
                            e
                        );
                    });
            }
            for delete in replicated_commit.deletes {
                self.storage
                    .delete(
                        self.range_id,
                        Bytes::from(delete.clone()),
                        KeyVersion {
                            epoch: replicated_commit.epoch,
                            // TODO(yanniszark): Do we need to set this?
                            version_counter: 0,
                        },
                    )
                    .await
                    .unwrap_or_else(|e| {
                        error!(
                            "Failed to delete key {:?} on secondary wal: {}",
                            Bytes::from(delete),
                            e
                        );
                    });
            }

            debug!(
                "Successfully applied log entry with id: {}",
                replicated_commit.transaction_id
            );
            self.applied_secondary_wal_offset.set(entry_offset);
            self.actual_applied_epoch.set(replicated_commit.epoch);
        }
    }

    async fn update_secondary_status_task(
        range_id: FullRangeId,
        applied_secondary_wal_offset: Arc<AtomicWalOffset>,
        storage: Arc<S>,
        cancellation_token: CancellationToken,
    ) {
        let mut last_persisted_offset = None;
        loop {
            if cancellation_token.is_cancelled() {
                info!("Log applicator cancelled.");
                return;
            }

            // If the offset has not changed, sleep for a bit.
            if last_persisted_offset == Some(applied_secondary_wal_offset.get()) {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            // Update the secondary status.
            let last_applied_offset = applied_secondary_wal_offset.get();
            match storage
                .update_secondary_status(
                    range_id,
                    last_applied_offset,
                    // TODO(yanniszark): Get this from the rangemanager.
                    0,
                )
                .await
            {
                Ok(_) => {
                    last_persisted_offset = Some(last_applied_offset);
                }
                Err(e) => {
                    error!("Failed to update secondary status on secondary wal: {}", e);
                }
            }

            // Sleep for a bit.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
