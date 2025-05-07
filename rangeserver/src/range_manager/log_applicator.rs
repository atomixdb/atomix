use bytes::Bytes;
use common::full_range_id::FullRangeId;
use prost::Message;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
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

struct DesiredAppliedEpoch {
    val: AtomicU64,
    notify: Notify,
}

impl DesiredAppliedEpoch {
    fn new(epoch: u64) -> Arc<Self> {
        Arc::new(Self {
            val: AtomicU64::new(epoch),
            notify: Notify::new(),
        })
    }

    fn set(&self, epoch: u64) {
        let prev = self.val.swap(epoch, Ordering::SeqCst);
        if epoch < prev {
            panic!("Epoch {} is less than previous epoch {}", epoch, prev);
        }
        self.notify.notify_one();
    }

    fn get(&self) -> u64 {
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
    applied_secondary_offset: Option<u64>,
    desired_applied_epoch: Arc<DesiredAppliedEpoch>,
    range_id: FullRangeId,
    wal: Arc<W>,
    storage: Arc<S>,
    cancellation_token: CancellationToken,
}

pub struct LogApplicatorHandle {
    desired_applied_epoch: Arc<DesiredAppliedEpoch>,
    cancellation_token: CancellationToken,
}

impl LogApplicatorHandle {
    pub fn set_desired_applied_epoch(&self, epoch: u64) {
        self.desired_applied_epoch.set(epoch);
    }

    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }
}

impl<S, W> LogApplicator<S, W>
where
    S: Storage,
    W: Wal,
{
    pub fn new(range_id: FullRangeId, wal: Arc<W>, storage: Arc<S>) -> (Self, LogApplicatorHandle) {
        let desired_applied_epoch = DesiredAppliedEpoch::new(0);
        let cancellation_token = CancellationToken::new();
        let child_token = cancellation_token.child_token();
        (
            Self {
                wal,
                storage,
                // TODO(yanniszark): Get this from storage.
                applied_secondary_offset: None,
                desired_applied_epoch: desired_applied_epoch.clone(),
                range_id,
                cancellation_token: child_token,
            },
            LogApplicatorHandle {
                desired_applied_epoch,
                cancellation_token,
            },
        )
    }

    pub async fn apply_loop(&mut self) {
        info!("Starting log applicator loop");
        loop {
            self.apply_loop_inner().await;
        }
    }

    async fn apply_loop_inner(&mut self) {
        // 1. Iterate over the WAL, starting from the applied offset.
        // 2. For each entry, check if the epoch is less or equal than the desired applied epoch.
        // 3. If it is, apply the entry to the storage.
        // 4. If it is not, break the loop.
        let mut wal_iterator = self.wal.iterator(self.applied_secondary_offset);
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
                    return;
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
            greatest_seen_epoch = Some(replicated_commit.epoch);

            // Wait until the epoch is greater than the desired applied epoch.
            loop {
                if replicated_commit.epoch > self.desired_applied_epoch.get() {
                    info!("Reached end of desired epoch.");
                    self.desired_applied_epoch.wait_for_updates().await;
                    continue;
                }
                break;
            }

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
            info!(
                "Successfully applied log entry with id: {}",
                replicated_commit.transaction_id
            );
            self.applied_secondary_offset = Some(entry_offset);
        }
    }
}
