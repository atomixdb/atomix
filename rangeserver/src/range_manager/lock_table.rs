use crate::{error::Error, transaction_abort_reason::TransactionAbortReason};
use chrono::DateTime;
use common::transaction_info::TransactionInfo;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

type UtcDateTime = DateTime<chrono::Utc>;

pub enum LockResult {
    Acquired,
    WaitToAcquire(oneshot::Receiver<()>),
}

pub struct CurrentLockHolder {
    transaction: Arc<TransactionInfo>,
    when_acquired: UtcDateTime,
    when_requested: UtcDateTime,
}

pub struct LockRequest {
    transaction: Arc<TransactionInfo>,
    sender: oneshot::Sender<()>,
    when_requested: UtcDateTime,
}

struct State {
    current_holder: Option<CurrentLockHolder>,
    waiting_for_release: VecDeque<LockRequest>,
    waiting_to_acquire: VecDeque<LockRequest>,
}

// Implements transaction lock table for the range.
// Currently there is just a single lock for the entire range despite having
// "Table" in the name, but we might partition the lock to allow for more
// concurrency down the line.
pub struct LockTable {
    state: RwLock<State>,
}

impl LockTable {
    pub fn new() -> LockTable {
        LockTable {
            state: RwLock::new(State {
                current_holder: None,
                waiting_for_release: VecDeque::new(),
                waiting_to_acquire: VecDeque::new(),
            }),
        }
    }
    pub async fn maybe_wait_for_current_holder(
        &self,
        tx: Arc<TransactionInfo>,
    ) -> oneshot::Receiver<()> {
        let (s, r) = oneshot::channel();
        let mut state = self.state.write().await;
        match &state.current_holder {
            None => s.send(()).unwrap(),
            Some(_) => {
                let req = LockRequest {
                    transaction: tx.clone(),
                    sender: s,
                    when_requested: chrono::Utc::now(),
                };
                state.waiting_for_release.push_back(req);
            }
        };
        r
    }

    pub async fn acquire(&self, tx: Arc<TransactionInfo>) -> Result<LockResult, Error> {
        let when_requested = chrono::Utc::now();
        let (s, r) = oneshot::channel();
        let mut state = self.state.write().await;
        match &state.current_holder {
            None => {
                let holder = CurrentLockHolder {
                    transaction: tx.clone(),
                    when_requested,
                    when_acquired: when_requested,
                };
                state.current_holder = Some(holder);
                s.send(()).unwrap();
                Ok(LockResult::Acquired)
            }
            Some(current_holder) => {
                if current_holder.transaction.id == tx.id {
                    s.send(()).unwrap();
                    Ok(LockResult::Acquired)
                } else {
                    let highest_waiter = state
                        .waiting_to_acquire
                        .back()
                        .map_or(current_holder.transaction.id, |r| r.transaction.id);
                    // TODO(yanniszark): re-enable this check.
                    // if highest_waiter > tx.id {
                    //     // TODO: allow for skipping these checks if locks are ordered!
                    //     Err(Error::TransactionAborted(TransactionAbortReason::WaitDie))
                    // } else {
                    let req = LockRequest {
                        transaction: tx.clone(),
                        sender: s,
                        when_requested: chrono::Utc::now(),
                    };
                    state.waiting_to_acquire.push_back(req);
                    Ok(LockResult::WaitToAcquire(r))
                    // }
                }
            }
        }
    }

    pub async fn release(&self) {
        let mut state = self.state.write().await;
        state.current_holder = None;
        while !state.waiting_for_release.is_empty() {
            let req = state.waiting_for_release.pop_front().unwrap();
            req.sender.send(()).unwrap();
        }
        match state.waiting_to_acquire.pop_front() {
            None => (),
            Some(req) => {
                let when_acquired = chrono::Utc::now();
                let new_holder = CurrentLockHolder {
                    transaction: req.transaction.clone(),
                    when_requested: req.when_requested,
                    when_acquired,
                };
                state.current_holder = Some(new_holder);
                req.sender.send(()).unwrap();
            }
        }
    }

    pub async fn is_currently_holding(&self, tx: Arc<TransactionInfo>) -> bool {
        let state = self.state.read().await;
        match &state.current_holder {
            None => false,
            Some(current) => current.transaction.id == tx.id,
        }
    }
}
