use crate::{error::Error, transaction_abort_reason::TransactionAbortReason};
use chrono::DateTime;
use common::transaction_info::TransactionInfo;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;

type UtcDateTime = DateTime<chrono::Utc>;
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

// Implements transaction lock table for the range.
// Currently there is just a single lock for the entire range despite having
// "Table" in the name, but we might partition the lock to allow for more
// concurrency down the line.
pub struct LockTable {
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
                    // let highest_waiter = self
                    //     .waiting_to_acquire
                    //     .back()
                    //     .map_or(current_holder.transaction.id, |r| r.transaction.id);
                    // if highest_waiter > tx.id {
                    //     // TODO: allow for skipping these checks if locks are ordered!
                    //     Err(Error::TransactionAborted(TransactionAbortReason::WaitDie))
                    // } else {
                    let req = LockRequest {
                        transaction: tx.clone(),
                        sender: s,
                        when_requested: chrono::Utc::now(),
                    };
                    self.waiting_to_acquire.push_back(req);
                    Ok(r)
                    // }
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
