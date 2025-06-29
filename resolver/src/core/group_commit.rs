use crate::core::resolver::TransactionInfo;
use common::full_range_id::FullRangeId;
use coordinator_rangeclient::{error::Error, rangeclient::RangeClient};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::info;
use tx_state_store::client::Client as TxStateStoreClient;
use uuid::Uuid;

// NOTE: Every participant range has its own group of transactions ready to commit.
// Each group is protected by a read-write lock. We acquire the write lock only when
// we need to add new transactions to the group or commit the existing ones.
// The entire hashmap is also protected by a read-write lock. We acquire the write lock only when
// we need to add new participant ranges to the hashmap.

#[derive(Default)]
pub struct Stats {
    pub committed_group_sizes: HashMap<String, usize>,
}

struct State {
    // Transactions ready to commit grouped by participant range
    group_per_participant: HashMap<FullRangeId, Arc<RwLock<Vec<TransactionInfo>>>>,
}

pub struct GroupCommit {
    state: Arc<RwLock<State>>,
    // Helps us keep track of the number of participant commits we need to wait for in order to register the transaction as committed
    num_pending_participant_commits_per_transaction: Arc<RwLock<HashMap<Uuid, u32>>>,
    non_empty_groups: Arc<RwLock<HashSet<FullRangeId>>>,
    range_client: Arc<RangeClient>,
    tx_state_store: Arc<TxStateStoreClient>,
    stats: Arc<RwLock<Stats>>,
    returned_transactions: Arc<RwLock<Vec<TransactionInfo>>>,
}

impl GroupCommit {
    pub fn new(range_client: Arc<RangeClient>, tx_state_store: Arc<TxStateStoreClient>) -> Self {
        GroupCommit {
            state: Arc::new(RwLock::new(State {
                group_per_participant: HashMap::new(),
            })),
            num_pending_participant_commits_per_transaction: Arc::new(RwLock::new(HashMap::new())),
            non_empty_groups: Arc::new(RwLock::new(HashSet::new())),
            range_client,
            tx_state_store,
            stats: Arc::new(RwLock::new(Stats::default())),
            returned_transactions: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn get_stats(&self) -> Stats {
        let mut stats = self.stats.write().await;
        let stats = std::mem::take(&mut *stats);
        stats
    }

    async fn maybe_insert_new_participant_ranges(
        &self,
        participant_ranges: Vec<FullRangeId>,
    ) -> Result<(), Error> {
        // Double-checked pattern: First read to avoid unnecessary write lock;
        // then write only if any participants are missing.
        let mut participants_to_insert = Vec::new();
        {
            let state = self.state.read().await;
            let group_per_participant = &state.group_per_participant;
            for participant_range in participant_ranges {
                if !group_per_participant.contains_key(&participant_range) {
                    participants_to_insert.push(participant_range);
                }
            }
        }
        // Acquire the write lock only if necessary
        if !participants_to_insert.is_empty() {
            let mut state = self.state.write().await;
            for participant_range in participants_to_insert {
                state
                    .group_per_participant
                    .entry(participant_range)
                    .or_insert_with(|| Arc::new(RwLock::new(Vec::new())));
            }
        }
        Ok(())
    }

    pub async fn add_transactions(&self, transactions: &Vec<TransactionInfo>) -> Result<(), Error> {
        // Group transactions by participant range
        info!("Grouping transactions by participant range");
        let mut tmp_group_per_participant = HashMap::new();
        let mut tmp_num_pending_commits = HashMap::new();
        for transaction in transactions {
            let mut num_pending_commits = 0;
            for participant_range in transaction.participant_ranges_info.iter() {
                // If a transaction has no writes in a participant range, it is not added to that participant range's group
                if participant_range.has_writes {
                    num_pending_commits += 1;
                    tmp_group_per_participant
                        .entry(participant_range.participant_range)
                        .or_insert_with(|| Vec::new())
                        .push(transaction.clone());
                }
            }
            tmp_num_pending_commits.insert(transaction.id, num_pending_commits);
        }

        let _ = self
            .maybe_insert_new_participant_ranges(
                tmp_group_per_participant.keys().cloned().collect(),
            )
            .await;
        info!("Inserted new participant ranges");

        info!(
            "Updating number of participant commits we need to wait for in order to register the transaction as committed"
        );
        {
            // Update the number of participant commits we need to wait for in order to register the transaction as committed
            let mut num_pending_participant_commits_per_transaction = self
                .num_pending_participant_commits_per_transaction
                .write()
                .await;
            for (transaction_id, num_pending_commits) in tmp_num_pending_commits.iter() {
                num_pending_participant_commits_per_transaction
                    .insert(*transaction_id, *num_pending_commits);
            }
        }

        {
            let mut join_set = JoinSet::<()>::new();
            for (participant_range, transactions) in tmp_group_per_participant.iter() {
                let state_clone = self.state.clone();
                let participant_range = participant_range.clone();
                let transactions = transactions.clone();
                // Spawning async tasks so that we try to acquire the locks for all groups in parallel
                join_set.spawn(async move {
                    let state = state_clone.read().await;
                    let mut group = state
                        .group_per_participant
                        .get(&participant_range)
                        .unwrap()
                        .write()
                        .await;
                    group.extend(transactions.iter().cloned());
                });
            }
            while let Some(_) = join_set.join_next().await {}
        }

        {
            // Keep track of non-empty groups so that we know fast which groups to commit
            let mut non_empty_groups = self.non_empty_groups.write().await;
            for (participant_range, _) in tmp_group_per_participant.iter() {
                non_empty_groups.insert(participant_range.clone());
            }
        }

        Ok(())
    }

    pub async fn commit(&self) -> Result<Vec<TransactionInfo>, Error> {
        let mut commit_join_set = {
            let mut non_empty_groups = HashSet::new();
            {
                let non_empty_groups_read = self.non_empty_groups.read().await;
                for participant_range in non_empty_groups_read.iter() {
                    non_empty_groups.insert(participant_range.clone());
                }
            }

            info!("Committing groups in parallel");
            let mut commit_join_set = JoinSet::<Result<(), Error>>::new();
            let state = self.state.read().await;
            for participant_range in non_empty_groups.iter() {
                let group_guard = state.group_per_participant.get(participant_range).unwrap();

                let range_client = self.range_client.clone();
                let participant_range_clone = participant_range.clone();
                let tx_state_store_clone = self.tx_state_store.clone();
                let group_guard_clone = group_guard.clone();
                let non_empty_groups_clone = self.non_empty_groups.clone();
                let stats_clone = self.stats.clone();
                let returned_transactions_clone = self.returned_transactions.clone();

                commit_join_set.spawn(async move {
                    // Acquire the write lock to clear the group
                    let mut group_clone = group_guard_clone.write().await;
                    let transactions = std::mem::take(&mut *group_clone);
                    {
                        let mut non_empty_groups = non_empty_groups_clone.write().await;
                        non_empty_groups.remove(&participant_range_clone);
                    }
                    drop(group_clone);

                    if transactions.is_empty() {
                        return Ok(());
                    }

                    let fake = transactions.first().map(|tx| tx.fake).unwrap();
                    if !fake {
                        // TODO: Handle cascading aborts
                        // TODO: Does order of tx_ids matter in the tx_state_store?
                        // NOTE: A transaction will be recorded as committed in the tx_state_store once per every participant range it is part of.
                        // Log all transactions of the group that are ready to commit as committed in the tx_state_store
                        let tx_ids_vec = transactions.iter().map(|tx| tx.id).collect();
                        tx_state_store_clone
                            .try_batch_commit_transactions(&tx_ids_vec, 0)
                            .await
                            .unwrap();

                        // Notify participant so that:
                        // - it applies the TXs' PrepareRecords in storage
                        // - and then quickly updates its PendingCommitTable to stop treating these transactions as dependencies for other transactions
                        // info!("Committing transactions {:?} in range {:?}", participant_range_clone, participant_range_clone);
                        if let Err(e) = range_client
                            .commit_transactions(tx_ids_vec, &participant_range_clone, 0)
                            .await
                        {
                            panic!(
                                "Error committing transactions {:?}: {:?}",
                                participant_range_clone, e
                            );
                        }
                        {
                            let mut stats = stats_clone.write().await;
                            stats
                                .committed_group_sizes
                                .entry(format!("Group size: {}", transactions.len()))
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                        }
                    }
                    {
                        let mut returned_transactions = returned_transactions_clone.write().await;
                        returned_transactions.extend(transactions);
                    }
                    Ok(())
                });
            }
            commit_join_set
        };

        while let Some(res) = commit_join_set.join_next().await {}

        let returned_transactions = std::mem::take(&mut *self.returned_transactions.write().await);
        info!("Returned transactions: {:?}", returned_transactions);

        // A cycle of group commits has just finished. Check which transactions have finished committing and send them back to the resolver.
        let mut finished_transactions = Vec::new();
        {
            let mut num_pending_participant_commits_per_transaction = self
                .num_pending_participant_commits_per_transaction
                .write()
                .await;
            for transaction in returned_transactions {
                assert!(
                    num_pending_participant_commits_per_transaction.contains_key(&transaction.id)
                );
                *num_pending_participant_commits_per_transaction
                    .get_mut(&transaction.id)
                    .unwrap() -= 1;
                if *num_pending_participant_commits_per_transaction
                    .get_mut(&transaction.id)
                    .unwrap()
                    == 0
                {
                    num_pending_participant_commits_per_transaction.remove(&transaction.id);
                    finished_transactions.push(transaction);
                }
            }
        }
        info!("Finished transactions: {:?}", finished_transactions);

        Ok(finished_transactions)
    }
}
