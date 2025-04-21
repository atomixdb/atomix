use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use common::full_range_id::{FullRangeId, FullRangeIdAndType};
use common::keyspace_id::KeyspaceId;
use common::range_type::RangeType;
use common::replication_mapping::ReplicationMapping;
use common::{config::Config, host_info::HostInfo};
use proto::warden::warden_client::WardenClient;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use uuid::Uuid;

use crate::epoch_supplier::EpochSupplier;

type WardenErr = Box<dyn std::error::Error + Sync + Send + 'static>;
struct StartedState {
    stopper: CancellationToken,
    assigned_ranges: RwLock<HashMap<FullRangeId, FullRangeIdAndType>>,
    primary_range_id_to_replication_mappings: RwLock<HashMap<FullRangeId, Vec<ReplicationMapping>>>,
}

enum State {
    NotStarted,
    Started(Arc<StartedState>),
    Stopped,
}

pub enum WardenUpdate {
    LoadRange(FullRangeIdAndType),
    UnloadRange(FullRangeIdAndType),
    NewReplicationMapping(ReplicationMapping),
}

pub struct WardenHandler {
    state: RwLock<State>,
    config: Config,
    host_info: HostInfo,
    epoch_supplier: Arc<dyn EpochSupplier>,
}

impl WardenHandler {
    pub fn new(
        config: &Config,
        host_info: &HostInfo,
        epoch_supplier: Arc<dyn EpochSupplier>,
    ) -> WardenHandler {
        WardenHandler {
            state: RwLock::new(State::NotStarted),
            config: config.clone(),
            host_info: host_info.clone(),
            epoch_supplier,
        }
    }

    fn full_range_id_from_proto(proto_range_id: &proto::warden::RangeId) -> FullRangeId {
        let keyspace_id = Uuid::from_str(proto_range_id.keyspace_id.as_str()).unwrap();
        let keyspace_id = KeyspaceId::new(keyspace_id);
        let range_id = Uuid::from_str(proto_range_id.range_id.as_str()).unwrap();
        FullRangeId {
            keyspace_id,
            range_id,
        }
    }

    fn full_range_id_and_type_from_proto(
        proto_range_id_and_type: &proto::warden::RangeIdAndType,
    ) -> FullRangeIdAndType {
        let proto_range_id = proto_range_id_and_type.range.as_ref().unwrap();
        let full_range_id = Self::full_range_id_from_proto(proto_range_id);
        let range_type = match proto_range_id_and_type.r#type() {
            proto::warden::RangeType::Primary => RangeType::Primary,
            proto::warden::RangeType::Secondary => RangeType::Secondary,
        };
        FullRangeIdAndType {
            full_range_id,
            range_type,
        }
    }

    fn replication_mapping_from_proto(
        proto_replication_mapping: &proto::warden::ReplicationMapping,
    ) -> ReplicationMapping {
        let primary_range = proto_replication_mapping.primary_range.as_ref().unwrap();
        let secondary_range = proto_replication_mapping.secondary_range.as_ref().unwrap();
        ReplicationMapping {
            primary_range: Self::full_range_id_from_proto(primary_range),
            secondary_range: Self::full_range_id_from_proto(secondary_range),
            assignee: proto_replication_mapping.assignee.clone(),
        }
    }

    fn process_warden_update(
        update: &proto::warden::WardenUpdate,
        updates_sender: &mpsc::UnboundedSender<WardenUpdate>,
        assigned_ranges: &mut HashMap<FullRangeId, FullRangeIdAndType>,
        primary_id_to_replication_mappings: &mut HashMap<FullRangeId, Vec<ReplicationMapping>>,
    ) {
        let update = update.update.as_ref().unwrap();
        match update {
            proto::warden::warden_update::Update::FullAssignment(full_assignment) => {
                let new_assignment: HashMap<FullRangeId, FullRangeIdAndType> = full_assignment
                    .range
                    .iter()
                    .map(|r| {
                        let range_info = Self::full_range_id_and_type_from_proto(r);
                        (range_info.full_range_id, range_info)
                    })
                    .collect();

                // Unload any ranges that are no longer assigned to us.
                for (range_id, range_info) in assigned_ranges.iter() {
                    if !new_assignment.contains_key(range_id) {
                        updates_sender
                            .send(WardenUpdate::UnloadRange(*range_info))
                            .unwrap();
                    }
                }

                // Load any ranges that got newly assigned to us.
                for (assigned_range_id, assigned_range_info) in &new_assignment {
                    if !assigned_ranges.contains_key(assigned_range_id) {
                        updates_sender
                            .send(WardenUpdate::LoadRange(*assigned_range_info))
                            .unwrap();
                    }
                }

                // TODO: Replication mappings for our assigned primary ranges.
                // First, add the RMs to the replication mappings.
                for proto_rm in &full_assignment.replication_mapping {
                    let rm = Self::replication_mapping_from_proto(proto_rm);
                    // Does this mapping already exist?
                    let rm_exists = primary_id_to_replication_mappings
                        .contains_key(&rm.primary_range)
                        && primary_id_to_replication_mappings
                            .get(&rm.primary_range)
                            .unwrap()
                            .iter()
                            .any(|existing_rm| *existing_rm == rm);
                    if rm_exists {
                        continue;
                    }
                    // Otherwise, add the mapping.
                    primary_id_to_replication_mappings
                        .entry(rm.primary_range)
                        .or_insert_with(Vec::new)
                        .push(rm.clone());

                    updates_sender
                        .send(WardenUpdate::NewReplicationMapping(rm))
                        .unwrap();
                }

                // TODO(yanniszark): Find removed replication mappings

                assigned_ranges.clear();
                assigned_ranges.clone_from(&new_assignment);
            }
            proto::warden::warden_update::Update::IncrementalAssignment(incremental) => {
                for range_id in &incremental.load {
                    let assigned_range = Self::full_range_id_and_type_from_proto(range_id);
                    if !assigned_ranges.contains_key(&assigned_range.full_range_id) {
                        updates_sender
                            .send(WardenUpdate::LoadRange(assigned_range))
                            .unwrap();
                    }
                    assigned_ranges.insert(assigned_range.full_range_id, assigned_range);
                }

                for range_id in &incremental.unload {
                    let removed_range = Self::full_range_id_and_type_from_proto(range_id);
                    if assigned_ranges.contains_key(&removed_range.full_range_id) {
                        updates_sender
                            .send(WardenUpdate::UnloadRange(removed_range))
                            .unwrap();
                    }
                    assigned_ranges.remove(&removed_range.full_range_id);
                }
            }
        }
    }

    async fn continuously_connect_and_register_inner(
        host_info: HostInfo,
        config: common::config::RegionConfig,
        updates_sender: mpsc::UnboundedSender<WardenUpdate>,
        state: Arc<StartedState>,
        epoch_supplier: Arc<dyn EpochSupplier>,
    ) -> Result<(), WardenErr> {
        let addr = format!("http://{}", config.warden_address.clone());
        let epoch = epoch_supplier.read_epoch().await?;
        let mut client = WardenClient::connect(addr).await?;
        let registration_request = proto::warden::RegisterRangeServerRequest {
            range_server: Some(proto::warden::HostInfo {
                identity: host_info.identity.name.clone(),
                zone: host_info.identity.zone.name.clone(),
                epoch,
            }),
        };
        let mut stream = client
            .register_range_server(Request::new(registration_request))
            .await?
            .into_inner();

        loop {
            tokio::select! {
                () = state.stopper.cancelled() => return Ok(()),
                maybe_update = stream.message() => {
                    let maybe_update = maybe_update?;
                    match maybe_update {
                        None => {
                            return Err("connection closed with warden!".into())
                        }
                        Some(update) => {
                            let mut assigned_ranges_lock = state.assigned_ranges.write().await;
                            let mut primary_id_to_replication_mappings_lock =
                                state.primary_range_id_to_replication_mappings.write().await;
                            Self::process_warden_update(
                                &update, &updates_sender,
                                assigned_ranges_lock.deref_mut(),
                                primary_id_to_replication_mappings_lock.deref_mut());
                        }
                    }
                }

            }
        }
    }

    async fn continuously_connect_and_register(
        host_info: HostInfo,
        config: common::config::RegionConfig,
        updates_sender: mpsc::UnboundedSender<WardenUpdate>,
        state: Arc<StartedState>,
        epoch_supplier: Arc<dyn EpochSupplier>,
    ) -> Result<(), WardenErr> {
        loop {
            match Self::continuously_connect_and_register_inner(
                host_info.clone(),
                config.clone(),
                updates_sender.clone(),
                state.clone(),
                epoch_supplier.clone(),
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    println!("Warden Loop Error: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    // If this starts correctly, returns a channel receiver that can be used to determine when
    // the warden_handler has stopped. Otherwise returns an error.
    pub async fn start(
        &self,
        updates_sender: mpsc::UnboundedSender<WardenUpdate>,
    ) -> Result<oneshot::Receiver<Result<(), WardenErr>>, WardenErr> {
        let mut state = self.state.write().await;
        if let State::NotStarted = state.deref_mut() {
            match self
                .config
                .regions
                .get(&self.host_info.identity.zone.region)
            {
                None => Err("unknown region!".into()),
                Some(config) => {
                    let (done_tx, done_rx) = oneshot::channel::<Result<(), WardenErr>>();
                    let host_info = self.host_info.clone();
                    let config = config.clone();
                    let stop = CancellationToken::new();
                    let started_state = Arc::new(StartedState {
                        stopper: stop,
                        assigned_ranges: RwLock::new(HashMap::new()),
                        primary_range_id_to_replication_mappings: RwLock::new(HashMap::new()),
                    });
                    *state = State::Started(started_state.clone());
                    drop(state);
                    let ec_clone = self.epoch_supplier.clone();
                    let _ = tokio::spawn(async move {
                        let exit_result = Self::continuously_connect_and_register(
                            host_info,
                            config.clone(),
                            updates_sender,
                            started_state,
                            ec_clone,
                        )
                        .await;
                        done_tx.send(exit_result).unwrap();
                    });
                    Ok(done_rx)
                }
            }
        } else {
            Err("warden_handler can only be started once".into())
        }
    }

    pub async fn stop(&self) {
        let mut state = self.state.write().await;
        let old_state = std::mem::replace(&mut *state, State::Stopped);
        if let State::Started(s) = old_state {
            s.stopper.cancel();
        }
    }

    pub async fn is_assigned(&self, range_id: &FullRangeId) -> bool {
        let state = self.state.read().await;
        match state.deref() {
            State::NotStarted => false,
            State::Stopped => false,
            State::Started(state) => {
                let assigned_ranges = state.assigned_ranges.read().await;
                (*assigned_ranges).contains_key(range_id)
            }
        }
    }
}
