use common::full_range_id::{FullRangeId, FullRangeIdAndType};
use common::keyspace_id::KeyspaceId;
use common::range_type::RangeType;
use common::replication_mapping::ReplicationMapping;
use common::{config::Config, host_info::HostInfo};
use proto::warden::loaded_range_status;
use proto::warden::warden_client::WardenClient;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::ops::DerefMut;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Streaming};
use tracing::error;
use tracing::info;
use uuid::Uuid;

use crate::epoch_supplier::EpochSupplier;
use crate::error::Error;
use crate::range_manager::r#impl::RangeManager;
use crate::range_manager::RangeManager as RangeManagerTrait;
use crate::secondary_range_manager::r#impl::SecondaryRangeManager;
use crate::secondary_range_manager::SecondaryRangeManager as SecondaryRangeManagerTrait;
use crate::storage::Storage;
use crate::wal::Wal;

enum State {
    NotStarted,
    Started(Arc<StartedState>),
    Stopped,
}

pub enum WardenUpdate {
    LoadRange(FullRangeIdAndType),
    UnloadRange(FullRangeIdAndType),
    NewReplicationMapping(ReplicationMapping),
    DesiredAppliedEpoch(KeyspaceId, u64),
}

pub struct WardenHandler {
    state: RwLock<State>,
    config: Config,
    host_info: HostInfo,
    epoch_supplier: Arc<dyn EpochSupplier>,
    heartbeat_supplier: Arc<dyn HeartbeatSupplier>,
}

impl WardenHandler {
    pub fn new(
        config: &Config,
        host_info: &HostInfo,
        epoch_supplier: Arc<dyn EpochSupplier>,
        heartbeat_supplier: Arc<dyn HeartbeatSupplier>,
    ) -> WardenHandler {
        WardenHandler {
            state: RwLock::new(State::NotStarted),
            config: config.clone(),
            host_info: host_info.clone(),
            epoch_supplier,
            heartbeat_supplier,
        }
    }

    fn full_range_id_and_type_from_proto(
        proto_range_id_and_type: &proto::warden::RangeIdAndType,
    ) -> FullRangeIdAndType {
        let proto_range_id = proto_range_id_and_type.range.as_ref().unwrap();
        let full_range_id: FullRangeId = proto_range_id.into();
        let range_type = match proto_range_id_and_type.r#type() {
            proto::warden::RangeType::Primary => RangeType::Primary,
            proto::warden::RangeType::Secondary => RangeType::Secondary,
        };
        FullRangeIdAndType {
            full_range_id,
            range_type,
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

                // Replication mappings for our assigned primary ranges.
                // First, add the RMs to the replication mappings.
                for proto_rm in &full_assignment.replication_mapping {
                    let rm: ReplicationMapping = proto_rm.into();
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

                // Desired applied epochs.
                for desired_applied_epoch in &full_assignment.desired_applied_epoch {
                    updates_sender
                        .send(WardenUpdate::DesiredAppliedEpoch(
                            KeyspaceId::from_str(&desired_applied_epoch.keyspace_id).unwrap(),
                            desired_applied_epoch.epoch,
                        ))
                        .unwrap();
                }

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
        heartbeat_supplier: Arc<dyn HeartbeatSupplier>,
    ) -> Result<(), WardenErr> {
        let addr = format!("http://{}", config.warden_address.clone());
        let epoch = epoch_supplier.read_epoch().await?;
        let mut client = WardenClient::connect(addr).await?;
        let registration_request = proto::warden::RangeServerRequest {
            request: Some(proto::warden::range_server_request::Request::Register(
                proto::warden::RegisterRangeServerRequest {
                    range_server: Some(proto::warden::HostInfo {
                        identity: host_info.identity.name.clone(),
                        zone: host_info.identity.zone.name.clone(),
                        epoch,
                    }),
                },
            )),
        };
        // TODO(yanniszark): Ensure that we don't queue up too many messages.
        let (send_tx, send_rx) = mpsc::channel(2);
        send_tx.send(registration_request).await.unwrap();
        let stream = ReceiverStream::new(send_rx);
        let mut stream = client
            .register_range_server(Request::new(stream))
            .await?
            .into_inner();

        // TODO(yanniszark): Make this configurable.
        let mut heartbeat_ticker = tokio::time::interval(tokio::time::Duration::from_secs(1));
        heartbeat_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let heartbeat_sender = send_tx;
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
                _ = heartbeat_ticker.tick() => {
                    let heartbeat = heartbeat_supplier.get_heartbeat().await;
                    info!("Sending heartbeat");
                    // Print the heartbeat statuses.
                    for range_status in heartbeat.range_status.iter() {
                        match &range_status.status {
                            Some(proto::warden::loaded_range_status::Status::Primary(primary_range_status)) => {
                                info!("Primary range status: range_id={:?}", primary_range_status.range_id);
                            }
                            Some(proto::warden::loaded_range_status::Status::Secondary(secondary_range_status)) => {
                                info!("Secondary range status: range_id={:?}, wal_epoch={:?}, applied_epoch={:?}",
                                    secondary_range_status.range_id,
                                    secondary_range_status.wal_epoch,
                                    secondary_range_status.applied_epoch);
                            }
                            None => {
                                info!("Range status: unknown");
                            }
                        }
                    }
                    heartbeat_sender.send(proto::warden::RangeServerRequest {
                        request: Some(proto::warden::range_server_request::Request::Heartbeat(heartbeat)),
                    }).await.unwrap();
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
        heartbeat_supplier: Arc<dyn HeartbeatSupplier>,
    ) -> Result<(), WardenErr> {
        loop {
            match Self::continuously_connect_and_register_inner(
                host_info.clone(),
                config.clone(),
                updates_sender.clone(),
                state.clone(),
                epoch_supplier.clone(),
                heartbeat_supplier.clone(),
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
                    let heartbeat_supplier_clone = self.heartbeat_supplier.clone();
                    let _ = tokio::spawn(async move {
                        let exit_result = Self::continuously_connect_and_register(
                            host_info,
                            config.clone(),
                            updates_sender,
                            started_state,
                            ec_clone,
                            heartbeat_supplier_clone,
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

#[async_trait::async_trait]
pub trait HeartbeatSupplier: Send + Sync + 'static {
    async fn get_heartbeat(&self) -> proto::warden::Heartbeat;
}

pub struct HeartbeatSupplierImpl<S, W>
where
    S: Storage,
    W: Wal,
{
    loaded_ranges: Arc<RwLock<HashMap<Uuid, Arc<RangeManager<S, W>>>>>,
    secondary_loaded_ranges: Arc<RwLock<HashMap<Uuid, Arc<SecondaryRangeManager<S, W>>>>>,
}

impl<S, W> HeartbeatSupplierImpl<S, W>
where
    S: Storage,
    W: Wal,
{
    pub fn new(
        loaded_ranges: Arc<RwLock<HashMap<Uuid, Arc<RangeManager<S, W>>>>>,
        secondary_loaded_ranges: Arc<RwLock<HashMap<Uuid, Arc<SecondaryRangeManager<S, W>>>>>,
    ) -> Self {
        Self {
            loaded_ranges,
            secondary_loaded_ranges,
        }
    }
}

#[async_trait::async_trait]
impl<S, W> HeartbeatSupplier for HeartbeatSupplierImpl<S, W>
where
    S: Storage,
    W: Wal,
{
    async fn get_heartbeat(&self) -> proto::warden::Heartbeat {
        let mut ranges_statuses: Vec<proto::warden::LoadedRangeStatus> = Vec::new();
        {
            let loaded_ranges = self.loaded_ranges.read().await;
            for (range_id, range_manager) in loaded_ranges.deref() {
                // Check if the range is loaded, else continue.
                let status = match range_manager.status().await {
                    Ok(status) => status,
                    Err(Error::RangeIsNotLoaded) => continue,
                    Err(e) => {
                        error!("Error getting status for range {}: {:?}", range_id, e);
                        continue;
                    }
                };
                ranges_statuses.push(proto::warden::LoadedRangeStatus {
                    status: Some(loaded_range_status::Status::Primary(status)),
                });
            }
        }
        {
            let secondary_loaded_ranges = self.secondary_loaded_ranges.read().await;
            for (_, range_manager) in secondary_loaded_ranges.deref() {
                // Check if the range is loaded, else continue.
                let status = match range_manager.status().await {
                    Ok(status) => status,
                    Err(_) => continue,
                };
                ranges_statuses.push(proto::warden::LoadedRangeStatus {
                    status: Some(loaded_range_status::Status::Secondary(status)),
                });
            }
        }
        proto::warden::Heartbeat {
            range_status: ranges_statuses,
        }
    }
}

type WardenErr = Box<dyn std::error::Error + Sync + Send + 'static>;
struct StartedState {
    stopper: CancellationToken,
    assigned_ranges: RwLock<HashMap<FullRangeId, FullRangeIdAndType>>,
    primary_range_id_to_replication_mappings: RwLock<HashMap<FullRangeId, Vec<ReplicationMapping>>>,
}
