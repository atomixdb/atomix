use std::collections::{BinaryHeap, HashMap};
use std::ops::Add;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashSet, ops::Deref, sync::Mutex};

use bytes::Bytes;
use common::host_info::HostInfo;
use common::key_range::KeyRange;
use common::range_type::RangeType;
use common::region::Region;
use proto::universe::universe_client::UniverseClient;
use proto::universe::ListKeyspacesRequest;
use proto::warden::{FullAssignment, WardenUpdate};
use std::cmp::{Ordering, Reverse};
use std::hash::{Hash, Hasher};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::persistence::{Persistence, RangeAssignment, RangeInfo};

// TODO(purujit): Convert these to configuration.
const MIN_NUM_RANGE_SERVERS: usize = 1;
const MAX_VERSIONS_TO_KEEP: usize = 5;

/// We need to implement Eq, Ord and Hash for Range Server identities in
/// HostInfo.  Since that type is in another crate and may have a different
/// implementation of these traits, we need to wrap it. This is utilizing the
/// `NewType` pattern from chapter 19 of the Rust book.
#[derive(Clone, Debug)]
struct HostInfoWrapper(HostInfo);
impl Deref for HostInfoWrapper {
    type Target = HostInfo;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Eq for HostInfoWrapper {}

impl PartialEq for HostInfoWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl PartialOrd for HostInfoWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HostInfoWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identity.name.cmp(&other.identity.name)
    }
}

impl Hash for HostInfoWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}

pub trait AssignmentComputation {
    fn register_range_server(&self, host_info: HostInfo) -> Result<Receiver<i64>, Status>;
    fn notify_range_server_unavailable(&self, host_info: HostInfo);
    fn get_assignment_update(
        &self,
        host_info: &HostInfo,
        version: i64,
        full_update: bool,
    ) -> Option<WardenUpdate>;
}

pub struct AssignmentComputationImpl {
    base_ranges: Mutex<Vec<RangeInfo>>,
    universe_client: UniverseClient<tonic::transport::Channel>,
    region: Region,
    range_assignments: Mutex<HashMap<i64, Vec<RangeAssignment>>>,
    current_version: Mutex<i64>,
    ready_range_servers: Mutex<HashSet<HostInfoWrapper>>,
    unassigned_base_ranges: Mutex<Vec<RangeInfo>>,
    assignment_update_sender: Sender<i64>,
    persistence: Arc<dyn Persistence + Send + Sync + 'static>,
}

impl AssignmentComputationImpl {
    pub async fn new(
        universe_client: UniverseClient<tonic::transport::Channel>,
        region: Region,
        persistence: Arc<dyn Persistence + Send + Sync + 'static>,
    ) -> Arc<Self> {
        let s = Arc::new(Self {
            base_ranges: Mutex::new(vec![]),
            universe_client,
            region,
            range_assignments: Mutex::new(HashMap::new()),
            // TODO(purujit): Use a better sequence generator for version.
            current_version: Mutex::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            ),
            ready_range_servers: Mutex::new(HashSet::new()),
            unassigned_base_ranges: Mutex::new(vec![]),
            // Using capacity 1 here because receivers will resync if they lag.
            assignment_update_sender: channel(1).0,
            persistence,
        });
        s
    }

    pub fn start_computation(
        self: Arc<Self>,
        runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
    ) {
        runtime.spawn(async move {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Cancellation token triggered. Exiting assignment computation loop.")
                }
                _ = Self::assignment_computation_loop(self.clone()) => {
                    info!("Assignment computation loop exited.")
                }
            }
        });
    }

    pub async fn read_base_ranges(&self) -> Result<(), tonic::Status> {
        let request = tonic::Request::new(ListKeyspacesRequest {
            region: Some(self.region.clone().into()),
        });
        // We rely on the default duration and that seems to be a few seconds in practice.
        // We need to decide a project-wide sane default and apply everywhere.
        // request.set_timeout(Duration::from_millis(10));
        let mut client = self.universe_client.clone();
        let response = client.list_keyspaces(request).await?;
        let key_spaces = response.into_inner().keyspaces;

        let mut base_ranges = Vec::new();
        for keyspace in key_spaces.iter() {
            for range in keyspace.base_key_ranges.iter() {
                base_ranges.push(RangeInfo {
                    keyspace_id: common::keyspace_id::KeyspaceId {
                        id: Uuid::parse_str(&keyspace.keyspace_id)
                            .map_err(|e| tonic::Status::internal(e.to_string()))?,
                    },
                    id: Uuid::parse_str(&range.base_range_uuid)
                        .map_err(|e| tonic::Status::internal(e.to_string()))?,
                    key_range: KeyRange {
                        lower_bound_inclusive: if range.lower_bound_inclusive.is_empty() {
                            None
                        } else {
                            Some(Bytes::from(range.lower_bound_inclusive.clone()))
                        },
                        upper_bound_exclusive: if range.upper_bound_exclusive.is_empty() {
                            None
                        } else {
                            Some(Bytes::from(range.upper_bound_exclusive.clone()))
                        },
                    },
                    range_type: RangeType::Primary,
                });
            }
        }
        // Create secondary ranges for keyspaces with secondary zones in this region
        let secondary_base_ranges: Vec<RangeInfo> = key_spaces
            .iter()
            .flat_map(|k| {
                k.secondary_key_ranges.iter().map(|zkr| {
                    let range = zkr.key_range.as_ref().unwrap();
                    RangeInfo {
                        keyspace_id: common::keyspace_id::KeyspaceId {
                            id: Uuid::parse_str(&k.keyspace_id).unwrap(),
                        },
                        id: Uuid::parse_str(&range.base_range_uuid).unwrap(),
                        key_range: KeyRange {
                            lower_bound_inclusive: if range.lower_bound_inclusive.is_empty() {
                                None
                            } else {
                                Some(Bytes::from(range.lower_bound_inclusive.clone()))
                            },
                            upper_bound_exclusive: if range.upper_bound_exclusive.is_empty() {
                                None
                            } else {
                                Some(Bytes::from(range.upper_bound_exclusive.clone()))
                            },
                        },
                        range_type: RangeType::Secondary,
                    }
                })
            })
            .collect();
        base_ranges.extend(secondary_base_ranges);

        let mut new_base_ranges = vec![];
        {
            let mut l = self.base_ranges.lock().unwrap();
            let current_base_ranges: &mut Vec<RangeInfo> = l.as_mut();
            // Create a set of UUIDs from self.base_ranges.
            let base_ranges_set: HashSet<_> = current_base_ranges.iter().map(|r| r.id).collect();
            let reported_base_ranges_set: HashSet<_> = base_ranges.iter().map(|r| r.id).collect();
            for range in current_base_ranges {
                if !reported_base_ranges_set.contains(&range.id) {
                    return Err(tonic::Status::internal(format!(
                        "Base range {:?} not found in reported base ranges",
                        range
                    )));
                }
            }
            for range in base_ranges.iter() {
                if !base_ranges_set.contains(&range.id) {
                    new_base_ranges.push(range.clone());
                }
            }
        }

        self.persistence
            .insert_new_ranges(&new_base_ranges)
            .await
            .map_err(|e| tonic::Status::internal(format!("Failed to insert new ranges: {}", e)))?;
        self.base_ranges
            .lock()
            .unwrap()
            .extend(new_base_ranges.clone());
        self.unassigned_base_ranges
            .lock()
            .unwrap()
            .extend(new_base_ranges);
        Ok(())
    }

    async fn assignment_computation_loop(self: Arc<Self>) -> () {
        let mut ready_servers = HashSet::new();
        loop {
            let clone = self.clone();
            // It is not a fatal error to fail to read base ranges. We will just retry.
            let _ = self.read_base_ranges().await.map_err(|e| {
                error!("Failed to read base ranges: {}", e);
            });
            ready_servers = clone.run_assignment_computation(ready_servers).await;
        }
    }

    /// Runs the assignment computation loop, which is responsible for managing the assignment of key ranges to range servers.
    ///
    /// This function is called periodically to update the assignment of key ranges to range servers. It performs the following steps:
    ///
    /// 1. Checks if the number of ready range servers is at least the minimum required. If not, it waits for 1 second.
    /// 2. Computes the set of added and removed servers since the last run.
    /// 3. If there are no changes in the set of ready servers or unassigned base ranges, it waits for 1 second.
    /// 4. Constructs a map of assignee (range server) to the list of ranges assigned to that server.
    /// 5. Builds a min-heap of servers, where the priority is the number of ranges assigned to the server.
    /// 6. Reassigns any ranges from removed servers to the current servers, starting from the least loaded.
    /// 7. Assigns any newly added base ranges to the current servers, starting from the least loaded.
    /// 8. Updates the `range_assignments` vector with the new assignments.
    ///
    /// The function returns the current set of ready range servers.
    async fn run_assignment_computation(
        self: Arc<Self>,
        prev_ready_servers: HashSet<HostInfoWrapper>,
    ) -> HashSet<HostInfoWrapper> {
        let new_ready_servers = self.ready_range_servers.lock().unwrap().clone();
        let num_range_servers = new_ready_servers.len();
        if num_range_servers < MIN_NUM_RANGE_SERVERS {
            info!(
                "Need {} range servers at least, got {}. Will wait.",
                MIN_NUM_RANGE_SERVERS, num_range_servers
            );
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            return new_ready_servers;
        }

        let added_servers: Vec<_> = new_ready_servers.difference(&prev_ready_servers).collect();
        let removed_servers: Vec<_> = prev_ready_servers.difference(&new_ready_servers).collect();
        if added_servers.len() == 0
            && removed_servers.len() == 0
            && self.unassigned_base_ranges.lock().unwrap().len() == 0
        {
            debug!("No changes in the set of ready range servers or unassigned base ranges. Will wait.");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            return new_ready_servers;
        }
        debug!(
            "Added servers: {:?}, removed servers: {:?}.",
            added_servers, removed_servers
        );

        let mut assignee_to_range_info = HashMap::new();
        if self.range_assignments.lock().unwrap().len() > 0 {
            for assignment in self
                .range_assignments
                .lock()
                .unwrap()
                .get(&self.current_version.lock().unwrap())
                .unwrap()
                .iter()
            {
                assignee_to_range_info
                    .entry(assignment.assignee.clone())
                    .or_insert_with(Vec::new)
                    .push(assignment.range.clone());
            }
        }
        let mut server_heap = BinaryHeap::new();
        for (server, ranges) in assignee_to_range_info.iter() {
            server_heap.push(Reverse((ranges.len(), server.clone())));
        }
        for added_server in added_servers {
            server_heap.push(Reverse((0, added_server.identity.name.clone())));
        }
        if server_heap.is_empty() {
            for server in &new_ready_servers {
                server_heap.push(Reverse((0, server.identity.name.clone())));
            }
        }
        // TODO(purujit): Put the removed servers in quarantine for a few seconds, they might come back.
        // Reassign the ranges from the removed servers to the current servers starting from the least loaded.
        // Also, assign any newly added base ranges.
        let mut ranges_to_assign: Vec<RangeInfo>;
        // For now, versions are milliseconds since Unix epoch and adding 1 is safe since even if another Warden instance
        // comes up, it is unlikely to have any overlap with the previous one unless there is a lot of clock skew.
        // TODO(purujit): Use a more robust versioning scheme.
        let new_version = self.current_version.lock().unwrap().add(1);
        {
            let mut updated_assignments = vec![];
            ranges_to_assign = self.unassigned_base_ranges.lock().unwrap().clone();

            for removed_server in removed_servers.clone() {
                if let Some(ranges) = assignee_to_range_info.remove(&removed_server.identity.name) {
                    ranges_to_assign.extend(ranges);
                }
            }
            debug!("Ranges to assign: {:?}.", ranges_to_assign);

            let mut assigned_ranges = HashSet::new();
            for range in ranges_to_assign.into_iter() {
                let next_server = server_heap.pop().unwrap();
                debug!("Assigning range {:?} to server {:?}.", range, next_server);
                assigned_ranges.insert(range.id);
                assignee_to_range_info
                    .entry(next_server.0 .1.clone())
                    .or_insert_with(Vec::new)
                    .push(range.clone());
                server_heap.push(Reverse((next_server.0 .0 + 1, next_server.0 .1.clone())));
                updated_assignments.push(RangeAssignment {
                    assignee: next_server.0 .1.clone(),
                    range,
                });
            }
            if let Err(e) = self
                .persistence
                .update_range_assignments(new_version, updated_assignments)
                .await
            {
                print!("Failed to update range assignments: {:?}.", e);
                return new_ready_servers;
            }
            let mut previously_unassigned = self.unassigned_base_ranges.lock().unwrap();
            // Remove only the ranges that were assigned.
            // This is a safe-guard to make sure if we fail to assign any range, it still stays in the unassigned list.
            previously_unassigned.retain(|r| !assigned_ranges.contains(&r.id));
        }
        {
            let mut range_assignments = self.range_assignments.lock().unwrap();
            let mut current_version = self.current_version.lock().unwrap();
            if range_assignments.len() > MAX_VERSIONS_TO_KEEP {
                let oldest_version = range_assignments.keys().min().unwrap().clone();
                range_assignments.remove(&oldest_version);
            }

            assert!(!range_assignments.contains_key(&new_version));
            range_assignments.insert(new_version, Vec::new());
            let new_range_assignments = range_assignments.get_mut(&new_version).unwrap();
            for (assignee, ranges) in assignee_to_range_info {
                for range in ranges {
                    new_range_assignments.push(RangeAssignment {
                        assignee: assignee.clone(),
                        range,
                    });
                }
            }
            *current_version = new_version;
            debug!(
                "Range assignments at version {:?}: {:?}.",
                new_version, new_range_assignments
            );
            let send_result = self.assignment_update_sender.send(new_version);
            match send_result {
                Ok(num_receivers) => {
                    debug!("Sent assignment update to {} receivers.", num_receivers);
                }
                Err(e) => {
                    // This can happen if all receivers have been dropped. Future sends could succeed.
                    error!("Failed to send assignment update: {:?}.", e);
                }
            }
        }
        new_ready_servers
    }
}

impl AssignmentComputation for AssignmentComputationImpl {
    fn register_range_server(&self, host_info: HostInfo) -> Result<Receiver<i64>, Status> {
        info!("Registering range server: {:?}.", host_info);
        // Note that if this is a re-registration, the old receiver will
        // eventually be dropped when the gRPC stream for the old connection is
        // closed. gRPC will detect the disconnect when it tries to send an
        // update on the old connection.
        let mut ready_servers = self.ready_range_servers.lock().unwrap();
        if let Some(existing) = ready_servers.get(&HostInfoWrapper(host_info.clone())) {
            if existing.0.warden_connection_epoch >= host_info.warden_connection_epoch {
                // Reject because we need increasing epoch for reconnects.
                error!("Rejecting re-registration of range server {:?}.", host_info);
                return Err(Status::already_exists(
                    "Connection with a newer epoch already exists",
                ));
            }
        }
        ready_servers.replace(HostInfoWrapper(host_info.clone()));
        Ok(self.assignment_update_sender.subscribe())
    }

    fn get_assignment_update(
        &self,
        host_info: &HostInfo,
        version: i64,
        full_update: bool,
    ) -> Option<WardenUpdate> {
        if !full_update {
            // TODO(purujit): Implement incremental updates.
            warn!("Incremental update not implemented yet.");
        }
        match self.range_assignments.lock().unwrap().get(&version) {
            Some(range_assignments) => {
                let assigned_ranges: Vec<_> = range_assignments
                    .iter()
                    .filter(|r| r.assignee == host_info.identity.name)
                    .map(|r| proto::warden::RangeId {
                        keyspace_id: r.range.keyspace_id.id.to_string(),
                        range_id: r.range.id.to_string(),
                    })
                    .collect();
                let update = WardenUpdate {
                    update: Some(proto::warden::warden_update::Update::FullAssignment(
                        FullAssignment {
                            version: self.current_version.lock().unwrap().clone(),
                            range: assigned_ranges,
                        },
                    )),
                };
                Some(update)
            }
            None => {
                error!(
                    "No assignments cached for for version {:?}. Current version: {:?}",
                    version,
                    self.current_version.lock().unwrap()
                );
                return None;
            }
        }
    }

    fn notify_range_server_unavailable(&self, host_info: HostInfo) {
        // TODO(purujit): Implement Quarantine.
        debug!("Notifying range server {:?} is unavailable.", host_info);

        let mut ready_servers = self.ready_range_servers.lock().unwrap();
        if let Some(existing) = ready_servers.get(&HostInfoWrapper(host_info.clone())) {
            // Disconnect could come after a new connection is established.
            // We should not drop the new connection in that case.
            if existing.0.warden_connection_epoch <= host_info.warden_connection_epoch {
                ready_servers.remove(&HostInfoWrapper(host_info));
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use common::{
        host_info::HostIdentity,
        key_range::KeyRange,
        keyspace_id::KeyspaceId,
        region::{Region, Zone},
    };
    use once_cell::sync::Lazy;
    use proto::universe::{
        universe_server::{Universe, UniverseServer},
        CreateKeyspaceRequest, CreateKeyspaceResponse, GetKeyspaceInfoRequest,
        GetKeyspaceInfoResponse, KeyspaceInfo, ListKeyspacesResponse,
    };
    use scylla::{Session, SessionBuilder};
    use tokio::sync::oneshot;
    use tonic::{Code, Request, Response};
    use uuid::Uuid;

    use crate::persistence::cassandra::Cassandra;

    use super::*;
    use std::sync::Arc;
    fn make_zone() -> Zone {
        Zone {
            region: Region {
                cloud: None,
                name: "test".to_string(),
            },
            name: "test_zone".to_string(),
        }
    }

    fn make_range(start: u8, end: u8) -> RangeInfo {
        RangeInfo {
            keyspace_id: KeyspaceId { id: Uuid::new_v4() },
            key_range: KeyRange {
                lower_bound_inclusive: Some(Bytes::from(vec![start])),
                upper_bound_exclusive: Some(Bytes::from(vec![end])),
            },
            id: Uuid::new_v4(),
            range_type: RangeType::Primary,
        }
    }

    struct MockUniverseService {
        base_ranges: Arc<Mutex<Vec<RangeInfo>>>,
    }

    #[tonic::async_trait]
    impl Universe for MockUniverseService {
        async fn create_keyspace(
            &self,
            _: Request<CreateKeyspaceRequest>,
        ) -> Result<Response<CreateKeyspaceResponse>, Status> {
            Ok(Response::new(CreateKeyspaceResponse {
                keyspace_id: Uuid::new_v4().to_string(),
            }))
        }

        async fn list_keyspaces(
            &self,
            _request: Request<ListKeyspacesRequest>,
        ) -> Result<Response<ListKeyspacesResponse>, Status> {
            if (self.base_ranges.lock().unwrap()).len() == 0 {
                return Ok(Response::new(ListKeyspacesResponse { keyspaces: vec![] }));
            }
            let keyspace_id = self
                .base_ranges
                .lock()
                .unwrap()
                .get(0)
                .unwrap()
                .keyspace_id
                .id
                .to_string();
            Ok(Response::new(ListKeyspacesResponse {
                keyspaces: vec![KeyspaceInfo {
                    keyspace_id,
                    namespace: "test_namespace".to_string(),
                    name: "test_keyspace".to_string(),
                    primary_zone: Some(proto::universe::Zone {
                        region: Some(proto::universe::Region {
                            cloud: None,
                            name: "test".to_string(),
                        }),
                        name: "test_zone".to_string(),
                    }),
                    secondary_zones: vec![],
                    base_key_ranges: self
                        .base_ranges
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|range| proto::universe::KeyRange {
                            lower_bound_inclusive: range
                                .clone()
                                .key_range
                                .lower_bound_inclusive
                                .unwrap()
                                .into(),
                            upper_bound_exclusive: range
                                .clone()
                                .key_range
                                .upper_bound_exclusive
                                .unwrap()
                                .into(),
                            base_range_uuid: range.id.to_string(),
                        })
                        .collect(),
                    secondary_key_ranges: vec![],
                }],
            }))
        }

        async fn get_keyspace_info(
            &self,
            _request: Request<GetKeyspaceInfoRequest>,
        ) -> Result<Response<GetKeyspaceInfoResponse>, Status> {
            unreachable!()
        }
    }

    static RUNTIME: Lazy<tokio::runtime::Runtime> =
        Lazy::new(|| tokio::runtime::Runtime::new().unwrap());

    struct TestContext {
        assignment_computation: Arc<AssignmentComputationImpl>,
        server_shutdown_tx: Option<oneshot::Sender<()>>,
        base_ranges: Arc<Mutex<Vec<RangeInfo>>>,
        session: Arc<Session>,
        cancellation_token: CancellationToken,
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            let _ = self.server_shutdown_tx.take().unwrap().send(());
            // I could not get the wait on the server task's join handle to work.
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    async fn setup() -> TestContext {
        let port = portpicker::pick_unused_port().unwrap();
        let addr = format!("[::1]:{port}").parse().unwrap();
        let cancellation_token = CancellationToken::new();
        let (signal_tx, signal_rx) = oneshot::channel();
        let base_ranges = Arc::new(Mutex::new(vec![]));
        let base_ranges_clone = base_ranges.clone();
        let cc_clone = cancellation_token.clone();
        RUNTIME.spawn(async move {
            let universe_server = MockUniverseService {
                base_ranges: base_ranges_clone,
            };
            tonic::transport::Server::builder()
                .add_service(UniverseServer::new(universe_server))
                .serve_with_shutdown(addr, async {
                    signal_rx.await.ok();
                    cancellation_token.cancel();
                    info!("Server shutting down");
                })
                .await
                .unwrap();

            info!("Server task completed");
        });
        let client: UniverseClient<tonic::transport::Channel>;
        let addr_string = format!("http://[::1]:{}", port);
        loop {
            let client_result = UniverseClient::connect(addr_string.clone()).await;
            match client_result {
                Ok(client_ok) => {
                    client = client_ok;
                    break;
                }
                Err(e) => {
                    println!("Failed to connect to universe server: {}", e);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        }
        let session = Arc::new(
            SessionBuilder::new()
                .known_node("127.0.0.1:9042".to_string())
                .build()
                .await
                .unwrap(),
        );

        let persistence = Arc::new(Cassandra::new("127.0.0.1:9042".to_string()).await);
        let assignment_computation = AssignmentComputationImpl::new(
            client,
            Region {
                cloud: None,
                name: "".to_string(),
            },
            persistence,
        )
        .await;
        TestContext {
            assignment_computation,
            server_shutdown_tx: Some(signal_tx),
            base_ranges,
            session,
            cancellation_token: cc_clone,
        }
    }

    #[tokio::test]
    async fn test_run_assignment_computation_empty() {
        let context = setup().await;
        let computation = context.assignment_computation.clone();
        let computation_clone = computation.clone();
        computation_clone
            .run_assignment_computation(HashSet::new())
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        assert!(range_assignments.is_empty());
    }

    #[tokio::test]
    async fn test_run_assignment_computation_single_server() {
        let context = setup().await;
        let computation = context.assignment_computation.clone();
        let computation_clone = computation.clone();
        computation_clone
            .start_computation(RUNTIME.handle().clone(), context.cancellation_token.clone());

        // TODO(purujit): For some reason, doing this in the `drop` method of the TestContext does not work. The test exits before
        // the tasks spawned by the drop function are completed.
        // This is a workaround.
        let mut res = context
            .session
            .query("TRUNCATE atomix.range_leases", ())
            .await;
        match res {
            Ok(_) => info!("Truncated atomix.range_leases"),
            Err(e) => panic!("Failed to truncate atomix.range_leases: {}", e),
        }
        res = context.session.query("TRUNCATE atomix.range_map", ()).await;
        match res {
            Ok(_) => info!("Truncated atomix.range_map"),
            Err(e) => panic!("Failed to truncate atomix.range_map: {}", e),
        }
        // One base range and have one server to assign.
        // This test feeds the base ranges through the universe server. Other tests directly populate
        // the unassigned_base_ranges to not have to wait for assignments.
        let range = make_range(0, 127);
        context.base_ranges.lock().unwrap().push(range.clone());
        let host_info = HostInfo {
            identity: HostIdentity {
                name: "server1".to_string(),
                zone: make_zone(),
            },
            address: "1.2.3.4:8080".parse().unwrap(),
            warden_connection_epoch: 1,
        };

        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .insert(HostInfoWrapper(host_info.clone()));

        loop {
            let assignments = computation.range_assignments.lock().unwrap();
            if assignments.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            } else {
                break;
            }
        }
        let current_version;
        {
            current_version = computation.current_version.lock().unwrap().clone();
        }
        let update = computation
            .get_assignment_update(&host_info, current_version, true)
            .unwrap()
            .update
            .unwrap();
        match update {
            proto::warden::warden_update::Update::FullAssignment(full_assigment) => {
                assert_eq!(full_assigment.range.len(), 1);
                assert_eq!(full_assigment.range[0].range_id, range.id.to_string());
            }
            _ => panic!("Expected FullAssignment"),
        }

        assert_eq!(
            context
                .session
                .query(
                    "SELECT * from atomix.range_leases where range_id = ?",
                    (range.id,)
                )
                .await
                .unwrap()
                .rows
                .unwrap()
                .len(),
            1
        );
        let ranges = context
            .session
            .query(
                "SELECT * from atomix.range_map where keyspace_id = ? and range_id = ?",
                (range.keyspace_id.id, range.id),
            )
            .await
            .unwrap()
            .rows
            .unwrap()
            .len();
        assert_eq!(ranges, 1)
    }

    #[tokio::test]
    async fn test_run_assignment_computation_multiple_servers() {
        let context = setup().await;
        let computation = context.assignment_computation.clone();
        let computation_clone = computation.clone();

        // Populate unassigned_base_ranges with 3 ranges and have 2 servers to assign.
        let ranges = vec![
            make_range(0, 127),
            make_range(128, 211),
            make_range(212, 255),
        ];
        let expected_ranges = ranges.clone();
        computation
            .unassigned_base_ranges
            .lock()
            .unwrap()
            .extend(ranges);
        let servers = vec![
            HostInfoWrapper(HostInfo {
                identity: HostIdentity {
                    name: "server1".to_string(),
                    zone: make_zone(),
                },
                address: "1.2.3.4:8080".parse().unwrap(),

                warden_connection_epoch: 1,
            }),
            HostInfoWrapper(HostInfo {
                identity: HostIdentity {
                    name: "server2".to_string(),
                    zone: make_zone(),
                },
                address: "5.6.7.8:8081".parse().unwrap(),

                warden_connection_epoch: 1,
            }),
        ];
        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .extend(servers);

        computation_clone
            .run_assignment_computation(HashSet::new())
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        let current_version = computation.current_version.lock().unwrap();
        assert_eq!(range_assignments[&current_version].len(), 3);

        let mut assigned_ranges: Vec<RangeInfo> = range_assignments[&current_version]
            .iter()
            .map(|a| a.range.clone())
            .collect();
        let assigned_servers: HashSet<String> = range_assignments[&current_version]
            .iter()
            .map(|a| a.assignee.to_string())
            .collect();

        assigned_ranges.sort_by(|a, b| {
            a.key_range
                .lower_bound_inclusive
                .cmp(&b.key_range.lower_bound_inclusive)
        });
        assert_eq!(assigned_ranges, expected_ranges);

        assert_eq!(
            assigned_servers,
            HashSet::from_iter(vec!["server1".to_string(), "server2".to_string()])
        );
    }

    #[tokio::test]
    async fn test_run_assignment_computation_reassign_unavailable_server() {
        let context = setup().await;
        let computation = context.assignment_computation.clone();
        let computation_clone = computation.clone();

        // Populate unassigned_base_ranges with 2 ranges and have 2 servers to assign.
        let ranges = vec![make_range(0, 127), make_range(128, 255)];

        let servers = vec![
            HostInfoWrapper(HostInfo {
                identity: HostIdentity {
                    name: "server1".to_string(),
                    zone: make_zone(),
                },
                address: "1.2.3.4:8080".parse().unwrap(),
                warden_connection_epoch: 1,
            }),
            HostInfoWrapper(HostInfo {
                identity: HostIdentity {
                    name: "server2".to_string(),
                    zone: make_zone(),
                },
                address: "5.6.7.8:8081".parse().unwrap(),

                warden_connection_epoch: 1,
            }),
        ];

        {
            let mut range_assignments = computation.range_assignments.lock().unwrap();
            let current_version = computation.current_version.lock().unwrap();
            // Add a previous assignment for an unavailable server
            range_assignments.insert(
                current_version.clone(),
                vec![
                    RangeAssignment {
                        range: ranges[0].clone(),
                        assignee: "unavailable_server".to_string(),
                    },
                    RangeAssignment {
                        range: ranges[1].clone(),
                        assignee: "server1".to_string(),
                    },
                ],
            );
        }

        // Only add server2 to ready_range_servers
        computation
            .ready_range_servers
            .lock()
            .unwrap()
            .insert(servers[1].clone());

        let prev_servers: HashSet<HostInfoWrapper> = HashSet::from_iter(vec![
            HostInfoWrapper(HostInfo {
                identity: HostIdentity {
                    name: "unavailable_server".to_string(),
                    zone: make_zone(),
                },
                address: "0.0.0.0:0".parse().unwrap(),

                warden_connection_epoch: 1,
            }),
            servers[0].clone(),
        ]);
        computation_clone
            .run_assignment_computation(prev_servers)
            .await;

        let range_assignments = computation.range_assignments.lock().unwrap();
        let current_version = computation.current_version.lock().unwrap();

        assert_eq!(range_assignments.len(), 2);
        let mut assigned_ranges: Vec<RangeInfo> = range_assignments[&current_version]
            .iter()
            .map(|a| a.range.clone())
            .collect();
        let assigned_servers: HashSet<String> = range_assignments[&current_version]
            .iter()
            .map(|a| a.assignee.to_string())
            .collect();

        assigned_ranges.sort_by(|a, b| {
            a.key_range
                .lower_bound_inclusive
                .cmp(&b.key_range.lower_bound_inclusive)
        });
        assert_eq!(assigned_ranges, ranges);

        assert_eq!(
            assigned_servers,
            HashSet::from_iter(vec!["server1".to_string(), "server2".to_string()])
        );
    }

    #[tokio::test]
    async fn test_register_new_range_server() {
        let context = setup().await;
        let computation = context.assignment_computation.clone();
        let server = HostInfo {
            identity: HostIdentity {
                name: "new_server".to_string(),
                zone: make_zone(),
            },
            address: "127.0.0.1:8080".parse().unwrap(),

            warden_connection_epoch: 1,
        };

        let _ = computation.register_range_server(server.clone());

        {
            let ready_servers = computation.ready_range_servers.lock().unwrap();
            assert!(ready_servers.contains(&HostInfoWrapper(server.clone())));
        }
        assert_eq!(
            computation
                .register_range_server(server.clone())
                .err()
                .unwrap()
                .code(),
            Code::AlreadyExists
        );
    }

    #[tokio::test]
    async fn test_notify_range_server_unavailability() {
        let context = setup().await;
        let computation = context.assignment_computation.clone();
        let server = HostInfo {
            identity: HostIdentity {
                name: "server1".to_string(),
                zone: make_zone(),
            },
            address: "127.0.0.1:8080".parse().unwrap(),

            warden_connection_epoch: 1,
        };

        let _ = computation.register_range_server(server.clone());
        {
            let ready_servers = computation.ready_range_servers.lock().unwrap();
            assert!(ready_servers.contains(&HostInfoWrapper(server.clone())));
        }
        computation.notify_range_server_unavailable(server.clone());
        let ready_servers = computation.ready_range_servers.lock().unwrap();
        assert!(!ready_servers.contains(&HostInfoWrapper(server)));
    }
    #[tokio::test]
    async fn test_notify_range_server_unavailability_older_epoch() {
        let context = setup().await;
        let computation = context.assignment_computation.clone();
        let server = HostInfo {
            identity: HostIdentity {
                name: "server1".to_string(),
                zone: make_zone(),
            },
            address: "127.0.0.1:8080".parse().unwrap(),
            warden_connection_epoch: 2,
        };

        let _ = computation.register_range_server(server.clone());
        {
            let ready_servers = computation.ready_range_servers.lock().unwrap();
            assert!(ready_servers.contains(&HostInfoWrapper(server.clone())));
        }

        // Notify with an older epoch
        let older_epoch_server = HostInfo {
            identity: HostIdentity {
                name: "server1".to_string(),
                zone: make_zone(),
            },
            address: "127.0.0.1:8080".parse().unwrap(),
            warden_connection_epoch: 1,
        };
        computation.notify_range_server_unavailable(older_epoch_server);

        let ready_servers = computation.ready_range_servers.lock().unwrap();
        assert!(ready_servers.contains(&HostInfoWrapper(server)));
    }
}
