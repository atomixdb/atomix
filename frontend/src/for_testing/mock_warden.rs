use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

use common::full_range_id::FullRangeId;
use proto::warden::{
    warden_server::{Warden, WardenServer},
    warden_update::Update::{FullAssignment, IncrementalAssignment},
    RegisterRangeServerRequest, WardenUpdate,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, RwLock},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

struct WardenState {
    range_to_host: RwLock<HashMap<Uuid, String>>,
    host_ranges: RwLock<HashMap<String, HashSet<FullRangeId>>>,
    rs_connections: RwLock<HashMap<String, mpsc::Sender<Result<WardenUpdate, Status>>>>,
}
pub struct MockWarden {
    state: Arc<WardenState>,
}

fn range_id_proto(range: &FullRangeId) -> proto::warden::RangeId {
    proto::warden::RangeId {
        keyspace_id: range.keyspace_id.id.to_string(),
        range_id: range.range_id.to_string(),
    }
}

impl Default for MockWarden {
    fn default() -> Self {
        Self::new()
    }
}

impl MockWarden {
    pub fn new() -> MockWarden {
        let warden_state = Arc::new(WardenState {
            range_to_host: RwLock::new(HashMap::new()),
            host_ranges: RwLock::new(HashMap::new()),
            rs_connections: RwLock::new(HashMap::new()),
        });

        MockWarden {
            state: warden_state,
        }
    }

    pub async fn start(
        &self,
        address: Option<SocketAddr>,
    ) -> Result<SocketAddr, Box<dyn std::error::Error>> {
        let address = address.unwrap_or("127.0.0.1:0".parse().unwrap());
        let state = self.state.clone();
        let listener = TcpListener::bind(address).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async {
            let svc = WardenServer::from_arc(state);
            let _ = Server::builder()
                .add_service(svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        Ok(addr)
    }

    pub async fn unassign(&self, range: &FullRangeId) {
        let range_id = &range.range_id;
        let range_proto = range_id_proto(range);
        let mut range_to_host = self.state.range_to_host.write().await;
        let previous_host_for_range = (*range_to_host).remove(range_id);
        let mut host_ranges = self.state.host_ranges.write().await;
        match previous_host_for_range {
            None => (),
            Some(previous_host) => {
                (*host_ranges)
                    .get_mut(&previous_host)
                    .unwrap()
                    .remove(range);
                let incremental = proto::warden::IncrementalAssignment {
                    version: 2,
                    previous_version: 1,
                    unload: vec![range_proto],
                    load: vec![],
                };
                let warden_update = WardenUpdate {
                    update: Some(IncrementalAssignment(incremental)),
                };
                let connections = self.state.rs_connections.read().await;
                match connections.get(&previous_host) {
                    None => (),
                    Some(sender) => sender.send(Ok(warden_update)).await.unwrap(),
                }
            }
        }
    }

    pub async fn assign(&self, range: &FullRangeId, host: &String) {
        self.unassign(range).await;
        let range_id = &range.range_id;
        let range_proto = range_id_proto(range);
        {
            let mut range_to_host = self.state.range_to_host.write().await;
            let mut host_ranges = self.state.host_ranges.write().await;
            (*range_to_host).insert(*range_id, host.clone());
            if !host_ranges.contains_key(host) {
                host_ranges.insert(host.clone(), HashSet::new());
            }
            (*host_ranges).get_mut(host).unwrap().insert(*range);
        }
        let incremental = proto::warden::IncrementalAssignment {
            version: 2,
            previous_version: 1,
            load: vec![range_proto],
            unload: vec![],
        };
        let warden_update = WardenUpdate {
            update: Some(IncrementalAssignment(incremental)),
        };
        let connections = self.state.rs_connections.read().await;
        match connections.get(host) {
            None => (),
            Some(sender) => sender.send(Ok(warden_update)).await.unwrap(),
        }
    }

    pub async fn is_connected(&self, host: &String) -> bool {
        let connections = self.state.rs_connections.read().await;
        connections.get(host).is_some()
    }

    pub async fn disconnect(&self, host: &String) {
        let mut connections = self.state.rs_connections.write().await;
        connections.remove(host);
    }
}

#[tonic::async_trait]
impl Warden for WardenState {
    type RegisterRangeServerStream = ReceiverStream<Result<WardenUpdate, Status>>;

    async fn register_range_server(
        &self,
        request: Request<RegisterRangeServerRequest>,
    ) -> Result<Response<Self::RegisterRangeServerStream>, Status> {
        let host = request.get_ref().range_server.clone().unwrap().identity;
        let (tx, rx) = mpsc::channel(40);
        let host_ranges = self.host_ranges.read().await;
        let mut connections = self.rs_connections.write().await;
        (*connections).insert(host.clone(), tx.clone());
        let assigned_ranges = match (*host_ranges).get(&host) {
            None => vec![],
            Some(ranges) => ranges.iter().map(range_id_proto).collect(),
        };
        let full = proto::warden::FullAssignment {
            version: 1,
            range: assigned_ranges,
        };
        let warden_update = WardenUpdate {
            update: Some(FullAssignment(full)),
        };
        tx.send(Ok(warden_update)).await.unwrap();
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
