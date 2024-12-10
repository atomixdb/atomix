use once_cell::sync::Lazy;
use std::{
    sync::{Arc, Mutex},
    vec,
};
use tonic::{Request, Response, Status};

use common::config::Config;

use proto::universe::{
    get_keyspace_info_request::KeyspaceInfoSearchField,
    universe_client::UniverseClient,
    universe_server::{Universe, UniverseServer},
    CreateKeyspaceRequest, CreateKeyspaceResponse, GetKeyspaceInfoRequest, GetKeyspaceInfoResponse,
    KeyspaceInfo, ListKeyspacesRequest, ListKeyspacesResponse,
};
use tokio::sync::oneshot;
use tracing::info;
use uuid::Uuid;

static RUNTIME: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().unwrap());

pub struct MockUniverse {
    keyspaces_info: Arc<Mutex<Vec<KeyspaceInfo>>>,
    server_shutdown_tx: Option<oneshot::Sender<()>>,
}

#[tonic::async_trait]
impl Universe for MockUniverse {
    async fn create_keyspace(
        &self,
        _request: Request<CreateKeyspaceRequest>,
    ) -> Result<Response<CreateKeyspaceResponse>, Status> {
        let req_inner = _request.into_inner();
        let base_key_ranges: Vec<proto::universe::KeyRange> = req_inner
            .base_key_ranges
            .into_iter()
            .map(|kr| proto::universe::KeyRange {
                base_range_uuid: Uuid::new_v4().to_string(),
                lower_bound_inclusive: kr.lower_bound_inclusive,
                upper_bound_exclusive: kr.upper_bound_exclusive,
            })
            .collect();

        let base_key_ranges = if base_key_ranges.is_empty() {
            vec![proto::universe::KeyRange {
                base_range_uuid: Uuid::new_v4().to_string(),
                lower_bound_inclusive: vec![],
                upper_bound_exclusive: vec![],
            }]
        } else {
            base_key_ranges
        };
        let keyspace_info = KeyspaceInfo {
            keyspace_id: Uuid::new_v4().to_string(),
            namespace: req_inner.namespace,
            name: req_inner.name,
            primary_zone: req_inner.primary_zone,
            secondary_zones: req_inner.secondary_zones,
            base_key_ranges,
            secondary_key_ranges: vec![],
        };
        self.keyspaces_info
            .lock()
            .unwrap()
            .push(keyspace_info.clone());
        Ok(Response::new(CreateKeyspaceResponse {
            keyspace_id: keyspace_info.keyspace_id,
        }))
    }

    async fn list_keyspaces(
        &self,
        _request: Request<ListKeyspacesRequest>,
    ) -> Result<Response<ListKeyspacesResponse>, Status> {
        unreachable!()
    }

    async fn get_keyspace_info(
        &self,
        _request: Request<GetKeyspaceInfoRequest>,
    ) -> Result<Response<GetKeyspaceInfoResponse>, Status> {
        let keyspace_info_search_field = _request.into_inner().keyspace_info_search_field.unwrap();
        match keyspace_info_search_field {
            KeyspaceInfoSearchField::KeyspaceId(keyspace_id) => {
                for keyspace_info in self.keyspaces_info.lock().unwrap().iter() {
                    if keyspace_info.keyspace_id == keyspace_id {
                        return Ok(Response::new(GetKeyspaceInfoResponse {
                            keyspace_info: Some(keyspace_info.clone()),
                        }));
                    }
                }
            }
            KeyspaceInfoSearchField::Keyspace(keyspace) => {
                for keyspace_info in self.keyspaces_info.lock().unwrap().iter() {
                    if keyspace_info.namespace == keyspace.namespace
                        && keyspace_info.name == keyspace.name
                    {
                        return Ok(Response::new(GetKeyspaceInfoResponse {
                            keyspace_info: Some(keyspace_info.clone()),
                        }));
                    }
                }
            }
        }
        Err(Status::not_found("Keyspace not found"))
    }
}

impl MockUniverse {
    pub async fn start(
        config: &Config,
    ) -> Result<UniverseClient<tonic::transport::Channel>, Status> {
        let addr = config.universe.proto_server_addr.to_string();
        let (signal_tx, signal_rx) = oneshot::channel();
        let keyspaces_info = Arc::new(Mutex::new(vec![]));
        let keyspaces_info_clone = keyspaces_info.clone();

        RUNTIME.spawn(async move {
            let universe_server = MockUniverse {
                keyspaces_info: keyspaces_info_clone,
                server_shutdown_tx: Some(signal_tx),
            };
            let addr = addr.parse().unwrap();
            tonic::transport::Server::builder()
                .add_service(UniverseServer::new(universe_server))
                .serve_with_shutdown(addr, async {
                    signal_rx.await.ok();
                    info!("Server shutting down");
                })
                .await
                .unwrap();
            info!("Server task completed");
        });

        //  Get UniverseClient
        let client: UniverseClient<tonic::transport::Channel>;
        let addr_string = format!("http://{}", config.universe.proto_server_addr);
        loop {
            let client_result = UniverseClient::connect(addr_string.clone()).await;
            match client_result {
                Ok(client_ok) => {
                    client = client_ok;
                    break;
                }
                Err(e) => {
                    info!("Failed to connect to universe server: {}", e);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        }
        Ok(client)
    }
}

impl Drop for MockUniverse {
    fn drop(&mut self) {
        let _ = self.server_shutdown_tx.take().unwrap().send(());
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
