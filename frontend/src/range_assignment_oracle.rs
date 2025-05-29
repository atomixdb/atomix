use async_trait::async_trait;
use bytes::Bytes;
use uuid::Uuid;

use common::membership::range_assignment_oracle::RangeAssignmentOracle as RangeAssignmentOracleTrait;
use common::{
    full_range_id::FullRangeId,
    host_info::{HostIdentity, HostInfo},
    key_range::KeyRange,
    keyspace::Keyspace,
    keyspace_id::KeyspaceId,
    region::{Region, Zone},
};
use proto::universe::universe_client::UniverseClient;
use proto::universe::{
    get_keyspace_info_request::KeyspaceInfoSearchField, GetKeyspaceInfoRequest,
    Keyspace as ProtoKeyspace, KeyspaceInfo,
};
use std::{collections::HashMap, str::FromStr};
use tokio::sync::RwLock;

// TODO: Dumb little Oracle -- redesign it

pub struct RangeAssignmentOracle {
    universe_client: UniverseClient<tonic::transport::Channel>,
    // Using a couple of caches to avoid making too many requests to the universe client
    key_to_full_range_id: RwLock<HashMap<String, FullRangeId>>,
    keyspace_to_keyspace_info: RwLock<HashMap<String, KeyspaceInfo>>,
}

impl RangeAssignmentOracle {
    pub fn new(universe_client: UniverseClient<tonic::transport::Channel>) -> Self {
        RangeAssignmentOracle {
            universe_client,
            key_to_full_range_id: RwLock::new(HashMap::new()),
            keyspace_to_keyspace_info: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl RangeAssignmentOracleTrait for RangeAssignmentOracle {
    async fn full_range_id_of_key(&self, keyspace: &Keyspace, key: Bytes) -> Option<FullRangeId> {
        // TODO: Pretty slow way of resolving the range id -- optimize this
        // Get KeyspaceInfo by keyspace_id from universe client
        // First,we have already fetched the keyspace_info in Transaction::resolve_keyspace so this is redundant
        // second, we do linear search through the base ranges of keyspace_info to find the range_id which is inefficient

        //  Fast path: Get full range id from cache
        let key_str = String::from_utf8(key.to_vec()).unwrap();
        let key_to_full_range_id_cache_key =
            keyspace.namespace.clone() + &keyspace.name.clone() + &key_str;
        if let Some(full_range_id) = self
            .key_to_full_range_id
            .read()
            .await
            .get(&key_to_full_range_id_cache_key)
        {
            return Some(full_range_id.clone());
        }

        let keyspace_info_cache_key = keyspace.namespace.clone() + &keyspace.name.clone();
        if self
            .keyspace_to_keyspace_info
            .read()
            .await
            .get(&keyspace_info_cache_key)
            .is_none()
        {
            //  Slow path: Get keyspace_info from universe client
            let mut client = self.universe_client.clone();
            let keyspace_info_request = GetKeyspaceInfoRequest {
                keyspace_info_search_field: Some(KeyspaceInfoSearchField::Keyspace(
                    ProtoKeyspace {
                        name: keyspace.name.to_string(),
                        namespace: keyspace.namespace.to_string(),
                    },
                )),
            };
            let keyspace_info_response = client
                .get_keyspace_info(keyspace_info_request)
                .await
                .unwrap();
            let keyspace_info = keyspace_info_response.into_inner().keyspace_info.unwrap();
            self.keyspace_to_keyspace_info
                .write()
                .await
                .insert(keyspace_info_cache_key.clone(), keyspace_info);
        }

        //  Read keyspace_info from cache and find the full range id
        let keyspace_info_guard = self.keyspace_to_keyspace_info.read().await;
        let keyspace_info = keyspace_info_guard.get(&keyspace_info_cache_key).unwrap();
        let keyspace_id = KeyspaceId::from_str(&keyspace_info.keyspace_id).unwrap();
        for range in keyspace_info.base_key_ranges.iter() {
            let key_range = KeyRange::from(range);
            if key_range.includes(key.clone()) {
                let full_range_id = FullRangeId {
                    keyspace_id: keyspace_id.clone(),
                    range_id: Uuid::parse_str(&range.base_range_uuid).unwrap(),
                };
                //  Update cache
                self.key_to_full_range_id
                    .write()
                    .await
                    .insert(key_str, full_range_id.clone());
                return Some(full_range_id.clone());
            }
        }
        None
    }

    async fn host_of_range(&self, range_id: &FullRangeId) -> Option<HostInfo> {
        //  TODO: Ask warden for the host of the range
        //  Hardcoding RangeServer address for now to work my way through the tests
        let identity: String = "test_server".into();
        let region = Region {
            cloud: None,
            name: "test-region".into(),
        };
        let zone = Zone {
            region: region.clone(),
            name: "a".into(),
        };
        Some(HostInfo {
            identity: HostIdentity {
                name: identity.clone(),
                zone,
            },
            address: "127.0.0.1:50055".parse().unwrap(),

            warden_connection_epoch: 0,
        })
    }
    fn maybe_refresh_host_of_range(&self, range_id: &FullRangeId) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use proto::universe::universe_client::UniverseClient;
    use proto::universe::{
        universe_server::{Universe, UniverseServer},
        CreateKeyspaceRequest, CreateKeyspaceResponse, GetKeyspaceInfoRequest,
        GetKeyspaceInfoResponse, KeyRange as ProtoKeyRange, KeyspaceInfo, ListKeyspacesRequest,
        ListKeyspacesResponse, Region as ProtoRegion, Zone as ProtoZone,
    };
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};
    use tracing::info;
    use uuid::Uuid;

    fn make_zone() -> ProtoZone {
        ProtoZone {
            region: Some(ProtoRegion {
                cloud: None,
                name: "test".to_string(),
            }),
            name: "test_zone".to_string(),
        }
    }

    fn make_keyspaceinfo(name: String, namespace: String) -> KeyspaceInfo {
        KeyspaceInfo {
            keyspace_id: Uuid::new_v4().to_string(),
            name: name,
            namespace: namespace,
            primary_zone: Some(make_zone()),
            base_key_ranges: vec![ProtoKeyRange {
                lower_bound_inclusive: vec![0],
                upper_bound_exclusive: vec![10],
                base_range_uuid: Uuid::new_v4().to_string(),
            }],
        }
    }

    struct MockUniverseService {
        keyspaces_info: Arc<Mutex<Vec<KeyspaceInfo>>>,
    }

    #[tonic::async_trait]
    impl Universe for MockUniverseService {
        async fn create_keyspace(
            &self,
            _: Request<CreateKeyspaceRequest>,
        ) -> Result<Response<CreateKeyspaceResponse>, Status> {
            unreachable!()
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
            let keyspace_info_search_field =
                _request.into_inner().keyspace_info_search_field.unwrap();
            match keyspace_info_search_field {
                KeyspaceInfoSearchField::Keyspace(keyspace) => {
                    //  search keyspaces until we find the one with the same keyspace_id
                    for keyspace_info in self.keyspaces_info.lock().unwrap().iter() {
                        if keyspace_info.name == keyspace.name
                            && keyspace_info.namespace == keyspace.namespace
                        {
                            return Ok(Response::new(GetKeyspaceInfoResponse {
                                keyspace_info: Some(keyspace_info.clone()),
                            }));
                        }
                    }
                }
                _ => {
                    return Err(Status::invalid_argument(
                        "Invalid keyspace info search field",
                    ));
                }
            }
            Err(Status::not_found("Keyspace not found"))
        }
    }

    static RUNTIME: Lazy<tokio::runtime::Runtime> =
        Lazy::new(|| tokio::runtime::Runtime::new().unwrap());

    struct TestContext {
        range_assignment_oracle: Arc<RangeAssignmentOracle>,
        server_shutdown_tx: Option<oneshot::Sender<()>>,
        keyspaces_info: Arc<Mutex<Vec<KeyspaceInfo>>>,
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            let _ = self.server_shutdown_tx.take().unwrap().send(());
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    async fn setup() -> TestContext {
        let port = portpicker::pick_unused_port().unwrap();
        let addr = format!("[::1]:{port}").parse().unwrap();
        let (signal_tx, signal_rx) = oneshot::channel();
        let keyspaces_info = Arc::new(Mutex::new(vec![]));
        let keyspaces_info_clone = keyspaces_info.clone();
        RUNTIME.spawn(async move {
            let universe_server = MockUniverseService {
                keyspaces_info: keyspaces_info_clone,
            };
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

        let range_assignment_oracle = Arc::new(RangeAssignmentOracle::new(client));
        TestContext {
            range_assignment_oracle,
            server_shutdown_tx: Some(signal_tx),
            keyspaces_info,
        }
    }

    #[tokio::test]
    async fn test_full_range_id_of_key() {
        let context = setup().await;
        let range_assignment_oracle = context.range_assignment_oracle.clone();
        let keyspace_info =
            make_keyspaceinfo("test_keyspace".to_string(), "test_namespace".to_string());
        context
            .keyspaces_info
            .lock()
            .unwrap()
            .push(keyspace_info.clone());

        //  Use oracle to get the full range id of a key
        let key = Bytes::from_static(&[5]);
        let full_range_id = range_assignment_oracle
            .full_range_id_of_key(
                &Keyspace {
                    name: "test_keyspace".to_string(),
                    namespace: "test_namespace".to_string(),
                },
                key,
            )
            .await
            .unwrap();
        assert_eq!(
            full_range_id.range_id,
            Uuid::parse_str(&keyspace_info.base_key_ranges[0].base_range_uuid).unwrap()
        );
    }
}
