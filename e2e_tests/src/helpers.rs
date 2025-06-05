use std::net::ToSocketAddrs;
use std::{net::UdpSocket, sync::Arc};

use common::host_info::{HostIdentity, HostInfo};
use common::network::fast_network::FastNetwork;
use common::region::{Region, Zone};
use common::{config::Config, network::for_testing::udp_fast_network::UdpFastNetwork};
use proto::universe::universe_client::UniverseClient;
use proto::universe::KeyspaceInfo;
use proto::universe::{
    get_keyspace_info_request::KeyspaceInfoSearchField, CreateKeyspaceRequest,
    GetKeyspaceInfoRequest, KeyRangeRequest, Keyspace as ProtoKeyspace, ListKeyspacesRequest,
    ListKeyspacesResponse, Region as ProtoRegion, Zone as ProtoZone,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

pub struct SimpleCluster {
    pub config: Config,
    pub zone: Zone,
    pub universe_handle: UniverseHandle,
    pub epoch_handle: EpochHandle,
    pub epoch_publisher_handle: EpochPublisherHandle,
    pub frontend_handle: FrontendHandle,
    pub warden_handle: WardenHandle,
    pub range_server_handle: RangeServerHandle,
}

pub struct RangeServerHandle {
    pub runtime: tokio::runtime::Runtime,
    pub bg_runtime: tokio::runtime::Runtime,
    pub server: Arc<rangeserver::server::Server<rangeserver::storage::cassandra::Cassandra>>,
    pub server_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct WardenHandle {
    pub runtime: tokio::runtime::Runtime,
    pub assignment_computation: Arc<warden::assignment_computation::AssignmentComputationImpl>,
    pub server_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct UniverseHandle {
    pub runtime: tokio::runtime::Runtime,
    pub server_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct EpochHandle {
    pub runtime: tokio::runtime::Runtime,
    pub server_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct EpochPublisherHandle {
    pub runtime: tokio::runtime::Runtime,
    pub bg_runtime: tokio::runtime::Runtime,
    pub server_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct FrontendHandle {
    pub runtime: tokio::runtime::Runtime,
    pub bg_runtime: tokio::runtime::Runtime,
    pub server_handle: Option<tokio::task::JoinHandle<()>>,
}

impl SimpleCluster {
    pub async fn new() -> Self {
        // Based on the default config.json file.
        let zone = Zone {
            region: Region {
                cloud: None,
                name: "test-region".into(),
            },
            name: "a".into(),
        };
        let config = Config {
            range_server: common::config::RangeServerConfig {
                range_maintenance_duration: std::time::Duration::new(1, 0),
                proto_server_addr: common::config::HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 50054,
                },
                fast_network_addr: common::config::HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 50055,
                },
            },
            universe: common::config::UniverseConfig {
                proto_server_addr: common::config::HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 50056,
                },
            },
            frontend: common::config::FrontendConfig {
                proto_server_addr: common::config::HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 50057,
                },
                fast_network_addr: common::config::HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 50058,
                },
                transaction_overall_timeout: std::time::Duration::new(10, 0),
            },
            epoch: common::config::EpochConfig {
                proto_server_addr: common::config::HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 50050,
                },
                epoch_duration: std::time::Duration::new(0, 10000000),
            },
            cassandra: common::config::CassandraConfig {
                cql_addr: common::config::HostPort {
                    host: "127.0.0.1".to_string(),
                    port: 9042,
                },
            },
            regions: std::collections::HashMap::from([(
                common::region::Region {
                    cloud: None,
                    name: "test-region".to_string(),
                },
                common::config::RegionConfig {
                    warden_address: common::config::HostPort {
                        host: "127.0.0.1".to_string(),
                        port: 50053,
                    },
                    epoch_publishers: std::collections::HashSet::from([
                        common::config::EpochPublisherSet {
                            name: "ps1".to_string(),
                            zone: zone.clone(),
                            publishers: std::collections::HashSet::from([
                                common::config::EpochPublisher {
                                    name: "ep1".to_string(),
                                    backend_addr: common::config::HostPort {
                                        host: "127.0.0.1".to_string(),
                                        port: 50051,
                                    },
                                    fast_network_addr: common::config::HostPort {
                                        host: "127.0.0.1".to_string(),
                                        port: 50052,
                                    },
                                },
                            ]),
                        },
                    ]),
                },
            )]),
        };

        let universe_handle = start_universe(config.clone()).await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let epoch_handle = start_epoch(config.clone()).await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let epoch_publisher_handle = start_epoch_publisher(config.clone()).await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let frontend_handle = start_frontend(config.clone(), zone.clone()).await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let warden_handle = start_warden(config.clone()).await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let range_server_handle = start_range_server(config.clone(), zone.clone()).await;

        Self {
            config,
            zone,
            universe_handle,
            epoch_handle,
            epoch_publisher_handle,
            frontend_handle,
            warden_handle,
            range_server_handle,
        }
    }

    pub async fn shutdown(self) {
        self.universe_handle.runtime.shutdown_background();
        self.warden_handle.runtime.shutdown_background();
        self.epoch_handle.runtime.shutdown_background();
        self.epoch_publisher_handle.runtime.shutdown_background();
        self.epoch_publisher_handle.bg_runtime.shutdown_background();
        self.range_server_handle.runtime.shutdown_background();
        self.range_server_handle.bg_runtime.shutdown_background();
        self.frontend_handle.runtime.shutdown_background();
        self.frontend_handle.bg_runtime.shutdown_background();
    }
}

pub async fn start_range_server(config: Config, zone: Zone) -> RangeServerHandle {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let runtime_handle = runtime.handle().clone();

    let fast_network_addr = config
        .range_server
        .fast_network_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    let proto_server_addr = config
        .range_server
        .proto_server_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    let fast_network = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind(fast_network_addr).unwrap(),
    ));
    let fast_network_clone = fast_network.clone();

    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });

    let bg_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let bg_runtime_handle = bg_runtime.handle().clone();

    let server_starter_task = runtime.spawn(async move {
        let cancellation_token = CancellationToken::new();
        let host_info = HostInfo {
            identity: HostIdentity {
                name: "127.0.0.1".into(),
                zone,
            },
            address: proto_server_addr.to_string().parse().unwrap(),
            warden_connection_epoch: 0,
        };

        let region_config = config.regions.get(&host_info.identity.zone.region).unwrap();
        let publisher_set = region_config
            .epoch_publishers
            .iter()
            .find(|&s| s.zone == host_info.identity.zone)
            .unwrap();

        let proto_server_addr = config
            .range_server
            .proto_server_addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let proto_server_listener = TcpListener::bind(proto_server_addr).await.unwrap();

        let storage = Arc::new(
            rangeserver::storage::cassandra::Cassandra::new(config.cassandra.cql_addr.to_string())
                .await,
        );

        let epoch_supplier = Arc::new(rangeserver::epoch_supplier::reader::Reader::new(
            fast_network.clone(),
            runtime_handle,
            bg_runtime_handle.clone(),
            publisher_set.clone(),
            cancellation_token.clone(),
        ));

        let server = rangeserver::server::Server::<_>::new(
            config.clone(),
            host_info,
            storage,
            epoch_supplier,
            bg_runtime_handle,
        );

        let server_clone = server.clone();
        let res = rangeserver::server::Server::start(
            server,
            fast_network,
            CancellationToken::new(),
            proto_server_listener,
        )
        .await
        .unwrap();
        (res, server_clone)
    });

    let (server_exit_receiver, server) = server_starter_task.await.unwrap();

    let server_handle = runtime.spawn(async move {
        server_exit_receiver.await.unwrap().unwrap();
    });

    RangeServerHandle {
        runtime,
        bg_runtime,
        server,
        server_handle: Some(server_handle),
    }
}

pub async fn start_warden(config: Config) -> WardenHandle {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Region is the first key in the regions map.
    let region = config.regions.keys().next().unwrap().clone();
    let region_config = config.regions.get(&region).unwrap();
    let warden_addr = region_config.warden_address.to_string();
    let universe_addr = format!("http://{}", config.universe.proto_server_addr.to_string());
    let token = CancellationToken::new();
    let runtime_handle = runtime.handle().clone();
    let cassandra_addr = config.cassandra.cql_addr.to_string();

    let universe_client = UniverseClient::connect(universe_addr).await.unwrap();
    let (stream_mux, heartbeat_receiver) =
        warden::stream_multiplexer::StreamMultiplexer::new(100, runtime_handle.clone());
    let assignment_computation = warden::assignment_computation::AssignmentComputationImpl::new(
        universe_client,
        stream_mux,
        region,
        Arc::new(warden::persistence::cassandra::Cassandra::new(cassandra_addr).await),
    )
    .await;
    let assignment_computation_clone = assignment_computation.clone();
    let assignment_computation_clone1 = assignment_computation.clone();
    assignment_computation_clone.start_computation(
        heartbeat_receiver,
        runtime_handle,
        CancellationToken::new(),
    );
    let warden_server = warden::server::WardenServer::new(assignment_computation);

    let warden_server_handle = runtime.spawn(async move {
        info!("WardenServer listening on {}", warden_addr);

        tonic::transport::Server::builder()
            .add_service(proto::warden::warden_server::WardenServer::new(
                warden_server,
            ))
            .serve(warden_addr.parse().unwrap())
            .await
            .unwrap();
    });

    WardenHandle {
        runtime,
        assignment_computation: assignment_computation_clone1,
        server_handle: Some(warden_server_handle),
    }
}

pub async fn start_universe(config: Config) -> UniverseHandle {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let addr = config.universe.proto_server_addr.to_string();
    let addr = addr.to_socket_addrs().unwrap().next().unwrap();
    let storage =
        universe::storage::cassandra::Cassandra::new(config.cassandra.cql_addr.to_string()).await;

    let universe_server = universe::server::UniverseServer::new(Arc::new(storage));

    let server_handle = runtime.spawn(async move {
        info!("UniverseServer listening on {}", addr);
        tonic::transport::Server::builder()
            .add_service(proto::universe::universe_server::UniverseServer::new(
                universe_server,
            ))
            .serve(addr)
            .await
            .unwrap();
    });

    UniverseHandle {
        runtime,
        server_handle: Some(server_handle),
    }
}

pub async fn start_epoch(config: Config) -> EpochHandle {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let addr = config.epoch.proto_server_addr.to_string();
    let addr = addr.to_socket_addrs().unwrap().next().unwrap();
    let storage = epoch::storage::cassandra::Cassandra::new(
        config.cassandra.cql_addr.to_string(),
        "GLOBAL".to_string(),
    )
    .await;

    let server = Arc::new(epoch::server::Server::new(storage, config.clone()));
    let cancellation_token = CancellationToken::new();

    let server_handle = runtime.spawn(async move {
        info!("EpochServer listening on {}", addr);
        epoch::server::Server::start(server, cancellation_token).await;
    });

    EpochHandle {
        runtime,
        server_handle: Some(server_handle),
    }
}

pub async fn start_epoch_publisher(config: Config) -> EpochPublisherHandle {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let publisher_config = config
        .regions
        .values()
        .next()
        .unwrap()
        .epoch_publishers
        .iter()
        .next()
        .unwrap()
        .publishers
        .iter()
        .next()
        .unwrap()
        .clone();

    let fast_network_addr = publisher_config
        .fast_network_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind(fast_network_addr).unwrap(),
    ));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });

    let bg_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let server = epoch_publisher::server::Server::new(
        config.clone(),
        publisher_config,
        bg_runtime.handle().clone(),
    );
    let cancellation_token = CancellationToken::new();
    let ct_clone = cancellation_token.clone();
    let rt_handle = runtime.handle().clone();
    let server_handle = bg_runtime.spawn(async move {
        epoch_publisher::server::Server::start(server, fast_network.clone(), rt_handle, ct_clone)
            .await;
    });
    EpochPublisherHandle {
        runtime,
        bg_runtime,
        server_handle: Some(server_handle),
    }
}

pub async fn start_frontend(config: Config, zone: Zone) -> FrontendHandle {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let frontend_config = config.frontend.clone();

    let fast_network_addr = frontend_config
        .fast_network_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind(fast_network_addr).unwrap(),
    ));
    let fast_network_clone = fast_network.clone();

    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });

    let cancellation_token = CancellationToken::new();
    let runtime_handle = runtime.handle().clone();
    let ct_clone = cancellation_token.clone();
    let bg_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let bg_runtime_clone = bg_runtime.handle().clone();

    let server_handle = runtime.spawn(async move {
        let proto_server_addr = &config.universe.proto_server_addr;
        let client = UniverseClient::connect(format!("http://{}", proto_server_addr))
            .await
            .unwrap();
        let cache = Arc::new(coordinator::cache::in_memory::InMemoryCache::new(
            client,
            runtime_handle.clone(),
        ));
        let range_assignment_oracle =
            Arc::new(frontend::range_assignment_oracle::RangeAssignmentOracle::new(cache.clone()));
        let server = frontend::frontend::Server::new(
            config.clone(),
            zone,
            fast_network.clone(),
            range_assignment_oracle,
            cache,
            runtime_handle,
            bg_runtime_clone,
            ct_clone,
        )
        .await;

        frontend::frontend::Server::start(server).await;
    });

    FrontendHandle {
        runtime,
        bg_runtime,
        server_handle: Some(server_handle),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_context_starts() {
        let simple_cluster = SimpleCluster::new().await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        simple_cluster.shutdown().await;
    }
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::new("info,epoch=off,epoch_publisher=off"))
        .try_init();
}

pub async fn create_keyspace_if_not_exists(
    universe_client: &mut UniverseClient<tonic::transport::Channel>,
    namespace: String,
    name: String,
    zone: ProtoZone,
) -> KeyspaceInfo {
    // List all keyspaces
    let response = universe_client
        .list_keyspaces(ListKeyspacesRequest { region: None })
        .await
        .unwrap();

    info!("Existing keyspaces: {:?}", response.get_ref());

    // Check if keyspace exists by looking through the list response
    let ListKeyspacesResponse { keyspaces } = response.into_inner();
    let existing_keyspace = keyspaces
        .iter()
        .find(|k| k.name == name && k.namespace == namespace);

    match existing_keyspace {
        Some(keyspace_info) => {
            info!("Found existing keyspace: {:?}", keyspace_info);
            keyspace_info.clone()
        }
        None => {
            // Create new keyspace if it doesn't exist
            let response = universe_client
                .create_keyspace(CreateKeyspaceRequest {
                    namespace: namespace.clone(),
                    name: name.clone(),
                    primary_zone: Some(zone.clone()),
                    secondary_zones: vec![zone],
                    base_key_ranges: vec![],
                })
                .await
                .unwrap();

            // Sleep a bit so ranges can be assigned
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            let keyspace_id = response.get_ref().keyspace_id.clone();
            info!("Created new keyspace: {:?}", keyspace_id);

            // Get the keyspace info
            let keyspace_info_request = GetKeyspaceInfoRequest {
                keyspace_info_search_field: Some(KeyspaceInfoSearchField::Keyspace(
                    ProtoKeyspace { namespace, name },
                )),
            };
            let keyspace_info = universe_client
                .get_keyspace_info(keyspace_info_request)
                .await
                .unwrap();
            keyspace_info
                .get_ref()
                .keyspace_info
                .as_ref()
                .unwrap()
                .clone()
        }
    }
}
