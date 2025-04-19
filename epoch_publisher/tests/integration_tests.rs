use epoch_publisher::client::EpochPublisherClient;
use epoch_publisher::error::Error;
use epoch_publisher::for_testing::mock_epoch::MockEpoch;
use epoch_publisher::server::Server;
use std::{
    collections::HashSet,
    net::{SocketAddr, UdpSocket},
    str::FromStr,
    sync::Arc,
    time,
};
use tokio_util::sync::CancellationToken;

use common::{
    config::{
        CassandraConfig, Config, EpochConfig, EpochPublisher as EpochPublisherConfig,
        FrontendConfig, HostPort, RangeServerConfig, RegionConfig, UniverseConfig,
    },
    host_info::{HostIdentity, HostInfo},
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use test_log::test;
use tokio::runtime::Builder;

struct TestContext {
    client: Arc<EpochPublisherClient>,
    cancellation_token: CancellationToken,
    server_runtime: tokio::runtime::Runtime,
    client_runtime: tokio::runtime::Runtime,
    client_bg_runtime: tokio::runtime::Runtime,
}

fn get_config(epoch_address: SocketAddr) -> Config {
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let region_config = RegionConfig {
        // Not used in these tests.
        warden_address: "127.0.0.1:1".parse().unwrap(),
        epoch_publishers: HashSet::new(),
    };
    let epoch_config = EpochConfig {
        // Not used in these tests.
        proto_server_addr: HostPort::from_socket_addr(epoch_address),
        epoch_duration: time::Duration::from_millis(10),
    };
    let mut config = Config {
        range_server: RangeServerConfig {
            range_maintenance_duration: time::Duration::from_secs(1),
            proto_server_addr: HostPort::from_str("127.0.0.1:50054").unwrap(),
            fast_network_addr: HostPort::from_str("127.0.0.1:50055").unwrap(),
        },
        universe: UniverseConfig {
            proto_server_addr: "127.0.0.1:123".parse().unwrap(),
        },
        frontend: FrontendConfig {
            proto_server_addr: HostPort::from_str("127.0.0.1:50057").unwrap(),
            fast_network_addr: HostPort::from_str("127.0.0.1:50058").unwrap(),
            transaction_overall_timeout: time::Duration::from_secs(10),
        },
        cassandra: CassandraConfig {
            cql_addr: "127.0.0.1:9042".parse().unwrap(),
        },
        regions: std::collections::HashMap::new(),
        epoch: epoch_config,
    };
    config.regions.insert(region, region_config);
    config
}

fn get_publisher_config(fast_network_addr: SocketAddr) -> EpochPublisherConfig {
    let backend_addr = HostPort::from_str("127.0.0.1:10010").unwrap();
    EpochPublisherConfig {
        name: "ep1".to_string(),
        backend_addr,
        fast_network_addr: HostPort::from_socket_addr(fast_network_addr),
    }
}

async fn setup_server(
    server_socket: UdpSocket,
    cancellation_token: CancellationToken,
    epoch_address: SocketAddr,
) -> tokio::runtime::Runtime {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let fast_network_addr = server_socket.local_addr().unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(server_socket));
    let fast_network_clone = fast_network.clone();
    let runtime_clone = runtime.handle().clone();
    let (server_ready_tx, server_ready_rx) = tokio::sync::oneshot::channel();
    runtime.spawn(async move {
        let config = get_config(epoch_address);
        let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let server = Server::new(
            config,
            get_publisher_config(fast_network_addr),
            bg_runtime.handle().clone(),
        );
        Server::start(server, fast_network, runtime_clone, cancellation_token).await;
        server_ready_tx.send(()).unwrap();
    });
    runtime.spawn(async move {
        loop {
            for _ in 1..5 {
                fast_network_clone.poll();
            }
            tokio::time::sleep(core::time::Duration::from_millis(1)).await;
        }
    });
    server_ready_rx.await.unwrap();
    runtime
}

fn get_server_host_info(address: SocketAddr) -> HostInfo {
    let identity: String = "test_publisher".into();
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let zone = Zone {
        region: region.clone(),
        name: "a".into(),
    };
    HostInfo {
        identity: HostIdentity {
            name: identity.clone(),
            zone,
        },
        address,
        warden_connection_epoch: 0,
    }
}

async fn setup_client(
    cancellation_token: CancellationToken,
    server_address: SocketAddr,
) -> (
    Arc<EpochPublisherClient>,
    tokio::runtime::Runtime,
    tokio::runtime::Runtime,
) {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let udp_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(udp_socket));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            for _ in 1..5 {
                fast_network_clone.poll();
            }
            tokio::time::sleep(core::time::Duration::from_millis(1)).await;
        }
    });
    let client = EpochPublisherClient::new(
        fast_network,
        runtime.handle().clone(),
        bg_runtime.handle().clone(),
        get_server_host_info(server_address),
        cancellation_token.clone(),
    );
    (client, runtime, bg_runtime)
}

async fn setup(initial_epoch: u64) -> TestContext {
    let server_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_address = server_socket.local_addr().unwrap();
    let mock_epoch = MockEpoch::new();
    let epoch_address = mock_epoch.start().await.unwrap();
    mock_epoch.set_epoch(initial_epoch).await;
    let cancellation_token = CancellationToken::new();
    let server_runtime =
        setup_server(server_socket, cancellation_token.clone(), epoch_address).await;
    let (client, client_runtime, client_bg_runtime) =
        setup_client(cancellation_token.clone(), server_address).await;
    TestContext {
        client,
        cancellation_token,
        server_runtime,
        client_runtime,
        client_bg_runtime,
    }
}

async fn tear_down(context: TestContext) {
    context.cancellation_token.cancel();
    // TODO: investigate why shutdown isn't clean.
    context.server_runtime.shutdown_background();
    context.client_runtime.shutdown_background();
    context.client_bg_runtime.shutdown_background();
}

#[test(tokio::test)]
async fn read_uninitialized_epoch() {
    let context = setup(0).await;
    let err = context
        .client
        .read_epoch(chrono::Duration::seconds(30))
        .await;
    let err = err.err().unwrap();
    assert!(err == Error::EpochUnknown);
    tear_down(context).await
}

#[test(tokio::test)]
async fn read_epoch() {
    let context = setup(42).await;
    let epoch = context
        .client
        .read_epoch(chrono::Duration::seconds(30))
        .await
        .unwrap();
    assert!(epoch == 42);
    tear_down(context).await
}
