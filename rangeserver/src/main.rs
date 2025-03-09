use std::{
    net::{ToSocketAddrs, UdpSocket},
    sync::Arc,
};

use common::{
    config::Config,
    host_info::{HostIdentity, HostInfo},
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use rangeserver::{cache::memtabledb::MemTableDB, server::Server, storage::cassandra::Cassandra};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
use tracing::info;

fn main() {
    tracing_subscriber::fmt::init();
    // TODO(tamer): take the config path as an argument.
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let runtime_handle = runtime.handle().clone();
    let fast_network_addr = config
        .range_server
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
    let server_handle = runtime.spawn(async move {
        let cancellation_token = CancellationToken::new();
        let host_info = get_host_info();
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
        info!("Connecting to Cassandra at {}", config.cassandra.cql_addr);
        let storage = Arc::new(Cassandra::new(config.cassandra.cql_addr.to_string()).await);
        // TODO: set number of threads and pin to cores.
        let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        let epoch_supplier = Arc::new(rangeserver::epoch_supplier::reader::Reader::new(
            fast_network.clone(),
            runtime_handle,
            bg_runtime.handle().clone(),
            publisher_set.clone(),
            cancellation_token.clone(),
        ));
        let server = Server::<_>::new(
            config,
            host_info,
            storage,
            epoch_supplier,
            bg_runtime.handle().clone(),
        );
        let res = Server::start(
            server,
            fast_network,
            CancellationToken::new(),
            proto_server_listener,
        )
        .await
        .unwrap();
        res.await.unwrap()
    });
    info!("Starting RangeServer...");
    runtime.block_on(server_handle).unwrap().unwrap();
}

fn get_host_info() -> HostInfo {
    // TODO: should be read from enviroment!
    let identity: String = "test_server".into();
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
        address: "127.0.0.1:50054".parse().unwrap(),

        warden_connection_epoch: 0,
    }
}
