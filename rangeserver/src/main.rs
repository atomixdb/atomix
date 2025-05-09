use clap::Parser;
use std::{
    fs::read_to_string,
    net::{ToSocketAddrs, UdpSocket},
    sync::Arc,
};

use common::{
    config::Config,
    host_info::{HostIdentity, HostInfo},
    network::{
        fast_network::spawn_tokio_polling_thread, for_testing::udp_fast_network::UdpFastNetwork,
    },
    region::{Region, Zone},
    util::core_affinity::restrict_to_cores,
};
use core_affinity;
use rangeserver::{cache::memtabledb::MemTableDB, server::Server, storage::cassandra::Cassandra};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "rangeserver")]
#[command(about = "Rangeserver", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,

    #[arg(long, default_value = "test-region")]
    region: String,

    #[arg(long, default_value = "a")]
    zone: String,

    #[arg(long, default_value = "test_server")]
    identity: String,

    #[arg(long, default_value = "127.0.0.1:50054")]
    address: String,
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = serde_json::from_str(&read_to_string(&args.config).unwrap()).unwrap();

    let mut background_runtime_cores = config.range_server.background_runtime_core_ids.clone();
    if background_runtime_cores.is_empty() {
        background_runtime_cores = core_affinity::get_core_ids()
            .unwrap()
            .iter()
            .map(|id| id.id as u32)
            .collect();
    }
    restrict_to_cores(&background_runtime_cores);

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
        spawn_tokio_polling_thread(
            "fast-network-poller-rangeserver",
            fast_network_clone,
            config.range_server.fast_network_polling_core_id as usize,
        )
        .await;
    });
    let server_handle = runtime.spawn(async move {
        let cancellation_token = CancellationToken::new();
        let host_info = HostInfo {
            identity: HostIdentity {
                name: args.identity.into(),
                zone: Zone {
                    region: Region {
                        cloud: None,
                        name: args.region.into(),
                    },
                    name: args.zone.into(),
                },
            },
            address: args.address.parse().unwrap(),
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
        info!("Connecting to Cassandra at {}", config.cassandra.cql_addr);
        let storage = Arc::new(Cassandra::new(config.cassandra.cql_addr.to_string()).await);

        let bg_runtime = Builder::new_multi_thread()
            .worker_threads(background_runtime_cores.len())
            .enable_all()
            .build()
            .unwrap();

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
