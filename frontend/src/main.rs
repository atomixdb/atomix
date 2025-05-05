use clap::Parser;
use common::{
    config::Config,
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use std::{
    fs::read_to_string,
    net::{ToSocketAddrs, UdpSocket},
    sync::Arc,
};

use frontend::frontend::Server;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
use tracing::info;

use frontend::range_assignment_oracle::RangeAssignmentOracle;
use proto::universe::universe_client::UniverseClient;

#[derive(Parser, Debug)]
#[command(name = "frontend")]
#[command(about = "Frontend", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,

    #[arg(long, default_value = "test-region")]
    region: String,

    #[arg(long, default_value = "a")]
    zone: String,
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config: Config = serde_json::from_str(&read_to_string(&args.config).unwrap()).unwrap();

    let zone = Zone {
        region: Region {
            cloud: None,
            name: args.region.into(),
        },
        name: args.zone.into(),
    };

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let fast_network_addr = config
        .frontend
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
    let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let bg_runtime_clone = bg_runtime.handle().clone();

    runtime.spawn(async move {
        let proto_server_addr = &config.universe.proto_server_addr;
        let client = UniverseClient::connect(format!("http://{}", proto_server_addr))
            .await
            .unwrap();
        let range_assignment_oracle = Arc::new(RangeAssignmentOracle::new(client));
        let server = Server::new(
            config,
            zone,
            fast_network.clone(),
            range_assignment_oracle,
            runtime_handle,
            bg_runtime_clone,
            ct_clone,
        )
        .await;

        Server::start(server).await;
    });
    info!("Hello Frontend...");
    runtime.block_on(async move { cancellation_token.cancelled().await });
}
