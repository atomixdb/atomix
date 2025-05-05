use clap::Parser;
use std::net::UdpSocket;
use std::{fs::read_to_string, net::ToSocketAddrs, sync::Arc};

use common::{
    config::Config,
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use epoch_publisher::server;

use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(name = "epoch_publisher")]
#[command(about = "Epoch Publisher", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,

    #[arg(long, default_value = "test-region")]
    region: String,

    #[arg(long, default_value = "a")]
    zone: String,

    #[arg(long, default_value = "ps1")]
    publisher_set_name: String,

    #[arg(long, default_value = "ep1")]
    publisher_name: String,
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = serde_json::from_str(&read_to_string(&args.config).unwrap()).unwrap();

    let region = Region {
        cloud: None,
        name: args.region.into(),
    };
    let zone = Zone {
        region: region.clone(),
        name: args.zone.into(),
    };
    let publisher_set_name = args.publisher_set_name;
    let publisher_name = args.publisher_name;
    let region_config = config.regions.get(&region).unwrap();
    let publisher_set = region_config
        .epoch_publishers
        .iter()
        .find(|&x| x.zone == zone && x.name == publisher_set_name)
        .unwrap();
    let publisher_config = publisher_set
        .publishers
        .iter()
        .find(|&x| x.name == publisher_name)
        .unwrap();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
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
    let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let server = server::Server::new(
        config.clone(),
        publisher_config.clone(),
        bg_runtime.handle().clone(),
    );
    let cancellation_token = CancellationToken::new();
    let ct_clone = cancellation_token.clone();
    let rt_handle = runtime.handle().clone();
    bg_runtime.spawn(async move {
        server::Server::start(server, fast_network.clone(), rt_handle, ct_clone).await;
    });

    runtime.block_on(async move { cancellation_token.cancelled().await });
}
