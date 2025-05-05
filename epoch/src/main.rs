use std::{fs::read_to_string, sync::Arc};

use clap::Parser;
use common::config::Config;
use epoch::server;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(name = "epoch")]
#[command(about = "Epoch", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = serde_json::from_str(&read_to_string(&args.config).unwrap()).unwrap();
    let storage = epoch::storage::cassandra::Cassandra::new(
        config.cassandra.cql_addr.to_string(),
        "GLOBAL".to_string(),
    )
    .await;

    let server = Arc::new(server::Server::new(storage, config));
    let cancellation_token = CancellationToken::new();
    server::Server::start(server, cancellation_token).await;
}
