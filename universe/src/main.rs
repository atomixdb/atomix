use clap::Parser;
use common::config::Config;
use std::fs::read_to_string;
use tracing::info;
use universe::server;
use universe::storage::cassandra::Cassandra;

#[derive(Parser, Debug)]
#[command(name = "universe")]
#[command(about = "Universe", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Hello, Universe Manager!");
    let args = Args::parse();
    let config: Config = serde_json::from_str(&read_to_string(&args.config).unwrap()).unwrap();
    let addr = config.universe.proto_server_addr.to_string();
    let storage = Cassandra::new(config.cassandra.cql_addr.to_string()).await;

    server::run_universe_server(addr, storage).await?;
    Ok(())
}
