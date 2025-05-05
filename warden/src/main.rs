use clap::Parser;
use common::config::Config;
use common::region::Region;
use server::run_warden_server;
use std::fs::read_to_string;
use tokio_util::sync::CancellationToken;
use tracing::info;

mod assignment_computation;
mod persistence;
mod server;

#[derive(Parser, Debug)]
#[command(name = "warden")]
#[command(about = "Warden", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,

    #[arg(long, default_value = "test-region")]
    region: String,

    #[arg(long, default_value = "a")]
    zone: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt::Subscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;
    info!("Hello, Warden!");
    let args = Args::parse();
    let config: Config = serde_json::from_str(&read_to_string(&args.config).unwrap()).unwrap();
    let region = Region {
        cloud: None,
        name: args.region.into(),
    };
    let region_config = config.regions.get(&region).unwrap();
    let addr = region_config.warden_address.to_string();
    let universe_addr = format!("http://{}", config.universe.proto_server_addr.to_string());
    let token = CancellationToken::new();
    // TODO(purujit): set up map computation and plug it in.
    run_warden_server(
        addr,
        universe_addr,
        config.cassandra.cql_addr.to_string(),
        region,
        tokio::runtime::Handle::current(),
        token,
    )
    .await?;
    Ok(())
}
