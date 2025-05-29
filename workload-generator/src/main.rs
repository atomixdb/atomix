use clap::Parser;
use common::config::Config;
use common::util::core_affinity::restrict_to_cores;
use core_affinity;
use proto::frontend::frontend_client::FrontendClient;
use serde_json;
use std::fs;
use std::sync::Arc;
use tokio::runtime::{Builder, Handle};
use tokio::time::{sleep, Duration};
use tracing::info;
use workload_generator::{
    workload_config::WorkloadConfig,
    workload_generator::{Metrics, WorkloadGenerator},
};

#[derive(Parser, Debug)]
#[command(name = "workload-generator")]
#[command(about = "Generates workloads based on config files", long_about = None)]
struct Args {
    #[arg(long, default_value = "configs/config.json")]
    config: String,

    #[arg(long, default_value = "workload-generator/configs/config.json")]
    workload_config: String,

    #[arg(long, default_value = "false")]
    create_keyspace: bool,
}

async fn run_workload(
    runtime_handle: Handle,
    config: Config,
    workload_config: WorkloadConfig,
    create_keyspace: bool,
) -> Metrics {
    let frontend_addr = config.frontend.proto_server_addr.to_string();
    let mut client = FrontendClient::connect(format!("http://{}", frontend_addr))
        .await
        .unwrap();

    let workload_generator = Arc::new(WorkloadGenerator::new(workload_config, client));
    if create_keyspace {
        workload_generator.create_keyspace().await;
        sleep(Duration::from_millis(2000)).await;
    }
    let workload_generator_clone = workload_generator.clone();
    let runtime_handle_clone = runtime_handle.clone();
    let workload_handle = runtime_handle
        .spawn(async move { workload_generator_clone.run(runtime_handle_clone).await });
    let metrics = workload_handle.await.unwrap();
    info!("Workload generator finished");
    metrics
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = serde_json::from_str(&fs::read_to_string(&args.config).unwrap()).unwrap();
    let workload_config: WorkloadConfig =
        serde_json::from_str(&fs::read_to_string(&args.workload_config).unwrap()).unwrap();

    let mut background_runtime_cores = workload_config.background_runtime_core_ids.clone();
    if background_runtime_cores.is_empty() {
        background_runtime_cores = core_affinity::get_core_ids()
            .unwrap()
            .iter()
            .map(|id| id.id as u32)
            .collect();
    }
    restrict_to_cores(&background_runtime_cores);
    let runtime = Builder::new_multi_thread()
        .worker_threads(background_runtime_cores.len())
        .enable_all()
        .build()
        .unwrap();
    let runtime_handle = runtime.handle().clone();
    let metrics = runtime.block_on(async move {
        run_workload(
            runtime_handle,
            config,
            workload_config,
            args.create_keyspace,
        )
        .await
    });

    // Return metrics in a format that the Python script can parse
    println!(
        "METRICS_START\n\
        Total Duration: {:?}\n\
        Total Transactions: {}\n\
        Average Latency: {:?}\n\
        P50 Latency: {:?}\n\
        P95 Latency: {:?}\n\
        P99 Latency: {:?}\n\
        Throughput: {:.2} transactions/second\n\
        METRICS_END",
        metrics.total_duration,
        metrics.total_transactions,
        metrics.avg_latency,
        metrics.p50_latency,
        metrics.p95_latency,
        metrics.p99_latency,
        metrics.throughput
    )
}
