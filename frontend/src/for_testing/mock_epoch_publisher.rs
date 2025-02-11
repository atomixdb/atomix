use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};
use tracing::info;

use proto::epoch_publisher::{
    epoch_publisher_server::{EpochPublisher, EpochPublisherServer},
    SetEpochRequest, SetEpochResponse,
};

use common::config::Config;
use proto::epoch_publisher::epoch_publisher_client::EpochPublisherClient;

static RUNTIME: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().unwrap());

pub struct MockEpochPublisher {
    epoch: AtomicU64,
    server_shutdown_tx: Option<oneshot::Sender<()>>,
}

#[tonic::async_trait]
impl EpochPublisher for MockEpochPublisher {
    async fn set_epoch(
        &self,
        request: Request<SetEpochRequest>,
    ) -> Result<Response<SetEpochResponse>, Status> {
        let new_epoch = request.into_inner().epoch;
        let current_epoch = self.epoch.load(Ordering::SeqCst);

        if current_epoch == 0 {
            return Err(Status::failed_precondition("Epoch not yet initialized"));
        }

        if new_epoch > current_epoch {
            self.epoch.store(new_epoch, Ordering::SeqCst);
        }

        Ok(Response::new(SetEpochResponse {}))
    }
}

impl MockEpochPublisher {
    pub async fn start(
        config: &Config,
    ) -> Result<EpochPublisherClient<tonic::transport::Channel>, Status> {
        let addr = config.epoch.proto_server_addr.to_string();
        let (signal_tx, signal_rx) = oneshot::channel();

        RUNTIME.spawn(async move {
            let publisher = MockEpochPublisher {
                epoch: AtomicU64::new(1), // Initialize with epoch 1 to allow updates
                server_shutdown_tx: Some(signal_tx),
            };

            let addr = addr.parse().unwrap();
            tonic::transport::Server::builder()
                .add_service(EpochPublisherServer::new(publisher))
                .serve_with_shutdown(addr, async {
                    signal_rx.await.ok();
                    info!("Server shutting down");
                })
                .await
                .unwrap();

            info!("Server task completed");
        });

        // Get EpochPublisherClient
        let addr_string = format!("http://{}", config.epoch.proto_server_addr);
        loop {
            let client_result = EpochPublisherClient::connect(addr_string.clone()).await;
            match client_result {
                Ok(client) => return Ok(client),
                Err(e) => {
                    println!("Failed to connect to publisher server: {}", e);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        }
    }
}

impl Drop for MockEpochPublisher {
    fn drop(&mut self) {
        let _ = self.server_shutdown_tx.take().unwrap().send(());
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
