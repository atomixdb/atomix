use std::sync::Arc;

use crate::publisher_set_updater::PublisherSetUpdater;
use crate::storage::Storage;
use common::config::Config;
use proto::epoch::epoch_server::{Epoch, EpochServer};
use proto::epoch::{ReadEpochRequest, ReadEpochResponse};
use std::net::ToSocketAddrs;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};
use tracing::{error, info, instrument};

pub struct Server<S>
where
    S: Storage,
{
    storage: S,
    config: Config,
    publisher_sets: Vec<Arc<PublisherSetUpdater>>,
}

struct ProtoServer<S>
where
    S: Storage,
{
    server: Arc<Server<S>>,
}

#[tonic::async_trait]
impl<S> Epoch for ProtoServer<S>
where
    S: Storage,
{
    #[instrument(skip(self))]
    async fn read_epoch(
        &self,
        _request: Request<ReadEpochRequest>,
    ) -> Result<Response<ReadEpochResponse>, TStatus> {
        match self.server.storage.read_latest().await {
            Err(e) => {
                error!("read epoch RPC failed with={}", e);
                let status = TStatus::new(tonic::Code::Internal, "Failed to read latest epoch");
                Err(status)
            }
            Ok(epoch) => {
                let reply = ReadEpochResponse { epoch };
                Ok(Response::new(reply))
            }
        }
    }
}

impl<S> Server<S>
where
    S: Storage,
{
    pub fn new(storage: S, config: Config) -> Server<S> {
        let publisher_sets: Vec<Arc<PublisherSetUpdater>> = config
            .regions
            .values()
            .flat_map(|rc| {
                let updaters: Vec<Arc<PublisherSetUpdater>> = rc
                    .epoch_publishers
                    .iter()
                    .map(|set| Arc::new(PublisherSetUpdater::new(set.clone())))
                    .collect();
                updaters
            })
            .collect();
        Server {
            storage,
            config,
            publisher_sets,
        }
    }

    pub async fn start(server: Arc<Server<S>>, cancellation_token: CancellationToken) {
        info!("Starting epoch service.");
        // TODO: log errors
        server.storage.initialize_epoch().await.unwrap();
        let proto_server = ProtoServer {
            server: server.clone(),
        };
        Server::<S>::start_update_loop(server.clone(), cancellation_token);
        // TODO(tamer): make this configurable.
        let addr = server
            .config
            .epoch
            .proto_server_addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        if let Err(e) = TServer::builder()
            .add_service(EpochServer::new(proto_server))
            .serve(addr)
            .await
        {
            panic!("Unable to start proto server: {}", e);
        }
    }

    fn start_update_loop(server: Arc<Server<S>>, cancellation_token: CancellationToken) {
        tokio::spawn(async move {
            let mut last_update = chrono::Utc::now() - server.config.epoch.epoch_duration;
            'outer: loop {
                if cancellation_token.is_cancelled() {
                    info!("Update loop cancelled, exiting.");
                    return;
                }
                info!("Starting an update epoch iteration.");
                let read_result = server.storage.read_latest().await;
                let epoch = match read_result {
                    Err(e) => {
                        error!(
                            "Failed to read latest epoch value from storage. Error:{}",
                            e
                        );
                        continue;
                    }
                    Ok(epoch) => epoch,
                };
                info!("Got current epoch: {}", epoch);
                // Fire off a request to update the epoch at each of the sets in parallel.
                let mut join_set = JoinSet::new();
                for s in server.publisher_sets.iter() {
                    let s = s.clone();
                    join_set.spawn(async move { s.update_epoch(epoch).await });
                }

                // If all succeeded, we can advance the epoch.
                while let Some(res) = join_set.join_next().await {
                    let res = match res {
                        Err(e) => {
                            error!("Updating a publisher set failed, abandoning epoch update. Error: {}", e);
                            continue 'outer;
                        }
                        Ok(res) => res,
                    };
                    if let Err(e) = res {
                        error!(
                            "Updating a publisher set failed, abandoning epoch update. Error: {}",
                            e
                        );
                        continue 'outer;
                    };
                }
                // If we got here then we updated all the broadcaster sets, we can advance the epoch.
                let next_update_in = chrono::TimeDelta::max(chrono::TimeDelta::zero(), (last_update + server.config.epoch.epoch_duration) - chrono::Utc::now()).to_std().unwrap();
                tokio::time::sleep(next_update_in).await;
                if let Err(e) = server.storage.conditional_update(epoch + 1, epoch).await {
                    error!("Failed to increment epoch. Error: {}", e);
                    continue 'outer;
                }
                last_update = chrono::Utc::now();
            }
        });
    }
}
