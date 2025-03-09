use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common::{
    host_info::{HostIdentity, HostInfo},
    region::{Region, Zone},
};
use pin_project::{pin_project, pinned_drop};
use proto::{
    universe::universe_client::UniverseClient,
    warden::{warden_server::Warden, RegisterRangeServerRequest, WardenUpdate},
};
use tokio::sync::broadcast;
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    Stream,
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

use crate::{
    assignment_computation::{AssignmentComputation, AssignmentComputationImpl},
    persistence::cassandra::Cassandra,
};

/// Implementation of the Warden service.
pub struct WardenServer {
    assignment_computation: Arc<dyn AssignmentComputation + Sync + Send>,
}

#[tonic::async_trait]
impl Warden for WardenServer {
    type RegisterRangeServerStream = AssignmentUpdateStream<'static>;

    #[instrument(skip(self))]
    async fn register_range_server(
        &self,
        request: Request<RegisterRangeServerRequest>,
    ) -> Result<Response<Self::RegisterRangeServerStream>, Status> {
        debug!("Got a register_range_server request: {:?}", request);

        let register_request = request.into_inner();
        match register_request.range_server {
            None => {
                return Err(Status::invalid_argument(
                    "range_server field is not set in the request",
                ))
            }
            Some(range_server) => {
                info!("Registering range server: {}", range_server.identity);
                let host_info = HostInfo {
                    identity: HostIdentity {
                        name: range_server.identity,
                        zone: Zone {
                            name: range_server.zone,
                            // TODO(purujit): Get the region from the range server.
                            region: Region {
                                cloud: None,
                                name: "".to_string(),
                            },
                        },
                    },
                    // todo(purujit): Get the address from the range server.
                    address: SocketAddr::from(([0, 0, 0, 0], 0)),

                    warden_connection_epoch: range_server.epoch,
                };
                match self
                    .assignment_computation
                    .register_range_server(host_info.clone())
                {
                    Ok(update_receiver) => Ok(Response::new(AssignmentUpdateStream::new(
                        update_receiver,
                        self.assignment_computation.clone(),
                        host_info,
                    ))),
                    Err(e) => Err(e),
                }
            }
        }
    }
}

impl WardenServer {
    pub fn new(assignment_computation: Arc<dyn AssignmentComputation + Sync + Send>) -> Self {
        Self {
            assignment_computation,
        }
    }
}

/// Runs the Warden server, listening on the provided address and using the given update callback
/// function to generate updates for registered range servers.
///
/// # Arguments
/// - `addr`: The address for the Warden server to listen on.
///
/// # Errors
/// This function will return an error if the provided address is invalid or if there is an issue
/// starting the Warden server.
pub async fn run_warden_server(
    addr: impl AsRef<str> + std::net::ToSocketAddrs,
    universe_addr: String,
    cassandra_addr: String,
    region: Region,
    runtime: tokio::runtime::Handle,
    cancellation_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.to_socket_addrs()?.next().ok_or("Invalid address")?;
    let universe_client = UniverseClient::connect(universe_addr).await?;
    let assignment_computation = AssignmentComputationImpl::new(
        universe_client,
        region,
        Arc::new(Cassandra::new(cassandra_addr).await),
    )
    .await;
    let clone = assignment_computation.clone();
    clone.start_computation(runtime, cancellation_token);
    let warden_server = WardenServer::new(assignment_computation);

    info!("WardenServer listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(proto::warden::warden_server::WardenServer::new(
            warden_server,
        ))
        .serve(addr)
        .await?;

    Ok(())
}

/// A stream that provides updates to assignment versions.
///
/// This stream is created by the `WardenServer` to provide updates to registered range servers.
/// It uses a `broadcast::Receiver` to receive version updates, and a callback function to
/// generate the `WardenUpdate` messages to be sent to the clients.
///
/// The stream will send a full update on the first message, and then only send incremental
/// updates after that. If the channel buffer is exhausted, the stream will return an error
/// indicating that the client should reopen the channel.
// Cannot use pin_project_lite because we need a drop implementation.
// https://docs.rs/pin-project/latest/pin_project/attr.pinned_drop.html
// https://dtantsur.github.io/rust-openstack/pin_project_lite/index.html mentions that custom drop is not supported.
#[pin_project(PinnedDrop)]
pub struct AssignmentUpdateStream<'a> {
    sent_full_update: bool,
    update_stream: BroadcastStream<i64>,
    assignment_computation: Arc<dyn AssignmentComputation + Sync + Send + 'a>,
    host_info: HostInfo,
}

impl<'a> AssignmentUpdateStream<'a> {
    pub fn new(
        version_update_receiver: broadcast::Receiver<i64>,
        assignment_computation: Arc<dyn AssignmentComputation + Sync + Send + 'a>,
        host_info: HostInfo,
    ) -> Self {
        AssignmentUpdateStream {
            sent_full_update: false,
            update_stream: version_update_receiver.into(),
            assignment_computation,
            host_info,
        }
    }
}

impl<'a> Stream for AssignmentUpdateStream<'a> {
    type Item = Result<WardenUpdate, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pinned = std::pin::pin!(self.as_mut().project().update_stream);
        match pinned.poll_next(cx) {
            Poll::Ready(update) => match update {
                Some(result) => match result {
                    Ok(version) => {
                        let update = self.assignment_computation.get_assignment_update(
                            &self.host_info,
                            version,
                            !self.sent_full_update,
                        );
                        self.sent_full_update = true;
                        match update {
                            Some(update) => Poll::Ready(Some(Ok(update))),
                            None => Poll::Ready(Some(Err(Status::internal(format!(
                                "Unexpected failure to retrieve assignment version {}",
                                version
                            ))))),
                        }
                    }
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        Poll::Ready(Some(Err(Status::resource_exhausted(format!(
                            "Channel Buffer exhausted. Please reopen channel. Lagged by {}",
                            n
                        )))))
                    }
                },
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

#[pinned_drop]
// We need a custom drop implementation because we need to detect gRCP disconnects
// See: https://github.com/hyperium/tonic/issues/196
// TODO(purujit): Verify that this gets invoked when the client goes away.
impl PinnedDrop for AssignmentUpdateStream<'_> {
    fn drop(self: Pin<&mut Self>) {
        let host_info = self.host_info.clone();
        self.assignment_computation
            .notify_range_server_unavailable(host_info);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use proto::warden::warden_client::WardenClient;
    use tokio::sync::broadcast::Receiver;
    use tonic::transport::Channel;

    const FULL_UPDATE_VERSION: i64 = 1;
    const PARTIAL_UPDATE_VERSION: i64 = 2;

    struct FakeMapProducer {
        full_update: WardenUpdate,
        incremental_update: WardenUpdate,
        update_sender: broadcast::Sender<i64>,
        dropped_clients: Mutex<Vec<HostInfo>>,
    }

    impl FakeMapProducer {
        pub fn new() -> Self {
            Self {
                full_update: WardenUpdate {
                    update: Some(proto::warden::warden_update::Update::FullAssignment(
                        proto::warden::FullAssignment {
                            version: FULL_UPDATE_VERSION,
                            range: vec![proto::warden::RangeId {
                                keyspace_id: "test_keyspace".to_string(),
                                range_id: "test_range".to_string(),
                            }],
                        },
                    )),
                },
                incremental_update: WardenUpdate {
                    update: Some(proto::warden::warden_update::Update::IncrementalAssignment(
                        proto::warden::IncrementalAssignment {
                            version: PARTIAL_UPDATE_VERSION,
                            previous_version: FULL_UPDATE_VERSION,
                            load: vec![proto::warden::RangeId {
                                keyspace_id: "test_keyspace".to_string(),
                                range_id: "test_range".to_string(),
                            }],
                            unload: vec![],
                        },
                    )),
                },
                update_sender: broadcast::channel(1).0,
                dropped_clients: Mutex::new(vec![]),
            }
        }
        pub fn send_full_update(&self) -> () {
            self.update_sender.send(FULL_UPDATE_VERSION).unwrap();
        }
        pub fn send_incremental_update(&self) -> () {
            self.update_sender.send(PARTIAL_UPDATE_VERSION).unwrap();
        }
    }

    impl AssignmentComputation for FakeMapProducer {
        fn get_assignment_update(
            &self,
            host_info: &HostInfo,
            _version: i64,
            full_update: bool,
        ) -> Option<WardenUpdate> {
            if full_update {
                Some(self.full_update.clone())
            } else {
                Some(self.incremental_update.clone())
            }
        }
        fn register_range_server(
            &self,
            _: common::host_info::HostInfo,
        ) -> Result<Receiver<i64>, Status> {
            Ok(self.update_sender.subscribe())
        }

        fn notify_range_server_unavailable(&self, host_info: HostInfo) {
            self.dropped_clients.lock().unwrap().push(host_info)
        }
    }
    #[tokio::test]
    async fn test_warden_server_startup_and_client_updates() {
        let fake_map_producer = Arc::new(FakeMapProducer::new());
        let warden_server = WardenServer::new(fake_map_producer.clone());

        // Start the server in a separate task
        let server_task = tokio::spawn(async move {
            let addr = "[::1]:50052".parse().unwrap();
            tonic::transport::Server::builder()
                .add_service(proto::warden::warden_server::WardenServer::new(
                    warden_server,
                ))
                .serve(addr)
                .await
                .unwrap();
        });

        // Give the server a moment to start up
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a client and connect to the server
        let channel = Channel::from_static("http://[::1]:50052")
            .connect()
            .await
            .unwrap();
        let mut client = WardenClient::new(channel);

        // Call register_range_server
        let request = tonic::Request::new(RegisterRangeServerRequest {
            range_server: Some(proto::warden::HostInfo {
                identity: "test_server".to_string(),
                zone: "test_zone".to_string(),
                epoch: 1,
            }),
        });
        let response = client.register_range_server(request).await.unwrap();
        let mut stream = response.into_inner();

        fake_map_producer.send_full_update();

        // Verify that the client receives the full update
        let received_update = stream.message().await.unwrap().unwrap();
        assert_eq!(received_update, fake_map_producer.full_update);

        fake_map_producer.send_incremental_update();
        let received_incremental_update = stream.message().await.unwrap().unwrap();
        assert_eq!(
            received_incremental_update,
            fake_map_producer.incremental_update
        );

        server_task.abort();
    }
}
