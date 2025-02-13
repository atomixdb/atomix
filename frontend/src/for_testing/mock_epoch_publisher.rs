use bytes::Bytes;
use common::network::fast_network::FastNetwork;
use common::network::for_testing::udp_fast_network::UdpFastNetwork;
use flatbuf::epoch_publisher_flatbuffers::epoch_publisher::*;
use flatbuffers::FlatBufferBuilder;
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, trace};
type DynamicErr = Box<dyn std::error::Error + Sync + Send + 'static>;

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

//  Mock Epoch Publisher:
//  1) Listens for read_epoch requests and responds with the current epoch
//  2) Starts a loop that increments the epoch every millisecond
pub struct MockEpochPublisher {
    epoch: AtomicUsize,
}

impl MockEpochPublisher {
    #[instrument(skip(self, network))]
    async fn read_epoch(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: ReadEpochRequest<'_>,
    ) -> Result<(), DynamicErr> {
        trace!("received read_epoch");
        let mut fbb = FlatBufferBuilder::new();
        let fbb_root = match request.request_id() {
            None => ReadEpochResponse::create(
                &mut fbb,
                &ReadEpochResponseArgs {
                    request_id: None,
                    status: Status::InvalidRequestFormat,
                    epoch: 0,
                },
            ),
            Some(request_id) => {
                let epoch = self.epoch.load(SeqCst);
                let status = if epoch == 0 {
                    Status::EpochUnknown
                } else {
                    Status::Ok
                };
                let request_id = Some(Uuidu128::create(
                    &mut fbb,
                    &Uuidu128Args {
                        lower: request_id.lower(),
                        upper: request_id.upper(),
                    },
                ));
                ReadEpochResponse::create(
                    &mut fbb,
                    &ReadEpochResponseArgs {
                        request_id,
                        status,
                        epoch: epoch as u64,
                    },
                )
            }
        };

        fbb.finish(fbb_root, None);
        self.send_response(network, sender, MessageType::ReadEpoch, fbb.finished_data())?;
        Ok(())
    }

    fn send_response(
        &self,
        fast_network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        msg_type: MessageType,
        msg_payload: &[u8],
    ) -> Result<(), std::io::Error> {
        // TODO: many allocations and copies in this function.
        let mut fbb = FlatBufferBuilder::new();
        let bytes = fbb.create_vector(msg_payload);
        let fb_root = ResponseEnvelope::create(
            &mut fbb,
            &ResponseEnvelopeArgs {
                type_: msg_type,
                bytes: Some(bytes),
            },
        );
        fbb.finish(fb_root, None);
        let response = Bytes::copy_from_slice(fbb.finished_data());
        fast_network.send(sender, response)
    }

    async fn handle_message(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        msg: Bytes,
    ) -> Result<(), DynamicErr> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        let msg = msg.to_vec();
        let envelope = flatbuffers::root::<RequestEnvelope>(msg.as_slice())?;
        match envelope.type_() {
            MessageType::ReadEpoch => {
                let req = flatbuffers::root::<ReadEpochRequest>(envelope.bytes().unwrap().bytes())?;
                server.read_epoch(fast_network.clone(), sender, req).await?
            }
            _ => error!("Received a message of an unknown type: {:#?}", envelope),
        };
        Ok(())
    }

    async fn network_server_loop(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        network_receiver: tokio::sync::mpsc::UnboundedReceiver<(SocketAddr, Bytes)>,
        cancellation_token: CancellationToken,
    ) {
        let mut network_receiver = network_receiver;
        loop {
            let () = tokio::select! {
                () = cancellation_token.cancelled() => {
                    return
                }
                maybe_message = network_receiver.recv() => {
                    match maybe_message {
                        None => {
                            error!("fast network closed unexpectedly!");
                            cancellation_token.cancel()
                        }
                        Some((sender, msg)) => {
                            let server = server.clone();
                            let fast_network = fast_network.clone();
                            tokio::spawn(async move{
                                if let Err(e) = Self::handle_message(server, fast_network, sender, msg).await {
                                    error!("error handling a network message: {}", e);
                                }
                            });

                        }
                    }
                }
            };
        }
    }

    pub async fn start(
        fast_network: Arc<UdpFastNetwork>,
        cancellation_token: CancellationToken,
    ) -> Result<(), Status> {
        let publisher = Arc::new(MockEpochPublisher {
            epoch: AtomicUsize::new(1),
        });

        let fast_network_clone = fast_network.clone();
        RUNTIME.spawn(async move {
            loop {
                fast_network_clone.poll();
                tokio::task::yield_now().await
            }
        });

        let publisher_clone = publisher.clone();
        let ct_clone = cancellation_token.clone();

        let (listener_tx, listener_rx) = oneshot::channel();
        RUNTIME.spawn(async move {
            let network_receiver = fast_network.listen_default();
            info!("Listening to fast network");
            listener_tx.send(()).unwrap();
            let _ = Self::network_server_loop(
                publisher_clone,
                fast_network,
                network_receiver,
                ct_clone,
            )
            .await;
            info!("Network server loop exited!")
        });
        listener_rx.await.unwrap();

        // // Start a loop that increments the epoch every second
        // RUNTIME.spawn(async move {
        //     loop {
        //         tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        //         publisher.epoch.fetch_add(1, SeqCst);
        //     }
        // });
        Ok(())
    }
}
