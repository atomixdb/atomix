use std::sync::Arc;
use std::{collections::BinaryHeap, collections::HashMap};

use crate::error::Error;
use bytes::Bytes;
use chrono::DateTime;
use common::{host_info::HostInfo, network::fast_network::FastNetwork};
use flatbuf::epoch_publisher_flatbuffers::epoch_publisher::*;
use flatbuffers::FlatBufferBuilder;
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace};
use uuid::Uuid;

/// Provides an async rpc interface to a specific epoch publisher.
pub struct EpochPublisherClient {
    fast_network: Arc<dyn FastNetwork>,
    publisher_host_info: HostInfo,
    outstanding_requests: Mutex<HashMap<Uuid, RpcInfo>>,
    timeout_queue: Mutex<BinaryHeap<(DateTime<chrono::Utc>, Uuid)>>,
}

struct RpcInfo {
    time_sent: DateTime<chrono::Utc>,
    timeout: chrono::Duration,
    response_sender: oneshot::Sender<Result<Bytes, Error>>,
}

pub fn serialize_uuid(uuid: Uuid) -> Uuidu128Args {
    let uint128 = uuid.to_u128_le();
    let lower = uint128 as u64;
    let upper = (uint128 >> 64) as u64;
    Uuidu128Args { lower, upper }
}

pub fn deserialize_uuid(uuidf: Uuidu128<'_>) -> Uuid {
    let res: u128 = ((uuidf.upper() as u128) << 64) | (uuidf.lower() as u128);
    Uuid::from_u128_le(res)
}

impl EpochPublisherClient {
    pub fn new(
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        bg_runtime: tokio::runtime::Handle,
        host_info: HostInfo,
        cancellation_token: CancellationToken,
    ) -> Arc<EpochPublisherClient> {
        let pc = Arc::new(EpochPublisherClient {
            fast_network,
            publisher_host_info: host_info,
            outstanding_requests: Mutex::new(HashMap::new()),
            timeout_queue: Mutex::new(BinaryHeap::new()),
        });

        let pc_clone = pc.clone();
        let ct_clone = cancellation_token.clone();
        runtime.spawn(async move {
            let _ = Self::network_loop(pc_clone, ct_clone).await;
            info!("Epoch Publisher Client: Network loop exited!")
        });
        let pc_clone = pc.clone();
        let ct_clone = cancellation_token.clone();
        bg_runtime.spawn(async move {
            Self::background_loop(pc_clone, ct_clone).await;
            info!("Epoch Publisher Client: Background loop exited!")
        });
        pc
    }

    #[instrument(skip(self))]
    pub async fn read_epoch(&self, timeout: chrono::Duration) -> Result<u64, Error> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        let req_id = Uuid::new_v4();
        trace!(
            "issuing read_epoch rpc. Epoch Publisher: {:#?}. req_id: {}",
            self.publisher_host_info,
            req_id
        );

        // Register the RPC.
        let (tx, rx) = oneshot::channel();
        let rpc = RpcInfo {
            time_sent: chrono::Utc::now(),
            timeout,
            response_sender: tx,
        };
        let expiration_time = rpc.time_sent + rpc.timeout;
        {
            let mut outstanding_requests = self.outstanding_requests.lock().await;
            outstanding_requests.insert(req_id, rpc);
        }
        {
            let mut timeout_queue = self.timeout_queue.lock().await;
            timeout_queue.push((expiration_time, req_id));
        }

        // Send over the wire.
        let request_bytes = self.create_and_serialize_read_epoch_request(&req_id);
        self.fast_network
            .send(self.publisher_host_info.address, request_bytes)
            .unwrap();

        // Wait for response.
        let response = rx.await.unwrap()?;

        trace!("Got response for read_epoch rpc. req_id: {}", req_id);

        // Parse the response.
        let msg = response.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        match envelope.type_() {
            MessageType::ReadEpoch => {
                let response_msg =
                    flatbuffers::root::<ReadEpochResponse>(envelope.bytes().unwrap().bytes())
                        .unwrap();
                let () = Error::from_flatbuf_status(response_msg.status())?;
                trace!("Read epoch: {}", response_msg.epoch());
                return Ok(response_msg.epoch());
            }
            _ => return Err(Error::InvalidResponseFormat),
        }
    }

    fn create_and_serialize_read_epoch_request(&self, req_id: &Uuid) -> Bytes {
        // TODO: too much copying :(
        let mut fbb = FlatBufferBuilder::new();
        let request_id = Some(Uuidu128::create(&mut fbb, &serialize_uuid(*req_id)));
        let fbb_root = ReadEpochRequest::create(&mut fbb, &ReadEpochRequestArgs { request_id });
        fbb.finish(fbb_root, None);
        let get_request_bytes = Bytes::copy_from_slice(fbb.finished_data());
        let mut envelope_fbb = FlatBufferBuilder::new();
        let request_bytes =
            self.create_msg_envelope(&mut envelope_fbb, MessageType::ReadEpoch, get_request_bytes);
        Bytes::copy_from_slice(request_bytes)
    }

    async fn network_loop(client: Arc<Self>, cancellation_token: CancellationToken) {
        let mut network_receiver = client
            .fast_network
            .register(client.publisher_host_info.address);
        loop {
            let () = tokio::select! {
                () = cancellation_token.cancelled() => {
                    return
                }
                maybe_message = network_receiver.recv() => {
                    match maybe_message {
                        None => {
                            error!("Epoch Publisher Client: fast network closed unexpectedly!");
                            cancellation_token.cancel()
                        }
                        Some(msg) => {
                            let req_id = Self::get_request_id_from_response(msg.clone());
                            if let Some(req_id) = req_id {
                                let mut outstanding_requests = client.outstanding_requests.lock().await;
                                let rpc_info = outstanding_requests.remove(&req_id);
                                match rpc_info {
                                    None => debug!("received response for unknown rpc, could be a \
                                        duplicate or have timed out. Dropping. req_id: {}", req_id),
                                    Some(rpc_info) => if let Err(e) = rpc_info.response_sender.send(Ok(msg)) {
                                        error!("failed to send response on onseshot channel for \
                                            req_id: {}. Error {:?}", req_id, e)
                                    }
                                }
                            } else {
                                error!("Epoch Client received a message with an unknown request id. \
                                    Epoch Publisher: {:#?}", client.publisher_host_info.identity)
                            }
                        }
                    }
                }
            };
        }
    }

    fn get_request_id_from_response(msg: Bytes) -> Option<Uuid> {
        let msg = msg.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        let req_id = match envelope.type_() {
            MessageType::ReadEpoch => {
                let msg = flatbuffers::root::<ReadEpochResponse>(envelope.bytes().unwrap().bytes())
                    .unwrap();
                msg.request_id()
            }
            _ => {
                error!("Received a message of an unknown type: {:#?}", envelope);
                return None;
            }
        };
        Some(deserialize_uuid(req_id.unwrap()))
    }

    fn create_msg_envelope<'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'a>,
        msg_type: MessageType,
        bytes: Bytes,
    ) -> &'a [u8] {
        let bytes = fbb.create_vector(bytes.to_vec().as_slice());
        let fbb_root = RequestEnvelope::create(
            fbb,
            &RequestEnvelopeArgs {
                type_: msg_type,
                bytes: Some(bytes),
            },
        );
        fbb.finish(fbb_root, None);
        fbb.finished_data()
    }

    async fn background_loop(client: Arc<Self>, cancellation_token: CancellationToken) {
        loop {
            if cancellation_token.is_cancelled() {
                return;
            }

            'inner: loop {
                let expired_req_id = {
                    let mut timeout_queue = client.timeout_queue.lock().await;
                    if let Some(item) = timeout_queue.peek() {
                        let (expiration_time, _) = item;
                        if (*expiration_time) < chrono::Utc::now() {
                            let (_, req_id) = timeout_queue.pop().unwrap();
                            req_id
                        } else {
                            break 'inner;
                        }
                    } else {
                        break 'inner;
                    }
                };
                // This RPC is past expiration time, let's remove it from outstanding request
                // and return an error if it has not completed.
                let rpc_info = {
                    let mut outstanding_requests = client.outstanding_requests.lock().await;
                    outstanding_requests.remove(&expired_req_id)
                };
                if let Some(rpc) = rpc_info {
                    if let Err(e) = rpc.response_sender.send(Err(Error::Timeout)) {
                        error!(
                            "failed to send response on onseshot channel for req_id: {}. Error {:?}",
                            expired_req_id, e
                        )
                    }
                }
            }

            // TODO(tamer): make this configurable.
            // Also, consider sleeping until the next expiration time.
            tokio::time::sleep(core::time::Duration::from_millis(10)).await;
        }
    }
}
