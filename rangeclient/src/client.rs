use bytes::Bytes;
use common::network::fast_network::FastNetwork;
use common::util;
use common::{
    epoch_lease::EpochLease, full_range_id::FullRangeId, host_info::HostInfo, record::Record,
    transaction_info::TransactionInfo,
};
use flatbuf::rangeserver_flatbuffers::range_server::Record as FlatbufRecord;
use flatbuf::rangeserver_flatbuffers::range_server::TransactionInfo as FlatbufTransactionInfo;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use flatbuffers::FlatBufferBuilder;
use proto::rangeserver::range_server_client::RangeServerClient;
use proto::rangeserver::{PrefetchRequest, RangeId, RangeKey};
use rangeserver::error::Error as RangeServerError;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::Request;
use uuid::Uuid;

pub type Error = RangeServerError;
pub struct PrepareOk {
    pub highest_known_epoch: u64,
    pub epoch_lease: EpochLease,
}

#[derive(Debug)]
pub struct GetResult {
    pub vals: Vec<Option<Bytes>>,
    pub leader_sequence_number: i64,
}

struct StartedState {
    // TODO: make more typeful and store more information to e.g. allow timing out.
    outstanding_requests: HashMap<Uuid, oneshot::Sender<Result<Bytes, RangeServerError>>>,
}

enum State {
    NotStarted,
    Started(StartedState),
    Stopped,
}

// Provides an async rpc interface to a specific range server.
pub struct RangeClient {
    fast_network: Arc<dyn FastNetwork>,
    range_server_info: HostInfo,
    state: RwLock<State>,
    proto_client: Option<Arc<RangeServerClient<Channel>>>,
}

impl RangeClient {
    pub async fn new(
        fast_network: Arc<dyn FastNetwork>,
        host_info: HostInfo,
        // TODO(tamer): make a "RangeServerHostIdentity" and have proto server
        // be a required field on it.
        proto_server_addr: Option<SocketAddr>,
    ) -> Arc<RangeClient> {
        let proto_client = match proto_server_addr {
            None => None,
            Some(proto_server_addr) => {
                let addr = format!("http://{}", proto_server_addr);
                Some(Arc::new(RangeServerClient::connect(addr).await.unwrap()))
            }
        };
        Arc::new(RangeClient {
            fast_network,
            range_server_info: host_info,
            state: RwLock::new(State::NotStarted),
            proto_client,
        })
    }

    pub async fn start(
        rc: Arc<Self>,
        runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
    ) {
        let mut state = rc.state.write().await;
        match state.deref_mut() {
            State::Started(_) | State::Stopped => return (),
            State::NotStarted => (),
        };
        let started_state = StartedState {
            outstanding_requests: HashMap::new(),
        };
        let _ = std::mem::replace(state.deref_mut(), State::Started(started_state));
        let rc_clone = rc.clone();
        runtime.spawn(async move {
            let _ = Self::network_loop(rc_clone, cancellation_token).await;
            println!("Network loop exited!")
        });
    }

    pub async fn is_stopped(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            State::Stopped => true,
            State::NotStarted | State::Started(_) => false,
        }
    }

    pub fn host_info(&self) -> HostInfo {
        self.range_server_info.clone()
    }

    pub async fn get(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        keys: Vec<Bytes>,
    ) -> Result<GetResult, RangeServerError> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        // TODO: too much copying :(
        let req_id = Uuid::new_v4();
        let mut fbb = FlatBufferBuilder::new();
        let transaction_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(tx.id),
        ));
        let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &range_id));
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(req_id),
        ));
        // TODO: only supply the transaction info on the first request to the RS.
        let transaction_info = Some(FlatbufTransactionInfo::create(
            &mut fbb,
            &TransactionInfoArgs {
                overall_timeout_us: tx.overall_timeout.as_micros() as u32,
            },
        ));
        let mut keys_vector = Vec::new();
        for key in keys {
            let k = Some(fbb.create_vector(key.to_vec().as_slice()));
            let key = Key::create(&mut fbb, &KeyArgs { k });
            keys_vector.push(key)
        }
        let keys = Some(fbb.create_vector(&keys_vector));
        let fbb_root = GetRequest::create(
            &mut fbb,
            &GetRequestArgs {
                request_id,
                transaction_id,
                range_id,
                transaction_info,
                keys,
            },
        );
        fbb.finish(fbb_root, None);
        let (tx, rx) = oneshot::channel();
        self.record_outstanding_request(req_id, tx).await?;
        let get_request_bytes = Bytes::copy_from_slice(fbb.finished_data());
        let mut envelope_fbb = FlatBufferBuilder::new();
        let request_bytes =
            self.create_msg_envelope(&mut envelope_fbb, MessageType::Get, get_request_bytes);
        self.fast_network
            .send(
                self.range_server_info.address,
                Bytes::copy_from_slice(request_bytes),
            )
            .unwrap();
        let response = rx.await.unwrap()?;
        let msg = response.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        match envelope.type_() {
            MessageType::Get => {
                let response_msg =
                    flatbuffers::root::<GetResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                let () = rangeserver::error::Error::from_flatbuf_status(response_msg.status())?;
                let leader_sequence_number = response_msg.leader_sequence_number();
                let mut result = Vec::new();
                for record in response_msg.records().iter() {
                    for rec in record.iter() {
                        let val = match rec.value() {
                            None => None,
                            Some(val) => Some(Bytes::copy_from_slice(val.bytes())),
                        };
                        result.push(val);
                    }
                }
                return Ok(GetResult {
                    vals: result,
                    leader_sequence_number,
                });
            }
            _ => return Err(RangeServerError::InvalidRequestFormat),
        }
    }

    pub async fn prepare_transaction(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        has_reads: bool,
        writes: &[Record],
        deletes: &[Bytes],
    ) -> Result<PrepareOk, RangeServerError> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        // TODO: too much copying :(
        let req_id = Uuid::new_v4();
        let mut fbb = FlatBufferBuilder::new();
        let transaction_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(tx.id),
        ));
        let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &range_id));
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(req_id),
        ));
        let mut deletes_vector = Vec::new();
        for key in deletes {
            let k = Some(fbb.create_vector(key.to_vec().as_slice()));
            let key = Key::create(&mut fbb, &KeyArgs { k });
            deletes_vector.push(key)
        }
        let deletes = Some(fbb.create_vector(&deletes_vector));
        let mut puts_vector = Vec::new();
        for record in writes {
            let k = Some(fbb.create_vector(record.key.to_vec().as_slice()));
            let key = Key::create(&mut fbb, &KeyArgs { k });
            let value = fbb.create_vector(record.val.to_vec().as_slice());
            puts_vector.push(FlatbufRecord::create(
                &mut fbb,
                &RecordArgs {
                    key: Some(key),
                    value: Some(value),
                },
            ));
        }
        let puts = Some(fbb.create_vector(&puts_vector));
        let fbb_root = PrepareRequest::create(
            &mut fbb,
            &PrepareRequestArgs {
                request_id,
                transaction_id,
                range_id,
                has_reads,
                puts,
                deletes,
            },
        );
        fbb.finish(fbb_root, None);
        let (tx, rx) = oneshot::channel();
        self.record_outstanding_request(req_id, tx).await?;
        let prepare_request_bytes = Bytes::copy_from_slice(fbb.finished_data());
        let mut envelope_fbb = FlatBufferBuilder::new();
        let request_bytes = self.create_msg_envelope(
            &mut envelope_fbb,
            MessageType::Prepare,
            prepare_request_bytes,
        );
        self.fast_network
            .send(
                self.range_server_info.address,
                Bytes::copy_from_slice(request_bytes),
            )
            .unwrap();
        let response = rx.await.unwrap()?;
        let msg = response.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        match envelope.type_() {
            MessageType::Prepare => {
                let response_msg =
                    flatbuffers::root::<PrepareResponse>(envelope.bytes().unwrap().bytes())
                        .unwrap();
                let () = rangeserver::error::Error::from_flatbuf_status(response_msg.status())?;
                let epoch_lease = response_msg.epoch_lease().unwrap();
                return Ok(PrepareOk {
                    highest_known_epoch: response_msg.highest_known_epoch(),
                    epoch_lease: EpochLease {
                        lower_bound_inclusive: epoch_lease.lower_bound_inclusive(),
                        upper_bound_inclusive: epoch_lease.upper_bound_inclusive(),
                    },
                });
            }
            _ => return Err(RangeServerError::InvalidRequestFormat),
        }
    }

    pub async fn abort_transaction(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
    ) -> Result<(), RangeServerError> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        // TODO: too much copying :(
        let req_id = Uuid::new_v4();
        let mut fbb = FlatBufferBuilder::new();
        let transaction_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(tx.id),
        ));
        let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &range_id));
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(req_id),
        ));
        let fbb_root = AbortRequest::create(
            &mut fbb,
            &AbortRequestArgs {
                request_id,
                transaction_id,
                range_id,
            },
        );
        fbb.finish(fbb_root, None);
        let (tx, rx) = oneshot::channel();
        self.record_outstanding_request(req_id, tx).await?;
        let abort_request_bytes = Bytes::copy_from_slice(fbb.finished_data());
        let mut envelope_fbb = FlatBufferBuilder::new();
        let request_bytes =
            self.create_msg_envelope(&mut envelope_fbb, MessageType::Abort, abort_request_bytes);
        self.fast_network
            .send(
                self.range_server_info.address,
                Bytes::copy_from_slice(request_bytes),
            )
            .unwrap();
        let response = rx.await.unwrap()?;
        let msg = response.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        match envelope.type_() {
            MessageType::Abort => {
                let response_msg =
                    flatbuffers::root::<AbortResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                let () = rangeserver::error::Error::from_flatbuf_status(response_msg.status())?;

                return Ok(());
            }
            _ => return Err(RangeServerError::InvalidRequestFormat),
        }
    }

    pub async fn commit_transaction(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        epoch: u64,
    ) -> Result<(), RangeServerError> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        // TODO: too much copying :(
        let req_id = Uuid::new_v4();
        let mut fbb = FlatBufferBuilder::new();
        let transaction_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(tx.id),
        ));
        let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &range_id));
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(req_id),
        ));
        let fbb_root = CommitRequest::create(
            &mut fbb,
            &CommitRequestArgs {
                request_id,
                transaction_id,
                range_id,
                epoch,
                vid: 0,
            },
        );
        fbb.finish(fbb_root, None);
        let (tx, rx) = oneshot::channel();
        self.record_outstanding_request(req_id, tx).await?;
        let commit_request_bytes = Bytes::copy_from_slice(fbb.finished_data());
        let mut envelope_fbb = FlatBufferBuilder::new();
        let request_bytes =
            self.create_msg_envelope(&mut envelope_fbb, MessageType::Commit, commit_request_bytes);
        self.fast_network
            .send(
                self.range_server_info.address,
                Bytes::copy_from_slice(request_bytes),
            )
            .unwrap();
        let response = rx.await.unwrap()?;
        let msg = response.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        match envelope.type_() {
            MessageType::Commit => {
                let response_msg =
                    flatbuffers::root::<CommitResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                let () = rangeserver::error::Error::from_flatbuf_status(response_msg.status())?;
                return Ok(());
            }
            _ => return Err(RangeServerError::InvalidRequestFormat),
        }
    }

    fn get_request_id_from_response(msg: Bytes) -> Uuid {
        let msg = msg.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        let req_id = match envelope.type_() {
            MessageType::Get => {
                let msg =
                    flatbuffers::root::<GetResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                msg.request_id()
            }
            MessageType::Prepare => {
                let msg = flatbuffers::root::<PrepareResponse>(envelope.bytes().unwrap().bytes())
                    .unwrap();
                msg.request_id()
            }
            MessageType::Abort => {
                let msg =
                    flatbuffers::root::<AbortResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                msg.request_id()
            }
            MessageType::Commit => {
                let msg =
                    flatbuffers::root::<CommitResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                msg.request_id()
            }
            _ => panic!("unknown response message type"), // TODO: return and log unknown message type error.
        };
        common::util::flatbuf::deserialize_uuid(req_id.unwrap())
    }

    async fn network_loop(client: Arc<Self>, cancellation_token: CancellationToken) {
        let mut network_receiver = client
            .fast_network
            .register(client.range_server_info.address);
        loop {
            let () = tokio::select! {
                () = cancellation_token.cancelled() => {
                    // TODO(tamer): remove from fast network as well.
                    client.close().await;
                    return ()
                }
                maybe_message = network_receiver.recv() => {
                    match maybe_message {
                        None => {
                            println!("fast network closed unexpectedly!");
                            cancellation_token.cancel()
                        }
                        Some(msg) => {
                            let req_id = Self::get_request_id_from_response(msg.clone());
                            let mut state = client.state.write().await;
                            let outstanding_requests = match state.deref_mut() {
                                State::NotStarted | State::Stopped => {
                                    cancellation_token.cancel();
                                    continue
                                }
                                State::Started(started_state) => &mut started_state.outstanding_requests,
                            };
                            let sender = outstanding_requests.remove(&req_id).unwrap();
                            sender.send(Ok(msg)).unwrap()
                        }
                    }
                }
            };
        }
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

    async fn record_outstanding_request(
        &self,
        req_id: Uuid,
        tx: oneshot::Sender<Result<Bytes, RangeServerError>>,
    ) -> Result<(), RangeServerError> {
        {
            let mut state = self.state.write().await;
            let outstanding_requests = match state.deref_mut() {
                State::NotStarted | State::Stopped => {
                    return Err(RangeServerError::ConnectionClosed)
                }
                State::Started(started_state) => &mut started_state.outstanding_requests,
            };
            outstanding_requests.insert(req_id, tx);
            Ok(())
        }
    }

    async fn close(&self) {
        let mut outstanding_requests = {
            let mut state = self.state.write().await;
            let old_state = std::mem::replace(state.deref_mut(), State::Stopped);
            match old_state {
                State::NotStarted | State::Stopped => HashMap::new(),
                State::Started(started_state) => started_state.outstanding_requests,
            }
        };
        for (_, tx) in outstanding_requests.drain().take(1) {
            tx.send(Err(RangeServerError::ConnectionClosed)).unwrap();
        }
    }

    pub async fn prefetch(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        keys: Vec<Bytes>,
    ) -> Result<(), RangeServerError> {
        let mut client = match &self.proto_client {
            None => return Ok(()),
            Some(proto_client) => (**proto_client).clone(),
        };
        // Create a PrefetchRequest
        let transaction_id = tx.id.to_string();
        let keyspace_id = range_id.keyspace_id.id.to_string();
        let range_id = range_id.range_id.to_string();

        let range = RangeId {
            keyspace_id,
            range_id,
        };

        let range_keys: Vec<RangeKey> = keys
            .into_iter()
            .map(|key| RangeKey {
                range: Some(range.clone()),
                key: key.to_vec(),
            })
            .collect();

        let request = PrefetchRequest {
            transaction_id,
            range_key: range_keys,
        };

        // Send the request
        match client.prefetch(Request::new(request)).await {
            Ok(response) => {
                println!("RESPONSE={:?}", response);
                Ok(())
            }
            Err(e) => {
                println!("Failed prefetch: {:?}", e);
                Err(RangeServerError::PrefetchError)
            }
        }
    }
}
