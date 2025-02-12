use bytes::Bytes;
use common::config::Config;
use common::{
    network::fast_network::FastNetwork, network::for_testing::udp_fast_network::UdpFastNetwork,
    region::Zone, util,
};
use epoch_reader::reader::EpochReader;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use flatbuffers::FlatBufferBuilder;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::{oneshot, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;
type DynamicErr = Box<dyn std::error::Error + Sync + Send + 'static>;

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub struct MockRangeServer {
    epoch_reader: Arc<EpochReader>,
    state: Arc<RangeServerState>,
}

struct RangeServerState {
    // Map of key -> value
    data: Arc<RwLock<HashMap<Bytes, Bytes>>>,
    // Assuming we have only one range in the range server for simplicity
    pending_prepare_records: Arc<Mutex<HashMap<Uuid, Bytes>>>,
}
// Mock Range Server:
// 1) Maintains one range which we assume will always cover the requested keyspace/key
// 2) Listens for prepare/get/commit/abort requests
impl MockRangeServer {
    async fn handle_get(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: GetRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let mut fbb = FlatBufferBuilder::new();
        let request_id = request.request_id().unwrap();
        let request_id = util::flatbuf::deserialize_uuid(request_id);
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(request_id),
        ));
        let data = self.state.data.read().await;

        let mut records = Vec::new();
        for key in request.keys().iter() {
            for k in key.iter() {
                let key_bytes = Bytes::copy_from_slice(k.k().unwrap().bytes());
                let value = data.get(&key_bytes).cloned();

                let k = Some(fbb.create_vector(key_bytes.as_ref()));
                let key = Key::create(&mut fbb, &KeyArgs { k });
                let value = value.map(|v| fbb.create_vector(v.as_ref()));

                records.push(Record::create(
                    &mut fbb,
                    &RecordArgs {
                        key: Some(key),
                        value,
                    },
                ));
            }
        }
        let records = Some(fbb.create_vector(&records));
        let response = GetResponse::create(
            &mut fbb,
            &GetResponseArgs {
                request_id,
                status: Status::Ok,
                records,
                leader_sequence_number: 1,
            },
        );

        fbb.finish(response, None);
        self.send_response(network, sender, MessageType::Get, fbb.finished_data())?;
        Ok(())
    }

    async fn handle_commit(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: CommitRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let mut fbb = FlatBufferBuilder::new();
        let req_id = request.request_id().unwrap();
        let request_id = util::flatbuf::deserialize_uuid(req_id);
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(request_id),
        ));
        let transaction_id = match request.transaction_id() {
            None => panic!("Transaction ID is required"),
            Some(id) => util::flatbuf::deserialize_uuid(id),
        };
        let prepare_record_bytes = {
            let mut pending_prepare_records = self.state.pending_prepare_records.lock().unwrap();
            pending_prepare_records
                .remove(&transaction_id)
                .unwrap()
                .clone()
        };
        let prepare_record = flatbuffers::root::<PrepareRequest>(prepare_record_bytes.as_ref())?;
        for put in prepare_record.puts().iter() {
            for put in put.iter() {
                let key = Bytes::copy_from_slice(put.key().unwrap().k().unwrap().bytes());
                let val = Bytes::copy_from_slice(put.value().unwrap().bytes());
                let mut data = self.state.data.write().await;
                data.insert(key, val);
            }
        }
        for del in prepare_record.deletes().iter() {
            for del in del.iter() {
                let key = Bytes::copy_from_slice(del.k().unwrap().bytes());
                let mut data = self.state.data.write().await;
                data.remove(&key);
            }
        }
        let commit_response = CommitResponse::create(
            &mut fbb,
            &CommitResponseArgs {
                request_id: request_id,
                status: Status::Ok,
            },
        );
        fbb.finish(commit_response, None);
        self.send_response(network, sender, MessageType::Commit, fbb.finished_data())?;
        Ok(())
    }
    async fn handle_prepare(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: PrepareRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let transaction_id = match request.transaction_id() {
            None => panic!("Transaction ID is required"),
            Some(id) => util::flatbuf::deserialize_uuid(id),
        };

        let mut fbb = FlatBufferBuilder::new();
        let req_id = request.request_id().unwrap();
        let request_id = util::flatbuf::deserialize_uuid(req_id);
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(request_id),
        ));
        let epoch = self.epoch_reader.read_epoch().await.unwrap();
        let epoch_lease = Some(EpochLease::create(
            &mut fbb,
            &EpochLeaseArgs {
                lower_bound_inclusive: epoch,
                upper_bound_inclusive: epoch + 1,
            },
        ));
        let prepare_response = PrepareResponse::create(
            &mut fbb,
            &PrepareResponseArgs {
                request_id: request_id,
                status: Status::Ok,
                epoch_lease: epoch_lease,
                highest_known_epoch: epoch,
            },
        );

        let mut pending_prepare_records = self.state.pending_prepare_records.lock().unwrap();
        pending_prepare_records.insert(transaction_id, Bytes::copy_from_slice(request._tab.buf()));

        fbb.finish(prepare_response, None);
        self.send_response(network, sender, MessageType::Prepare, fbb.finished_data())?;
        Ok(())
    }

    async fn handle_abort(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: AbortRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let mut fbb = FlatBufferBuilder::new();
        let req_id = request.request_id().unwrap();
        let request_id = util::flatbuf::deserialize_uuid(req_id);
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(request_id),
        ));
        let transaction_id = match request.transaction_id() {
            None => panic!("Transaction ID is required"),
            Some(id) => util::flatbuf::deserialize_uuid(id),
        };
        let mut pending_prepare_records = self.state.pending_prepare_records.lock().unwrap();
        pending_prepare_records.remove(&transaction_id);
        let abort_response = AbortResponse::create(
            &mut fbb,
            &AbortResponseArgs {
                request_id: request_id,
                status: Status::Ok,
            },
        );
        fbb.finish(abort_response, None);
        self.send_response(network, sender, MessageType::Abort, fbb.finished_data())?;
        Ok(())
    }

    fn send_response(
        &self,
        fast_network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        msg_type: MessageType,
        msg_payload: &[u8],
    ) -> Result<(), std::io::Error> {
        let mut fbb = FlatBufferBuilder::new();
        let bytes = fbb.create_vector(msg_payload);
        let response = ResponseEnvelope::create(
            &mut fbb,
            &ResponseEnvelopeArgs {
                type_: msg_type,
                bytes: Some(bytes),
            },
        );
        fbb.finish(response, None);
        fast_network.send(sender, Bytes::copy_from_slice(fbb.finished_data()))
    }

    async fn handle_message(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        msg: Bytes,
    ) -> Result<(), DynamicErr> {
        let msg = msg.to_vec();
        let envelope = flatbuffers::root::<RequestEnvelope>(msg.as_slice())?;

        match envelope.type_() {
            MessageType::Get => {
                let req = flatbuffers::root::<GetRequest>(envelope.bytes().unwrap().bytes())?;
                server.handle_get(fast_network, sender, req).await?
            }
            MessageType::Prepare => {
                let req = flatbuffers::root::<PrepareRequest>(envelope.bytes().unwrap().bytes())?;
                server.handle_prepare(fast_network, sender, req).await?
            }
            MessageType::Commit => {
                let req = flatbuffers::root::<CommitRequest>(envelope.bytes().unwrap().bytes())?;
                server.handle_commit(fast_network, sender, req).await?
            }
            MessageType::Abort => {
                let req = flatbuffers::root::<AbortRequest>(envelope.bytes().unwrap().bytes())?;
                server.handle_abort(fast_network, sender, req).await?
            }
            _ => error!("Received unknown message type: {:?}", envelope.type_()),
        }
        Ok(())
    }

    pub async fn start(
        config: &Config,
        zone: Zone,
        fast_network: Arc<UdpFastNetwork>,
        cancellation_token: CancellationToken,
    ) -> Result<(), DynamicErr> {
        let region_config = config.regions.get(&zone.region).unwrap();
        let publisher_set = region_config
            .epoch_publishers
            .iter()
            .find(|&s| s.zone == zone)
            .unwrap();
        let epoch_reader = Arc::new(EpochReader::new(
            fast_network.clone(),
            RUNTIME.handle().clone(),
            RUNTIME.handle().clone(),
            publisher_set.clone(),
            cancellation_token.clone(),
        ));

        let server = Arc::new(MockRangeServer {
            epoch_reader,
            state: Arc::new(RangeServerState {
                data: Arc::new(RwLock::new(HashMap::new())),
                pending_prepare_records: Arc::new(Mutex::new(HashMap::new())),
            }),
        });

        //  Start polling the fast network
        let fast_network_clone = fast_network.clone();
        RUNTIME.spawn(async move {
            loop {
                fast_network_clone.poll();
                tokio::task::yield_now().await
            }
        });

        //  Start listening for messages
        let (listener_tx, listener_rx) = oneshot::channel();
        RUNTIME.spawn(async move {
            let mut network_receiver = fast_network.listen_default();
            info!("Range server listening");
            listener_tx.send(()).unwrap();

            loop {
                let () = tokio::select! {
                    () = cancellation_token.cancelled() => {
                        return
                    }
                    maybe_message = network_receiver.recv() => {
                        match maybe_message {
                            None => {
                                error!("Fast network closed unexpectedly!");
                                cancellation_token.cancel();
                            }
                            Some((sender, msg)) => {
                                let server = server.clone();
                                let fast_network = fast_network.clone();
                                RUNTIME.spawn(async move {
                                    if let Err(e) = Self::handle_message(server, fast_network, sender, msg).await {
                                        error!("Error handling message: {}", e);
                                    }
                                });
                            }
                        }
                    }
                };
            }
        });

        listener_rx.await.unwrap();
        Ok(())
    }
}
