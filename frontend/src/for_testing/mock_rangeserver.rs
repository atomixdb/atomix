// use bytes::Bytes;
// use common::{
//     full_range_id::FullRangeId,
//     transaction_info::TransactionInfo,
//     network::fast_network::FastNetwork,
// };
// use flatbuf::rangeserver_flatbuffers::range_server::*;
// use flatbuffers::FlatBufferBuilder;
// use std::{
//     collections::HashMap,
//     net::SocketAddr,
//     sync::Arc,
// };
// use tokio::sync::{mpsc, RwLock};
// use uuid::Uuid;

// pub struct MockRangeServer {
//     state: Arc<RangeServerState>,
//     address: SocketAddr,
// }

// struct RangeServerState {
//     // Map of key -> (value, epoch)
//     data: RwLock<HashMap<Bytes, (Bytes, u64)>>,
//     // Map of transaction_id -> prepared state
//     prepared_txns: RwLock<HashMap<Uuid, bool>>,
//     // Map of transaction_id -> committed state
//     committed_txns: RwLock<HashMap<Uuid, bool>>,
// }

// impl MockRangeServer {
//     pub fn new(address: SocketAddr) -> Self {
//         MockRangeServer {
//             state: Arc::new(RangeServerState {
//                 data: RwLock::new(HashMap::new()),
//                 prepared_txns: RwLock::new(HashMap::new()),
//                 committed_txns: RwLock::new(HashMap::new()),
//             }),
//             address,
//         }
//     }

//     pub async fn start(
//         self: Arc<Self>,
//         fast_network: Arc<dyn FastNetwork>,
//     ) -> mpsc::UnboundedReceiver<()> {
//         let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel();
//         let mut network_receiver = fast_network.listen_default();

//         tokio::spawn(async move {
//             loop {
//                 match network_receiver.recv().await {
//                     None => break,
//                     Some((sender, msg)) => {
//                         let msg = msg.to_vec();
//                         let envelope = flatbuffers::root::<RequestEnvelope>(&msg).unwrap();

//                         let response = match envelope.type_() {
//                             MessageType::Get => self.handle_get(envelope).await,
//                             MessageType::Prepare => self.handle_prepare(envelope).await,
//                             MessageType::Commit => self.handle_commit(envelope).await,
//                             MessageType::Abort => self.handle_abort(envelope).await,
//                             _ => continue,
//                         };

//                         let mut fbb = FlatBufferBuilder::new();
//                         let response_bytes = fbb.create_vector(&response);
//                         let response_root = ResponseEnvelope::create(
//                             &mut fbb,
//                             &ResponseEnvelopeArgs {
//                                 type_: envelope.type_(),
//                                 bytes: Some(response_bytes),
//                             },
//                         );
//                         fbb.finish(response_root, None);

//                         let _ = fast_network.send(
//                             sender,
//                             Bytes::copy_from_slice(fbb.finished_data()),
//                         );
//                     }
//                 }
//             }
//             let _ = shutdown_tx.send(());
//         });

//         shutdown_rx
//     }

//     async fn handle_get(&self, envelope: RequestEnvelope<'_>) -> Vec<u8> {
//         let request = flatbuffers::root::<GetRequest>(envelope.bytes().unwrap().bytes()).unwrap();
//         let key = Bytes::copy_from_slice(request.keys().unwrap().get(0).k().unwrap());

//         let mut fbb = FlatBufferBuilder::new();
//         let data = self.state.data.read().await;

//         let (response_bytes, epoch) = match data.get(&key) {
//             Some((value, epoch)) => {
//                 let value_bytes = fbb.create_vector(value);
//                 let record = Record::create(
//                     &mut fbb,
//                     &RecordArgs {
//                         key: None,
//                         value: Some(value_bytes),
//                     },
//                 );
//                 (Some(record), *epoch)
//             }
//             None => (None, 0),
//         };

//         let response = GetResponse::create(
//             &mut fbb,
//             &GetResponseArgs {
//                 request_id: request.request_id(),
//                 status: None,
//                 records: response_bytes.map(|r| fbb.create_vector(&[r])),
//                 leader_sequence_number: 0,
//             },
//         );
//         fbb.finish(response, None);
//         fbb.finished_data().to_vec()
//     }

//     // Additional handler implementations for prepare, commit, and abort...
// }

//     // let identity: String = "test_server".into();
//     // let host_info = HostInfo {
//     //     identity: HostIdentity {
//     //         name: identity.clone(),
//     //         zone,
//     //     },
//     //     address: "127.0.0.1:50054".parse().unwrap(),
//     //     warden_connection_epoch: epoch_supplier.read_epoch().await.unwrap(),
//     // };
