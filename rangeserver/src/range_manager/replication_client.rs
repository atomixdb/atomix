use common::config::HostPort;
use common::full_range_id::FullRangeId;
use common::util::flatbuf::deserialize_uuid;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use proto::rangeserver::range_server_client::RangeServerClient;
use proto::rangeserver::replicate_request;
use proto::rangeserver::replicate_response;
use proto::rangeserver::RangeId;
use proto::rangeserver::ReplicateDataRequest;
use proto::rangeserver::ReplicateInitRequest;
use proto::rangeserver::ReplicateRequest;
use proto::rangeserver::ReplicateResponse;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::error::Error;

struct RunningState {
    last_acknowledged_offset: Option<u64>,
}

enum State {
    NotStarted,
    Running(RunningState),
    Stopped,
}

pub struct ReplicationClient {
    secondary_address: HostPort,
    secondary_range_id: FullRangeId,
    update_recv: tokio::sync::mpsc::Receiver<ReplicateDataRequest>,
    state: Arc<RwLock<State>>,
}

pub struct ReplicationClientHandle {
    state: Arc<RwLock<State>>,
    update_send: tokio::sync::mpsc::Sender<ReplicateDataRequest>,
}

impl ReplicationClientHandle {
    pub async fn queue_update(
        &self,
        wal_offset: u64,
        commit_epoch: u64,
        prepare: &PrepareRequest<'_>,
    ) -> Result<(), Error> {
        let puts = prepare.puts().map_or(vec![], |puts| {
            puts.iter()
                .map(|record| proto::rangeserver::Record {
                    key: record.key().unwrap().k().unwrap().bytes().to_vec(),
                    value: record.value().unwrap().bytes().to_vec(),
                })
                .collect()
        });

        let transaction_id = prepare
            .transaction_id()
            .map(|uuid| deserialize_uuid(uuid).to_string())
            .unwrap();

        let deletes = prepare.deletes().map_or(vec![], |deletes| {
            deletes
                .iter()
                .map(|key| key.k().unwrap().bytes().to_vec())
                .collect()
        });

        let data = ReplicateDataRequest {
            deletes: deletes,
            has_reads: prepare.has_reads(),
            puts,
            transaction_id,
            primary_wal_offset: wal_offset,
            epoch: commit_epoch,
        };
        self.update_send.send(data).await.map_err(|e| {
            error!("Failed to send update to replication client: {:#?}", e);
            Error::InternalError(Arc::new(e))
        })?;

        debug!(
            "Successfully queued update with primary wal_offset {}",
            wal_offset
        );
        Ok(())
    }
}

impl ReplicationClient {
    pub fn new(
        secondary_address: HostPort,
        secondary_range_id: FullRangeId,
    ) -> (Self, ReplicationClientHandle) {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let state = Arc::new(RwLock::new(State::NotStarted));
        (
            Self {
                state: state.clone(),
                secondary_address,
                secondary_range_id,
                update_recv: rx,
            },
            ReplicationClientHandle {
                state: state,
                update_send: tx,
            },
        )
    }

    /// Serves the replication stream, sending updates to the secondary range.
    pub async fn serve(&mut self) -> Result<(), Error> {
        info!(
            "Starting replication client for primary range {} to secondary range {}",
            self.secondary_range_id.range_id, self.secondary_range_id.range_id
        );
        let rs_client = RangeServerClient::connect(format!(
            "http://{}:{}",
            self.secondary_address.host, self.secondary_address.port
        ))
        .await
        .map_err(|e| {
            error!(
                "Failed to connect to secondary range server at {}:{}: {:#?}",
                self.secondary_address.host, self.secondary_address.port, e
            );
            Error::InternalError(Arc::new(e))
        })?;

        info!("Successfully connected to secondary range server");

        {
            let mut state = self.state.write().await;
            *state = State::Running(RunningState {
                last_acknowledged_offset: Some(0),
            });
        }
        let result = self.serve_inner(rs_client).await;
        {
            let mut state = self.state.write().await;
            *state = State::Stopped;
        }
        match result {
            Ok(()) => {
                info!("Replication client stopped gracefully");
                Ok(())
            }
            Err(e) => {
                error!(
                    "Replication client failed for keyspace_id {} and range_id {}: {:?}",
                    self.secondary_range_id.keyspace_id.id, self.secondary_range_id.range_id, e
                );
                Err(e)
            }
        }
    }

    // TODO(yanniszark): Implement serve
    async fn serve_inner(
        &mut self,
        mut rs_client: RangeServerClient<Channel>,
    ) -> Result<(), Error> {
        info!("Starting replication client inner loop");
        // Find where the replication should start from (should the secondary send that?)
        // First send the init request
        // TODO(yanniszark): Make channel size configurable
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let send_stream = tx;
        // Send init request
        let init_req = ReplicateRequest {
            request: Some(replicate_request::Request::Init(ReplicateInitRequest {
                range: Some(RangeId {
                    keyspace_id: self.secondary_range_id.keyspace_id.id.to_string(),
                    range_id: self.secondary_range_id.range_id.to_string(),
                }),
            })),
        };
        // Put the init request in the channel before creating the receive
        // stream. Otherwise, the server will not be able to send the init
        // response.
        send_stream
            .send(init_req)
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?;

        let mut recv_stream = rs_client
            .replicate(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?
            .into_inner();

        // Verify init response
        match recv_stream.message().await {
            Ok(Some(response)) => {
                // Assert that we got a ReplicateInitResponse
                let response = response.response;
                match response {
                    Some(replicate_response::Response::Init(init_resp)) => {
                        // Got init response, can proceed
                        if !init_resp.success {
                            return Err(Error::internal_error_from_string(
                                "ReplicationInitResponse returned success=false",
                            ));
                        }
                    }
                    _ => return Err(Error::internal_error_from_string("Expected Init response")),
                }
            }
            Ok(None) => {
                return Err(Error::ConnectionClosed);
            }
            Err(e) => {
                return Err(Error::InternalError(Arc::new(e)));
            }
        };

        // Start sending data.
        info!("Starting data sending loop");
        loop {
            tokio::select! {
                // Replicate the next prepare request
                update = self.update_recv.recv() => {
                    debug!("Sending update to secondary range");
                    Self::process_data(&send_stream, update).await?;
                }

                // Process messages from the server
                msg = recv_stream.message() => {
                    debug!("Processing response from secondary range");
                    self.process_response(msg).await?;
                }
            }
        }
    }

    async fn process_data(
        send_stream: &tokio::sync::mpsc::Sender<ReplicateRequest>,
        update: Option<ReplicateDataRequest>,
    ) -> Result<(), Error> {
        match update {
            Some(data_req) => {
                let req = ReplicateRequest {
                    request: Some(replicate_request::Request::Data(data_req)),
                };
                send_stream
                    .send(req)
                    .await
                    .map_err(|e| Error::InternalError(Arc::new(e)))?;
                Ok(())
            }
            None => {
                // Channel closed, replication stream ended
                // TODO(yannisark): Retry
                Err(Error::ConnectionClosed)
            }
        }
    }

    async fn process_response(
        &self,
        msg: Result<Option<ReplicateResponse>, tonic::Status>,
    ) -> Result<(), Error> {
        match msg {
            Ok(Some(repl_response)) => {
                // Process response if needed
                match repl_response.response {
                    Some(replicate_response::Response::Data(data_resp)) => {
                        let mut state = self.state.write().await;
                        let State::Running(ref mut s) = *state else {
                            unreachable!()
                        };
                        s.last_acknowledged_offset = Some(data_resp.acked_wal_offset);
                        Ok(())
                    }
                    _ => {
                        // Unexpected response type, log and return error
                        Err(Error::internal_error_from_string(
                            "Unexpected replication response type",
                        ))
                    }
                }
            }
            Ok(None) => Err(Error::ConnectionClosed),
            Err(e) => Err(Error::InternalError(Arc::new(e))),
        }
    }
}
