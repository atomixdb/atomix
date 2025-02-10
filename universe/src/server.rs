use std::sync::Arc;

use proto::universe::universe_server::Universe;
use proto::universe::{
    CreateKeyspaceRequest, CreateKeyspaceResponse, GetKeyspaceInfoRequest, GetKeyspaceInfoResponse,
    ListKeyspacesRequest, ListKeyspacesResponse,
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};
use uuid::Uuid;

use crate::storage::{KeyspaceInfoSearchField, Storage};

/// Implementation of the Universe manager.
pub struct UniverseServer<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> UniverseServer<S> {
    /// Creates a new UniverseServer.
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[tonic::async_trait]
impl<S: Storage> Universe for UniverseServer<S> {
    #[instrument(skip(self))]
    async fn create_keyspace(
        &self,
        request: Request<CreateKeyspaceRequest>,
    ) -> Result<Response<CreateKeyspaceResponse>, Status> {
        info!("Got a create_keyspace request: {:?}", request);

        let req_inner = request.into_inner();
        // TODO: Validate the base key ranges. Must be non-overlapping and
        // cover the entire key space.

        let base_key_ranges: Vec<proto::universe::KeyRange> = req_inner
            .base_key_ranges
            .into_iter()
            .map(|kr| proto::universe::KeyRange {
                base_range_uuid: Uuid::new_v4().to_string(),
                lower_bound_inclusive: kr.lower_bound_inclusive,
                upper_bound_exclusive: kr.upper_bound_exclusive,
            })
            .collect();

        // If the base_key_ranges are empty, create a single range that covers
        // the entire key space.
        let base_key_ranges = if base_key_ranges.is_empty() {
            vec![proto::universe::KeyRange {
                base_range_uuid: Uuid::new_v4().to_string(),
                lower_bound_inclusive: vec![],
                upper_bound_exclusive: vec![],
            }]
        } else {
            base_key_ranges
        };

        let keyspace_id = self
            .storage
            .create_keyspace(
                Uuid::new_v4().to_string().as_str(),
                &req_inner.name,
                &req_inner.namespace,
                req_inner.primary_zone.unwrap(),
                base_key_ranges,
            )
            .await
            .map_err(|e| Status::internal(format!("Failed to create keyspace: {}", e)))?;

        let response = CreateKeyspaceResponse { keyspace_id };
        Ok(Response::new(response))
    }

    #[instrument(skip(self))]
    async fn list_keyspaces(
        &self,
        request: Request<ListKeyspacesRequest>,
    ) -> Result<Response<ListKeyspacesResponse>, Status> {
        debug!("Got a list_keyspaces request: {:?}", request);

        let keyspaces = self
            .storage
            .list_keyspaces(request.into_inner().region)
            .await
            .map_err(|e| Status::internal(format!("Failed to list keyspaces: {}", e)))?;

        let response = ListKeyspacesResponse { keyspaces };
        Ok(Response::new(response))
    }

    #[instrument(skip(self))]
    async fn get_keyspace_info(
        &self,
        request: Request<GetKeyspaceInfoRequest>,
    ) -> Result<Response<GetKeyspaceInfoResponse>, Status> {
        info!("Got a get_keyspace_info request: {:?}", request);

        let req_inner = request.into_inner();
        let keyspace_info_search_field =
            KeyspaceInfoSearchField::from(req_inner.keyspace_info_search_field.unwrap());
        let keyspace_info = self
            .storage
            .get_keyspace_info(keyspace_info_search_field)
            .await
            .map_err(|e| Status::internal(format!("Failed to get keyspace info: {}", e)))?;

        let response = GetKeyspaceInfoResponse {
            keyspace_info: Some(keyspace_info),
        };
        Ok(Response::new(response))
    }
}

/// Runs the Universe Manager, listening on the provided address.
///
/// # Arguments
/// - `addr`: The address for the Universe server to listen on.
///
/// # Errors
/// This function will return an error if the provided address is invalid or if there is an issue
/// starting the Universe server.
pub async fn run_universe_server<S: Storage>(
    addr: impl AsRef<str> + std::net::ToSocketAddrs,
    storage: S,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.to_socket_addrs()?.next().ok_or("Invalid address")?;

    info!("UniverseServer listening on {}", addr);

    let universe_server = UniverseServer::new(Arc::new(storage));

    tonic::transport::Server::builder()
        .add_service(proto::universe::universe_server::UniverseServer::new(
            universe_server,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
