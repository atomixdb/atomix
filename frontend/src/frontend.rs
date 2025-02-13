use std::sync::Arc;

use common::{
    config::Config, network::fast_network::FastNetwork, region::Zone,
    transaction_info::TransactionInfo,
};
use std::collections::HashMap;
use uuid::Uuid;

use coordinator::{coordinator::Coordinator, keyspace::Keyspace, transaction::Transaction};
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};

use std::net::ToSocketAddrs;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use tracing::instrument;

use proto::frontend::frontend_server::{Frontend, FrontendServer};
use proto::frontend::{
    AbortRequest, AbortResponse, CommitRequest, CommitResponse, DeleteRequest, DeleteResponse,
    GetRequest, GetResponse, PutRequest, PutResponse, StartTransactionRequest,
    StartTransactionResponse,
};
use proto::universe::universe_client::UniverseClient;
use proto::universe::{CreateKeyspaceRequest, CreateKeyspaceResponse};

use crate::range_assignment_oracle::RangeAssignmentOracle;
use chrono::Utc;

#[derive(Clone)]
struct ProtoServer {
    parent_server: Arc<Server>,
}

#[tonic::async_trait]
impl Frontend for ProtoServer {
    //  Creates a new keyspace in the universe
    ///
    /// # Arguments
    /// * `request` - The gRPC request containing the keyspace creation parameters
    ///
    /// # Returns
    /// * `Result<Response<CreateKeyspaceResponse>, TStatus>` - A response containing:
    ///   - status: Success message
    ///   - keyspace_id: UUID of the newly created keyspace
    #[instrument(skip(self))]
    async fn create_keyspace(
        &self,
        request: Request<CreateKeyspaceRequest>,
    ) -> Result<Response<CreateKeyspaceResponse>, TStatus> {
        println!("Got a request: {:?}", request);

        let proto_server_addr = &self.parent_server.config.universe.proto_server_addr;
        println!("Connecting to proto server at: {}", proto_server_addr);
        let mut client = UniverseClient::connect(format!("http://{}", proto_server_addr))
            .await
            .map_err(|e| TStatus::internal(format!("Failed to connect to universe: {:?}", e)))?;
        let keyspace_id = client
            .create_keyspace(request)
            .await
            .unwrap()
            .into_inner()
            .keyspace_id;
        Ok(Response::new(CreateKeyspaceResponse {
            // status: "Create keyspace request processed succe!ssfully".to_string(),
            keyspace_id: keyspace_id.to_string(),
        }))
    }

    /// Initiates a new transaction in the system
    ///
    /// # Arguments
    /// * `request` - The gRPC request containing transaction initialization parameters
    ///
    /// # Returns
    /// * `Result<Response<StartTransactionResponse>, TStatus>` - A response containing:
    ///   - status: Success message
    ///   - transaction_id: UUID of the newly created transaction
    #[instrument(skip(self))]
    async fn start_transaction(
        &self,
        request: Request<StartTransactionRequest>,
    ) -> Result<Response<StartTransactionResponse>, TStatus> {
        println!("Got a request: {:?}", request);

        // Generate a new transaction id
        let transaction_id = Uuid::new_v4();
        let transaction_info = Arc::new(TransactionInfo {
            id: transaction_id,
            started: Utc::now(),
            overall_timeout: self
                .parent_server
                .config
                .frontend
                .transaction_overall_timeout,
        });

        let transaction = self
            .parent_server
            .coordinator
            .start_transaction(transaction_info)
            .await;
        self.parent_server
            .transaction_table
            .write()
            .await
            .insert(transaction_id, Arc::new(Mutex::new(transaction)));

        println!("Transaction started: {:?}", transaction_id);

        Ok(Response::new(StartTransactionResponse {
            status: "Start transaction request processed successfully".to_string(),
            transaction_id: transaction_id.to_string(),
        }))
    }

    /// Retrieves a value for a given key in a given keyspace
    ///
    /// # Arguments
    /// * `request` - Contains transaction_id, keyspace, and key
    ///
    /// # Returns
    /// * `Result<Response<GetResponse>, TStatus>` - A response containing:
    ///   - status: Success message
    ///   - value: Value of the key if found, or None if not found
    #[instrument(skip(self))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, TStatus> {
        let req = request.get_ref();

        // Parse the transaction ID
        let transaction_id = Uuid::parse_str(&req.transaction_id).map_err(|e| {
            TStatus::invalid_argument(format!("Invalid transaction ID format: {}", e))
        })?;

        let keyspace_proto = req
            .keyspace
            .as_ref()
            .ok_or_else(|| TStatus::invalid_argument("Missing keyspace"))?;
        let keyspace = Keyspace {
            namespace: keyspace_proto.namespace.clone(),
            name: keyspace_proto.name.clone(),
        };
        let key = bytes::Bytes::copy_from_slice(&req.key);

        // Get the transaction
        let tx_table = self.parent_server.transaction_table.read().await;
        let transaction = tx_table
            .get(&transaction_id)
            .ok_or_else(|| TStatus::not_found("Transaction not found"))?;

        let mut tx = transaction.lock().await;
        let result = tx
            .get(&keyspace, key)
            .await
            .map_err(|e| TStatus::internal(format!("Get operation failed: {:?}", e)))?;

        Ok(Response::new(GetResponse {
            status: "Get request processed successfully".to_string(),
            value: result.map(|v| v.to_vec()),
        }))
    }

    //  Puts a value into a keyspace
    ///
    /// # Arguments
    /// * `request` - Contains transaction_id, keyspace, key, and value
    ///
    /// # Returns
    /// * `Result<Response<PutResponse>, TStatus>` - A response containing:
    ///   - status: Success message
    #[instrument(skip(self))]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, TStatus> {
        let req = request.get_ref();

        let transaction_id = Uuid::parse_str(&req.transaction_id).map_err(|e| {
            TStatus::invalid_argument(format!("Invalid transaction ID format: {}", e))
        })?;
        let keyspace_proto = req
            .keyspace
            .as_ref()
            .ok_or_else(|| TStatus::invalid_argument("Missing keyspace"))?;
        let keyspace = Keyspace {
            namespace: keyspace_proto.namespace.clone(),
            name: keyspace_proto.name.clone(),
        };
        let key = bytes::Bytes::copy_from_slice(&req.key);
        let value = bytes::Bytes::copy_from_slice(&req.value);

        // Get the transaction
        let tx_table = self.parent_server.transaction_table.read().await;
        let transaction = tx_table
            .get(&transaction_id)
            .ok_or_else(|| TStatus::not_found("Transaction not found"))?;

        let mut tx = transaction.lock().await;
        tx.put(&keyspace, key, value)
            .await
            .map_err(|e| TStatus::internal(format!("Put operation failed: {:?}", e)))?;

        Ok(Response::new(PutResponse {
            status: "Put request processed successfully".to_string(),
        }))
    }

    /// Deletes a key in a given keyspace
    ///
    /// # Arguments
    /// * `request` - Contains transaction_id, keyspace, and key
    ///
    /// # Returns
    /// * `Result<Response<DeleteResponse>, TStatus>` - A response containing:
    ///   - status: Success message
    #[instrument(skip(self))]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, TStatus> {
        let req = request.get_ref();

        // Parse the transaction ID
        let transaction_id = Uuid::parse_str(&req.transaction_id).map_err(|e| {
            TStatus::invalid_argument(format!("Invalid transaction ID format: {}", e))
        })?;

        let keyspace_proto = req
            .keyspace
            .as_ref()
            .ok_or_else(|| TStatus::invalid_argument("Missing keyspace"))?;
        let keyspace = Keyspace {
            namespace: keyspace_proto.namespace.clone(),
            name: keyspace_proto.name.clone(),
        };
        let key = bytes::Bytes::copy_from_slice(&req.key);

        // Get the transaction
        let tx_table = self.parent_server.transaction_table.read().await;
        let transaction = tx_table
            .get(&transaction_id)
            .ok_or_else(|| TStatus::not_found("Transaction not found"))?;

        let mut tx = transaction.lock().await;
        tx.del(&keyspace, key)
            .await
            .map_err(|e| TStatus::internal(format!("Delete operation failed: {:?}", e)))?;

        Ok(Response::new(DeleteResponse {
            status: "Delete request processed successfully".to_string(),
        }))
    }

    /// Aborts a transaction
    ///
    /// # Arguments
    /// * `request` - Contains transaction_id
    ///
    /// # Returns
    /// * `Result<Response<AbortResponse>, TStatus>` - A response containing:
    ///   - status: Success message
    #[instrument(skip(self))]
    async fn abort(
        &self,
        request: Request<AbortRequest>,
    ) -> Result<Response<AbortResponse>, TStatus> {
        let req = request.get_ref();

        // Parse the transaction ID
        let transaction_id = Uuid::parse_str(&req.transaction_id).map_err(|e| {
            TStatus::invalid_argument(format!("Invalid transaction ID format: {}", e))
        })?;

        // Get the transaction
        {
            let tx_table = self.parent_server.transaction_table.read().await;
            let transaction = tx_table
                .get(&transaction_id)
                .ok_or_else(|| TStatus::not_found("Transaction not found"))?;

            let mut tx = transaction.lock().await;
            tx.abort()
                .await
                .map_err(|e| TStatus::internal(format!("Abort operation failed: {:?}", e)))?;
        }
        // Remove the transaction from the transaction table
        self.parent_server
            .transaction_table
            .write()
            .await
            .remove(&transaction_id);

        Ok(Response::new(AbortResponse {
            status: "Abort request processed successfully".to_string(),
        }))
    }

    //  Commits a transaction
    ///
    /// # Arguments
    /// * `request` - Contains transaction_id
    ///
    /// # Returns
    /// * `Result<Response<CommitResponse>, TStatus>` - A response containing:
    ///   - status: Success message
    #[instrument(skip(self))]
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, TStatus> {
        let req = request.get_ref();

        let transaction_id = Uuid::parse_str(&req.transaction_id).map_err(|e| {
            TStatus::invalid_argument(format!("Invalid transaction ID format: {}", e))
        })?;

        // Get the transaction
        {
            let tx_table = self.parent_server.transaction_table.read().await;
            let transaction = tx_table
                .get(&transaction_id)
                .ok_or_else(|| TStatus::not_found("Transaction not found"))?;

            let mut tx = transaction.lock().await;
            tx.commit()
                .await
                .map_err(|e| TStatus::internal(format!("Commit operation failed: {:?}", e)))?;
        }
        // Remove the transaction from the transaction table
        self.parent_server
            .transaction_table
            .write()
            .await
            .remove(&transaction_id);
        Ok(Response::new(CommitResponse {
            status: "Commit request processed successfully".to_string(),
        }))
    }
}

// Implementation of the Frontend service
pub struct Server {
    config: Config,
    coordinator: Coordinator,
    //  Keeping track of transactions that haven't committed yet
    transaction_table: RwLock<HashMap<Uuid, Arc<Mutex<Transaction>>>>,
    bg_runtime: tokio::runtime::Handle,
}
// TODO: add a trait for Frontend?
impl Server {
    pub async fn new(
        config: Config,
        zone: Zone,
        fast_network: Arc<dyn FastNetwork>,
        range_assignment_oracle: Arc<RangeAssignmentOracle>,
        runtime: tokio::runtime::Handle,
        bg_runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
    ) -> Arc<Self> {
        let coordinator = Coordinator::new(
            &config,
            zone,
            range_assignment_oracle,
            fast_network.clone(),
            runtime.clone(),
            bg_runtime.clone(),
            cancellation_token,
        )
        .await;

        Arc::new(Server {
            config,
            coordinator,
            transaction_table: RwLock::new(HashMap::new()),
            bg_runtime,
        })
    }

    pub async fn start(server: Arc<Self>) {
        let proto_server = ProtoServer {
            parent_server: server.clone(),
        };

        let addr = server
            .config
            .frontend
            .proto_server_addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();

        server.bg_runtime.spawn(async move {
            if let Err(e) = TServer::builder()
                .add_service(FrontendServer::new(proto_server))
                .serve(addr)
                .await
            {
                panic!("Unable to start proto server: {:?}", e);
            }
        });
    }
}
