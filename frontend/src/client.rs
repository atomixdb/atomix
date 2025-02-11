use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

use common::{key_range::KeyRange, region::Zone};
use tonic::transport::Channel;

use coordinator::keyspace::Keyspace;
use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::{GetRequest, Keyspace as ProtoKeyspace, PutRequest, StartTransactionRequest};

use proto::universe::{CreateKeyspaceRequest, KeyRangeRequest};

use crate::error::Error;

/// Client for interacting with the Frontend service
pub struct Client {
    proto_client: FrontendClient<Channel>,
}

impl Client {
    /// Creates a new Frontend client connected to the specified address
    ///
    /// # Arguments
    /// * `addr` - Address of the Frontend server (e.g. "127.0.0.1:50057")
    ///
    /// # Returns a new Frontend client
    pub async fn new(addr: String) -> Result<Arc<Self>, Error> {
        let proto_client = FrontendClient::connect(format!("http://{}", addr))
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?;

        Ok(Arc::new(Client { proto_client }))
    }

    //  Creates a new keyspace
    /// # Arguments
    /// * `keyspace` - Keyspace to create
    /// * `zone` - Zone of the keyspace
    /// * `base_key_ranges` - Base key ranges of the keyspace
    /// # Returns
    /// * Transaction ID that can be used for subsequent operations
    pub async fn create_keyspace(
        &mut self,
        keyspace: &Keyspace,
        zone: Zone,
        base_key_ranges: Vec<KeyRange>,
    ) -> Result<String, Error> {
        println!("Creating keyspace");
        let key_range_reqs: Vec<KeyRangeRequest> = base_key_ranges
            .into_iter()
            .map(|k| KeyRangeRequest {
                lower_bound_inclusive: k
                    .lower_bound_inclusive
                    .map_or_else(|| vec![], |b| b.to_vec()),
                upper_bound_exclusive: k
                    .upper_bound_exclusive
                    .map_or_else(|| vec![], |b| b.to_vec()),
            })
            .collect();

        let keyspace_req = CreateKeyspaceRequest {
            namespace: keyspace.namespace.clone(),
            name: keyspace.name.clone(),
            primary_zone: Some(zone.into()),
            base_key_ranges: key_range_reqs,
        };

        let response = self
            .proto_client
            .create_keyspace(keyspace_req)
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?;

        let keyspace_id = response.get_ref().keyspace_id.clone();
        println!("Created keyspace with ID: {:?}", keyspace_id);
        Ok(keyspace_id)
    }

    /// Starts a new transaction
    /// # Returns
    /// * Transaction ID that can be used for subsequent operations
    pub async fn start_transaction(&mut self) -> Result<Uuid, Error> {
        println!("Starting transaction");
        let response = self
            .proto_client
            .start_transaction(StartTransactionRequest {})
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?;

        let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id)
            .map_err(|_| Error::InvalidRequestFormat)?;
        println!("Started transaction with ID: {:?}", transaction_id);
        Ok(transaction_id)
    }

    //  Puts a value into a keyspace
    /// # Arguments
    /// * `transaction_id` - ID of the current transaction
    /// * `keyspace` - Keyspace containing the namespace and name
    /// * `key` - Key to put the value into
    /// * `value` - Value to put into the key
    /// # Returns
    /// * `()` - Empty response
    pub async fn put(
        &mut self,
        transaction_id: Uuid,
        keyspace: &Keyspace,
        key: &Bytes,
        value: &Bytes,
    ) -> Result<(), Error> {
        println!("Putting key-value pair into keyspace");
        self.proto_client
            .put(PutRequest {
                transaction_id: transaction_id.to_string(),
                keyspace: Some(ProtoKeyspace {
                    namespace: keyspace.namespace.clone(),
                    name: keyspace.name.clone(),
                }),
                key: key.to_vec(),
                value: value.to_vec(),
            })
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?;
        Ok(())
    }

    /// Gets a value for a key in a keyspace
    ///
    /// # Arguments
    /// * `transaction_id` - ID of the current transaction
    /// * `keyspace` - Keyspace containing the namespace and name
    /// * `key` - Key to look up
    ///
    /// # Returns
    /// * The value if found, None if not found
    pub async fn get(
        &mut self,
        transaction_id: Uuid,
        keyspace: &Keyspace,
        key: Bytes,
    ) -> Result<Option<Bytes>, Error> {
        println!("Getting value for key");
        let response = self
            .proto_client
            .get(GetRequest {
                transaction_id: transaction_id.to_string(),
                keyspace: Some(ProtoKeyspace {
                    namespace: keyspace.namespace.clone(),
                    name: keyspace.name.clone(),
                }),
                key: key.to_vec(),
            })
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?;

        Ok(response.get_ref().value.clone().map(Bytes::from))
    }
}
