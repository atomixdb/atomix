use std::str::FromStr;
use std::sync::Arc;

use common::keyspace_id::KeyspaceId;
use proto::universe::get_keyspace_info_request::KeyspaceInfoSearchField;
use proto::universe::universe_client::UniverseClient;
use proto::universe::{GetKeyspaceInfoRequest, Keyspace, KeyspaceInfo};
use tonic::async_trait;

use crate::cache::KeyspaceCache;
use crate::error::Error;

/// A cache that doesn't store anything. Useful for testing.
struct NoOpCache {
    universe_client: UniverseClient<tonic::transport::Channel>,
}

impl NoOpCache {
    pub fn new(universe_client: UniverseClient<tonic::transport::Channel>) -> Self {
        NoOpCache { universe_client }
    }
}

#[async_trait]
impl KeyspaceCache for NoOpCache {
    async fn get_keyspace_id(&self, name: &str, namespace: &str) -> Result<KeyspaceId, Error> {
        let request = GetKeyspaceInfoRequest {
            keyspace_info_search_field: Some(KeyspaceInfoSearchField::Keyspace(Keyspace {
                name: name.to_string(),
                namespace: namespace.to_string(),
            })),
        };

        let response = self
            .universe_client
            .clone()
            .get_keyspace_info(request)
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?
            .into_inner();

        let keyspace_id = response.keyspace_info.unwrap().keyspace_id;
        let keyspace_id = KeyspaceId::from_str(&keyspace_id).unwrap();
        Ok(keyspace_id)
    }

    async fn get_keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo, Error> {
        let request = GetKeyspaceInfoRequest {
            keyspace_info_search_field: Some(KeyspaceInfoSearchField::KeyspaceId(
                keyspace_id.id.to_string(),
            )),
        };

        let response = self
            .universe_client
            .clone()
            .get_keyspace_info(request)
            .await
            .map_err(|e| Error::InternalError(Arc::new(e)))?
            .into_inner();

        Ok(response.keyspace_info.unwrap())
    }
}
