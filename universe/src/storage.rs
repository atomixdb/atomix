use proto::universe::{
    get_keyspace_info_request::KeyspaceInfoSearchField as ProtoKeyspaceInfoSearchField, KeyRange,
    Keyspace, KeyspaceInfo, Zone, ZonedKeyRange,
};
use std::sync::Arc;
use thiserror::Error;

pub mod cassandra;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Timeout Error")]
    Timeout,
    #[error("Keyspace already exists")]
    KeyspaceAlreadyExists,
    #[error("Storage Layer error: {}", .0.as_ref().map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".to_string()))]
    InternalError(Option<Arc<dyn std::error::Error + Send + Sync>>),
    #[error("Keyspace does not exist")]
    KeyspaceDoesNotExist,
}

#[derive(Debug)]
pub enum KeyspaceInfoSearchField {
    Keyspace { namespace: String, name: String },
    KeyspaceId(String),
}

impl From<ProtoKeyspaceInfoSearchField> for KeyspaceInfoSearchField {
    fn from(val: ProtoKeyspaceInfoSearchField) -> Self {
        match val {
            ProtoKeyspaceInfoSearchField::Keyspace(keyspace) => KeyspaceInfoSearchField::Keyspace {
                namespace: keyspace.namespace,
                name: keyspace.name,
            },
            ProtoKeyspaceInfoSearchField::KeyspaceId(keyspace_id) => {
                KeyspaceInfoSearchField::KeyspaceId(keyspace_id)
            }
        }
    }
}

impl From<KeyspaceInfoSearchField> for ProtoKeyspaceInfoSearchField {
    fn from(val: KeyspaceInfoSearchField) -> Self {
        match val {
            KeyspaceInfoSearchField::Keyspace { namespace, name } => {
                ProtoKeyspaceInfoSearchField::Keyspace(Keyspace { namespace, name })
            }
            KeyspaceInfoSearchField::KeyspaceId(keyspace_id) => {
                ProtoKeyspaceInfoSearchField::KeyspaceId(keyspace_id)
            }
        }
    }
}

pub trait Storage: Send + Sync + 'static {
    fn create_keyspace(
        &self,
        keyspace_id: &str,
        name: &str,
        namespace: &str,
        primary_zone: Zone,
        secondary_zones: Vec<Zone>,
        base_key_ranges: Vec<KeyRange>,
        secondary_key_ranges: Vec<ZonedKeyRange>,
    ) -> impl std::future::Future<Output = Result<String, Error>> + Send;

    fn list_keyspaces(
        &self,
        region: Option<proto::universe::Region>,
    ) -> impl std::future::Future<Output = Result<Vec<KeyspaceInfo>, Error>> + Send;

    fn get_keyspace_info(
        &self,
        keyspace_info_search_field: KeyspaceInfoSearchField,
    ) -> impl std::future::Future<Output = Result<KeyspaceInfo, Error>> + Send;
}
