use std::str::FromStr;

use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct KeyspaceId {
    pub id: Uuid,
}

impl KeyspaceId {
    pub fn new(id: Uuid) -> KeyspaceId {
        KeyspaceId { id }
    }
}

impl FromStr for KeyspaceId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = Uuid::parse_str(s).map_err(|_| "Invalid UUID".to_string())?;
        Ok(KeyspaceId { id })
    }
}

impl Serialize for KeyspaceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.id.to_string())
    }
}
