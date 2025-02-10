use std::str::FromStr;
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
