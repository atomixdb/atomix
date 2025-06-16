use std::str::FromStr;

use serde::Serialize;
use uuid::Uuid;

use crate::keyspace_id::KeyspaceId;
use crate::range_type::RangeType;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize)]
pub struct FullRangeId {
    pub keyspace_id: KeyspaceId,
    pub range_id: Uuid,
}

impl From<&proto::warden::RangeId> for FullRangeId {
    fn from(proto_range_id: &proto::warden::RangeId) -> Self {
        let keyspace_id = Uuid::from_str(proto_range_id.keyspace_id.as_str()).unwrap();
        let keyspace_id = KeyspaceId::new(keyspace_id);
        let range_id = Uuid::from_str(proto_range_id.range_id.as_str()).unwrap();
        FullRangeId {
            keyspace_id,
            range_id,
        }
    }
}

impl From<FullRangeId> for proto::warden::RangeId {
    fn from(full_range_id: FullRangeId) -> Self {
        proto::warden::RangeId {
            keyspace_id: full_range_id.keyspace_id.id.to_string(),
            range_id: full_range_id.range_id.to_string(),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct FullRangeIdAndType {
    pub full_range_id: FullRangeId,
    pub range_type: RangeType,
}
