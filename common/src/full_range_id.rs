use uuid::Uuid;

use crate::keyspace_id::KeyspaceId;
use crate::range_type::RangeType;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct FullRangeId {
    pub keyspace_id: KeyspaceId,
    pub range_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct FullRangeIdAndType {
    pub full_range_id: FullRangeId,
    pub range_type: RangeType,
}
