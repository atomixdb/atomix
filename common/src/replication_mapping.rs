use crate::full_range_id::FullRangeId;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct ReplicationMapping {
    pub primary_range: FullRangeId,
    pub secondary_range: FullRangeId,
    pub assignee: String,
}
