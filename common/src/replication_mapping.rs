use crate::full_range_id::FullRangeId;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct ReplicationMapping {
    pub primary_range: FullRangeId,
    pub secondary_range: FullRangeId,
    pub assignee: String,
}

impl From<&proto::warden::ReplicationMapping> for ReplicationMapping {
    fn from(proto_rm: &proto::warden::ReplicationMapping) -> Self {
        let primary_range = proto_rm.primary_range.as_ref().unwrap();
        let secondary_range = proto_rm.secondary_range.as_ref().unwrap();
        ReplicationMapping {
            primary_range: FullRangeId::from(primary_range),
            secondary_range: FullRangeId::from(secondary_range),
            assignee: proto_rm.assignee.clone(),
        }
    }
}
