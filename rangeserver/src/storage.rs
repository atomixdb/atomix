pub mod cassandra;

use std::sync::Arc;

use crate::key_version::KeyVersion;
use bytes::Bytes;
use common::full_range_id::FullRangeId;
use common::key_range::KeyRange;
use common::range_type::RangeType;
use thiserror::Error;
use uuid::Uuid;

type EpochLease = (u64, u64);

#[derive(Clone, Debug)]
pub struct CommonRangeInfo {
    pub id: Uuid,
    pub key_range: KeyRange,
    pub leader_sequence_number: u64,
    pub epoch_lease: EpochLease,
    pub range_type: RangeType,
}

pub enum RangeInfo {
    Primary {
        common: CommonRangeInfo,
    },
    Secondary {
        common: CommonRangeInfo,
        applied_secondary_wal_offset: Option<u64>,
    },
}

impl RangeInfo {
    pub fn common(&self) -> &CommonRangeInfo {
        match self {
            RangeInfo::Primary { common } => common,
            RangeInfo::Secondary { common, .. } => common,
        }
    }

    pub fn common_mut(&mut self) -> &mut CommonRangeInfo {
        match self {
            RangeInfo::Primary { common } => common,
            RangeInfo::Secondary { common, .. } => common,
        }
    }
}

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Timeout Error")]
    Timeout,
    #[error("No range with this id exists in the storage layer")]
    RangeDoesNotExist,
    #[error("Range ownership claimed by another range server")]
    RangeOwnershipLost,
    #[error("Storage Layer error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

pub trait Storage: Send + Sync + 'static {
    fn take_ownership_and_load_range(
        &self,
        range_id: FullRangeId,
    ) -> impl std::future::Future<Output = Result<RangeInfo, Error>> + Send;

    fn renew_epoch_lease(
        &self,
        range_id: FullRangeId,
        new_lease: EpochLease,
        leader_sequence_number: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    fn update_secondary_status(
        &self,
        range_id: FullRangeId,
        applied_secondary_offset: Option<u64>,
        leader_sequence_number: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    fn upsert(
        &self,
        range_id: FullRangeId,
        key: Bytes,
        val: Bytes,
        version: KeyVersion,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn delete(
        &self,
        range_id: FullRangeId,
        key: Bytes,
        version: KeyVersion,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn get(
        &self,
        range_id: FullRangeId,
        key: Bytes,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, Error>> + Send;
}
