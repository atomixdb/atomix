use crate::{
    cache::Error as CacheError, storage::Error as StorageError,
    transaction_abort_reason::TransactionAbortReason, wal::Error as WalError,
};
use epoch_publisher::error::Error as EpochSupplierError;

use flatbuf::rangeserver_flatbuffers::range_server::Status;

use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Error {
    InvalidRequestFormat,
    RangeDoesNotExist,
    RangeIsNotLoaded,
    KeyIsOutOfRange,
    RangeOwnershipLost,
    Timeout,
    ConnectionClosed,
    UnknownTransaction,
    CacheIsFull,
    PrefetchError,
    TransactionAborted(TransactionAbortReason),
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

impl Error {
    pub fn internal_error_from_string(msg: &str) -> Self {
        Self::InternalError(Arc::<dyn std::error::Error + Send + Sync>::from(Box::<
            dyn std::error::Error + Send + Sync,
        >::from(
            msg
        )))
    }

    pub fn from_storage_error(e: StorageError) -> Self {
        match e {
            StorageError::RangeDoesNotExist => Self::RangeDoesNotExist,
            StorageError::RangeOwnershipLost => Self::RangeOwnershipLost,
            StorageError::Timeout => Self::Timeout,
            StorageError::InternalError(_) => Self::InternalError(Arc::new(e)),
        }
    }

    pub fn from_wal_error(e: WalError) -> Self {
        match e {
            WalError::Timeout => Self::Timeout,
            WalError::NotSynced => Self::RangeOwnershipLost,
            WalError::Internal(e) => Self::InternalError(e),
        }
    }

    pub fn from_cache_error(e: CacheError) -> Self {
        match e {
            CacheError::CacheIsFull => Self::CacheIsFull,
            CacheError::Timeout => Self::Timeout,
            CacheError::InternalError(_) => Self::InternalError(Arc::new(e)),
        }
    }

    pub fn from_epoch_supplier_error(e: EpochSupplierError) -> Self {
        Self::InternalError(Arc::new(e))
    }

    pub fn to_flatbuf_status(&self) -> Status {
        println!("Error: {:?}", self);
        println!("Error backtrace: {}", std::backtrace::Backtrace::capture());
        match self {
            Self::InvalidRequestFormat => Status::InvalidRequestFormat,
            Self::RangeDoesNotExist => Status::RangeDoesNotExist,
            Self::RangeIsNotLoaded => Status::RangeIsNotLoaded,
            Self::KeyIsOutOfRange => Status::KeyIsOutOfRange,
            Self::RangeOwnershipLost => Status::RangeOwnershipLost,
            Self::Timeout => Status::Timeout,
            Self::UnknownTransaction => Status::UnknownTransaction,
            Self::TransactionAborted(_) => Status::TransactionAborted,
            Self::CacheIsFull => Status::CacheIsFull,
            Self::InternalError(_) => Status::InternalError,
            // ConnectionClosed is really a client-side error and should not be
            // returned from the server.
            Self::ConnectionClosed => Status::InternalError,
            Self::PrefetchError => Status::PrefetchError,
        }
    }

    pub fn from_flatbuf_status(status: Status) -> Result<(), Self> {
        match status {
            Status::Ok => Ok(()),
            Status::InvalidRequestFormat => Err(Self::InvalidRequestFormat),
            Status::RangeDoesNotExist => Err(Self::RangeDoesNotExist),
            Status::RangeIsNotLoaded => Err(Self::RangeIsNotLoaded),
            Status::KeyIsOutOfRange => Err(Self::KeyIsOutOfRange),
            Status::RangeOwnershipLost => Err(Self::RangeOwnershipLost),
            Status::Timeout => Err(Self::Timeout),
            Status::UnknownTransaction => Err(Self::UnknownTransaction),
            Status::CacheIsFull => Err(Self::CacheIsFull),
            Status::TransactionAborted => {
                // TODO: get the reason from the message.
                Err(Self::TransactionAborted(TransactionAbortReason::Other))
            }
            Status::InternalError => {
                // TODO: get the error from the message.
                Err(Self::InternalError(Arc::new(std::fmt::Error)))
            }
            Status::PrefetchError => Err(Self::PrefetchError),
            _ => Err(Self::InternalError(Arc::new(std::fmt::Error))),
        }
    }
}
