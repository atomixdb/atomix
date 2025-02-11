use coordinator::error::Error as CoordinatorError;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Error {
    InvalidRequestFormat,
    TransactionNotFound,
    CoordinatorError(CoordinatorError),
    Timeout,
    ConnectionClosed,
    KeyspaceDoesNotExist,
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}
