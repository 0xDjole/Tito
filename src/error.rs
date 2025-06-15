use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum TitoError {
    #[error("Failed to connect to TiKV: {0}")]
    ConnectionFailed(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Transaction failed: {0}")]
    TransactionFailed(String),

    #[error("Operation failed in read-only mode")]
    ReadOnlyMode,

    #[error("Failed to create resource: {0}")]
    CreateFailed(String),

    #[error("Failed to update resource: {0}")]
    UpdateFailed(String),

    #[error("Failed to delete resource: {0}")]
    DeleteFailed(String),

    #[error("Database query failed: {0}")]
    QueryFailed(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Serialization error: {0}")]
    SerializationFailed(String),

    #[error("Deserialization error: {0}")]
    DeserializationFailed(String),

    #[error("Index error: {0}")]
    IndexError(String),

    #[error("Relationship error: {0}")]
    RelationshipError(String),

    #[error("Job queue error: {0}")]
    JobQueueError(String),

    #[error("Lock acquisition failed: {0}")]
    LockError(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Unexpected error: {0}")]
    Internal(String),

    #[error("Configuration error: {0}")]
    Configuration(String),
}
