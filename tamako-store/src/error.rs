//! Error type of tamako-store. Library crates return typed errors.
//! Refer to AGENT.md Section 6.4.

/// Error type for all store operations.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid chat_id: {0}")]
    InvalidChatId(String),
    #[error("invalid stored value for key {key}: {value}")]
    InvalidValue { key: String, value: String },
}

pub type Result<T> = std::result::Result<T, StoreError>;
