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
    #[error(
        "single-group helpers take no chat_id and need exactly one open group on this Store, found {0}"
    )]
    AmbiguousGroup(usize),
    #[error(
        "database schema version {found} exceeds this binary's known maximum {known}: \
         the store.db was written by a NEWER tamako; run that binary (or upgrade) instead \
         of opening it with an older one"
    )]
    SchemaFromTheFuture { found: u32, known: u32 },
}

pub type Result<T> = std::result::Result<T, StoreError>;
