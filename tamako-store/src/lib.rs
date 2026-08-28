//! tamako-store: store.db access, migrations, the raw message log, and
//! the session-state table. Refer to specs.md Section 5.2.
//!
//! All APIs in this crate are synchronous. Callers run them inside
//! `tokio::task::spawn_blocking`. Refer to AGENT.md Section 6.2.

mod error;
mod media_store;
mod schema;
mod store;

pub use error::{Result, StoreError};
pub use media_store::MediaStore;
pub use store::{
    read_group_status, register_sqlite_vec, ContextSummaryRow, DeadLetterRow, Direction, EventType,
    GroupStatus, InjectedMemoryRow, InsertOutcome, MergeAuditRow, MessageRow, NewMessage,
    NewReaction, PendingEmbedding, ReactionRow, RelatedPairRow, ReplyTargetRow, Store,
    EMBEDDING_DIM,
};
