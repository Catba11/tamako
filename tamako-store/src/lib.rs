//! tamako-store: store.db access, migrations, the raw message log, and
//! the session-state table. Refer to specs.md Section 5.2.
//!
//! All APIs in this crate are synchronous. Callers run them inside
//! `tokio::task::spawn_blocking`. Refer to AGENT.md Section 6.2.

mod error;
mod schema;
mod store;

pub use error::{Result, StoreError};
pub use store::{
    read_group_status, DeadLetterRow, Direction, EventType, GroupStatus, InjectedMemoryRow,
    InsertOutcome, MessageRow, NewMessage, NewReaction, ReactionRow, Store,
};
