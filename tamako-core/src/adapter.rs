//! The platform adapter contract. Refer to specs.md Section 4.
//! Rule A1: platform-specific types stay inside the adapter.
//! Rule P7: the platform frontend is loosely coupled.

use crate::event::{InboundEvent, OutboundAction};

/// Errors of the adapter layer.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// The inbound event source failed.
    #[error("adapter source failed: {0}")]
    Source(String),
    /// The outbound action sink failed.
    #[error("adapter sink failed: {0}")]
    Sink(String),
    /// The adapter does not support this outbound action in this phase
    /// (example: SendMedia is Phase 3 territory; the pet's replies are text).
    #[error("outbound action not supported by this adapter: {0}")]
    Unsupported(String),
}

/// Rule A1: platform-specific types stay inside the adapter. The actor sees
/// normalized events only.
///
/// The trait is used through generics, not `dyn`. A native `async fn` in a
/// trait requires Rust 1.75 or later.
#[allow(async_fn_in_trait)]
pub trait PlatformAdapter: Send {
    /// Returns the next inbound event. Returns `Ok(None)` when the event
    /// stream has ended.
    async fn next_event(&mut self) -> Result<Option<InboundEvent>, AdapterError>;

    /// Executes one outbound action.
    async fn execute(&self, action: OutboundAction) -> Result<(), AdapterError>;
}
