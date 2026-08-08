//! tamako-core: normalized events, the per-group actor, trigger logic,
//! and session state. Refer to AGENT.md Section 4.
//!
//! Phase 0 scope: normalized platform types (`event`), the platform adapter
//! contract (`adapter`), typed configuration (`config`), pure trigger
//! scheduling (`trigger`), the session state (`session`), and the per-group
//! actor skeleton (`actor`). The wake and digest procedures enter in
//! Phase 1.

pub mod actor;
pub mod adapter;
pub mod config;
pub mod digest;
pub mod event;
pub mod session;
pub mod trigger;
