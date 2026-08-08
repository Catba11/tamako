//! tamako-core: normalized events, the per-group actor, trigger logic,
//! session state, and the live context. Refer to AGENT.md Section 4.
//!
//! Phase 0 delivered the normalized platform types (`event`), the platform
//! adapter contract (`adapter`), typed configuration (`config`), pure
//! trigger scheduling (`trigger`), the session state (`session`), and the
//! per-group actor skeleton (`actor`). Phase 1 adds the context lifecycle:
//! `context` is the live context of specs.md Section 7 (rules C1-C5).
//! Phase 1 M4 adds the wake-procedure contracts (`wake`, specs.md
//! Section 9): the recall seam, the participation gate, and the reply
//! generator; the implementations live in tamako-agent.
//!
//! This crate is model-agnostic: no rig or LLM dependency. tamako-agent
//! converts `context::ContextMessage` to rig completion messages in M4.

pub mod actor;
pub mod adapter;
pub mod config;
pub mod context;
pub mod digest;
pub mod event;
pub mod session;
pub mod trigger;
pub mod wake;
