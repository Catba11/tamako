//! Telegram platform adapter (specs.md Section 4.2). The first real
//! platform adapter.
//!
//! Rule A1: no teloxide type crosses the adapter boundary. The `normalize`
//! module converts teloxide update types into `tamako_core::event` types;
//! only those normalized types leave this crate.
//!
//! Rule A5: a Matrix adapter must remain possible without actor changes.
//! This crate therefore keeps all Telegram specifics inside the adapter and
//! emits only the platform-neutral `tamako_core::event` types.

pub mod adapter;
pub mod capability;
pub mod normalize;

pub use adapter::{GroupEvent, TeloxideAdapter};
pub use capability::BotChatStatus;
pub use normalize::BotIdentity;
