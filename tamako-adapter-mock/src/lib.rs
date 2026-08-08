//! tamako-adapter-mock: the mock platform adapter and the replay fixture.
//!
//! The mock adapter replays a recorded chat log (the replay fixture) as
//! normalized inbound events. It records every outbound action for test
//! assertions. It is the Phase 0 demo and test harness. Refer to
//! dev-roadmap.md Section 2.
//!
//! Rule A1: platform-specific types stay inside the adapter. This crate
//! emits and consumes only the normalized types of tamako-core.
//! Rule A5: a second platform adapter must be possible without changes to
//! the actor. This crate proves that the normalized contract is sufficient.

pub mod fixture;
pub mod mock;

pub use fixture::{FixtureError, FixtureEvent, ReplayFixture};
pub use mock::MockAdapter;
