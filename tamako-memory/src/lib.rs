//! tamako-memory: the graph backend trait and its implementation.
//! Refer to proposed-graph-database-specs.md.

pub mod backend;
pub mod identifiers;
mod lbug_backend;

pub use backend::{
    MemoryBackend, MemoryBatch, MemoryEdge, MemoryError, MemoryNode, NodeType, Result,
};
pub use lbug_backend::LbugBackend;
