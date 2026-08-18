//! tamako-memory: the graph backend trait and its implementation.
//! Refer to proposed-graph-database-specs.md.

pub mod backend;
pub mod identifiers;
mod lbug_backend;

pub use backend::{
    AliasTarget, MemoryBackend, MemoryBatch, MemoryEdge, MemoryError, MemoryNode, MergeOutcome,
    MergeSnapshot, MergeSnapshotEdge, MergeSnapshotEdgeKey, MergeSnapshotNode, NeighborEdge,
    NodeContent, NodeMergeStats, NodeResolutionInfo, NodeType, Result, NEIGHBOR_EXPANSION_LIMIT,
};
pub use lbug_backend::LbugBackend;
