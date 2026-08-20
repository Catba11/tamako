//! tamako-memory: the graph backend trait and its implementation.
//! Refer to proposed-graph-database-specs.md.

pub mod backend;
pub mod identifiers;
mod lbug_backend;

pub use backend::{
    AliasTarget, EdgeId, MemoryBackend, MemoryBatch, MemoryEdge, MemoryError, MemoryNode,
    MergeOutcome, MergeSnapshot, MergeSnapshotEdge, MergeSnapshotEdgeKey, MergeSnapshotNode,
    NeighborEdge, NodeContent, NodeFactEdge, NodeFacts, NodeMergeStats, NodeResolutionInfo,
    NodeType, Result, UpsertOutcome, NEIGHBOR_EXPANSION_LIMIT,
};
pub use lbug_backend::LbugBackend;
