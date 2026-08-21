//! tamako-memory: the graph backend trait and its implementation.
//! Refer to proposed-graph-database-specs.md.

pub mod backend;
pub mod identifiers;
mod lbug_backend;

pub use backend::{
    AliasTarget, CandidateEdge, EdgeId, MemoryBackend, MemoryBatch, MemoryEdge, MemoryError,
    MemoryNode, MergeOutcome, MergeSnapshot, MergeSnapshotEdge, MergeSnapshotEdgeKey,
    MergeSnapshotNode, NeighborEdge, NodeContent, NodeFactEdge, NodeFacts, NodeMergeStats,
    NodeResolutionInfo, NodeType, Result, TopicCandidate, UpsertOutcome, NEIGHBOR_EXPANSION_LIMIT,
    RECALL_TIME_WINDOW_DAYS,
};
pub use lbug_backend::LbugBackend;
