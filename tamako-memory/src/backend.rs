//! The memory backend trait and the write-path data types.
//!
//! Refer to proposed-graph-database-specs.md. The trait is the stable
//! interface of Section 5.2: callers get an asynchronous interface, and
//! the implementation wraps the synchronous driver calls in
//! `tokio::task::spawn_blocking` (AGENT.md Section 6.2).

use std::future::Future;

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// Errors of the memory backend.
#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Driver-level failure: open, query, or checkpoint.
    #[error("backend error: {0}")]
    Backend(String),
    /// Rule P5: the chat_id becomes a directory name. Reject separators.
    #[error("invalid chat_id: {0}")]
    InvalidChatId(String),
}

pub type Result<T> = std::result::Result<T, MemoryError>;

/// Node type. Closed set of proposed-graph-database-specs.md Section 6.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Person,
    Alias,
    Concept,
    MessageBatch,
}

impl NodeType {
    /// The value of the `type` column.
    pub fn as_str(self) -> &'static str {
        match self {
            NodeType::Person => "Person",
            NodeType::Alias => "Alias",
            NodeType::Concept => "Concept",
            NodeType::MessageBatch => "MessageBatch",
        }
    }

    /// Parses the value of the `type` column back. Returns `None` for an
    /// unknown string; read paths skip such rows instead of failing.
    // Not std::str::FromStr: a closed-set lookup that returns Option reads
    // better at the call sites than a Result with an empty error type.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Person" => Some(NodeType::Person),
            "Alias" => Some(NodeType::Alias),
            "Concept" => Some(NodeType::Concept),
            "MessageBatch" => Some(NodeType::MessageBatch),
            _ => None,
        }
    }
}

/// One target of an alias node: the source node of a `known_as` or
/// `also_known_as` edge that points to the alias (Section 7.4 step 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasTarget {
    pub node_id: String,
    pub node_type: NodeType,
}

/// One node of a digest batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryNode {
    /// Deterministic UUID5 string. Refer to Section 7.1. Rule R3 applies.
    pub id: String,
    pub name: String,
    pub node_type: NodeType,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    /// JSON blob. Display fields only. Rule R1 applies.
    pub properties: Option<String>,
}

/// One edge of a digest batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEdge {
    pub source_id: String,
    pub target_id: String,
    /// System name or open-vocabulary snake_case name. Section 6.3.
    pub relationship_name: String,
    pub valid_at: OffsetDateTime,
    /// NULL means the fact is valid now. Section 6.1.
    pub invalid_at: Option<OffsetDateTime>,
    pub edge_text: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    /// JSON blob. Display fields only. Rule R1 applies.
    pub properties: Option<String>,
}

/// One digest batch for the write path of Section 7.
///
/// The batch identifier is stable across retries (specs.md Section 10.3).
/// The writes are idempotent under the deterministic identifiers of
/// Section 7.1 (anti-defer list of dev-roadmap.md Section 7).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryBatch {
    pub batch_id: String,
    pub nodes: Vec<MemoryNode>,
    pub edges: Vec<MemoryEdge>,
}

/// The expansion limit of Section 8.2: at most this many edges per node
/// on one fetch. The fetch truncates by `created_at` descending.
pub const NEIGHBOR_EXPANSION_LIMIT: usize = 500;

/// Read path, Section 8.2: one valid edge of a resolved entry node with
/// its endpoints. `contains` edges never occur here (provenance only,
/// Section 6.3/8.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeighborEdge {
    pub source_id: String,
    pub target_id: String,
    /// System name or open-vocabulary snake_case name. Section 6.3.
    pub relationship_name: String,
    pub edge_text: String,
    pub valid_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
    /// The endpoint that is NOT the queried node.
    pub other_node_id: String,
    pub other_node_name: String,
}

impl NeighborEdge {
    /// The stable dedup key of the natural key of the edge. The EDGE
    /// table has no id column (Section 6.1), so the key is
    /// `{source_id}|{relationship_name}|{target_id}|{valid_at}` with
    /// `valid_at` rendered RFC 3339. This string is the `edge_id` that
    /// the `injected_memories` dedup table stores (specs.md Section 9.3).
    pub fn edge_id(&self) -> String {
        let valid_at = self
            .valid_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| format!("{:?}", self.valid_at));
        format!(
            "{}|{}|{}|{}",
            self.source_id, self.relationship_name, self.target_id, valid_at
        )
    }
}

/// The graph memory backend. One database file per group at
/// `{data_root}/{chat_id}/memory.lbug` (Section 5.1, Rule P5).
///
/// Connection management follows Section 5.2: open on first use, cache
/// the handle in the process, one writer per group file, `CHECKPOINT`
/// after each batch write.
///
/// The methods are declared in the desugared form so the returned futures
/// are `Send`: callers can `.await` them inside `tokio::spawn`.
/// Implementations keep writing plain `async fn`; the compiler checks the
/// `Send` bound at the impl. Consumers use generics, not `dyn`.
pub trait MemoryBackend: Send + Sync {
    /// Opens the database of the group and creates the schema of
    /// Section 6.1 when needed. Idempotent.
    fn ensure_schema<'a>(
        &'a self,
        chat_id: &'a str,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Writes one batch. Idempotent for the same batch content.
    /// Runs `CHECKPOINT` at the end (Section 5.2, rule 5).
    fn upsert_batch<'a>(
        &'a self,
        chat_id: &'a str,
        batch: &'a MemoryBatch,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Runs `CHECKPOINT` on the database of the group.
    fn checkpoint<'a>(&'a self, chat_id: &'a str) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Entity resolution, Section 7.4 step 2: returns the target nodes of
    /// one alias node. Enters the graph through the deterministic alias
    /// identifier (Rule R5). An empty result means the alias is unknown.
    fn alias_targets<'a>(
        &'a self,
        chat_id: &'a str,
        alias_node_id: &'a str,
    ) -> impl Future<Output = Result<Vec<AliasTarget>>> + Send + 'a;

    /// Read path, Section 8.2: the valid direct neighbors of one resolved
    /// entry node, one hop. Enters through the node identifier (Rule R5).
    /// `contains` edges are excluded (provenance only, Section 6.3/8.2).
    /// At most NEIGHBOR_EXPANSION_LIMIT edges, truncated by `created_at`
    /// descending. An unknown node id yields an empty vec.
    ///
    /// Documented Phase 1 simplifications of Section 8.2:
    /// - Hub marking (degree above 1000) is SKIPPED. The 500-edge
    ///   truncation applies unconditionally to every node, which is
    ///   strictly stronger than the hub rule requires.
    /// - The 90-day default time window is NOT applied. The graphs are
    ///   young; the window exists for year-scale deployments.
    fn neighbors<'a>(
        &'a self,
        chat_id: &'a str,
        node_id: &'a str,
    ) -> impl Future<Output = Result<Vec<NeighborEdge>>> + Send + 'a;

    /// Closes the cached handle of the group. Later calls reopen it.
    fn close<'a>(&'a self, chat_id: &'a str) -> impl Future<Output = Result<()>> + Send + 'a;
}
