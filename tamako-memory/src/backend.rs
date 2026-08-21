//! The memory backend trait and the write-path data types.
//!
//! Refer to proposed-graph-database-specs.md. The trait is the stable
//! interface of Section 5.2: callers get an asynchronous interface, and
//! the implementation wraps the synchronous driver calls in
//! `tokio::task::spawn_blocking` (AGENT.md Section 6.2).

use std::future::Future;

use serde::{Deserialize, Serialize};
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

/// The time window of the Section 8.2 traversal rules, in days: an edge
/// qualifies for a recall expansion when its `valid_at` OR its
/// `created_at` falls inside the window (the edge is recent by either
/// measure — a long-valid fact re-extracted yesterday still counts).
/// Decision 76 (a) runs the two-hop expansion "under the Section 8.2
/// rules", which MANDATE the window ("a time window on `valid_at` or
/// `created_at`. The default window is 90 days.") — unlike the
/// decision-58 shallow read (`neighbors`), which documented NOT
/// applying it.
pub const RECALL_TIME_WINDOW_DAYS: i64 = 90;

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

/// Decision 76 / graph-spec Section 8.2: one recall-candidate edge of
/// the deep read path, with BOTH endpoint names hydrated (the candidate
/// render speaks names). The `edge_id` is the opaque [`EdgeId::encode`]
/// of the natural key — the same dedup key shape as
/// [`NeighborEdge::edge_id`] (specs.md Section 9.3).
///
/// The field set is a superset of the decision-58 recall candidate
/// render: `tamako-agent`'s `RecallCandidate` consumes `edge_id`,
/// `edge_text`, `valid_at`, `source_id`, `relationship_name`, and
/// `target_id` verbatim from here; the names feed the deep-recall
/// candidate text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateEdge {
    /// The opaque edge id: [`EdgeId::encode`] of the natural key.
    pub edge_id: String,
    pub source_id: String,
    pub source_name: String,
    pub target_id: String,
    pub target_name: String,
    /// System name or open-vocabulary snake_case name. Section 6.3.
    pub relationship_name: String,
    /// The stored `edge_text` (the description).
    pub edge_text: String,
    pub valid_at: OffsetDateTime,
}

/// Decision 75: the opaque edge identifier of the manual fact ops
/// (`--facts` / `--invalidate` / `--revalidate`).
///
/// The EDGE table has NO id column (Section 6.1); the natural key is
/// `(source_id, relationship_name, target_id, valid_at)`. The edge id is
/// a compact JSON object encoding exactly that key, so it is stable,
/// unambiguous (no separator-escaping concerns), and round-trips:
/// [`NodeFacts`] emits it, and `invalidate_edge` / `revalidate_edge`
/// parse it back.
///
/// Format: `{"source_id":…,"relationship_name":…,"target_id":…,"valid_at":…}`
/// where `valid_at` is RFC 3339. Malformed input is a loud error at the
/// parse sites, never a silent no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeId {
    pub source_id: String,
    pub relationship_name: String,
    pub target_id: String,
    pub valid_at: OffsetDateTime,
}

impl EdgeId {
    /// Renders the opaque edge-id string (compact JSON of the natural
    /// key).
    pub fn encode(&self) -> String {
        let valid_at = self
            .valid_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| format!("{:?}", self.valid_at));
        serde_json::json!({
            "source_id": self.source_id,
            "relationship_name": self.relationship_name,
            "target_id": self.target_id,
            "valid_at": valid_at,
        })
        .to_string()
    }

    /// Parses an opaque edge-id string back into its natural key. A
    /// malformed id (bad JSON, a missing field, an unparseable
    /// `valid_at`) is a loud error: the manual ops must never silently
    /// target the wrong edge.
    pub fn decode(encoded: &str) -> Result<EdgeId> {
        let value: serde_json::Value = serde_json::from_str(encoded).map_err(|error| {
            MemoryError::Backend(format!("invalid edge id {encoded:?}: {error}"))
        })?;
        let field = |key: &str| -> Result<String> {
            value
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| {
                    MemoryError::Backend(format!(
                        "invalid edge id {encoded:?}: missing or non-string field {key}"
                    ))
                })
        };
        let source_id = field("source_id")?;
        let relationship_name = field("relationship_name")?;
        let target_id = field("target_id")?;
        let valid_at_string = field("valid_at")?;
        let valid_at = OffsetDateTime::parse(&valid_at_string, &Rfc3339).map_err(|error| {
            MemoryError::Backend(format!(
                "invalid edge id {encoded:?}: unparseable valid_at {valid_at_string:?}: {error}"
            ))
        })?;
        Ok(EdgeId {
            source_id,
            relationship_name,
            target_id,
            valid_at,
        })
    }
}

/// One edge row of [`NodeFacts`] (decision 75 `--facts`). Carries the
/// opaque [`EdgeId`] plus the display fields the operator needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeFactEdge {
    /// The opaque edge id; hand it to `--invalidate` / `--revalidate`.
    pub edge_id: String,
    /// The relationship name (the predicate).
    pub relationship_name: String,
    /// The endpoint that is NOT the queried node.
    pub other_node_name: String,
    /// True when the queried node is the SOURCE of the edge.
    pub outgoing: bool,
    /// The stored `edge_text` (the description).
    pub edge_text: String,
    pub valid_at: OffsetDateTime,
    /// NULL means the fact is valid now (Section 6.1).
    pub invalid_at: Option<OffsetDateTime>,
}

/// Decision 75: the fact listing of one node (`--facts <chat_id>
/// <name>`). Resolved through the exact-alias machinery of Section 7.4
/// step 2; `None` from `node_facts` means the name is unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeFacts {
    /// The resolved node id the edges hang off.
    pub node_id: String,
    /// The stored name of the resolved node.
    pub node_name: String,
    /// Every edge of the node, both directions, valid AND invalid
    /// (invalid edges are marked via `invalid_at`).
    pub edges: Vec<NodeFactEdge>,
}

/// Phase 2 (current-state.md decision 66): the STORED display content of
/// one node, read back from the graph. The embedding worker drains the
/// `pending_embeddings` queue against these values: pipeline-known values
/// are not authoritative post-merge, because alias-bound nodes keep their
/// stored properties through the MERGE coalesce (Rule R4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeContent {
    pub name: String,
    /// The `description` field of the `properties` JSON blob. Empty when
    /// the node carries no description.
    pub description: String,
}

/// Decision 73: the per-candidate resolution info of the vector
/// pre-screen of entity resolution. The kind comes from the stored
/// `type` column (the closed set of Section 6.2). `alias_target` is the
/// source node id of a `known_as`/`also_known_as` edge into the alias
/// (Section 7.4 step 2) and is `Some` only for Alias nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeResolutionInfo {
    pub kind: NodeType,
    pub alias_target: Option<String>,
}

/// The outcome of one `merge_nodes` call (decision 74, graph-spec
/// Section 7.7 step 3). `snapshot_json` is the serialized
/// `MergeSnapshot`; the caller stores it in the `merge_audit` row
/// (specs.md Section 5.2) and hands it to `rollback_merge` when needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeOutcome {
    /// The rollback snapshot, taken BEFORE any mutation.
    pub snapshot_json: String,
    /// Edges re-pointed from the loser to the survivor.
    pub edges_moved: u32,
    /// Loser↔survivor edges (and loser self-edges) dropped because they
    /// would become self-loops on the survivor.
    pub self_loops_dropped: u32,
    /// Re-pointed edges skipped because the survivor already carried an
    /// equivalent edge (same direction, same relationship name, same
    /// other endpoint, same edge text).
    pub edges_deduped: u32,
    /// Decision 75 / Section 7.7: valid same-predicate edges invalidated
    /// on the survivor by the single-value invariant pass after
    /// re-pointing. Zero when the registry is empty (the plain
    /// `merge_nodes` delegate).
    pub single_value_invalidated: u32,
}

/// The outcome of one `upsert_batch_with_registry` call (decision 75,
/// graph-spec Section 7.5).
///
/// A small outcome struct rather than a bare `u32` — matching the house
/// style of [`MergeOutcome`] — so further per-class counts can be added
/// later without another signature change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UpsertOutcome {
    /// The number of previously-valid edges invalidated by the
    /// single-value write path (feeds the `facts_invalidated_total`
    /// counter, decision 75 (e)). Multi-value edges are never counted.
    pub invalidated: u32,
}

/// The rollback snapshot of one merge (decision 74, graph-spec Section
/// 7.7 step 3). Serialized into the `snapshot_json` of `MergeOutcome` and
/// the `merge_audit` row (specs.md Section 5.2); `rollback_merge` parses
/// it back. Version-tolerant: fields added later carry serde defaults so
/// older snapshots keep parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeSnapshot {
    /// Snapshot layout version. 1 today; snapshots written before the
    /// field existed parse as 0.
    #[serde(default)]
    pub version: u32,
    /// The survivor node id. Rollback refuses loudly when this node no
    /// longer exists (chained-merge rollback is out of scope, Section
    /// 7.7 step 4).
    #[serde(default)]
    pub survivor_id: String,
    /// The loser node, exactly as stored before the merge.
    pub node: MergeSnapshotNode,
    /// Every edge of the loser, both directions, with full properties.
    #[serde(default)]
    pub edges: Vec<MergeSnapshotEdge>,
    /// The natural keys of the edges the merge created on the survivor.
    /// Rollback deletes exactly these.
    #[serde(default)]
    pub created_edges: Vec<MergeSnapshotEdgeKey>,
}

/// The loser node of one merge, exactly as stored before the merge. The
/// `properties` blob is stored verbatim, so the node can be recreated
/// field-for-field on rollback; the description of specs.md Section 5.2
/// lives inside it (decision 66).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeSnapshotNode {
    pub id: String,
    /// The `type` column value (the closed set of Section 6.2).
    pub kind: String,
    pub name: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    /// The raw `properties` JSON blob; `None` for a NULL column.
    #[serde(default)]
    pub properties: Option<String>,
}

/// One original edge of the loser, with every stored column. The
/// explicit endpoints (rather than a direction flag) recreate the edge
/// verbatim on rollback; the loser is always one of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeSnapshotEdge {
    pub source_id: String,
    pub target_id: String,
    pub relationship_name: String,
    #[serde(with = "time::serde::rfc3339")]
    pub valid_at: OffsetDateTime,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub invalid_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub edge_text: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    /// The raw `properties` JSON blob; `None` for a NULL column.
    #[serde(default)]
    pub properties: Option<String>,
}

/// The natural key of one edge the merge created on the survivor:
/// (source, relationship_name, target, valid_at), the same key shape as
/// `NeighborEdge::edge_id` (Section 6.1). Rollback deletes exactly the
/// edges matching these keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeSnapshotEdgeKey {
    pub source_id: String,
    pub relationship_name: String,
    pub target_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub valid_at: OffsetDateTime,
}

/// Decision 74 / graph-spec Section 7.7 step 3: the survivor-choice
/// inputs of one node — the edge degree (EVERY EDGE row touching the
/// node, both directions; a self-loop counts once) and the stored
/// `created_at`. `NodeResolutionInfo` carries no degree and
/// `neighbors` truncates at [`NEIGHBOR_EXPANSION_LIMIT`] and excludes
/// `contains`/invalid edges, so neither answers the survivor question
/// ("higher edge degree wins; a tie goes to the older `created_at`").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeMergeStats {
    /// Every EDGE row touching the node, both directions.
    pub edge_degree: u64,
    /// The stored `created_at` column of the node.
    pub created_at: OffsetDateTime,
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
    ///
    /// Equivalent to [`MemoryBackend::upsert_batch_with_registry`] with an
    /// EMPTY single-value registry: no invalidation runs, every predicate
    /// behaves as multi-value (the decision-74 write path, preserved
    /// exactly). Kept as a delegate so existing callers compile unchanged.
    fn upsert_batch<'a>(
        &'a self,
        chat_id: &'a str,
        batch: &'a MemoryBatch,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Decision 75 / Section 7.5: writes one batch with single-value fact
    /// invalidation. For each edge whose `relationship_name` is in
    /// `single_value_predicates`, processed in BATCH ORDER inside the same
    /// single transaction: first every OTHER valid edge with the same
    /// (subject = the edge's SOURCE, relationship_name) gets
    /// `invalid_at`/`updated_at` = now, then the new edge is MERGEd with
    /// `valid_at` refreshed. Exactly one valid edge per (subject,
    /// predicate) survives at commit — the last write wins, so one batch
    /// carrying a change ("quit A, now at B") commits correctly. Edges
    /// whose predicate is absent from the registry are multi-value and
    /// untouched. A REPLAYED batch converges: the deterministic natural
    /// key MERGEs and the replayed edge's own invalidation re-validates
    /// it, so a re-run is a state-wise no-op. Runs `CHECKPOINT` at the
    /// end (Section 5.2, rule 5).
    ///
    /// Returns the number of edges invalidated (for the
    /// `facts_invalidated_total` counter). An EMPTY registry performs no
    /// invalidation and returns zero — the decision-74 behavior.
    ///
    /// The default errors loudly so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn upsert_batch_with_registry<'a>(
        &'a self,
        chat_id: &'a str,
        batch: &'a MemoryBatch,
        single_value_predicates: &'a [String],
    ) -> impl Future<Output = Result<UpsertOutcome>> + Send + 'a {
        let _ = (chat_id, batch, single_value_predicates);
        async {
            Err(MemoryError::Backend(
                "upsert_batch_with_registry is not implemented by this backend".to_string(),
            ))
        }
    }

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

    /// Decision 76 / graph-spec Section 8.2: the two-hop recall
    /// expansion. Hop 1 fetches the valid whitelist edges of every entry
    /// node (entered through the node identifiers, Rule R5); hop 2
    /// fetches the valid whitelist edges of every hop-1 neighbor node.
    /// The result is the deduped union: hop-1 edges first (entry order,
    /// per node newest first), then hop-2 edges (frontier order, per
    /// node newest first).
    ///
    /// The Section 8.2 rules applied, per decision 76:
    /// - WHITELIST: every relationship EXCEPT `contains` (provenance)
    ///   and `known_as` (surface forms, resolved at entry);
    ///   `also_known_as` IS traversed (the cross-language bridge,
    ///   decision 76 (b)).
    /// - VALID ONLY: `invalid_at IS NULL`.
    /// - TIME WINDOW: [`RECALL_TIME_WINDOW_DAYS`] on `valid_at` OR
    ///   `created_at`, relative to `now`. Decision 76 (a) runs the
    ///   expansion "under the Section 8.2 rules", which mandate the
    ///   window — unlike the decision-58 shallow read (`neighbors`).
    /// - PER-NODE LIMIT: `per_node_limit` edges per queried node at BOTH
    ///   hops (entry-side at hop 1, neighbor-side at hop 2), truncated
    ///   by `created_at` descending (decision 76 (d)); the call site
    ///   passes [`NEIGHBOR_EXPANSION_LIMIT`]. Hub marking (degree above
    ///   1000) is SKIPPED — the unconditional truncation is strictly
    ///   stronger than the hub rule requires (the decision-58
    ///   simplification, kept: decision 76 is silent on hub marking).
    ///
    /// `now` is a parameter (the house pattern of `invalidate_edge`) so
    /// the window is deterministic under test. An empty entry list
    /// yields an empty vec without opening the database of the group.
    ///
    /// The default returns an empty vec so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn two_hop_edges<'a>(
        &'a self,
        chat_id: &'a str,
        entry_node_ids: &'a [String],
        now: OffsetDateTime,
        per_node_limit: usize,
    ) -> impl Future<Output = Result<Vec<CandidateEdge>>> + Send + 'a {
        let _ = (chat_id, entry_node_ids, now, per_node_limit);
        async { Ok(Vec::new()) }
    }

    /// Decision 76 (c): hydrates sidecar `edge_texts` hits into full
    /// recall candidates. Each id is the opaque [`EdgeId`] string the
    /// sidecar stores; the edge is fetched by its natural key (Rule R5 —
    /// entry through the endpoint identifiers) with the endpoint names
    /// hydrated. Only VALID edges hydrate (`invalid_at IS NULL`): a
    /// candidate the relevance gate sees must be a currently-valid fact,
    /// the same policy as every other candidate-producing read.
    ///
    /// A stale id (a sidecar row whose graph edge was deleted or
    /// invalidated between reconciliations, Section 7.6 step 6) and a
    /// malformed id simply do not occur in the result — the read path
    /// skips, it does not fail. An empty id list yields an empty vec
    /// without opening the database of the group.
    ///
    /// The default returns an empty vec so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn edges_by_ids<'a>(
        &'a self,
        chat_id: &'a str,
        edge_ids: &'a [String],
    ) -> impl Future<Output = Result<Vec<CandidateEdge>>> + Send + 'a {
        let _ = (chat_id, edge_ids);
        async { Ok(Vec::new()) }
    }

    /// Decision 76 (c) / Section 7.6 step 6: EVERY edge of the group —
    /// valid AND invalid, every relationship name including `contains`
    /// — as (opaque edge id, `edge_text`) pairs. Backs the startup
    /// reconciliation diff of the `edge_texts` sidecar: the diff needs
    /// the full edge set to catch sidecar orphans, so the
    /// valid-only filter of Section 8.2 deliberately does NOT apply
    /// here. Edge counts are in the thousands, so the full listing
    /// carries no paging.
    ///
    /// RULE R5 EXCEPTION: this is a full-graph scan by design — the
    /// reconciliation pass has no entry identifiers. It is the
    /// documented exception, mirroring `list_node_contents`
    /// (decision 66).
    ///
    /// The default returns an empty vec so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn list_all_edges<'a>(
        &'a self,
        chat_id: &'a str,
    ) -> impl Future<Output = Result<Vec<(String, String)>>> + Send + 'a {
        let _ = chat_id;
        async { Ok(Vec::new()) }
    }

    /// Phase 2 (current-state.md decision 66): the stored name and
    /// description of one node, entered through the node identifier
    /// (Rule R5). Returns `None` when the node does not exist, which
    /// covers the existence checks of the merge-tool tombstone cleanup
    /// (decision 67).
    ///
    /// The default returns `None` so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn node_content<'a>(
        &'a self,
        chat_id: &'a str,
        node_id: &'a str,
    ) -> impl Future<Output = Result<Option<NodeContent>>> + Send + 'a {
        let _ = (chat_id, node_id);
        async { Ok(None) }
    }

    /// Phase 2 (current-state.md decision 66): the stored name and
    /// description of EVERY node of the group, as (node_id, content)
    /// pairs. Backs the startup reconciliation pass of the embedding
    /// worker. Node counts are hundreds-to-low-thousands, so the full
    /// listing carries no paging.
    ///
    /// The default returns an empty vec so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn list_node_contents<'a>(
        &'a self,
        chat_id: &'a str,
    ) -> impl Future<Output = Result<Vec<(String, NodeContent)>>> + Send + 'a {
        let _ = chat_id;
        async { Ok(Vec::new()) }
    }

    /// Decision 73: the kind and, for Alias nodes, the alias-bound
    /// target of each given node id, entered through the node
    /// identifiers (Rule R5). Backs the vector pre-screen of entity
    /// resolution: one call covers the whole KNN overfetch. A missing
    /// node id simply does not occur in the result, and neither does a
    /// node whose stored type string sits outside the closed set of
    /// Section 6.2 (read paths skip, they do not fail). An empty id
    /// list yields an empty vec.
    ///
    /// The default returns an empty vec so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn node_resolution_infos<'a>(
        &'a self,
        chat_id: &'a str,
        node_ids: &'a [String],
    ) -> impl Future<Output = Result<Vec<(String, NodeResolutionInfo)>>> + Send + 'a {
        let _ = (chat_id, node_ids);
        async { Ok(Vec::new()) }
    }

    /// Decision 74 / graph-spec Section 7.7 step 1: is there a
    /// `known_as`/`also_known_as` edge between the two nodes, in EITHER
    /// direction? The merge-tool candidate scan excludes already-linked
    /// pairs — those are legitimate surface-form links, not duplicates
    /// (decision 74 point 6). Alias edges run entity -> alias and
    /// [`MemoryBackend::link_also_known_as`] writes a -> b, so the
    /// check must not depend on direction.
    ///
    /// The default returns `false` so that noop test doubles stay
    /// source-compatible with the extended trait (a read default; a
    /// false negative only means a linked pair reaches the confirmer).
    fn are_linked<'a>(
        &'a self,
        chat_id: &'a str,
        a_id: &'a str,
        b_id: &'a str,
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        let _ = (chat_id, a_id, b_id);
        async { Ok(false) }
    }

    /// Decision 74 / graph-spec Section 7.7 step 3: the survivor-choice
    /// inputs of each given node id, entered through the node
    /// identifiers (Rule R5). One call covers a whole candidate set. A
    /// missing node id simply does not occur in the result. An empty id
    /// list yields an empty vec.
    ///
    /// The default returns an empty vec so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn node_merge_stats<'a>(
        &'a self,
        chat_id: &'a str,
        node_ids: &'a [String],
    ) -> impl Future<Output = Result<Vec<(String, NodeMergeStats)>>> + Send + 'a {
        let _ = (chat_id, node_ids);
        async { Ok(Vec::new()) }
    }

    /// Decision 74 / graph-spec Section 7.7 step 3: merges the loser
    /// node into the survivor node. Snapshots the loser and all its
    /// edges BEFORE any mutation, re-points every loser edge to the
    /// survivor (dropping loser↔survivor self-loops, deduping against
    /// the survivor's equivalent edges), then hard-deletes the loser.
    /// A missing loser or survivor is a loud error — re-merging a
    /// tombstoned loser errors loudly (Section 7.7 step 5). Survivor
    /// selection (higher edge degree, tiebreak older `created_at`) is
    /// the caller's job; this method takes the decision as given.
    ///
    /// Equivalent to [`MemoryBackend::merge_nodes_with_registry`] with an
    /// EMPTY single-value registry: no single-value invariant pass runs
    /// (the decision-74 behavior, preserved exactly). Kept as a delegate
    /// so existing callers compile unchanged.
    ///
    /// The default errors loudly so that noop test doubles stay
    /// source-compatible with the extended trait: a silent `Ok` would
    /// pretend a destructive operation succeeded.
    fn merge_nodes<'a>(
        &'a self,
        chat_id: &'a str,
        loser_id: &'a str,
        survivor_id: &'a str,
    ) -> impl Future<Output = Result<MergeOutcome>> + Send + 'a {
        let _ = (chat_id, loser_id, survivor_id);
        async {
            Err(MemoryError::Backend(
                "merge_nodes is not implemented by this backend".to_string(),
            ))
        }
    }

    /// Decision 75 / graph-spec Section 7.7: merges the loser node into
    /// the survivor node, then enforces the single-value invariant on the
    /// survivor.
    ///
    /// The merge itself is identical to [`MemoryBackend::merge_nodes`].
    /// AFTER re-pointing (at the END of the same transaction), for every
    /// predicate in `single_value_predicates` where the survivor now
    /// carries TWO OR MORE valid edges, every valid edge except the
    /// NEWEST by `valid_at` is invalidated. This closes the hole where
    /// loser and survivor each had one valid same-predicate edge and the
    /// re-point leaves two on the survivor. The invariant holds globally,
    /// not just at the digest path (decision 75 (c)).
    ///
    /// Tiebreak (deterministic): the newest `valid_at` wins; on a
    /// `valid_at` tie the lexicographically SMALLEST edge-id string wins
    /// (the same natural-key ordering the manual ops use), so the kept
    /// edge is reproducible. Each invalidation is counted into
    /// `MergeOutcome::single_value_invalidated` and DEBUG-logged. An
    /// EMPTY registry skips the pass entirely (the decision-74 behavior).
    ///
    /// The default errors loudly so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn merge_nodes_with_registry<'a>(
        &'a self,
        chat_id: &'a str,
        loser_id: &'a str,
        survivor_id: &'a str,
        single_value_predicates: &'a [String],
    ) -> impl Future<Output = Result<MergeOutcome>> + Send + 'a {
        let _ = (chat_id, loser_id, survivor_id, single_value_predicates);
        async {
            Err(MemoryError::Backend(
                "merge_nodes_with_registry is not implemented by this backend".to_string(),
            ))
        }
    }

    /// Decision 75 / Section 7.5: the manual invalidation of one edge
    /// (the `--invalidate <chat_id> <edge_id>` offline command).
    /// `edge_id` is the opaque [`EdgeId`] string emitted by
    /// [`MemoryBackend::node_facts`]. The edge is addressed by its
    /// natural key — the EDGE table has no id column (Section 6.1).
    ///
    /// Contract: a malformed edge id is a loud error
    /// ([`EdgeId::decode`]); a well-formed id whose natural key matches
    /// no edge row is a loud error (never a silent no-op on the wrong
    /// edge); an already-invalid edge is a no-op — `Ok(false)` plus a
    /// DEBUG note. Returns `Ok(true)` when this call set `invalid_at`
    /// AND `updated_at` to `now`. Invalidation is non-destructive and
    /// self-recording (decision 75 (d)): it lives on the edge row
    /// itself, no audit table.
    ///
    /// The default errors loudly so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn invalidate_edge<'a>(
        &'a self,
        chat_id: &'a str,
        edge_id: &'a str,
        now: OffsetDateTime,
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        let _ = (chat_id, edge_id, now);
        async {
            Err(MemoryError::Backend(
                "invalidate_edge is not implemented by this backend".to_string(),
            ))
        }
    }

    /// Decision 75 / Section 7.5: the manual re-validation of one edge
    /// (the `--revalidate <chat_id> <edge_id>` offline command — the
    /// typo safety net). Clears `invalid_at` (sets it to NULL) and
    /// refreshes `updated_at` to `now`, addressed by the natural key,
    /// with the same loud-error contract as
    /// [`MemoryBackend::invalidate_edge`]: a malformed id or an unknown
    /// natural key errors loudly; an already-valid edge is a no-op
    /// (`Ok(false)` plus a DEBUG note). Returns `Ok(true)` when this
    /// call cleared `invalid_at`.
    ///
    /// Re-validation is PLAIN: it does NOT invalidate single-value
    /// siblings. Decision 75 specifies the command as the undo of an
    /// invalidation and rules no sibling pass, so a re-validated edge
    /// may temporarily leave TWO valid edges for one (subject,
    /// predicate) pair; the next single-value write or a
    /// [`MemoryBackend::merge_nodes_with_registry`] restores the
    /// invariant. The operator's explicit correction wins.
    ///
    /// The default errors loudly so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn revalidate_edge<'a>(
        &'a self,
        chat_id: &'a str,
        edge_id: &'a str,
        now: OffsetDateTime,
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        let _ = (chat_id, edge_id, now);
        async {
            Err(MemoryError::Backend(
                "revalidate_edge is not implemented by this backend".to_string(),
            ))
        }
    }

    /// Decision 75 / Section 7.5: the fact listing of one node (the
    /// `--facts <chat_id> <name>` offline command). Every edge of the
    /// resolved node, BOTH directions, valid AND invalid, each with the
    /// opaque [`EdgeId`] (hand it to `--invalidate` / `--revalidate`)
    /// and the other endpoint's name.
    ///
    /// Entry resolution is the exact-alias machinery of Section 7.4
    /// step 2: the name is normalized per `identifiers.rs` into the
    /// deterministic alias identifier (Rule R5 — no fuzzy scans), and
    /// the alias targets pick the entry: exactly one target → the
    /// target node; otherwise the alias node itself when it exists —
    /// an ambiguous alias (mirroring the recall entry resolution) or a
    /// target-less alias carrying fallback-attached facts (Section 7.4
    /// step 4). `None` means the name is unknown (no alias node).
    ///
    /// The default errors loudly so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn node_facts<'a>(
        &'a self,
        chat_id: &'a str,
        name_or_alias: &'a str,
    ) -> impl Future<Output = Result<Option<NodeFacts>>> + Send + 'a {
        let _ = (chat_id, name_or_alias);
        async {
            Err(MemoryError::Backend(
                "node_facts is not implemented by this backend".to_string(),
            ))
        }
    }

    /// Decision 74 / graph-spec Section 7.7 step 2: the `related`
    /// verdict of the merge tool links the two nodes with an
    /// `also_known_as` edge. The direction mirrors the resolve path of
    /// Section 7.4 step 5 (the entity is the SOURCE of its
    /// `also_known_as` edge), so the edge runs `a_id -> b_id`; one
    /// direction is enough because the read paths match alias edges in
    /// both directions. Idempotent: an existing edge is kept. A missing
    /// endpoint is a loud error.
    ///
    /// The default errors loudly so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn link_also_known_as<'a>(
        &'a self,
        chat_id: &'a str,
        a_id: &'a str,
        b_id: &'a str,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        let _ = (chat_id, a_id, b_id);
        async {
            Err(MemoryError::Backend(
                "link_also_known_as is not implemented by this backend".to_string(),
            ))
        }
    }

    /// Decision 74 / graph-spec Section 7.7 step 4: rolls one merge
    /// back from its snapshot. Recreates the loser node with its
    /// original properties, recreates its original edges, and deletes
    /// the merge-created edges listed in the snapshot. Refuses loudly
    /// when the survivor no longer exists (chained-merge rollback is
    /// out of scope) and on a malformed snapshot.
    ///
    /// The default errors loudly so that noop test doubles stay
    /// source-compatible with the extended trait.
    fn rollback_merge<'a>(
        &'a self,
        chat_id: &'a str,
        snapshot_json: &'a str,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        let _ = (chat_id, snapshot_json);
        async {
            Err(MemoryError::Backend(
                "rollback_merge is not implemented by this backend".to_string(),
            ))
        }
    }

    /// Closes the cached handle of the group. Later calls reopen it.
    fn close<'a>(&'a self, chat_id: &'a str) -> impl Future<Output = Result<()>> + Send + 'a;
}
