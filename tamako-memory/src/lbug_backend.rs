//! The LadybugDB implementation of `MemoryBackend`, backed by the official
//! `lbug` crate. Refer to docs/adr-0001-ladybugdb-binding.md for the
//! binding decision.
//!
//! Deployment and connection management follow
//! proposed-graph-database-specs.md Section 5:
//!
//! - One database file per group at `{data_root}/{chat_id}/memory.lbug`
//!   (Section 5.1, Rule P5).
//! - Open on first use and cache the handle in the process (Section 5.2,
//!   rule 1). The cached handle is the `Database`. `lbug::Connection`
//!   borrows its `Database`, so a connection cannot be cached next to it
//!   without a self-referential structure. `Database` and `Connection` are
//!   both `Send + Sync`, and connection creation is cheap, so each blocking
//!   closure opens a fresh `Connection` on the cached `Database`.
//! - Every synchronous driver call runs inside
//!   `tokio::task::spawn_blocking` (Section 5.2 rule 3, AGENT.md
//!   Section 6.2).
//! - All user data goes through `$param` parameters (Section 5.2 rule 4).
//!   Timestamps use the native `Value::Timestamp` parameter of the driver.
//!   NULL values use `Value::Null` with the matching logical type.
//! - `CHECKPOINT` runs after each batch write (Section 5.2 rule 5).
//!
//! Concurrency: `Send + Sync` on the lbug types does NOT imply
//! read-during-write safety in lbug 0.18. The C++ storage layer races a
//! lock-free reader of `FileHandle::pageStates` against writer-side
//! `ConcurrentVector::resize` (annotated "Not thread-safe" upstream) and
//! the CHECKPOINT truncate path. The crash signature is a SIGSEGV in the
//! storage layer (`BufferManager::optimisticRead`,
//! `CSRNodeGroup::scanCommittedInMem`). Refer to
//! docs/adr-0001-ladybugdb-binding.md (addendum 2026-08-08). Tamako
//! serializes ALL per-group operations in `LbugBackend::with_conn`,
//! reads and CHECKPOINT included, as a binding-level requirement
//! (proposed-graph-database-specs.md Section 6.1 rule 3).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use lbug::{Connection, Database, LogicalType, PreparedStatement, SystemConfig, Value};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use tokio::sync::Mutex;

use crate::backend::{
    AliasTarget, CandidateEdge, EdgeId, MemoryBackend, MemoryBatch, MemoryError, MergeOutcome,
    MergeSnapshot, MergeSnapshotEdge, MergeSnapshotEdgeKey, MergeSnapshotNode, NeighborEdge,
    NodeContent, NodeFactEdge, NodeFacts, NodeMergeStats, NodeResolutionInfo, NodeType, Result,
    UpsertOutcome, NEIGHBOR_EXPANSION_LIMIT, RECALL_TIME_WINDOW_DAYS,
};

// The DDL of proposed-graph-database-specs.md Section 6.1, verbatim.
const DDL_NODE: &str = "CREATE NODE TABLE IF NOT EXISTS Node(
    id STRING PRIMARY KEY,
    name STRING,
    type STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    properties STRING
)";

const DDL_EDGE: &str = "CREATE REL TABLE IF NOT EXISTS EDGE(
    FROM Node TO Node,
    relationship_name STRING,
    valid_at TIMESTAMP,
    invalid_at TIMESTAMP,
    edge_text STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    properties STRING
)";

// The MessageBatch skeleton node. Section 7.2 step 5: a batch without
// extraction still stores the skeleton. The stable batch identifier makes
// the write idempotent (anti-defer list of dev-roadmap.md Section 7).
const MERGE_BATCH: &str = "MERGE (b:Node {id: $id})
ON CREATE SET b.name = $id, b.type = 'MessageBatch', b.created_at = $now, b.updated_at = $now
ON MATCH SET b.updated_at = $now";

// Node upsert. MERGE by the deterministic identifier. Rule R4: on match,
// update scalar columns only. A NULL properties parameter keeps the stored
// value through coalesce.
const MERGE_NODE: &str = "MERGE (n:Node {id: $id})
ON CREATE SET n.name = $name, n.type = $type, n.created_at = $created_at, n.updated_at = $updated_at, n.properties = $properties
ON MATCH SET n.name = $name, n.updated_at = $updated_at, n.properties = coalesce($properties, n.properties)";

// Edge upsert. The MERGE pattern carries the natural key of the edge:
// (source, target, relationship_name, valid_at). The endpoints match by
// identifier. Rule R5 applies: no full-graph scan entry.
//
// Replay convergence (decision 75 (b), Section 7.5): ON MATCH sets
// `r.invalid_at = $invalid_at` from the BATCH, and a replayed batch carries
// the edge's ORIGINAL `invalid_at` (NULL for a valid fact). So the replay of
// an edge that was invalidated in the meantime RE-VALIDATES it. Together with
// the INVALIDATE_SIBLINGS exclusion of the edge being written itself (full
// natural key), a replayed batch whose single-value edges have distinct
// (subject, predicate) pairs invalidates nothing on the second run and is a
// state-wise no-op: the deterministic natural key MERGEs and `valid_at`
// refreshes.
const MERGE_EDGE: &str = "MATCH (s:Node {id: $source_id}), (t:Node {id: $target_id})
MERGE (s)-[r:EDGE {relationship_name: $rel, valid_at: $valid_at}]->(t)
ON CREATE SET r.edge_text = $edge_text, r.invalid_at = $invalid_at, r.created_at = $created_at, r.updated_at = $updated_at, r.properties = $properties
ON MATCH SET r.edge_text = $edge_text, r.invalid_at = $invalid_at, r.updated_at = $updated_at, r.properties = coalesce($properties, r.properties)";

// Entity resolution, Section 7.4 step 2: the targets of one alias node.
// Alias edges are Person -> Alias (known_as) and Concept -> Alias
// (also_known_as), so the targets are the SOURCES of the edges into the
// alias node. The query enters the graph through the deterministic alias
// identifier (Rule R5); the alias id is a $param (Section 5.2 rule 4).
const ALIAS_TARGETS: &str = "MATCH (s:Node)-[r:EDGE]->(a:Node {id: $alias_id})
WHERE r.relationship_name IN ['known_as', 'also_known_as']
RETURN s.id, s.type";

// Read path, Section 8.2: the valid direct neighbors of one entry node,
// both directions in one query. The filter drops invalid edges and the
// `contains` provenance edges (Section 6.3/8.2). The query enters the
// graph through the node identifier (Rule R5); the node id is a $param
// (Section 5.2 rule 4). The LIMIT is interpolated at the call site from
// the trusted const NEIGHBOR_EXPANSION_LIMIT, the same "trusted values
// only" policy as `query_rows`.
const NEIGHBORS: &str = "MATCH (s:Node)-[r:EDGE]->(t:Node)
WHERE (s.id = $node_id OR t.id = $node_id)
  AND r.invalid_at IS NULL
  AND r.relationship_name <> 'contains'
RETURN s.id, s.name, t.id, t.name, r.relationship_name, r.edge_text, r.valid_at, r.created_at
ORDER BY r.created_at DESC
LIMIT ";

// Phase 2 (current-state.md decision 66): the stored display content of
// one node. The query enters the graph through the node identifier (Rule
// R5); the node id is a $param (Section 5.2 rule 4).
const NODE_CONTENT: &str = "MATCH (n:Node {id: $node_id})
RETURN n.name, n.properties";

// Phase 2 (current-state.md decision 66): the stored display content of
// every node of the group, for the startup reconciliation pass of the
// embedding worker. Node counts are hundreds-to-low-thousands, so the
// full scan needs no paging.
const LIST_NODE_CONTENTS: &str = "MATCH (n:Node)
RETURN n.id, n.name, n.properties";

// Decision 73: the vector pre-screen of entity resolution needs, per
// candidate node id, the node kind and, for Alias nodes, the bound
// target. One query covers the whole KNN overfetch: the id list is a
// single LIST parameter (Section 5.2 rule 4), so k round trips and k
// op-lock acquisitions collapse into one. The OPTIONAL MATCH mirrors
// ALIAS_TARGETS: alias edges point INTO the alias node, so the target
// is the SOURCE of a known_as/also_known_as edge. An alias carries at
// most one row per alias edge; ORDER BY makes the first-wins pick of
// the decoder deterministic.
const NODE_RESOLUTION_INFOS: &str = "MATCH (n:Node)
WHERE n.id IN $node_ids
OPTIONAL MATCH (s:Node)-[r:EDGE]->(n)
WHERE r.relationship_name IN ['known_as', 'also_known_as']
RETURN n.id, n.type, s.id
ORDER BY n.id, s.id";

// Decision 74 / Section 7.7 step 1: the merge-tool candidate scan
// excludes pairs already linked by known_as/also_known_as. Both
// relationship names, both directions — alias edges point INTO the
// alias and link_also_known_as writes a -> b, so direction must not
// matter.
const ARE_LINKED: &str = "MATCH (a:Node)-[r:EDGE]->(b:Node)
WHERE ((a.id = $a_id AND b.id = $b_id) OR (a.id = $b_id AND b.id = $a_id))
  AND r.relationship_name IN ['known_as', 'also_known_as']
RETURN count(r)";

// Decision 74 / Section 7.7 step 3: the survivor choice needs, per
// candidate node id, the edge degree (every EDGE row touching the node,
// both directions; DISTINCT keeps a self-loop at one) and the stored
// created_at. One query covers a whole candidate set: the id list is a
// single LIST parameter (Section 5.2 rule 4), the same shape as
// NODE_RESOLUTION_INFOS.
const NODE_MERGE_STATS: &str = "MATCH (n:Node)
WHERE n.id IN $node_ids
OPTIONAL MATCH (n)-[r:EDGE]-()
RETURN n.id, n.created_at, count(DISTINCT r)
ORDER BY n.id";

// Decision 74 / Section 7.7 step 3: the merge snapshot reads. Both
// enter the graph through the node identifier (Rule R5); the node id is
// a $param (Section 5.2 rule 4).
const SNAPSHOT_NODE: &str = "MATCH (n:Node {id: $node_id})
RETURN n.name, n.type, n.created_at, n.updated_at, n.properties";

const SNAPSHOT_EDGES: &str = "MATCH (s:Node)-[r:EDGE]->(t:Node)
WHERE s.id = $node_id OR t.id = $node_id
RETURN s.id, t.id, r.relationship_name, r.valid_at, r.invalid_at, r.edge_text, r.created_at, r.updated_at, r.properties
ORDER BY r.created_at, s.id, t.id, r.relationship_name";

// Decision 74: lbug cannot re-endpoint an edge, so re-pointing is
// copy-properties-CREATE-then-delete (Section 7.7 step 3). CREATE (not
// MERGE) keeps rollback exact: the snapshot records the natural key of
// every created edge, and rollback deletes exactly those. The dedup
// pass before the CREATE guarantees no natural-key collision with a
// pre-existing survivor edge.
const CREATE_EDGE: &str = "MATCH (s:Node {id: $source_id}), (t:Node {id: $target_id})
CREATE (s)-[:EDGE {relationship_name: $rel, valid_at: $valid_at, invalid_at: $invalid_at, edge_text: $edge_text, created_at: $created_at, updated_at: $updated_at, properties: $properties}]->(t)";

// Edge delete by natural key (source, relationship_name, target,
// valid_at), the same key shape as NeighborEdge::edge_id (Section 6.1).
const DELETE_EDGE: &str = "MATCH (s:Node {id: $source_id})-[r:EDGE]->(t:Node {id: $target_id})
WHERE r.relationship_name = $rel AND r.valid_at = $valid_at
DELETE r";

// Rollback (Section 7.7 step 4): recreate the loser node verbatim.
const CREATE_NODE: &str = "CREATE (n:Node {id: $id, name: $name, type: $type, created_at: $created_at, updated_at: $updated_at, properties: $properties})";

// Hard-delete tombstone (Section 7.7 step 3). DETACH DELETE is
// supported by the bundled lbug: lbug-src
// src/parser/transform/transform_updating_clause.cpp maps ctx.DETACH()
// to common::DeleteNodeType::DETACH_DELETE, executed by
// src/processor/operator/persistent/delete_executor.cpp. The merge
// already deleted every loser edge one by one, so DETACH is
// belt-and-braces for anything the snapshot read missed.
const DETACH_DELETE_NODE: &str = "MATCH (n:Node {id: $node_id}) DETACH DELETE n";

// Decision 74 / Section 7.7 step 2: the `related` verdict links the two
// nodes with also_known_as. The direction mirrors the resolve path of
// Section 7.4 step 5 (the entity is the SOURCE of its also_known_as
// edge), so the edge runs a -> b. MERGE on the relationship name makes
// the link idempotent: a repeat call matches the existing edge.
const LINK_ALSO_KNOWN_AS: &str = "MATCH (a:Node {id: $a_id}), (b:Node {id: $b_id})
MERGE (a)-[r:EDGE {relationship_name: 'also_known_as'}]->(b)
ON CREATE SET r.valid_at = $now, r.edge_text = $edge_text, r.created_at = $now, r.updated_at = $now";

// Decision 75 / Section 7.5 step 1: the single-value write-path
// invalidation. For a new single-value edge, invalidate every OTHER valid
// edge with the same (subject, relationship_name). The subject of a fact is
// the edge's SOURCE: the EDGE table is directed (`FROM Node TO Node`,
// Section 6.1) and Section 7.5 keys the invariant on the subject, so the
// match binds `s.id = $subject_id` with `$subject_id` = the new edge's
// `source_id`.
//
// The `NOT (o.id = $target_id AND r.valid_at = $valid_at)` exclusion spares
// the edge being written itself (its full natural key). That is what makes
// a REPLAYED batch converge: the replayed edge is not invalidated by its own
// pass, and its MERGE (step 2) re-validates it, so a re-run invalidates
// nothing and is a state-wise no-op. Without the exclusion a replay would
// invalidate-then-revalidate the same edge.
//
// `RETURN count(r)` yields the number of edges invalidated (feeds the
// `facts_invalidated_total` counter, decision 75 (e)). Verified against the
// real driver: the MATCH sees edges MERGEd earlier in the SAME transaction
// (read-your-own-writes), which is what makes "quit A, now at B" in one
// batch commit with the last write winning.
const INVALIDATE_SIBLINGS: &str = "MATCH (s:Node)-[r:EDGE]->(o:Node)
WHERE s.id = $subject_id AND r.relationship_name = $rel AND r.invalid_at IS NULL
  AND NOT (o.id = $target_id AND r.valid_at = $valid_at)
SET r.invalid_at = $now, r.updated_at = $now
RETURN count(r)";

// Decision 75: set the `invalid_at` of one edge addressed by its natural
// key (source, relationship_name, target, valid_at) — the EDGE table has no
// id column (Section 6.1), so the natural key IS the address. `$invalid_at`
// is a timestamp (invalidate) or NULL (revalidate). Shared by the manual
// `--invalidate` / `--revalidate` ops and the merge single-value invariant
// pass.
const SET_EDGE_INVALID_AT: &str =
    "MATCH (s:Node {id: $source_id})-[r:EDGE]->(t:Node {id: $target_id})
WHERE r.relationship_name = $rel AND r.valid_at = $valid_at
SET r.invalid_at = $invalid_at, r.updated_at = $updated_at";

// Decision 75: read one edge by natural key, returning its current
// `invalid_at` (NULL when the fact is valid). Zero rows means the edge does
// not exist — a loud error at the manual-op call sites, never a silent
// no-op on the wrong edge.
const FIND_EDGE_BY_KEY: &str = "MATCH (s:Node {id: $source_id})-[r:EDGE]->(t:Node {id: $target_id})
WHERE r.relationship_name = $rel AND r.valid_at = $valid_at
RETURN r.invalid_at";

// Decision 75 (`--facts`): every edge of one node, BOTH directions, valid
// AND invalid, with both endpoint names. The caller derives the opaque edge
// id from the natural key and marks the direction. `contains` provenance and
// alias edges are included on purpose: the operator sees the full picture.
const NODE_FACTS_EDGES: &str = "MATCH (s:Node)-[r:EDGE]->(t:Node)
WHERE s.id = $node_id OR t.id = $node_id
RETURN s.id, s.name, t.id, t.name, r.relationship_name, r.edge_text, r.valid_at, r.invalid_at
ORDER BY r.valid_at DESC, r.relationship_name, t.id";

// Decision 76 / Section 8.2: one per-node expansion query of the two-hop
// recall read. The SAME query serves hop 1 (the entry nodes) and hop 2
// (the hop-1 neighbor nodes); the caller loops over the node ids and
// dedupes the union by edge id. The whitelist drops `contains`
// (provenance) and `known_as` (surface forms, resolved at entry) and
// KEEPS `also_known_as` (the cross-language bridge, decision 76 (b)).
// Valid edges only; the Section 8.2 window (RECALL_TIME_WINDOW_DAYS)
// qualifies an edge recent by EITHER measure (`valid_at` or
// `created_at`). The query enters the graph through the node identifier
// (Rule R5); the node id and the cutoff are $params (Section 5.2 rule
// 4). The LIMIT is interpolated at the call site from the trusted
// per_node_limit argument, the same "trusted values only" policy as
// `query_rows` and NEIGHBORS.
const TWO_HOP_EDGES: &str = "MATCH (s:Node)-[r:EDGE]->(t:Node)
WHERE (s.id = $node_id OR t.id = $node_id)
  AND r.invalid_at IS NULL
  AND r.relationship_name <> 'contains'
  AND r.relationship_name <> 'known_as'
  AND (r.valid_at >= $cutoff OR r.created_at >= $cutoff)
RETURN s.id, s.name, t.id, t.name, r.relationship_name, r.edge_text, r.valid_at
ORDER BY r.created_at DESC
LIMIT ";

// Decision 76 (c): hydrate one sidecar `edge_texts` hit by the natural
// key of its edge (the decoded EdgeId), with both endpoint names. Valid
// edges only: a recall candidate must be a currently-valid fact, the
// same policy as every other candidate-producing read. Rule R5: entry
// through the endpoint identifiers; every value is a $param (Section
// 5.2 rule 4).
const EDGE_BY_KEY_HYDRATE: &str =
    "MATCH (s:Node {id: $source_id})-[r:EDGE]->(t:Node {id: $target_id})
WHERE r.relationship_name = $rel AND r.valid_at = $valid_at
  AND r.invalid_at IS NULL
RETURN s.name, t.name, r.edge_text";

// Decision 76 (c) / Section 7.6 step 6: EVERY edge of the group, valid
// and invalid, for the reconciliation diff of the `edge_texts` sidecar.
// Documented Rule R5 exception (the diff has no entry identifiers),
// mirroring LIST_NODE_CONTENTS.
const LIST_ALL_EDGES: &str = "MATCH (s:Node)-[r:EDGE]->(t:Node)
RETURN s.id, r.relationship_name, t.id, r.valid_at, r.edge_text";

// Decision 75 (c) / Section 7.7: the merge single-value invariant pass
// reads the survivor's currently VALID outgoing edges of one predicate.
// The pass runs at the END of the merge transaction, so the MATCH must see
// edges CREATEd earlier in the SAME transaction (read-your-own-writes,
// verified against the real driver for INVALIDATE_SIBLINGS). The winner
// selection and the invalidation of the rest happen in Rust
// (`enforce_single_value_invariant`): the tiebreak is the lexicographically
// smallest opaque edge-id string and every invalidation is DEBUG-logged.
const VALID_OUTGOING_EDGES: &str = "MATCH (s:Node {id: $subject_id})-[r:EDGE]->(o:Node)
WHERE r.relationship_name = $rel AND r.invalid_at IS NULL
RETURN o.id, r.valid_at";

fn backend(error: lbug::Error) -> MemoryError {
    MemoryError::Backend(error.to_string())
}

fn opt_string(value: &Option<String>) -> Value {
    match value {
        Some(value) => Value::String(value.clone()),
        None => Value::Null(LogicalType::String),
    }
}

fn opt_timestamp(value: Option<OffsetDateTime>) -> Value {
    match value {
        Some(value) => Value::Timestamp(value),
        None => Value::Null(LogicalType::Timestamp),
    }
}

/// Decision 66: the description lives in the `description` field of the
/// `properties` JSON blob (written by the digest pipeline). A NULL blob,
/// malformed JSON, or a missing/non-string field yields an empty
/// description — the read path skips, it does not fail (the same policy
/// as `alias_targets`).
fn description_of(properties: Option<String>) -> String {
    properties
        .and_then(|blob| serde_json::from_str::<serde_json::Value>(&blob).ok())
        .and_then(|value| value.get("description")?.as_str().map(str::to_string))
        .unwrap_or_default()
}

/// Decodes one `n.name, n.properties` (or `n.id, n.name, n.properties`)
/// column tail into a `NodeContent`. Returns `None` on an unexpected
/// shape: the row is skipped, it does not fail the query.
fn node_content_of(name: Option<Value>, properties: Option<Value>) -> Option<NodeContent> {
    let Some(Value::String(name)) = name else {
        return None;
    };
    let properties = match properties {
        Some(Value::String(blob)) => Some(blob),
        _ => None,
    };
    let description = description_of(properties);
    Some(NodeContent { name, description })
}

/// Renders one `lbug::Value` as a display string. Used by the read
/// helper `LbugBackend::query_rows`.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Int64(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Timestamp(value) => value
            .format(&Rfc3339)
            .unwrap_or_else(|_| format!("{value:?}")),
        Value::Null(_) => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

/// Rule P5: the chat_id becomes a directory name. Reject separators and
/// parent references. Same validation as tamako-store.
fn validate_chat_id(chat_id: &str) -> Result<()> {
    if chat_id.is_empty()
        || chat_id.contains('/')
        || chat_id.contains('\\')
        || chat_id.contains('\0')
        || chat_id.contains("..")
    {
        return Err(MemoryError::InvalidChatId(chat_id.to_string()));
    }
    Ok(())
}

/// The `lbug`-backed memory backend. Refer to the module documentation.
pub struct LbugBackend {
    data_root: PathBuf,
    // Section 5.2 rule 1: cached database handles, one per group. The
    // mutex serializes open and close. One writer per group file is
    // guaranteed by LadybugDB itself (Section 5.1).
    databases: Mutex<HashMap<String, Arc<Database>>>,
    // Section 6.1 rule 3 / adr-0001 addendum 2026-08-08: one async mutex
    // per group. lbug 0.18 `Send + Sync` does not make a read concurrent
    // with a write safe, so `with_conn` holds this lock for the full
    // duration of each operation, reads and CHECKPOINT included. Entries
    // are never removed: a `close` with in-flight operations must not
    // create a second lock for the same group.
    op_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl LbugBackend {
    /// Creates a backend with databases under `data_root`.
    pub fn new(data_root: impl Into<PathBuf>) -> Self {
        LbugBackend {
            data_root: data_root.into(),
            databases: Mutex::new(HashMap::new()),
            op_locks: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the cached database of the group. Opens the database and
    /// creates the schema on first use (Section 5.2 rule 1).
    async fn database(&self, chat_id: &str) -> Result<Arc<Database>> {
        validate_chat_id(chat_id)?;
        let mut cache = self.databases.lock().await;
        if let Some(db) = cache.get(chat_id) {
            return Ok(db.clone());
        }
        let dir = self.data_root.join(chat_id);
        let path = dir.join("memory.lbug");
        let db = tokio::task::spawn_blocking(move || -> Result<Database> {
            std::fs::create_dir_all(&dir)?;
            let db = Database::new(&path, SystemConfig::default()).map_err(backend)?;
            // Section 6.1. IF NOT EXISTS makes the DDL idempotent.
            let conn = Connection::new(&db).map_err(backend)?;
            conn.query(DDL_NODE).map_err(backend)?;
            conn.query(DDL_EDGE).map_err(backend)?;
            drop(conn);
            Ok(db)
        })
        .await
        .map_err(|error| MemoryError::Backend(format!("task join error: {error}")))??;
        let db = Arc::new(db);
        cache.insert(chat_id.to_string(), db.clone());
        Ok(db)
    }

    /// Returns the per-group operation lock. Section 6.1 rule 3 and
    /// adr-0001 addendum 2026-08-08: all operations of one group run
    /// serialized, reads included.
    async fn op_lock(&self, chat_id: &str) -> Arc<Mutex<()>> {
        self.op_locks
            .lock()
            .await
            .entry(chat_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Runs `f` on a fresh connection of the group database, inside
    /// `tokio::task::spawn_blocking` (Section 5.2 rule 3). The per-group
    /// operation lock is held from before the spawn until the blocking
    /// join completes (Section 6.1 rule 3, adr-0001 addendum 2026-08-08).
    async fn with_conn<F, T>(&self, chat_id: &str, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let db = self.database(chat_id).await?;
        let op_lock = self.op_lock(chat_id).await;
        let _guard = op_lock.lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let conn = Connection::new(&db).map_err(backend)?;
            f(&conn)
        })
        .await
        .map_err(|error| MemoryError::Backend(format!("task join error: {error}")))?
    }

    /// Runs a read-only Cypher query and returns each row as display
    /// strings. Read-path support for tests and the demo. The full read
    /// path is Phase 1 M5 / Phase 2 (Section 8). Interpolate only
    /// trusted values into `cypher`.
    pub async fn query_rows(&self, chat_id: &str, cypher: &str) -> Result<Vec<Vec<String>>> {
        let cypher = cypher.to_string();
        self.with_conn(chat_id, move |conn| {
            let result = conn.query(&cypher).map_err(backend)?;
            let rows = result
                .map(|row| row.iter().map(value_to_string).collect())
                .collect();
            Ok(rows)
        })
        .await
    }
}

/// Decision 74: reads the stored node row for the merge snapshot.
/// Returns `None` when the node does not exist. Unlike the read paths,
/// a row of an unexpected shape is a LOUD error: merge is destructive
/// and must not snapshot partial data.
fn read_snapshot_node(conn: &Connection, node_id: &str) -> Result<Option<MergeSnapshotNode>> {
    let mut statement = conn.prepare(SNAPSHOT_NODE).map_err(backend)?;
    let mut result = conn
        .execute(
            &mut statement,
            vec![("node_id", Value::String(node_id.to_string()))],
        )
        .map_err(backend)?;
    // The id is the primary key: at most one row.
    let Some(row) = result.next() else {
        return Ok(None);
    };
    let mut columns = row.into_iter();
    let decoded = (
        columns.next(),
        columns.next(),
        columns.next(),
        columns.next(),
        columns.next(),
    );
    match decoded {
        (
            Some(Value::String(name)),
            Some(Value::String(kind)),
            Some(Value::Timestamp(created_at)),
            Some(Value::Timestamp(updated_at)),
            properties,
        ) => {
            let properties = match properties {
                Some(Value::String(blob)) => Some(blob),
                _ => None,
            };
            Ok(Some(MergeSnapshotNode {
                id: node_id.to_string(),
                kind,
                name,
                created_at,
                updated_at,
                properties,
            }))
        }
        _ => Err(MemoryError::Backend(format!(
            "merge snapshot: unexpected node row shape for {node_id}"
        ))),
    }
}

/// Decision 75 (`--facts`): the stored name of one node, for the
/// `node_facts` entry. Returns `None` when the node does not exist.
fn read_node_name(conn: &Connection, node_id: &str) -> Result<Option<String>> {
    Ok(read_snapshot_node(conn, node_id)?.map(|node| node.name))
}

/// Decision 74: reads every edge of the node, both directions, with all
/// stored columns, for the merge snapshot. A row of an unexpected shape
/// is a LOUD error (same policy as `read_snapshot_node`).
fn read_snapshot_edges(conn: &Connection, node_id: &str) -> Result<Vec<MergeSnapshotEdge>> {
    let mut statement = conn.prepare(SNAPSHOT_EDGES).map_err(backend)?;
    let result = conn
        .execute(
            &mut statement,
            vec![("node_id", Value::String(node_id.to_string()))],
        )
        .map_err(backend)?;
    let mut edges = Vec::new();
    for row in result {
        let mut columns = row.into_iter();
        let decoded = (
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
        );
        match decoded {
            (
                Some(Value::String(source_id)),
                Some(Value::String(target_id)),
                Some(Value::String(relationship_name)),
                Some(Value::Timestamp(valid_at)),
                invalid_at,
                Some(Value::String(edge_text)),
                Some(Value::Timestamp(created_at)),
                Some(Value::Timestamp(updated_at)),
                properties,
            ) => {
                let invalid_at = match invalid_at {
                    Some(Value::Timestamp(invalid_at)) => Some(invalid_at),
                    _ => None,
                };
                let properties = match properties {
                    Some(Value::String(blob)) => Some(blob),
                    _ => None,
                };
                edges.push(MergeSnapshotEdge {
                    source_id,
                    target_id,
                    relationship_name,
                    valid_at,
                    invalid_at,
                    edge_text,
                    created_at,
                    updated_at,
                    properties,
                });
            }
            _ => {
                return Err(MemoryError::Backend(format!(
                    "merge snapshot: unexpected edge row shape for node {node_id}"
                )))
            }
        }
    }
    Ok(edges)
}

/// Creates one edge with the given columns. Used for the re-pointed
/// edges of a merge (endpoints swapped to the survivor) and for the
/// verbatim restore of a rollback.
fn create_edge(
    conn: &Connection,
    statement: &mut PreparedStatement,
    edge: &MergeSnapshotEdge,
) -> Result<()> {
    conn.execute(
        statement,
        vec![
            ("source_id", Value::String(edge.source_id.clone())),
            ("target_id", Value::String(edge.target_id.clone())),
            ("rel", Value::String(edge.relationship_name.clone())),
            ("valid_at", Value::Timestamp(edge.valid_at)),
            ("invalid_at", opt_timestamp(edge.invalid_at)),
            ("edge_text", Value::String(edge.edge_text.clone())),
            ("created_at", Value::Timestamp(edge.created_at)),
            ("updated_at", Value::Timestamp(edge.updated_at)),
            ("properties", opt_string(&edge.properties)),
        ],
    )
    .map_err(backend)?;
    Ok(())
}

/// Deletes every edge matching the natural key (source,
/// relationship_name, target, valid_at), the same key shape as
/// `NeighborEdge::edge_id` (Section 6.1). Used for the old loser edges
/// of a merge and for the merge-created edges of a rollback.
fn delete_edge(
    conn: &Connection,
    statement: &mut PreparedStatement,
    source_id: &str,
    relationship_name: &str,
    target_id: &str,
    valid_at: OffsetDateTime,
) -> Result<()> {
    conn.execute(
        statement,
        vec![
            ("source_id", Value::String(source_id.to_string())),
            ("target_id", Value::String(target_id.to_string())),
            ("rel", Value::String(relationship_name.to_string())),
            ("valid_at", Value::Timestamp(valid_at)),
        ],
    )
    .map_err(backend)?;
    Ok(())
}

/// Decision 75 / Section 7.5 step 1: invalidate every OTHER valid edge
/// with the same (subject, relationship_name) as the edge being written.
/// The subject is the edge's SOURCE. The exclusion clause spares the edge
/// being written itself (full natural key), which is what makes a replayed
/// batch converge. Returns the number of edges invalidated.
fn invalidate_siblings(
    conn: &Connection,
    statement: &mut PreparedStatement,
    subject_id: &str,
    relationship_name: &str,
    target_id: &str,
    valid_at: OffsetDateTime,
    now: OffsetDateTime,
) -> Result<u32> {
    let mut result = conn
        .execute(
            statement,
            vec![
                ("subject_id", Value::String(subject_id.to_string())),
                ("rel", Value::String(relationship_name.to_string())),
                ("target_id", Value::String(target_id.to_string())),
                ("valid_at", Value::Timestamp(valid_at)),
                ("now", Value::Timestamp(now)),
            ],
        )
        .map_err(backend)?;
    // `RETURN count(r)` always yields exactly one row.
    match result.next().and_then(|row| row.into_iter().next()) {
        Some(Value::Int64(count)) => Ok(u32::try_from(count).unwrap_or(0)),
        other => Err(MemoryError::Backend(format!(
            "invalidate_siblings: count() returned an unexpected shape: {other:?}"
        ))),
    }
}

/// Decision 75: set the `invalid_at` of one edge addressed by its natural
/// key (the [`EdgeId`]). `$invalid_at` is a timestamp (invalidate) or NULL
/// (revalidate).
fn set_edge_invalid_at(
    conn: &Connection,
    statement: &mut PreparedStatement,
    key: &EdgeId,
    invalid_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
) -> Result<()> {
    conn.execute(
        statement,
        vec![
            ("source_id", Value::String(key.source_id.clone())),
            ("target_id", Value::String(key.target_id.clone())),
            ("rel", Value::String(key.relationship_name.clone())),
            ("valid_at", Value::Timestamp(key.valid_at)),
            ("invalid_at", opt_timestamp(invalid_at)),
            ("updated_at", Value::Timestamp(updated_at)),
        ],
    )
    .map_err(backend)?;
    Ok(())
}

/// Decision 75: read the current `invalid_at` of one edge by natural key.
/// Returns `Ok(None)` when the edge does not exist (zero rows); the manual
/// ops turn that into a loud error. Returns `Ok(Some(invalid_at))` when the
/// edge exists, where `invalid_at` is `None` for a currently-valid fact.
fn find_edge_invalid_at(
    conn: &Connection,
    statement: &mut PreparedStatement,
    source_id: &str,
    relationship_name: &str,
    target_id: &str,
    valid_at: OffsetDateTime,
) -> Result<Option<Option<OffsetDateTime>>> {
    let mut result = conn
        .execute(
            statement,
            vec![
                ("source_id", Value::String(source_id.to_string())),
                ("target_id", Value::String(target_id.to_string())),
                ("rel", Value::String(relationship_name.to_string())),
                ("valid_at", Value::Timestamp(valid_at)),
            ],
        )
        .map_err(backend)?;
    let Some(row) = result.next() else {
        return Ok(None);
    };
    let invalid_at = match row.into_iter().next() {
        Some(Value::Timestamp(invalid_at)) => Some(invalid_at),
        Some(Value::Null(_)) | None => None,
        other => {
            return Err(MemoryError::Backend(format!(
                "find_edge_invalid_at: unexpected invalid_at shape: {other:?}"
            )))
        }
    };
    Ok(Some(invalid_at))
}

/// Entity resolution, Section 7.4 step 2: the target nodes of one alias
/// node — the sources of the `known_as`/`also_known_as` edges into the
/// alias. Shared by `alias_targets` and the `node_facts` entry resolution
/// (the same step-2 machinery, decision 75 (d)). A row of an unexpected
/// shape or an unknown type string is skipped, it does not fail the query
/// (same policy as the read paths).
fn read_alias_targets(conn: &Connection, alias_node_id: &str) -> Result<Vec<AliasTarget>> {
    let mut statement = conn.prepare(ALIAS_TARGETS).map_err(backend)?;
    let result = conn
        .execute(
            &mut statement,
            vec![("alias_id", Value::String(alias_node_id.to_string()))],
        )
        .map_err(backend)?;
    let mut targets = Vec::new();
    for row in result {
        let mut columns = row.into_iter();
        if let (Some(Value::String(node_id)), Some(Value::String(type_string))) =
            (columns.next(), columns.next())
        {
            if let Some(node_type) = NodeType::from_str(&type_string) {
                targets.push(AliasTarget { node_id, node_type });
            }
        }
    }
    Ok(targets)
}

/// Decision 76: builds the [`CandidateEdge`] of one edge from its
/// natural key, the endpoint names, and the stored `edge_text`. The
/// `edge_id` is the [`EdgeId::encode`] of the natural key — the same
/// dedup key shape as `NeighborEdge::edge_id` (specs.md Section 9.3).
fn candidate_edge(
    source_id: String,
    source_name: String,
    target_id: String,
    target_name: String,
    relationship_name: String,
    edge_text: String,
    valid_at: OffsetDateTime,
) -> CandidateEdge {
    let edge_id = EdgeId {
        source_id: source_id.clone(),
        relationship_name: relationship_name.clone(),
        target_id: target_id.clone(),
        valid_at,
    }
    .encode();
    CandidateEdge {
        edge_id,
        source_id,
        source_name,
        target_id,
        target_name,
        relationship_name,
        edge_text,
        valid_at,
    }
}

/// Decision 76 / Section 8.2: runs one per-node expansion query of the
/// two-hop read and decodes the rows. Shared by hop 1 (the entry nodes)
/// and hop 2 (the hop-1 neighbor nodes). A row of an unexpected shape is
/// skipped, it does not fail the query (same policy as `neighbors`).
fn read_expansion_edges(
    conn: &Connection,
    statement: &mut PreparedStatement,
    node_id: &str,
    cutoff: OffsetDateTime,
) -> Result<Vec<CandidateEdge>> {
    let result = conn
        .execute(
            statement,
            vec![
                ("node_id", Value::String(node_id.to_string())),
                ("cutoff", Value::Timestamp(cutoff)),
            ],
        )
        .map_err(backend)?;
    let mut edges = Vec::new();
    for row in result {
        let mut columns = row.into_iter();
        let decoded = (
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
            columns.next(),
        );
        let (
            Some(Value::String(source_id)),
            Some(Value::String(source_name)),
            Some(Value::String(target_id)),
            Some(Value::String(target_name)),
            Some(Value::String(relationship_name)),
            Some(Value::String(edge_text)),
            Some(Value::Timestamp(valid_at)),
        ) = decoded
        else {
            continue;
        };
        edges.push(candidate_edge(
            source_id,
            source_name,
            target_id,
            target_name,
            relationship_name,
            edge_text,
            valid_at,
        ));
    }
    Ok(edges)
}

/// Decision 75 (c) / Section 7.7: the merge single-value invariant pass.
/// Runs at the END of the merge transaction, so the MATCH sees the edges
/// CREATEd by the re-point earlier in the SAME transaction
/// (read-your-own-writes, verified against the real driver for
/// INVALIDATE_SIBLINGS). For every registry predicate where the survivor
/// carries TWO OR MORE valid OUTGOING edges, keeps the NEWEST by
/// `valid_at` and invalidates the rest. The subject of a single-value fact
/// is the edge's SOURCE (Section 6.1 directed `FROM Node TO Node`, Section
/// 7.5 keys the invariant on the subject), so only OUTGOING survivor edges
/// are considered.
///
/// Tiebreak (deterministic): the newest `valid_at` wins; on a `valid_at`
/// tie the lexicographically SMALLEST opaque edge-id string wins — the
/// same natural-key ordering the manual ops use. Each invalidation is
/// counted and DEBUG-logged. Returns the number of edges invalidated.
fn enforce_single_value_invariant(
    conn: &Connection,
    survivor_id: &str,
    single_value_predicates: &[String],
    now: OffsetDateTime,
) -> Result<u32> {
    let mut read_statement = conn.prepare(VALID_OUTGOING_EDGES).map_err(backend)?;
    let mut set_statement = conn.prepare(SET_EDGE_INVALID_AT).map_err(backend)?;
    let mut invalidated_total = 0u32;
    let mut seen: Vec<&str> = Vec::new();
    for predicate in single_value_predicates {
        // A duplicate registry entry would only find the single remaining
        // valid edge on its second pass; skip it anyway.
        if seen.contains(&predicate.as_str()) {
            continue;
        }
        seen.push(predicate.as_str());
        let result = conn
            .execute(
                &mut read_statement,
                vec![
                    ("subject_id", Value::String(survivor_id.to_string())),
                    ("rel", Value::String(predicate.clone())),
                ],
            )
            .map_err(backend)?;
        // Every valid outgoing edge of the predicate, as opaque edge ids.
        let mut edges: Vec<EdgeId> = Vec::new();
        for row in result {
            let mut columns = row.into_iter();
            // A row of an unexpected shape is a LOUD error: the invariant
            // pass must not skip an edge it cannot address.
            let (Some(Value::String(target_id)), Some(Value::Timestamp(valid_at))) =
                (columns.next(), columns.next())
            else {
                return Err(MemoryError::Backend(format!(
                    "merge single-value pass: unexpected edge row shape for survivor {survivor_id}"
                )));
            };
            edges.push(EdgeId {
                source_id: survivor_id.to_string(),
                relationship_name: predicate.clone(),
                target_id,
                valid_at,
            });
        }
        if edges.len() < 2 {
            continue;
        }
        // Winner: newest valid_at; on a tie the smallest edge-id string.
        let mut ranked: Vec<(String, &EdgeId)> =
            edges.iter().map(|edge| (edge.encode(), edge)).collect();
        ranked.sort_by(|left, right| {
            right
                .1
                .valid_at
                .cmp(&left.1.valid_at)
                .then_with(|| left.0.cmp(&right.0))
        });
        let (winner_edge_id, _) = &ranked[0];
        for (edge_id, edge) in &ranked[1..] {
            set_edge_invalid_at(conn, &mut set_statement, edge, Some(now), now)?;
            invalidated_total += 1;
            tracing::debug!(
                survivor_id = %survivor_id,
                relationship_name = %predicate,
                edge_id = %edge_id,
                winner_edge_id = %winner_edge_id,
                "merge single-value invariant pass invalidated a stale edge"
            );
        }
    }
    Ok(invalidated_total)
}

/// One endpoint's view of an edge, for the re-point dedup of Section
/// 7.7 step 3.
struct EndpointEdge {
    /// True when the endpoint is the SOURCE of the edge.
    outgoing: bool,
    other_id: String,
    relationship_name: String,
    edge_text: String,
    valid_at: OffsetDateTime,
}

impl EndpointEdge {
    /// The view of `edge` from `node_id`, which must be one of its
    /// endpoints.
    fn from_endpoint(node_id: &str, edge: &MergeSnapshotEdge) -> Self {
        let outgoing = edge.source_id == node_id;
        EndpointEdge {
            outgoing,
            other_id: if outgoing {
                edge.target_id.clone()
            } else {
                edge.source_id.clone()
            },
            relationship_name: edge.relationship_name.clone(),
            edge_text: edge.edge_text.clone(),
            valid_at: edge.valid_at,
        }
    }
}

/// Section 7.7 step 3: equivalent means same direction, same
/// relationship name, same other endpoint, and same description text
/// (the stored `edge_text`; the properties-description convention of
/// decision 66 applies to nodes, and the write path does not use edge
/// properties blobs as descriptions). A full natural-key match (same
/// `valid_at` on an otherwise identical re-pointed edge) also counts,
/// because the re-point CREATE must never duplicate the natural key of
/// a pre-existing survivor edge: rollback deletes by natural key and
/// would take the pre-existing edge with it.
fn edges_equivalent(a: &EndpointEdge, b: &EndpointEdge) -> bool {
    a.outgoing == b.outgoing
        && a.other_id == b.other_id
        && a.relationship_name == b.relationship_name
        && (a.edge_text == b.edge_text || a.valid_at == b.valid_at)
}

/// Runs `f` inside one lbug write transaction, with the upsert_batch
/// shape: BEGIN TRANSACTION, COMMIT on success, ROLLBACK on error, and
/// CHECKPOINT after the commit (Section 7.6 step 4, Section 5.2 rule 5).
/// The merge tool's reads happen BEFORE the transaction begins (the
/// snapshot must precede any mutation), so the transaction itself is
/// write-only and multi-statement-transaction intermediate reads never
/// come up. Returns whatever `f` returns (the decision-75 write path
/// returns the invalidation count).
fn transact<T>(conn: &Connection, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    conn.query("BEGIN TRANSACTION").map_err(backend)?;
    let value = match f(conn) {
        Ok(value) => value,
        Err(error) => {
            let _ = conn.query("ROLLBACK");
            return Err(error);
        }
    };
    conn.query("COMMIT").map_err(backend)?;
    conn.query("CHECKPOINT").map_err(backend)?;
    Ok(value)
}

/// Writes the batch inside one transaction. Section 7.6 step 4: CHECKPOINT
/// at the end.
///
/// `single_value_predicates` is the decision-75 registry. When a batch edge's
/// `relationship_name` is in the registry, the write path first invalidates
/// every OTHER valid edge with the same (subject = the edge's SOURCE,
/// relationship_name) and then MERGEs the new edge — all in batch order, so
/// one batch carrying a change commits with exactly one valid edge (the last
/// write wins). An EMPTY registry performs no invalidation (the decision-74
/// behavior). Returns the number of edges invalidated.
fn write_batch(
    conn: &Connection,
    batch: &MemoryBatch,
    single_value_predicates: &[String],
) -> Result<u32> {
    let now = OffsetDateTime::now_utc();

    let mut statement = conn.prepare(MERGE_BATCH).map_err(backend)?;
    conn.execute(
        &mut statement,
        vec![
            ("id", Value::String(batch.batch_id.clone())),
            ("now", Value::Timestamp(now)),
        ],
    )
    .map_err(backend)?;

    let mut statement = conn.prepare(MERGE_NODE).map_err(backend)?;
    for node in &batch.nodes {
        conn.execute(
            &mut statement,
            vec![
                ("id", Value::String(node.id.clone())),
                ("name", Value::String(node.name.clone())),
                ("type", Value::String(node.node_type.as_str().to_string())),
                ("created_at", Value::Timestamp(node.created_at)),
                ("updated_at", Value::Timestamp(node.updated_at)),
                ("properties", opt_string(&node.properties)),
            ],
        )
        .map_err(backend)?;
    }

    // Decision 75: the single-value invalidation statement is prepared once
    // and reused for every single-value edge of the batch. An empty registry
    // never prepares it.
    let mut invalidate_statement = if single_value_predicates.is_empty() {
        None
    } else {
        Some(conn.prepare(INVALIDATE_SIBLINGS).map_err(backend)?)
    };

    let mut statement = conn.prepare(MERGE_EDGE).map_err(backend)?;
    let mut invalidated_total = 0u32;
    for edge in &batch.edges {
        // Decision 75 / Section 7.5: process each new single-value edge in
        // batch order — invalidate the siblings first, then MERGE the new
        // edge. Both steps run inside this single transaction, so the
        // invariant holds at commit.
        if let Some(invalidate_statement) = invalidate_statement.as_mut() {
            if single_value_predicates.contains(&edge.relationship_name) {
                let invalidated = invalidate_siblings(
                    conn,
                    invalidate_statement,
                    &edge.source_id,
                    &edge.relationship_name,
                    &edge.target_id,
                    edge.valid_at,
                    now,
                )?;
                if invalidated > 0 {
                    tracing::debug!(
                        subject_id = %edge.source_id,
                        relationship_name = %edge.relationship_name,
                        invalidated,
                        "single-value write path invalidated sibling edges"
                    );
                }
                invalidated_total += invalidated;
            }
        }
        conn.execute(
            &mut statement,
            vec![
                ("source_id", Value::String(edge.source_id.clone())),
                ("target_id", Value::String(edge.target_id.clone())),
                ("rel", Value::String(edge.relationship_name.clone())),
                ("valid_at", Value::Timestamp(edge.valid_at)),
                ("invalid_at", opt_timestamp(edge.invalid_at)),
                ("edge_text", Value::String(edge.edge_text.clone())),
                ("created_at", Value::Timestamp(edge.created_at)),
                ("updated_at", Value::Timestamp(edge.updated_at)),
                ("properties", opt_string(&edge.properties)),
            ],
        )
        .map_err(backend)?;
    }
    Ok(invalidated_total)
}

impl MemoryBackend for LbugBackend {
    async fn ensure_schema(&self, chat_id: &str) -> Result<()> {
        // Open runs the idempotent DDL of Section 6.1.
        self.database(chat_id).await.map(|_| ())
    }

    async fn upsert_batch(&self, chat_id: &str, batch: &MemoryBatch) -> Result<()> {
        // Additive-seam delegate: the empty registry performs no
        // invalidation and discards the count, preserving the decision-74
        // behavior (and the `Result<()>` return) exactly.
        self.upsert_batch_with_registry(chat_id, batch, &[])
            .await
            .map(|_| ())
    }

    /// Decision 75 / Section 7.5: the single-value write path. See the
    /// trait doc; the invalidation and the MERGE run in batch order inside
    /// the single `transact` transaction, so exactly one valid edge per
    /// (subject, predicate) survives at commit and a replayed batch
    /// converges.
    async fn upsert_batch_with_registry(
        &self,
        chat_id: &str,
        batch: &MemoryBatch,
        single_value_predicates: &[String],
    ) -> Result<UpsertOutcome> {
        let batch = batch.clone();
        let registry = single_value_predicates.to_vec();
        let invalidated = self
            .with_conn(chat_id, move |conn| {
                // Section 5.2 rule 5: CHECKPOINT after each batch write
                // (inside `transact`).
                transact(conn, |conn| {
                    let invalidated = write_batch(conn, &batch, &registry)?;
                    Ok(invalidated)
                })
            })
            .await?;
        Ok(UpsertOutcome { invalidated })
    }

    async fn checkpoint(&self, chat_id: &str) -> Result<()> {
        self.with_conn(chat_id, |conn| {
            conn.query("CHECKPOINT").map_err(backend)?;
            Ok(())
        })
        .await
    }

    async fn alias_targets(&self, chat_id: &str, alias_node_id: &str) -> Result<Vec<AliasTarget>> {
        // Section 7.4 step 2. An empty result means the alias is unknown.
        let alias_node_id = alias_node_id.to_string();
        self.with_conn(chat_id, move |conn| {
            read_alias_targets(conn, &alias_node_id)
        })
        .await
    }

    async fn neighbors(&self, chat_id: &str, node_id: &str) -> Result<Vec<NeighborEdge>> {
        // Section 8.2. An empty result means the node id is unknown or
        // has no valid non-contains edges.
        let node_id = node_id.to_string();
        let cypher = format!("{NEIGHBORS}{NEIGHBOR_EXPANSION_LIMIT}");
        self.with_conn(chat_id, move |conn| {
            let mut statement = conn.prepare(&cypher).map_err(backend)?;
            let result = conn
                .execute(
                    &mut statement,
                    vec![("node_id", Value::String(node_id.clone()))],
                )
                .map_err(backend)?;
            let mut edges = Vec::new();
            for row in result {
                let mut columns = row.into_iter();
                let decoded = (
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                );
                // A row with an unexpected shape is skipped, it does not
                // fail the query (same policy as `alias_targets`).
                let (
                    Some(Value::String(source_id)),
                    Some(Value::String(source_name)),
                    Some(Value::String(target_id)),
                    Some(Value::String(target_name)),
                    Some(Value::String(relationship_name)),
                    Some(Value::String(edge_text)),
                    Some(Value::Timestamp(valid_at)),
                    Some(Value::Timestamp(created_at)),
                ) = decoded
                else {
                    continue;
                };
                // Section 8.2: the caller expands FROM the entry node, so
                // report the endpoint that is not the queried node.
                let (other_node_id, other_node_name) = if source_id == node_id {
                    (target_id.clone(), target_name)
                } else {
                    (source_id.clone(), source_name)
                };
                edges.push(NeighborEdge {
                    source_id,
                    target_id,
                    relationship_name,
                    edge_text,
                    valid_at,
                    created_at,
                    other_node_id,
                    other_node_name,
                });
            }
            Ok(edges)
        })
        .await
    }

    /// Decision 76 / Section 8.2: the two-hop recall expansion. See the
    /// trait doc for the rules. Fan-out shape: ONE `with_conn`
    /// acquisition (decision 47) holds the per-group lock for the whole
    /// read; inside it, hop 1 runs the TWO_HOP_EDGES query once per
    /// entry node (per-node LIMIT interpolated), then hop 2 runs the
    /// SAME query once per hop-1 neighbor node (per-node LIMIT
    /// interpolated again). The union is deduped by edge id: an edge
    /// between two queried nodes surfaces exactly once.
    async fn two_hop_edges(
        &self,
        chat_id: &str,
        entry_node_ids: &[String],
        now: OffsetDateTime,
        per_node_limit: usize,
    ) -> Result<Vec<CandidateEdge>> {
        // An empty entry list short-circuits without opening the
        // database of the group (same policy as node_resolution_infos).
        if entry_node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let entry_node_ids = entry_node_ids.to_vec();
        let cypher = format!("{TWO_HOP_EDGES}{per_node_limit}");
        self.with_conn(chat_id, move |conn| {
            let cutoff = now - Duration::days(RECALL_TIME_WINDOW_DAYS);
            let mut statement = conn.prepare(&cypher).map_err(backend)?;
            let mut candidates = Vec::new();
            // Dedup sets: `seen_nodes` guards against re-querying a node
            // (an entry id that is also a hop-1 neighbor, a duplicate
            // entry id); `seen_edges` dedupes the union by edge id.
            let mut seen_nodes: HashSet<String> = entry_node_ids.iter().cloned().collect();
            let mut seen_edges: HashSet<String> = HashSet::new();
            // Hop 1: the entry nodes. The frontier collects the hop-1
            // neighbor nodes in first-seen order.
            let mut frontier: Vec<String> = Vec::new();
            for node_id in &entry_node_ids {
                for edge in read_expansion_edges(conn, &mut statement, node_id, cutoff)? {
                    let other = if edge.source_id == *node_id {
                        edge.target_id.clone()
                    } else {
                        edge.source_id.clone()
                    };
                    if seen_nodes.insert(other.clone()) {
                        frontier.push(other);
                    }
                    if seen_edges.insert(edge.edge_id.clone()) {
                        candidates.push(edge);
                    }
                }
            }
            // Hop 2: the hop-1 neighbor nodes, same query, same per-node
            // limit. Every frontier node is unqueried by construction.
            for node_id in &frontier {
                for edge in read_expansion_edges(conn, &mut statement, node_id, cutoff)? {
                    if seen_edges.insert(edge.edge_id.clone()) {
                        candidates.push(edge);
                    }
                }
            }
            Ok(candidates)
        })
        .await
    }

    /// Decision 76 (c): hydrate sidecar `edge_texts` hits into full
    /// candidates. Serialized per group by `with_conn` (decision 47).
    async fn edges_by_ids(&self, chat_id: &str, edge_ids: &[String]) -> Result<Vec<CandidateEdge>> {
        if edge_ids.is_empty() {
            return Ok(Vec::new());
        }
        // Decode BEFORE opening the database. A malformed id is skipped
        // with a DEBUG note — the read path skips, it does not fail (a
        // corrupt sidecar row must not break the recall read); the loud
        // decode contract of EdgeId applies to the manual ops.
        let mut keys = Vec::new();
        for edge_id in edge_ids {
            match EdgeId::decode(edge_id) {
                Ok(key) => keys.push(key),
                Err(error) => {
                    tracing::debug!(edge_id = %edge_id, %error, "edges_by_ids: skipping a malformed edge id");
                }
            }
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.with_conn(chat_id, move |conn| {
            let mut statement = conn.prepare(EDGE_BY_KEY_HYDRATE).map_err(backend)?;
            let mut seen: HashSet<String> = HashSet::new();
            let mut candidates = Vec::new();
            for key in &keys {
                // Duplicate ids collapse to one candidate.
                if !seen.insert(key.encode()) {
                    continue;
                }
                let mut result = conn
                    .execute(
                        &mut statement,
                        vec![
                            ("source_id", Value::String(key.source_id.clone())),
                            ("target_id", Value::String(key.target_id.clone())),
                            ("rel", Value::String(key.relationship_name.clone())),
                            ("valid_at", Value::Timestamp(key.valid_at)),
                        ],
                    )
                    .map_err(backend)?;
                // The natural key addresses at most one edge row. Zero
                // rows: a stale id whose graph edge is gone (deleted or
                // invalidated between reconciliations, Section 7.6 step
                // 6) — it simply does not occur in the result.
                let Some(row) = result.next() else {
                    continue;
                };
                let mut columns = row.into_iter();
                let decoded = (columns.next(), columns.next(), columns.next());
                // A row of an unexpected shape is skipped, it does not
                // fail the query (same policy as `neighbors`).
                let (
                    Some(Value::String(source_name)),
                    Some(Value::String(target_name)),
                    Some(Value::String(edge_text)),
                ) = decoded
                else {
                    continue;
                };
                candidates.push(candidate_edge(
                    key.source_id.clone(),
                    source_name,
                    key.target_id.clone(),
                    target_name,
                    key.relationship_name.clone(),
                    edge_text,
                    key.valid_at,
                ));
            }
            Ok(candidates)
        })
        .await
    }

    /// Decision 76 (c) / Section 7.6 step 6: EVERY edge of the group,
    /// valid and invalid, for the reconciliation diff (the documented
    /// Rule R5 exception, mirroring `list_node_contents`). Serialized
    /// per group by `with_conn` (decision 47).
    async fn list_all_edges(&self, chat_id: &str) -> Result<Vec<(String, String)>> {
        self.with_conn(chat_id, |conn| {
            let result = conn.query(LIST_ALL_EDGES).map_err(backend)?;
            let mut edges = Vec::new();
            for row in result {
                let mut columns = row.into_iter();
                let decoded = (
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                );
                // A row of an unexpected shape is skipped, it does not
                // fail the listing (same policy as `neighbors`).
                let (
                    Some(Value::String(source_id)),
                    Some(Value::String(relationship_name)),
                    Some(Value::String(target_id)),
                    Some(Value::Timestamp(valid_at)),
                    Some(Value::String(edge_text)),
                ) = decoded
                else {
                    continue;
                };
                let edge_id = EdgeId {
                    source_id,
                    relationship_name,
                    target_id,
                    valid_at,
                }
                .encode();
                edges.push((edge_id, edge_text));
            }
            Ok(edges)
        })
        .await
    }

    async fn node_content(&self, chat_id: &str, node_id: &str) -> Result<Option<NodeContent>> {
        // Decision 66. None means the node does not exist.
        let node_id = node_id.to_string();
        self.with_conn(chat_id, move |conn| {
            let mut statement = conn.prepare(NODE_CONTENT).map_err(backend)?;
            let mut result = conn
                .execute(&mut statement, vec![("node_id", Value::String(node_id))])
                .map_err(backend)?;
            // The id is the primary key: at most one row.
            match result.next() {
                Some(row) => {
                    let mut columns = row.into_iter();
                    Ok(node_content_of(columns.next(), columns.next()))
                }
                None => Ok(None),
            }
        })
        .await
    }

    async fn list_node_contents(&self, chat_id: &str) -> Result<Vec<(String, NodeContent)>> {
        // Decision 66: the startup reconciliation pass lists every node.
        self.with_conn(chat_id, |conn| {
            let result = conn.query(LIST_NODE_CONTENTS).map_err(backend)?;
            let mut contents = Vec::new();
            for row in result {
                let mut columns = row.into_iter();
                let decoded = (columns.next(), columns.next(), columns.next());
                // A row with an unexpected shape is skipped, it does not
                // fail the query (same policy as `alias_targets`).
                if let (Some(Value::String(node_id)), name, properties) = decoded {
                    if let Some(content) = node_content_of(name, properties) {
                        contents.push((node_id, content));
                    }
                }
            }
            Ok(contents)
        })
        .await
    }

    async fn node_resolution_infos(
        &self,
        chat_id: &str,
        node_ids: &[String],
    ) -> Result<Vec<(String, NodeResolutionInfo)>> {
        // Decision 73. An empty pre-screen short-circuits without
        // opening the database of the group.
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let node_ids = node_ids.to_vec();
        self.with_conn(chat_id, move |conn| {
            let mut statement = conn.prepare(NODE_RESOLUTION_INFOS).map_err(backend)?;
            let result = conn
                .execute(
                    &mut statement,
                    vec![(
                        "node_ids",
                        Value::List(
                            LogicalType::String,
                            node_ids.into_iter().map(Value::String).collect(),
                        ),
                    )],
                )
                .map_err(backend)?;
            let mut infos: Vec<(String, NodeResolutionInfo)> = Vec::new();
            for row in result {
                let mut columns = row.into_iter();
                let decoded = (columns.next(), columns.next(), columns.next());
                // A row with an unexpected shape or an unknown type
                // string is skipped, it does not fail the query (same
                // policy as `alias_targets`).
                let (Some(Value::String(node_id)), Some(Value::String(type_string)), target) =
                    decoded
                else {
                    continue;
                };
                let Some(kind) = NodeType::from_str(&type_string) else {
                    continue;
                };
                let target = match target {
                    Some(Value::String(target)) => Some(target),
                    _ => None,
                };
                // ORDER BY n.id groups the rows of one node; first-wins
                // under ORDER BY s.id keeps the alias-target pick
                // deterministic when an alias carries several alias
                // edges. Only an Alias binds a target.
                match infos.last_mut() {
                    Some((last_id, info)) if *last_id == node_id => {
                        if info.alias_target.is_none() {
                            info.alias_target = target;
                        }
                    }
                    _ => infos.push((
                        node_id,
                        NodeResolutionInfo {
                            kind,
                            alias_target: if kind == NodeType::Alias {
                                target
                            } else {
                                None
                            },
                        },
                    )),
                }
            }
            Ok(infos)
        })
        .await
    }

    /// Decision 74 / Section 7.7 step 1: the merge-tool candidate scan
    /// excludes already-linked pairs. Serialized per group by
    /// `with_conn` (decision 47).
    async fn are_linked(&self, chat_id: &str, a_id: &str, b_id: &str) -> Result<bool> {
        let a_id = a_id.to_string();
        let b_id = b_id.to_string();
        self.with_conn(chat_id, move |conn| {
            let mut statement = conn.prepare(ARE_LINKED).map_err(backend)?;
            let mut result = conn
                .execute(
                    &mut statement,
                    vec![("a_id", Value::String(a_id)), ("b_id", Value::String(b_id))],
                )
                .map_err(backend)?;
            // count() always yields exactly one row.
            match result.next().and_then(|row| row.into_iter().next()) {
                Some(Value::Int64(count)) => Ok(count > 0),
                _ => Err(MemoryError::Backend(
                    "are_linked: count() returned an unexpected shape".to_string(),
                )),
            }
        })
        .await
    }

    /// Decision 74 / Section 7.7 step 3: the survivor-choice inputs of
    /// the candidate nodes. Serialized per group by `with_conn`
    /// (decision 47).
    async fn node_merge_stats(
        &self,
        chat_id: &str,
        node_ids: &[String],
    ) -> Result<Vec<(String, NodeMergeStats)>> {
        // An empty candidate set short-circuits without opening the
        // database of the group (same policy as node_resolution_infos).
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let node_ids = node_ids.to_vec();
        self.with_conn(chat_id, move |conn| {
            let mut statement = conn.prepare(NODE_MERGE_STATS).map_err(backend)?;
            let result = conn
                .execute(
                    &mut statement,
                    vec![(
                        "node_ids",
                        Value::List(
                            LogicalType::String,
                            node_ids.into_iter().map(Value::String).collect(),
                        ),
                    )],
                )
                .map_err(backend)?;
            let mut stats: Vec<(String, NodeMergeStats)> = Vec::new();
            for row in result {
                let mut columns = row.into_iter();
                let decoded = (columns.next(), columns.next(), columns.next());
                // A row with an unexpected shape is skipped, it does
                // not fail the query (same policy as `alias_targets`).
                if let (
                    Some(Value::String(node_id)),
                    Some(Value::Timestamp(created_at)),
                    Some(Value::Int64(degree)),
                ) = decoded
                {
                    stats.push((
                        node_id,
                        NodeMergeStats {
                            edge_degree: u64::try_from(degree).unwrap_or(0),
                            created_at,
                        },
                    ));
                }
            }
            Ok(stats)
        })
        .await
    }

    /// Decision 74 / Section 7.7 step 3: the empty-registry delegate —
    /// no single-value invariant pass runs, the decision-74 behavior
    /// preserved exactly (`single_value_invalidated` = 0).
    async fn merge_nodes(
        &self,
        chat_id: &str,
        loser_id: &str,
        survivor_id: &str,
    ) -> Result<MergeOutcome> {
        self.merge_nodes_with_registry(chat_id, loser_id, survivor_id, &[])
            .await
    }

    /// Decision 75 / Section 7.7: the merge plus the single-value
    /// invariant pass at the END of the SAME transaction. All snapshot
    /// reads happen BEFORE the transaction begins (the snapshot must
    /// precede any mutation), so the transaction is write-only and the
    /// intermediate-read question of a multi-statement transaction
    /// never comes up — except the invariant pass, whose MATCH
    /// deliberately reads the edges CREATEd by the re-point earlier in
    /// the same transaction (read-your-own-writes, verified against the
    /// real driver). Serialized per group by `with_conn` (decision 47).
    async fn merge_nodes_with_registry(
        &self,
        chat_id: &str,
        loser_id: &str,
        survivor_id: &str,
        single_value_predicates: &[String],
    ) -> Result<MergeOutcome> {
        let loser_id = loser_id.to_string();
        let survivor_id = survivor_id.to_string();
        let registry = single_value_predicates.to_vec();
        self.with_conn(chat_id, move |conn| {
            if loser_id == survivor_id {
                return Err(MemoryError::Backend(format!(
                    "merge_nodes: loser and survivor are the same node ({loser_id})"
                )));
            }
            // Snapshot BEFORE any mutation (Section 7.7 step 3).
            let node = read_snapshot_node(conn, &loser_id)?.ok_or_else(|| {
                MemoryError::Backend(format!(
                    "merge_nodes: loser node {loser_id} does not exist \
                     (re-merging a tombstoned loser is a loud error, Section 7.7 step 5)"
                ))
            })?;
            read_snapshot_node(conn, &survivor_id)?.ok_or_else(|| {
                MemoryError::Backend(format!(
                    "merge_nodes: survivor node {survivor_id} does not exist"
                ))
            })?;
            let loser_edges = read_snapshot_edges(conn, &loser_id)?;
            let survivor_edges = read_snapshot_edges(conn, &survivor_id)?;

            // The dedup set: the survivor's existing edges, extended
            // with each re-pointed edge as it is created, so two
            // equivalent loser edges do not both move.
            let mut existing: Vec<EndpointEdge> = survivor_edges
                .iter()
                .map(|edge| EndpointEdge::from_endpoint(&survivor_id, edge))
                .collect();

            let now = OffsetDateTime::now_utc();
            let mut moved = 0u32;
            let mut self_loops_dropped = 0u32;
            let mut deduped = 0u32;
            let mut created_edges: Vec<MergeSnapshotEdgeKey> = Vec::new();

            let single_value_invalidated = transact(conn, |conn| {
                let mut create_statement = conn.prepare(CREATE_EDGE).map_err(backend)?;
                let mut delete_statement = conn.prepare(DELETE_EDGE).map_err(backend)?;
                let mut detach_statement = conn.prepare(DETACH_DELETE_NODE).map_err(backend)?;
                for edge in &loser_edges {
                    let view = EndpointEdge::from_endpoint(&loser_id, edge);
                    // Loser<->survivor edges and loser self-edges would
                    // become self-loops on the survivor: drop them and
                    // count them in the audit row (Section 7.7 step 3).
                    if view.other_id == survivor_id || view.other_id == loser_id {
                        delete_edge(
                            conn,
                            &mut delete_statement,
                            &edge.source_id,
                            &edge.relationship_name,
                            &edge.target_id,
                            edge.valid_at,
                        )?;
                        self_loops_dropped += 1;
                        continue;
                    }
                    // Section 7.7 step 3: skip the re-point when the
                    // survivor already carries an equivalent edge.
                    if existing.iter().any(|e| edges_equivalent(e, &view)) {
                        delete_edge(
                            conn,
                            &mut delete_statement,
                            &edge.source_id,
                            &edge.relationship_name,
                            &edge.target_id,
                            edge.valid_at,
                        )?;
                        deduped += 1;
                        continue;
                    }
                    // lbug cannot re-endpoint an edge: copy the
                    // properties, create the new edge, delete the old
                    // one. `created_at` keeps its original value
                    // (provenance); `updated_at` marks the re-point.
                    let moved_edge = MergeSnapshotEdge {
                        source_id: if view.outgoing {
                            survivor_id.clone()
                        } else {
                            view.other_id.clone()
                        },
                        target_id: if view.outgoing {
                            view.other_id.clone()
                        } else {
                            survivor_id.clone()
                        },
                        relationship_name: edge.relationship_name.clone(),
                        valid_at: edge.valid_at,
                        invalid_at: edge.invalid_at,
                        edge_text: edge.edge_text.clone(),
                        created_at: edge.created_at,
                        updated_at: now,
                        properties: edge.properties.clone(),
                    };
                    create_edge(conn, &mut create_statement, &moved_edge)?;
                    // Rollback deletes exactly these edges.
                    created_edges.push(MergeSnapshotEdgeKey {
                        source_id: moved_edge.source_id.clone(),
                        relationship_name: moved_edge.relationship_name.clone(),
                        target_id: moved_edge.target_id.clone(),
                        valid_at: moved_edge.valid_at,
                    });
                    existing.push(view);
                    delete_edge(
                        conn,
                        &mut delete_statement,
                        &edge.source_id,
                        &edge.relationship_name,
                        &edge.target_id,
                        edge.valid_at,
                    )?;
                    moved += 1;
                }
                // Hard-delete tombstone (Section 7.7 step 3). Every
                // loser edge is already deleted above; DETACH covers
                // anything the snapshot read missed.
                conn.execute(
                    &mut detach_statement,
                    vec![("node_id", Value::String(loser_id.clone()))],
                )
                .map_err(backend)?;
                // Decision 75 (c) / Section 7.7: the single-value
                // invariant pass at the END of the SAME transaction,
                // after every re-point and the tombstone. An empty
                // registry skips it entirely (the decision-74 behavior).
                let single_value_invalidated = if registry.is_empty() {
                    0
                } else {
                    enforce_single_value_invariant(conn, &survivor_id, &registry, now)?
                };
                Ok(single_value_invalidated)
            })?;

            let snapshot = MergeSnapshot {
                version: 1,
                survivor_id: survivor_id.clone(),
                node,
                edges: loser_edges,
                created_edges,
            };
            let snapshot_json = serde_json::to_string(&snapshot).map_err(|error| {
                MemoryError::Backend(format!("merge snapshot serialization failed: {error}"))
            })?;
            Ok(MergeOutcome {
                snapshot_json,
                edges_moved: moved,
                self_loops_dropped,
                edges_deduped: deduped,
                single_value_invalidated,
            })
        })
        .await
    }

    /// Decision 75 / Section 7.5: the manual invalidation of one edge
    /// (the `--invalidate` offline command). Serialized per group by
    /// `with_conn` (decision 47).
    async fn invalidate_edge(
        &self,
        chat_id: &str,
        edge_id: &str,
        now: OffsetDateTime,
    ) -> Result<bool> {
        // Malformed id: loud error at the parse site (EdgeId::decode).
        let key = EdgeId::decode(edge_id)?;
        let edge_id = edge_id.to_string();
        self.with_conn(chat_id, move |conn| {
            let mut find_statement = conn.prepare(FIND_EDGE_BY_KEY).map_err(backend)?;
            let current = find_edge_invalid_at(
                conn,
                &mut find_statement,
                &key.source_id,
                &key.relationship_name,
                &key.target_id,
                key.valid_at,
            )?;
            // Zero rows: a well-formed id that matches no edge row is a
            // loud error, never a silent no-op on the wrong edge.
            let Some(current_invalid_at) = current else {
                return Err(MemoryError::Backend(format!(
                    "invalidate_edge: no edge matches the natural key {edge_id}"
                )));
            };
            if current_invalid_at.is_some() {
                // Already invalid: a no-op with a DEBUG note, not an
                // error — the operator's safety net.
                tracing::debug!(edge_id = %edge_id, "invalidate_edge: the edge is already invalid");
                return Ok(false);
            }
            transact(conn, |conn| {
                let mut set_statement = conn.prepare(SET_EDGE_INVALID_AT).map_err(backend)?;
                set_edge_invalid_at(conn, &mut set_statement, &key, Some(now), now)
            })?;
            Ok(true)
        })
        .await
    }

    /// Decision 75 / Section 7.5: the manual re-validation of one edge
    /// (the `--revalidate` offline command — the typo safety net).
    /// PLAIN re-validation: no sibling invalidation (see the trait doc).
    /// Serialized per group by `with_conn` (decision 47).
    async fn revalidate_edge(
        &self,
        chat_id: &str,
        edge_id: &str,
        now: OffsetDateTime,
    ) -> Result<bool> {
        let key = EdgeId::decode(edge_id)?;
        let edge_id = edge_id.to_string();
        self.with_conn(chat_id, move |conn| {
            let mut find_statement = conn.prepare(FIND_EDGE_BY_KEY).map_err(backend)?;
            let current = find_edge_invalid_at(
                conn,
                &mut find_statement,
                &key.source_id,
                &key.relationship_name,
                &key.target_id,
                key.valid_at,
            )?;
            let Some(current_invalid_at) = current else {
                return Err(MemoryError::Backend(format!(
                    "revalidate_edge: no edge matches the natural key {edge_id}"
                )));
            };
            if current_invalid_at.is_none() {
                tracing::debug!(edge_id = %edge_id, "revalidate_edge: the edge is already valid");
                return Ok(false);
            }
            transact(conn, |conn| {
                let mut set_statement = conn.prepare(SET_EDGE_INVALID_AT).map_err(backend)?;
                // NULL clears invalid_at (the edge is valid again).
                set_edge_invalid_at(conn, &mut set_statement, &key, None, now)
            })?;
            Ok(true)
        })
        .await
    }

    /// Decision 75 / Section 7.5: the fact listing of one node (the
    /// `--facts` offline command). Entry via the exact-alias machinery
    /// of Section 7.4 step 2. Serialized per group by `with_conn`
    /// (decision 47).
    async fn node_facts(&self, chat_id: &str, name_or_alias: &str) -> Result<Option<NodeFacts>> {
        // Normalization per identifiers.rs: the deterministic alias
        // identifier of the surface form (Rule R5 — no fuzzy scans).
        let alias_node_id = crate::identifiers::alias_id(name_or_alias);
        self.with_conn(chat_id, move |conn| {
            // Step 2: exact alias match. An empty result means the alias
            // node itself does not exist: the name is unknown.
            let targets = read_alias_targets(conn, &alias_node_id)?;
            let (node_id, node_name) = match targets.as_slice() {
                // Exactly one target: the target node is the entry.
                [target] => {
                    let content = read_node_name(conn, &target.node_id)?;
                    let Some(name) = content else {
                        return Err(MemoryError::Backend(format!(
                            "node_facts: alias target {} of alias {alias_node_id} does not exist",
                            target.node_id
                        )));
                    };
                    (target.node_id.clone(), name)
                }
                // Zero or several targets: the alias node itself is the
                // entry — an ambiguous alias (mirroring the recall entry
                // resolution) or a target-less alias carrying
                // fallback-attached facts (Section 7.4 step 4).
                _ => {
                    let Some(name) = read_node_name(conn, &alias_node_id)? else {
                        // No alias node: the name is unknown.
                        return Ok(None);
                    };
                    (alias_node_id.clone(), name)
                }
            };
            let mut statement = conn.prepare(NODE_FACTS_EDGES).map_err(backend)?;
            let result = conn
                .execute(
                    &mut statement,
                    vec![("node_id", Value::String(node_id.clone()))],
                )
                .map_err(backend)?;
            let mut edges = Vec::new();
            for row in result {
                let mut columns = row.into_iter();
                let decoded = (
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                    columns.next(),
                );
                // A row of an unexpected shape is skipped, it does not
                // fail the listing (same policy as `neighbors`).
                let (
                    Some(Value::String(source_id)),
                    Some(Value::String(source_name)),
                    Some(Value::String(target_id)),
                    Some(Value::String(target_name)),
                    Some(Value::String(relationship_name)),
                    Some(Value::String(edge_text)),
                    Some(Value::Timestamp(valid_at)),
                    invalid_at,
                ) = decoded
                else {
                    continue;
                };
                let invalid_at = match invalid_at {
                    Some(Value::Timestamp(invalid_at)) => Some(invalid_at),
                    _ => None,
                };
                // The endpoint that is NOT the queried node.
                let (outgoing, other_node_name) = if source_id == node_id {
                    (true, target_name)
                } else {
                    (false, source_name)
                };
                edges.push(NodeFactEdge {
                    edge_id: EdgeId {
                        source_id,
                        relationship_name: relationship_name.clone(),
                        target_id,
                        valid_at,
                    }
                    .encode(),
                    relationship_name,
                    other_node_name,
                    outgoing,
                    edge_text,
                    valid_at,
                    invalid_at,
                });
            }
            Ok(Some(NodeFacts {
                node_id,
                node_name,
                edges,
            }))
        })
        .await
    }

    /// Decision 74 / Section 7.7 step 2: the `related` verdict links
    /// the two nodes with `also_known_as` (a -> b, mirroring the
    /// resolve path of Section 7.4 step 5). Serialized per group by
    /// `with_conn` (decision 47).
    async fn link_also_known_as(&self, chat_id: &str, a_id: &str, b_id: &str) -> Result<()> {
        let a_id = a_id.to_string();
        let b_id = b_id.to_string();
        self.with_conn(chat_id, move |conn| {
            if a_id == b_id {
                return Err(MemoryError::Backend(format!(
                    "link_also_known_as: a self-link on {a_id} is meaningless"
                )));
            }
            // The MERGE matches nothing when an endpoint is missing, so
            // check existence explicitly: a silent no-op would hide a
            // tool bug.
            let a = read_snapshot_node(conn, &a_id)?.ok_or_else(|| {
                MemoryError::Backend(format!("link_also_known_as: node {a_id} does not exist"))
            })?;
            let b = read_snapshot_node(conn, &b_id)?.ok_or_else(|| {
                MemoryError::Backend(format!("link_also_known_as: node {b_id} does not exist"))
            })?;
            let edge_text = format!("{} is also known as {}.", a.name, b.name);
            let now = OffsetDateTime::now_utc();
            transact(conn, |conn| {
                let mut statement = conn.prepare(LINK_ALSO_KNOWN_AS).map_err(backend)?;
                conn.execute(
                    &mut statement,
                    vec![
                        ("a_id", Value::String(a_id.clone())),
                        ("b_id", Value::String(b_id.clone())),
                        ("now", Value::Timestamp(now)),
                        ("edge_text", Value::String(edge_text.clone())),
                    ],
                )
                .map_err(backend)?;
                Ok(())
            })
        })
        .await
    }

    /// Decision 74 / Section 7.7 step 4. Serialized per group by
    /// `with_conn` (decision 47).
    async fn rollback_merge(&self, chat_id: &str, snapshot_json: &str) -> Result<()> {
        let snapshot_json = snapshot_json.to_string();
        self.with_conn(chat_id, move |conn| {
            let snapshot: MergeSnapshot =
                serde_json::from_str(&snapshot_json).map_err(|error| {
                    MemoryError::Backend(format!("rollback_merge: malformed snapshot: {error}"))
                })?;
            // Section 7.7 step 4: refuse when the survivor was itself
            // tombstoned by a later merge; chained-merge rollback is
            // out of scope.
            read_snapshot_node(conn, &snapshot.survivor_id)?.ok_or_else(|| {
                MemoryError::Backend(format!(
                    "rollback_merge: survivor {} no longer exists; \
                     chained-merge rollback is out of scope",
                    snapshot.survivor_id
                ))
            })?;
            // A loser that still exists means the merge never happened
            // or was already rolled back: refuse loudly instead of
            // double-creating the node and its edges.
            if read_snapshot_node(conn, &snapshot.node.id)?.is_some() {
                return Err(MemoryError::Backend(format!(
                    "rollback_merge: loser node {} still exists; \
                     the merge did not happen or was already rolled back",
                    snapshot.node.id
                )));
            }
            transact(conn, |conn| {
                let mut node_statement = conn.prepare(CREATE_NODE).map_err(backend)?;
                conn.execute(
                    &mut node_statement,
                    vec![
                        ("id", Value::String(snapshot.node.id.clone())),
                        ("name", Value::String(snapshot.node.name.clone())),
                        ("type", Value::String(snapshot.node.kind.clone())),
                        ("created_at", Value::Timestamp(snapshot.node.created_at)),
                        ("updated_at", Value::Timestamp(snapshot.node.updated_at)),
                        ("properties", opt_string(&snapshot.node.properties)),
                    ],
                )
                .map_err(backend)?;
                // Restore the original edges verbatim.
                let mut create_statement = conn.prepare(CREATE_EDGE).map_err(backend)?;
                for edge in &snapshot.edges {
                    create_edge(conn, &mut create_statement, edge)?;
                }
                // Delete exactly the edges the merge created.
                let mut delete_statement = conn.prepare(DELETE_EDGE).map_err(backend)?;
                for key in &snapshot.created_edges {
                    delete_edge(
                        conn,
                        &mut delete_statement,
                        &key.source_id,
                        &key.relationship_name,
                        &key.target_id,
                        key.valid_at,
                    )?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn close(&self, chat_id: &str) -> Result<()> {
        validate_chat_id(chat_id)?;
        let db = self.databases.lock().await.remove(chat_id);
        if let Some(db) = db {
            // Database teardown can touch the file. Keep it off the async
            // runtime (Section 5.2 rule 3).
            tokio::task::spawn_blocking(move || drop(db))
                .await
                .map_err(|error| MemoryError::Backend(format!("task join error: {error}")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
impl LbugBackend {
    /// Runs a `RETURN count(...)` query. Test helper, delegates to
    /// `query_rows`.
    async fn count(&self, chat_id: &str, cypher: &str) -> Result<i64> {
        let rows = self.query_rows(chat_id, cypher).await?;
        match rows.first().and_then(|row| row.first()) {
            Some(count) => count
                .parse::<i64>()
                .map_err(|_| MemoryError::Backend("unexpected count result".to_string())),
            None => Err(MemoryError::Backend("unexpected count result".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemoryEdge, MemoryNode, NodeType};
    use time::macros::datetime;
    use time::Duration;

    fn sample_batch() -> MemoryBatch {
        let person = MemoryNode {
            id: crate::identifiers::person_id("1001"),
            name: "Tama".to_string(),
            node_type: NodeType::Person,
            created_at: datetime!(2026-08-07 10:00 UTC),
            updated_at: datetime!(2026-08-07 10:00 UTC),
            properties: Some(r#"{"tg_user_id":"1001"}"#.to_string()),
        };
        let concept = MemoryNode {
            id: crate::identifiers::concept_id("GRPO"),
            name: "GRPO".to_string(),
            node_type: NodeType::Concept,
            created_at: datetime!(2026-08-07 10:00 UTC),
            updated_at: datetime!(2026-08-07 10:00 UTC),
            properties: None,
        };
        let edge = MemoryEdge {
            source_id: person.id.clone(),
            target_id: concept.id.clone(),
            relationship_name: "likes".to_string(),
            valid_at: datetime!(2026-08-07 10:00 UTC),
            invalid_at: None,
            edge_text: "Tama likes GRPO.".to_string(),
            created_at: datetime!(2026-08-07 10:00 UTC),
            updated_at: datetime!(2026-08-07 10:00 UTC),
            properties: None,
        };
        MemoryBatch {
            batch_id: crate::identifiers::batch_id(1, 50),
            nodes: vec![person, concept],
            edges: vec![edge],
        }
    }

    #[tokio::test]
    async fn ensure_schema_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        backend.ensure_schema("chat_a").await.unwrap();
        backend.ensure_schema("chat_a").await.unwrap();
        backend.close("chat_a").await.unwrap();
        // A close plus a reopen keeps the schema idempotent.
        backend.ensure_schema("chat_a").await.unwrap();
    }

    #[tokio::test]
    async fn upsert_batch_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let batch = sample_batch();
        backend.upsert_batch("chat_b", &batch).await.unwrap();
        // Retry with the same content. The anti-defer list requires
        // idempotent batch writes under stable batch identifiers.
        backend.upsert_batch("chat_b", &batch).await.unwrap();

        let batches = backend
            .count(
                "chat_b",
                "MATCH (n:Node) WHERE n.type = 'MessageBatch' RETURN count(n)",
            )
            .await
            .unwrap();
        let entities = backend
            .count(
                "chat_b",
                "MATCH (n:Node) WHERE n.type <> 'MessageBatch' RETURN count(n)",
            )
            .await
            .unwrap();
        let edges = backend
            .count("chat_b", "MATCH ()-[r:EDGE]->() RETURN count(r)")
            .await
            .unwrap();
        assert_eq!(batches, 1);
        assert_eq!(entities, 2);
        assert_eq!(edges, 1);
    }

    #[tokio::test]
    async fn checkpoint_and_close_do_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        backend
            .upsert_batch("chat_c", &sample_batch())
            .await
            .unwrap();
        backend.checkpoint("chat_c").await.unwrap();
        backend.close("chat_c").await.unwrap();
        // A close reopens transparently on the next call.
        backend
            .upsert_batch("chat_c", &sample_batch())
            .await
            .unwrap();
        backend.close("chat_c").await.unwrap();
        // A close of a group without a cached handle does not error.
        backend.close("chat_c").await.unwrap();
    }

    #[tokio::test]
    async fn groups_are_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        backend
            .upsert_batch("chat_d1", &sample_batch())
            .await
            .unwrap();
        // Rule P5: one group's data never crosses into another group.
        let nodes = backend
            .count("chat_d2", "MATCH (n:Node) RETURN count(n)")
            .await
            .unwrap();
        assert_eq!(nodes, 0);
        assert!(dir.path().join("chat_d1/memory.lbug").exists());
        assert!(dir.path().join("chat_d2/memory.lbug").exists());
    }

    #[tokio::test]
    async fn invalid_chat_ids_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        for bad in ["", "a/b", "a\\b", "a\0b", "..", "a/../b"] {
            match backend.ensure_schema(bad).await {
                Err(MemoryError::InvalidChatId(_)) => {}
                other => panic!("expected InvalidChatId for {bad:?}, got {other:?}"),
            }
        }
    }

    /// A batch with a Person node, an Alias node, and a `known_as` edge
    /// Person -> Alias. Section 7.4 step 2 test data.
    fn alias_batch() -> (MemoryBatch, MemoryNode, MemoryNode) {
        let person = MemoryNode {
            id: crate::identifiers::person_id("1001"),
            name: "Tama".to_string(),
            node_type: NodeType::Person,
            created_at: datetime!(2026-08-07 10:00 UTC),
            updated_at: datetime!(2026-08-07 10:00 UTC),
            properties: None,
        };
        let alias = MemoryNode {
            id: crate::identifiers::alias_id("tama"),
            name: "tama".to_string(),
            node_type: NodeType::Alias,
            created_at: datetime!(2026-08-07 10:00 UTC),
            updated_at: datetime!(2026-08-07 10:00 UTC),
            properties: None,
        };
        let edge = MemoryEdge {
            source_id: person.id.clone(),
            target_id: alias.id.clone(),
            relationship_name: "known_as".to_string(),
            valid_at: datetime!(2026-08-07 10:00 UTC),
            invalid_at: None,
            edge_text: "Tama is known as tama.".to_string(),
            created_at: datetime!(2026-08-07 10:00 UTC),
            updated_at: datetime!(2026-08-07 10:00 UTC),
            properties: None,
        };
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(1, 10),
            nodes: vec![person.clone(), alias.clone()],
            edges: vec![edge],
        };
        (batch, person, alias)
    }

    #[tokio::test]
    async fn alias_targets_returns_the_source_nodes_of_the_alias_edges() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, person, alias) = alias_batch();
        backend.upsert_batch("chat_e", &batch).await.unwrap();

        let targets = backend.alias_targets("chat_e", &alias.id).await.unwrap();
        assert_eq!(
            targets,
            vec![crate::backend::AliasTarget {
                node_id: person.id.clone(),
                node_type: NodeType::Person,
            }]
        );
    }

    #[tokio::test]
    async fn alias_targets_of_an_unknown_alias_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, _person, _alias) = alias_batch();
        backend.upsert_batch("chat_f", &batch).await.unwrap();

        // Section 7.4 step 2: an empty result means the alias is unknown.
        let targets = backend
            .alias_targets("chat_f", &crate::identifiers::alias_id("nobody"))
            .await
            .unwrap();
        assert!(targets.is_empty());
    }

    fn concept_node(name: &str, created_at: OffsetDateTime) -> MemoryNode {
        MemoryNode {
            id: crate::identifiers::concept_id(name),
            name: name.to_string(),
            node_type: NodeType::Concept,
            created_at,
            updated_at: created_at,
            properties: None,
        }
    }

    fn fact_edge(
        source_id: &str,
        target_id: &str,
        relationship_name: &str,
        invalid_at: Option<OffsetDateTime>,
        created_at: OffsetDateTime,
    ) -> MemoryEdge {
        MemoryEdge {
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            relationship_name: relationship_name.to_string(),
            valid_at: created_at,
            invalid_at,
            edge_text: format!("{source_id} {relationship_name} {target_id}"),
            created_at,
            updated_at: created_at,
            properties: None,
        }
    }

    /// Section 8.2 test graph: B -likes-> A -dislikes-> C, with one
    /// invalid edge D -likes-> A and one `contains` edge A -> C.
    fn neighbor_batch() -> (MemoryBatch, MemoryNode, MemoryNode, MemoryNode) {
        let base = datetime!(2026-08-07 10:00 UTC);
        let a = concept_node("Alpha", base);
        let b = concept_node("Beta", base);
        let c = concept_node("Gamma", base);
        let d = concept_node("Delta", base);
        let edges = vec![
            fact_edge(&b.id, &a.id, "likes", None, base + Duration::seconds(1)),
            fact_edge(&a.id, &c.id, "dislikes", None, base + Duration::seconds(2)),
            // Invalid edge: excluded by the `invalid_at IS NULL` filter.
            fact_edge(
                &d.id,
                &a.id,
                "likes",
                Some(base + Duration::seconds(3)),
                base + Duration::seconds(3),
            ),
            // Provenance edge: excluded (Section 6.3/8.2).
            fact_edge(&a.id, &c.id, "contains", None, base + Duration::seconds(4)),
        ];
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(2, 60),
            nodes: vec![a.clone(), b.clone(), c.clone(), d],
            edges,
        };
        (batch, a, b, c)
    }

    #[tokio::test]
    async fn neighbors_returns_valid_edges_in_both_directions() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, a, b, c) = neighbor_batch();
        backend.upsert_batch("chat_g", &batch).await.unwrap();

        let edges = backend.neighbors("chat_g", &a.id).await.unwrap();
        // Two valid non-contains edges: one incoming, one outgoing.
        assert_eq!(edges.len(), 2);
        // Truncation order of Section 8.2: created_at descending.
        assert_eq!(edges[0].relationship_name, "dislikes");
        assert_eq!(edges[0].other_node_id, c.id);
        assert_eq!(edges[0].other_node_name, "Gamma");
        assert_eq!(edges[0].source_id, a.id);
        assert_eq!(edges[0].target_id, c.id);
        assert_eq!(edges[1].relationship_name, "likes");
        assert_eq!(edges[1].other_node_id, b.id);
        assert_eq!(edges[1].other_node_name, "Beta");
        assert_eq!(edges[1].source_id, b.id);
        assert_eq!(edges[1].target_id, a.id);
    }

    #[tokio::test]
    async fn neighbors_excludes_invalid_and_contains_edges() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, _a, _b, c) = neighbor_batch();
        backend.upsert_batch("chat_h", &batch).await.unwrap();

        // Gamma has one valid edge (dislikes). The contains edge Alpha ->
        // Gamma and the invalid edge into Alpha must not occur here.
        let edges = backend.neighbors("chat_h", &c.id).await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_name, "dislikes");
    }

    #[tokio::test]
    async fn neighbors_of_an_unknown_node_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, _a, _b, _c) = neighbor_batch();
        backend.upsert_batch("chat_i", &batch).await.unwrap();

        // Section 8.2: an unknown node id yields an empty vec.
        let edges = backend
            .neighbors("chat_i", &crate::identifiers::concept_id("nobody"))
            .await
            .unwrap();
        assert!(edges.is_empty());
    }

    #[tokio::test]
    async fn neighbors_truncates_to_the_expansion_limit_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let hub = concept_node("Hub", base);
        // 505 valid edges on one node, distinct targets, increasing
        // created_at. The natural key (source, target, rel, valid_at)
        // stays distinct because the targets differ.
        let over = NEIGHBOR_EXPANSION_LIMIT + 5;
        let mut nodes = vec![hub.clone()];
        let mut edges = Vec::with_capacity(over);
        for i in 0..over {
            let target = concept_node(&format!("Target{i:04}"), base);
            edges.push(fact_edge(
                &hub.id,
                &target.id,
                "mentions",
                None,
                base + Duration::seconds(i as i64),
            ));
            nodes.push(target);
        }
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(3, 70),
            nodes,
            edges,
        };
        backend.upsert_batch("chat_j", &batch).await.unwrap();

        let edges = backend.neighbors("chat_j", &hub.id).await.unwrap();
        assert_eq!(edges.len(), NEIGHBOR_EXPANSION_LIMIT);
        // Section 8.2: the 500 NEWEST edges, created_at descending.
        for (rank, edge) in edges.iter().enumerate() {
            let index = over - 1 - rank;
            assert_eq!(
                edge.other_node_id,
                crate::identifiers::concept_id(&format!("Target{index:04}")),
                "rank {rank} must be Target{index:04}"
            );
        }
    }

    /// Decision 66 test data: one Concept node whose properties carry a
    /// description, one Person node whose properties carry tg_user_id
    /// plus a description, and one node without properties.
    fn content_batch() -> (MemoryBatch, [MemoryNode; 3]) {
        let base = datetime!(2026-08-07 10:00 UTC);
        let concept = MemoryNode {
            id: crate::identifiers::concept_id("GRPO"),
            name: "GRPO".to_string(),
            node_type: NodeType::Concept,
            created_at: base,
            updated_at: base,
            properties: Some(
                serde_json::json!({ "description": "A training method." }).to_string(),
            ),
        };
        let person = MemoryNode {
            id: crate::identifiers::person_id("1001"),
            name: "Tama".to_string(),
            node_type: NodeType::Person,
            created_at: base,
            updated_at: base,
            properties: Some(
                serde_json::json!({
                    "tg_user_id": "1001",
                    "display_name": "Tama",
                    "description": "A group member.",
                })
                .to_string(),
            ),
        };
        let bare = MemoryNode {
            id: crate::identifiers::concept_id("Bare"),
            name: "Bare".to_string(),
            node_type: NodeType::Concept,
            created_at: base,
            updated_at: base,
            properties: None,
        };
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(4, 80),
            nodes: vec![concept.clone(), person.clone(), bare.clone()],
            edges: vec![],
        };
        (batch, [concept, person, bare])
    }

    #[tokio::test]
    async fn node_content_returns_the_stored_name_and_description() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [concept, person, _bare]) = content_batch();
        backend.upsert_batch("chat_k", &batch).await.unwrap();

        // Decision 66: the values come from the STORED row, not from the
        // pipeline's in-memory knowledge.
        let content = backend
            .node_content("chat_k", &concept.id)
            .await
            .unwrap()
            .expect("the concept node exists");
        assert_eq!(
            content,
            NodeContent {
                name: "GRPO".to_string(),
                description: "A training method.".to_string(),
            }
        );
        // The description sits next to other fields in the same blob.
        let content = backend
            .node_content("chat_k", &person.id)
            .await
            .unwrap()
            .expect("the person node exists");
        assert_eq!(
            content,
            NodeContent {
                name: "Tama".to_string(),
                description: "A group member.".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn node_content_of_a_missing_node_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [_concept, _person, _bare]) = content_batch();
        backend.upsert_batch("chat_l", &batch).await.unwrap();

        // None covers the existence checks of the tombstone cleanup.
        let content = backend
            .node_content("chat_l", &crate::identifiers::concept_id("nobody"))
            .await
            .unwrap();
        assert_eq!(content, None);
    }

    #[tokio::test]
    async fn node_content_without_a_description_yields_an_empty_description() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [_concept, _person, bare]) = content_batch();
        backend.upsert_batch("chat_m", &batch).await.unwrap();

        // A NULL properties blob is not an error.
        let content = backend
            .node_content("chat_m", &bare.id)
            .await
            .unwrap()
            .expect("the bare node exists");
        assert_eq!(
            content,
            NodeContent {
                name: "Bare".to_string(),
                description: String::new(),
            }
        );
    }

    #[tokio::test]
    async fn list_node_contents_returns_all_upserted_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [concept, person, bare]) = content_batch();
        backend.upsert_batch("chat_n", &batch).await.unwrap();

        let mut contents = backend.list_node_contents("chat_n").await.unwrap();
        contents.sort_by(|(left, _), (right, _)| left.cmp(right));
        // The MessageBatch skeleton node rides along: the listing covers
        // every node of the group, the embedding worker filters.
        let mut expected: Vec<(String, NodeContent)> = vec![
            (
                batch.batch_id.clone(),
                NodeContent {
                    name: batch.batch_id.clone(),
                    description: String::new(),
                },
            ),
            (
                concept.id,
                NodeContent {
                    name: "GRPO".to_string(),
                    description: "A training method.".to_string(),
                },
            ),
            (
                bare.id,
                NodeContent {
                    name: "Bare".to_string(),
                    description: String::new(),
                },
            ),
            (
                person.id,
                NodeContent {
                    name: "Tama".to_string(),
                    description: "A group member.".to_string(),
                },
            ),
        ];
        expected.sort_by(|(left, _), (right, _)| left.cmp(right));
        assert_eq!(contents, expected);
    }

    /// Decision 73 test graph: a Person and a Concept, each with a
    /// surface Alias (known_as / also_known_as), plus the MessageBatch
    /// skeleton that `upsert_batch` always writes.
    fn resolution_batch() -> (MemoryBatch, MemoryNode, MemoryNode, MemoryNode, MemoryNode) {
        let base = datetime!(2026-08-07 10:00 UTC);
        let person = MemoryNode {
            id: crate::identifiers::person_id("1001"),
            name: "Tama".to_string(),
            node_type: NodeType::Person,
            created_at: base,
            updated_at: base,
            properties: None,
        };
        let concept = concept_node("GRPO", base);
        let person_alias = MemoryNode {
            id: crate::identifiers::alias_id("tama"),
            name: "tama".to_string(),
            node_type: NodeType::Alias,
            created_at: base,
            updated_at: base,
            properties: None,
        };
        let concept_alias = MemoryNode {
            id: crate::identifiers::alias_id("grpo"),
            name: "grpo".to_string(),
            node_type: NodeType::Alias,
            created_at: base,
            updated_at: base,
            properties: None,
        };
        let edges = vec![
            fact_edge(&person.id, &person_alias.id, "known_as", None, base),
            fact_edge(&concept.id, &concept_alias.id, "also_known_as", None, base),
        ];
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(5, 90),
            nodes: vec![
                person.clone(),
                concept.clone(),
                person_alias.clone(),
                concept_alias.clone(),
            ],
            edges,
        };
        (batch, person, concept, person_alias, concept_alias)
    }

    #[tokio::test]
    async fn node_resolution_infos_reports_kinds_and_alias_targets() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, person, concept, person_alias, concept_alias) = resolution_batch();
        backend.upsert_batch("chat_o", &batch).await.unwrap();

        let ids = vec![
            batch.batch_id.clone(),
            person.id.clone(),
            concept.id.clone(),
            person_alias.id.clone(),
            concept_alias.id.clone(),
        ];
        let infos: HashMap<String, NodeResolutionInfo> = backend
            .node_resolution_infos("chat_o", &ids)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(infos.len(), 5);
        // The MessageBatch skeleton of Section 7.2 step 5 is detectable
        // by kind, so the resolver can exclude it.
        assert_eq!(
            infos[&batch.batch_id],
            NodeResolutionInfo {
                kind: NodeType::MessageBatch,
                alias_target: None,
            }
        );
        assert_eq!(
            infos[&person.id],
            NodeResolutionInfo {
                kind: NodeType::Person,
                alias_target: None,
            }
        );
        assert_eq!(
            infos[&concept.id],
            NodeResolutionInfo {
                kind: NodeType::Concept,
                alias_target: None,
            }
        );
        // An alias binds to the source of its known_as edge.
        assert_eq!(
            infos[&person_alias.id],
            NodeResolutionInfo {
                kind: NodeType::Alias,
                alias_target: Some(person.id.clone()),
            }
        );
        // ... and of its also_known_as edge.
        assert_eq!(
            infos[&concept_alias.id],
            NodeResolutionInfo {
                kind: NodeType::Alias,
                alias_target: Some(concept.id.clone()),
            }
        );
    }

    #[tokio::test]
    async fn node_resolution_infos_skips_missing_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, person, _concept, _person_alias, _concept_alias) = resolution_batch();
        backend.upsert_batch("chat_p", &batch).await.unwrap();

        let ids = vec![person.id.clone(), crate::identifiers::person_id("nobody")];
        let infos = backend.node_resolution_infos("chat_p", &ids).await.unwrap();
        // A missing id does not occur in the result (the batched shape
        // of the None semantics).
        assert_eq!(
            infos,
            vec![(
                person.id.clone(),
                NodeResolutionInfo {
                    kind: NodeType::Person,
                    alias_target: None,
                },
            )]
        );
    }

    #[tokio::test]
    async fn node_resolution_infos_of_an_empty_id_list_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        // The empty pre-screen short-circuits: no database is opened.
        let infos = backend.node_resolution_infos("chat_q", &[]).await.unwrap();
        assert!(infos.is_empty());
        assert!(!dir.path().join("chat_q").exists());
    }

    #[test]
    fn neighbor_edge_id_is_stable_and_distinct() {
        let base = datetime!(2026-08-07 10:00 UTC);
        let edge = NeighborEdge {
            source_id: "s".to_string(),
            target_id: "t".to_string(),
            relationship_name: "likes".to_string(),
            edge_text: "text".to_string(),
            valid_at: base,
            created_at: base,
            other_node_id: "t".to_string(),
            other_node_name: "T".to_string(),
        };
        // Stable across calls; RFC 3339 rendering of valid_at.
        let first = edge.edge_id();
        assert_eq!(first, edge.edge_id());
        assert_eq!(
            first,
            format!("s|likes|t|{}", base.format(&Rfc3339).unwrap())
        );
        // Distinct for each part of the natural key (Section 6.1).
        let mut other = edge.clone();
        other.target_id = "u".to_string();
        assert_ne!(first, other.edge_id());
        let mut other = edge.clone();
        other.relationship_name = "dislikes".to_string();
        assert_ne!(first, other.edge_id());
        let mut other = edge.clone();
        other.valid_at = base + Duration::seconds(1);
        assert_ne!(first, other.edge_id());
        // Display fields are not part of the key.
        let mut other = edge.clone();
        other.edge_text = "other text".to_string();
        assert_eq!(first, other.edge_id());
    }

    /// Decision 74 test graph: a loser Concept, a survivor Concept, and
    /// bystander concepts X, Y, Z. Loser edges:
    /// - loser -likes-> X, edge_text "likes-text", properties blob:
    ///   DEDUPED against the survivor's equivalent edge.
    /// - Y -dislikes-> loser, invalid_at set, properties blob: MOVED.
    /// - loser -mentions-> Z: MOVED.
    /// - loser -knows-> survivor: becomes a self-loop, DROPPED.
    ///
    /// Plus one survivor edge: survivor -likes-> X with the same
    /// relationship name, other endpoint and edge_text as the loser's
    /// edge (Section 7.7 step 3 dedup).
    fn merge_batch() -> (MemoryBatch, [MemoryNode; 5], [MemoryEdge; 5]) {
        let base = datetime!(2026-08-07 10:00 UTC);
        let loser = concept_node("Loser", base);
        let survivor = concept_node("Survivor", base);
        let x = concept_node("Xray", base);
        let y = concept_node("Yankee", base);
        let z = concept_node("Zulu", base);
        let mut likes_from_loser =
            fact_edge(&loser.id, &x.id, "likes", None, base + Duration::seconds(1));
        likes_from_loser.edge_text = "likes-text".to_string();
        likes_from_loser.properties = Some(serde_json::json!({ "note": "from loser" }).to_string());
        let mut dislikes_to_loser = fact_edge(
            &y.id,
            &loser.id,
            "dislikes",
            None,
            base + Duration::seconds(2),
        );
        dislikes_to_loser.edge_text = "dislikes-text".to_string();
        dislikes_to_loser.invalid_at = Some(base + Duration::seconds(50));
        dislikes_to_loser.properties =
            Some(serde_json::json!({ "note": "into loser" }).to_string());
        let mut mentions = fact_edge(
            &loser.id,
            &z.id,
            "mentions",
            None,
            base + Duration::seconds(3),
        );
        mentions.edge_text = "mentions-text".to_string();
        let knows = fact_edge(
            &loser.id,
            &survivor.id,
            "knows",
            None,
            base + Duration::seconds(4),
        );
        let mut likes_from_survivor = fact_edge(
            &survivor.id,
            &x.id,
            "likes",
            None,
            base + Duration::seconds(5),
        );
        likes_from_survivor.edge_text = "likes-text".to_string();
        let edges = [
            likes_from_loser,
            dislikes_to_loser,
            mentions,
            knows,
            likes_from_survivor,
        ];
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(6, 100),
            nodes: vec![
                loser.clone(),
                survivor.clone(),
                x.clone(),
                y.clone(),
                z.clone(),
            ],
            edges: edges.to_vec(),
        };
        (batch, [loser, survivor, x, y, z], edges)
    }

    /// Test helper: every node row and every edge row, sorted, for
    /// pre/post shape comparison.
    async fn graph_shape(
        backend: &LbugBackend,
        chat_id: &str,
    ) -> (Vec<Vec<String>>, Vec<Vec<String>>) {
        let nodes = backend
            .query_rows(
                chat_id,
                "MATCH (n:Node) RETURN n.id, n.name, n.type, n.created_at, n.updated_at, n.properties ORDER BY n.id",
            )
            .await
            .unwrap();
        let edges = backend
            .query_rows(
                chat_id,
                "MATCH (s:Node)-[r:EDGE]->(t:Node) \
                 RETURN s.id, t.id, r.relationship_name, r.valid_at, r.invalid_at, r.edge_text, r.created_at, r.updated_at, r.properties \
                 ORDER BY s.id, t.id, r.relationship_name, r.valid_at",
            )
            .await
            .unwrap();
        (nodes, edges)
    }

    #[tokio::test]
    async fn merge_nodes_moves_edges_in_both_directions_with_properties_intact() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, _x, y, z], _edges) = merge_batch();
        backend.upsert_batch("chat_r", &batch).await.unwrap();

        let outcome = backend
            .merge_nodes("chat_r", &loser.id, &survivor.id)
            .await
            .unwrap();
        assert_eq!(outcome.edges_moved, 2);
        assert_eq!(outcome.self_loops_dropped, 1);
        assert_eq!(outcome.edges_deduped, 1);

        // The incoming edge Y -dislikes-> loser is re-pointed to
        // Y -dislikes-> survivor, the outgoing loser -mentions-> Z to
        // survivor -mentions-> Z. Properties, edge_text, valid_at,
        // invalid_at and the original created_at ride along.
        let moved = backend
            .query_rows(
                "chat_r",
                "MATCH (s:Node)-[r:EDGE]->(t:Node) \
                 WHERE r.relationship_name IN ['dislikes', 'mentions'] \
                 RETURN s.id, t.id, r.relationship_name, r.edge_text, r.invalid_at, r.properties, r.created_at \
                 ORDER BY r.relationship_name",
            )
            .await
            .unwrap();
        let dislikes_created = (datetime!(2026-08-07 10:00 UTC) + Duration::seconds(2))
            .format(&Rfc3339)
            .unwrap();
        let dislikes_invalid = (datetime!(2026-08-07 10:00 UTC) + Duration::seconds(50))
            .format(&Rfc3339)
            .unwrap();
        let mentions_created = (datetime!(2026-08-07 10:00 UTC) + Duration::seconds(3))
            .format(&Rfc3339)
            .unwrap();
        assert_eq!(
            moved,
            vec![
                vec![
                    y.id.clone(),
                    survivor.id.clone(),
                    "dislikes".to_string(),
                    "dislikes-text".to_string(),
                    dislikes_invalid,
                    r#"{"note":"into loser"}"#.to_string(),
                    dislikes_created,
                ],
                vec![
                    survivor.id.clone(),
                    z.id.clone(),
                    "mentions".to_string(),
                    "mentions-text".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    mentions_created,
                ],
            ]
        );
    }

    #[tokio::test]
    async fn merge_nodes_drops_loser_survivor_edges_as_self_loops() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, _x, _y, _z], _edges) = merge_batch();
        backend.upsert_batch("chat_s", &batch).await.unwrap();

        let outcome = backend
            .merge_nodes("chat_s", &loser.id, &survivor.id)
            .await
            .unwrap();
        assert_eq!(outcome.self_loops_dropped, 1);

        // No survivor self-loop and no leftover `knows` edge.
        let self_loops = backend
            .count(
                "chat_s",
                "MATCH (s:Node)-[r:EDGE]->(t:Node) WHERE s.id = t.id RETURN count(r)",
            )
            .await
            .unwrap();
        assert_eq!(self_loops, 0);
        let knows = backend
            .count(
                "chat_s",
                "MATCH ()-[r:EDGE]->() WHERE r.relationship_name = 'knows' RETURN count(r)",
            )
            .await
            .unwrap();
        assert_eq!(knows, 0);
    }

    #[tokio::test]
    async fn merge_nodes_dedups_against_equivalent_survivor_edges() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, x, _y, _z], _edges) = merge_batch();
        backend.upsert_batch("chat_t", &batch).await.unwrap();

        let outcome = backend
            .merge_nodes("chat_t", &loser.id, &survivor.id)
            .await
            .unwrap();
        assert_eq!(outcome.edges_deduped, 1);

        // Exactly one survivor -likes-> X edge remains, and it is the
        // survivor's ORIGINAL one (created_at base + 5s).
        let likes = backend
            .query_rows(
                "chat_t",
                "MATCH (s:Node)-[r:EDGE]->(t:Node) \
                 WHERE r.relationship_name = 'likes' RETURN s.id, t.id, r.created_at",
            )
            .await
            .unwrap();
        let original_created = (datetime!(2026-08-07 10:00 UTC) + Duration::seconds(5))
            .format(&Rfc3339)
            .unwrap();
        assert_eq!(
            likes,
            vec![vec![survivor.id.clone(), x.id.clone(), original_created]]
        );
    }

    #[tokio::test]
    async fn merge_nodes_snapshot_covers_the_node_every_edge_and_the_created_keys() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, x, y, z], edges) = merge_batch();
        backend.upsert_batch("chat_u", &batch).await.unwrap();

        let outcome = backend
            .merge_nodes("chat_u", &loser.id, &survivor.id)
            .await
            .unwrap();
        let snapshot: MergeSnapshot = serde_json::from_str(&outcome.snapshot_json).unwrap();

        // The loser node, exactly as stored.
        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.survivor_id, survivor.id);
        assert_eq!(snapshot.node.id, loser.id);
        assert_eq!(snapshot.node.kind, "Concept");
        assert_eq!(snapshot.node.name, "Loser");
        assert_eq!(snapshot.node.created_at, loser.created_at);

        // Every edge of the loser, both directions, full properties.
        assert_eq!(snapshot.edges.len(), 4);
        let likes = &snapshot.edges[0];
        assert_eq!(likes.source_id, loser.id);
        assert_eq!(likes.target_id, x.id);
        assert_eq!(likes.relationship_name, "likes");
        assert_eq!(likes.edge_text, "likes-text");
        assert_eq!(
            likes.properties,
            Some(r#"{"note":"from loser"}"#.to_string())
        );
        let dislikes = &snapshot.edges[1];
        assert_eq!(dislikes.source_id, y.id);
        assert_eq!(dislikes.target_id, loser.id);
        assert_eq!(
            dislikes.invalid_at,
            Some(datetime!(2026-08-07 10:00 UTC) + Duration::seconds(50))
        );

        // The created-edge keys: exactly the two moved edges, in the
        // processing order (created_at ascending).
        assert_eq!(
            snapshot.created_edges,
            vec![
                MergeSnapshotEdgeKey {
                    source_id: y.id.clone(),
                    relationship_name: "dislikes".to_string(),
                    target_id: survivor.id.clone(),
                    valid_at: edges[1].valid_at,
                },
                MergeSnapshotEdgeKey {
                    source_id: survivor.id.clone(),
                    relationship_name: "mentions".to_string(),
                    target_id: z.id.clone(),
                    valid_at: edges[2].valid_at,
                },
            ]
        );
    }

    #[tokio::test]
    async fn merge_nodes_tombstones_the_loser_for_reads() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, _x, _y, _z], _edges) = merge_batch();
        backend.upsert_batch("chat_v", &batch).await.unwrap();
        backend
            .merge_nodes("chat_v", &loser.id, &survivor.id)
            .await
            .unwrap();

        // The tombstoned loser is invisible to every read path.
        let content = backend.node_content("chat_v", &loser.id).await.unwrap();
        assert_eq!(content, None);
        let infos = backend
            .node_resolution_infos("chat_v", std::slice::from_ref(&loser.id))
            .await
            .unwrap();
        assert!(infos.is_empty());
        let edges = backend.neighbors("chat_v", &loser.id).await.unwrap();
        assert!(edges.is_empty());
    }

    #[tokio::test]
    async fn rollback_merge_restores_the_pre_merge_shape() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, _x, _y, _z], _edges) = merge_batch();
        backend.upsert_batch("chat_w", &batch).await.unwrap();
        let pre_merge = graph_shape(&backend, "chat_w").await;

        let outcome = backend
            .merge_nodes("chat_w", &loser.id, &survivor.id)
            .await
            .unwrap();
        backend
            .rollback_merge("chat_w", &outcome.snapshot_json)
            .await
            .unwrap();

        // The graph is back to the pre-merge shape: the loser and its
        // original edges are restored, the merge-created edges are gone.
        let post_rollback = graph_shape(&backend, "chat_w").await;
        assert_eq!(post_rollback, pre_merge);
    }

    #[tokio::test]
    async fn rollback_merge_refuses_when_the_survivor_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, x, _y, _z], _edges) = merge_batch();
        backend.upsert_batch("chat_x", &batch).await.unwrap();

        let outcome = backend
            .merge_nodes("chat_x", &loser.id, &survivor.id)
            .await
            .unwrap();
        // The survivor is itself tombstoned by a later merge.
        backend
            .merge_nodes("chat_x", &survivor.id, &x.id)
            .await
            .unwrap();

        // Section 7.7 step 4: chained-merge rollback is refused.
        let error = backend
            .rollback_merge("chat_x", &outcome.snapshot_json)
            .await
            .expect_err("rollback must refuse when the survivor is gone");
        assert!(
            error.to_string().contains("survivor"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn rollback_merge_rejects_a_malformed_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, _nodes, _edges) = merge_batch();
        backend.upsert_batch("chat_y", &batch).await.unwrap();

        let error = backend
            .rollback_merge("chat_y", "not a snapshot")
            .await
            .expect_err("a malformed snapshot is a loud error");
        assert!(
            error.to_string().contains("malformed snapshot"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn merge_nodes_of_a_tombstoned_loser_errors_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, _x, _y, _z], _edges) = merge_batch();
        backend.upsert_batch("chat_z", &batch).await.unwrap();
        backend
            .merge_nodes("chat_z", &loser.id, &survivor.id)
            .await
            .unwrap();

        // Section 7.7 step 5: re-merging an already-merged loser is a
        // loud error.
        let error = backend
            .merge_nodes("chat_z", &loser.id, &survivor.id)
            .await
            .expect_err("re-merging a tombstoned loser must error loudly");
        assert!(
            error.to_string().contains("loser"),
            "unexpected error: {error}"
        );
        // A never-existing loser errors the same way.
        let error = backend
            .merge_nodes(
                "chat_z",
                &crate::identifiers::concept_id("nobody"),
                &survivor.id,
            )
            .await
            .expect_err("merging a missing loser must error loudly");
        assert!(
            error.to_string().contains("loser"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn link_also_known_as_creates_the_edge_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let a = concept_node("GRPO", base);
        let b = concept_node("Group Relative Policy Optimization", base);
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(7, 110),
            nodes: vec![a.clone(), b.clone()],
            edges: vec![],
        };
        backend.upsert_batch("chat_aa", &batch).await.unwrap();

        backend
            .link_also_known_as("chat_aa", &a.id, &b.id)
            .await
            .unwrap();
        // The edge runs a -> b with the reserved name (Section 7.7
        // step 2, direction of Section 7.4 step 5).
        let edges = backend
            .query_rows(
                "chat_aa",
                "MATCH (s:Node)-[r:EDGE]->(t:Node) \
                 WHERE r.relationship_name = 'also_known_as' RETURN s.id, t.id, r.edge_text",
            )
            .await
            .unwrap();
        assert_eq!(
            edges,
            vec![vec![
                a.id.clone(),
                b.id.clone(),
                "GRPO is also known as Group Relative Policy Optimization.".to_string(),
            ]]
        );

        // A repeat call matches the existing edge: no duplicate.
        backend
            .link_also_known_as("chat_aa", &a.id, &b.id)
            .await
            .unwrap();
        let count = backend
            .count(
                "chat_aa",
                "MATCH ()-[r:EDGE]->() WHERE r.relationship_name = 'also_known_as' RETURN count(r)",
            )
            .await
            .unwrap();
        assert_eq!(count, 1);

        // A missing endpoint is a loud error, not a silent no-op.
        let error = backend
            .link_also_known_as("chat_aa", &a.id, &crate::identifiers::concept_id("nobody"))
            .await
            .expect_err("linking to a missing node must error loudly");
        assert!(
            error.to_string().contains("does not exist"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn are_linked_matches_both_names_in_both_directions() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let a = concept_node("GRPO", base);
        let b = concept_node("Group Relative Policy Optimization", base);
        let person = MemoryNode {
            id: crate::identifiers::person_id("u1"),
            name: "Ada".to_string(),
            node_type: NodeType::Person,
            created_at: base,
            updated_at: base,
            properties: None,
        };
        let alias = MemoryNode {
            id: crate::identifiers::alias_id("ada"),
            name: "ada".to_string(),
            node_type: NodeType::Alias,
            created_at: base,
            updated_at: base,
            properties: None,
        };
        // The alias edge runs Person -> Alias (known_as), Section 7.4.
        let known_as = fact_edge(&person.id, &alias.id, "known_as", None, base);
        let unrelated_edge = fact_edge(&a.id, &person.id, "mentioned_with", None, base);
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(7, 111),
            nodes: vec![a.clone(), b.clone(), person.clone(), alias.clone()],
            edges: vec![known_as, unrelated_edge],
        };
        backend.upsert_batch("chat_ab", &batch).await.unwrap();

        // also_known_as, written a -> b by link_also_known_as.
        backend
            .link_also_known_as("chat_ab", &a.id, &b.id)
            .await
            .unwrap();

        // Both directions answer true for both relationship names.
        assert!(backend.are_linked("chat_ab", &a.id, &b.id).await.unwrap());
        assert!(backend.are_linked("chat_ab", &b.id, &a.id).await.unwrap());
        assert!(backend
            .are_linked("chat_ab", &person.id, &alias.id)
            .await
            .unwrap());
        assert!(backend
            .are_linked("chat_ab", &alias.id, &person.id)
            .await
            .unwrap());
        // A fact edge is NOT a surface-form link (decision 74 point 6:
        // only known_as/also_known_as exclude a pair).
        assert!(!backend
            .are_linked("chat_ab", &a.id, &person.id)
            .await
            .unwrap());
        // An unknown node is simply not linked.
        assert!(!backend
            .are_linked("chat_ab", &a.id, &crate::identifiers::concept_id("nobody"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn node_merge_stats_reports_degree_and_created_at() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let (batch, [loser, survivor, x, _y, _z], _edges) = merge_batch();
        backend.upsert_batch("chat_ac", &batch).await.unwrap();
        // One more edge on the survivor, so the degree discriminates.
        let extra = fact_edge(&x.id, &survivor.id, "likes", None, base);
        backend
            .upsert_batch(
                "chat_ac",
                &MemoryBatch {
                    batch_id: crate::identifiers::batch_id(8, 112),
                    nodes: vec![],
                    edges: vec![extra],
                },
            )
            .await
            .unwrap();

        let ids = vec![loser.id.clone(), survivor.id.clone()];
        let stats = backend.node_merge_stats("chat_ac", &ids).await.unwrap();
        let map: HashMap<String, NodeMergeStats> = stats.into_iter().collect();
        // Edges in both directions count (the survivor-choice rule of
        // Section 7.7 step 3 knows no direction): merge_batch gives the
        // loser four edges (likes, dislikes, mentions, knows) and the
        // survivor two (knows, likes), plus the extra incoming edge
        // above.
        assert_eq!(map.get(&loser.id).expect("loser stats").edge_degree, 4);
        assert_eq!(
            map.get(&survivor.id).expect("survivor stats").edge_degree,
            3
        );
        assert_eq!(
            map.get(&survivor.id).expect("survivor stats").created_at,
            survivor.created_at
        );
    }

    #[tokio::test]
    async fn node_merge_stats_skips_missing_nodes_and_an_empty_list() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, _survivor, _x, _y, _z], _edges) = merge_batch();
        backend.upsert_batch("chat_ad", &batch).await.unwrap();

        let stats = backend
            .node_merge_stats(
                "chat_ad",
                &[loser.id.clone(), crate::identifiers::concept_id("nobody")],
            )
            .await
            .unwrap();
        assert_eq!(stats.len(), 1, "a missing node id does not occur");
        assert_eq!(stats[0].0, loser.id);

        // An empty id list short-circuits without touching the group.
        let stats = backend
            .node_merge_stats("chat_never_opened", &[])
            .await
            .unwrap();
        assert!(stats.is_empty());
        assert!(!dir.path().join("chat_never_opened").exists());
    }

    // Decision 75 test data.

    fn registry() -> Vec<String> {
        vec!["works_at".to_string()]
    }

    fn person_node(name: &str, user_id: &str, created_at: OffsetDateTime) -> MemoryNode {
        MemoryNode {
            id: crate::identifiers::person_id(user_id),
            name: name.to_string(),
            node_type: NodeType::Person,
            created_at,
            updated_at: created_at,
            properties: None,
        }
    }

    fn org_node(name: &str, created_at: OffsetDateTime) -> MemoryNode {
        concept_node(name, created_at)
    }

    /// A Person with its Section 7.4 step 5 alias node and the `known_as`
    /// edge, so `node_facts` can resolve the name through the exact-alias
    /// machinery.
    fn aliased_person(
        name: &str,
        user_id: &str,
        created_at: OffsetDateTime,
    ) -> (MemoryNode, MemoryNode, MemoryEdge) {
        let person = person_node(name, user_id, created_at);
        let alias = MemoryNode {
            id: crate::identifiers::alias_id(name),
            name: crate::identifiers::normalize(name),
            node_type: NodeType::Alias,
            created_at,
            updated_at: created_at,
            properties: None,
        };
        let known_as = fact_edge(&person.id, &alias.id, "known_as", None, created_at);
        (person, alias, known_as)
    }

    /// A valid fact edge with an explicit `valid_at` (the decision-75
    /// tests separate `valid_at` from `created_at`).
    fn works_at_edge(
        source_id: &str,
        target_id: &str,
        valid_at: OffsetDateTime,
        created_at: OffsetDateTime,
    ) -> MemoryEdge {
        MemoryEdge {
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            relationship_name: "works_at".to_string(),
            valid_at,
            invalid_at: None,
            edge_text: format!("{source_id} works_at {target_id}"),
            created_at,
            updated_at: created_at,
            properties: None,
        }
    }

    fn works_at_valid(facts: &NodeFacts) -> Vec<&NodeFactEdge> {
        facts
            .edges
            .iter()
            .filter(|edge| edge.relationship_name == "works_at" && edge.invalid_at.is_none())
            .collect()
    }

    fn works_at_invalid(facts: &NodeFacts) -> Vec<&NodeFactEdge> {
        facts
            .edges
            .iter()
            .filter(|edge| edge.relationship_name == "works_at" && edge.invalid_at.is_some())
            .collect()
    }

    #[tokio::test]
    async fn upsert_batch_with_registry_invalidates_single_value_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let (person, alias, known_as) = aliased_person("Tama", "2001", base);
        let org_a = org_node("OrgA", base);
        let org_b = org_node("OrgB", base);
        let first = works_at_edge(
            &person.id,
            &org_a.id,
            base + Duration::seconds(1),
            base + Duration::seconds(1),
        );
        let second = works_at_edge(
            &person.id,
            &org_b.id,
            base + Duration::seconds(2),
            base + Duration::seconds(2),
        );
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 120),
            nodes: vec![person.clone(), alias, org_a.clone(), org_b.clone()],
            edges: vec![known_as, first, second],
        };

        // The second single-value edge of the batch invalidates the first
        // (batch order, last write wins, decision 75 (b)).
        let outcome = backend
            .upsert_batch_with_registry("chat_d75a", &batch, &registry())
            .await
            .unwrap();
        assert_eq!(outcome.invalidated, 1);

        let facts = backend
            .node_facts("chat_d75a", "Tama")
            .await
            .unwrap()
            .expect("the person resolves through its alias");
        assert_eq!(facts.node_id, person.id);
        let valid = works_at_valid(&facts);
        assert_eq!(valid.len(), 1, "exactly one valid works_at at commit");
        assert_eq!(valid[0].other_node_name, "OrgB");
        let invalid = works_at_invalid(&facts);
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].other_node_name, "OrgA");
    }

    #[tokio::test]
    async fn two_same_predicate_edges_in_one_batch_last_write_wins() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let (person, alias, known_as) = aliased_person("Tama", "2002", base);
        let org_a = org_node("OrgA", base);
        let org_b = org_node("OrgB", base);
        // Same (subject, predicate), distinct natural keys; the SECOND
        // edge of the batch must win.
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 121),
            nodes: vec![person.clone(), alias, org_a.clone(), org_b.clone()],
            edges: vec![
                known_as,
                works_at_edge(
                    &person.id,
                    &org_a.id,
                    base + Duration::seconds(1),
                    base + Duration::seconds(1),
                ),
                works_at_edge(
                    &person.id,
                    &org_b.id,
                    base + Duration::seconds(2),
                    base + Duration::seconds(2),
                ),
            ],
        };
        let outcome = backend
            .upsert_batch_with_registry("chat_d75b", &batch, &registry())
            .await
            .unwrap();
        assert_eq!(outcome.invalidated, 1);

        // No duplicate edges, exactly one valid — the last write.
        let total = backend
            .count(
                "chat_d75b",
                "MATCH ()-[r:EDGE]->() WHERE r.relationship_name = 'works_at' RETURN count(r)",
            )
            .await
            .unwrap();
        assert_eq!(total, 2);
        let valid = backend
            .count(
                "chat_d75b",
                "MATCH ()-[r:EDGE]->() WHERE r.relationship_name = 'works_at' AND r.invalid_at IS NULL RETURN count(r)",
            )
            .await
            .unwrap();
        assert_eq!(valid, 1);
        let facts = backend
            .node_facts("chat_d75b", "Tama")
            .await
            .unwrap()
            .unwrap();
        let valid = works_at_valid(&facts);
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].other_node_name, "OrgB");
    }

    #[tokio::test]
    async fn unregistered_predicate_accumulates() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let (person, alias, known_as) = aliased_person("Tama", "2003", base);
        let org_a = org_node("OrgA", base);
        let org_b = org_node("OrgB", base);
        // `likes` is absent from the registry: multi-value, accumulate.
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 122),
            nodes: vec![person.clone(), alias, org_a.clone(), org_b.clone()],
            edges: vec![
                known_as,
                fact_edge(
                    &person.id,
                    &org_a.id,
                    "likes",
                    None,
                    base + Duration::seconds(1),
                ),
                fact_edge(
                    &person.id,
                    &org_b.id,
                    "likes",
                    None,
                    base + Duration::seconds(2),
                ),
            ],
        };
        let outcome = backend
            .upsert_batch_with_registry("chat_d75c", &batch, &registry())
            .await
            .unwrap();
        assert_eq!(outcome.invalidated, 0);
        let facts = backend
            .node_facts("chat_d75c", "Tama")
            .await
            .unwrap()
            .unwrap();
        // Both likes edges stay valid.
        let likes_valid: Vec<&NodeFactEdge> = facts
            .edges
            .iter()
            .filter(|edge| edge.relationship_name == "likes" && edge.invalid_at.is_none())
            .collect();
        assert_eq!(likes_valid.len(), 2);
        let likes_invalid: Vec<&NodeFactEdge> = facts
            .edges
            .iter()
            .filter(|edge| edge.relationship_name == "likes" && edge.invalid_at.is_some())
            .collect();
        assert_eq!(likes_invalid.len(), 0);
    }

    #[tokio::test]
    async fn replayed_batch_converges() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let (person, alias, known_as) = aliased_person("Tama", "2004", base);
        let org_a = org_node("OrgA", base);
        let grpo = org_node("GRPO", base);
        // One single-value edge per (subject, predicate) pair — the shape
        // that converges with zero invalidations on replay.
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 123),
            nodes: vec![person.clone(), alias, org_a.clone(), grpo.clone()],
            edges: vec![
                known_as,
                works_at_edge(
                    &person.id,
                    &org_a.id,
                    base + Duration::seconds(1),
                    base + Duration::seconds(1),
                ),
                fact_edge(
                    &person.id,
                    &grpo.id,
                    "likes",
                    None,
                    base + Duration::seconds(1),
                ),
            ],
        };
        let first = backend
            .upsert_batch_with_registry("chat_d75d", &batch, &registry())
            .await
            .unwrap();
        assert_eq!(first.invalidated, 0);
        let (nodes_first, edges_first) = graph_shape(&backend, "chat_d75d").await;

        // Between the runs the operator invalidates the fact; the replay
        // must re-validate it (MERGE_EDGE's ON MATCH sets invalid_at from
        // the batch's own NULL).
        let facts = backend
            .node_facts("chat_d75d", "Tama")
            .await
            .unwrap()
            .unwrap();
        let edge_id = works_at_valid(&facts)[0].edge_id.clone();
        backend
            .invalidate_edge("chat_d75d", &edge_id, base + Duration::hours(1))
            .await
            .unwrap();
        assert!(works_at_valid(
            &backend
                .node_facts("chat_d75d", "Tama")
                .await
                .unwrap()
                .unwrap()
        )
        .is_empty());

        // The identical batch replayed: the deterministic natural key
        // MERGEs, the siblings pass excludes the replayed edge itself (the
        // WHERE clause spares the full natural key and the only sibling is
        // already invalid), and the MERGE re-validates. Convergent: no
        // duplicates, exactly one valid works_at edge, ZERO invalidations.
        let second = backend
            .upsert_batch_with_registry("chat_d75d", &batch, &registry())
            .await
            .unwrap();
        assert_eq!(second.invalidated, 0);
        let total = backend
            .count(
                "chat_d75d",
                "MATCH ()-[r:EDGE]->() WHERE r.relationship_name = 'works_at' RETURN count(r)",
            )
            .await
            .unwrap();
        assert_eq!(total, 1, "no duplicate edges on replay");
        let valid = backend
            .count(
                "chat_d75d",
                "MATCH ()-[r:EDGE]->() WHERE r.relationship_name = 'works_at' AND r.invalid_at IS NULL RETURN count(r)",
            )
            .await
            .unwrap();
        assert_eq!(valid, 1, "the replayed edge is re-validated");

        // State-wise no-op: every edge row is bit-identical (all replayed
        // columns come from the batch, invalid_at and updated_at
        // included). The node rows are identical except the batch
        // skeleton's wall-clock updated_at (MERGE_BATCH ON MATCH), which
        // is excluded here.
        let (nodes_second, edges_second) = graph_shape(&backend, "chat_d75d").await;
        assert_eq!(edges_first, edges_second);
        let node_key_columns = |rows: Vec<Vec<String>>| -> Vec<Vec<String>> {
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .enumerate()
                        .filter(|(index, _)| *index != 4)
                        .map(|(_, value)| value)
                        .collect()
                })
                .collect()
        };
        assert_eq!(
            node_key_columns(nodes_first),
            node_key_columns(nodes_second)
        );
    }

    #[tokio::test]
    async fn invalidate_revalidate_round_trip_through_node_facts() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let (person, alias, known_as) = aliased_person("Tama", "2005", base);
        let org_a = org_node("OrgA", base);
        let org_b = org_node("OrgB", base);
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 124),
            nodes: vec![person.clone(), alias, org_a.clone(), org_b.clone()],
            edges: vec![
                known_as,
                works_at_edge(
                    &person.id,
                    &org_a.id,
                    base + Duration::seconds(1),
                    base + Duration::seconds(1),
                ),
                works_at_edge(
                    &person.id,
                    &org_b.id,
                    base + Duration::seconds(2),
                    base + Duration::seconds(2),
                ),
            ],
        };
        backend
            .upsert_batch_with_registry("chat_d75e", &batch, &registry())
            .await
            .unwrap();

        // The OrgB edge is the valid one; invalidate it manually.
        let facts = backend
            .node_facts("chat_d75e", "Tama")
            .await
            .unwrap()
            .unwrap();
        let valid = works_at_valid(&facts);
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].other_node_name, "OrgB");
        let valid_edge_id = valid[0].edge_id.clone();
        let now = base + Duration::hours(1);
        assert!(backend
            .invalidate_edge("chat_d75e", &valid_edge_id, now)
            .await
            .unwrap());

        // node_facts shows it invalid. OrgA was ALREADY invalid from the
        // write path, so both works_at edges are invalid now and none is
        // valid.
        let facts = backend
            .node_facts("chat_d75e", "Tama")
            .await
            .unwrap()
            .unwrap();
        assert!(works_at_valid(&facts).is_empty());
        let invalid = works_at_invalid(&facts);
        assert_eq!(invalid.len(), 2);
        let org_b_edge = invalid
            .iter()
            .find(|edge| edge.other_node_name == "OrgB")
            .expect("the OrgB edge is listed");
        assert_eq!(org_b_edge.invalid_at, Some(now));
        assert!(invalid.iter().any(|edge| edge.other_node_name == "OrgA"));

        // Revalidate (the typo safety net): OrgB is valid again, PLAIN —
        // no sibling invalidation, so OrgA stays invalid.
        assert!(backend
            .revalidate_edge("chat_d75e", &valid_edge_id, now + Duration::minutes(1))
            .await
            .unwrap());
        let facts = backend
            .node_facts("chat_d75e", "Tama")
            .await
            .unwrap()
            .unwrap();
        let valid = works_at_valid(&facts);
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].other_node_name, "OrgB");
        let invalid = works_at_invalid(&facts);
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].other_node_name, "OrgA");

        // PLAIN re-validation, proven: revalidate the OrgA edge too. A
        // sibling-invalidating re-validation would flip OrgB back to
        // invalid; plain re-validation leaves BOTH valid.
        let org_a_edge_id = invalid[0].edge_id.clone();
        assert!(backend
            .revalidate_edge("chat_d75e", &org_a_edge_id, now + Duration::minutes(2))
            .await
            .unwrap());
        let facts = backend
            .node_facts("chat_d75e", "Tama")
            .await
            .unwrap()
            .unwrap();
        let valid = works_at_valid(&facts);
        assert_eq!(valid.len(), 2, "re-validation never invalidates siblings");
        assert!(valid.iter().any(|edge| edge.other_node_name == "OrgA"));
        assert!(valid.iter().any(|edge| edge.other_node_name == "OrgB"));
    }

    #[tokio::test]
    async fn manual_ops_error_loudly_on_malformed_and_unknown_edge_ids() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let person = person_node("Tama", "2006", base);
        let org_a = org_node("OrgA", base);
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 125),
            nodes: vec![person.clone(), org_a.clone()],
            edges: vec![works_at_edge(
                &person.id,
                &org_a.id,
                base + Duration::seconds(1),
                base + Duration::seconds(1),
            )],
        };
        backend
            .upsert_batch_with_registry("chat_d75f", &batch, &registry())
            .await
            .unwrap();
        let now = base + Duration::hours(1);

        // Malformed edge ids: loud errors at the parse site, both ops.
        for bad in ["not json", r#"{"source_id":"x"}"#] {
            for result in [
                backend
                    .invalidate_edge("chat_d75f", bad, now)
                    .await
                    .map(|_| ()),
                backend
                    .revalidate_edge("chat_d75f", bad, now)
                    .await
                    .map(|_| ()),
            ] {
                let error = result.expect_err("a malformed edge id must error loudly");
                assert!(
                    error.to_string().contains("invalid edge id"),
                    "unexpected error: {error}"
                );
            }
        }

        // Well-formed but unknown natural keys: loud errors, both ops.
        let unknown = EdgeId {
            source_id: person.id.clone(),
            relationship_name: "works_at".to_string(),
            target_id: org_a.id.clone(),
            valid_at: base + Duration::seconds(999),
        }
        .encode();
        for result in [
            backend
                .invalidate_edge("chat_d75f", &unknown, now)
                .await
                .map(|_| ()),
            backend
                .revalidate_edge("chat_d75f", &unknown, now)
                .await
                .map(|_| ()),
        ] {
            let error = result.expect_err("an unknown edge id must error loudly");
            assert!(
                error.to_string().contains("no edge matches"),
                "unexpected error: {error}"
            );
        }
    }

    #[tokio::test]
    async fn manual_ops_on_already_settled_edges_are_no_ops() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let (person, alias, known_as) = aliased_person("Tama", "2007", base);
        let org_a = org_node("OrgA", base);
        let org_b = org_node("OrgB", base);
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 126),
            nodes: vec![person.clone(), alias, org_a.clone(), org_b.clone()],
            edges: vec![
                known_as,
                works_at_edge(
                    &person.id,
                    &org_a.id,
                    base + Duration::seconds(1),
                    base + Duration::seconds(1),
                ),
                works_at_edge(
                    &person.id,
                    &org_b.id,
                    base + Duration::seconds(2),
                    base + Duration::seconds(2),
                ),
            ],
        };
        backend
            .upsert_batch_with_registry("chat_d75g", &batch, &registry())
            .await
            .unwrap();
        let facts = backend
            .node_facts("chat_d75g", "Tama")
            .await
            .unwrap()
            .unwrap();
        let invalid = works_at_invalid(&facts);
        let valid = works_at_valid(&facts);
        assert_eq!(invalid.len(), 1);
        assert_eq!(valid.len(), 1);
        let invalid_edge_id = invalid[0].edge_id.clone();
        let invalid_invalid_at = invalid[0].invalid_at;
        let valid_edge_id = valid[0].edge_id.clone();
        let now = base + Duration::hours(1);

        // Already-invalid invalidate: Ok(false), no double-stamp.
        assert!(!backend
            .invalidate_edge("chat_d75g", &invalid_edge_id, now)
            .await
            .unwrap());
        // Already-valid revalidate: Ok(false).
        assert!(!backend
            .revalidate_edge("chat_d75g", &valid_edge_id, now)
            .await
            .unwrap());

        // The invalid_at of the already-invalid edge is untouched (the
        // no-op must not re-stamp it).
        let facts = backend
            .node_facts("chat_d75g", "Tama")
            .await
            .unwrap()
            .unwrap();
        let invalid = works_at_invalid(&facts);
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].invalid_at, invalid_invalid_at);
        assert_eq!(works_at_valid(&facts).len(), 1);
    }

    #[tokio::test]
    async fn node_facts_lists_both_directions_and_resolves_alias_names() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let person = person_node("Tama", "2008", base);
        let other = person_node("Koko", "2009", base);
        let org = org_node("OrgA", base);
        let grpo = org_node("GRPO", base);
        let alias = MemoryNode {
            id: crate::identifiers::alias_id("tama"),
            name: "tama".to_string(),
            node_type: NodeType::Alias,
            created_at: base,
            updated_at: base,
            properties: None,
        };
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 127),
            nodes: vec![
                person.clone(),
                other.clone(),
                org.clone(),
                grpo.clone(),
                alias.clone(),
            ],
            edges: vec![
                fact_edge(&person.id, &alias.id, "known_as", None, base),
                fact_edge(
                    &person.id,
                    &org.id,
                    "works_at",
                    None,
                    base + Duration::seconds(1),
                ),
                fact_edge(
                    &person.id,
                    &grpo.id,
                    "likes",
                    None,
                    base + Duration::seconds(2),
                ),
                // Incoming edge: the other person knows Tama.
                fact_edge(
                    &other.id,
                    &person.id,
                    "knows",
                    None,
                    base + Duration::seconds(3),
                ),
            ],
        };
        backend
            .upsert_batch_with_registry("chat_d75h", &batch, &registry())
            .await
            .unwrap();

        // By canonical name AND by alias surface form (normalization per
        // identifiers.rs: "TAMA" folds onto the alias of "Tama").
        for name in ["Tama", "tama", "TAMA"] {
            let facts = backend
                .node_facts("chat_d75h", name)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("'{name}' must resolve"));
            assert_eq!(facts.node_id, person.id);
            assert_eq!(facts.node_name, "Tama");
            // Both directions, with the other endpoint's name; alias and
            // provenance-class edges included on purpose.
            assert_eq!(facts.edges.len(), 4);
            let outgoing: Vec<&NodeFactEdge> = facts.edges.iter().filter(|e| e.outgoing).collect();
            assert_eq!(outgoing.len(), 3);
            let incoming: Vec<&NodeFactEdge> = facts.edges.iter().filter(|e| !e.outgoing).collect();
            assert_eq!(incoming.len(), 1);
            assert_eq!(incoming[0].relationship_name, "knows");
            assert_eq!(incoming[0].other_node_name, "Koko");
            let mut names: Vec<&str> = facts
                .edges
                .iter()
                .map(|e| e.other_node_name.as_str())
                .collect();
            names.sort_unstable();
            assert_eq!(names, vec!["GRPO", "Koko", "OrgA", "tama"]);
            // Every edge carries a decodable opaque id.
            for edge in &facts.edges {
                EdgeId::decode(&edge.edge_id).unwrap();
            }
        }

        // An unknown name is None.
        assert_eq!(
            backend.node_facts("chat_d75h", "nobody").await.unwrap(),
            None
        );
    }

    /// Decision 75 (c) test graph: loser and survivor EACH carry one
    /// valid outgoing `works_at` edge (to OrgOld / OrgNew). A merge
    /// without the invariant pass would leave two valid works_at edges
    /// on the survivor.
    fn single_value_merge_batch() -> (MemoryBatch, [MemoryNode; 4]) {
        let base = datetime!(2026-08-07 10:00 UTC);
        let loser = concept_node("Loser", base);
        let survivor = concept_node("Survivor", base);
        let org_old = org_node("OrgOld", base);
        let org_new = org_node("OrgNew", base);
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(11, 130),
            nodes: vec![
                loser.clone(),
                survivor.clone(),
                org_old.clone(),
                org_new.clone(),
            ],
            edges: vec![
                works_at_edge(
                    &loser.id,
                    &org_old.id,
                    base + Duration::seconds(1),
                    base + Duration::seconds(1),
                ),
                works_at_edge(
                    &survivor.id,
                    &org_new.id,
                    base + Duration::seconds(2),
                    base + Duration::seconds(2),
                ),
            ],
        };
        (batch, [loser, survivor, org_old, org_new])
    }

    #[tokio::test]
    async fn merge_nodes_with_registry_enforces_the_single_value_invariant() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, org_old, org_new]) = single_value_merge_batch();
        backend.upsert_batch("chat_d75i", &batch).await.unwrap();

        let outcome = backend
            .merge_nodes_with_registry("chat_d75i", &loser.id, &survivor.id, &registry())
            .await
            .unwrap();
        assert_eq!(outcome.edges_moved, 1, "the loser works_at edge moves");
        assert_eq!(outcome.single_value_invalidated, 1);

        // Exactly ONE valid works_at on the survivor — the NEWEST
        // (OrgNew); the moved OrgOld edge is invalidated.
        let valid = backend
            .count(
                "chat_d75i",
                &format!(
                    "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->() \
                     WHERE r.relationship_name = 'works_at' AND r.invalid_at IS NULL RETURN count(r)",
                    survivor.id
                ),
            )
            .await
            .unwrap();
        assert_eq!(valid, 1);
        let kept = backend
            .query_rows(
                "chat_d75i",
                &format!(
                    "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->(t:Node) \
                     WHERE r.relationship_name = 'works_at' AND r.invalid_at IS NULL RETURN t.id",
                    survivor.id
                ),
            )
            .await
            .unwrap();
        assert_eq!(kept, vec![vec![org_new.id.clone()]]);
        let invalidated = backend
            .query_rows(
                "chat_d75i",
                &format!(
                    "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->(t:Node) \
                     WHERE r.relationship_name = 'works_at' AND r.invalid_at IS NOT NULL RETURN t.id",
                    survivor.id
                ),
            )
            .await
            .unwrap();
        assert_eq!(invalidated, vec![vec![org_old.id.clone()]]);
    }

    #[tokio::test]
    async fn merge_nodes_with_an_empty_registry_preserves_decision_74() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, _org_old, _org_new]) = single_value_merge_batch();
        backend.upsert_batch("chat_d75j", &batch).await.unwrap();

        // The plain delegate: no invariant pass, both works_at edges stay
        // valid on the survivor, the field is zero.
        let outcome = backend
            .merge_nodes("chat_d75j", &loser.id, &survivor.id)
            .await
            .unwrap();
        assert_eq!(outcome.edges_moved, 1);
        assert_eq!(outcome.single_value_invalidated, 0);
        let valid = backend
            .count(
                "chat_d75j",
                &format!(
                    "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->() \
                     WHERE r.relationship_name = 'works_at' AND r.invalid_at IS NULL RETURN count(r)",
                    survivor.id
                ),
            )
            .await
            .unwrap();
        assert_eq!(valid, 2, "decision-74 behavior: no invariant pass");
    }

    /// Tiebreak scenario: loser and survivor each carry one valid
    /// `works_at` edge with the SAME `valid_at`, so the merge leaves two
    /// tied valid edges on the survivor and the invariant pass must pick
    /// by the edge-id string.
    fn tiebreak_batch(base: OffsetDateTime) -> (MemoryBatch, [MemoryNode; 4]) {
        let loser = concept_node("Loser", base);
        let survivor = concept_node("Survivor", base);
        let org_a = org_node("Aaa", base);
        let org_z = org_node("Zzz", base);
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(11, 131),
            nodes: vec![
                loser.clone(),
                survivor.clone(),
                org_a.clone(),
                org_z.clone(),
            ],
            edges: vec![
                works_at_edge(
                    &loser.id,
                    &org_z.id,
                    base + Duration::seconds(1),
                    base + Duration::seconds(1),
                ),
                works_at_edge(
                    &survivor.id,
                    &org_a.id,
                    base + Duration::seconds(1),
                    base + Duration::seconds(2),
                ),
            ],
        };
        (batch, [loser, survivor, org_a, org_z])
    }

    /// Runs one tiebreak merge on a fresh graph and returns the kept
    /// target id plus the two candidate target ids.
    async fn run_tiebreak_merge(chat_id: &str, base: OffsetDateTime) -> (String, String, String) {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [loser, survivor, org_a, org_z]) = tiebreak_batch(base);
        backend.upsert_batch(chat_id, &batch).await.unwrap();
        let outcome = backend
            .merge_nodes_with_registry(chat_id, &loser.id, &survivor.id, &registry())
            .await
            .unwrap();
        assert_eq!(outcome.single_value_invalidated, 1);
        let kept = backend
            .query_rows(
                chat_id,
                &format!(
                    "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->(t:Node) \
                     WHERE r.relationship_name = 'works_at' AND r.invalid_at IS NULL RETURN t.id",
                    survivor.id
                ),
            )
            .await
            .unwrap();
        assert_eq!(kept.len(), 1);
        (kept[0][0].clone(), org_a.id.clone(), org_z.id.clone())
    }

    #[tokio::test]
    async fn merge_invariant_tiebreak_keeps_the_smallest_edge_id_deterministically() {
        let base = datetime!(2026-08-07 10:00 UTC);
        let (kept_first, org_a_id, org_z_id) = run_tiebreak_merge("chat_d75k1", base).await;
        let (kept_second, _, _) = run_tiebreak_merge("chat_d75k2", base).await;

        // Determinism: two fresh graphs keep the SAME edge.
        assert_eq!(kept_first, kept_second);
        // And it is the smallest-edge-id one of the two candidates (the
        // documented tiebreak rule, computed from the ids — uuid5 has no
        // predictable lexical order).
        let survivor_id = crate::identifiers::concept_id("Survivor");
        let id_a = EdgeId {
            source_id: survivor_id.clone(),
            relationship_name: "works_at".to_string(),
            target_id: org_a_id.clone(),
            valid_at: base + Duration::seconds(1),
        }
        .encode();
        let id_z = EdgeId {
            source_id: survivor_id,
            relationship_name: "works_at".to_string(),
            target_id: org_z_id.clone(),
            valid_at: base + Duration::seconds(1),
        }
        .encode();
        let expected = if id_a < id_z { org_a_id } else { org_z_id };
        assert_eq!(kept_first, expected);
    }

    /// Decision 76 test graph: A -likes-> B -likes-> C, all valid.
    fn two_hop_batch() -> (MemoryBatch, [MemoryNode; 3]) {
        let base = datetime!(2026-08-07 10:00 UTC);
        let a = concept_node("Alpha", base);
        let b = concept_node("Beta", base);
        let c = concept_node("Gamma", base);
        let edges = vec![
            fact_edge(&a.id, &b.id, "likes", None, base + Duration::seconds(1)),
            fact_edge(&b.id, &c.id, "likes", None, base + Duration::seconds(2)),
        ];
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(6, 100),
            nodes: vec![a.clone(), b.clone(), c.clone()],
            edges,
        };
        (batch, [a, b, c])
    }

    #[tokio::test]
    async fn two_hop_edges_surfaces_the_hop_two_edges_with_names_hydrated() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [a, b, c]) = two_hop_batch();
        backend.upsert_batch("chat_d76a", &batch).await.unwrap();

        let now = datetime!(2026-08-08 10:00 UTC);
        let candidates = backend
            .two_hop_edges(
                "chat_d76a",
                std::slice::from_ref(&a.id),
                now,
                NEIGHBOR_EXPANSION_LIMIT,
            )
            .await
            .unwrap();
        // Hop 1 surfaces A -> B; hop 2 surfaces B -> C, both with the
        // endpoint names hydrated.
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].source_id, a.id);
        assert_eq!(candidates[0].source_name, "Alpha");
        assert_eq!(candidates[0].target_id, b.id);
        assert_eq!(candidates[0].target_name, "Beta");
        assert_eq!(candidates[1].source_id, b.id);
        assert_eq!(candidates[1].source_name, "Beta");
        assert_eq!(candidates[1].target_id, c.id);
        assert_eq!(candidates[1].target_name, "Gamma");
        assert_eq!(candidates[1].relationship_name, "likes");
        // The opaque edge id is the EdgeId of the natural key and
        // round-trips (specs.md Section 9.3 dedup key shape).
        let key = EdgeId::decode(&candidates[1].edge_id).unwrap();
        assert_eq!(key.source_id, b.id);
        assert_eq!(key.target_id, c.id);
        assert_eq!(key.valid_at, candidates[1].valid_at);
    }

    #[tokio::test]
    async fn two_hop_edges_whitelist_excludes_contains_and_known_as_but_traverses_also_known_as() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let a = concept_node("Alpha", base);
        let n = concept_node("Nick", base);
        let x = concept_node("Xray", base);
        let b = concept_node("Beta", base);
        let y = concept_node("Yankee", base);
        let k = concept_node("Kilo", base);
        let z = concept_node("Zulu", base);
        let edges = vec![
            // known_as: never traversed (surface forms, resolved at
            // entry) — and the edge BEHIND it never surfaces either.
            fact_edge(&a.id, &n.id, "known_as", None, base + Duration::seconds(1)),
            fact_edge(&n.id, &x.id, "likes", None, base + Duration::seconds(2)),
            // contains: never traversed (provenance only) — and no
            // expansion through it.
            fact_edge(&a.id, &b.id, "contains", None, base + Duration::seconds(3)),
            fact_edge(&b.id, &y.id, "likes", None, base + Duration::seconds(4)),
            // also_known_as: INCLUDED (the cross-language bridge,
            // decision 76 (b)) — the edge itself and the hop-2 edge
            // behind it both surface.
            fact_edge(
                &a.id,
                &k.id,
                "also_known_as",
                None,
                base + Duration::seconds(5),
            ),
            fact_edge(&k.id, &z.id, "likes", None, base + Duration::seconds(6)),
        ];
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(7, 110),
            nodes: vec![
                a.clone(),
                n.clone(),
                x.clone(),
                b.clone(),
                y.clone(),
                k.clone(),
                z.clone(),
            ],
            edges,
        };
        backend.upsert_batch("chat_d76b", &batch).await.unwrap();

        let now = datetime!(2026-08-08 10:00 UTC);
        let candidates = backend
            .two_hop_edges(
                "chat_d76b",
                std::slice::from_ref(&a.id),
                now,
                NEIGHBOR_EXPANSION_LIMIT,
            )
            .await
            .unwrap();
        let relationships: Vec<&str> = candidates
            .iter()
            .map(|edge| edge.relationship_name.as_str())
            .collect();
        assert_eq!(relationships, vec!["also_known_as", "likes"]);
        assert_eq!(candidates[0].target_id, k.id);
        assert_eq!(candidates[1].source_id, k.id);
        assert_eq!(candidates[1].target_id, z.id);
    }

    /// Decision 76 test graph with invalid edges: A -likes-> B valid,
    /// B -likes-> C INVALID, A -likes-> D INVALID, D -likes-> E valid.
    fn two_hop_invalid_batch() -> (MemoryBatch, [MemoryNode; 5]) {
        let base = datetime!(2026-08-07 10:00 UTC);
        let nodes: Vec<MemoryNode> = ["Alpha", "Beta", "Gamma", "Delta", "Echo"]
            .iter()
            .map(|name| concept_node(name, base))
            .collect();
        let [a, b, c, d, e]: [MemoryNode; 5] = nodes.try_into().unwrap();
        let edges = vec![
            fact_edge(&a.id, &b.id, "likes", None, base + Duration::seconds(1)),
            // Invalid hop-2 edge: excluded by `invalid_at IS NULL`.
            fact_edge(
                &b.id,
                &c.id,
                "likes",
                Some(base + Duration::seconds(10)),
                base + Duration::seconds(2),
            ),
            // Invalid hop-1 edge: excluded, and D never enters the
            // frontier, so the valid D -> E edge behind it stays out.
            fact_edge(
                &a.id,
                &d.id,
                "likes",
                Some(base + Duration::seconds(10)),
                base + Duration::seconds(3),
            ),
            fact_edge(&d.id, &e.id, "likes", None, base + Duration::seconds(4)),
        ];
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(8, 120),
            nodes: vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone()],
            edges,
        };
        (batch, [a, b, c, d, e])
    }

    #[tokio::test]
    async fn two_hop_edges_excludes_invalid_edges_and_never_expands_through_them() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [a, b, _c, _d, _e]) = two_hop_invalid_batch();
        backend.upsert_batch("chat_d76c", &batch).await.unwrap();

        let now = datetime!(2026-08-08 10:00 UTC);
        let candidates = backend
            .two_hop_edges(
                "chat_d76c",
                std::slice::from_ref(&a.id),
                now,
                NEIGHBOR_EXPANSION_LIMIT,
            )
            .await
            .unwrap();
        // Only the valid hop-1 edge A -> B survives.
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].source_id, a.id);
        assert_eq!(candidates[0].target_id, b.id);
    }

    #[tokio::test]
    async fn list_all_edges_returns_every_edge_valid_and_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, nodes) = two_hop_invalid_batch();
        let [a, b, c, d, e] = nodes;
        backend.upsert_batch("chat_d76d", &batch).await.unwrap();

        let listed = backend.list_all_edges("chat_d76d").await.unwrap();
        // The reconciliation diff needs the FULL edge set: all four
        // edges, the two invalid ones included.
        assert_eq!(listed.len(), 4);
        let mut keys: Vec<(String, String, String)> = listed
            .iter()
            .map(|(edge_id, edge_text)| {
                // Every listed id round-trips through EdgeId::decode —
                // the same id shape the sidecar diff reconciles against.
                let key = EdgeId::decode(edge_id).unwrap();
                assert!(edge_text.contains("likes"));
                (key.source_id, key.relationship_name, key.target_id)
            })
            .collect();
        keys.sort();
        let mut expected: Vec<(String, String, String)> = vec![
            (a.id.clone(), "likes".to_string(), b.id.clone()),
            (b.id.clone(), "likes".to_string(), c.id.clone()),
            (a.id.clone(), "likes".to_string(), d.id.clone()),
            (d.id.clone(), "likes".to_string(), e.id.clone()),
        ];
        expected.sort();
        assert_eq!(keys, expected);
    }

    #[tokio::test]
    async fn two_hop_edges_truncates_per_node_at_both_hops() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let hub = concept_node("Hub", base);
        let mut nodes = vec![hub.clone()];
        let mut edges = Vec::new();
        // Three hop-1 edges on the entry node, increasing created_at.
        for i in 0..3 {
            let target = concept_node(&format!("Hop1{i}"), base);
            edges.push(fact_edge(
                &hub.id,
                &target.id,
                "mentions",
                None,
                base + Duration::seconds(i + 1),
            ));
            nodes.push(target);
        }
        // Three hop-2 edges on the NEWEST hop-1 neighbor (Hop12),
        // newer than every hop-1 edge.
        let hop12 = crate::identifiers::concept_id("Hop12");
        for j in 0..3 {
            let target = concept_node(&format!("Hop2{j}"), base);
            edges.push(fact_edge(
                &hop12,
                &target.id,
                "mentions",
                None,
                base + Duration::seconds(10 + j),
            ));
            nodes.push(target);
        }
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(9, 130),
            nodes,
            edges,
        };
        backend.upsert_batch("chat_d76e", &batch).await.unwrap();

        // The test passes a SMALL per-node limit of 2; the call site
        // passes NEIGHBOR_EXPANSION_LIMIT.
        let now = datetime!(2026-08-08 10:00 UTC);
        let candidates = backend
            .two_hop_edges("chat_d76e", std::slice::from_ref(&hub.id), now, 2)
            .await
            .unwrap();
        let targets: Vec<&str> = candidates
            .iter()
            .map(|edge| edge.target_name.as_str())
            .collect();
        // Hop 1 keeps the 2 NEWEST edges of Hub (Section 8.2 truncation
        // by created_at descending); hop 2 then queries Hop12 and Hop11
        // and keeps the 2 newest edges of Hop12. Hop11 carries only the
        // already-collected hub edge, so it adds nothing.
        assert_eq!(targets, vec!["Hop12", "Hop11", "Hop22", "Hop21"]);
        // The hop-2 expansion of Hop12 was itself truncated: Hop20
        // never surfaces.
        assert!(!candidates.iter().any(|edge| edge.target_name == "Hop20"));
    }

    #[tokio::test]
    async fn two_hop_edges_applies_the_ninety_day_window() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let old = datetime!(2026-01-01 10:00 UTC);
        let recent = datetime!(2026-08-20 10:00 UTC);
        let a = concept_node("Alpha", recent);
        let b = concept_node("Beta", recent);
        let c = concept_node("Gamma", recent);
        let d = concept_node("Delta", recent);
        // valid_at = created_at = old: outside the 90-day window by
        // BOTH measures (decision 76 (a) keeps the Section 8.2 window,
        // unlike the decision-58 shallow read).
        let old_edge = fact_edge(&a.id, &b.id, "likes", None, old);
        // A recent edge BEHIND the windowed-out edge: B never enters
        // the frontier, so B -> C must not surface either.
        let behind = fact_edge(&b.id, &c.id, "likes", None, recent);
        // Old valid_at but recent created_at: inside the window — the
        // Section 8.2 window qualifies an edge recent by EITHER measure.
        let revalidated = MemoryEdge {
            valid_at: old,
            ..fact_edge(&a.id, &d.id, "likes", None, recent)
        };
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(10, 140),
            nodes: vec![a.clone(), b.clone(), c.clone(), d.clone()],
            edges: vec![old_edge, behind, revalidated],
        };
        backend.upsert_batch("chat_d76f", &batch).await.unwrap();

        let now = datetime!(2026-08-21 10:00 UTC);
        let candidates = backend
            .two_hop_edges(
                "chat_d76f",
                std::slice::from_ref(&a.id),
                now,
                NEIGHBOR_EXPANSION_LIMIT,
            )
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].source_id, a.id);
        assert_eq!(candidates[0].target_id, d.id);
        assert_eq!(candidates[0].valid_at, old);
    }

    #[tokio::test]
    async fn two_hop_edges_with_an_empty_entry_list_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [_a, _b, _c]) = two_hop_batch();
        backend.upsert_batch("chat_d76g", &batch).await.unwrap();

        let now = datetime!(2026-08-08 10:00 UTC);
        let candidates = backend
            .two_hop_edges("chat_d76g", &[], now, NEIGHBOR_EXPANSION_LIMIT)
            .await
            .unwrap();
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn edges_by_ids_hydrates_and_drops_stale_malformed_and_invalid_ids() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let base = datetime!(2026-08-07 10:00 UTC);
        let a = concept_node("Alpha", base);
        let b = concept_node("Beta", base);
        let c = concept_node("Gamma", base);
        let valid_edge = fact_edge(&a.id, &b.id, "likes", None, base);
        let invalid_edge = fact_edge(
            &a.id,
            &c.id,
            "likes",
            Some(base + Duration::seconds(10)),
            base + Duration::seconds(1),
        );
        let valid_id = EdgeId {
            source_id: a.id.clone(),
            relationship_name: "likes".to_string(),
            target_id: b.id.clone(),
            valid_at: valid_edge.valid_at,
        }
        .encode();
        let invalid_id = EdgeId {
            source_id: a.id.clone(),
            relationship_name: "likes".to_string(),
            target_id: c.id.clone(),
            valid_at: invalid_edge.valid_at,
        }
        .encode();
        // A well-formed id whose natural key matches no edge row: the
        // sidecar row outlived its graph edge (normal between
        // reconciliations, Section 7.6 step 6).
        let stale_id = EdgeId {
            source_id: a.id.clone(),
            relationship_name: "likes".to_string(),
            target_id: crate::identifiers::concept_id("Nobody"),
            valid_at: base,
        }
        .encode();
        let batch = MemoryBatch {
            batch_id: crate::identifiers::batch_id(11, 150),
            nodes: vec![a.clone(), b.clone(), c.clone()],
            edges: vec![valid_edge, invalid_edge],
        };
        backend.upsert_batch("chat_d76h", &batch).await.unwrap();

        let ids = vec![
            valid_id.clone(),
            invalid_id,
            stale_id,
            "this is not an edge id".to_string(),
        ];
        let candidates = backend.edges_by_ids("chat_d76h", &ids).await.unwrap();
        // Exactly the one valid, existing edge hydrates, with the
        // endpoint names. The invalid edge (valid-only hydration), the
        // stale id, and the malformed id are silently dropped.
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].edge_id, valid_id);
        assert_eq!(candidates[0].source_name, "Alpha");
        assert_eq!(candidates[0].target_name, "Beta");
        assert_eq!(candidates[0].edge_text, format!("{} likes {}", a.id, b.id));
    }

    #[tokio::test]
    async fn edges_by_ids_with_an_empty_id_list_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LbugBackend::new(dir.path());
        let (batch, [_a, _b, _c]) = two_hop_batch();
        backend.upsert_batch("chat_d76i", &batch).await.unwrap();

        let candidates = backend.edges_by_ids("chat_d76i", &[]).await.unwrap();
        assert!(candidates.is_empty());
    }
}
