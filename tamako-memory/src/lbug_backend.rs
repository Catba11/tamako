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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use lbug::{Connection, Database, LogicalType, PreparedStatement, SystemConfig, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::backend::{
    AliasTarget, MemoryBackend, MemoryBatch, MemoryError, MergeOutcome, MergeSnapshot,
    MergeSnapshotEdge, MergeSnapshotEdgeKey, MergeSnapshotNode, NeighborEdge, NodeContent,
    NodeMergeStats, NodeResolutionInfo, NodeType, Result, NEIGHBOR_EXPANSION_LIMIT,
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
/// come up.
fn transact(conn: &Connection, f: impl FnOnce(&Connection) -> Result<()>) -> Result<()> {
    conn.query("BEGIN TRANSACTION").map_err(backend)?;
    match f(conn) {
        Ok(()) => {
            conn.query("COMMIT").map_err(backend)?;
        }
        Err(error) => {
            let _ = conn.query("ROLLBACK");
            return Err(error);
        }
    }
    conn.query("CHECKPOINT").map_err(backend)?;
    Ok(())
}

/// Writes the batch inside one transaction. Section 7.6 step 4: CHECKPOINT
/// at the end.
fn write_batch(conn: &Connection, batch: &MemoryBatch) -> Result<()> {
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

    let mut statement = conn.prepare(MERGE_EDGE).map_err(backend)?;
    for edge in &batch.edges {
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
    Ok(())
}

impl MemoryBackend for LbugBackend {
    async fn ensure_schema(&self, chat_id: &str) -> Result<()> {
        // Open runs the idempotent DDL of Section 6.1.
        self.database(chat_id).await.map(|_| ())
    }

    async fn upsert_batch(&self, chat_id: &str, batch: &MemoryBatch) -> Result<()> {
        let batch = batch.clone();
        self.with_conn(chat_id, move |conn| {
            // Section 5.2 rule 5: CHECKPOINT after each batch write
            // (inside `transact`).
            transact(conn, |conn| write_batch(conn, &batch))
        })
        .await
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
            let mut statement = conn.prepare(ALIAS_TARGETS).map_err(backend)?;
            let result = conn
                .execute(
                    &mut statement,
                    vec![("alias_id", Value::String(alias_node_id))],
                )
                .map_err(backend)?;
            let mut targets = Vec::new();
            for row in result {
                let mut columns = row.into_iter();
                if let (Some(Value::String(node_id)), Some(Value::String(type_string))) =
                    (columns.next(), columns.next())
                {
                    // A row with an unknown type string is skipped, it
                    // does not fail the query.
                    if let Some(node_type) = NodeType::from_str(&type_string) {
                        targets.push(AliasTarget { node_id, node_type });
                    }
                }
            }
            Ok(targets)
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

    /// Decision 74 / Section 7.7 step 3. All snapshot reads happen
    /// BEFORE the transaction begins (the snapshot must precede any
    /// mutation), so the transaction is write-only and the
    /// intermediate-read question of a multi-statement transaction
    /// never comes up. Serialized per group by `with_conn` (decision
    /// 47).
    async fn merge_nodes(
        &self,
        chat_id: &str,
        loser_id: &str,
        survivor_id: &str,
    ) -> Result<MergeOutcome> {
        let loser_id = loser_id.to_string();
        let survivor_id = survivor_id.to_string();
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

            transact(conn, |conn| {
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
                Ok(())
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
            })
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
}
