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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use lbug::{Connection, Database, LogicalType, SystemConfig, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::backend::{
    AliasTarget, MemoryBackend, MemoryBatch, MemoryError, NeighborEdge, NodeType, Result,
    NEIGHBOR_EXPANSION_LIMIT,
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
}

impl LbugBackend {
    /// Creates a backend with databases under `data_root`.
    pub fn new(data_root: impl Into<PathBuf>) -> Self {
        LbugBackend {
            data_root: data_root.into(),
            databases: Mutex::new(HashMap::new()),
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

    /// Runs `f` on a fresh connection of the group database, inside
    /// `tokio::task::spawn_blocking` (Section 5.2 rule 3).
    async fn with_conn<F, T>(&self, chat_id: &str, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let db = self.database(chat_id).await?;
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
            conn.query("BEGIN TRANSACTION").map_err(backend)?;
            match write_batch(conn, &batch) {
                Ok(()) => {
                    conn.query("COMMIT").map_err(backend)?;
                }
                Err(error) => {
                    let _ = conn.query("ROLLBACK");
                    return Err(error);
                }
            }
            // Section 5.2 rule 5: CHECKPOINT after each batch write.
            conn.query("CHECKPOINT").map_err(backend)?;
            Ok(())
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
}
