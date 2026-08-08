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
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::backend::{MemoryBackend, MemoryBatch, MemoryError, Result};

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
    /// Runs a `RETURN count(...)` query. Test helper.
    async fn count(&self, chat_id: &str, cypher: &str) -> Result<i64> {
        let cypher = cypher.to_string();
        self.with_conn(chat_id, move |conn| {
            let mut result = conn.query(&cypher).map_err(backend)?;
            match result.next().and_then(|row| row.into_iter().next()) {
                Some(Value::Int64(count)) => Ok(count),
                _ => Err(MemoryError::Backend("unexpected count result".to_string())),
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemoryEdge, MemoryNode, NodeType};
    use time::macros::datetime;

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
}
