//! Store type and row types for store.db. Refer to specs.md Section 5.
//!
//! Rule P5: one SQLite file per group at `{data_root}/{chat_id}/store.db`.
//! All store APIs take a `chat_id`. One group's data never crosses into
//! another group.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use rusqlite::{Connection, OptionalExtension};
use time::OffsetDateTime;

use crate::error::{Result, StoreError};
use crate::schema;

/// Direction of a log row. Rule B1 (specs.md Section 10.4): the bot's own
/// messages are written to the raw log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Direction::Inbound => "inbound",
            Direction::Outbound => "outbound",
        }
    }

    fn from_str(s: &str) -> rusqlite::Result<Self> {
        match s {
            "inbound" => Ok(Direction::Inbound),
            "outbound" => Ok(Direction::Outbound),
            _ => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown direction: {s}").into(),
            )),
        }
    }
}

/// Kind of log event. An edit is appended as a new row (specs.md Section 15,
/// open item 4); it is never a retraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Message,
    Edit,
}

impl EventType {
    fn as_str(self) -> &'static str {
        match self {
            EventType::Message => "message",
            EventType::Edit => "edit",
        }
    }

    fn from_str(s: &str) -> rusqlite::Result<Self> {
        match s {
            "message" => Ok(EventType::Message),
            "edit" => Ok(EventType::Edit),
            _ => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown event_type: {s}").into(),
            )),
        }
    }
}

/// A new log row to append. Rule P1: every inbound message is persisted
/// before any processing.
#[derive(Debug, Clone)]
pub struct NewMessage {
    pub platform_msg_id: String,
    pub direction: Direction,
    pub event_type: EventType,
    pub timestamp: OffsetDateTime,
    pub sender_id: String,
    pub sender_display_name: String,
    pub text: String,
    pub reply_to_platform_msg_id: Option<String>,
    pub mentions_bot: bool,
    pub is_reply_to_bot: bool,
}

/// A row of the raw message log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRow {
    pub id: i64,
    pub platform_msg_id: String,
    pub direction: Direction,
    pub event_type: EventType,
    pub timestamp: OffsetDateTime,
    pub sender_id: String,
    pub sender_display_name: String,
    pub text: String,
    pub reply_to_platform_msg_id: Option<String>,
    pub mentions_bot: bool,
    pub is_reply_to_bot: bool,
}

/// Result of an idempotent log insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome {
    /// Row was inserted; carries the new rowid.
    Inserted(i64),
    /// Row already existed. The insert is idempotent (AGENT.md Section 6.2).
    Duplicate,
}

/// A row of the `injected_memories` dedup table. Refer to specs.md
/// Section 9.3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectedMemoryRow {
    pub id: i64,
    pub edge_id: String,
    /// Id of the log row after which the memory was injected.
    pub injection_position: i64,
    /// Message-id range tag of the current chunk.
    pub range_tag: String,
    /// The rendered injection text. Persisted for the bit-identical
    /// context rebuild of specs.md Section 7.1 (Rule P1): the text is not
    /// derivable from the graph.
    pub content: String,
}

/// A row of the `dead_letter` table. Refer to specs.md Section 10.3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterRow {
    pub id: i64,
    pub batch_id: String,
    /// JSON skeleton of the failed batch.
    pub batch_skeleton: String,
    pub error: String,
    pub created_at: OffsetDateTime,
}

/// Synchronous store rooted at one data root. One connection per group,
/// opened lazily and cached. Refer to specs.md Section 5.
pub struct Store {
    data_root: PathBuf,
    connections: Mutex<HashMap<String, Connection>>,
}

impl Store {
    pub fn new(data_root: impl Into<PathBuf>) -> Self {
        Store {
            data_root: data_root.into(),
            connections: Mutex::new(HashMap::new()),
        }
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    /// Rule P5: creates {data_root}/{chat_id}/ if needed, opens store.db,
    /// sets WAL + synchronous=NORMAL, and runs pending migrations.
    /// Rejects chat_id values containing '/', '\\', '\0', or "..".
    pub fn open_group(&self, chat_id: &str) -> Result<()> {
        validate_chat_id(chat_id)?;
        let dir = self.data_root.join(chat_id);
        std::fs::create_dir_all(&dir)?;
        let mut conn = Connection::open(dir.join("store.db"))?;
        // Refer to specs.md Section 5.1: WAL mode, synchronous=NORMAL.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        schema::run_migrations(&mut conn)?;
        self.lock().insert(chat_id.to_string(), conn);
        Ok(())
    }

    /// Rule P1: append to the raw log. Idempotent: UNIQUE(platform_msg_id,
    /// direction, event_type, timestamp) + INSERT OR IGNORE.
    pub fn insert_message(&self, chat_id: &str, msg: &NewMessage) -> Result<InsertOutcome> {
        self.with_conn(chat_id, |conn| {
            let timestamp = schema::format_rfc3339(msg.timestamp)?;
            let n = conn.execute(
                "INSERT OR IGNORE INTO messages (
                    platform_msg_id, direction, event_type, timestamp,
                    sender_id, sender_display_name, text,
                    reply_to_platform_msg_id, mentions_bot, is_reply_to_bot
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    msg.platform_msg_id,
                    msg.direction.as_str(),
                    msg.event_type.as_str(),
                    timestamp,
                    msg.sender_id,
                    msg.sender_display_name,
                    msg.text,
                    msg.reply_to_platform_msg_id,
                    msg.mentions_bot,
                    msg.is_reply_to_bot,
                ],
            )?;
            if n == 0 {
                Ok(InsertOutcome::Duplicate)
            } else {
                Ok(InsertOutcome::Inserted(conn.last_insert_rowid()))
            }
        })
    }

    /// All log rows ordered by rowid. Used by rebuild and by tests.
    /// Refer to specs.md Section 6.1, rule 4.
    pub fn list_messages(&self, chat_id: &str) -> Result<Vec<MessageRow>> {
        self.with_conn(chat_id, |conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages ORDER BY id"
            ))?;
            let rows = stmt
                .query_map([], message_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// All log rows with `id > after_id`, ordered by `id`. The digest
    /// pipeline reads the range `(last_digest_boundary_msg_id, tail]` with
    /// this method (specs.md Section 10.1).
    pub fn list_messages_after(&self, chat_id: &str, after_id: i64) -> Result<Vec<MessageRow>> {
        self.with_conn(chat_id, |conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages WHERE id > ?1 ORDER BY id"
            ))?;
            let rows = stmt
                .query_map(rusqlite::params![after_id], message_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    // --- Session state KV (specs.md Sections 5.2 and 6.1) ---
    //
    // The state table is a generic key-value store. The design uses these
    // keys (the actor in tamako-core writes them; this crate does not
    // hard-code accessors):
    //   last_digest_boundary_msg_id, prev_digest_boundary_msg_id,
    //   last_digest_at, muted_flag, consecutive_bot_msgs,
    //   wake_msgs_since_wake, wake_last_wake_at, wake_current_interval_ms.
    // Counter keys of specs.md Section 12 (used with increment_counter):
    //   wakes_total, participations_total, injection_wakes_total,
    //   digest_failures_total, dead_letters_total.

    pub fn get_state(&self, chat_id: &str, key: &str) -> Result<Option<String>> {
        self.with_conn(chat_id, |conn| {
            let value = conn
                .query_row(
                    "SELECT value FROM state WHERE key = ?1",
                    rusqlite::params![key],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(value)
        })
    }

    pub fn set_state(&self, chat_id: &str, key: &str, value: &str) -> Result<()> {
        self.with_conn(chat_id, |conn| upsert_state(conn, key, value))
    }

    /// Transactional multi-key write. The actor persists the session state
    /// after every mutation (specs.md Section 6.1, rule 4).
    pub fn set_state_many(&self, chat_id: &str, pairs: &[(String, String)]) -> Result<()> {
        self.with_conn(chat_id, |conn| {
            let tx = conn.transaction()?;
            for (key, value) in pairs {
                upsert_state(&tx, key, value)?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    /// Atomic increment of a numeric counter key. Returns the new value.
    /// Creates the key at 0 + delta when missing. Used for the counters of
    /// specs.md Section 12 (the mechanism only; counters can start unused).
    pub fn increment_counter(&self, chat_id: &str, key: &str, delta: i64) -> Result<i64> {
        self.with_conn(chat_id, |conn| {
            let tx = conn.transaction()?;
            let current: Option<String> = tx
                .query_row(
                    "SELECT value FROM state WHERE key = ?1",
                    rusqlite::params![key],
                    |row| row.get(0),
                )
                .optional()?;
            let base = match current {
                None => 0,
                Some(value) => value.parse::<i64>().map_err(|_| StoreError::InvalidValue {
                    key: key.to_string(),
                    value: value.clone(),
                })?,
            };
            // checked_add: an overflow is reported, it does not panic.
            let new_value = base
                .checked_add(delta)
                .ok_or_else(|| StoreError::InvalidValue {
                    key: key.to_string(),
                    value: format!("{base} (overflow on +{delta})"),
                })?;
            upsert_state(&tx, key, &new_value.to_string())?;
            tx.commit()?;
            Ok(new_value)
        })
    }

    pub fn load_all_state(&self, chat_id: &str) -> Result<HashMap<String, String>> {
        self.with_conn(chat_id, |conn| {
            let mut stmt = conn.prepare("SELECT key, value FROM state")?;
            let pairs = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<HashMap<_, _>, _>>()?;
            Ok(pairs)
        })
    }

    // --- injected_memories (specs.md Sections 5.2 and 9.3; anti-defer list) ---

    /// Records one injected recall item. `content` holds the rendered
    /// injection text for the context rebuild (specs.md Section 7.1).
    /// Returns the new rowid.
    pub fn insert_injected_memory(
        &self,
        chat_id: &str,
        edge_id: &str,
        injection_position: i64,
        range_tag: &str,
        content: &str,
    ) -> Result<i64> {
        self.with_conn(chat_id, |conn| {
            conn.execute(
                "INSERT INTO injected_memories
                    (edge_id, injection_position, range_tag, content, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    edge_id,
                    injection_position,
                    range_tag,
                    content,
                    schema::now_rfc3339()?,
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    pub fn list_injected_memories(&self, chat_id: &str) -> Result<Vec<InjectedMemoryRow>> {
        self.with_conn(chat_id, |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, edge_id, injection_position, range_tag, content
                 FROM injected_memories ORDER BY id",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(InjectedMemoryRow {
                        id: row.get("id")?,
                        edge_id: row.get("edge_id")?,
                        injection_position: row.get("injection_position")?,
                        range_tag: row.get("range_tag")?,
                        content: row.get("content")?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// Deletes all rows with `injection_position <= msg_id`. Returns the
    /// number of deleted rows. Refer to specs.md Section 10.2 step 4: at
    /// digest time the dedup set is pruned together with the Rule C3
    /// context removal. Rows at or below the previous digest boundary go.
    pub fn delete_injected_memories_up_to(&self, chat_id: &str, msg_id: i64) -> Result<usize> {
        self.with_conn(chat_id, |conn| {
            let deleted = conn.execute(
                "DELETE FROM injected_memories WHERE injection_position <= ?1",
                rusqlite::params![msg_id],
            )?;
            Ok(deleted)
        })
    }

    // --- dead_letter (specs.md Section 10.3) ---

    /// Writes a failed digest batch to the dead-letter table. A failed
    /// batch never blocks later batches. Returns the new rowid.
    pub fn insert_dead_letter(
        &self,
        chat_id: &str,
        batch_id: &str,
        batch_skeleton: &str,
        error: &str,
    ) -> Result<i64> {
        self.with_conn(chat_id, |conn| {
            conn.execute(
                "INSERT INTO dead_letter
                    (batch_id, batch_skeleton, error, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![batch_id, batch_skeleton, error, schema::now_rfc3339()?,],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    pub fn list_dead_letters(&self, chat_id: &str) -> Result<Vec<DeadLetterRow>> {
        self.with_conn(chat_id, |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, batch_id, batch_skeleton, error, created_at
                 FROM dead_letter ORDER BY id",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    let created_at: String = row.get("created_at")?;
                    Ok(DeadLetterRow {
                        id: row.get("id")?,
                        batch_id: row.get("batch_id")?,
                        batch_skeleton: row.get("batch_skeleton")?,
                        error: row.get("error")?,
                        created_at: schema::parse_rfc3339(&created_at)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// Opens the group connection when it is not cached, then runs `f`
    /// on it.
    fn with_conn<T>(
        &self,
        chat_id: &str,
        f: impl FnOnce(&mut Connection) -> Result<T>,
    ) -> Result<T> {
        if !self.lock().contains_key(chat_id) {
            // open_group is idempotent. A concurrent open is harmless.
            self.open_group(chat_id)?;
        }
        let mut guard = self.lock();
        let conn = guard
            .get_mut(chat_id)
            .ok_or_else(|| StoreError::InvalidChatId(chat_id.to_string()))?;
        f(conn)
    }

    /// Locks the connection map. A poisoned mutex is recovered; the
    /// connections inside stay valid.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Connection>> {
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

// The column list of the raw-log SELECT queries. `list_messages` and
// `list_messages_after` share it.
const MESSAGE_COLUMNS: &str = "id, platform_msg_id, direction, event_type, timestamp,
        sender_id, sender_display_name, text,
        reply_to_platform_msg_id, mentions_bot, is_reply_to_bot";

/// Maps one row of a raw-log SELECT to a `MessageRow`.
fn message_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRow> {
    let direction: String = row.get("direction")?;
    let event_type: String = row.get("event_type")?;
    let timestamp: String = row.get("timestamp")?;
    Ok(MessageRow {
        id: row.get("id")?,
        platform_msg_id: row.get("platform_msg_id")?,
        direction: Direction::from_str(&direction)?,
        event_type: EventType::from_str(&event_type)?,
        timestamp: schema::parse_rfc3339(&timestamp)?,
        sender_id: row.get("sender_id")?,
        sender_display_name: row.get("sender_display_name")?,
        text: row.get("text")?,
        reply_to_platform_msg_id: row.get("reply_to_platform_msg_id")?,
        mentions_bot: row.get("mentions_bot")?,
        is_reply_to_bot: row.get("is_reply_to_bot")?,
    })
}

/// Rejects chat_id values that could escape the per-group directory.
/// Rule P5: one group's data never crosses into another group.
fn validate_chat_id(chat_id: &str) -> Result<()> {
    if chat_id.is_empty()
        || chat_id.contains('/')
        || chat_id.contains('\\')
        || chat_id.contains('\0')
        || chat_id.contains("..")
    {
        return Err(StoreError::InvalidChatId(chat_id.to_string()));
    }
    Ok(())
}

/// Inserts or replaces one state row and stamps updated_at.
fn upsert_state(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO state (key, value, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at",
        rusqlite::params![key, value, schema::now_rfc3339()?],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::new(dir.path().to_path_buf());
        (dir, store)
    }

    fn sample_message() -> NewMessage {
        NewMessage {
            platform_msg_id: "m1".to_string(),
            direction: Direction::Inbound,
            event_type: EventType::Message,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp"),
            sender_id: "u1".to_string(),
            sender_display_name: "Alice".to_string(),
            text: "hello".to_string(),
            reply_to_platform_msg_id: Some("m0".to_string()),
            mentions_bot: true,
            is_reply_to_bot: false,
        }
    }

    #[test]
    fn migrations_run_on_first_open_and_are_idempotent_on_reopen() {
        let (dir, store) = temp_store();
        store.open_group("c1").expect("first open");
        // A second open on the same Store is a no-op.
        store.open_group("c1").expect("second open");
        // A reopen through a new Store instance must not fail.
        let store2 = Store::new(dir.path().to_path_buf());
        store2.open_group("c1").expect("reopen");

        let conn = Connection::open(dir.path().join("c1").join("store.db")).expect("open db");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .expect("count migrations");
        assert_eq!(count, 2);
        let versions: Vec<u32> = {
            let mut stmt = conn
                .prepare("SELECT version FROM schema_migrations ORDER BY version")
                .expect("prepare versions");
            stmt.query_map([], |row| row.get(0))
                .expect("query versions")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect versions")
        };
        assert_eq!(versions, vec![1, 2]);
    }

    #[test]
    fn insert_message_is_idempotent_and_round_trips() {
        let (_dir, store) = temp_store();
        let msg = sample_message();

        let first = store.insert_message("c1", &msg).expect("first insert");
        let second = store.insert_message("c1", &msg).expect("duplicate insert");

        let inserted_id = match first {
            InsertOutcome::Inserted(id) => id,
            other => panic!("expected Inserted, got {other:?}"),
        };
        assert_eq!(second, InsertOutcome::Duplicate);

        let rows = store.list_messages("c1").expect("list");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.id, inserted_id);
        assert_eq!(row.platform_msg_id, msg.platform_msg_id);
        assert_eq!(row.direction, msg.direction);
        assert_eq!(row.event_type, msg.event_type);
        assert_eq!(row.timestamp, msg.timestamp);
        assert_eq!(row.sender_id, msg.sender_id);
        assert_eq!(row.sender_display_name, msg.sender_display_name);
        assert_eq!(row.text, msg.text);
        assert_eq!(row.reply_to_platform_msg_id, msg.reply_to_platform_msg_id);
        assert_eq!(row.mentions_bot, msg.mentions_bot);
        assert_eq!(row.is_reply_to_bot, msg.is_reply_to_bot);
    }

    #[test]
    fn two_edits_at_different_timestamps_both_land_as_rows() {
        let (_dir, store) = temp_store();
        let base = sample_message();
        let edit1 = NewMessage {
            event_type: EventType::Edit,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp"),
            text: "hello (edited)".to_string(),
            ..base.clone()
        };
        let edit2 = NewMessage {
            event_type: EventType::Edit,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_200).expect("valid timestamp"),
            text: "hello (edited again)".to_string(),
            ..base.clone()
        };

        assert!(matches!(
            store.insert_message("c1", &base).expect("insert message"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store.insert_message("c1", &edit1).expect("insert edit 1"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store.insert_message("c1", &edit2).expect("insert edit 2"),
            InsertOutcome::Inserted(_)
        ));

        let rows = store.list_messages("c1").expect("list");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].event_type, EventType::Edit);
        assert_eq!(rows[2].event_type, EventType::Edit);
        assert_eq!(rows[1].text, "hello (edited)");
        assert_eq!(rows[2].text, "hello (edited again)");
    }

    #[test]
    fn list_messages_after_returns_the_tail_range_in_order() {
        // The digest pipeline reads the range (boundary, tail] with this
        // method (specs.md Section 10.1).
        let (_dir, store) = temp_store();
        let base = sample_message();
        let mut ids = Vec::new();
        for index in 1..=3_i64 {
            let msg = NewMessage {
                platform_msg_id: format!("m{index}"),
                timestamp: base.timestamp + time::Duration::seconds(index),
                text: format!("text {index}"),
                ..base.clone()
            };
            match store.insert_message("c1", &msg).expect("insert") {
                InsertOutcome::Inserted(id) => ids.push(id),
                other => panic!("expected Inserted, got {other:?}"),
            }
        }

        // The full log from a zero boundary.
        let all = store.list_messages_after("c1", 0).expect("list all");
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].text, "text 1");
        assert_eq!(all[2].text, "text 3");

        // The range (first_row_id, tail] holds the remaining two rows, in
        // id order.
        let tail = store.list_messages_after("c1", ids[0]).expect("list tail");
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].id, ids[1]);
        assert_eq!(tail[1].id, ids[2]);
        assert_eq!(tail[0].text, "text 2");
        assert_eq!(tail[1].text, "text 3");

        // An empty tail returns an empty vec.
        let empty = store
            .list_messages_after("c1", ids[2])
            .expect("list empty tail");
        assert!(empty.is_empty());
    }

    #[test]
    fn state_round_trip_set_overwrite_and_missing() {
        let (_dir, store) = temp_store();
        assert_eq!(store.get_state("c1", "muted_flag").expect("get"), None);

        store.set_state("c1", "muted_flag", "0").expect("set");
        assert_eq!(
            store.get_state("c1", "muted_flag").expect("get"),
            Some("0".to_string())
        );

        store.set_state("c1", "muted_flag", "1").expect("overwrite");
        assert_eq!(
            store.get_state("c1", "muted_flag").expect("get"),
            Some("1".to_string())
        );
    }

    #[test]
    fn set_state_many_writes_all_keys_atomically() {
        let (_dir, store) = temp_store();
        let pairs = vec![
            ("consecutive_bot_msgs".to_string(), "2".to_string()),
            ("wake_msgs_since_wake".to_string(), "7".to_string()),
            ("wake_current_interval_ms".to_string(), "60000".to_string()),
        ];
        store.set_state_many("c1", &pairs).expect("set many");
        for (key, value) in &pairs {
            assert_eq!(
                store.get_state("c1", key).expect("get"),
                Some(value.clone())
            );
        }
    }

    #[test]
    fn increment_counter_from_missing_and_existing_values() {
        let (_dir, store) = temp_store();
        // Missing key starts at 0 + delta.
        assert_eq!(
            store
                .increment_counter("c1", "wakes_total", 3)
                .expect("increment missing"),
            3
        );
        assert_eq!(
            store
                .increment_counter("c1", "wakes_total", 2)
                .expect("increment existing"),
            5
        );
        // Negative delta works.
        assert_eq!(
            store
                .increment_counter("c1", "wakes_total", -1)
                .expect("decrement"),
            4
        );
    }

    #[test]
    fn increment_counter_on_non_numeric_value_returns_invalid_value() {
        let (_dir, store) = temp_store();
        store
            .set_state("c1", "participations_total", "not-a-number")
            .expect("set");
        let err = store
            .increment_counter("c1", "participations_total", 1)
            .expect_err("must fail");
        match err {
            StoreError::InvalidValue { key, value } => {
                assert_eq!(key, "participations_total");
                assert_eq!(value, "not-a-number");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn load_all_state_returns_all_pairs() {
        let (_dir, store) = temp_store();
        store
            .set_state_many(
                "c1",
                &[
                    ("muted_flag".to_string(), "0".to_string()),
                    (
                        "wake_last_wake_at".to_string(),
                        "2026-01-01T00:00:00Z".to_string(),
                    ),
                ],
            )
            .expect("set many");
        store
            .increment_counter("c1", "digest_failures_total", 1)
            .expect("counter");

        let all = store.load_all_state("c1").expect("load all");
        assert_eq!(all.len(), 3);
        assert_eq!(all.get("muted_flag"), Some(&"0".to_string()));
        assert_eq!(
            all.get("wake_last_wake_at"),
            Some(&"2026-01-01T00:00:00Z".to_string())
        );
        assert_eq!(all.get("digest_failures_total"), Some(&"1".to_string()));
    }

    #[test]
    fn open_group_rejects_invalid_chat_ids() {
        let (_dir, store) = temp_store();
        for bad in ["../evil", "a/b", "a\\b", "a\0b", "", ".."] {
            match store.open_group(bad) {
                Err(StoreError::InvalidChatId(_)) => {}
                other => panic!("expected InvalidChatId for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn injected_memories_insert_and_list_round_trip() {
        let (_dir, store) = temp_store();
        let id1 = store
            .insert_injected_memory("c1", "edge-1", 42, "m1-m10", "Alice likes tea")
            .expect("insert 1");
        let id2 = store
            .insert_injected_memory("c1", "edge-2", 99, "m11-m20", "Bob plays go")
            .expect("insert 2");
        assert!(id2 > id1);

        let rows = store.list_injected_memories("c1").expect("list");
        assert_eq!(
            rows,
            vec![
                InjectedMemoryRow {
                    id: id1,
                    edge_id: "edge-1".to_string(),
                    injection_position: 42,
                    range_tag: "m1-m10".to_string(),
                    content: "Alice likes tea".to_string(),
                },
                InjectedMemoryRow {
                    id: id2,
                    edge_id: "edge-2".to_string(),
                    injection_position: 99,
                    range_tag: "m11-m20".to_string(),
                    content: "Bob plays go".to_string(),
                },
            ]
        );
    }

    #[test]
    fn injected_memory_content_round_trips_bit_identically() {
        // The content column feeds the bit-identical context rebuild of
        // specs.md Section 7.1 (Rule P1). Unicode and newlines must survive
        // the round trip.
        let (_dir, store) = temp_store();
        let content = "- Alice: likes お茶\n- Bob: \"quoted\" text\n";
        store
            .insert_injected_memory("c1", "edge-1", 10, "m1-m10", content)
            .expect("insert");

        let rows = store.list_injected_memories("c1").expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, content);
    }

    #[test]
    fn delete_injected_memories_up_to_removes_rows_at_or_below_the_boundary() {
        // specs.md Section 10.2 step 4 / Rule C3: at digest time the dedup
        // set is pruned. Rows at or below the previous boundary go; rows
        // above stay.
        let (_dir, store) = temp_store();
        store
            .insert_injected_memory("c1", "edge-1", 10, "m1-m10", "a")
            .expect("insert 1");
        store
            .insert_injected_memory("c1", "edge-2", 20, "m11-m20", "b")
            .expect("insert 2");
        store
            .insert_injected_memory("c1", "edge-3", 30, "m21-m30", "c")
            .expect("insert 3");

        // The boundary is inclusive: rows at 10 and 20 go, the row at 30
        // stays.
        let deleted = store
            .delete_injected_memories_up_to("c1", 20)
            .expect("delete up to 20");
        assert_eq!(deleted, 2);

        let rows = store.list_injected_memories("c1").expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].edge_id, "edge-3");
        assert_eq!(rows[0].injection_position, 30);
        assert_eq!(rows[0].content, "c");

        // Deleting the same boundary again deletes nothing.
        let deleted_again = store
            .delete_injected_memories_up_to("c1", 20)
            .expect("delete again");
        assert_eq!(deleted_again, 0);
    }

    #[test]
    fn dead_letter_insert_and_list_round_trip() {
        let (_dir, store) = temp_store();
        let id = store
            .insert_dead_letter("c1", "batch-1", "{\"items\":[]}", "boom")
            .expect("insert");

        let rows = store.list_dead_letters("c1").expect("list");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.id, id);
        assert_eq!(row.batch_id, "batch-1");
        assert_eq!(row.batch_skeleton, "{\"items\":[]}");
        assert_eq!(row.error, "boom");
        // The timestamp is written by the store. It must parse back.
        assert!(row.created_at.unix_timestamp() > 0);
    }
}
