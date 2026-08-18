//! Store type and row types for store.db. Refer to specs.md Section 5.
//!
//! Rule P5: one SQLite file per group at `{data_root}/{chat_id}/store.db`.
//! All store APIs take a `chat_id`. One group's data never crosses into
//! another group.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension};
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
    /// The sender's username, when the platform provides one. `None` for
    /// senders without a username and for rows written before migration v4.
    pub sender_username: Option<String>,
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
    /// The sender's username, when the platform provides one. `None` for
    /// senders without a username and for rows written before migration v4.
    pub sender_username: Option<String>,
    pub text: String,
    pub reply_to_platform_msg_id: Option<String>,
    pub mentions_bot: bool,
    pub is_reply_to_bot: bool,
}

/// Resolved reply target of the raw message log. Refer to
/// `Store::find_reply_target`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyTargetRow {
    /// Row id of the ORIGINAL logged row for the platform message id.
    pub row_id: i64,
    /// Display name of the target sender, fixed at intake.
    pub display_name: String,
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

/// A row of the `context_summaries` table.
///
/// One row summarizes one digested chunk of the raw log: the range
/// `(first_msg_id, last_msg_id]` that Rule C3 removed from the live
/// context at digest completion. An LLM-written summary is not derivable
/// from persisted state (Rule P1), so `content` is persisted at creation
/// time. The context rebuild keeps the TWO newest summaries.
///
/// Retention: rows that rotate out of the keep-two window are NOT
/// pruned. They stay in the table for forensics. The table grows one
/// small row per digest. This is deliberate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSummaryRow {
    pub id: i64,
    /// Id of the raw-log row just before the summarized chunk. The range
    /// is exclusive of this boundary: rows with `id > first_msg_id` were
    /// digested.
    pub first_msg_id: i64,
    /// Id of the last raw-log row of the summarized chunk, inclusive.
    pub last_msg_id: i64,
    /// The LLM-written summary text. Persisted at creation time because
    /// it is not derivable from the raw log (Rule P1).
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

/// A read-only status snapshot of one group (specs.md Sections 10.3 and
/// 12). The operator-facing --status mode renders this.
#[derive(Debug)]
pub struct GroupStatus {
    /// The state-table keys of the session (present ones only):
    /// last_digest_boundary_msg_id, prev_digest_boundary_msg_id,
    /// muted_flag, consecutive_bot_msgs, plus the counters of
    /// specs.md Section 12 (wakes_total, participations_total,
    /// injection_wakes_total, digest_failures_total, dead_letters_total).
    pub state: HashMap<String, String>,
    /// The number of rows of the dead_letter table.
    pub dead_letter_count: u64,
    /// Newest first, up to the requested limit.
    pub recent_dead_letters: Vec<DeadLetterRow>,
}

/// A new reaction event row. Refer to specs.md Section 5.2.
#[derive(Debug, Clone)]
pub struct NewReaction {
    pub platform_msg_id: String,
    /// `None` for aggregated count updates with no reactor identity.
    pub reactor_user_id: Option<String>,
    pub anonymous: bool,
    pub aggregated: bool,
    /// JSON array of emoji strings before the event.
    pub old_emojis: Vec<String>,
    /// JSON array of emoji strings after the event.
    pub new_emojis: Vec<String>,
    pub timestamp: OffsetDateTime,
}

/// A row of the `reactions` table. Refer to specs.md Section 5.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactionRow {
    pub id: i64,
    pub platform_msg_id: String,
    /// `None` for aggregated count updates with no reactor identity.
    pub reactor_user_id: Option<String>,
    pub anonymous: bool,
    pub aggregated: bool,
    pub old_emojis: Vec<String>,
    pub new_emojis: Vec<String>,
    pub timestamp: OffsetDateTime,
}

/// Embedding dimension pinned by current-state.md decision 66. The vec0
/// virtual-table dimension is fixed at table creation (migration v7).
pub const EMBEDDING_DIM: usize = 4096;

/// A claimed row of the `pending_embeddings` queue (migration v7,
/// decision 66).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingEmbedding {
    pub id: i64,
    pub node_id: String,
    pub content_hash: String,
    pub attempts: u32,
}

/// A row of the `merge_audit` table (migration v9, decision 74,
/// specs.md Section 5.2): one append-only record per merge-tool action.
///
/// All three verdicts are audited: 'same' (the pair merged), 'related'
/// (the pair was linked via `also_known_as`), and 'different' (the pair
/// was skipped). Only a 'same' merge carries a `snapshot` (JSON: the
/// loser node and its original edges, plus the created edge
/// identifiers) — the rollback source of graph-spec Section 7.7;
/// non-merge verdicts store None.
///
/// On `insert_merge_audit` the `id`, `rolled_back`, and `created_at`
/// fields are IGNORED: the id is the autoincrement rowid, rolled_back
/// starts at the database default 0 (flip it with
/// `mark_merge_rolled_back`), and created_at is stamped from the Rust
/// side (the house RFC 3339 TEXT idiom).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeAuditRow {
    pub id: i64,
    /// Node id of the merged-away node. For non-merge verdicts: the
    /// candidate that WOULD have been merged away.
    pub loser_id: String,
    /// Node id of the surviving node.
    pub survivor_id: String,
    /// Kind of the loser (Person, Concept, ...); kind-compatible pairs
    /// only (graph-spec Section 7.7).
    pub loser_kind: String,
    pub loser_name: String,
    pub loser_description: Option<String>,
    /// The three-way confirmation verdict: 'same', 'related', or
    /// 'different'. Enforced by a CHECK constraint.
    pub verdict: String,
    /// The LLM's or operator's justification of the verdict.
    pub reason: String,
    /// Who confirmed: `llm:<model>` or `operator` (decision 74).
    pub confirmed_by: String,
    /// Edges re-pointed from the loser to the survivor.
    pub edges_moved: u32,
    /// Loser↔survivor edges that became self-loops and were dropped.
    pub self_loops_dropped: u32,
    /// Re-points skipped because the survivor already had an equivalent
    /// edge (same predicate, other endpoint, description text).
    pub edges_deduped: u32,
    /// The rollback snapshot. Some only for a 'same' merge.
    pub snapshot: Option<String>,
    /// Flipped to true by `mark_merge_rolled_back` when the merge was
    /// rolled back.
    pub rolled_back: bool,
    pub created_at: OffsetDateTime,
}

/// Registers the sqlite-vec (vec0) extension on a connection. Migration
/// v7's node_embeddings table and every vec0 query need it; registration
/// is PER-CONNECTION and never persisted, so every open path
/// (Store::open_group, the read-only status path) calls this
/// unconditionally.
///
/// # Safety contained here
/// `sqlite-vec` 0.1.9 exports only the raw C entry point
/// `sqlite3_vec_init`, declared (incorrectly, with no parameters) as a
/// Rust extern. The real C signature (compiled with `SQLITE_CORE`) is
/// the standard 3-argument SQLite extension entry point, so we
/// transmute the symbol address to the correct fn pointer type and call
/// it per connection. Sound on the SysV/Win64 ABIs and the same pattern
/// the crate's own test uses for `sqlite3_auto_extension`.
/// (`rusqlite::ffi` re-exports `libsqlite3-sys`; `Connection::handle()`
/// is unconditionally available.)
pub fn register_sqlite_vec(conn: &Connection) -> Result<()> {
    type VecInit = unsafe extern "C" fn(
        db: *mut rusqlite::ffi::sqlite3,
        pz_err_msg: *mut *mut std::ffi::c_char,
        p_api: *const rusqlite::ffi::sqlite3_api_routines,
    ) -> std::ffi::c_int;
    let init: VecInit = unsafe { std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ()) };
    let rc = unsafe { init(conn.handle(), std::ptr::null_mut(), std::ptr::null()) };
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rc),
            Some("sqlite3_vec_init failed".to_string()),
        )
        .into());
    }
    Ok(())
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
        // Registration is per-connection and never persisted; the vec0
        // module must resolve before migration v7 (and any later reopen)
        // touches node_embeddings.
        register_sqlite_vec(&conn)?;
        schema::run_migrations(&mut conn)?;
        self.lock().insert(chat_id.to_string(), conn);
        Ok(())
    }

    /// Rule P1: append to the raw log. Idempotent: UNIQUE(platform_msg_id,
    /// direction, event_type, timestamp, text) + INSERT OR IGNORE. The
    /// `text` column in the key (migration v6, decision 65) keeps
    /// same-second edits with different text distinct; byte-identical
    /// redelivery still dedups.
    pub fn insert_message(&self, chat_id: &str, msg: &NewMessage) -> Result<InsertOutcome> {
        self.with_conn(chat_id, |conn| {
            let timestamp = schema::format_rfc3339(msg.timestamp)?;
            let n = conn.execute(
                "INSERT OR IGNORE INTO messages (
                    platform_msg_id, direction, event_type, timestamp,
                    sender_id, sender_display_name, sender_username, text,
                    reply_to_platform_msg_id, mentions_bot, is_reply_to_bot
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    msg.platform_msg_id,
                    msg.direction.as_str(),
                    msg.event_type.as_str(),
                    timestamp,
                    msg.sender_id,
                    msg.sender_display_name,
                    msg.sender_username,
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

    /// All log rows with `after_msg_id < id <= up_to_msg_id_inclusive`,
    /// ordered by `id`. The segmented summarizer reads the raw-log range
    /// of the chunk it replaces with this method.
    pub fn list_messages_in_range(
        &self,
        chat_id: &str,
        after_msg_id: i64,
        up_to_msg_id_inclusive: i64,
    ) -> Result<Vec<MessageRow>> {
        self.with_conn(chat_id, |conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages WHERE id > ?1 AND id <= ?2 ORDER BY id"
            ))?;
            let rows = stmt
                .query_map(
                    rusqlite::params![after_msg_id, up_to_msg_id_inclusive],
                    message_row,
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// Counts the INBOUND raw-log rows with `id > after_id`. The M4
    /// recency re-check (specs.md Section 6.2) uses it: newer human
    /// messages after the target decide whether a generated reply is
    /// stale.
    pub fn count_inbound_after(&self, chat_id: &str, after_id: i64) -> Result<u32> {
        self.with_conn(chat_id, |conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM messages WHERE id > ?1 AND direction = 'inbound'",
                rusqlite::params![after_id],
                |row| row.get(0),
            )?;
            // COUNT(*) is never negative; the conversion saturates on the
            // theoretical overflow.
            Ok(u32::try_from(count).unwrap_or(u32::MAX))
        })
    }
    /// Returns (sender_id, sender_display_name) of the message with the
    /// given platform id, newest row first when duplicates exist.
    /// Read-path support of the M5 recall worker (Section 8.1 step 1 of
    /// the database spec: replies resolve to Person entries).
    pub fn find_sender_by_platform_msg_id(
        &self,
        chat_id: &str,
        platform_msg_id: &str,
    ) -> Result<Option<(String, String)>> {
        self.with_conn(chat_id, |conn| {
            let row = conn
                .query_row(
                    "SELECT sender_id, sender_display_name FROM messages
                     WHERE platform_msg_id = ?1 ORDER BY id DESC LIMIT 1",
                    rusqlite::params![platform_msg_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            Ok(row)
        })
    }

    /// Resolves a reply target to the ORIGINAL logged row (Rule P1:
    /// deterministic from the append-only raw log). Edit rows share
    /// `platform_msg_id` with their original, so MIN(id) pins the
    /// original; its display name is fixed at intake. Returns None when
    /// the target is absent from the log (e.g. it predates the bot
    /// joining).
    ///
    /// No direction filter: in practice only inbound rows carry real
    /// platform ids. Outbound rows use synthetic `bot-out:{nanos}` ids
    /// that never collide with real ones.
    pub fn find_reply_target(
        &self,
        chat_id: &str,
        platform_msg_id: &str,
    ) -> Result<Option<ReplyTargetRow>> {
        self.with_conn(chat_id, |conn| {
            let row = conn
                .query_row(
                    "SELECT id, sender_display_name FROM messages
                     WHERE platform_msg_id = ?1 ORDER BY id ASC LIMIT 1",
                    rusqlite::params![platform_msg_id],
                    |row| {
                        Ok(ReplyTargetRow {
                            row_id: row.get("id")?,
                            display_name: row.get("sender_display_name")?,
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
    }

    /// The NEWEST raw-log row (highest `id`) for one platform message id,
    /// any event type, or None when the platform id is absent from the
    /// log. Edit rows share `platform_msg_id` with their original
    /// (specs.md Section 15), so the newest row is the latest edit when
    /// edits exist, else the original message.
    ///
    /// No direction filter: outbound rows use synthetic `bot-out:{nanos}`
    /// ids that never collide with real platform ids (see
    /// find_reply_target).
    pub fn find_latest_message_by_platform_msg_id(
        &self,
        chat_id: &str,
        platform_msg_id: &str,
    ) -> Result<Option<MessageRow>> {
        self.with_conn(chat_id, |conn| {
            let row = conn
                .query_row(
                    &format!(
                        "SELECT {MESSAGE_COLUMNS} FROM messages
                         WHERE platform_msg_id = ?1 ORDER BY id DESC LIMIT 1"
                    ),
                    rusqlite::params![platform_msg_id],
                    message_row,
                )
                .optional()?;
            Ok(row)
        })
    }

    //
    // --- Session state KV (specs.md Sections 5.2 and 6.1) ---
    //
    // The state table is a generic key-value store. The design uses these
    // keys (the actor in tamako-core writes them; this crate does not
    // hard-code accessors):
    //   last_digest_boundary_msg_id, prev_digest_boundary_msg_id,
    //   last_digest_at, muted_flag, consecutive_bot_msgs,
    //   wake_msgs_since_wake, wake_last_wake_at, wake_current_interval_ms,
    //   wake_last_row_id (M4).
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
                .query_map([], dead_letter_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    // --- reactions (specs.md Section 5.2) ---

    /// specs.md Section 5.2: one row per reaction event on a group
    /// message. Reaction data is not recoverable later, so collection
    /// starts at intake time in Phase 1. Idempotent: INSERT OR IGNORE
    /// over the reactions_dedup index (AGENT.md Section 6.2). A
    /// reconnect redelivers the same reaction update; the redelivery
    /// lands as a Duplicate.
    pub fn insert_reaction(&self, chat_id: &str, reaction: &NewReaction) -> Result<InsertOutcome> {
        self.with_conn(chat_id, |conn| {
            let timestamp = schema::format_rfc3339(reaction.timestamp)?;
            let old_emojis = serialize_emojis(&reaction.old_emojis)?;
            let new_emojis = serialize_emojis(&reaction.new_emojis)?;
            let n = conn.execute(
                "INSERT OR IGNORE INTO reactions (
                    platform_msg_id, reactor_user_id, anonymous, aggregated,
                    old_emojis, new_emojis, timestamp
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    reaction.platform_msg_id,
                    reaction.reactor_user_id,
                    reaction.anonymous,
                    reaction.aggregated,
                    old_emojis,
                    new_emojis,
                    timestamp,
                ],
            )?;
            if n == 0 {
                Ok(InsertOutcome::Duplicate)
            } else {
                Ok(InsertOutcome::Inserted(conn.last_insert_rowid()))
            }
        })
    }

    /// All reaction rows ordered by rowid. Used by tests and the Phase 2
    /// warmup backoff. Refer to specs.md Section 5.2.
    pub fn list_reactions(&self, chat_id: &str) -> Result<Vec<ReactionRow>> {
        self.with_conn(chat_id, |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, platform_msg_id, reactor_user_id, anonymous, aggregated,
                        old_emojis, new_emojis, timestamp
                 FROM reactions ORDER BY id",
            )?;
            let rows = stmt
                .query_map([], reaction_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    // --- context_summaries (specs.md Section 10; segmented summarization) ---
    //
    // One row per digested chunk. The raw-log range (first_msg_id,
    // last_msg_id] of the chunk is the natural dedup key. Retention
    // policy: rotated-out summaries are NOT pruned; see ContextSummaryRow.

    /// Finds the summary of the digested range
    /// `(first_msg_id, last_msg_id]`. Check-before-call support (Rule P1
    /// replay idempotency): a re-run of a digest-completion handler finds
    /// the existing row and skips the LLM call.
    pub fn find_context_summary(
        &self,
        chat_id: &str,
        first_msg_id: i64,
        last_msg_id: i64,
    ) -> Result<Option<ContextSummaryRow>> {
        self.with_conn(chat_id, |conn| {
            let row = conn
                .query_row(
                    "SELECT id, first_msg_id, last_msg_id, content
                     FROM context_summaries
                     WHERE first_msg_id = ?1 AND last_msg_id = ?2",
                    rusqlite::params![first_msg_id, last_msg_id],
                    context_summary_row,
                )
                .optional()?;
            Ok(row)
        })
    }

    /// Records the summary of one digested chunk. Idempotent: INSERT OR
    /// IGNORE over the natural key (AGENT.md Section 6.2). Returns the
    /// row id: the new one on insert, the existing one on a replay of
    /// the same range.
    pub fn insert_context_summary(
        &self,
        chat_id: &str,
        first_msg_id: i64,
        last_msg_id: i64,
        content: &str,
    ) -> Result<i64> {
        self.with_conn(chat_id, |conn| {
            let inserted = conn.execute(
                "INSERT OR IGNORE INTO context_summaries
                    (first_msg_id, last_msg_id, content, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![first_msg_id, last_msg_id, content, schema::now_rfc3339()?,],
            )?;
            if inserted == 1 {
                Ok(conn.last_insert_rowid())
            } else {
                // The range already has a summary. Return the existing row
                // id so the caller is independent of which path won.
                Ok(conn.query_row(
                    "SELECT id FROM context_summaries
                     WHERE first_msg_id = ?1 AND last_msg_id = ?2",
                    rusqlite::params![first_msg_id, last_msg_id],
                    |row| row.get(0),
                )?)
            }
        })
    }

    /// The N newest summary rows by id, returned OLDEST FIRST. The
    /// keep-two retention source for the context rebuild: the caller
    /// places the rows after the preamble in this order.
    pub fn list_newest_context_summaries(
        &self,
        chat_id: &str,
        limit: u32,
    ) -> Result<Vec<ContextSummaryRow>> {
        self.with_conn(chat_id, |conn| {
            // u32 to i64 is lossless.
            let limit = i64::from(limit);
            let mut stmt = conn.prepare(
                "SELECT id, first_msg_id, last_msg_id, content
                 FROM context_summaries ORDER BY id DESC LIMIT ?1",
            )?;
            let mut rows = stmt
                .query_map(rusqlite::params![limit], context_summary_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            // The query returns newest first. Reverse for context
            // placement: oldest first, newest last.
            rows.reverse();
            Ok(rows)
        })
    }

    /// Enqueues (node_id, content_hash) pairs into the embedding queue
    /// (decision 66). INSERT OR IGNORE dedups through the
    /// pending_embeddings_dedup UNIQUE index; returns the number of
    /// rows actually inserted. A pair already queued (or already done)
    /// inserts nothing; a changed hash for a known node inserts a new
    /// row.
    pub fn enqueue_embeddings(&self, items: &[(String, String)]) -> Result<usize> {
        self.with_single_group_conn(|conn| {
            let now = schema::now_rfc3339()?;
            let mut stmt = conn.prepare(
                "INSERT OR IGNORE INTO pending_embeddings
                    (node_id, content_hash, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?3)",
            )?;
            let mut inserted = 0;
            for (node_id, content_hash) in items {
                inserted += stmt.execute(rusqlite::params![node_id, content_hash, now])?;
            }
            Ok(inserted)
        })
    }

    /// Claims the oldest pending queue rows for the embedding worker:
    /// status='pending' and attempts below the cap, ordered by id. This
    /// is a pure SELECT — the store side keeps no claim state and no
    /// backoff column; the worker paces itself via its own interval and
    /// a crash simply re-claims the same rows (idempotent by content
    /// hash).
    pub fn claim_embedding_batch(&self, limit: usize) -> Result<Vec<PendingEmbedding>> {
        self.with_single_group_conn(|conn| {
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);
            let mut stmt = conn.prepare(&format!(
                "SELECT id, node_id, content_hash, attempts
                 FROM pending_embeddings
                 WHERE status = 'pending' AND attempts < {MAX_EMBEDDING_ATTEMPTS}
                 ORDER BY id LIMIT ?1"
            ))?;
            let rows = stmt
                .query_map(rusqlite::params![limit], pending_embedding_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// Marks a claimed queue row done after its vector was written.
    pub fn mark_embedding_done(&self, id: i64) -> Result<()> {
        self.with_single_group_conn(|conn| {
            conn.execute(
                "UPDATE pending_embeddings
                 SET status = 'done', updated_at = ?2
                 WHERE id = ?1",
                rusqlite::params![id, schema::now_rfc3339()?],
            )?;
            Ok(())
        })
    }

    /// Records a failed embedding attempt: attempts += 1. When the
    /// attempts cap is reached the row flips to 'failed' and stays
    /// inspectable; below the cap it stays 'pending' and is re-claimed
    /// on a later pass.
    pub fn mark_embedding_attempt_failed(&self, id: i64) -> Result<()> {
        self.with_single_group_conn(|conn| {
            conn.execute(
                &format!(
                    "UPDATE pending_embeddings
                     SET attempts = attempts + 1,
                         status = CASE WHEN attempts + 1 >= {MAX_EMBEDDING_ATTEMPTS}
                                       THEN 'failed' ELSE status END,
                         updated_at = ?2
                     WHERE id = ?1"
                ),
                rusqlite::params![id, schema::now_rfc3339()?],
            )?;
            Ok(())
        })
    }

    /// Writes (or overwrites) the embedding of one graph node. The
    /// vector is stored as a little-endian f32 blob of EMBEDDING_DIM
    /// dimensions; any other length is rejected before hitting sqlite.
    ///
    /// vec0 0.1.9 does not implement INSERT OR REPLACE on a TEXT primary
    /// key (it raises the UNIQUE constraint error instead of
    /// replacing), so the upsert is DELETE + INSERT in one transaction.
    pub fn upsert_node_embedding(&self, node_id: &str, embedding: &[f32]) -> Result<()> {
        let blob = embedding_to_blob(embedding)?;
        self.with_single_group_conn(|conn| {
            let tx = conn.transaction()?;
            tx.execute("DELETE FROM node_embeddings WHERE node_id = ?1", [node_id])?;
            tx.execute(
                "INSERT INTO node_embeddings (node_id, embedding) VALUES (?1, ?2)",
                rusqlite::params![node_id, blob],
            )?;
            tx.commit()?;
            Ok(())
        })
    }

    /// The tombstone-cleanup primitive (decisions 66/67): removes every
    /// trace of a merged-away node — its vec row AND all its queue rows
    /// — in one transaction.
    pub fn delete_node_embedding_rows(&self, node_id: &str) -> Result<()> {
        self.with_single_group_conn(|conn| {
            let tx = conn.transaction()?;
            tx.execute("DELETE FROM node_embeddings WHERE node_id = ?1", [node_id])?;
            tx.execute(
                "DELETE FROM pending_embeddings WHERE node_id = ?1",
                [node_id],
            )?;
            tx.commit()?;
            Ok(())
        })
    }

    /// KNN over node_embeddings with the vec0 0.1.x idiom
    /// (`embedding MATCH ? AND k = ?`). Returns (node_id, distance)
    /// pairs, nearest first; fewer than k rows when the table holds
    /// fewer vectors.
    ///
    /// Metric (migration v8, decision 73): the vector column is
    /// declared `distance_metric=cosine`, so the returned `distance` is
    /// the vec0 COSINE DISTANCE, `1 - cosine_similarity`
    /// (distance_cosine_float in sqlite-vec.c). The decision-73
    /// thresholds are similarities, so the resolver converts with
    /// `similarity = 1.0 - distance` (identical vectors -> distance 0,
    /// orthogonal vectors -> distance 1). Ordering is ascending
    /// distance = descending similarity, so the best match is first.
    pub fn knn_node_embeddings(&self, query: &[f32], k: usize) -> Result<Vec<(String, f32)>> {
        let blob = embedding_to_blob(query)?;
        self.with_single_group_conn(|conn| {
            let k = i64::try_from(k).unwrap_or(i64::MAX);
            let mut stmt = conn.prepare(
                "SELECT node_id, distance FROM node_embeddings
                 WHERE embedding MATCH ?1 AND k = ?2",
            )?;
            let rows = stmt
                .query_map(rusqlite::params![blob, k], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, f32>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// The latest done (node_id, content_hash) of every node: the
    /// first-startup backfill (decision 66) diffs the graph against
    /// this set to find nodes whose current content was never embedded.
    /// A node may hold several done rows from successive contents; the
    /// row with the max id per node wins.
    pub fn done_embedding_hashes(&self) -> Result<Vec<(String, String)>> {
        self.with_single_group_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT node_id, content_hash FROM pending_embeddings AS done
                 WHERE done.status = 'done'
                   AND done.id = (
                       SELECT MAX(id) FROM pending_embeddings
                       WHERE node_id = done.node_id AND status = 'done'
                   )
                 ORDER BY node_id",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// The done-JOURNAL of the embedding sidecar (decision 66): records
    /// the (node_id, content_hash) pair that was ACTUALLY embedded. The
    /// recorded hash is the hash of the STORED node content, which can
    /// differ from the claimed row's candidate hash under alias drift
    /// (the stored properties win the MERGE coalesce, Rule R4), so the
    /// worker journals what it embedded instead of trusting the claim.
    /// `done_embedding_hashes` then reflects reality.
    ///
    /// INSERT OR IGNORE through the pending_embeddings_dedup UNIQUE
    /// index: re-recording the same pair (or recording a pair whose row
    /// the worker just marked done) is a no-op.
    pub fn record_node_embedded(&self, node_id: &str, content_hash: &str) -> Result<()> {
        self.with_single_group_conn(|conn| {
            let now = schema::now_rfc3339()?;
            conn.execute(
                "INSERT OR IGNORE INTO pending_embeddings
                    (node_id, content_hash, status, attempts, created_at, updated_at)
                 VALUES (?1, ?2, 'done', 0, ?3, ?3)",
                rusqlite::params![node_id, content_hash, now],
            )?;
            Ok(())
        })
    }

    /// Every node id known to the embedding sidecar: the UNION of vec
    /// rows and queue rows. Reconciliation diffs this against the graph
    /// to detect orphans (vec rows whose node was deleted without a
    /// tombstone cleanup).
    pub fn all_embedding_node_ids(&self) -> Result<Vec<String>> {
        self.with_single_group_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT node_id FROM node_embeddings
                 UNION
                 SELECT node_id FROM pending_embeddings
                 ORDER BY node_id",
            )?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// Every node id with a VEC TABLE row (decision 74): the merge-tool
    /// candidate scan KNNs each embedded node with its own stored
    /// vector, so it must seed from the vec table only — a queue-only
    /// id has no vector yet, and the UNION of
    /// [`Store::all_embedding_node_ids`] (the reconciliation orphan
    /// detector) would mislead the scan.
    pub fn embedded_node_ids(&self) -> Result<Vec<String>> {
        self.with_single_group_conn(|conn| {
            let mut stmt = conn.prepare("SELECT node_id FROM node_embeddings ORDER BY node_id")?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// The stored vector of one node (decision 74): the merge-tool
    /// candidate scan KNNs each embedded node with the node's OWN
    /// stored vector, so it needs a per-node vector read (the KNN
    /// helper takes the query vector as an argument and never reads
    /// one back). `None` when the node has no vec row.
    pub fn node_embedding(&self, node_id: &str) -> Result<Option<Vec<f32>>> {
        self.with_single_group_conn(|conn| {
            let mut stmt =
                conn.prepare("SELECT embedding FROM node_embeddings WHERE node_id = ?1")?;
            let mut rows = stmt.query([node_id])?;
            match rows.next()? {
                Some(row) => {
                    let blob: Vec<u8> = row.get(0)?;
                    Ok(Some(blob_to_embedding(&blob)?))
                }
                None => Ok(None),
            }
        })
    }

    /// Appends one merge-tool action to `merge_audit` (migration v9,
    /// decision 74). The audit row is written for ALL three verdicts —
    /// the snapshot is the rollback source of a 'same' merge and is
    /// None for 'related'/'different'. The `id`, `rolled_back`, and
    /// `created_at` fields of `row` are ignored (see MergeAuditRow).
    /// Returns the new audit id.
    ///
    /// Takes no chat_id (the same single-group contract as the
    /// embedding helpers): the merge tool is an offline operator tool
    /// that opens exactly one group per run.
    pub fn insert_merge_audit(&self, row: &MergeAuditRow) -> Result<i64> {
        self.with_single_group_conn(|conn| {
            conn.execute(
                "INSERT INTO merge_audit (
                    loser_id, survivor_id, loser_kind, loser_name,
                    loser_description, verdict, reason, confirmed_by,
                    edges_moved, self_loops_dropped, edges_deduped,
                    snapshot, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                rusqlite::params![
                    row.loser_id,
                    row.survivor_id,
                    row.loser_kind,
                    row.loser_name,
                    row.loser_description,
                    row.verdict,
                    row.reason,
                    row.confirmed_by,
                    row.edges_moved,
                    row.self_loops_dropped,
                    row.edges_deduped,
                    row.snapshot,
                    schema::now_rfc3339()?,
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    /// All merge_audit rows ordered by id. Used by tests and a possible
    /// future inspect mode. Audit reads are rare; the table has no
    /// indexes beyond the primary key, so this is a deliberate full
    /// scan (migration v9 comment).
    pub fn list_merge_audit(&self) -> Result<Vec<MergeAuditRow>> {
        self.with_single_group_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, loser_id, survivor_id, loser_kind, loser_name,
                        loser_description, verdict, reason, confirmed_by,
                        edges_moved, self_loops_dropped, edges_deduped,
                        snapshot, rolled_back, created_at
                 FROM merge_audit ORDER BY id",
            )?;
            let rows = stmt
                .query_map([], merge_audit_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// Flips the rolled_back flag of one audit row (the
    /// `--merge-rollback` flow of graph-spec Section 7.7). A missing
    /// audit id is a LOUD error — a rollback against a nonexistent
    /// audit row must never pass silently.
    pub fn mark_merge_rolled_back(&self, id: i64) -> Result<()> {
        self.with_single_group_conn(|conn| {
            let updated = conn.execute(
                "UPDATE merge_audit SET rolled_back = 1 WHERE id = ?1",
                rusqlite::params![id],
            )?;
            if updated == 0 {
                return Err(StoreError::InvalidValue {
                    key: "merge_audit_id".to_string(),
                    value: format!("no merge_audit row with id {id}"),
                });
            }
            Ok(())
        })
    }

    /// Runs `f` on the Store's single open group connection. The
    /// single-group helpers (the embedding sidecar and merge_audit)
    /// take no chat_id (their interface is pinned by the parallel
    /// subtasks), so they operate on the Store's single open group: a
    /// Store used through these helpers must have exactly one group
    /// open. Rule P5 still holds — the connection is the same per-group
    /// store.db; the group was fixed at open_group time.
    fn with_single_group_conn<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        let mut guard = self.lock();
        if guard.len() != 1 {
            return Err(StoreError::AmbiguousGroup(guard.len()));
        }
        let conn = guard.values_mut().next().expect("len checked above");
        f(conn)
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

// The column list of the raw-log SELECT queries. `list_messages`,
// `list_messages_after`, and `list_messages_in_range` share it.
const MESSAGE_COLUMNS: &str = "id, platform_msg_id, direction, event_type, timestamp,
        sender_id, sender_display_name, sender_username, text,
        reply_to_platform_msg_id, mentions_bot, is_reply_to_bot";

/// Attempts cap of the embedding queue (decision 66). At the cap a row
/// flips from 'pending' to 'failed' and stays inspectable.
const MAX_EMBEDDING_ATTEMPTS: u32 = 3;

/// Encodes an f32 vector as the little-endian byte blob vec0 accepts
/// for `float[N]` columns. The length is validated against the pinned
/// dimension so a wrong-dimension call fails with a store error instead
/// of vec0's less contextual message.
fn embedding_to_blob(embedding: &[f32]) -> Result<Vec<u8>> {
    if embedding.len() != EMBEDDING_DIM {
        return Err(StoreError::InvalidValue {
            key: "embedding_dim".to_string(),
            value: format!("expected {EMBEDDING_DIM}, got {}", embedding.len()),
        });
    }
    Ok(embedding.iter().flat_map(|f| f.to_le_bytes()).collect())
}

/// Decodes the little-endian float32 blob vec0 returns for a
/// `float[N]` column back into a vector (the inverse of
/// [`embedding_to_blob`]). A length-mismatched blob is a store error,
/// not a silent truncation.
fn blob_to_embedding(blob: &[u8]) -> Result<Vec<f32>> {
    if blob.len() != EMBEDDING_DIM * 4 {
        return Err(StoreError::InvalidValue {
            key: "embedding_blob".to_string(),
            value: format!("expected {} bytes, got {}", EMBEDDING_DIM * 4, blob.len()),
        });
    }
    Ok(blob
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Maps one row of a pending_embeddings SELECT to a `PendingEmbedding`.
fn pending_embedding_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PendingEmbedding> {
    Ok(PendingEmbedding {
        id: row.get("id")?,
        node_id: row.get("node_id")?,
        content_hash: row.get("content_hash")?,
        attempts: row.get("attempts")?,
    })
}

/// Maps one row of a merge_audit SELECT to a `MergeAuditRow`.
fn merge_audit_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MergeAuditRow> {
    let created_at: String = row.get("created_at")?;
    Ok(MergeAuditRow {
        id: row.get("id")?,
        loser_id: row.get("loser_id")?,
        survivor_id: row.get("survivor_id")?,
        loser_kind: row.get("loser_kind")?,
        loser_name: row.get("loser_name")?,
        loser_description: row.get("loser_description")?,
        verdict: row.get("verdict")?,
        reason: row.get("reason")?,
        confirmed_by: row.get("confirmed_by")?,
        edges_moved: row.get("edges_moved")?,
        self_loops_dropped: row.get("self_loops_dropped")?,
        edges_deduped: row.get("edges_deduped")?,
        snapshot: row.get("snapshot")?,
        rolled_back: row.get("rolled_back")?,
        created_at: schema::parse_rfc3339(&created_at)?,
    })
}

/// Maps one row of a dead_letter SELECT to a `DeadLetterRow`.
/// `list_dead_letters` and `read_group_status` share it.
fn dead_letter_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeadLetterRow> {
    let created_at: String = row.get("created_at")?;
    Ok(DeadLetterRow {
        id: row.get("id")?,
        batch_id: row.get("batch_id")?,
        batch_skeleton: row.get("batch_skeleton")?,
        error: row.get("error")?,
        created_at: schema::parse_rfc3339(&created_at)?,
    })
}

/// Opens a group store.db READ-ONLY with a 2 s busy timeout and
/// registers sqlite-vec. Registration is per-connection: even a status
/// reader must register, or any query touching node_embeddings fails
/// with "no such module: vec0".
///
/// Brief SQLITE_BUSY windows exist while the bot shuts down or recovers;
/// the busy timeout waits them out before failing.
fn open_read_only(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_secs(2))?;
    register_sqlite_vec(&conn)?;
    Ok(conn)
}

/// Opens the group store.db READ-ONLY (SQLITE_OPEN_READ_ONLY, no
/// directory creation, no migrations) and reads the status snapshot of
/// specs.md Sections 10.3 and 12. Returns `Ok(None)` when store.db does
/// not exist (the group was never served). Validates chat_id with the
/// same rules as Store.
///
/// The read is safe while the live bot holds the store: a WAL-mode
/// database (specs.md Section 5.1) accepts read-only connections as long
/// as the -shm/-wal files exist. The 2 s busy timeout covers the brief
/// SQLITE_BUSY windows of a bot shutdown or recovery.
///
/// Caveat: after a CLEAN shutdown of the bot the -wal/-shm files are
/// removed. The read-only open still succeeds, but the FIRST QUERY can
/// fail with SQLITE_READONLY when the containing directory is not
/// writable. The caller surfaces this with an operator hint.
pub fn read_group_status(
    data_root: &Path,
    chat_id: &str,
    recent_dead_letters: usize,
) -> Result<Option<GroupStatus>> {
    validate_chat_id(chat_id)?;
    let path = data_root.join(chat_id).join("store.db");
    if !path.exists() {
        return Ok(None);
    }
    let conn = open_read_only(&path)?;

    // The state-table key set is open: read all pairs, never an
    // enumerated key list.
    let state = {
        let mut stmt = conn.prepare("SELECT key, value FROM state")?;
        let pairs = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<std::result::Result<HashMap<_, _>, _>>()?;
        pairs
    };
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM dead_letter", [], |row| row.get(0))?;
    let limit = i64::try_from(recent_dead_letters).unwrap_or(i64::MAX);
    let recent = {
        let mut stmt = conn.prepare(
            "SELECT id, batch_id, batch_skeleton, error, created_at
             FROM dead_letter ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![limit], dead_letter_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows
    };
    Ok(Some(GroupStatus {
        state,
        // COUNT(*) is never negative; the conversion saturates on the
        // theoretical overflow.
        dead_letter_count: u64::try_from(count).unwrap_or(u64::MAX),
        recent_dead_letters: recent,
    }))
}

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
        sender_username: row.get("sender_username")?,
        text: row.get("text")?,
        reply_to_platform_msg_id: row.get("reply_to_platform_msg_id")?,
        mentions_bot: row.get("mentions_bot")?,
        is_reply_to_bot: row.get("is_reply_to_bot")?,
    })
}

/// Maps one row of a reactions SELECT to a `ReactionRow`.
fn reaction_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReactionRow> {
    let old_emojis: String = row.get("old_emojis")?;
    let new_emojis: String = row.get("new_emojis")?;
    let timestamp: String = row.get("timestamp")?;
    Ok(ReactionRow {
        id: row.get("id")?,
        platform_msg_id: row.get("platform_msg_id")?,
        reactor_user_id: row.get("reactor_user_id")?,
        anonymous: row.get("anonymous")?,
        aggregated: row.get("aggregated")?,
        old_emojis: parse_emojis(&old_emojis)?,
        new_emojis: parse_emojis(&new_emojis)?,
        timestamp: schema::parse_rfc3339(&timestamp)?,
    })
}

/// Maps one row of a context_summaries SELECT to a `ContextSummaryRow`.
/// `find_context_summary` and `list_newest_context_summaries` share it.
fn context_summary_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContextSummaryRow> {
    Ok(ContextSummaryRow {
        id: row.get("id")?,
        first_msg_id: row.get("first_msg_id")?,
        last_msg_id: row.get("last_msg_id")?,
        content: row.get("content")?,
    })
}

/// Serializes an emoji set as a JSON array string. A serialization
/// failure is a storage failure. It is mapped into the Sqlite variant
/// the same way schema::format_rfc3339 maps formatting failures.
fn serialize_emojis(emojis: &[String]) -> Result<String> {
    serde_json::to_string(emojis)
        .map_err(|e| StoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(e))))
}

/// Parses a JSON emoji set from the database. A corrupt stored value
/// becomes a sqlite conversion error.
fn parse_emojis(s: &str) -> rusqlite::Result<Vec<String>> {
    serde_json::from_str(s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
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
            sender_username: Some("alice".to_string()),
            text: "hello".to_string(),
            reply_to_platform_msg_id: Some("m0".to_string()),
            mentions_bot: true,
            is_reply_to_bot: false,
        }
    }

    #[test]
    fn count_inbound_after_counts_only_inbound_rows_above_the_boundary() {
        // The M4 recency re-check (specs.md Section 6.2): newer human
        // messages after the target decide whether a reply is stale.
        let (_dir, store) = temp_store();
        let base = sample_message();
        let mut ids = Vec::new();
        for index in 1..=4_i64 {
            // Rows 1-3 inbound, row 4 outbound (the bot's own speech,
            // Rule B1). The recency re-check counts human messages only.
            let direction = if index == 4 {
                Direction::Outbound
            } else {
                Direction::Inbound
            };
            let msg = NewMessage {
                platform_msg_id: format!("m{index}"),
                direction,
                timestamp: base.timestamp + time::Duration::seconds(index),
                text: format!("text {index}"),
                ..base.clone()
            };
            match store.insert_message("c1", &msg).expect("insert") {
                InsertOutcome::Inserted(id) => ids.push(id),
                other => panic!("expected Inserted, got {other:?}"),
            }
        }

        // From a zero boundary: the three inbound rows, not the outbound
        // row.
        assert_eq!(store.count_inbound_after("c1", 0).expect("count"), 3);
        // Exact boundary semantics: `id > after_id` excludes the boundary
        // row itself.
        assert_eq!(store.count_inbound_after("c1", ids[0]).expect("count"), 2);
        assert_eq!(store.count_inbound_after("c1", ids[2]).expect("count"), 0);
        // The outbound tail row counts nothing.
        assert_eq!(store.count_inbound_after("c1", ids[3]).expect("count"), 0);
    }

    #[test]
    fn find_sender_by_platform_msg_id_returns_the_newest_matching_row() {
        // The M5 recall worker resolves a reply target to its sender
        // (Section 8.1 step 1 of the database spec). Duplicates of one
        // platform id exist (an edit appends a row, specs.md Section 15);
        // the newest row wins.
        let (_dir, store) = temp_store();
        assert_eq!(
            store
                .find_sender_by_platform_msg_id("c1", "m-missing")
                .expect("missing lookup"),
            None
        );

        let base = sample_message();
        let first = NewMessage {
            platform_msg_id: "m-dup".to_string(),
            sender_id: "u1".to_string(),
            sender_display_name: "Alice".to_string(),
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp"),
            ..base.clone()
        };
        let second = NewMessage {
            platform_msg_id: "m-dup".to_string(),
            sender_id: "u1".to_string(),
            sender_display_name: "Alice (renamed)".to_string(),
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp"),
            ..base
        };
        assert!(matches!(
            store.insert_message("c1", &first).expect("insert first"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store.insert_message("c1", &second).expect("insert second"),
            InsertOutcome::Inserted(_)
        ));

        assert_eq!(
            store
                .find_sender_by_platform_msg_id("c1", "m-dup")
                .expect("lookup"),
            Some(("u1".to_string(), "Alice (renamed)".to_string()))
        );
        // Rule P5: one group's data never crosses into another group.
        assert_eq!(
            store
                .find_sender_by_platform_msg_id("c2", "m-dup")
                .expect("other group lookup"),
            None
        );
    }

    #[test]
    fn sender_username_round_trips() {
        // The XML context rendering (msg 标签) shows the username beside
        // the display name. The value must survive the log round trip.
        let (_dir, store) = temp_store();
        let msg = NewMessage {
            sender_username: Some("alice_w".to_string()),
            ..sample_message()
        };
        assert!(matches!(
            store.insert_message("c1", &msg).expect("insert"),
            InsertOutcome::Inserted(_)
        ));

        let rows = store.list_messages("c1").expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sender_username, Some("alice_w".to_string()));
    }

    #[test]
    fn missing_sender_username_reads_back_as_none() {
        // A sender without a username, and every row written before
        // migration v4, holds NULL. Migration v4 added the column as
        // nullable with no default, so both cases read back as None.
        let (_dir, store) = temp_store();
        let msg = NewMessage {
            sender_username: None,
            ..sample_message()
        };
        assert!(matches!(
            store.insert_message("c1", &msg).expect("insert"),
            InsertOutcome::Inserted(_)
        ));

        let rows = store.list_messages("c1").expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sender_username, None);
    }

    #[test]
    fn find_reply_target_returns_the_original_row_for_shared_platform_msg_id() {
        // Rule P1: the append-only raw log decides the reply target.
        // An edit appends a second row with the same platform_msg_id
        // (specs.md Section 15). MIN(id) pins the ORIGINAL row; its
        // display name is fixed at intake, not the edit's.
        let (_dir, store) = temp_store();
        let original = NewMessage {
            platform_msg_id: "m-target".to_string(),
            sender_display_name: "Alice".to_string(),
            ..sample_message()
        };
        let edit = NewMessage {
            platform_msg_id: "m-target".to_string(),
            event_type: EventType::Edit,
            sender_display_name: "Alice (renamed)".to_string(),
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp"),
            text: "hello (edited)".to_string(),
            ..sample_message()
        };

        let original_id = match store
            .insert_message("c1", &original)
            .expect("insert original")
        {
            InsertOutcome::Inserted(id) => id,
            other => panic!("expected Inserted, got {other:?}"),
        };
        assert!(matches!(
            store.insert_message("c1", &edit).expect("insert edit"),
            InsertOutcome::Inserted(_)
        ));

        assert_eq!(
            store.find_reply_target("c1", "m-target").expect("lookup"),
            Some(ReplyTargetRow {
                row_id: original_id,
                display_name: "Alice".to_string(),
            })
        );
    }

    #[test]
    fn find_reply_target_returns_none_for_unknown_platform_msg_id() {
        // A reply to a message the bot never logged (e.g. it predates the
        // bot joining the group) has no target.
        let (_dir, store) = temp_store();
        assert!(matches!(
            store
                .insert_message("c1", &sample_message())
                .expect("insert"),
            InsertOutcome::Inserted(_)
        ));

        assert_eq!(
            store
                .find_reply_target("c1", "m-missing")
                .expect("missing lookup"),
            None
        );
        // Rule P5: one group's data never crosses into another group.
        assert_eq!(
            store
                .find_reply_target("c2", "m1")
                .expect("other group lookup"),
            None
        );
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
        assert_eq!(count, 9);
        let versions: Vec<u32> = {
            let mut stmt = conn
                .prepare("SELECT version FROM schema_migrations ORDER BY version")
                .expect("prepare versions");
            stmt.query_map([], |row| row.get(0))
                .expect("query versions")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect versions")
        };
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // Migration v4 added sender_username. The SELECT proves the column
        // exists: a missing column is an error, an empty log yields Ok(None).
        conn.query_row("SELECT sender_username FROM messages LIMIT 1", [], |row| {
            row.get::<_, Option<String>>(0)
        })
        .optional()
        .expect("sender_username column must exist");

        // Migration v5 added context_summaries. The SELECT proves the
        // table exists: a missing table is an error, an empty table yields
        // Ok(None).
        conn.query_row("SELECT id FROM context_summaries LIMIT 1", [], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .expect("context_summaries table must exist");

        // Migration v6 rebuilt messages_dedup with `text` in the key.
        let dedup_columns = index_columns(&conn, "messages_dedup");
        assert_eq!(
            dedup_columns,
            vec![
                "platform_msg_id",
                "direction",
                "event_type",
                "timestamp",
                "text"
            ]
        );

        // Migration v7 added the embedding sidecar. This connection is
        // NOT registered with sqlite-vec, so node_embeddings cannot be
        // queried here; its existence is proven via sqlite_master.
        let node_embeddings_exists: bool = conn
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master
                     WHERE type = 'table' AND name = 'node_embeddings'
                 )",
                [],
                |row| row.get(0),
            )
            .expect("node_embeddings lookup");
        assert!(node_embeddings_exists, "node_embeddings must exist");
    }

    #[test]
    fn migration_v6_upgrades_a_v5_database_in_place() {
        // A database created by the previous release carries migrations
        // v1-v5 and live data. Opening it with this build must apply only
        // v6 (index-only: no data touched) plus the additive v7-v9
        // (embedding sidecar, merge audit), keep the data, and be a
        // no-op on reopen.
        let dir = tempfile::tempdir().expect("tempdir");
        let group_dir = dir.path().join("c1");
        std::fs::create_dir_all(&group_dir).expect("create group dir");
        {
            let conn = Connection::open(group_dir.join("store.db")).expect("open v5 db");
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                    version    INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );",
            )
            .expect("create schema_migrations");
            // Replay the v5-era runner: migrations 1-5 only, each with its
            // recorded applied_at.
            for (version, sql) in schema::MIGRATIONS.iter().take(5) {
                conn.execute_batch(sql).expect("apply v5 migration");
                conn.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
                    rusqlite::params![version, schema::now_rfc3339().expect("timestamp")],
                )
                .expect("record v5 migration");
            }
            // The v5-era index shape: no `text` column in the key.
            assert_eq!(
                index_columns(&conn, "messages_dedup"),
                vec!["platform_msg_id", "direction", "event_type", "timestamp"]
            );
            // Data written by the v5-era bot must survive the upgrade.
            let msg = sample_message();
            let timestamp = schema::format_rfc3339(msg.timestamp).expect("format");
            conn.execute(
                "INSERT INTO messages (
                    platform_msg_id, direction, event_type, timestamp,
                    sender_id, sender_display_name, sender_username, text,
                    reply_to_platform_msg_id, mentions_bot, is_reply_to_bot
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    msg.platform_msg_id,
                    msg.direction.as_str(),
                    msg.event_type.as_str(),
                    timestamp,
                    msg.sender_id,
                    msg.sender_display_name,
                    msg.sender_username,
                    msg.text,
                    msg.reply_to_platform_msg_id,
                    msg.mentions_bot,
                    msg.is_reply_to_bot,
                ],
            )
            .expect("insert v5 message");
        }

        // The upgrade open applies v6 through v9. A reopen is a no-op.
        let store = Store::new(dir.path().to_path_buf());
        store.open_group("c1").expect("upgrade open");
        let store2 = Store::new(dir.path().to_path_buf());
        store2.open_group("c1").expect("reopen after upgrade");

        // The v5-era message survives the index-only upgrade.
        let rows = store.list_messages("c1").expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].platform_msg_id, "m1");

        // The index now carries `text`; the migration is index-only, so
        // the name is unchanged and no other index appeared.
        let conn = Connection::open(dir.path().join("c1").join("store.db")).expect("open db");
        assert_eq!(
            index_columns(&conn, "messages_dedup"),
            vec![
                "platform_msg_id",
                "direction",
                "event_type",
                "timestamp",
                "text"
            ]
        );

        // Migration bookkeeping: exactly four versions were added.
        let versions: Vec<u32> = {
            let mut stmt = conn
                .prepare("SELECT version FROM schema_migrations ORDER BY version")
                .expect("prepare versions");
            stmt.query_map([], |row| row.get(0))
                .expect("query versions")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect versions")
        };
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    /// The column names of one index, in key order, via PRAGMA index_info.
    fn index_columns(conn: &Connection, index: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA index_info({index})"))
            .expect("prepare index_info");
        stmt.query_map([], |row| row.get::<_, String>(2))
            .expect("query index_info")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("collect index_info")
    }

    #[test]
    fn migration_v5_upgrades_a_v4_database_in_place() {
        // A database created by the previous release carries migrations
        // v1-v4 and live data. Opening it with this build must apply only
        // v5, keep the data, and be a no-op on reopen.
        let dir = tempfile::tempdir().expect("tempdir");
        let group_dir = dir.path().join("c1");
        std::fs::create_dir_all(&group_dir).expect("create group dir");
        {
            let conn = Connection::open(group_dir.join("store.db")).expect("open v4 db");
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                    version    INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );",
            )
            .expect("create schema_migrations");
            // Replay the v4-era runner: migrations 1-4 only, each with its
            // recorded applied_at.
            for (version, sql) in schema::MIGRATIONS.iter().take(4) {
                conn.execute_batch(sql).expect("apply v4 migration");
                conn.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
                    rusqlite::params![version, schema::now_rfc3339().expect("timestamp")],
                )
                .expect("record v4 migration");
            }
            // Data written by the v4-era bot must survive the upgrade.
            let msg = sample_message();
            let timestamp = schema::format_rfc3339(msg.timestamp).expect("format");
            conn.execute(
                "INSERT INTO messages (
                    platform_msg_id, direction, event_type, timestamp,
                    sender_id, sender_display_name, sender_username, text,
                    reply_to_platform_msg_id, mentions_bot, is_reply_to_bot
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    msg.platform_msg_id,
                    msg.direction.as_str(),
                    msg.event_type.as_str(),
                    timestamp,
                    msg.sender_id,
                    msg.sender_display_name,
                    msg.sender_username,
                    msg.text,
                    msg.reply_to_platform_msg_id,
                    msg.mentions_bot,
                    msg.is_reply_to_bot,
                ],
            )
            .expect("insert v4 message");
        }

        // The upgrade open applies v5. A reopen is a no-op.
        let store = Store::new(dir.path().to_path_buf());
        store.open_group("c1").expect("upgrade open");
        let store2 = Store::new(dir.path().to_path_buf());
        store2.open_group("c1").expect("reopen after upgrade");

        // The v4-era message survives the in-place upgrade.
        let rows = store.list_messages("c1").expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].platform_msg_id, "m1");

        // The new table is usable right after the upgrade.
        let summary_id = store
            .insert_context_summary("c1", 0, rows[0].id, "the first chunk")
            .expect("insert summary");
        assert!(summary_id > 0);

        // Migration bookkeeping: the upgrade applied v5 through v9.
        let conn = Connection::open(dir.path().join("c1").join("store.db")).expect("open db");
        let versions: Vec<u32> = {
            let mut stmt = conn
                .prepare("SELECT version FROM schema_migrations ORDER BY version")
                .expect("prepare versions");
            stmt.query_map([], |row| row.get(0))
                .expect("query versions")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect versions")
        };
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
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
        assert_eq!(row.sender_username, msg.sender_username);
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
    fn byte_identical_inbound_redelivery_still_dedups_to_one_row() {
        // Migration v6 (decision 65) added `text` to the dedup key. An
        // inbound redelivery is byte-identical, so message dedup holds.
        let (_dir, store) = temp_store();
        let msg = sample_message();

        assert!(matches!(
            store.insert_message("c1", &msg).expect("first insert"),
            InsertOutcome::Inserted(_)
        ));
        assert_eq!(
            store.insert_message("c1", &msg).expect("redelivery"),
            InsertOutcome::Duplicate
        );
        assert_eq!(store.list_messages("c1").expect("list").len(), 1);
    }

    #[test]
    fn same_second_edits_with_different_text_both_persist() {
        // The H1 fix (migration v6, decision 65): the pre-v6 dedup key
        // collapsed the second edit into the first. With `text` in the
        // key, two same-second edits with different text both land.
        let (_dir, store) = temp_store();
        let base = sample_message();
        let edit_timestamp =
            OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp");
        let edit1 = NewMessage {
            event_type: EventType::Edit,
            timestamp: edit_timestamp,
            text: "hello (edit A)".to_string(),
            ..base.clone()
        };
        let edit2 = NewMessage {
            event_type: EventType::Edit,
            timestamp: edit_timestamp,
            text: "hello (edit B)".to_string(),
            ..base.clone()
        };

        assert!(matches!(
            store.insert_message("c1", &base).expect("insert message"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store.insert_message("c1", &edit1).expect("insert edit A"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store.insert_message("c1", &edit2).expect("insert edit B"),
            InsertOutcome::Inserted(_)
        ));

        let rows = store.list_messages("c1").expect("list");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].text, "hello (edit A)");
        assert_eq!(rows[2].text, "hello (edit B)");
    }

    #[test]
    fn same_second_identical_text_edits_collapse() {
        // The documented harmless case of migration v6 (decision 65): two
        // same-second edits with IDENTICAL text share the full dedup key,
        // so the second collapses. Nothing changed between them, so no
        // information is lost.
        let (_dir, store) = temp_store();
        let base = sample_message();
        let edit = NewMessage {
            event_type: EventType::Edit,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp"),
            text: "hello (edited)".to_string(),
            ..base.clone()
        };

        assert!(matches!(
            store.insert_message("c1", &base).expect("insert message"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store.insert_message("c1", &edit).expect("insert edit"),
            InsertOutcome::Inserted(_)
        ));
        assert_eq!(
            store
                .insert_message("c1", &edit)
                .expect("insert identical edit"),
            InsertOutcome::Duplicate
        );

        // The original plus one edit row: the collapse keeps the count at 2.
        assert_eq!(store.list_messages("c1").expect("list").len(), 2);
    }

    #[test]
    fn find_latest_message_by_platform_msg_id_returns_the_newest_row() {
        // Edit rows share platform_msg_id with their original (specs.md
        // Section 15). The newest row (highest id) is the latest edit.
        let (_dir, store) = temp_store();
        let base = sample_message();
        let original = NewMessage {
            platform_msg_id: "m-edit".to_string(),
            text: "original text".to_string(),
            ..base.clone()
        };
        let edit = NewMessage {
            platform_msg_id: "m-edit".to_string(),
            event_type: EventType::Edit,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp"),
            text: "edited text".to_string(),
            ..base.clone()
        };
        let edit2 = NewMessage {
            platform_msg_id: "m-edit".to_string(),
            event_type: EventType::Edit,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_200).expect("valid timestamp"),
            text: "edited text v2".to_string(),
            ..base
        };

        assert!(matches!(
            store
                .insert_message("c1", &original)
                .expect("insert original"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store.insert_message("c1", &edit).expect("insert edit"),
            InsertOutcome::Inserted(_)
        ));
        let latest_id = match store.insert_message("c1", &edit2).expect("insert edit 2") {
            InsertOutcome::Inserted(id) => id,
            other => panic!("expected Inserted, got {other:?}"),
        };

        let latest = store
            .find_latest_message_by_platform_msg_id("c1", "m-edit")
            .expect("lookup")
            .expect("the row exists");
        assert_eq!(latest.id, latest_id);
        assert_eq!(latest.event_type, EventType::Edit);
        assert_eq!(latest.text, "edited text v2");
        assert_eq!(latest.platform_msg_id, "m-edit");
    }

    #[test]
    fn find_latest_message_by_platform_msg_id_returns_none_when_absent() {
        let (_dir, store) = temp_store();
        assert!(matches!(
            store
                .insert_message("c1", &sample_message())
                .expect("insert"),
            InsertOutcome::Inserted(_)
        ));

        assert_eq!(
            store
                .find_latest_message_by_platform_msg_id("c1", "m-missing")
                .expect("missing lookup"),
            None
        );
        // Rule P5: one group's data never crosses into another group.
        assert_eq!(
            store
                .find_latest_message_by_platform_msg_id("c2", "m1")
                .expect("other group lookup"),
            None
        );
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
    fn list_messages_in_range_returns_the_inclusive_range_in_order() {
        // The segmented summarizer reads the raw-log range (first, last]
        // of the chunk it replaces. The lower boundary is exclusive, the
        // upper boundary inclusive.
        let (_dir, store) = temp_store();
        let base = sample_message();
        let mut ids = Vec::new();
        for index in 1..=4_i64 {
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

        // (0, ids[1]] holds rows 1 and 2.
        let range = store
            .list_messages_in_range("c1", 0, ids[1])
            .expect("range (0, id2]");
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].id, ids[0]);
        assert_eq!(range[1].id, ids[1]);

        // (ids[0], ids[2]] excludes row 1, includes rows 2 and 3.
        let middle = store
            .list_messages_in_range("c1", ids[0], ids[2])
            .expect("range (id1, id3]");
        assert_eq!(middle.len(), 2);
        assert_eq!(middle[0].id, ids[1]);
        assert_eq!(middle[1].id, ids[2]);

        // An empty range returns an empty vec.
        let empty = store
            .list_messages_in_range("c1", ids[1], ids[1])
            .expect("empty range");
        assert!(empty.is_empty());

        // Rule P5: one group's data never crosses into another group.
        let other = store
            .list_messages_in_range("c2", 0, ids[3])
            .expect("other group range");
        assert!(other.is_empty());
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

    fn named_reaction() -> NewReaction {
        NewReaction {
            platform_msg_id: "m1".to_string(),
            reactor_user_id: Some("u1".to_string()),
            anonymous: false,
            aggregated: false,
            old_emojis: vec![],
            new_emojis: vec!["👍".to_string()],
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp"),
        }
    }

    #[test]
    fn insert_reaction_round_trips_all_three_event_kinds() {
        // specs.md Section 5.2: one row per reaction event. The three
        // kinds are a named reaction, an anonymous admin reaction, and
        // an aggregated count update without a reactor identity.
        let (_dir, store) = temp_store();
        let named = named_reaction();
        let anonymous_admin = NewReaction {
            platform_msg_id: "m2".to_string(),
            reactor_user_id: Some("chat:-1001234567890".to_string()),
            anonymous: true,
            aggregated: false,
            old_emojis: vec!["👍".to_string()],
            new_emojis: vec!["👍".to_string(), "❤".to_string()],
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp"),
        };
        let aggregated_count = NewReaction {
            platform_msg_id: "m1".to_string(),
            reactor_user_id: None,
            anonymous: false,
            aggregated: true,
            old_emojis: vec![],
            new_emojis: vec!["👍".to_string()],
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_200).expect("valid timestamp"),
        };

        assert!(matches!(
            store.insert_reaction("c1", &named).expect("insert named"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store
                .insert_reaction("c1", &anonymous_admin)
                .expect("insert anonymous admin"),
            InsertOutcome::Inserted(_)
        ));
        assert!(matches!(
            store
                .insert_reaction("c1", &aggregated_count)
                .expect("insert aggregated"),
            InsertOutcome::Inserted(_)
        ));

        let rows = store.list_reactions("c1").expect("list");
        assert_eq!(rows.len(), 3);

        let row = &rows[0];
        assert_eq!(row.platform_msg_id, named.platform_msg_id);
        assert_eq!(row.reactor_user_id, named.reactor_user_id);
        assert_eq!(row.anonymous, named.anonymous);
        assert_eq!(row.aggregated, named.aggregated);
        assert_eq!(row.old_emojis, named.old_emojis);
        assert_eq!(row.new_emojis, named.new_emojis);
        assert_eq!(row.timestamp, named.timestamp);

        let row = &rows[1];
        assert_eq!(row.platform_msg_id, anonymous_admin.platform_msg_id);
        assert_eq!(row.reactor_user_id, anonymous_admin.reactor_user_id);
        assert_eq!(row.anonymous, anonymous_admin.anonymous);
        assert_eq!(row.aggregated, anonymous_admin.aggregated);
        assert_eq!(row.old_emojis, anonymous_admin.old_emojis);
        assert_eq!(row.new_emojis, anonymous_admin.new_emojis);
        assert_eq!(row.timestamp, anonymous_admin.timestamp);

        let row = &rows[2];
        assert_eq!(row.platform_msg_id, aggregated_count.platform_msg_id);
        assert_eq!(row.reactor_user_id, None);
        assert_eq!(row.anonymous, aggregated_count.anonymous);
        assert_eq!(row.aggregated, aggregated_count.aggregated);
        assert_eq!(row.old_emojis, aggregated_count.old_emojis);
        assert_eq!(row.new_emojis, aggregated_count.new_emojis);
        assert_eq!(row.timestamp, aggregated_count.timestamp);
    }

    #[test]
    fn insert_reaction_is_idempotent_and_keeps_distinct_reactors() {
        // AGENT.md Section 6.2: a reconnect redelivers the same reaction
        // update. The redelivery lands as a Duplicate.
        let (_dir, store) = temp_store();
        let reaction = named_reaction();

        let first = store
            .insert_reaction("c1", &reaction)
            .expect("first insert");
        let second = store
            .insert_reaction("c1", &reaction)
            .expect("duplicate insert");

        assert!(matches!(first, InsertOutcome::Inserted(_)));
        assert_eq!(second, InsertOutcome::Duplicate);
        assert_eq!(store.list_reactions("c1").expect("list").len(), 1);

        // Two different named reactors on the same message, with the same
        // emoji sets and timestamp, both land. The reactor is part of the
        // dedup key.
        let other_reactor = NewReaction {
            reactor_user_id: Some("u2".to_string()),
            ..reaction.clone()
        };
        assert!(matches!(
            store
                .insert_reaction("c1", &other_reactor)
                .expect("insert second reactor"),
            InsertOutcome::Inserted(_)
        ));

        let rows = store.list_reactions("c1").expect("list");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].reactor_user_id, Some("u1".to_string()));
        assert_eq!(rows[1].reactor_user_id, Some("u2".to_string()));
    }

    #[test]
    fn negative_telegram_group_chat_id_is_accepted() {
        // Rule P5: Telegram group chat ids are negative integers. The
        // path-safety validation must accept them.
        let (dir, store) = temp_store();
        let chat_id = "-1001234567890";
        store.open_group(chat_id).expect("open negative chat id");
        assert!(dir.path().join(chat_id).join("store.db").is_file());

        let reaction = named_reaction();
        assert!(matches!(
            store.insert_reaction(chat_id, &reaction).expect("insert"),
            InsertOutcome::Inserted(_)
        ));
        let rows = store.list_reactions(chat_id).expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].platform_msg_id, reaction.platform_msg_id);
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

    #[test]
    fn read_group_status_returns_none_when_store_db_is_missing() {
        // A group that was never served has no store.db.
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(read_group_status(dir.path(), "c1", 5)
            .expect("read")
            .is_none());
    }

    #[test]
    fn read_group_status_rejects_invalid_chat_ids() {
        // The same path-safety rules as Store apply (Rule P5).
        let dir = tempfile::tempdir().expect("tempdir");
        for bad in ["../evil", "a/b", "a\\b", "a\0b", "", ".."] {
            match read_group_status(dir.path(), bad, 5) {
                Err(StoreError::InvalidChatId(_)) => {}
                other => panic!("expected InvalidChatId for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn read_group_status_reads_the_state_and_the_newest_dead_letters() {
        let (dir, store) = temp_store();
        store
            .set_state_many(
                "c1",
                &[
                    ("muted_flag".to_string(), "1".to_string()),
                    ("last_digest_boundary_msg_id".to_string(), "91".to_string()),
                ],
            )
            .expect("set state");
        store
            .increment_counter("c1", "wakes_total", 30)
            .expect("counter");
        for index in 1..=3 {
            store
                .insert_dead_letter(
                    "c1",
                    &format!("batch-{index}"),
                    "{\"items\":[]}",
                    &format!("boom {index}"),
                )
                .expect("insert dead letter");
        }

        let status = read_group_status(dir.path(), "c1", 2)
            .expect("read")
            .expect("the group store exists");
        assert_eq!(status.state.get("muted_flag"), Some(&"1".to_string()));
        assert_eq!(
            status.state.get("last_digest_boundary_msg_id"),
            Some(&"91".to_string())
        );
        assert_eq!(status.state.get("wakes_total"), Some(&"30".to_string()));
        assert_eq!(status.dead_letter_count, 3);
        // The limit is respected and the order is newest first.
        assert_eq!(status.recent_dead_letters.len(), 2);
        assert_eq!(status.recent_dead_letters[0].batch_id, "batch-3");
        assert_eq!(status.recent_dead_letters[1].batch_id, "batch-2");
    }

    #[test]
    fn read_group_status_works_while_a_store_holds_the_group_open() {
        // The live-bot case: the -shm/-wal files exist while the writer
        // runs, so the read-only open sees the committed data. No WAL
        // write is needed for the read itself.
        let (dir, store) = temp_store();
        store
            .set_state("c1", "wakes_total", "7")
            .expect("set state");
        // `store` still holds its connection open.
        let status = read_group_status(dir.path(), "c1", 5)
            .expect("read")
            .expect("the group store exists");
        assert_eq!(status.state.get("wakes_total"), Some(&"7".to_string()));
        assert_eq!(status.dead_letter_count, 0);
        assert!(status.recent_dead_letters.is_empty());
    }

    #[test]
    fn context_summary_insert_find_list_round_trip() {
        // The digest-completion handler persists the LLM summary of one
        // digested chunk (Rule P1), and the rebuild reads it back.
        let (_dir, store) = temp_store();
        // Check-before-call: a missing range reads as None.
        assert_eq!(
            store
                .find_context_summary("c1", 1, 10)
                .expect("find missing"),
            None
        );

        let id1 = store
            .insert_context_summary("c1", 1, 10, "Alice and Bob played go")
            .expect("insert 1");
        let id2 = store
            .insert_context_summary("c1", 11, 20, "Carol joined the chat")
            .expect("insert 2");
        assert!(id2 > id1);

        let found = store
            .find_context_summary("c1", 1, 10)
            .expect("find")
            .expect("row exists");
        assert_eq!(
            found,
            ContextSummaryRow {
                id: id1,
                first_msg_id: 1,
                last_msg_id: 10,
                content: "Alice and Bob played go".to_string(),
            }
        );

        // The list returns the newest rows by id, oldest first.
        let newest = store.list_newest_context_summaries("c1", 10).expect("list");
        assert_eq!(newest.len(), 2);
        assert_eq!(newest[0].id, id1);
        assert_eq!(newest[1].id, id2);

        // Rule P5: one group's data never crosses into another group.
        assert_eq!(
            store
                .find_context_summary("c2", 1, 10)
                .expect("other group find"),
            None
        );
        assert!(store
            .list_newest_context_summaries("c2", 10)
            .expect("other group list")
            .is_empty());
    }

    #[test]
    fn insert_context_summary_is_idempotent_on_the_natural_key() {
        // Rule P1 replay idempotency: a re-run of the digest-completion
        // handler re-inserts the same range. INSERT OR IGNORE keeps one
        // row and returns the existing row id.
        let (_dir, store) = temp_store();
        let id1 = store
            .insert_context_summary("c1", 1, 10, "first text")
            .expect("insert");
        let id2 = store
            .insert_context_summary("c1", 1, 10, "second text")
            .expect("re-insert");

        assert_eq!(id1, id2);
        let rows = store.list_newest_context_summaries("c1", 10).expect("list");
        assert_eq!(rows.len(), 1);
        // The first text wins: the re-insert did not overwrite it.
        assert_eq!(rows[0].content, "first text");
    }

    #[test]
    fn list_newest_context_summaries_returns_the_n_newest_oldest_first() {
        // The keep-two retention source for the rebuild: the N newest rows
        // by id, returned oldest first for placement after the preamble.
        let (_dir, store) = temp_store();
        let mut ids = Vec::new();
        for index in 0..5_i64 {
            let first = index * 10 + 1;
            let last = first + 9;
            let id = store
                .insert_context_summary("c1", first, last, &format!("summary {index}"))
                .expect("insert");
            ids.push(id);
        }

        let keep_two = store
            .list_newest_context_summaries("c1", 2)
            .expect("list newest 2");
        assert_eq!(keep_two.len(), 2);
        assert_eq!(keep_two[0].id, ids[3]);
        assert_eq!(keep_two[0].content, "summary 3");
        assert_eq!(keep_two[1].id, ids[4]);
        assert_eq!(keep_two[1].content, "summary 4");

        // A limit above the row count returns all rows, oldest first.
        let all = store
            .list_newest_context_summaries("c1", 100)
            .expect("list all");
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].id, ids[0]);
        assert_eq!(all[4].id, ids[4]);
    }

    // --- Embedding sidecar (migration v7, decision 66) ---------------

    /// Deterministic test vector: a gradient seeded by `seed`, always
    /// EMBEDDING_DIM long.
    fn test_vector(seed: u32) -> Vec<f32> {
        (0..EMBEDDING_DIM)
            .map(|i| ((i as u32).wrapping_mul(seed) % 997) as f32 / 997.0)
            .collect()
    }

    /// Opens group "c1" so the chat_id-less embedding helpers resolve to
    /// its connection.
    fn embedding_store() -> (tempfile::TempDir, Store) {
        let (dir, store) = temp_store();
        store.open_group("c1").expect("open group");
        (dir, store)
    }

    #[test]
    fn migration_v7_creates_embedding_tables_and_is_idempotent_on_reopen() {
        let (_dir, store) = embedding_store();
        store
            .with_conn("c1", |conn| {
                // The queue table, the vec0 virtual table, and its shadow
                // tables all exist after v7.
                let tables: Vec<String> = conn
                    .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")?
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                assert!(tables.iter().any(|t| t == "pending_embeddings"));
                assert!(tables.iter().any(|t| t == "node_embeddings"));
                assert!(
                    tables.iter().any(|t| t == "node_embeddings_rowids"),
                    "vec0 shadow tables must exist: {tables:?}"
                );
                // v7 is recorded.
                let applied: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = 7)",
                    [],
                    |row| row.get(0),
                )?;
                assert!(applied, "v7 must be recorded in schema_migrations");
                Ok(())
            })
            .expect("v7 assertions");

        // Reopen: migrations are a no-op, the connection re-registers
        // vec0, and the queue keeps working.
        store.open_group("c1").expect("reopen");
        assert_eq!(
            store
                .enqueue_embeddings(&[("n1".to_string(), "h1".to_string())])
                .expect("enqueue after reopen"),
            1
        );
    }

    /// Builds a v7-shaped store.db by hand: the v7 DDL (L2-metric
    /// node_embeddings + the queue), data rows, and schema_migrations
    /// stamped at 1..=7, so Store::open_group runs ONLY migration v8.
    fn v7_shaped_db(dir: &std::path::Path) {
        let group = dir.join("c1");
        std::fs::create_dir_all(&group).expect("group dir");
        let conn = Connection::open(group.join("store.db")).expect("open v7 db");
        register_sqlite_vec(&conn).expect("register vec0");
        conn.execute_batch(
            "CREATE TABLE schema_migrations (
                version    INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );",
        )
        .expect("schema_migrations");
        for version in 1..=7u32 {
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at)
                 VALUES (?1, '2026-08-16T00:00:00Z')",
                [version],
            )
            .expect("stamp version");
        }
        let (_, v7_sql) = schema::MIGRATIONS
            .iter()
            .find(|(v, _)| *v == 7)
            .expect("v7 migration entry");
        conn.execute_batch(v7_sql).expect("v7 DDL");

        // Queue rows in every status: a pending claim, a failed row,
        // and two done-journal rows (decision 66 journal).
        for (node_id, hash, status) in [
            ("n-pending", "h-p", "pending"),
            ("n-failed", "h-f", "failed"),
            ("n-done-1", "h-d1", "done"),
            ("n-done-2", "h-d2", "done"),
        ] {
            conn.execute(
                "INSERT INTO pending_embeddings
                    (node_id, content_hash, status, attempts, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 0, '2026-08-16T00:00:00Z', '2026-08-16T00:00:00Z')",
                rusqlite::params![node_id, hash, status],
            )
            .expect("queue row");
        }

        // Vectors in the L2 table that v8 must discard.
        let blob = embedding_to_blob(&test_vector(7)).expect("blob");
        conn.execute(
            "INSERT INTO node_embeddings (node_id, embedding) VALUES ('n-done-1', ?1)",
            [blob],
        )
        .expect("vec row");
    }

    #[test]
    fn migration_v8_recreates_node_embeddings_with_cosine_and_resets_the_done_journal() {
        let dir = tempfile::tempdir().expect("tempdir");
        v7_shaped_db(dir.path());

        // Open through the real path: v8 runs on top of the v7 shape.
        let store = Store::new(dir.path().to_path_buf());
        store.open_group("c1").expect("open_group runs v8");

        store
            .with_conn("c1", |conn| {
                // v8 is recorded.
                let applied: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = 8)",
                    [],
                    |row| row.get(0),
                )?;
                assert!(applied, "v8 must be recorded in schema_migrations");

                // The vec0 table was recreated: a fresh shadow set exists
                // (the DROP removed the old one — a lingering shadow
                // table would have failed the CREATE on a name
                // collision), and the pre-v8 vector is gone.
                let tables: Vec<String> = conn
                    .prepare(
                        "SELECT name FROM sqlite_master
                         WHERE type = 'table' AND name LIKE 'node_embeddings%'",
                    )?
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                assert!(tables.iter().any(|t| t == "node_embeddings"));
                assert!(
                    tables.iter().any(|t| t == "node_embeddings_rowids"),
                    "fresh vec0 shadow tables must exist: {tables:?}"
                );
                let vec_rows: i64 =
                    conn.query_row("SELECT COUNT(*) FROM node_embeddings", [], |row| row.get(0))?;
                assert_eq!(vec_rows, 0, "recreation drops the old L2 rows");

                // The done-journal was reset; pending and failed rows
                // survive and stay claimable.
                let statuses: Vec<String> = conn
                    .prepare("SELECT status FROM pending_embeddings ORDER BY node_id")?
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                assert_eq!(statuses, vec!["failed", "pending"]);
                Ok(())
            })
            .expect("v8 assertions");

        assert_eq!(store.done_embedding_hashes().expect("journal"), vec![]);
        let claimed = store.claim_embedding_batch(10).expect("claim");
        assert_eq!(
            claimed
                .iter()
                .map(|p| p.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["n-pending"],
            "the surviving pending row stays claimable"
        );

        // The fresh cosine table takes new vectors immediately.
        let v1 = test_vector(7);
        store.upsert_node_embedding("n-new", &v1).expect("upsert");
        let hits = store.knn_node_embeddings(&v1, 1).expect("knn");
        assert_eq!(hits[0].0, "n-new");
        assert!(hits[0].1.abs() < 1e-6);

        // Tip idempotency: a reopen re-runs nothing, data survives.
        store.open_group("c1").expect("reopen");
        let hits = store.knn_node_embeddings(&v1, 1).expect("knn after reopen");
        assert_eq!(hits[0].0, "n-new");
        store
            .with_conn("c1", |conn| {
                let v8_rows: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version = 8",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(v8_rows, 1, "v8 recorded exactly once");
                Ok(())
            })
            .expect("reopen assertions");
    }

    #[test]
    fn knn_node_embeddings_returns_cosine_distance() {
        let (_dir, store) = embedding_store();

        // Sparse one-hot-style vectors make the cosine math exact.
        let base = {
            let mut v = vec![0.0f32; EMBEDDING_DIM];
            v[0] = 1.0;
            v
        };
        // Same direction, 3x the magnitude: cosine distance 0. Under
        // the old L2 metric this row would sit at distance 2.0, so it
        // discriminates the v8 metric cutover.
        let scaled = {
            let mut v = vec![0.0f32; EMBEDDING_DIM];
            v[0] = 3.0;
            v
        };
        // Near-parallel ("paraphrase-like"): cos = 1/sqrt(1.0625)
        // ~= 0.970, so similarity clears the decision-73 match
        // threshold of 0.92.
        let near = {
            let mut v = vec![0.0f32; EMBEDDING_DIM];
            v[0] = 1.0;
            v[1] = 0.25;
            v
        };
        // Orthogonal: cosine similarity 0, distance exactly 1.
        let orthogonal = {
            let mut v = vec![0.0f32; EMBEDDING_DIM];
            v[1] = 1.0;
            v
        };

        store
            .upsert_node_embedding("n-orth", &orthogonal)
            .expect("orth");
        store.upsert_node_embedding("n-near", &near).expect("near");
        store
            .upsert_node_embedding("n-scaled", &scaled)
            .expect("scaled");

        let hits = store.knn_node_embeddings(&base, 3).expect("knn");
        assert_eq!(hits.len(), 3);
        let sim = |node_id: &str| {
            1.0 - hits
                .iter()
                .find(|(id, _)| id == node_id)
                .unwrap_or_else(|| panic!("{node_id} in hits"))
                .1
        };

        // Ascending distance = descending similarity, best match first.
        assert_eq!(hits[0].0, "n-scaled");
        assert_eq!(hits[1].0, "n-near");
        assert_eq!(hits[2].0, "n-orth");

        // The documented conversion: similarity = 1 - distance.
        assert!(
            (sim("n-scaled") - 1.0).abs() < 1e-6,
            "same direction, any magnitude: distance 0 under cosine \
             (L2 would give 2.0)"
        );
        assert!(
            (sim("n-near") - 1.0 / (1.0625f32).sqrt()).abs() < 1e-4,
            "near-parallel similarity ~0.970, got {}",
            sim("n-near")
        );
        assert!(sim("n-orth").abs() < 1e-6, "orthogonal: distance 1");

        // Decision-73 threshold sanity: the near-parallel vector clears
        // the 0.92 match threshold; the orthogonal one is below the
        // 0.80 candidate threshold.
        assert!(sim("n-near") > 0.92);
        assert!(sim("n-orth") < 0.80);
    }

    #[test]
    fn node_embedding_round_trips_the_stored_vector() {
        let (_dir, store) = embedding_store();
        let vector = {
            let mut v = vec![0.0f32; EMBEDDING_DIM];
            v[0] = 1.0;
            v[7] = 0.5;
            v
        };
        store.upsert_node_embedding("n1", &vector).expect("upsert");

        let stored = store.node_embedding("n1").expect("read").expect("row");
        assert_eq!(stored, vector);
        // A node without a vec row reads as None (the queue-only case).
        assert_eq!(store.node_embedding("n-missing").expect("read"), None);
    }

    #[test]
    fn embedded_node_ids_lists_vec_rows_only() {
        let (_dir, store) = embedding_store();
        let vector = vec![1.0f32; EMBEDDING_DIM];
        store
            .upsert_node_embedding("n-vec", &vector)
            .expect("upsert");
        // A queue-only id (no vec row yet) must NOT occur: the
        // merge-tool scan has no stored vector to KNN it with.
        store
            .enqueue_embeddings(&[("n-queue".to_string(), "h1".to_string())])
            .expect("enqueue");

        assert_eq!(
            store.embedded_node_ids().expect("ids"),
            vec!["n-vec".to_string()]
        );
        // The UNION helper of the reconciliation still sees both.
        let all = store.all_embedding_node_ids().expect("all");
        assert!(all.contains(&"n-vec".to_string()));
        assert!(all.contains(&"n-queue".to_string()));
    }

    #[test]
    fn enqueue_embeddings_dedups_and_requeues_on_a_new_hash() {
        let (_dir, store) = embedding_store();
        let pair = || ("n1".to_string(), "h1".to_string());

        assert_eq!(store.enqueue_embeddings(&[pair()]).expect("enqueue"), 1);
        // The same pair again inserts nothing (UNIQUE index + OR IGNORE).
        assert_eq!(store.enqueue_embeddings(&[pair()]).expect("dedup"), 0);
        // A mixed batch reports only the rows actually inserted.
        assert_eq!(
            store
                .enqueue_embeddings(&[pair(), ("n2".to_string(), "h1".to_string())])
                .expect("mixed batch"),
            1
        );
        // A changed hash for a KNOWN node is a new row (re-embed).
        assert_eq!(
            store
                .enqueue_embeddings(&[("n1".to_string(), "h2".to_string())])
                .expect("new hash"),
            1
        );
        assert_eq!(store.claim_embedding_batch(100).expect("claim").len(), 3);
    }

    #[test]
    fn embedding_queue_claim_done_failed_lifecycle() {
        let (_dir, store) = embedding_store();
        store
            .enqueue_embeddings(&[
                ("n1".to_string(), "h1".to_string()),
                ("n2".to_string(), "h2".to_string()),
                ("n3".to_string(), "h3".to_string()),
            ])
            .expect("enqueue");

        // Claim: oldest first, all three pending.
        let batch = store.claim_embedding_batch(10).expect("claim");
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].node_id, "n1");
        assert_eq!(batch[0].attempts, 0);
        // The limit truncates the batch.
        assert_eq!(store.claim_embedding_batch(2).expect("claim 2").len(), 2);

        // Done rows leave the claim set.
        store.mark_embedding_done(batch[0].id).expect("done");
        let remaining = store.claim_embedding_batch(10).expect("claim after done");
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].node_id, "n2");

        // Failures below the cap keep the row pending with attempts + 1.
        store
            .mark_embedding_attempt_failed(batch[1].id)
            .expect("fail 1");
        store
            .mark_embedding_attempt_failed(batch[1].id)
            .expect("fail 2");
        let batch2 = store
            .claim_embedding_batch(10)
            .expect("claim after 2 fails");
        let n2 = batch2
            .iter()
            .find(|p| p.node_id == "n2")
            .expect("n2 pending");
        assert_eq!(n2.attempts, 2);

        // The third failure hits the cap: the row flips to 'failed',
        // leaves the claim set, and stays inspectable.
        store
            .mark_embedding_attempt_failed(batch[1].id)
            .expect("fail 3");
        let batch3 = store.claim_embedding_batch(10).expect("claim after cap");
        assert_eq!(batch3.len(), 1);
        assert_eq!(batch3[0].node_id, "n3");
        store
            .with_conn("c1", |conn| {
                let (status, attempts): (String, i64) = conn.query_row(
                    "SELECT status, attempts FROM pending_embeddings WHERE id = ?1",
                    [batch[1].id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                assert_eq!(status, "failed");
                assert_eq!(attempts, 3);
                Ok(())
            })
            .expect("failed row stays inspectable");
    }

    #[test]
    fn node_embedding_vec_roundtrip_replace_and_delete() {
        let (_dir, store) = embedding_store();
        let v1 = test_vector(7);
        let v2 = test_vector(13);

        store.upsert_node_embedding("n1", &v1).expect("upsert n1");
        store.upsert_node_embedding("n2", &v2).expect("upsert n2");

        // KNN with an inserted vector: itself first at distance 0.
        let hits = store.knn_node_embeddings(&v1, 2).expect("knn");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].0, "n1");
        assert!(hits[0].1.abs() < 1e-6, "distance ~0, got {}", hits[0].1);

        // A wrong-dimension vector is rejected before hitting sqlite.
        assert!(store.upsert_node_embedding("nx", &[1.0; 8]).is_err());
        assert!(store.knn_node_embeddings(&[1.0; 8], 1).is_err());

        // Replace overwrites: n1 now holds the new vector v3 and ranks
        // first for a v3 query; the old v1 row is gone, so a v1 query
        // has no exact match left.
        let v3 = test_vector(29);
        store.upsert_node_embedding("n1", &v3).expect("replace n1");
        let hits = store
            .knn_node_embeddings(&v3, 2)
            .expect("knn after replace");
        assert_eq!(hits[0].0, "n1");
        assert!(hits[0].1.abs() < 1e-6, "replaced vector matches at ~0");
        let hits = store.knn_node_embeddings(&v1, 2).expect("knn stale query");
        assert!(
            hits[0].1 > 0.0,
            "the replaced-away vector must have no exact match left"
        );

        // The tombstone primitive clears the vec row AND the queue rows.
        store
            .enqueue_embeddings(&[("n1".to_string(), "h1".to_string())])
            .expect("enqueue n1");
        store.delete_node_embedding_rows("n1").expect("tombstone");
        assert_eq!(
            store.all_embedding_node_ids().expect("node ids"),
            vec!["n2".to_string()]
        );
        let hits = store.knn_node_embeddings(&v2, 5).expect("knn after delete");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "n2");
    }

    #[test]
    fn done_embedding_hashes_returns_the_latest_done_row_per_node() {
        let (_dir, store) = embedding_store();
        store
            .enqueue_embeddings(&[
                ("n1".to_string(), "h-old".to_string()),
                ("n1".to_string(), "h-new".to_string()),
                ("n2".to_string(), "h2".to_string()),
                ("n3".to_string(), "h3".to_string()),
            ])
            .expect("enqueue");
        let batch = store.claim_embedding_batch(10).expect("claim");
        // n1 embedded twice (successive contents), n2 done, n3 pending.
        for row in batch.iter().filter(|p| p.node_id != "n3") {
            store.mark_embedding_done(row.id).expect("done");
        }

        let done = store.done_embedding_hashes().expect("done hashes");
        assert_eq!(
            done,
            vec![
                ("n1".to_string(), "h-new".to_string()),
                ("n2".to_string(), "h2".to_string())
            ],
            "only the latest done hash per node; pending n3 excluded"
        );
    }

    #[test]
    fn record_node_embedded_journals_the_actual_hash_and_dedups() {
        let (_dir, store) = embedding_store();

        // The journal entry appears in done_embedding_hashes without a
        // claim/mark-done round trip.
        store
            .record_node_embedded("n1", "h-stored")
            .expect("record");
        assert_eq!(
            store.done_embedding_hashes().expect("done hashes"),
            vec![("n1".to_string(), "h-stored".to_string())]
        );

        // A duplicate record is a no-op (INSERT OR IGNORE through the
        // dedup index): still exactly one done row for the node.
        store
            .record_node_embedded("n1", "h-stored")
            .expect("duplicate record");
        assert_eq!(
            store.done_embedding_hashes().expect("done hashes"),
            vec![("n1".to_string(), "h-stored".to_string())]
        );

        // Journal rows are status='done': they never enter the claim
        // set.
        assert!(store.claim_embedding_batch(10).expect("claim").is_empty());
    }

    #[test]
    fn sqlite_vec_is_registered_on_both_open_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("c1").join("store.db");
        let v1 = test_vector(7);

        // First Store instance writes vec rows, then closes (drop).
        {
            let store = Store::new(dir.path().to_path_buf());
            store.open_group("c1").expect("open");
            store.upsert_node_embedding("n1", &v1).expect("upsert");
        }

        // Reopen via open_group on a NEW Store (fresh connection):
        // registration ran, KNN works.
        {
            let store = Store::new(dir.path().to_path_buf());
            store.open_group("c1").expect("reopen");
            let hits = store
                .knn_node_embeddings(&v1, 1)
                .expect("knn via open_group");
            assert_eq!(hits[0].0, "n1");
            assert!(hits[0].1.abs() < 1e-6);
        }

        // Reopen via the read-only status path: registration ran there
        // too, so vec0 resolves on the read-only connection.
        let conn = open_read_only(&db_path).expect("read-only open");
        let (node_id, distance): (String, f32) = conn
            .query_row(
                "SELECT node_id, distance FROM node_embeddings
                 WHERE embedding MATCH ?1 AND k = ?2",
                rusqlite::params![embedding_to_blob(&v1).expect("blob"), 1i64],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("knn via read-only path");
        assert_eq!(node_id, "n1");
        assert!(distance.abs() < 1e-6);
    }

    #[test]
    fn embedding_helpers_reject_an_ambiguous_store() {
        // The chat_id-less embedding helpers require exactly one open
        // group; zero or several groups is an operator-visible error,
        // never a silent write to the wrong group (Rule P5).
        let (_dir, store) = temp_store();
        let err = store
            .enqueue_embeddings(&[("n1".to_string(), "h1".to_string())])
            .expect_err("no open group must fail");
        assert!(matches!(err, StoreError::AmbiguousGroup(0)));

        store.open_group("c1").expect("open c1");
        store.open_group("c2").expect("open c2");
        let err = store
            .claim_embedding_batch(1)
            .expect_err("two open groups must fail");
        assert!(matches!(err, StoreError::AmbiguousGroup(2)));
    }

    // --- merge_audit (migration v9, decision 74) ---------------------

    /// A full 'same'-verdict audit row, snapshot and counters included.
    /// `id`, `rolled_back`, and `created_at` are placeholders — the
    /// insert ignores them (see MergeAuditRow).
    fn sample_merge_audit() -> MergeAuditRow {
        MergeAuditRow {
            id: 0,
            loser_id: "n-loser".to_string(),
            survivor_id: "n-survivor".to_string(),
            loser_kind: "Person".to_string(),
            loser_name: "yux".to_string(),
            loser_description: Some("the other yux node".to_string()),
            verdict: "same".to_string(),
            reason: "same display name, same description".to_string(),
            confirmed_by: "llm:k3-256k".to_string(),
            edges_moved: 4,
            self_loops_dropped: 1,
            edges_deduped: 2,
            snapshot: Some(
                r#"{"node":{"id":"n-loser"},"edges":[{"id":"e1"}],"created_edge_ids":["e9"]}"#
                    .to_string(),
            ),
            rolled_back: false,
            created_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn migration_v9_creates_merge_audit_and_is_idempotent_on_reopen() {
        let (dir, store) = embedding_store();
        store
            .with_conn("c1", |conn| {
                // The audit table exists after v9.
                let exists: bool = conn.query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master
                         WHERE type = 'table' AND name = 'merge_audit'
                     )",
                    [],
                    |row| row.get(0),
                )?;
                assert!(exists, "merge_audit must exist");
                // v9 is recorded.
                let applied: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = 9)",
                    [],
                    |row| row.get(0),
                )?;
                assert!(applied, "v9 must be recorded in schema_migrations");
                Ok(())
            })
            .expect("v9 assertions");

        // Reopen through a NEW Store instance: migrations are a no-op
        // and v9 stays recorded exactly once.
        let store2 = Store::new(dir.path().to_path_buf());
        store2.open_group("c1").expect("reopen");
        store2
            .with_conn("c1", |conn| {
                let v9_rows: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version = 9",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(v9_rows, 1, "v9 recorded exactly once");
                Ok(())
            })
            .expect("reopen assertions");
    }

    #[test]
    fn merge_audit_insert_and_list_round_trip() {
        let (_dir, store) = embedding_store();
        let before = OffsetDateTime::now_utc();

        // A full 'same' merge row: snapshot JSON blob and all counters.
        let merge = sample_merge_audit();
        let merge_id = store.insert_merge_audit(&merge).expect("insert merge");
        assert!(merge_id > 0);

        // A 'different' verdict is audited too (specs.md Section 5.2:
        // one row per merge-tool action), with a NULL snapshot and zero
        // counters.
        let different = MergeAuditRow {
            loser_id: "n-a".to_string(),
            survivor_id: "n-b".to_string(),
            loser_kind: "Concept".to_string(),
            loser_name: "tea".to_string(),
            loser_description: None,
            verdict: "different".to_string(),
            reason: "same word, unrelated senses".to_string(),
            confirmed_by: "operator".to_string(),
            edges_moved: 0,
            self_loops_dropped: 0,
            edges_deduped: 0,
            snapshot: None,
            ..merge.clone()
        };
        let different_id = store
            .insert_merge_audit(&different)
            .expect("insert different");

        // Ordered by id, both rows come back intact.
        let rows = store.list_merge_audit().expect("list");
        assert_eq!(rows.len(), 2);
        let merged = &rows[0];
        assert_eq!(merged.id, merge_id);
        assert_eq!(merged.loser_id, merge.loser_id);
        assert_eq!(merged.survivor_id, merge.survivor_id);
        assert_eq!(merged.loser_kind, merge.loser_kind);
        assert_eq!(merged.loser_name, merge.loser_name);
        assert_eq!(merged.loser_description, merge.loser_description);
        assert_eq!(merged.verdict, "same");
        assert_eq!(merged.reason, merge.reason);
        assert_eq!(merged.confirmed_by, merge.confirmed_by);
        assert_eq!(merged.edges_moved, 4);
        assert_eq!(merged.self_loops_dropped, 1);
        assert_eq!(merged.edges_deduped, 2);
        assert_eq!(merged.snapshot, merge.snapshot);
        assert!(!merged.rolled_back, "rolled_back defaults to 0 on insert");
        assert!(
            merged.created_at >= before,
            "created_at is stamped at insert time"
        );

        let skipped = &rows[1];
        assert_eq!(skipped.id, different_id);
        assert_eq!(skipped.verdict, "different");
        assert_eq!(skipped.confirmed_by, "operator");
        assert_eq!(skipped.snapshot, None);
        assert_eq!(skipped.edges_moved, 0);
        assert!(!skipped.rolled_back);
    }

    #[test]
    fn mark_merge_rolled_back_sets_the_flag_and_errors_on_a_missing_id() {
        let (_dir, store) = embedding_store();
        let id = store
            .insert_merge_audit(&sample_merge_audit())
            .expect("insert");

        store.mark_merge_rolled_back(id).expect("mark rolled back");
        let rows = store.list_merge_audit().expect("list");
        assert!(rows[0].rolled_back, "the flag is set");
        // The snapshot survives the flag flip: rollback does not erase
        // the audit trail.
        assert!(rows[0].snapshot.is_some());

        // A missing audit id is a loud error, never a silent no-op.
        let err = store
            .mark_merge_rolled_back(id + 1000)
            .expect_err("missing id must fail");
        assert!(
            matches!(err, StoreError::InvalidValue { .. }),
            "expected InvalidValue, got {err}"
        );
    }

    #[test]
    fn merge_audit_rejects_an_unknown_verdict() {
        let (_dir, store) = embedding_store();
        let bad = MergeAuditRow {
            verdict: "maybe".to_string(),
            ..sample_merge_audit()
        };
        let err = store
            .insert_merge_audit(&bad)
            .expect_err("a verdict outside the CHECK set must fail");
        assert!(
            matches!(
                &err,
                StoreError::Sqlite(rusqlite::Error::SqliteFailure(e, _))
                    if e.code == rusqlite::ErrorCode::ConstraintViolation
            ),
            "expected a CHECK constraint violation, got {err}"
        );
        // Nothing was written.
        assert!(store.list_merge_audit().expect("list").is_empty());
    }
}
