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
    let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    // Brief SQLITE_BUSY windows exist while the bot shuts down or
    // recovers; wait up to 2 s before failing.
    conn.busy_timeout(Duration::from_secs(2))?;

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
        assert_eq!(count, 6);
        let versions: Vec<u32> = {
            let mut stmt = conn
                .prepare("SELECT version FROM schema_migrations ORDER BY version")
                .expect("prepare versions");
            stmt.query_map([], |row| row.get(0))
                .expect("query versions")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect versions")
        };
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6]);

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
    }

    #[test]
    fn migration_v6_upgrades_a_v5_database_in_place() {
        // A database created by the previous release carries migrations
        // v1-v5 and live data. Opening it with this build must apply only
        // v6 (index-only: no data touched), keep the data, and be a no-op
        // on reopen.
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

        // The upgrade open applies v6. A reopen is a no-op.
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

        // Migration bookkeeping: exactly one version was added.
        let versions: Vec<u32> = {
            let mut stmt = conn
                .prepare("SELECT version FROM schema_migrations ORDER BY version")
                .expect("prepare versions");
            stmt.query_map([], |row| row.get(0))
                .expect("query versions")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect versions")
        };
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6]);
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

        // Migration bookkeeping: the upgrade applied v5 and v6.
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
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6]);
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
}
