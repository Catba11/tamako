//! Embedded schema migrations for store.db. Refer to specs.md Section 5.2.
//!
//! There is no migration framework. MIGRATIONS is an ordered list of
//! (version, sql). The runner applies each pending version in one
//! transaction and records the version in `schema_migrations`. Opening an
//! existing database a second time is a no-op.

use rusqlite::Connection;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::error::{Result, StoreError};

/// Ordered list of schema migrations. Append only. Never edit an entry
/// after it has shipped.
pub const MIGRATIONS: &[(u32, &str)] = &[
    (
        1,
        "\
CREATE TABLE messages (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_msg_id         TEXT NOT NULL,
    direction               TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    event_type              TEXT NOT NULL CHECK (event_type IN ('message', 'edit')),
    timestamp               TEXT NOT NULL,
    sender_id               TEXT NOT NULL,
    sender_display_name     TEXT NOT NULL,
    text                    TEXT NOT NULL,
    reply_to_platform_msg_id TEXT,
    mentions_bot            INTEGER NOT NULL DEFAULT 0,
    is_reply_to_bot         INTEGER NOT NULL DEFAULT 0
);

-- Rule P1: the raw log is the source of truth. The unique index makes the
-- insert of a message idempotent. Refer to AGENT.md Section 6.2.
CREATE UNIQUE INDEX messages_dedup
    ON messages (platform_msg_id, direction, event_type, timestamp);

-- Session-state key-value table. Refer to specs.md Sections 5.2 and 6.1.
CREATE TABLE state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Dedup set for injected recall items. Refer to specs.md Section 9.3.
-- This table exists from day one. Refer to the anti-defer list in
-- dev-roadmap.md Section 7.
CREATE TABLE injected_memories (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    edge_id            TEXT NOT NULL,
    injection_position INTEGER NOT NULL,
    range_tag          TEXT NOT NULL,
    created_at         TEXT NOT NULL
);

-- Dead-letter table for failed digest batches. Refer to specs.md
-- Section 10.3. A failed batch never blocks later batches.
CREATE TABLE dead_letter (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id       TEXT NOT NULL,
    batch_skeleton TEXT NOT NULL,
    error          TEXT NOT NULL,
    created_at     TEXT NOT NULL
);
",
    ),
    (
        2,
        "\
-- The live context rebuild (Rule P1, specs.md Section 7.1) must restore
-- injection items bit-identically after a restart. The rendered injection
-- text is not derivable from the graph, so it is persisted with the dedup
-- row.
ALTER TABLE injected_memories ADD COLUMN content TEXT NOT NULL DEFAULT '';
",
    ),
    (
        3,
        "\
-- The reactions table of specs.md Section 5.2. One row per reaction
-- event on a group message. Reaction data is not recoverable later, so
-- collection starts at intake time in Phase 1. The Phase 2 warmup
-- backoff consumes this table.
CREATE TABLE reactions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_msg_id TEXT NOT NULL,
    reactor_user_id TEXT,
    anonymous       INTEGER NOT NULL DEFAULT 0,
    aggregated      INTEGER NOT NULL DEFAULT 0,
    old_emojis      TEXT NOT NULL DEFAULT '[]',
    new_emojis      TEXT NOT NULL DEFAULT '[]',
    timestamp       TEXT NOT NULL
);

-- Idempotent intake (AGENT.md Section 6.2): a reconnect redelivers the
-- same reaction update. COALESCE normalizes the NULL reactor (anonymous
-- aggregated updates) into the dedup key.
CREATE UNIQUE INDEX reactions_dedup
    ON reactions (platform_msg_id, COALESCE(reactor_user_id, ''),
                  old_emojis, new_emojis, timestamp);
",
    ),
];

/// Applies all pending migrations. Each version runs in one transaction.
pub fn run_migrations(conn: &mut Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );",
    )?;

    let mut stmt = conn.prepare("SELECT version FROM schema_migrations")?;
    let applied: Vec<u32> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    drop(stmt);

    for (version, sql) in MIGRATIONS {
        if applied.contains(version) {
            continue;
        }
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
            rusqlite::params![version, now_rfc3339()?],
        )?;
        tx.commit()?;
    }
    Ok(())
}

/// Returns the current UTC time as an RFC 3339 string. Timestamps are
/// stored as TEXT in RFC 3339 format. Refer to specs.md Section 5.2.
pub(crate) fn now_rfc3339() -> Result<String> {
    format_rfc3339(OffsetDateTime::now_utc())
}

/// Formats a timestamp as an RFC 3339 string.
pub(crate) fn format_rfc3339(ts: OffsetDateTime) -> Result<String> {
    // A formatting failure is a storage failure. It is mapped into the
    // Sqlite variant because StoreError has no variant for it.
    ts.format(&Rfc3339)
        .map_err(|e| StoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(e))))
}

/// Parses an RFC 3339 string from the database. A corrupt stored value
/// becomes a sqlite conversion error.
pub(crate) fn parse_rfc3339(s: &str) -> rusqlite::Result<OffsetDateTime> {
    OffsetDateTime::parse(s, &Rfc3339).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}
