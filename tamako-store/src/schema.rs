//! Embedded schema migrations for store.db. Refer to specs.md Section 5.2.
//!
//! There is no migration framework. MIGRATIONS is an ordered list of
//! (version, sql). The runner applies each pending version in one
//! transaction and records the version in `schema_migrations`. Opening an
//! existing database a second time is a no-op.
//!
//! Timestamp cutover note (specs.md Section 4.2 backfill note): edit rows
//! persisted before the K3 fix (the `normalize_edited_message` entry
//! point) carry the ORIGINAL send date in their `timestamp` column; edit
//! rows persisted after the fix carry the edit date. The raw log is
//! append-only (Rule P1), so no migration rewrites the old rows. Readers
//! of the log see mixed edit-timestamp semantics across the cutover.

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
    (
        4,
        "\
-- The XML context rendering (msg 标签) shows the sender's username beside
-- the display name. Nullable and purely additive: a mid-soak restart
-- loses nothing, and rows written before this migration read as NULL.
ALTER TABLE messages ADD COLUMN sender_username TEXT;
",
    ),
    (
        5,
        "\
-- Segmented context summarization: one row per digested chunk. The row
-- replaces the raw-log range (first_msg_id, last_msg_id] that Rule C3
-- removed from the live context. An LLM-written summary is not derivable
-- from persisted state (Rule P1), so its text is persisted at creation
-- time. The context keeps the TWO newest summaries for the rebuild.
--
-- Retention: rotated-out summaries are NOT pruned. They stay in the
-- table for forensics. The table grows one small row per digest. This
-- is deliberate.
CREATE TABLE context_summaries (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    first_msg_id INTEGER NOT NULL,
    last_msg_id  INTEGER NOT NULL,
    content      TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

-- The range of the digested chunk is the natural dedup key. Rule P1
-- replay idempotency: a re-run of a digest-completion handler finds the
-- existing row and skips the LLM call.
CREATE UNIQUE INDEX context_summaries_dedup
    ON context_summaries (first_msg_id, last_msg_id);
",
    ),
    (
        6,
        "\
-- H1 edit-dedup collapse fix (decision 65). The v1 messages_dedup key
-- (platform_msg_id, direction, event_type, timestamp) collapsed every
-- same-second edit of one message into the first persisted edit: raw-log
-- loss under Rule P1. Adding `text` to the key distinguishes edits.
-- Inbound redelivery is byte-identical, so message dedup still holds.
-- Same-second IDENTICAL-text edits still collapse; that case is
-- semantically harmless (nothing changed) and documented as such.
--
-- This migration is INDEX-ONLY: it drops and recreates an index. No
-- table rebuild, no data touched, nothing is lost.
--
-- Query-plan audit (store.rs): every messages-table query that filters
-- platform_msg_id (find_reply_target, find_sender_by_platform_msg_id,
-- find_latest_message_by_platform_msg_id) uses `platform_msg_id = ?`,
-- which matches the new index's leftmost column. No query depends on
-- the old index shape.
DROP INDEX messages_dedup;

CREATE UNIQUE INDEX messages_dedup
    ON messages (platform_msg_id, direction, event_type, timestamp, text);
",
    ),
    (
        7,
        "\
-- Embedding sidecar (decision 66). The digest transaction writes graph
-- rows and ENQUEUES (node_id, content_hash) here; a rate-limited
-- background worker drains the queue and writes the vectors. An
-- embeddings-API outage grows the queue but never blocks a digest.
-- Rows are never deleted by the worker: 'failed' rows (attempts cap
-- reached) stay inspectable. Timestamps are RFC 3339 TEXT written from
-- the Rust side, the house idiom.
CREATE TABLE pending_embeddings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'done', 'failed')),
    attempts     INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

-- Idempotent enqueue: the same (node, content) pair queued twice is one
-- row. A changed description hash enqueues a NEW row for the same node,
-- so a re-embed follows every content change.
CREATE UNIQUE INDEX pending_embeddings_dedup
    ON pending_embeddings (node_id, content_hash);

-- The vectors themselves. vec0 (sqlite-vec 0.1.9) virtual table; the
-- TEXT primary key is the graph node id (vec0 backs it with an int64
-- rowid + a text-unique shadow column). The dimension is fixed at
-- creation and pinned to 4096 by decision 66. chunk_size = 128 gives
-- 2 MiB preallocation granules (128 * 4096 * 4 bytes) instead of the
-- 16 MiB default — the scale-appropriate choice for per-group node
-- counts in the low thousands. Revisitable by recreating the table:
-- embeddings are recomputable from node content.
--
-- The vec0 module must be registered on the connection BEFORE this
-- migration runs (register_sqlite_vec in Store::open_group);
-- registration is per-connection and never persisted.
CREATE VIRTUAL TABLE node_embeddings USING vec0(
    node_id TEXT PRIMARY KEY,
    embedding float[4096],
    chunk_size=128
);
",
    ),
    (
        8,
        "\
-- Metric cutover (decision 73). The v7 node_embeddings table shipped
-- with the vec0 DEFAULT distance metric (L2), but the decision-73
-- vector pre-screen thresholds (vector_match_threshold 0.92,
-- vector_candidate_threshold 0.80) are COSINE similarities. The pinned
-- sqlite-vec 0.1.9 supports cosine as a per-vector-column option
-- (sqlite-vec.c: parsing in the float[N] column clause, KNN dispatch
-- to distance_cosine_float, which returns 1 - cosine_similarity), so
-- the table is dropped and recreated with distance_metric=cosine. No
-- L2<->cosine conversion hack: embeddings are derived data,
-- recomputable from node content, so dropping them is free. The DROP
-- removes the vec0 shadow tables; the CREATE rebuilds a fresh set.
DROP TABLE node_embeddings;

-- Same shape as v7 (TEXT primary key, 4096 dims, chunk_size = 128)
-- plus the cosine metric on the vector column. Registration of the
-- vec0 module still happens before migrations in Store::open_group.
CREATE VIRTUAL TABLE node_embeddings USING vec0(
    node_id TEXT PRIMARY KEY,
    embedding float[4096] distance_metric=cosine,
    chunk_size=128
);

-- Done-journal reset (decision 66 reconciliation contract): the
-- status='done' rows of pending_embeddings are the journal telling
-- startup reconciliation which (node, content_hash) pairs already live
-- in node_embeddings. The recreation above just invalidated every one
-- of those claims, so they are deleted. The next startup
-- reconciliation diffs the graph against the now-empty journal and
-- re-enqueues every node, producing a full re-embed into the fresh
-- cosine table. 'pending' and 'failed' rows SURVIVE: they remain
-- claimable and will embed into the new table.
DELETE FROM pending_embeddings WHERE status = 'done';
",
    ),
    (
        9,
        "\
-- Merge audit (decision 74, specs.md Section 5.2, graph-spec Section
-- 7.7). One append-only row per merge-tool action: a 'same' verdict
-- merges, 'related' links the pair via also_known_as, 'different'
-- skips — ALL THREE are audited (the row is the record of the LLM or
-- operator confirmation). Only a 'same' merge carries a snapshot (JSON:
-- the loser node and its original edges, plus the created edge
-- identifiers); non-merge verdicts store NULL. The rolled_back flag
-- flips when --merge-rollback restores the loser. Timestamps are
-- RFC 3339 TEXT written from the Rust side, the house idiom.
--
-- No indexes beyond the primary key: audit reads are rare (rollback
-- looks up one row by id; a possible future inspect mode scans the
-- whole table), and the table grows one row per operator-reviewed
-- action. A full scan is fine.
CREATE TABLE merge_audit (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    loser_id           TEXT NOT NULL,
    survivor_id        TEXT NOT NULL,
    loser_kind         TEXT NOT NULL,
    loser_name         TEXT NOT NULL,
    loser_description  TEXT,
    verdict            TEXT NOT NULL
                       CHECK (verdict IN ('same', 'related', 'different')),
    reason             TEXT NOT NULL,
    confirmed_by       TEXT NOT NULL,
    edges_moved        INTEGER NOT NULL DEFAULT 0,
    self_loops_dropped INTEGER NOT NULL DEFAULT 0,
    edges_deduped      INTEGER NOT NULL DEFAULT 0,
    snapshot           TEXT,
    rolled_back        INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL
);
",
    ),
    (
        10,
        "\
-- Deep-recall edge-description sidecar (decision 76, ruling 76c). One
-- row per graph edge: the opaque edge id (tamako-memory's EdgeId JSON
-- encoding of source/relationship/target/valid_at — the graph EDGE
-- table has no id column, so the store treats edge ids as opaque TEXT)
-- plus the edge description text. Written post-commit at digest time
-- and repaired by the startup reconciliation pass (extended to edges).
--
-- WHY NOT FTS5 (76c): at our edge counts (thousands) a trigram index
-- buys nothing, and the FTS5 trigram tokenizer cannot match CJK terms
-- shorter than three characters — a fatal hole for two-character
-- Chinese words like 咖啡, the deployment language. The table is
-- scanned with parameterized LIKE and ESCAPE '\\' (see
-- Store::search_edge_texts); FTS5-trigram is the documented upgrade
-- path when edge counts justify it.
--
-- No timestamps: an edge_texts row is derived data, re-writable from
-- the graph edge at any time, unlike the audit/queue rows that carry
-- the house RFC 3339 stamp.
CREATE TABLE edge_texts (
    edge_id   TEXT PRIMARY KEY,
    edge_text TEXT NOT NULL
);
",
    ),
    (
        11,
        "\
-- Model switch (decision 81): the default embedding model becomes
-- google/gemini-embedding-2 at its NATIVE 3072 dimensions (Vertex,
-- ZDR). 3072 is the model's native dimension (the top of the
-- Matryoshka ladder — the full vector, not a truncation), so the
-- OpenAI-compatible `dimensions` request parameter is behaviorally
-- irrelevant and the hard length pin (EMBEDDING_DIM in store.rs) is
-- the guard. Embeddings are derived data, recomputable from node
-- content, so the drop is free — the same drop-and-recreate template
-- as v8 (metric cutover), now for the dimension change.
DROP TABLE node_embeddings;

-- Same shape as v8 (TEXT primary key, cosine metric, chunk_size =
-- 128) at the new dimension. chunk_size = 128 now gives 1.5 MiB
-- preallocation granules (128 * 3072 * 4 bytes). The vec0 module
-- registration still happens before migrations in Store::open_group.
CREATE VIRTUAL TABLE node_embeddings USING vec0(
    node_id TEXT PRIMARY KEY,
    embedding float[3072] distance_metric=cosine,
    chunk_size=128
);

-- Done-journal reset (identical contract to v8): the recreated
-- table invalidates every 'done' claim of pending_embeddings, so
-- they are deleted and the next startup reconciliation re-enqueues
-- every node for a full re-embed. 'pending' and 'failed' rows
-- SURVIVE and stay claimable.
DELETE FROM pending_embeddings WHERE status = 'done';
",
    ),
    (
        12,
        "\
-- Related-pair side table (decision 83, specs.md Section 5.2,
-- graph-spec Section 7.7 step 6). The merge tool's 'related' verdict
-- no longer creates an also_known_as edge: alias edges BIND in entity
-- resolution (graph-spec Section 7.4 steps 2/5), so a merely-related
-- pair could merge by the back door — a semantic error. The pair
-- lands here instead as write-only 'dotted edge' state, one row per
-- 'related' verdict, awaiting a future digest-side promotion pass
-- that grounds a real relationship edge from full batch text and
-- flips the row to 'promoted' (operator dismissal flips 'dismissed').
--
-- The status column ships NOW (operator ruling): the promotion
-- pass's first query is WHERE status='pending'; adding the column
-- later would force a migration right before that pass for no
-- benefit. The pair is UNORDERED, normalized node_a_id < node_b_id
-- (the same discipline as the merge candidate scan) with
-- UNIQUE(node_a_id, node_b_id); writes are INSERT OR IGNORE,
-- first-write-wins — a re-confirmed pair keeps its original row.
-- Timestamps are RFC 3339 TEXT written from the Rust side, the house
-- idiom. A 'same' merge rewrites the loser's rows to the survivor in
-- the same apply (Store::rewrite_related_pairs_loser, decision 83(c)).
--
-- NO read path touches this table — not recall, not resolution, not
-- status (decision 83(d)) — so no indexes beyond the primary key and
-- the UNIQUE pair constraint; the promotion pass's status='pending'
-- scan is rare and the table grows one row per reviewed verdict.
CREATE TABLE related_pairs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    node_a_id    TEXT NOT NULL,
    node_b_id    TEXT NOT NULL,
    reason       TEXT NOT NULL,
    confirmed_by TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','promoted','dismissed')),
    created_at   TEXT NOT NULL,
    UNIQUE(node_a_id, node_b_id)
);
",
    ),
];

/// Applies all pending migrations. Each version runs in one transaction.
///
/// Downgrade guard (decision 77, S6-F9): a database whose recorded schema
/// version EXCEEDS the binary's known maximum was written by a newer
/// tamako. Opening it with this binary would misread newer rows, so the
/// runner refuses with [`StoreError::SchemaFromTheFuture`] naming both
/// versions. A fresh (zero-version) database still migrates to the tip.
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

    let known = MIGRATIONS.last().map(|(version, _)| *version).unwrap_or(0);
    if let Some(&found) = applied.iter().max() {
        if found > known {
            return Err(StoreError::SchemaFromTheFuture { found, known });
        }
    }

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
