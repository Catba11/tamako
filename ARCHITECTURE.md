# ARCHITECTURE.md — Tamako, as built

This document describes the crate-level architecture as it exists in the
repository today. It documents what is built and tested, not what is
planned. The governing documents (`specs.md`,
`proposed-graph-database-specs.md`, `dev-roadmap.md`) define intent; this
document records the current implementation.

## 1. Workspace layout

A Cargo workspace at the repository root with six crates. Shared
dependency versions are pinned in `[workspace.dependencies]`.

| Crate | Role | Tests |
|---|---|---|
| `tamako` | Binary. CLI, wiring, the `--replay` demo. | 1 (integration) |
| `tamako-core` | Normalized events and actions, the adapter trait, configuration, trigger scheduling, session state, the per-group actor. | 26 |
| `tamako-store` | `store.db`: SQLite access, migrations, the raw message log, the session-state table, `injected_memories`, `dead_letter`. | 11 |
| `tamako-memory` | The `MemoryBackend` trait, the `lbug` implementation, deterministic identifiers. | 10 |
| `tamako-persona` | The global persona configuration and the preamble rendering layer. | 7 |
| `tamako-adapter-mock` | The mock platform adapter and the replay fixture. | 6 |

Total: 61 tests. Build, test, clippy (`-D warnings`), and fmt are clean.

## 2. Dependency direction

```
tamako ──▶ tamako-core ──▶ tamako-store
   │           │    └─────▶ tamako-memory
   │           └──────────▶ tamako-persona
   ├──▶ tamako-store  ──── (all lower crates are independent)
   ├──▶ tamako-memory
   ├──▶ tamako-persona
   └──▶ tamako-adapter-mock ──▶ tamako-core (types only)
```

- The binary depends on all crates and does the wiring.
- `tamako-core` depends on the storage and persona crates. No crate
  depends on `tamako-core` except adapters, and adapters use the
  normalized types only (Rule A1, Rule P7).
- `tamako-store`, `tamako-memory`, and `tamako-persona` do not depend on
  each other. There are no cycles.

## 3. The adapter boundary

`tamako-core::adapter::PlatformAdapter` is the platform contract
(specs.md Section 4):

- Inbound: `next_event()` returns normalized `InboundEvent` values
  (`Message`, `EditedMessage`, `Reaction`, `MemberJoin`, `MemberLeave`).
  `None` ends the stream.
- Outbound: `execute()` takes `OutboundAction` values (`SendText`,
  `SendMedia`, `React`).
- A `NormalizedMessage` carries the platform message id, an RFC 3339
  timestamp, the sender id and display name, the text, the reply-to id,
  and two mention flags (`mentions_bot`, `is_reply_to_bot`). Rule A4
  applies. No platform type crosses this boundary.

The mock adapter replays a JSON fixture
(`tamako-adapter-mock/fixtures/replay_chat.json`, 14 events, one group)
and records outbound actions for test assertions. It is the Phase 0 demo
and the integration-test harness.

## 4. The per-group actor

`tamako-core::actor` implements the actor model of specs.md Section 6.

- One tokio task and one mpsc inbox per group (`ActorCommand`:
  `Inbound`, `Tick`, `Snapshot`, `Shutdown`). All events enter one FIFO
  inbox (Section 6.1, rule 1).
- Startup runs inside the spawned task: open the group store, ensure the
  graph schema, load and decode the session state. The handle returns
  immediately; `snapshot()` acts as a FIFO barrier and `shutdown()`
  surfaces startup errors through the join handle.
- Message intake order is fixed (Rule P1): first persist the raw-log row
  through `spawn_blocking`, then update the in-memory session, then
  persist the session, then evaluate triggers. A duplicate delivery
  yields one log row; the wake counter counts each delivery (specs.md
  Section 8.1).
- Edited messages append a new log row with `event_type = 'edit'`. An
  edit never retracts (specs.md Section 15, open item 4).
- Trigger evaluation is scheduling only. A mention or a reply logs a
  forced-wake stub; a fired wake or digest condition logs a stub and
  resets the scheduler. The procedures themselves are Phase 1.
- The wake scheduler (`tamako-core::trigger`) is pure logic: fire on the
  first of message count or jittered interval, subject to the floor
  (specs.md Section 8.3). The jittered interval is normalized to whole
  milliseconds so the persisted encoding is lossless. The digest
  thresholds of Section 8.2 are a pure function over tail statistics.

All synchronous storage calls run inside `tokio::task::spawn_blocking`
(AGENT.md Section 6.2).

## 5. Storage layout per group

Rule P5: one directory per group at `{data_root}/{chat_id}/`.

| File | Content |
|---|---|
| `store.db` | SQLite, WAL mode, `synchronous=NORMAL`. Tables: `messages` (raw log, source of truth), `state` (session KV plus counters), `injected_memories`, `dead_letter`, `schema_migrations`. |
| `memory.lbug` | LadybugDB graph. One `Node` table, one `EDGE` rel table (Section 6.1 of the database specification). |

`tamako-store::Store` is rooted at the data root and takes a `chat_id`
in every API. Connections open lazily and are cached in a
`Mutex<HashMap>`. Migrations are an ordered constant array applied
through a minimal runner; the raw-log insert is idempotent
(`INSERT OR IGNORE` on `(platform_msg_id, direction, event_type,
timestamp)`). `chat_id` values with path separators or `..` are
rejected.

The session state (`tamako-core::session`) encodes to state-table keys
(`last_digest_boundary_msg_id`, `muted_flag`, `consecutive_bot_msgs`,
`wake_*`). The actor persists it after every mutation and rebuilds it on
restart. The monologue lock mechanism (`record_human_message`,
`record_bot_message`) exists; live bot speech arrives in Phase 1.

## 6. The MemoryBackend design

`tamako-memory::backend::MemoryBackend` is the graph contract:
`ensure_schema`, `upsert_batch`, `checkpoint`, `close`. The methods are
declared in desugared form so the returned futures are `Send`;
implementations write plain `async fn`.

`LbugBackend` implements the trait over the official LadybugDB Rust
crate. Key decisions:

- **Version pin `lbug = "0.18"`.** The on-disk storage format v42
  (magic `LBUG`) starts at LadybugDB 0.18.0 and matches the pinned
  Python package `ladybug>=0.16.0,<=0.18.2` of the database
  specification. Version 0.19 can change the storage version; verify
  before an upgrade. Refer to `docs/adr-0001-ladybugdb-binding.md`.
- **The cache holds `Database` handles, not `Connection` handles.** An
  lbug `Connection<'a>` borrows its `Database`, so a connection cache
  would be self-referential. A fresh `Connection` opens inside each
  blocking closure; connection creation is cheap. The database handle,
  the resource Section 5.2 of the database specification cares about,
  is cached per group.
- **Writes are transactional and idempotent.** `upsert_batch` runs
  `MERGE` for nodes and edges under the deterministic UUID5 identifiers
  of Section 7.1 (`tamako-memory::identifiers`, NFKC normalization), in
  one transaction with `CHECKPOINT` at the end. The same batch applied
  twice yields the same graph. Timestamps travel as native `TIMESTAMP`
  parameters; all user data goes through `$param` parameters.
- Every driver call runs inside `spawn_blocking` (Section 5.2, rule 3).

## 7. The binary

`tamako --replay <fixture> [--data-root <dir>] [--config <file>]` loads
the configuration (Section 13 defaults with per-group overrides from
`[groups.<chat_id>]` TOML tables), loads the persona (`{data_root}/
persona.toml`, then the repo-root example, then a built-in default),
renders the preamble through the `PreambleRenderer` trait, spawns one
actor for the fixture's group, feeds the mock replay, and prints a
summary. CLI parsing is hand-rolled; no clap.

## 8. Phase 1 outlook

Phase 1 turns the stubs into a living pet: the digest pipeline against
the replayed log, the context lifecycle, the teloxide adapter with live
intake (including the `reactions` table of specs.md Section 5.2), the
wake procedure with a real timer driver and counters, shallow recall
with the full injection protocol, and the monologue lock in live
operation. Refer to `current-state.md` for the milestone breakdown and
to `dev-roadmap.md` Section 3 for the phase scope.
