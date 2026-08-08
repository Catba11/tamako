# ARCHITECTURE.md — Tamako, as built

This document describes the crate-level architecture as it exists in the
repository today. It documents what is built and tested, not what is
planned. The governing documents (`specs.md`,
`proposed-graph-database-specs.md`, `dev-roadmap.md`) define intent; this
document records the current implementation.

## 1. Workspace layout

A Cargo workspace at the repository root with eight crates. Shared
dependency versions are pinned in `[workspace.dependencies]`.

| Crate | Role | Tests |
|---|---|---|
| `tamako` | Binary. CLI, wiring, the `--replay` demo, the `--live` mode. | 16 (8 integration + 8 CLI unit) |
| `tamako-core` | Normalized events and actions, the adapter trait, configuration, trigger scheduling, session state, the live context (`context`), the per-group actor, the digest pipeline contract. | 57 |
| `tamako-store` | `store.db`: SQLite access, migrations (v1–v3), the raw message log, the session-state table, `injected_memories`, `dead_letter`, `reactions`. | 17 |
| `tamako-memory` | The `MemoryBackend` trait, the `lbug` implementation, deterministic identifiers. | 12 |
| `tamako-persona` | The global persona configuration and the preamble rendering layer. | 7 |
| `tamako-adapter-mock` | The mock platform adapter and the replay fixture. | 6 |
| `tamako-adapter-teloxide` | The live Telegram adapter: pure normalization plus polling intake and outbound actions. | 50 (+1 ignored live test) |
| `tamako-agent` | All LLM concerns: the extraction call (rig), the digest pipeline (assembly, validation, entity resolution, retries, dead-letter). | 42 (+1 ignored live test) |

Total: 207 tests (+2 ignored live tests). Build, test,
clippy (`-D warnings`), and fmt are clean.

## 2. Dependency direction

```
tamako ──▶ tamako-core ──▶ tamako-store
   │           │    └─────▶ tamako-memory
   │           └──────────▶ tamako-persona
   ├──▶ tamako-store  ──── (all lower crates are independent)
   ├──▶ tamako-memory
   ├──▶ tamako-persona
   ├──▶ tamako-agent ──▶ tamako-core (contract), tamako-store, tamako-memory
   ├──▶ tamako-adapter-mock ──▶ tamako-core (types only)
   └──▶ tamako-adapter-teloxide ──▶ tamako-core (types only)
```

- The binary depends on all crates and does the wiring.
- `tamako-core` depends on the storage and persona crates. No crate
  depends on `tamako-core` except adapters and `tamako-agent`, and
  adapters use the normalized types only (Rule A1, Rule P7).
- `tamako-agent` owns every LLM concern and is the only crate that
  depends on `rig` (rig-core 0.41). `tamako-core` defines the digest
  pipeline CONTRACT (`tamako_core::digest`) so the actor drives the
  pipeline without a dependency on the agent crate. No cycles.
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

The live adapter is `tamako-adapter-teloxide` (teloxide 0.17, M3). Its
`normalize` module is pure: it converts Telegram updates into the
normalized types — messages with mention and reply resolution at intake
(specs.md Section 4.2, against the bot identity from get_me), edits,
member join/leave service messages, and reactions (named, anonymous,
and aggregated counts). Display names fall back "First Last" →
"@username" → numeric id; a lone first name never wins. Anonymous group
admins (messages sent as the chat) and anonymous reaction actors get
the synthetic sender id `chat:{id}`. Aggregated count updates persist
the emoji set only: `total_count` is dropped and `old_emojis` is empty
(the Bot API carries no previous state); `CustomEmoji` normalizes to
its `custom_emoji_id` string and `Paid` is skipped. Because
`Polling::as_stream` borrows the listener mutably and teloxide 0.17
has no owned-stream API, a spawned task owns the polling listener and
forwards updates over a bounded mpsc channel (capacity 100); the
adapter consumes the channel (no Dispatcher, no dptree; transient
stream errors are skipped inside the task). The inherent
`next_group_event()` returns `GroupEvent { chat_id, event }` so the
binary routes multi-group traffic by chat id (Rule P5); the
`PlatformAdapter` trait impl is the Rule A5 substitutability proof and
drops the chat id. Outbound, `SendText` (optional reply through
ReplyParameters) and `React` (setMessageReaction) are implemented;
`SendMedia` returns `AdapterError::Unsupported` (Phase 3). The poller
requests `allowed_updates` = message, edited_message,
message_reaction, message_reaction_count; reaction updates are
delivered to administrators only, so a non-administrator bot simply
never receives them — administrator status is optional, not required
(specs.md Section 4.2). `bot_chat_status` classifies the per-group
membership from `getChatMember` (`BotChatStatus`: `Administrator` —
owner counts — `Member`, `RestrictedOrOther`, `Unknown` on query
failure); the binary runs the detection pass at startup and logs one
warning per non-administrator group. Outbound permission failures
(missing rights or access) are tolerated: logged with the chat id,
never fatal. `TELOXIDE_API_URL` is honored only by
`Bot::from_env`, not by `Bot::new`, so the adapter applies
`set_api_url` itself; a custom Bot API server works.

## 4. The per-group actor

`tamako-core::actor` implements the actor model of specs.md Section 6.

- One tokio task and one mpsc inbox per group (`ActorCommand`:
  `Inbound`, `Tick`, `Snapshot`, `ContextSnapshot`, `DigestCompleted`,
  `Shutdown`). All events enter one FIFO inbox (Section 6.1, rule 1).
- Startup runs inside the spawned task: open the group store, ensure the
  graph schema, load and decode the session state. The handle returns
  immediately; `snapshot()` acts as a FIFO barrier and `shutdown()`
  surfaces startup errors through the join handle.
- Message intake order is fixed (Rule P1): first persist the raw-log row
  through `spawn_blocking`, then append the item to the live context
  (Rule C1), then update the in-memory session, then persist the
  session, then evaluate triggers. A duplicate delivery yields one log
  row and no context append; the wake counter counts each delivery
  (specs.md Section 8.1).
- Edited messages append a new log row with `event_type = 'edit'` and a
  context item like any other new row (uniform with the rebuild, which
  renders edits identically). An edit never retracts (specs.md
  Section 15, open item 4).
- Reaction intake is passive collection (M3): the reaction row persists
  first into the `reactions` table (Rule P1, idempotent under
  redelivery through the dedup unique index). No context item, no
  wake-counter advance, no session mutation. Member join/leave events
  stay debug-only.
- Trigger evaluation (specs.md Section 6.2: Digest before Wake). The
  digest trigger of Section 8.2 is live (M1): on fire, the actor spawns
  the digest pipeline as a task that reports back through the inbox
  (`ActorCommand::DigestCompleted`), so extraction and backoff never
  block the FIFO queue (Section 6.1, rule 3). Session mutations stay
  serialized in the loop. The wake trigger still logs a stub; the wake
  procedure is M4.
- On digest completion the actor performs the Rule C3 context removal
  (M2): every context item with a range tag at or below the PREVIOUS
  boundary goes (the one-chunk lag of Section 7.1 — the chunk just
  digested stays as the new overlap buffer), the `injected_memories`
  dedup set is pruned at the same cutoff (specs.md Section 10.2
  step 4), and the session boundaries advance
  (`prev_digest_boundary_msg_id` stays `None` until the SECOND digest
  completes; `last_digest_boundary_msg_id` advances for EVERY outcome —
  a dead-lettered batch is skipped and never blocks later batches,
  specs.md Section 10.3). The session is persisted once after all
  mutations, then the `PostDigestHook` runs (a seam for observers that
  need no actor state; `NoopPostDigestHook` is the default), then the
  digest trigger re-evaluates once.
- The wake scheduler (`tamako-core::trigger`) is pure logic: fire on the
  first of message count or jittered interval, subject to the floor
  (specs.md Section 8.3). The jittered interval is normalized to whole
  milliseconds so the persisted encoding is lossless. The digest
  thresholds of Section 8.2 are a pure function over tail statistics
  (`tail_stats` computes them from the raw-log tail; one tail scan per
  evaluation, bounded by the fire thresholds in practice).

All synchronous storage calls run inside `tokio::task::spawn_blocking`
(AGENT.md Section 6.2).

## 5. The live context (tamako-core, M2)

`tamako-core::context` implements the context lifecycle of specs.md
Section 7. The actor owns one `LiveContext` per group and mutates it
only inside the actor loop (Section 6.1, rule 2).

- **Item model**: an ordered list. Item 0 is always the system preamble
  from the persona service — the provider cache anchor (Rule C4). Every
  other item carries a message-id range tag (Section 7.1, raw-log row
  ids) and a role (system / user / assistant). Kinds: `HumanMessage`
  (user role, speaker label `[{display_name} {HH:MM}] {text}`, HH:MM in
  UTC — Section 7.2 step 4), `BotSpeech` (assistant role, verbatim
  text; the label distinguishes group members, not the bot — Rule B1),
  `RecallInjection` (assistant role; the kind exists now, only M5
  produces these items), `ToolOutput` (reserved, no producer).
- **Append-only by construction (Rules C1, C2)**: the item vector is
  private and the public API permits appends at the tail ONLY. No
  method inserts into, removes from, or mutates the middle of the
  history. The only destructive operations are `remove_at_or_below`
  (the Rule C3 digest-time removal) and `reload_preamble` (the Rule C4
  item-0 replacement; a preamble change is a deliberate full
  invalidation event).
- **Restart rebuild (Rule P1)**: `LiveContext::rebuild(preamble, rows,
  injections)` reconstructs the context from the raw-log rows above the
  removal cutoff (`prev_digest_boundary_msg_id.unwrap_or(0)`) and the
  `injected_memories` rows above the same cutoff. Injections land
  directly after the row at their recorded position (Rule C2). The
  rebuild is bit-identical to the pre-restart context: one render
  helper serves both intake and rebuild, and the rendered injection
  text is persisted (`injected_memories.content`, migration v2).
- **LLM-facing view**: `messages_for_llm()` returns the ordered items
  as model-agnostic `ContextMessage { role, content }` values, preamble
  first; tamako-agent converts them to rig types in M4. `stats()`
  (item count, estimated bytes) feeds future metrics.

## 6. Storage layout per group

Rule P5: one directory per group at `{data_root}/{chat_id}/`.

| File | Content |
|---|---|
| `store.db` | SQLite, WAL mode, `synchronous=NORMAL`. Tables: `messages` (raw log, source of truth), `state` (session KV plus counters), `injected_memories` (dedup set plus the rendered injection `content`, since migration v2), `dead_letter`, `reactions` (reaction rows from intake time, since migration v3 — specs.md Section 5.2), `schema_migrations`. |
| `memory.lbug` | LadybugDB graph. One `Node` table, one `EDGE` rel table (Section 6.1 of the database specification). |

`tamako-store::Store` is rooted at the data root and takes a `chat_id`
in every API. Connections open lazily and are cached in a
`Mutex<HashMap>`. Migrations are an ordered constant array (v1: the
Phase 0 tables; v2: `injected_memories.content` for the bit-identical
context rebuild; v3: the `reactions` table) applied through a minimal
runner; the raw-log insert
is idempotent (`INSERT OR IGNORE` on `(platform_msg_id, direction,
event_type, timestamp)`). `chat_id` values with path separators or
`..` are rejected.

The session state (`tamako-core::session`) encodes to state-table keys
(`last_digest_boundary_msg_id`, `prev_digest_boundary_msg_id`,
`last_digest_at`, `muted_flag`, `consecutive_bot_msgs`, `wake_*`). The
actor persists it after every mutation and rebuilds it on restart. The
monologue lock mechanism (`record_human_message`,
`record_bot_message`) exists; live bot speech arrives in Phase 1.

## 7. The MemoryBackend design

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
- The backend exposes two read operations (Section 8 entry resolution):
  `MemoryBackend::alias_targets` (entity resolution step 2: the targets
  of one alias node, entered through the deterministic alias identifier,
  Rule R5) and `LbugBackend::query_rows`, a display-string Cypher helper
  for tests and the demo.

## 8. The digest pipeline (tamako-agent, M1)

specs.md Section 10 and proposed-graph-database-specs.md Section 7,
built end to end against the replayed log:

1. **Batch assembly** (`pipeline.rs`): the raw-log range
   `(last_digest_boundary_msg_id, tail]` through
   `Store::list_messages_after`. Speaker labels
   `[{display_name} {HH:MM}] {text}` (Section 7.2 step 4, UTC). The
   mention/reply → `tg_user_id` map comes from the stored log rows
   (sender bindings plus reply-target bindings, specs.md Section 10.1).
   The batch id `uuid5("batch:{first}:{last}")` is stable across
   retries.
2. **Skeleton skip** (`skeleton.rs`, Section 7.2 rule 5): an
   emoji/greeting-only batch stores the MessageBatch skeleton without an
   extraction call. The detector is deterministic and conservative:
   when in doubt, extract.
3. **Extraction** (`extract.rs`, `rig_impl.rs`, `prompt.rs`,
   `graph.rs`): the `KnowledgeExtractor` trait has two implementations —
   the live `RigExtractor` and the scripted `ScriptedExtractor` for
   tests and the offline demo. rig-core 0.41 has no `Extractor` type;
   extraction is a completion request with
   `output_schema(schemars::schema_for!(KnowledgeGraph))`, which
   Anthropic serves as native JSON-schema structured output. The
   `KnowledgeGraph` type (serde + schemars 1.x) constrains nodes to
   `Person` and `Concept`. The prompt requires snake_case relationship
   names, one specific description per edge, coreference resolution, no
   knowledge outside the text, and supplies the mention map as
   structured context (Section 7.3).
4. **Post-validation** (`validate.rs`, Section 6.3), in plain Rust:
   relationship names must be snake_case identifiers and must not be a
   reserved system name (`contains`, `known_as`, `also_known_as`,
   `is_a`, `supersedes`). Violations become `related_to` with the
   original name in the edge properties.
5. **Entity resolution** (`resolve.rs`, Section 7.4 steps 1, 2, 4
   only): mention/reply binding to `uuid5("tg_user:{id}")`; exact alias
   match with exactly one target binds to that target; an ambiguous or
   unresolvable person attaches its facts to the Alias node (no
   guessing; the node carries the `"attachment": "fallback"` marker of
   the primary quality metric). Unresolved concepts get
   `uuid5("concept:{normalized}")`. Surface forms become Alias nodes
   with `known_as`/`also_known_as` edges (step 5). Alias-bound nodes
   carry `properties: None` so the MERGE coalesce keeps the stored
   identity blob. Fact validity is the Phase 1 multi-value form:
   `valid_at` = batch end, `invalid_at` NULL (dev-roadmap.md Section 3
   item 5). Every batch writes its MessageBatch node and `contains`
   provenance edges (Section 6.3).
6. **Write**: one transactional idempotent `upsert_batch` with
   `CHECKPOINT`. No embeddings — the Phase 2 hook point is marked in
   `pipeline.rs`.
7. **Failure handling** (specs.md Section 10.3): exponential backoff
   (base 2 s, doubling, capped at 60 s) with the same batch id; after
   `digest_max_retries` total attempts (default 5) the skeleton plus
   error lands in `dead_letter`, `digest_failures_total` and
   `dead_letters_total` counters increment (best effort), and the batch
   is SKIPPED — the boundary advances and later batches proceed.

Provider configuration: the extraction model defaults to
`claude-haiku-4-5` (cheap tier), overridable by the env var
`TAMAKO_DIGEST_MODEL` or the config-file key `digest_model` (env wins;
deviation: specs.md Section 13 has no LLM keys yet). The API key is
`ANTHROPIC_API_KEY`, read by rig's `Client::from_env()`. Without the
key, the binary runs with the digest pipeline disabled.

## 9. The binary

`tamako --replay <fixture> [--data-root <dir>] [--config <file>]` loads
the configuration (Section 13 defaults with per-group overrides from
`[groups.<chat_id>]` TOML tables), loads the persona (`{data_root}/
persona.toml`, then the repo-root example, then a built-in default),
renders the preamble through the `PreambleRenderer` trait (the actor
stores it as item 0 of the live context, Rule C4), spawns one actor for
the fixture's group, feeds the mock replay, and prints a summary
(including the digest boundary and the dead-letter count). CLI parsing
is hand-rolled; no clap. When `ANTHROPIC_API_KEY` is set, the binary
wires the live `RigExtractor` digest pipeline; without the key, digests
are disabled and the replay stays offline.

`tamako --live [--data-root <dir>] [--config <file>]` (mutually
exclusive with `--replay`) connects the teloxide adapter: the token
comes from `TELOXIDE_TOKEN`, and the served groups come from the
`[groups.<chat_id>]` tables of the config file. An actor spawns lazily
on the first event of each configured group; events from
non-configured groups are logged once and ignored (Rule P5). The
binary routes events by the chat id of `next_group_event`. Ctrl-c
shuts every actor down gracefully (session flush per group) and prints
a summary log. An adapter error that escapes `next_group_event` is
fatal: graceful shutdown, then the error propagates. The config file
is not watched; restart to pick up new groups.

An offline digest demo lives at `tamako-agent/examples/digest_demo.rs`:
`cargo run -p tamako-agent --example digest_demo` replays the fixture
through the real actor and the real LadybugDB backend with a scripted
extractor and prints the resulting graph.

## 10. Phase 1 outlook

The context lifecycle (M2) and the teloxide adapter with live intake
(M3, including the `reactions` table of specs.md Section 5.2 — store
migration v3) are built and tested (Sections 3 and 5). Phase 1
continues with the wake procedure with a real timer driver and
counters (M4 consumes `LiveContext::messages_for_llm` and
`append_bot_speech`), shallow recall with the full injection protocol
(M5 produces the `RecallInjection` items through
`append_recall_injection` and the `injected_memories` dedup table),
and the monologue lock in live operation (M6). Refer to
`current-state.md` for the milestone breakdown and to
`dev-roadmap.md` Section 3 for the phase scope.
