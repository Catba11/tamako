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
| `tamako` | Binary. CLI, wiring, the `--replay` demo, the `--live` mode, the `--status` operator modes. | 48 |
| `tamako-core` | Normalized events and actions, the adapter trait, configuration, trigger scheduling, session state, the live context (`context`), the per-group actor, the digest pipeline contract, the wake contracts. | 111 |
| `tamako-store` | `store.db`: SQLite access, migrations (v1–v4), the raw message log, the session-state table, `injected_memories`, `dead_letter`, `reactions`, the read-only status query. | 27 |
| `tamako-memory` | The `MemoryBackend` trait, the `lbug` implementation, deterministic identifiers. | 18 (incl. the concurrent-access regression test) |
| `tamako-persona` | The global persona configuration and the preamble rendering layer. | 14 |
| `tamako-adapter-mock` | The mock platform adapter and the replay fixture. | 9 |
| `tamako-adapter-teloxide` | The live Telegram adapter: pure normalization plus polling intake and outbound actions. | 51 (+1 ignored live test) |
| `tamako-agent` | All LLM concerns: the endpoint layer, the extraction call (rig), the digest pipeline (assembly, validation, entity resolution, retries, dead-letter), the participation gate, the reply generator, the shallow recall worker. | 147 (+3 ignored live tests) |

Total: 425 tests (+4 ignored live tests). Build, test,
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
  `WakeCompleted`, `Shutdown`). All events enter one FIFO inbox
  (Section 6.1, rule 1).
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
  serialized in the loop.
- The wake procedure of specs.md Section 9 is live (M4; the recall
  step is live since M5). Steps 1–5 actor side: (1) the monologue lock suppresses an unforced wake while
  muted (a forced wake is never suppressed, Section 8.1) without
  advancing `wake_last_row_id`; (2) the new messages of the wake are
  the inbound raw-log rows above `wake_last_row_id`, rendered with the
  same speaker-label helper as the live context; (3) the resets of spec
  steps 1 and 5 collapse into ONE reset at wake START plus the
  best-effort `wakes_total` increment, so messages arriving during a
  running wake count toward the next one; (4) the recall/gate/reply
  calls run in a spawned task over a context snapshot (the Section 6.1
  rule 3 analog; the task touches no actor state) and report back as
  `WakeCompleted`; (5) done at start. A wake over a silent group exits
  before the gate call. Forced wakes (mention/reply, Section 8.1)
  bypass the gate; a forced wake during a running wake queues in
  `forced_pending` with the intake timestamp of its forcing message
  (Section 6.2, deterministic replay clock) and starts immediately
  after the current wake completes.
- The reply text of step 4 passes the parrot filter of decision 59 IN
  the wake task, before the report: every line whose trimmed start
  matches the recall-injection prefix (`INJECTION_TEXT_PREFIX`, ASCII
  or full-width colon) is removed — the reply model can imitate the
  injection format it sees as assistant-role context, and a
  confabulated "I remember: ..." line must never become bot speech. A
  strip that leaves text logs one WARN with the chat id; an empty
  remainder is the empty-reply `CoreError::Wake` (log, skip, no
  crash). The live reply generator applies the same pure function at
  its own validation seam (`tamako-agent`, `trimmed_reply_or_error`).
- The `WakeCompleted` send path: the recency re-check of Section 6.2
  DISCARDS the reply when more than `reply_staleness_threshold` newer
  human messages arrived after the target (discard, not regenerate);
  otherwise the outbound raw-log row persists FIRST (Rules B1/P1,
  synthetic id `bot-out:{nanos}` — Rule A3 returns no platform id),
  then the `SendText` action goes into the outbound channel with
  `try_send` (a full or closed channel degrades to a logged drop — the
  row is already the source of truth), the bot speech enters the live
  context (Rule C1), `record_bot_message` drives the monologue lock
  (Section 8.5), and `participations_total` increments best effort.
- The timer driver (M4): a tokio interval inside the actor task emits
  `Tick(now_utc)` on `timer_cadence` (`wake_interval / 8` clamped to
  [1 s, min(wake_floor, 5 min)]; a zero cadence falls back to 1 s so
  `interval` never panics) with `MissedTickBehavior::Delay` — a delayed
  tick loses at most cadence time while Burst could storm the FIFO
  evaluation. The explicit `Tick` command and the timer tick share one
  handler, so the two paths cannot diverge. The driver also evaluates
  the Section 8.2 digest-timeout fallback of a silent group. Shutdown
  is structural: the ticker lives and dies inside the actor task.
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
  (user role), `BotSpeech` (assistant role — Rule B1),
  `RecallInjection` (assistant role; produced by the M5 recall
  worker through `append_recall_injection`, Rule C2), `ToolOutput`
  (reserved, no producer). Item CONTENT renders as XML (decision 61;
  the item model itself is unchanged): a human message renders
  `<msg from="{display_name}"[ user="{username}"] at="{HH:MM UTC}" id="{row_id}"[ kind="edit"][ reply="bot" | reply="user"[ reply_to_name="{name}" reply_to_id="{row_id}"]][ mention="bot"]>text</msg>`,
  bot speech renders `<you at="{HH:MM}" id="{row_id}">text</you>`, and
  a recall injection renders `<memory>escaped edge texts</memory>`.
  Every attribute derives from persisted raw-log columns (Rule P1);
  the reply target resolves through `Store::find_reply_target`
  (`platform_msg_id` → the MIN(id) original row, so edits of the
  target do not move it) at intake, in the wake gate input, and in
  the restart rebuild through the SAME store function — intake and
  rebuild renders are bit-identical even when the target sits below
  the C3 cutoff. A reply to the bot renders `reply="bot"` with no
  target: outbound rows carry synthetic `bot-out:{nanos}` ids (Rules
  A3/B1), so the target can never resolve — no fake resolution. Text
  content escapes `& < >`, attribute values additionally `"`, so a
  user cannot forge structure through message text. Legacy persisted
  injections keep their verbatim `I remember: ...` content and age
  out through C3 within one digest cycle; the decision-59 parrot
  filter matches both injection shapes.
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
| `store.db` | SQLite, WAL mode, `synchronous=NORMAL`. Tables: `messages` (raw log, source of truth; nullable `sender_username` since migration v4 — decision 61), `state` (session KV plus counters), `injected_memories` (dedup set plus the rendered injection `content`, since migration v2), `dead_letter`, `reactions` (reaction rows from intake time, since migration v3 — specs.md Section 5.2), `schema_migrations`. |
| `memory.lbug` | LadybugDB graph. One `Node` table, one `EDGE` rel table (Section 6.1 of the database specification). |

`tamako-store::Store` is rooted at the data root and takes a `chat_id`
in every API. Connections open lazily and are cached in a
`Mutex<HashMap>`. Migrations are an ordered constant array (v1: the
Phase 0 tables; v2: `injected_memories.content` for the bit-identical
context rebuild; v3: the `reactions` table; v4: the additive nullable
`messages.sender_username` for the decision-61 XML `user` attribute —
pre-v4 rows read NULL and render without the attribute) applied
through a minimal runner; the raw-log insert
is idempotent (`INSERT OR IGNORE` on `(platform_msg_id, direction,
event_type, timestamp)`). `chat_id` values with path separators or
`..` are rejected.

The session state (`tamako-core::session`) encodes to state-table keys
(`last_digest_boundary_msg_id`, `prev_digest_boundary_msg_id`,
`last_digest_at`, `muted_flag`, `consecutive_bot_msgs`, `wake_*`). The
actor persists it after every mutation and rebuilds it on restart. The
monologue lock (`record_human_message`, `record_bot_message`) is live
since M4: the wake send path records bot speech and the lock suppresses
unforced wakes while muted.

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
- **All per-group operations are serialized (M6).** The `Send + Sync`
  markers of lbug 0.18 do NOT imply read-during-write safety: the C++
  storage layer races lock-free readers of `FileHandle::pageStates`
  against writer-side `ConcurrentVector::resize` and the CHECKPOINT
  truncate path (SIGSEGV; reproduced 5/5). `LbugBackend::with_conn`
  holds a per-group async mutex for the full duration of every
  operation, reads and CHECKPOINT included — the actor spawns the
  digest pipeline and the wake procedure as concurrent tasks, so a
  recall scan could otherwise overlap an in-flight MERGE. Refer to
  `docs/adr-0001-ladybugdb-binding.md` (addendum 2026-08-08); the
  regression test is `tamako-memory/tests/lbug_concurrent_access.rs`.
- The backend exposes the read path of Section 8 in its Phase 1 form
  (M5): `MemoryBackend::alias_targets` (entry resolution step 2: the
  targets of one alias node, entered through the deterministic alias
  identifier, Rule R5), `MemoryBackend::neighbors` (the Section 8.2
  direct-neighbor fetch: both directions in one query, the filter
  `invalid_at IS NULL`, `contains` edges excluded as provenance-only,
  the 500-edge expansion limit truncated by `created_at` descending,
  one hop only; hub marking and the 90-day window are documented
  Phase 1 simplifications — the unconditional truncation is strictly
  stronger than the hub rule requires), and `LbugBackend::query_rows`,
  a display-string Cypher helper for tests and the demo.
  `NeighborEdge::edge_id()` renders the edge natural key
  (`{source_id}|{relationship_name}|{target_id}|{valid_at}`); it is the
  Section 9.3 dedup key of the `injected_memories` table.

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

Provider configuration: the `endpoint` module (M4) implements the
endpoint portability of specs.md Section 13. `LlmEndpoints::resolve`
maps the plain `LlmConfigValues` (the config-file keys `llm_api`,
`llm_base_url`, the per-purpose `digest`/`gate`/`reply` overrides, and
the three model keys) plus the environment into one `EndpointConfig`
per purpose; env wins at every level (`TAMAKO_LLM_API`,
`TAMAKO_LLM_BASE_URL`, `TAMAKO_{DIGEST,GATE,REPLY}_MODEL`), and an
unknown family string is `AgentError::ProviderConfig`, never a silent
default. The global-only `llm_session_id` (env
`TAMAKO_LLM_SESSION_ID`, default `"tamako"`) resolves into the
`x-opencode-session` default header of the client — the Opencode Go
gateway's session-affinity key for the provider prompt cache — sent on
every request of both families via rig's `ClientBuilder::http_headers`;
every successful completion logs the rig usage fields (including
`cached_input_tokens`) at DEBUG. `EndpointClient` hides the two rig model types behind one
async `complete` call. What rig 0.41 can and cannot do (verified
against its sources; the module docs carry the full list): custom base
URLs and free-form model names on every provider through the explicit
builder (so rig's own `*_BASE_URL` env vars deliberately do NOT apply);
Anthropic native structured output without a beta header; OpenAI
structured output with a hardcoded `strict: true` json_schema (a limit
for some openai-compatible endpoints); the default `openai::Client`
speaks the first-party-only Responses API, so the openai-compatible
path uses `CompletionsClient` (`POST {base}/chat/completions`); base
URL handling differs per family (Anthropic normalizes, OpenAI is
verbatim). API keys come from the environment only (`ANTHROPIC_API_KEY`
/ `OPENAI_API_KEY`); a missing key is `AgentError::ProviderConfig`. The
default models are `claude-haiku-4-5` (digest, gate) and
`claude-sonnet-4-5` (reply — a string literal; rig 0.41 has no
`CLAUDE_SONNET_4_5` constant). Without the family key, the binary runs
with the digest pipeline and the wake procedure disabled.

The wake LLM implementations (M4): `RigGate` (specs.md Section 9.6)
runs the binary participation decision over the cheap `gate_model`
with structured output (`GateOutput` — participate, target id, reason)
post-validated in plain Rust (a target outside the presented set falls
back to silence; never trust the model). `RigReplyGenerator`
(Section 9 step 4) generates the reply over the main `reply_model`;
its `context_messages_to_rig` is the ONLY core→rig conversion seam
(tamako-core stays model-agnostic), and an empty model output is a wake
error — the bot never sends an empty message. Both have scripted
doubles (`ScriptedGate`, `ScriptedReplyGenerator`) for hermetic tests.
The recall step of Section 9 is live (M5, the `recall` module):
`ShallowRecall` implements the tamako-core `RecallProvider` contract
over the shared store and graph. Candidate extraction is deterministic
(no LLM term extraction, Phase 1): the Person identifiers of the
senders and of the reply targets (Section 8.1 step 1 — reply targets
resolve through `Store::find_sender_by_platform_msg_id`), plus exact
alias matches of the candidate terms (step 2; a pure tokenizer with
two paths since decision 58 — the alphanumeric path splits on
non-alphanumerics with stopword and short-token drops, 20 terms per
wake, and the CJK path splits every maximal CJK run (U+4E00–9FFF,
Extension-A, kana) into all contiguous n-grams n 2..=5, 40 terms per
wake, longer n-grams first; Section 7.1 normalization on every term;
documented limits: no multi-word terms on the alphanumeric path, no
synonyms, English-only stopwords and no CJK stopword list). An alias
with several targets enters through the Alias node itself (the Section 7.4 step 4
ambiguity fallback, mirrored); an unknown term yields no entry
(step 4, Rule R5). The candidate set first collapses by fact key
(latest `valid_at` wins per `(source_id, relationship_name,
target_id)` — the same fact must not burn the cap twice; decision 58,
dev-roadmap.md Section 3 item 5); THEN the Section 9.3 dedup drops
candidates whose edge
id has an `injected_memories` row. Zero candidates never call the
cheap model (Section 9.1). The relevance gate `RigRelevanceGate`
(Section 9.2) runs on the cheap `gate` endpoint with structured output
(`RecallSelection`), post-validated in plain Rust (in-range indices
only, deduped, hard cap `recall_injection_cap`, default 5 — reported
for spec backfill); conservative by default, and ANY failure means
inject nothing — a wake never fails on a recall-gate error, and DEBUG
logs distinguish the three gate outcomes (no candidates — the gate is
not called, selected none, gate failure with a WARN; decision 58)
while decision 53's curated INFO wake line stays untouched. The render
is exactly one "I remember: ..." assistant message (Section 9.4); an
empty injection is forbidden. `ScriptedRelevanceGate` is the test
double (same pattern as `ScriptedGate`).

## 9. The binary

`tamako --replay <fixture> [--data-root <dir>] [--config <file>]` loads
the configuration (Section 13 defaults with per-group overrides from
`[groups.<chat_id>]` TOML tables), loads the persona with the lenient
fallback chain (`{data_root}/persona.toml`, then the repo-root example,
then a built-in default — offline demos must not require setup),
renders the preamble through the `PreambleRenderer` trait (the actor
stores it as item 0 of the live context, Rule C4), resolves the three
LLM endpoints from the group configuration and the environment (a bad
`llm_api` family string is a hard startup error), builds the digest
pipeline and the wake services from them (a missing family API key
degrades each to one warning and silence; the M5 recall wires
`ShallowRecall` over the shared store and graph with the relevance
gate on the cheap `gate` endpoint and `recall_injection_cap` from the
group configuration — a missing key degrades the recall alone to
`NoopRecall`), spawns one actor for the
fixture's group, feeds the mock replay, and prints a summary
(including the digest boundary and the dead-letter count). CLI parsing
is hand-rolled; no clap.

The wake wiring (M4) is symmetric in both modes: ONE
`mpsc::channel::<OutboundAction>(100)` per run serves every actor (the
actions carry their chat id), and the binary pumps it into
`PlatformAdapter::execute`. In replay mode the event loop is a
`tokio::select!` over `next_event()` and the outbound channel; after
the fixture ends and the snapshot barrier returns, the channel drains
with `try_recv` until empty (best effort for the demo — a wake spawned
by the last events can still be in flight; the integration tests, not
the replay demo, assert the wake behavior deterministically). In live
mode the pump is an arm of the main `select!`; an `AdapterError` warns
with the chat id and continues (Section 4.2 tolerance, never fatal).
Every spawn site passes the wake services, a clone of the outbound
sender, and the persona name (the sender display name of outbound
raw-log rows).

`tamako --live [--allow-default-persona] [--data-root <dir>] [--config <file>]` (mutually
exclusive with `--replay`) connects the teloxide adapter: the token
comes from `TELOXIDE_TOKEN`, and the served groups come from the
`[groups.<chat_id>]` tables of the config file. The persona policy is
STRICT in live mode (M6, specs.md Section 5.3): a missing
`{data_root}/persona.toml` is a hard startup error that points at the
repo-root example as the template, and a malformed file is a hard
error — a silent fallback to a default persona in production would hide
configuration mistakes (Rule C4: the preamble is the provider cache
anchor; its source must be deliberate). The `--allow-default-persona`
flag restores the lenient fallback chain for experiments. An actor spawns lazily
on the first event of each configured group; events from
non-configured groups are logged once and ignored (Rule P5). The
binary routes events by the chat id of `next_group_event`. Ctrl-c
shuts every actor down gracefully (session flush per group) and prints
a summary log. An adapter error that escapes `next_group_event` is
fatal: graceful shutdown, then the error propagates. The config file
is not watched; restart to pick up new groups.

`tamako --status <chat_id>` and `tamako --status-all` (M6) are the
offline operator modes (specs.md Sections 10.3 and 12): they open the
group store.db READ-ONLY (`SQLITE_OPEN_READ_ONLY` through
`Store::read_group_status` — no directory creation, no migrations, a
2 s busy timeout; verified safe against a live writer on the bundled
SQLite 3.46.0) and print the Section 12 counters and derived rates
(participation, injection), the digest boundaries, the muted state,
and the five most recent dead letters with batch id, derived attempts
(`digest_max_retries`, exhausted by definition), error, and timestamp.
No persona load, no token, no LLM endpoints, no actor spawn. The
per-group capability is not printed: it is never persisted and an
offline tool must not call Telegram. After a CLEAN bot shutdown the
WAL files are gone, so a read-only query can fail on a read-only
directory; the error carries an operator hint.

An offline digest demo lives at `tamako-agent/examples/digest_demo.rs`:
`cargo run -p tamako-agent --example digest_demo` replays the fixture
through the real actor and the real LadybugDB backend with a scripted
extractor and prints the resulting graph.

## 10. Phase 1 outlook

Phase 1 is feature-complete (M1–M6). Hardening (M6) added the persona
strict startup policy for `--live`, the read-only `--status` operator
modes (Section 9), the monologue-lock integration test across a
restart, the per-group serialization of all LadybugDB operations
(Section 7 — a production-critical read-during-write race), and the
stability replay loop (10 iterations of digests, wakes, injections,
and interleaved restarts, bit-identical rebuilds, zero dead letters).
The remaining Phase 1 step is the two-week test-group soak, operated
with `docs/soak-runbook.md`. Refer to `current-state.md` for the
milestone breakdown and to `dev-roadmap.md` Section 3 for the phase
scope.
