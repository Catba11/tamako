# current-state.md — Tamako progress

A living document. Update it at every milestone. Last update: Phase 1
M5 (shallow recall + full injection protocol) complete.

## 1. Where we are

- **Phase 0 (scaffolding): COMPLETE.** The exit criterion of
  `dev-roadmap.md` Section 2 is met and tested: the scripted mock
  adapter replays a recorded chat log, the actor persists the raw log
  and the session state, and a restart rebuilds the identical state.
- **Phase 1 (a living pet, MVP): IN PROGRESS.** Scope in
  `dev-roadmap.md` Section 3.
  - **M1 (digest pipeline end to end): COMPLETE.** The pipeline runs
    against the replayed log before any live traffic: batch assembly,
    skeleton skip, rig extraction with the `KnowledgeGraph` schema,
    post-validation, entity resolution steps 1/2/4, transactional
    idempotent write, exponential backoff with a stable batch id,
    dead-letter, boundary advance. The trigger of specs.md Section 8.2
    is wired in the actor and runs the pipeline.
  - **M2 (context lifecycle): COMPLETE.** The actor owns the live
    context of specs.md Section 7: an append-only item list (C1/C2,
    enforced structurally — no mid-history API exists) with the
    preamble as item 0 (C4) and message-id range tags on every other
    item. Intake appends human messages and edit rows; the C3 removal
    runs in the actor's digest-completion handler with the one-chunk
    lag (items at or below the PREVIOUS boundary go), the
    `injected_memories` dedup set is pruned at the same cutoff
    (Section 10.2 step 4), and `prev_digest_boundary_msg_id` persists
    the lag cutoff. Restart rebuilds the context bit-identically from
    the raw log, `injected_memories`, and the session state (P1). The
    M4 view (`messages_for_llm`) and the M5 injection append API are
    exposed.
  - **M3 (teloxide adapter + live intake): COMPLETE.** The new crate
    `tamako-adapter-teloxide` normalizes Telegram updates (messages
    with mention/reply resolution at intake per Section 4.2, edits,
    member join/leave service messages, named/anonymous/aggregated
    reactions) and polls them through a spawned listener task into a
    bounded mpsc channel. `next_group_event()` returns the chat id with
    each event, so the binary routes multi-group traffic (Rule P5). The
    `--live` mode of the binary spawns one actor per configured group
    lazily, logs and ignores non-configured groups once, and shuts down
    every actor gracefully on ctrl-c. Reaction intake persists
    `InboundEvent::Reaction` into the `reactions` table (migration v3,
    Section 5.2) at intake time (Rule P1), idempotent under redelivery,
    as passive collection: no context item, no wake-counter advance, no
    session mutation. Outbound: `SendText` (with optional reply) and
    `React` implemented; `SendMedia` returns
    `AdapterError::Unsupported` (Phase 3). Graceful capability model
    (specs.md Section 4.2): administrator status is recommended, not
    required. The binary detects the per-group membership status at
    startup (`getChatMember`, in-memory cache, never persisted —
    re-evaluated on each startup) and degrades to
    no-reaction-collection with one warning per non-administrator
    group.
  - **M4 (wake procedure + timer driver + counters): COMPLETE.** The
    wake procedure of specs.md Section 9 is live: a wake gathers the new
    messages above `wake_last_row_id`, runs recall (the M4 seam —
    `NoopRecall`, M5 replaces it with shallow recall), decides
    participation through the gate (Section 9.6, cheap `gate_model`;
    forced wakes bypass it per Section 8.1), generates the reply with
    the main `reply_model`, and sends it through one shared outbound
    channel into the platform adapter. The send path persists the
    outbound raw-log row FIRST (Rules B1/P1), and the recency re-check
    of Section 6.2 discards a stale reply (more than
    `reply_staleness_threshold` newer human messages after the target).
    The monologue lock (Section 8.5) suppresses unforced wakes while
    muted. The timer driver is a tokio interval inside the actor task
    (`timer_cadence`, `MissedTickBehavior::Delay`); it also closes the
    silent-group digest-timeout gap. The counters `wakes_total` and
    `participations_total` (Section 12) increment best effort. LLM
    access is endpoint-portable (Section 13): the `endpoint` module of
    tamako-agent resolves `llm_api` / `llm_base_url` (global and
    per-purpose) plus the models from config and env (env wins) into one
    `EndpointConfig` per purpose; the binary wires digest, gate, and
    reply from it and degrades to silence with one warning when the
    family API key is missing.
  - **M5 (shallow recall + full injection protocol): COMPLETE.** The
    read path of proposed-graph-database-specs.md Section 8 is live in
    its Phase 1 form: `MemoryBackend::neighbors` (Section 8.2 — valid
    edges only, `contains` excluded as provenance-only, the 500-edge
    expansion limit truncated by `created_at` descending, one hop,
    Rule R5 identifier entry). Entry resolution covers Section 8.1
    steps 1 and 2 only (no vector search, Phase 2): mentions/replies
    resolve through the deterministic `uuid5("tg_user:{id}")` Person
    identifiers, and normalized terms resolve through the exact Alias
    match of the M1 `alias_targets` pattern; a no-match yields no
    recall (step 4, no fuzzy scans). The `ShallowRecall` worker of
    tamako-agent replaces the M4 `NoopRecall` seam: deterministic
    candidate extraction (sender and reply-target Person entries, a
    pure tokenizer for alias terms — no LLM extraction), the Section
    9.3 dedup against `injected_memories`, and the conservative
    cheap-model relevance gate (Section 9.2, structured output,
    post-validated in plain Rust, hard cap `recall_injection_cap`
    default 5). Zero candidates never call the cheap model
    (Section 9.1); any gate failure means inject nothing. The
    injection protocol of Sections 9.3–9.5 is complete: exactly one
    "I remember: ..." assistant message per wake appended at the tail
    (Rule C2, applied by the actor before the participation outcome is
    known — a silent pet still remembers), one `injected_memories` row
    per edge id (the dedup key is the edge natural key), the C3 prune
    at the previous boundary at digest time (M2 path, verified), the
    digest input stays the raw log only (injections never re-enter the
    graph), and the preamble guardrail of Section 9.4 (M1/M2 era) is
    referenced, not duplicated. The `injection_wakes_total` counter
    (Section 12) is wired. The gate input and the reply-model snapshot
    both carry the injections.
  - M6: NOT STARTED. Milestones in Section 5 below.
- Verification: `cargo build --workspace`, `cargo test --workspace`
  (305 tests, 0 failures, 4 ignored live tests: the live-API smoke
  tests of tamako-agent — extraction and wake — and the live Telegram
  smoke test),
  `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` — all clean.
- Offline demo: `cargo run -p tamako-agent --example digest_demo`.
  Live replay: `ANTHROPIC_API_KEY=... cargo run -- --replay
  tamako-adapter-mock/fixtures/replay_chat.json` (without the key the
  replay runs with digests disabled). Live Telegram:
  `TELOXIDE_TOKEN=... cargo run -- --live` (groups come from the
  `[groups.<chat_id>]` tables of the config file).

## 2. What exists and is tested

| Crate | Content | Tests |
|---|---|---|
| `tamako` | CLI (`--replay`, `--live` — mutually exclusive, `--data-root`, `--config`), wiring: endpoint resolution of specs.md Section 13 (`TriggerConfig` → `LlmConfigValues` → `LlmEndpoints::resolve`; a bad family string is a hard startup error), the live digest pipeline and the wake services (recall + gate + reply) built from the resolved endpoints (M5: `ShallowRecall` over the shared store and graph, `RigRelevanceGate` on the cheap gate endpoint, `recall_injection_cap` from the group config; a missing family API key degrades each to a warning and silence — the recall alone falls back to `NoopRecall`) — a missing family API key degrades each to a warning and silence — and ONE shared outbound channel (capacity 100) pumped into the platform adapter in both modes (replay: `select!` over the fixture stream plus a best-effort drain after the barrier; live: an arm of the main `select!`, failures warn and continue per Section 4.2). Live mode (M3): token from `TELOXIDE_TOKEN`, configured groups from `[groups.<chat_id>]` config tables, lazy actor spawn on the first event per group, non-configured groups logged once and ignored (Rule P5), capability detection at startup (one `getChatMember` call per configured group, in-memory cache, spawn-time re-check, one warning per non-administrator group, admin INFO / unknown INFO guidance), ctrl-c graceful shutdown of every actor with a summary log. Integration tests: `replay_restart` (Phase 0 exit criterion), `digest_replay` (end-to-end digest, idempotency, dead-letter then recovery, skeleton-only batch, restart keeps the boundary), `context_replay` (M2: context growth on intake, one-chunk-lag removal across two digests, bit-identical restart rebuild with injections, dedup prune), `wake_replay` (M4: threshold wake end to end over the replay fixture, gate-no silence, forced bypass of a muted group, live monologue lock with tick-driven suppression, recency discard), and `recall_replay` (M5: injection end to end over a seeded graph — format/position/rows, Section 9.3 dedup across wakes of one chunk, zero candidates never call the cheap model, bit-identical restart rebuild, digest-time prune of the injection rows at the boundary) against the real store, the real graph, and scripted doubles. Plus CLI and capability-mapping unit tests. | 26 |
| `tamako-core` | Normalized events/actions (A1–A4), `PlatformAdapter` trait, typed config with Section 13 defaults and per-group TOML overrides, wake/digest trigger scheduling, `tail_stats` over the raw-log tail, session state encode/decode incl. `last_digest_at` and `prev_digest_boundary_msg_id`, the `context` module (M2: `LiveContext` ordered item model with range tags, structurally append-only per C1/C2, C3 `remove_at_or_below`, C4 `reload_preamble`, bit-identical `rebuild`, `messages_for_llm` view, stats), per-group actor with raw-log-first intake (P1), context appends at intake (messages and edits), reaction intake (M3: persist into `reactions` through `Store::insert_reaction` at intake, idempotent under redelivery, passive collection — no context item, no wake-counter advance, no session mutation; member join/leave stay debug-only), startup context rebuild, C3 removal + dedup prune in the digest-completion handler, edit appends, forced-wake and wake stubs, LIVE digest wiring (spawn + `DigestCompleted` through the inbox, digest before wake, one digest in flight per group), the `digest` contract module (`DigestPipeline`, `DigestOutcome`, `PostDigestHook` — a seam for stateless post-digest observers; the actor performs C3 itself before the hook), the `wake` contract module (`GateMessage` (with sender id, reply target, and raw text for the recall), `GateInput`, `GateDecision`, `RecallProvider` returning `RecallOutcome`/`PlannedInjection` (M5) + `NoopRecall`, `ParticipationGate`, `ReplyGenerator`, `WakeServices` — same contract-in-core pattern as the digest), the LIVE wake procedure of specs.md Section 9 (gather new messages above `wake_last_row_id`, reset-at-start + `wakes_total`, spawned task for the recall/gate/reply calls, `WakeCompleted` handler with the Section 6.2 recency discard, outbound row first per Rule B1 with the synthetic id `bot-out:{nanos}`, `try_send` into the outbound channel, context append, monologue-lock bookkeeping, `participations_total`; M5: the `WakeCompleted` handler applies the planned injections BEFORE the participation outcome is known — one `injected_memories` row per edge id, `append_recall_injection` at the tail (Rule C2), `injection_wakes_total` — and the reply-model snapshot carries the injections from step 2 on), the M4 timer driver (tokio interval at `timer_cadence` inside the actor task, `MissedTickBehavior::Delay`, zero-cadence guard), and the forced-wake queueing of Section 6.2. | 77 |
| `tamako-store` | `store.db`: embedded migration runner (v1–v3), `messages` (idempotent insert, range read `list_messages_after`), `state` KV with atomic multi-write and counters, `injected_memories` (with the rendered `content` column since v2; `delete_injected_memories_up_to` for the Section 10.2 step 4 prune), `dead_letter`, `reactions` (migration v3, Section 5.2: `insert_reaction` with dedup UNIQUE INDEX with COALESCE → Duplicate). `find_sender_by_platform_msg_id` (M5: reply-target entry resolution of the recall, Section 8.1 step 1). WAL + `synchronous=NORMAL`. `chat_id` validation. | 19 |
| `tamako-memory` | `MemoryBackend` trait (`Send` futures) with `alias_targets` (entity resolution step 2) and `neighbors` (M5: the Section 8.2 direct-neighbor read path — valid edges only, `contains` excluded, 500-edge expansion limit truncated by `created_at` descending, one hop, Rule R5 identifier entry; `NeighborEdge::edge_id` is the Section 9.3 dedup key), `LbugBackend` on `lbug 0.18` (schema creation, transactional idempotent `upsert_batch`, `CHECKPOINT`, per-group isolation, `query_rows` read helper), deterministic UUID5 identifiers with NFKC normalization. Tested against the real driver. | 17 |
| `tamako-persona` | `persona.toml` loading, `PreambleRenderer` trait, `PetPreambleRenderer` with the Section 9.4 injection guardrail, example persona at the repo root. | 7 |
| `tamako-adapter-mock` | JSON replay fixture format, `MockAdapter` (event replay, action recording), 14-event demo fixture. | 6 |
| `tamako-adapter-teloxide` | Live Telegram adapter (teloxide 0.17). Pure `normalize` module (bot identity from get_me, display-name fallback chain, message/service/reaction/count normalization; synthetic `chat:{id}` for anonymous actors) plus the live `TeloxideAdapter` (polling task + bounded mpsc channel of 100, `next_group_event() -> GroupEvent { chat_id, event }` for multi-group routing per Rule P5, `PlatformAdapter` impl as the Rule A5 substitutability proof). Capability model (specs.md Section 4.2): `bot_chat_status` classifies the per-group membership (`BotChatStatus`: `Administrator` — owner counts as administrator — `Member`, `RestrictedOrOther`, `Unknown`; query failures map to `Unknown`). Outbound: `SendText` (optional reply via ReplyParameters), `React` (setMessageReaction); `SendMedia` → `AdapterError::Unsupported` (Phase 3). Outbound permission failures (missing rights or access) are tolerated: logged with the chat id, never fatal. Live Telegram smoke test ignored by default (`TAMAKO_LIVE_TELEGRAM=1` + `TELOXIDE_TOKEN`). | 50 (+1 ignored) |
| `tamako-agent` | All LLM concerns (the only rig consumer). `KnowledgeGraph` extraction types (serde + schemars 1.x), `KnowledgeExtractor` trait with the live `RigExtractor` (rig-core 0.41 completion + `output_schema`, Anthropic native structured output, default model `claude-haiku-4-5`) and the scripted `ScriptedExtractor`, conservative emoji/greeting skeleton detector (Section 7.2 rule 5), plain-Rust relationship-name validation (Section 6.3), entity resolution steps 1/2/4 with the Alias-node fallback (Section 7.4), `AgentDigestPipeline` with exponential backoff and dead-letter (Section 10.3). M4: the `endpoint` module (specs.md Section 13 endpoint portability: `LlmConfigValues` → `LlmEndpoints::resolve` with env-wins precedence and per-purpose overrides, `EndpointClient` over the two API families — Anthropic Messages and OpenAI chat completions — with base-URL and model overrides; a missing family API key is `AgentError::ProviderConfig`), the participation gate `RigGate` (structured output, post-validated in plain Rust; Section 9.6) with its scripted double, and the reply generator `RigReplyGenerator` (the M2 context→rig conversion seam; Section 9 step 4) with its scripted double. M5: the `recall` module — `ShallowRecall` (deterministic candidate extraction: sender/reply-target Person entries per Section 8.1 step 1, exact alias matches per step 2, the pure candidate-term tokenizer with documented Phase 1 limits; Section 9.3 dedup against `injected_memories`; zero candidates never call the cheap model), the conservative relevance gate `RigRelevanceGate` (Section 9.2, structured output, post-validated in plain Rust, hard cap) with its scripted double, and the Section 9.4 render of exactly one "I remember: ..." injection. Live-API smoke tests ignored by default (`TAMAKO_LIVE_TEST=1`). | 103 (+3 ignored) |

## 3. Key decisions and deviations so far

1. **LadybugDB has official Rust bindings: the crate `lbug`.** We pin
   `lbug = "0.18"` for storage-format v42 (`LBUG` magic) compatibility
   with the pinned Python `ladybug>=0.16.0,<=0.18.2`. Full finding and
   rationale: `docs/adr-0001-ladybugdb-binding.md`.
2. **The memory cache holds `Database` handles, not `Connection`
   handles.** An lbug `Connection` borrows its `Database`; a connection
   cache would be self-referential. A fresh connection opens per
   blocking call. Documented in `ARCHITECTURE.md` Section 6.
3. **`MemoryBackend` methods are desugared to return `Send` futures.**
   Implementations write plain `async fn`. This removed a
   `block_in_place` workaround and the multi-thread runtime requirement
   in the actor.
4. **Duplicate deliveries: one log row, but the wake counter counts
   each delivery.** The counter is a scheduling hint; the log row is
   the source of truth. The user has since locked this choice into
   specs.md Section 8.1.
5. **Digest timeout fallback does not fire when no digest has ever
   run.** Locked into specs.md Section 8.2.
6. **Wake intervals are normalized to whole milliseconds** so the
   persisted session encoding is lossless and restart-rebuild is
   bit-identical. At most 1 ms of jitter error at a 1-hour scale.
7. **Actor spawn returns the handle immediately**; startup runs inside
   the task. `snapshot()` is the FIFO barrier; `shutdown()` surfaces
   startup errors.
8. **Persona loading falls back** from `{data_root}/persona.toml` to
   the repo-root example to a built-in default. A Phase 0 convenience;
   the strict startup policy is a Phase 1 decision (Section 4).
9. **The ADR was written although the binding gate took the
   real-implementation branch** — it records the pin decision and the
   docs.rs build caveat (docs.rs fails for lbug ≥ 0.17; build docs
   locally).
10. **rig-core 0.41 has no `Extractor` type.** The structured extraction
    of specs.md Section 3 is implemented as a rig completion request
    with `output_schema(schemars::schema_for!(KnowledgeGraph))` —
    Anthropic native JSON-schema structured output. rig-core 0.41 forces
    schemars 1.x; schemars 0.8 must not enter the workspace. The default
    extraction model is `claude-haiku-4-5` (cheap tier).
11. **Deviation (superseded in M4): new configuration key
    `digest_model`** (config file) plus env var `TAMAKO_DIGEST_MODEL`
    (env wins). M1 reported this because specs.md Section 13 had no LLM
    keys. M4 added the full Section 13 LLM key set (`llm_api`,
    `llm_base_url`, the per-purpose overrides, `gate_model`,
    `reply_model`; entry 32), so the deviation is now the whole set,
    reported for spec backfill.
12. **An unresolvable person attaches to its Alias node.** Section 7.4
    defines no identifier for a person without a `tg_user_id` binding;
    a wrong binding is worse than a missing fact (step 4), so the
    zero-target case shares the ambiguity fallback. Every fallback
    Alias node carries `"attachment": "fallback"` for the primary
    resolution quality metric (specs.md Section 12).
13. **Alias-bound nodes carry `properties: None`.** A MERGE coalesce
    keeps the stored identity blob (`tg_user_id`, `display_name`); an
    overwrite would drop it. Regression-tested.
14. **`digest_max_retries` is the TOTAL number of attempts** (first try
    included), default 5. The interpretation is documented on
    `PipelineConfig::max_retries`.
15. **Digest completion time is wall-clock.** `last_digest_at` stamps
    the real completion instant (the timeout fallback measures real
    time), not the message timestamps of the batch.
16. **The live context is actor-owned; the actor performs the Rule C3
    removal itself.** specs.md Section 6.1 makes the actor the owner of
    the live context and serializes context mutations in the actor
    loop, so the M1 `PostDigestHook` seam (which receives only
    `chat_id` and the outcome, no actor state) could not host the
    removal. The actor's `DigestCompleted` handler removes items at or
    below the previous boundary, prunes `injected_memories` at the same
    cutoff, updates the session boundaries, persists, and only THEN
    calls the hook. The hook stays as a seam for stateless observers.
17. **Deviation: `injected_memories` gained a `content` column**
    (migration v2). specs.md Section 5.2 lists edge id, injection
    position, and range tag only. The Rule P1 restart rebuild must
    restore injection items bit-identically, and the rendered injection
    text is not derivable from the graph (an edge can change after the
    injection), so the text is persisted with the dedup row. specs.md
    should gain the column at its next revision.
18. **`prev_digest_boundary_msg_id` stays `None` until the SECOND
    digest completes.** The first digest has no previous chunk, so the
    one-chunk-lag cutoff `prev.unwrap_or(0)` removes nothing. From the
    second digest on, prev is the boundary that was current before
    that digest. Transitional note: a group digested under M1 (no prev
    key in its state table) rebuilds its context over the FULL log
    once; the next digest re-establishes the lag.
19. **Context rendering: human items carry the Section 7.2 step 4
    speaker label; bot speech is plain assistant text.** The label
    distinguishes group members; the model knows the assistant-role
    items are its own words. Edit rows enter the context as ordinary
    new items (uniform with the rebuild, which renders edits
    identically).
20. **The `ToolOutput` context item kind is reserved.** No producer
    exists in Phase 1; the kind is part of the model now so a later
    tool channel does not change the item enum.
21. **teloxide 0.17 is pinned (rustc ≥ 1.85).** `Polling::new` does not
    exist in 0.17; the builder API is used. `TELOXIDE_API_URL` is
    honored only by `Bot::from_env`, not by `Bot::new` — the adapter
    calls `set_api_url` itself, so a custom Bot API server works.
22. **A spawned task owns the polling listener.** `Polling::as_stream`
    borrows the listener mutably and teloxide 0.17 has no owned-stream
    API, so the task owns the listener and forwards updates over a
    bounded mpsc channel (capacity 100); the adapter consumes the
    channel. No Dispatcher, no dptree.
23. **The `PlatformAdapter` trait view drops the chat id.** The live
    binary routes multi-group traffic by chat id through the inherent
    `next_group_event` (Rule P5). The trait impl is the Rule A5
    substitutability proof.
24. **Aggregated reaction counts persist the emoji SET only.**
    `MessageReactionCountUpdated.total_count` values are dropped
    (`ReactionEvent` has no count field), and `old_emojis` is empty for
    count updates (the Bot API carries no previous state).
    `ReactionType::CustomEmoji` normalizes to its `custom_emoji_id`
    string; `Paid` is skipped.
25. **Display-name fallback: "First Last" → "@username" → numeric
    id.** A lone first name never wins; it is too weak as identity.
26. **Anonymous actors get a synthetic id.** Anonymous group admins
    (sent as the chat) and anonymous reaction actors get `chat:{id}`.
27. **Live mode serves only configured groups.** The config file is
    not watched; restart to pick up new groups.
28. **An adapter error that escapes `next_group_event` is fatal.**
    The binary performs a graceful shutdown, then the error propagates.
    The adapter already skips transient stream errors internally.
29. **Administrator status is recommended, not required.** The
    capability model of specs.md Section 4.2: `bot_chat_status`
    classifies the per-group membership from `getChatMember` (an owner
    counts as administrator; query failures map to `Unknown`). The
    binary caches the status in memory per group, re-evaluates it on
    each startup, and never persists it — membership can change at any
    time.
30. **Outbound permission failures are tolerated.** The enumerated
    teloxide `ApiError` rights/access variants (the `NotEnoughRights*`
    family, `BotBlocked`, `BotKicked`, `BotKickedFromSupergroup`,
    `BotKickedFromChannel`, `ChatNotFound`) plus
    a documented text catch on `ApiError::Unknown` ("not enough
    rights", "administrator rights") classify a denied outbound action.
    The adapter logs the failure with the chat id and continues —
    never fatal (specs.md Section 4.2).
31. **Privacy mode is not queryable through the Bot API.** The
    non-administrator + privacy-mode-on combination is operator
    guidance in the logs and the README, not runtime detection.
32. **Deviation (M4): new configuration keys `reply_staleness_threshold`
    and the Section 13 LLM key set.** `reply_staleness_threshold`
    (default 20 newer human messages) bounds the Section 6.2 recency
    re-check: a generated reply is DISCARDED when more than this many
    newer human messages arrived after the target (discard, not
    regenerate — the next wake is the natural retry). specs.md Section
    13 has no such key; it is reported for spec backfill. The M4 layer
    also implemented the Section 13 LLM keys (`llm_api`,
    `llm_base_url`, the per-purpose `digest`/`gate`/`reply` overrides,
    `gate_model`, `reply_model` — they supersede the M1 deviation of
    entry 11) and the state-table key `wake_last_row_id` (Section 5.2
    lists an open key set — "include"). The M4 timer cadence is
    `wake_interval / 8` clamped to [1 s, min(wake_floor, 5 min)]
    (documented on `trigger::timer_cadence`).
33. **The resets of spec steps 1 and 5 collapse into ONE reset at wake
    START.** Messages that arrive during a running wake count toward
    the next wake instead of being zeroed at completion (Section 6.2:
    inbound messages during a call do not interrupt it).
34. **The outbound channel policy is `try_send` with drop+log.** The
    actor never blocks on the sink; a full or closed channel degrades
    to a logged drop — the outbound raw-log row (persisted FIRST, Rule
    B1) is already the source of truth at that point (Section 4.2
    tolerates outbound failures). The binary owns the platform adapter
    and pumps one shared channel (capacity 100) into
    `PlatformAdapter::execute`; actions carry their chat id, so all
    actors share it.
35. **Outbound raw-log rows carry the synthetic id
    `bot-out:{nanos}`.** Rule A3 returns no platform id for a sent
    message, so the row uses a local synthetic id; nanosecond time
    keeps the idempotency key unique.
36. **A queued forced wake carries the intake timestamp of its forcing
    message.** The `forced_pending` slot of Section 6.2 starts the
    queued wake on the deterministic replay clock, not on the
    wall-clock completion instant of the previous wake.
37. **The zero-cadence guard falls back to one second.**
    `timer_cadence` can return zero (only when `wake_floor` is zero)
    and `tokio::time::interval` panics on a zero period, so
    `timer_period` falls back to 1 s; with a zero floor the intake path
    drives nearly every wake anyway. Related rig note: rig-core 0.41
    has no `CLAUDE_SONNET_4_5` constant (only `CLAUDE_SONNET_4_6`), so
    the default `reply_model` is a string literal.
38. **A message-driven wake can never observe `muted`.** Any human
    message clears the monologue lock at intake (Section 8.5), so only
    TICK-driven wakes are suppressed; the forced wake bypasses the gate
    entirely (Section 8.1) and is never suppressed by the lock.
39. **The injection is applied regardless of the participation
    outcome.** specs.md Section 9 runs recall (step 2) BEFORE the
    participation decision (step 3), and the decision itself consumes
    the memories (Section 9.6), so the `WakeCompleted` handler applies
    the planned injections first: one `injected_memories` row per edge
    id (position = the tail raw-log row id at wake start, range tag
    `{position}-{position}`), one `append_recall_injection` at the tail
    (Rule C2), and `injection_wakes_total` when at least one injection
    was applied. A gate-no wake still remembers. The reply-model
    snapshot also carries the injections from step 2 on. Edge case,
    documented: messages that arrive DURING the recall/gate/reply task
    shift the live placement of the injection behind them; the
    canonical placement (the restart rebuild) follows the recorded
    position.
40. **The Section 9.3 dedup key is the edge natural key.** The EDGE
    table has no id column (Section 6.1), so
    `NeighborEdge::edge_id()` renders
    `{source_id}|{relationship_name}|{target_id}|{valid_at}` (RFC
    3339). One `injected_memories` row per edge id per injection.
41. **Hub marking and the 90-day window of Section 8.2 are skipped in
    Phase 1.** The 500-edge truncation applies unconditionally to every
    node, which is strictly stronger than the hub rule (degree above
    1000) requires; the graphs are young, so the default time window is
    not applied. Both are documented on `MemoryBackend::neighbors`.
42. **An alias term with several targets enters through the Alias
    node.** The read path mirrors the write-path ambiguity fallback of
    Section 7.4 step 4: the direct neighbors of the Alias node are the
    `known_as`/`also_known_as` edges to the candidate entities, so the
    relevance gate still sees them. No guessing (Rule R5).
43. **Deviation (M5): new configuration key `recall_injection_cap`.**
    The hard cap of injected memories per wake (Section 9.2), default
    5. specs.md Section 13 has no such key; it is reported for spec
    backfill.
44. **The recall tokenizer is pure and deterministic (Phase 1).** No
    LLM term extraction. Split on non-alphanumerics (CJK survives),
    Section 7.1 normalization per token, stopword and short-token
    drops (single CJK characters dropped too), 20 terms per wake.
    Documented limits: no multi-word terms, no synonyms, no
    cross-language merging (Section 7.1 CAUTION), English-only
    stopwords.

## 4. Known gaps carried into Phase 1 (after M4)

Deliberately not done, in priority order:

1. **No real timer driver.** DONE in M4: a tokio interval inside the
   actor task evaluates the triggers on `timer_cadence` ticks
   (`MissedTickBehavior::Delay`, zero-cadence guard 1 s), shared with
   the explicit `Tick` command through one handler. This also closed
   the silent-group digest-timeout gap (Section 8.2).
2. **Wake counters.** DONE in M4: `wakes_total` and
   `participations_total` increment best effort (log and swallow on
   failure). DONE in M5: `injection_wakes_total` increments on every
   wake with at least one applied injection. DONE earlier:
   `digest_failures_total` and `dead_letters_total` (M1).
3. **Reaction events are not stored.** DONE: migration v3 (the
   `reactions` table of specs.md Section 5.2) plus the intake wiring
   shipped in M3. `InboundEvent::Reaction` persists at intake
   (Rule P1), idempotent under redelivery, as passive collection.
   NOTE: the Phase 2 warmup backoff will consume the table.
4. **Persona strict startup policy.** The Phase 0 fallback chain is
   lenient. M6 decides whether a missing persona file is an error.
5. **Member join/leave events have no consumer** and are not stored.
   No spec requirement yet; revisit if the digest needs membership
   history.
6. **lbug 0.19 upgrade check.** Before any upgrade past 0.18, verify
   storage-version compatibility (ADR-0001).
7. **Tail statistics cost one tail scan per digest evaluation.** The
   tail is bounded by the size thresholds in practice; a very long
   never-digested tail re-scans on every message. Acceptable at Phase 1
   scale; revisit with real traffic data (M6).
8. **Concept alias ambiguity collapses to the deterministic id.** An
   alias with several Concept targets falls back to
   `concept_id(normalized_name)` (fragmentation, the accepted Phase 1
   defect of dev-roadmap.md Section 3). Phase 2 merges.
9. **No producer for bot-speech context items yet.** DONE in M4: the
   wake send path appends `BotSpeech` items (Rule C1) after persisting
   the outbound raw-log row (Rule B1). DONE in M5: the recall worker
   produces the `RecallInjection` items through
   `append_recall_injection` and the `injected_memories` dedup table;
   the M2 lifecycle (bit-identical rebuild, C3 prune) covers them.

## 5. Phase 1 milestones

Refer to `dev-roadmap.md` Section 3 for the phase scope and Section 8
for the dependency order. Build order:

| Milestone | Content | Depends on |
|---|---|---|
| **M1: Digest pipeline end to end — COMPLETE** | rig extraction with the `KnowledgeGraph` schema (via completion + `output_schema`; rig-core 0.41 has no `Extractor` type), deterministic identifiers, entity resolution steps 1, 2, 4 (mention binding, exact alias, ambiguity fallback to the Alias node), single-transaction write + `CHECKPOINT`, exponential backoff with stable batch id, dead-letter, boundary advance, skeleton skip, trigger wired in the actor, M2 hook point (`PostDigestHook`). Runs against a replayed log before any live traffic. | Phase 0 store + memory |
| **M2: Context lifecycle — COMPLETE** | Live context as a materialized view: preamble at item 0 (C4), previous digested chunk, current tail. Structurally append-only between digests (C1/C2), message-id range tags, C3 removal with the one-chunk lag in the actor's digest-completion handler, `injected_memories` prune at the same cutoff (Section 10.2 step 4), `prev_digest_boundary_msg_id` persistence, bit-identical rebuild from the raw log and the session state (P1). M4 view (`messages_for_llm`) and M5 injection append API exposed. | M1 |
| **M3: teloxide adapter + live intake — COMPLETE** | New crate `tamako-adapter-teloxide`: pure normalization (messages with mention/reply resolution at intake per Section 4.2, edits, member join/leave service messages, named/anonymous/aggregated reactions, bot identity from get_me, synthetic `chat:{id}` for anonymous actors), long polling through a spawned listener task and a bounded mpsc channel (teloxide 0.17 builder API), `next_group_event` chat-id routing (Rule P5). `reactions` table migration v3 + idempotent reaction intake at intake time (Section 5.2, Rule P1, passive collection). Outbound `SendText`/`React` (`SendMedia` → Unsupported, Phase 3). `--live` binary mode: `TELOXIDE_TOKEN`, configured groups, lazy actor spawn, log-once ignore, ctrl-c graceful shutdown. | M1 (M2 not required) |
| **M4: Wake procedure + timer driver + counters — COMPLETE** | Wake contracts in tamako-core (`GateMessage`, `GateInput`, `GateDecision`, `RecallProvider`/`NoopRecall` seam, `ParticipationGate`, `ReplyGenerator`, `WakeServices`), the wake procedure of specs.md Section 9 in the actor (gather above `wake_last_row_id`, reset-at-start, spawned recall/gate/reply task, `WakeCompleted` send path with the Section 6.2 recency discard, outbound row first per Rule B1, monologue lock live), forced-wake queueing per Section 6.2, real tokio timer driver (`timer_cadence`, `MissedTickBehavior::Delay`), counters (`wakes_total`, `participations_total`), endpoint portability of Section 13 (`LlmEndpoints::resolve`, `EndpointClient` over both API families, env-wins overrides), gate and reply implementations in tamako-agent (`RigGate`, `RigReplyGenerator`, scripted doubles), binary wiring in both modes (one shared outbound channel; degrade to silence without a family API key). | M2, M3 |
| **M5: Shallow recall + injection protocol — COMPLETE** | The Section 8 read path in its Phase 1 form (`MemoryBackend::neighbors`: valid edges only, `contains` excluded, 500-edge truncation by `created_at` descending, one hop, Rule R5 entry; entry resolution steps 1–2 only — deterministic Person ids and the exact alias match, no vector search), the `ShallowRecall` worker (deterministic candidate extraction + pure tokenizer, Section 9.3 dedup against `injected_memories`, conservative cheap-model relevance gate with post-validated structured output, hard cap `recall_injection_cap` default 5 — reported for spec backfill, zero candidates never call the cheap model, gate failure means inject nothing), and the full injection protocol: exactly one "I remember: ..." assistant message per wake at the tail (Rule C2, applied regardless of the participation outcome), one `injected_memories` row per edge id (edge natural key as the dedup key), C3 prune at the previous boundary (M2 path, verified), digest exclusion of injections (Section 9.5), the preamble guardrail referenced (Section 9.4), and the `injection_wakes_total` counter (Section 12). | M2, M1 |
| **M6: Hardening** | Monologue lock verified under live traffic, persona strict startup policy, integration hardening, dead-letter visibility. Prepares the two-week test-group soak (Phase 1 exit). | M3–M5 |

Phase 1 exit criteria: `dev-roadmap.md` Section 3 (two weeks in one
test group without operator intervention; restart loses no message and
no digest boundary; visible dead-letter rate).
