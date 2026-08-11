# current-state.md — Tamako progress

A living document. Update it at every milestone. Last update: Phase 1
M6 (hardening) complete. Phase 1 is COMPLETE pending the two-week
test-group soak (`docs/soak-runbook.md`).

## 1. Where we are

- **Phase 0 (scaffolding): COMPLETE.** The exit criterion of
  `dev-roadmap.md` Section 2 is met and tested: the scripted mock
  adapter replays a recorded chat log, the actor persists the raw log
  and the session state, and a restart rebuilds the identical state.
- **Phase 1 (a living pet, MVP): COMPLETE pending the soak.** Scope in
  `dev-roadmap.md` Section 3. All six milestones are done; the Phase 1
  exit is the two-week test-group soak (`docs/soak-runbook.md`), run by
  the operator.
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
    pure tokenizer for alias terms (CJK n-grams since decision 58) —
    no LLM extraction), the same-fact collapse and the Section 9.3
    dedup against `injected_memories` (decision 58), and the
    conservative cheap-model relevance gate (Section 9.2, structured
    output,
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
  - **M6 (hardening): COMPLETE.** The persona strict startup policy is
    live (decision 45): `--live` requires `{data_root}/persona.toml`,
    `--allow-default-persona` is the escape hatch, `--replay` keeps the
    lenient chain. The operator mode `--status <chat_id>` /
    `--status-all` opens store.db read-only (verified safe against a
    live writer) and prints the Section 12 counters and rates, the
    boundaries, the muted state, and the most recent dead letters with
    batch id, attempts, error, and timestamp (decision 46); the M1
    dead-letter path already logs at ERROR with chat id and batch id
    (verified, unchanged). The monologue lock now has an
    integration test across a restart (`monologue_restart.rs`: engage,
    persisted muted state after respawn, tick-driven suppression,
    unlock on a human message). A production-critical race is fixed
    (decision 47): lbug 0.18 read-during-write on one group's Database
    SIGSEGVs; `LbugBackend` now serializes ALL per-group operations.
    The stability run (`stability_replay.rs`) loops 10 iterations of
    digests, wakes, injections, and interleaved restarts in ~1.4 s,
    asserting bit-identical rebuilds and zero dead letters. The
    tail-stats cost is measured and documented (Section 4, gap 7).
    `docs/soak-runbook.md` prepares the Phase 1 exit.
- Verification: `cargo build --workspace`, `cargo test --workspace`
  (380 tests, 0 failures, 4 ignored live tests: the live-API smoke
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
| `tamako` | CLI (`--replay`, `--live`, `--status <chat_id>`, `--status-all` — mutually exclusive, `--allow-default-persona`, `--data-root`, `--config`), wiring: endpoint resolution of specs.md Section 13 (`TriggerConfig` → `LlmConfigValues` → `LlmEndpoints::resolve`; a bad family string is a hard startup error), the live digest pipeline and the wake services (recall + gate + reply) built from the resolved endpoints (M5: `ShallowRecall` over the shared store and graph, `RigRelevanceGate` on the cheap gate endpoint, `recall_injection_cap` from the group config; a missing family API key degrades each to a warning and silence — the recall alone falls back to `NoopRecall`) — a missing family API key degrades each to a warning and silence — and ONE shared outbound channel (capacity 100) pumped into the platform adapter in both modes (replay: `select!` over the fixture stream plus a best-effort drain after the barrier; live: an arm of the main `select!`, failures warn and continue per Section 4.2). Live mode (M3): token from `TELOXIDE_TOKEN`, configured groups from `[groups.<chat_id>]` config tables, lazy actor spawn on the first event per group, non-configured groups logged once and ignored (Rule P5), capability detection at startup (one `getChatMember` call per configured group, in-memory cache, spawn-time re-check, one warning per non-administrator group, admin INFO / unknown INFO guidance), ctrl-c graceful shutdown of every actor with a summary log. M6: the persona strict startup policy (`--live` requires `{data_root}/persona.toml`, decision 45) and the read-only operator modes `--status` / `--status-all` over `Store::read_group_status` (Section 12 counters and rates, boundaries, muted state, recent dead letters with derived attempts, decision 46). Integration tests: `replay_restart` (Phase 0 exit criterion), `digest_replay` (end-to-end digest, idempotency, dead-letter then recovery, skeleton-only batch, restart keeps the boundary), `context_replay` (M2: context growth on intake, one-chunk-lag removal across two digests, bit-identical restart rebuild with injections, dedup prune), `wake_replay` (M4: threshold wake end to end over the replay fixture, gate-no silence, forced bypass of a muted group, live monologue lock with tick-driven suppression, recency discard), `recall_replay` (M5: injection end to end over a seeded graph — format/position/rows, Section 9.3 dedup across wakes of one chunk, zero candidates never call the cheap model, bit-identical restart rebuild, digest-time prune of the injection rows at the boundary, a Chinese n-gram matching an Alias end to end — decision 58) against the real store, the real graph, and scripted doubles, `monologue_restart` (M6: the Section 8.5 lock engages, persists across a restart, suppresses a tick-driven wake, unlocks on a human message), and `stability_replay` (M6: 10 iterations of digests, wakes, injections, and interleaved restarts — bit-identical rebuilds, no dead letters, ~1.4 s). Plus CLI, persona-policy, and status-rendering unit tests. | 47 |
| `tamako-core` | Normalized events/actions (A1–A4), `PlatformAdapter` trait, typed config with Section 13 defaults and per-group TOML overrides, wake/digest trigger scheduling, `tail_stats` over the raw-log tail, session state encode/decode incl. `last_digest_at` and `prev_digest_boundary_msg_id`, the `context` module (M2: `LiveContext` ordered item model with range tags, structurally append-only per C1/C2, C3 `remove_at_or_below`, C4 `reload_preamble`, bit-identical `rebuild`, `messages_for_llm` view, stats), per-group actor with raw-log-first intake (P1), context appends at intake (messages and edits), reaction intake (M3: persist into `reactions` through `Store::insert_reaction` at intake, idempotent under redelivery, passive collection — no context item, no wake-counter advance, no session mutation; member join/leave stay debug-only), startup context rebuild, C3 removal + dedup prune in the digest-completion handler, edit appends, forced-wake and wake stubs, LIVE digest wiring (spawn + `DigestCompleted` through the inbox, digest before wake, one digest in flight per group), the `digest` contract module (`DigestPipeline`, `DigestOutcome`, `PostDigestHook` — a seam for stateless post-digest observers; the actor performs C3 itself before the hook), the `wake` contract module (`GateMessage` (with sender id, reply target, and raw text for the recall), `GateInput`, `GateDecision`, `RecallProvider` returning `RecallOutcome`/`PlannedInjection` (M5) + `NoopRecall`, `ParticipationGate`, `ReplyGenerator`, `WakeServices` — same contract-in-core pattern as the digest), the LIVE wake procedure of specs.md Section 9 (gather new messages above `wake_last_row_id`, reset-at-start + `wakes_total`, spawned task for the recall/gate/reply calls, `WakeCompleted` handler with the Section 6.2 recency discard, outbound row first per Rule B1 with the synthetic id `bot-out:{nanos}`, `try_send` into the outbound channel, context append, monologue-lock bookkeeping, `participations_total`; M5: the `WakeCompleted` handler applies the planned injections BEFORE the participation outcome is known — one `injected_memories` row per edge id, `append_recall_injection` at the tail (Rule C2), `injection_wakes_total` — and the reply-model snapshot carries the injections from step 2 on), the M4 timer driver (tokio interval at `timer_cadence` inside the actor task, `MissedTickBehavior::Delay`, zero-cadence guard), and the forced-wake queueing of Section 6.2. | 83 |
| `tamako-store` | `store.db`: embedded migration runner (v1–v3), `messages` (idempotent insert, range read `list_messages_after`), `state` KV with atomic multi-write and counters, `injected_memories` (with the rendered `content` column since v2; `delete_injected_memories_up_to` for the Section 10.2 step 4 prune), `dead_letter`, `reactions` (migration v3, Section 5.2: `insert_reaction` with dedup UNIQUE INDEX with COALESCE → Duplicate). `find_sender_by_platform_msg_id` (M5: reply-target entry resolution of the recall, Section 8.1 step 1). `read_group_status` (M6: the read-only status query of the `--status` modes — SQLITE_OPEN_READ_ONLY, no create, no migrate, 2 s busy timeout; specs.md Sections 10.3 and 12). WAL + `synchronous=NORMAL`. `chat_id` validation. | 23 |
| `tamako-memory` | `MemoryBackend` trait (`Send` futures) with `alias_targets` (entity resolution step 2) and `neighbors` (M5: the Section 8.2 direct-neighbor read path — valid edges only, `contains` excluded, 500-edge expansion limit truncated by `created_at` descending, one hop, Rule R5 identifier entry; `NeighborEdge::edge_id` is the Section 9.3 dedup key), `LbugBackend` on `lbug 0.18` (schema creation, transactional idempotent `upsert_batch`, `CHECKPOINT`, per-group isolation, `query_rows` read helper; M6: ALL per-group operations serialized on a per-group async mutex — lbug 0.18 `Send + Sync` does not imply read-during-write safety, decision 47), deterministic UUID5 identifiers with NFKC normalization. Tested against the real driver. | 18 (incl. the concurrent-access regression test) |
| `tamako-persona` | `persona.toml` loading, `PreambleRenderer` trait, `PetPreambleRenderer` with the Section 9.4 injection guardrail, example persona at the repo root. | 14 |
| `tamako-adapter-mock` | JSON replay fixture format, `MockAdapter` (event replay, action recording), 14-event demo fixture. | 6 |
| `tamako-adapter-teloxide` | Live Telegram adapter (teloxide 0.17). Pure `normalize` module (bot identity from get_me, display-name fallback chain, message/service/reaction/count normalization; synthetic `chat:{id}` for anonymous actors) plus the live `TeloxideAdapter` (polling task + bounded mpsc channel of 100, `next_group_event() -> GroupEvent { chat_id, event }` for multi-group routing per Rule P5, `PlatformAdapter` impl as the Rule A5 substitutability proof). Capability model (specs.md Section 4.2): `bot_chat_status` classifies the per-group membership (`BotChatStatus`: `Administrator` — owner counts as administrator — `Member`, `RestrictedOrOther`, `Unknown`; query failures map to `Unknown`). Outbound: `SendText` (optional reply via ReplyParameters), `React` (setMessageReaction); `SendMedia` → `AdapterError::Unsupported` (Phase 3). Outbound permission failures (missing rights or access) are tolerated: logged with the chat id, never fatal. Live Telegram smoke test ignored by default (`TAMAKO_LIVE_TELEGRAM=1` + `TELOXIDE_TOKEN`). | 50 (+1 ignored) |
| `tamako-agent` | All LLM concerns (the only rig consumer). `KnowledgeGraph` extraction types (serde + schemars 1.x, conservative field-name aliases, decision 48), `KnowledgeExtractor` trait with the live `RigExtractor` (rig-core 0.41 completion + `output_schema`, Anthropic native structured output, default model `claude-haiku-4-5`) and the scripted `ScriptedExtractor`, conservative emoji/greeting skeleton detector (Section 7.2 rule 5), plain-Rust relationship-name validation (Section 6.3), entity resolution steps 1/2/4 with the Alias-node fallback (Section 7.4), `AgentDigestPipeline` with exponential backoff and dead-letter (Section 10.3). M4: the `endpoint` module (specs.md Section 13 endpoint portability: `LlmConfigValues` → `LlmEndpoints::resolve` with env-wins precedence and per-purpose overrides, `EndpointClient` over the two API families — Anthropic Messages and OpenAI chat completions — with base-URL and model overrides; the global-only `llm_session_id` resolves to the `x-opencode-session` default header on every request of both families through rig's `ClientBuilder::http_headers` (gateway session affinity, decision 57) and every successful completion logs the rig `Usage` fields (incl. `cached_input_tokens`) at DEBUG (decision 53's curated INFO lines untouched); a missing family API key is `AgentError::ProviderConfig`; robustness fix, decisions 48/50/52: per-purpose `structured_output` modes `schema`/`json_object`/`prompt_only` with default `schema` — `json_object` via rig `additional_params` on the OpenAI family only, Anthropic degrades to prompt-only — and ONE shared repair retry for every structured call: preamble field-name skeletons, exact JSON, one repair completion on a schema-invalid-but-JSON response, then the original error class), the participation gate `RigGate` (structured output, post-validated in plain Rust; Section 9.6) with its scripted double, and the reply generator `RigReplyGenerator` (the M2 context→rig conversion seam; Section 9 step 4) with its scripted double. M5: the `recall` module — `ShallowRecall` (deterministic candidate extraction: sender/reply-target Person entries per Section 8.1 step 1, exact alias matches per step 2, the pure candidate-term tokenizer with documented Phase 1 limits — two paths since decision 58: alphanumeric tokens with the 20-term budget, CJK n-grams of every maximal run (n 2..=5, 40-term budget, longer-first); the same-fact collapse by fact key (latest `valid_at` wins, decision 58) BEFORE the Section 9.3 dedup against `injected_memories`; zero candidates never call the cheap model; DEBUG logs distinguish the three gate outcomes — no candidates, selected none, gate failure with a WARN (decision 58)), the conservative relevance gate `RigRelevanceGate` (Section 9.2, structured output, post-validated in plain Rust, hard cap) with its scripted double, and the Section 9.4 render of exactly one "I remember: ..." injection. Live-API smoke tests ignored by default (`TAMAKO_LIVE_TEST=1`). | 139 (+3 ignored) |

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
    `reply_model`; entry 32), so the deviation is now the whole set.
    LANDED in specs.md Section 13.
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
    regenerate — the next wake is the natural retry). LANDED in specs.md
    Section 13. The M4 layer
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
    5. LANDED in specs.md Sections 9.2 and 13.
44. **The recall tokenizer is pure and deterministic (Phase 1).** No
    LLM term extraction. Split on non-alphanumerics (CJK survives),
    Section 7.1 normalization per token, stopword and short-token
    drops (single CJK characters dropped too), 20 terms per wake.
    Documented limits: no multi-word terms, no synonyms, no
    cross-language merging (Section 7.1 CAUTION), English-only
    stopwords.
45. **Persona strict startup policy (M6).** `--live` requires a
    persona file at `{data_root}/persona.toml`: a missing file is a
    hard startup error that points at the repo-root example as the
    template, and a malformed file is a hard error with the parse
    context — never a silent fallback (Rule C4: the preamble is the
    provider cache anchor; its source must be deliberate). The
    `--allow-default-persona` flag restores the lenient fallback
    chain for experiments; `--replay` always keeps the lenient chain
    (offline demos must not require setup). LANDED in specs.md Section
    5.3. Supersedes decision 8.
46. **The `--status` modes are read-only; capability is not printed;
    attempts are derived (M6).** `--status <chat_id>` / `--status-all`
    open store.db with `SQLITE_OPEN_READ_ONLY` (2 s busy timeout, no
    create, no migrate). Verified against primary sources: rusqlite
    0.32.1 bundles SQLite 3.46.0, and since SQLite 3.22.0 a read-only
    open of a WAL database is reliable while a live writer holds it
    (`-shm`/`-wal` present). Caveats surfaced to operators: after a
    CLEAN shutdown the WAL files are gone and a read-only query can
    fail on a read-only directory (an operator hint is printed);
    `immutable=1` is never used (it ignores the WAL). The per-group
    capability is never persisted (decision 29) and an offline
    inspection tool must not call Telegram, so it is not printed. The
    dead-letter row does not store an attempts count; a dead-lettered
    batch by definition exhausted `digest_max_retries` total attempts
    (specs.md Section 10.3 item 2), so the display prints the
    effective per-group value as `attempts=N (exhausted)`.
47. **All per-group LadybugDB operations are serialized inside
    `LbugBackend` (M6).** The `Send + Sync` markers of lbug 0.18 do
    NOT imply read-during-write safety: the C++ storage layer races
    lock-free readers of `FileHandle::pageStates` against writer-side
    `ConcurrentVector::resize` (annotated "Not thread-safe" upstream)
    and the CHECKPOINT truncate path — SIGSEGV in
    `BufferManager::optimisticRead` / `CSRNodeGroup::scanCommittedInMem`.
    Reproduced 5/5 unserialized; clean 5/5 with the fix. The actor
    spawns the digest pipeline and the wake procedure as concurrent
    tasks, so a recall `neighbors` scan could overlap an in-flight
    MERGE — a production-critical race found by the M6 stability run.
    `LbugBackend::with_conn` now holds a per-group async mutex for the
    full duration of every operation, reads and CHECKPOINT included
    (Section 6.1 rule 3; ADR-0001 addendum 2026-08-08; regression test
    `tamako-memory/tests/lbug_concurrent_access.rs`). An upstream
    report to LadybugDB is recommended, not filed.
48. **Live-extraction robustness fix: four layers, because the endpoint
    schema enforcement cannot be the only carrier of the field
    names.** `prompt_only` mode — and any endpoint that silently
    ignores `response_format` — leaves the model free to drift, and
    even a strict endpoint enforces names only when the schema reaches
    the wire. Four layers landed together: (1) the extraction, gate,
    and recall preambles now state the exact JSON field names with a
    skeleton; (2) conservative serde aliases on the `KnowledgeGraph`
    types tolerate field-name drift (`node_type` accepts `type`/`kind`,
    and likewise); (3) one shared repair retry in `endpoint.rs` — a
    response that parses as JSON but fails schema validation triggers
    ONE repair completion (broken JSON + validation error + schema,
    "fix the JSON to match the schema, change nothing else"); (4) the
    per-purpose `structured_output` mode config of decision 51.
49. **Opencode Go chat/completions models honor `json_schema` strict —
    recommended mode `schema`.** Live probe and verification
    2026-08-08 against `https://opencode.ai/zen/go/v1`: mimo-v2.5 /
    mimo-v2.5-pro accept and honor `response_format: {type:
    "json_schema", strict: true}` — a trivial schema and the real
    nested `KnowledgeGraph` schema ($defs + enum), 5/5 runs each,
    byte-exact canonical field names, `reasoning_tokens: 0`. The
    `json_object` mode verified identical. With NO `response_format`
    mimo-v2.5 burns ~85–300 reasoning tokens per call, may wrap the
    output in markdown fences, and ran 5× slower on the trivial probe
    (5.7 s vs 1.1 s); `prompt_only` is strictly worse on this endpoint.
    End to end: five consecutive clean replay digests with mimo-v2.5
    extraction (101-message batches, ~18–19 s wall each, boundary
    advanced, zero dead letters, no repair-path activity), gate+reply
    ~3.5 s combined, prompt caching active, no rate limits and no empty
    responses across ~20 live calls. Residual drift is non-schema:
    occasional UPPERCASE relationship names and the reserved `is_a` —
    caught by the existing plain-Rust post-validation, not a parse
    failure.
50. **`json_object` mode reaches the wire through rig's
    `additional_params` on the OpenAI family only.** rig-core 0.41 has
    no first-class json_object switch; the mode adds `response_format:
    {type: "json_object"}` as an additional parameter on the
    chat-completions path. The Anthropic Messages API has no json_object
    format, so on anthropic-compatible endpoints `json_object` degrades
    to prompt-only (no output schema, no response format).
    `prompt_only` drops the schema unconditionally on both families.
    Documented in the `endpoint` module docs.
51. **Deviation: new configuration keys `structured_output`,
    `digest_structured_output`, `gate_structured_output`,
    `reply_structured_output`** (values `schema`|`json_object`|
    `prompt_only`, default `schema`, per-group overridable like every
    trigger key, in `TriggerConfig`/`TriggerConfigToml`) plus the env
    vars `TAMAKO_STRUCTURED_OUTPUT` (global fallback) and
    `TAMAKO_DIGEST_STRUCTURED_OUTPUT`, `TAMAKO_GATE_STRUCTURED_OUTPUT`,
    `TAMAKO_REPLY_STRUCTURED_OUTPUT` (env wins). An unparsable value is
    a hard startup error (`AgentError::ProviderConfig`). LANDED in
    specs.md Section 13.
52. **Repair-retry behavior contract: one repair, then the original
    error class.** Only a response that IS JSON but fails schema
    validation triggers the single repair completion (decision 48
    layer 3); non-JSON text returns the original parse error
    immediately, and a failed repair call or an unparsable repaired
    text also returns the original error. The classification of the
    call site is unchanged, so the existing exponential backoff and the
    dead-letter path of specs.md Section 10.3 apply exactly as before.
53. **Curated one-line-per-wake/digest logging, plus `-v`.** At INFO the
    actor emits exactly one `wake` line per wake event and one `digest`
    line per completed digest (target `tamako_core::actor`), with a
    fixed field vocabulary. `wake`: `chat_id`, `trigger`
    (`message_count`|`interval`|`forced`), `injections`, `gate`
    (`participate`|`silent`|`bypassed_forced`|`muted`|
    `in_flight_skipped`), `reason` (ABSENT when the gate has none),
    `action` (`reply_sent`|`discarded_stale`|`nothing`), `reply_to`
    (ABSENT unless `reply_sent`). `digest`: `chat_id`, `batch_id`,
    `range` rendered `(old,new]`, `outcome` (`written` with
    `nodes`/`edges`, or `skeleton`). A failed wake emits one ERROR
    `wake procedure failed; skipping this wake`; a dead-lettered digest
    keeps the pipeline's ERROR line (specs.md Section 10.3) as its one
    line (the actor's variant drops to debug). Retry WARNs
    (`digest attempt failed; retry scheduled`) are interpreted as
    attempt-level signals, not digest-outcome lines — they do not count
    against the one-line guarantee. The old `(stub)` INFO lines are
    gone: demoted to DEBUG, reworded `wake services disabled; wake
    trigger ignored` (the path means no LLM key configured). `-v` /
    `--verbose` (accepted in every mode) sets the filter to
    `tamako=debug` when `RUST_LOG` is unset; `RUST_LOG` ALWAYS wins;
    default stays `info`. Telemetry threading, no behavior change:
    `GateDecision.reason` is now `Option<String>` carried into the wake
    line; `WakeScheduler::should_fire` delegates to the new
    `fire_reason` introspection (`FireReason` enum) so firing is
    bit-identical. The capture test lives in its own test binary
    (`tamako/tests/curated_log_replay.rs`) because tracing's
    process-global callsite interest cache defeats scoped subscribers
    in a shared test binary. No spec backfill needed: the feature adds
    no configuration keys (the flag and the log contract are operator
    surface, not spec configuration).
54. **Optional persona key `system_prefix`, rendered first and
    verbatim.** `PersonaConfig` gains `system_prefix: Option<String>`
    (TOML key `system_prefix`, optional, defaults to None):
    system-level alignment directives rendered VERBATIM before the
    identity line of the preamble, followed by exactly one blank line
    and the unchanged existing sections (personality, speaking style,
    behavioral rules, guardrail). When None, the rendered preamble is
    BIT-IDENTICAL to the previous format: the preamble is the provider
    cache anchor (Rule C4), so any byte-level change would invalidate
    the provider cache for ALL groups, and existing deployments that
    do not set the key must keep their cache. The injection guardrail
    stays code-owned by design and always renders last; it is not
    configurable. The strict persona startup policy of decision 45 is
    unchanged: a malformed persona file fails `--live` startup. LANDED
    in specs.md Section 5.3. NOTE (decision 55): on rebuilt main the
    shipped example carries NO `system_prefix`; the activated
    directive_zero prefix lives on the `catball-self-use` branch.
55. **Branch surgery: main rebuilt, self-use content moved to
    `catball-self-use`.** The user had accidentally committed self-use
    prompt tuning to main (persona expansions, the directive_zero
    activation, the gate-preamble rebalance with the 30-to-60-percent
    participation band, gate mention-name tuning). main was rebuilt
    from `889a6a2` keeping only the features (the `system_prefix`
    slot, its docs, the gitignore addition, the Section 5.3 backfill)
    and the `max_tokens` ceiling raises (`ddc0f24`) — the low bounds
    truncated live responses, so the raises are NOT part of the
    self-use set. Everything self-use is preserved on the
    `catball-self-use` branch, which holds the full old history plus
    the WIP commit. Consequence: on main the gate preamble is the
    pre-rebalance scarce-attention text and specs.md Section 12 keeps
    the "below 50 percent" target; the 30-to-60 band and its spec
   alignment exist only on `catball-self-use`. The branch divergence
   is deliberate, not drift.
56. **DeepSeek models on Opencode Go reject `json_schema`;
   structured-output support is a per-MODEL capability.** Operator
   verification 2026-08-10 in the live deployment: `deepseek-v4-flash`
   on `https://opencode.ai/zen/go/v1` answers a
   `response_format: {type: "json_schema"}` request with a
   `bad response format` error, so `prompt_only` is REQUIRED for it.
   Decision 49's "schema honored, `prompt_only` strictly worse" verdict
   covers the mimo models only; neither verdict generalizes across
    models on one endpoint. The soak deployment runs all three purposes
    on `deepseek-v4-flash` with `structured_output = "prompt_only"`, so
    the four robustness layers of decision 48 carry the field-name
    integrity. Probe the exact model of each purpose with
    `probe_endpoint`; do not extrapolate across models.
57. **Global-only `llm_session_id` config key, sent as the
    `x-opencode-session` header on every request of both API
    families.** The Opencode Go gateway honors an undocumented request
    header `x-opencode-session`: the sticky id drives upstream
    selection and prompt-cache affinity (the gateway deletes the header
    before forwarding; body params like `prompt_cache_key` are
    rejected). Live probe 2026-08-10 against `deepseek-v4-flash`:
    2304/2309 prompt tokens cached WITH the header vs 0 without.
    Resolution: env `TAMAKO_LLM_SESSION_ID` wins → global config key
    `llm_session_id` → default `"tamako"`; empty strings count as
    unset, so an empty header is never emitted. The header is applied
    through rig 0.41's `ClientBuilder::http_headers(HeaderMap)` on the
    anthropic AND the openai builders (the gateway's sticky logic is
    format-agnostic; `build()` inserts the API-key auth header only
    when the map does not carry it, so the two never clash); a session
    id that is not a valid header value is
    `AgentError::ProviderConfig`. A wire-level test (a local
    TcpListener answering a minimal OpenAI chat-completion response)
    proves the header reaches the request head. Per-group session ids
    were deliberately NOT done: one session id per deployment. The key
    sits in both `TriggerConfig` and `TriggerConfigToml` like the
    `structured_output` keys (a group table could set it, harmless) —
    the agent layer reads the global value only. Alongside it, every
    successful completion (repair calls included) logs the rig `Usage`
    fields `input_tokens` / `cached_input_tokens` /
    `cache_creation_input_tokens` / `output_tokens` at DEBUG, so the
    cache behavior of the header is observable with `-v`; the curated
    INFO lines of decision 53 stay untouched. max_tokens audit: the
    bounds (digest/gate/recall 262144, reply 131072) are already
    generous against the trigger-bounded inputs — digest leaves >200k
    tokens of reasoning burn headroom, the gate output is a three-field
    JSON — so NOTHING changed. The key LANDED in specs.md Section 13.
58. **Recall fixes A+B+D from the live soak: the CJK n-gram tokenizer,
    the same-fact collapse, and gate observability.** Live soak
    evidence drove all three: the alias-term path contributed almost
    nothing because a Chinese clause arrived as ONE whole-run token
    (decision 44 splits on non-alphanumerics and CJK survives, so an
    entire clause is one token) that can never exactly match an Alias;
    the same fact extracted twice with different `valid_at` burned the
    `recall_injection_cap` twice in one wake (observed live:
    `pinches_ears` twice in one 5-edge wake); and the fail-closed gate
    made a `prompt_only` parse hiccup indistinguishable from a true
    "not relevant". **Fix A — CJK n-grams.** The candidate-term
    tokenizer gains a second path: every maximal CJK run
    (U+4E00–9FFF, Extension-A U+3400–4DBF, kana U+3040–30FF — kana
    mixed in deliberately) yields all contiguous n-grams with n in
    2..=5, each normalized with the Section 7.1 `normalize`, deduped in
    one shared namespace with the alphanumeric terms, capped at the new
    constant `MAX_NGRAM_TERMS = 40` per wake (longer n-grams win, ties
    keep the first occurrence). The alphanumeric path and its
    `MAX_CANDIDATE_TERMS = 20` budget are unchanged. There is no
    whole-run token anymore; 1-char CJK runs still yield no term; there
    is no CJK stopword list (an unmatched n-gram costs one indexed
    miss). Constants, NOT configuration keys; no new configuration
    keys, no spec backfill needed (the decision 53 rationale: the
    tokenizer is internal candidate machinery, not operator surface).
    Rule R5 stands — every lookup stays an exact match — and the
    tokenizer stays pure and deterministic (decision 44). Decision
    44's documented-limits list shrinks: "single-character CJK tokens
    dropped" is reworded to "1-char CJK runs yield no term" (the n-gram
    path starts at 2); "no multi-word terms" now applies to the
    alphanumeric path only (CJK n-grams are multi-character terms by
    design); added limit: no CJK stopword list. **Fix B — same-fact
    collapse.** The candidate set collapses by fact key (`source_id`,
    `relationship_name`, `target_id`) ignoring `valid_at`, keeping the
    latest-`valid_at` edge, BEFORE the Section 9.3 dedup against
    `injected_memories`: an older already-injected edge must not shadow
    the newer same-fact edge; the corollary is that when the latest
    edge was already injected, the fact drops entirely. This makes the
    injection path honor dev-roadmap.md Section 3 item 5.
    `RecallCandidate` gained the pub fields
    `source_id`/`relationship_name`/`target_id`; the dedup key (edge
    natural key, decision 40), the injection format, the cap, and the
    C3 lifecycle are unchanged. **Fix D — gate observability.** DEBUG
    logs in `tamako_agent::recall` now distinguish the three gate
    outcomes with counts: `recall found no candidates; the relevance
    gate is not called` (chat_id, entry_count, fetched_edge_count),
    `the relevance gate selected no candidates; injecting nothing`
    (chat_id, candidate_count), and a gate failure keeps the existing
    WARN `the relevance gate failed; injecting nothing` (error class)
    plus a new DEBUG twin with chat_id and candidate_count; a fourth
    DEBUG `recall presents candidates to the relevance gate` (chat_id,
    candidate_count, collapsed_count) marks the gate input. A pure
    `recall_verdict` classifier drives the logs (unit-tested).
    Decision 53's curated INFO wake line is untouched; no state-table
    counters. Mid-soak migration property: code-only, no schema or
    configuration changes — a restart picks it up, and the restart
    rebuild stays bit-identical (Rule P1 — persisted renderings
    untouched).

## 4. Known gaps carried into Phase 1 (after M6)

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
   `digest_failures_total` and `dead_letters_total` (M1). DONE in M6:
   all counters are VISIBLE through the `--status` modes.
3. **Reaction events are not stored.** DONE: migration v3 (the
   `reactions` table of specs.md Section 5.2) plus the intake wiring
   shipped in M3. `InboundEvent::Reaction` persists at intake
   (Rule P1), idempotent under redelivery, as passive collection.
   NOTE: the Phase 2 warmup backoff will consume the table.
4. **Persona strict startup policy.** DONE in M6 (decision 45):
   `--live` requires `{data_root}/persona.toml`, with
   `--allow-default-persona` as the escape hatch and the lenient
   chain kept for `--replay`.
5. **Member join/leave events have no consumer** and are not stored.
   RE-DEFERRED (M6): no spec requirement exists; the events stay
   debug-only. Revisit if the digest needs membership history.
6. **lbug 0.19 upgrade check.** RE-DEFERRED (M6): the workspace stays
   on `lbug = "0.18"`. Before any upgrade, verify storage-version
   compatibility and re-run the concurrent-access regression test
   (ADR-0001 and its 2026-08-08 addendum).
7. **Tail statistics cost one tail scan per digest evaluation.** DONE
   in M6 (measured, not optimized): `tail_stats` costs ~12 µs at the
   trigger-bounded tail (100 rows), ~1 ms at 10k rows, ~11 ms at 100k
   rows; the full actor path (`list_messages_after` + `tail_stats`)
   costs ~21 ms at 20k rows. With digests enabled the trigger keeps
   the tail bounded (~100 rows), so the cost is negligible. The
   pathological case is a long never-digested tail (pipeline disabled
   without an API key): ~1 ms per evaluation at 10k rows is still
   acceptable at Phase 1 scale. Revisit with real traffic data.
8. **Concept alias ambiguity collapses to the deterministic id.** An
   alias with several Concept targets falls back to
   `concept_id(normalized_name)` (fragmentation, the accepted Phase 1
   defect of dev-roadmap.md Section 3). Phase 2 merges.
9. **`ChatMember` updates are ignored.** RE-DEFERRED (M6): the poller
   does not request `chat_member` updates, so Telegram never sends
   them and the adapter has nothing to consume. Membership is
   re-evaluated with `getChatMember` at every startup (decision 29).
   No Phase 1 consumer exists; the soak runbook watches capability
   through the startup log lines.
10. **No producer for bot-speech context items yet.** DONE in M4: the
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
| **M5: Shallow recall + injection protocol — COMPLETE** | The Section 8 read path in its Phase 1 form (`MemoryBackend::neighbors`: valid edges only, `contains` excluded, 500-edge truncation by `created_at` descending, one hop, Rule R5 entry; entry resolution steps 1–2 only — deterministic Person ids and the exact alias match, no vector search), the `ShallowRecall` worker (deterministic candidate extraction + pure tokenizer, Section 9.3 dedup against `injected_memories`, conservative cheap-model relevance gate with post-validated structured output, hard cap `recall_injection_cap` default 5 — LANDED in specs.md Sections 9.2 and 13, zero candidates never call the cheap model, gate failure means inject nothing), and the full injection protocol: exactly one "I remember: ..." assistant message per wake at the tail (Rule C2, applied regardless of the participation outcome), one `injected_memories` row per edge id (edge natural key as the dedup key), C3 prune at the previous boundary (M2 path, verified), digest exclusion of injections (Section 9.5), the preamble guardrail referenced (Section 9.4), and the `injection_wakes_total` counter (Section 12). | M2, M1 |
| **M6: Hardening — COMPLETE** | Persona strict startup policy (decision 45: `--live` requires `{data_root}/persona.toml`, `--allow-default-persona` escape hatch, `--replay` stays lenient; spec backfill for Section 5.3). Operator mode `--status <chat_id>` / `--status-all` (read-only store open, Section 12 counters and rates, boundaries, muted state, recent dead letters with batch id, derived attempts, error, timestamp; decision 46; the M1 dead-letter ERROR log with chat id and batch id verified). Monologue lock integration test across a restart (`monologue_restart.rs`). Production-critical fix: per-group serialization of ALL LadybugDB operations (lbug 0.18 read-during-write SIGSEGV, decision 47, ADR-0001 addendum, `lbug_concurrent_access.rs` regression test). Stability run (`stability_replay.rs`: 10 iterations of digests, wakes, injections, restarts; bit-identical rebuilds; zero dead letters; ~1.4 s). Tail-stats cost measured and documented (Section 4, gap 7). Dependency audit: no new dependencies; schemars stays 1.x. `docs/soak-runbook.md` prepares the two-week test-group soak (Phase 1 exit). | M3–M5 |

Phase 1 exit criteria: `dev-roadmap.md` Section 3 (two weeks in one
test group without operator intervention; restart loses no message and
no digest boundary; visible dead-letter rate).
