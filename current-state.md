# current-state.md — Tamako progress

A living document. Update it at every milestone. Last update: Phase 1
M3 (teloxide adapter + live intake) complete.

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
  - M4–M6: NOT STARTED. Milestones in Section 5 below.
- Verification: `cargo build --workspace`, `cargo test --workspace`
  (207 tests, 0 failures, 2 ignored live tests: the live-API smoke
  test and the live Telegram smoke test),
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
| `tamako` | CLI (`--replay`, `--live` — mutually exclusive, `--data-root`, `--config`), wiring incl. the live digest pipeline when `ANTHROPIC_API_KEY` is set, demo summary with digest boundary and dead-letter count. Live mode (M3): token from `TELOXIDE_TOKEN`, configured groups from `[groups.<chat_id>]` config tables, lazy actor spawn on the first event per group, non-configured groups logged once and ignored (Rule P5), capability detection at startup (one `getChatMember` call per configured group, in-memory cache, spawn-time re-check, one warning per non-administrator group, admin INFO / unknown INFO guidance), ctrl-c graceful shutdown of every actor with a summary log. Integration tests: `replay_restart` (Phase 0 exit criterion), `digest_replay` (end-to-end digest, idempotency, dead-letter then recovery, skeleton-only batch, restart keeps the boundary), and `context_replay` (M2: context growth on intake, one-chunk-lag removal across two digests, bit-identical restart rebuild with injections, dedup prune) against the real LadybugDB backend. Plus CLI and capability-mapping unit tests. | 16 |
| `tamako-core` | Normalized events/actions (A1–A4), `PlatformAdapter` trait, typed config with Section 13 defaults and per-group TOML overrides, wake/digest trigger scheduling, `tail_stats` over the raw-log tail, session state encode/decode incl. `last_digest_at` and `prev_digest_boundary_msg_id`, the `context` module (M2: `LiveContext` ordered item model with range tags, structurally append-only per C1/C2, C3 `remove_at_or_below`, C4 `reload_preamble`, bit-identical `rebuild`, `messages_for_llm` view, stats), per-group actor with raw-log-first intake (P1), context appends at intake (messages and edits), reaction intake (M3: persist into `reactions` through `Store::insert_reaction` at intake, idempotent under redelivery, passive collection — no context item, no wake-counter advance, no session mutation; member join/leave stay debug-only), startup context rebuild, C3 removal + dedup prune in the digest-completion handler, edit appends, forced-wake and wake stubs, LIVE digest wiring (spawn + `DigestCompleted` through the inbox, digest before wake, one digest in flight per group), the `digest` contract module (`DigestPipeline`, `DigestOutcome`, `PostDigestHook` — a seam for stateless post-digest observers; the actor performs C3 itself before the hook). | 57 |
| `tamako-store` | `store.db`: embedded migration runner (v1–v3), `messages` (idempotent insert, range read `list_messages_after`), `state` KV with atomic multi-write and counters, `injected_memories` (with the rendered `content` column since v2; `delete_injected_memories_up_to` for the Section 10.2 step 4 prune), `dead_letter`, `reactions` (migration v3, Section 5.2: `insert_reaction` with dedup UNIQUE INDEX with COALESCE → Duplicate). WAL + `synchronous=NORMAL`. `chat_id` validation. | 17 |
| `tamako-memory` | `MemoryBackend` trait (`Send` futures) with `alias_targets` (entity resolution step 2), `LbugBackend` on `lbug 0.18` (schema creation, transactional idempotent `upsert_batch`, `CHECKPOINT`, per-group isolation, `query_rows` read helper), deterministic UUID5 identifiers with NFKC normalization. Tested against the real driver. | 12 |
| `tamako-persona` | `persona.toml` loading, `PreambleRenderer` trait, `PetPreambleRenderer` with the Section 9.4 injection guardrail, example persona at the repo root. | 7 |
| `tamako-adapter-mock` | JSON replay fixture format, `MockAdapter` (event replay, action recording), 14-event demo fixture. | 6 |
| `tamako-adapter-teloxide` | Live Telegram adapter (teloxide 0.17). Pure `normalize` module (bot identity from get_me, display-name fallback chain, message/service/reaction/count normalization; synthetic `chat:{id}` for anonymous actors) plus the live `TeloxideAdapter` (polling task + bounded mpsc channel of 100, `next_group_event() -> GroupEvent { chat_id, event }` for multi-group routing per Rule P5, `PlatformAdapter` impl as the Rule A5 substitutability proof). Capability model (specs.md Section 4.2): `bot_chat_status` classifies the per-group membership (`BotChatStatus`: `Administrator` — owner counts as administrator — `Member`, `RestrictedOrOther`, `Unknown`; query failures map to `Unknown`). Outbound: `SendText` (optional reply via ReplyParameters), `React` (setMessageReaction); `SendMedia` → `AdapterError::Unsupported` (Phase 3). Outbound permission failures (missing rights or access) are tolerated: logged with the chat id, never fatal. Live Telegram smoke test ignored by default (`TAMAKO_LIVE_TELEGRAM=1` + `TELOXIDE_TOKEN`). | 50 (+1 ignored) |
| `tamako-agent` | All LLM concerns (the only rig consumer). `KnowledgeGraph` extraction types (serde + schemars 1.x), `KnowledgeExtractor` trait with the live `RigExtractor` (rig-core 0.41 completion + `output_schema`, Anthropic native structured output, default model `claude-haiku-4-5`) and the scripted `ScriptedExtractor`, conservative emoji/greeting skeleton detector (Section 7.2 rule 5), plain-Rust relationship-name validation (Section 6.3), entity resolution steps 1/2/4 with the Alias-node fallback (Section 7.4), `AgentDigestPipeline` with exponential backoff and dead-letter (Section 10.3). Live-API smoke test ignored by default (`TAMAKO_LIVE_TEST=1`). | 42 (+1 ignored) |

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
11. **Deviation: new configuration key `digest_model`** (config file)
    plus env var `TAMAKO_DIGEST_MODEL` (env wins). specs.md Section 13
    has no LLM keys; AGENT.md Section 6.3 permits this with a reported
    deviation. specs.md should gain the key at its next revision.
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

## 4. Known gaps carried into Phase 1 (after M3)

Deliberately not done, in priority order:

1. **No real timer driver.** Wake timing is evaluated on `Tick`
   commands and message timestamps only. M4 adds the tokio interval
   driver. NOTE for M4: the digest timeout fallback (Section 8.2) is
   evaluated on the same points; a group with no traffic and a stale
   digest needs the timer to fire the fallback.
2. **Wake counters are not wired.** `wakes_total`,
   `participations_total`, `injection_wakes_total` start at zero. M4
   wires them. DONE: `digest_failures_total` and `dead_letters_total`
   increment in the digest pipeline (M1, best effort).
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
9. **No producer for bot-speech context items yet.** The append API
   (`LiveContext::append_bot_speech`) and the outbound-row rebuild
   path exist; the live call site arrives with M4's reply generation
   (Rule B1). Same for recall injections: the append API, the
   `content`-carrying dedup table, and the lifecycle exist; the
   producer is M5.

## 5. Phase 1 milestones

Refer to `dev-roadmap.md` Section 3 for the phase scope and Section 8
for the dependency order. Build order:

| Milestone | Content | Depends on |
|---|---|---|
| **M1: Digest pipeline end to end — COMPLETE** | rig extraction with the `KnowledgeGraph` schema (via completion + `output_schema`; rig-core 0.41 has no `Extractor` type), deterministic identifiers, entity resolution steps 1, 2, 4 (mention binding, exact alias, ambiguity fallback to the Alias node), single-transaction write + `CHECKPOINT`, exponential backoff with stable batch id, dead-letter, boundary advance, skeleton skip, trigger wired in the actor, M2 hook point (`PostDigestHook`). Runs against a replayed log before any live traffic. | Phase 0 store + memory |
| **M2: Context lifecycle — COMPLETE** | Live context as a materialized view: preamble at item 0 (C4), previous digested chunk, current tail. Structurally append-only between digests (C1/C2), message-id range tags, C3 removal with the one-chunk lag in the actor's digest-completion handler, `injected_memories` prune at the same cutoff (Section 10.2 step 4), `prev_digest_boundary_msg_id` persistence, bit-identical rebuild from the raw log and the session state (P1). M4 view (`messages_for_llm`) and M5 injection append API exposed. | M1 |
| **M3: teloxide adapter + live intake — COMPLETE** | New crate `tamako-adapter-teloxide`: pure normalization (messages with mention/reply resolution at intake per Section 4.2, edits, member join/leave service messages, named/anonymous/aggregated reactions, bot identity from get_me, synthetic `chat:{id}` for anonymous actors), long polling through a spawned listener task and a bounded mpsc channel (teloxide 0.17 builder API), `next_group_event` chat-id routing (Rule P5). `reactions` table migration v3 + idempotent reaction intake at intake time (Section 5.2, Rule P1, passive collection). Outbound `SendText`/`React` (`SendMedia` → Unsupported, Phase 3). `--live` binary mode: `TELOXIDE_TOKEN`, configured groups, lazy actor spawn, log-once ignore, ctrl-c graceful shutdown. | M1 (M2 not required) |
| **M4: Wake procedure + timer driver + counters** | Real tokio timer driving the wake trigger, participation decision (cheap model), reply generation (main model), monologue lock in live operation, counter wiring (`wakes_total`, `participations_total`, `digest_failures_total`), structured logs. | M2, M3 |
| **M5: Shallow recall + injection protocol** | Exact alias match only; the full injection protocol: "I remember:" format, preamble guardrail, `injected_memories` deduplication, C2 append-at-tail, C3 lifecycle, digest exclusion of injections. | M2, M1 |
| **M6: Hardening** | Monologue lock verified under live traffic, persona strict startup policy, integration hardening, dead-letter visibility. Prepares the two-week test-group soak (Phase 1 exit). | M3–M5 |

Phase 1 exit criteria: `dev-roadmap.md` Section 3 (two weeks in one
test group without operator intervention; restart loses no message and
no digest boundary; visible dead-letter rate).
