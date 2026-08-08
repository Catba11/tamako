# current-state.md — Tamako progress

A living document. Update it at every milestone. Last update: Phase 1
M1 (digest pipeline) complete.

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
  - M2–M6: NOT STARTED. Milestones in Section 5 below.
- Verification: `cargo build --workspace`, `cargo test --workspace`
  (119 tests, 0 failures, 1 ignored live-API smoke test),
  `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` — all clean.
- Offline demo: `cargo run -p tamako-agent --example digest_demo`.
  Live replay: `ANTHROPIC_API_KEY=... cargo run -- --replay
  tamako-adapter-mock/fixtures/replay_chat.json` (without the key the
  replay runs with digests disabled).

## 2. What exists and is tested

| Crate | Content | Tests |
|---|---|---|
| `tamako` | CLI (`--replay`, `--data-root`, `--config`), wiring incl. the live digest pipeline when `ANTHROPIC_API_KEY` is set, demo summary with digest boundary and dead-letter count. Integration tests: `replay_restart` (Phase 0 exit criterion) and `digest_replay` (end-to-end digest, idempotency, dead-letter then recovery, skeleton-only batch, restart keeps the boundary) against the real LadybugDB backend. | 6 |
| `tamako-core` | Normalized events/actions (A1–A4), `PlatformAdapter` trait, typed config with Section 13 defaults and per-group TOML overrides, wake/digest trigger scheduling, `tail_stats` over the raw-log tail, session state encode/decode incl. `last_digest_at`, per-group actor with raw-log-first intake (P1), edit appends, forced-wake and wake stubs, LIVE digest wiring (spawn + `DigestCompleted` through the inbox, digest before wake, one digest in flight per group), the `digest` contract module (`DigestPipeline`, `DigestOutcome`, `PostDigestHook` — the M2 Rule C3 wiring point). | 34 |
| `tamako-store` | `store.db`: embedded migration runner, `messages` (idempotent insert, range read `list_messages_after`), `state` KV with atomic multi-write and counters, `injected_memories`, `dead_letter`. WAL + `synchronous=NORMAL`. `chat_id` validation. | 12 |
| `tamako-memory` | `MemoryBackend` trait (`Send` futures) with `alias_targets` (entity resolution step 2), `LbugBackend` on `lbug 0.18` (schema creation, transactional idempotent `upsert_batch`, `CHECKPOINT`, per-group isolation, `query_rows` read helper), deterministic UUID5 identifiers with NFKC normalization. Tested against the real driver. | 12 |
| `tamako-persona` | `persona.toml` loading, `PreambleRenderer` trait, `PetPreambleRenderer` with the Section 9.4 injection guardrail, example persona at the repo root. | 7 |
| `tamako-adapter-mock` | JSON replay fixture format, `MockAdapter` (event replay, action recording), 14-event demo fixture. | 6 |
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

## 4. Known gaps carried into Phase 1 (after M1)

Deliberately not done, in priority order:

1. **No context lifecycle (M2).** The live context does not exist yet.
   The wiring point is ready: the actor calls `PostDigestHook` after
   every successful digest (Rule C3 removal goes there), and
   `last_digest_boundary_msg_id`/`last_digest_at` persist across
   restarts.
2. **No real timer driver.** Wake timing is evaluated on `Tick`
   commands and message timestamps only. M4 adds the tokio interval
   driver. NOTE for M4: the digest timeout fallback (Section 8.2) is
   evaluated on the same points; a group with no traffic and a stale
   digest needs the timer to fire the fallback.
3. **Wake counters are not wired.** `wakes_total`,
   `participations_total`, `injection_wakes_total` start at zero. M4
   wires them. DONE in M1: `digest_failures_total` and
   `dead_letters_total` increment in the digest pipeline (best effort).
4. **Reaction events are not stored.** NOTE: specs.md Section 5.2
   (user-locked) requires a `reactions` table in `store.db`, collected
   from intake time in Phase 1 — reaction data is not recoverable
   later. This is a migration v2 plus intake wiring, part of M3.
5. **Persona strict startup policy.** The Phase 0 fallback chain is
   lenient. M6 decides whether a missing persona file is an error.
6. **Member join/leave events have no consumer** and are not stored.
   No spec requirement yet; revisit if the digest needs membership
   history.
7. **lbug 0.19 upgrade check.** Before any upgrade past 0.18, verify
   storage-version compatibility (ADR-0001).
8. **Tail statistics cost one tail scan per digest evaluation.** The
   tail is bounded by the size thresholds in practice; a very long
   never-digested tail re-scans on every message. Acceptable at Phase 1
   scale; revisit with real traffic data (M6).
9. **Concept alias ambiguity collapses to the deterministic id.** An
   alias with several Concept targets falls back to
   `concept_id(normalized_name)` (fragmentation, the accepted Phase 1
   defect of dev-roadmap.md Section 3). Phase 2 merges.

## 5. Phase 1 milestones

Refer to `dev-roadmap.md` Section 3 for the phase scope and Section 8
for the dependency order. Build order:

| Milestone | Content | Depends on |
|---|---|---|
| **M1: Digest pipeline end to end — COMPLETE** | rig extraction with the `KnowledgeGraph` schema (via completion + `output_schema`; rig-core 0.41 has no `Extractor` type), deterministic identifiers, entity resolution steps 1, 2, 4 (mention binding, exact alias, ambiguity fallback to the Alias node), single-transaction write + `CHECKPOINT`, exponential backoff with stable batch id, dead-letter, boundary advance, skeleton skip, trigger wired in the actor, M2 hook point (`PostDigestHook`). Runs against a replayed log before any live traffic. | Phase 0 store + memory |
| **M2: Context lifecycle** | Live context as a materialized view: preamble, previous digested chunk, current tail. Append-only between digests (C1), range tags, C3 removal at digest time. Rebuild from the raw log and the session state (P1). | M1 |
| **M3: teloxide adapter + live intake** | Long polling, mention and reply resolution at intake (Section 4.2), `reactions` table migration + reaction logging from intake time (Section 5.2, user-locked), outbound actions. | M1 (M2 not required) |
| **M4: Wake procedure + timer driver + counters** | Real tokio timer driving the wake trigger, participation decision (cheap model), reply generation (main model), monologue lock in live operation, counter wiring (`wakes_total`, `participations_total`, `digest_failures_total`), structured logs. | M2, M3 |
| **M5: Shallow recall + injection protocol** | Exact alias match only; the full injection protocol: "I remember:" format, preamble guardrail, `injected_memories` deduplication, C2 append-at-tail, C3 lifecycle, digest exclusion of injections. | M2, M1 |
| **M6: Hardening** | Monologue lock verified under live traffic, persona strict startup policy, integration hardening, dead-letter visibility. Prepares the two-week test-group soak (Phase 1 exit). | M3–M5 |

Phase 1 exit criteria: `dev-roadmap.md` Section 3 (two weeks in one
test group without operator intervention; restart loses no message and
no digest boundary; visible dead-letter rate).
