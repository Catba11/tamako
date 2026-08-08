# current-state.md — Tamako progress

A living document. Update it at every milestone. Last update: Phase 0
complete, entering Phase 1.

## 1. Where we are

- **Phase 0 (scaffolding): COMPLETE.** The exit criterion of
  `dev-roadmap.md` Section 2 is met and tested: the scripted mock
  adapter replays a recorded chat log, the actor persists the raw log
  and the session state, and a restart rebuilds the identical state.
- **Phase 1 (a living pet, MVP): NOT STARTED.** Scope in
  `dev-roadmap.md` Section 3. Milestones in Section 5 below.
- Verification: `cargo build --workspace`, `cargo test --workspace`
  (61 tests, 0 failures), `cargo clippy --workspace --all-targets --
  -D warnings`, `cargo fmt --all --check` — all clean.

## 2. What exists and is tested

| Crate | Content | Tests |
|---|---|---|
| `tamako` | CLI (`--replay`, `--data-root`, `--config`), wiring, demo summary. Integration test `replay_restart` covers the Phase 0 exit criterion against the real LadybugDB backend. | 1 |
| `tamako-core` | Normalized events/actions (A1–A4), `PlatformAdapter` trait, typed config with Section 13 defaults and per-group TOML overrides, wake/digest trigger scheduling (jitter bounds, floor, whichever-first; 10 000-sample jitter test), session state encode/decode, per-group actor with raw-log-first intake (P1), edit appends, forced-wake and trigger stubs. | 26 |
| `tamako-store` | `store.db`: embedded migration runner, `messages` (idempotent insert), `state` KV with atomic multi-write and counters, `injected_memories`, `dead_letter`. WAL + `synchronous=NORMAL`. `chat_id` validation. | 11 |
| `tamako-memory` | `MemoryBackend` trait (`Send` futures), `LbugBackend` on `lbug 0.18` (schema creation, transactional idempotent `upsert_batch`, `CHECKPOINT`, per-group isolation), deterministic UUID5 identifiers with NFKC normalization. Tested against the real driver. | 10 |
| `tamako-persona` | `persona.toml` loading, `PreambleRenderer` trait, `PetPreambleRenderer` with the Section 9.4 injection guardrail, example persona at the repo root. | 7 |
| `tamako-adapter-mock` | JSON replay fixture format, `MockAdapter` (event replay, action recording), 14-event demo fixture. | 6 |

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

## 4. Known gaps carried into Phase 1

Deliberately not done in Phase 0, in priority order:

1. **Entity resolution does not exist.** `upsert_batch` is fully
   implemented, but nothing produces resolved nodes/edges yet. The
   digest pipeline (M1) supplies them.
2. **No real timer driver.** Wake timing is evaluated on `Tick`
   commands and message timestamps only. M4 adds the tokio interval
   driver.
3. **Counters are not wired.** The mechanism (`increment_counter`) and
   the key set exist; `wakes_total`, `participations_total`,
   `injection_wakes_total`, `digest_failures_total` start at zero. M4
   wires them to the wake and digest procedures.
4. **Reaction events are not stored.** Phase 0 logs them at debug
   level only. NOTE: specs.md Section 5.2 (user-locked) now requires a
   `reactions` table in `store.db`, collected from intake time in
   Phase 1 — reaction data is not recoverable later. This is a
   migration v2 plus intake wiring, part of M3.
5. **Persona strict startup policy.** The Phase 0 fallback chain is
   lenient. M6 decides whether a missing persona file is an error.
6. **Member join/leave events have no consumer** and are not stored.
   No spec requirement yet; revisit if the digest needs membership
   history.
7. **lbug 0.19 upgrade check.** Before any upgrade past 0.18, verify
   storage-version compatibility (ADR-0001).

## 5. Phase 1 milestones

Refer to `dev-roadmap.md` Section 3 for the phase scope and Section 8
for the dependency order. Build order:

| Milestone | Content | Depends on |
|---|---|---|
| **M1: Digest pipeline end to end** | rig `Extractor` with the `KnowledgeGraph` schema, deterministic identifiers, entity resolution steps 1, 2, 4 (mention binding, exact alias, ambiguity fallback to the Alias node), single-transaction write + `CHECKPOINT`, exponential backoff with stable batch id, dead-letter, context boundary advance. Runs against a replayed log before any live traffic. | Phase 0 store + memory |
| **M2: Context lifecycle** | Live context as a materialized view: preamble, previous digested chunk, current tail. Append-only between digests (C1), range tags, C3 removal at digest time. Rebuild from the raw log and the session state (P1). | M1 |
| **M3: teloxide adapter + live intake** | Long polling, mention and reply resolution at intake (Section 4.2), `reactions` table migration + reaction logging from intake time (Section 5.2, user-locked), outbound actions. | M1 (M2 not required) |
| **M4: Wake procedure + timer driver + counters** | Real tokio timer driving the wake trigger, participation decision (cheap model), reply generation (main model), monologue lock in live operation, counter wiring (`wakes_total`, `participations_total`, `digest_failures_total`), structured logs. | M2, M3 |
| **M5: Shallow recall + injection protocol** | Exact alias match only; the full injection protocol: "I remember:" format, preamble guardrail, `injected_memories` deduplication, C2 append-at-tail, C3 lifecycle, digest exclusion of injections. | M2, M1 |
| **M6: Hardening** | Monologue lock verified under live traffic, persona strict startup policy, integration hardening, dead-letter visibility. Prepares the two-week test-group soak (Phase 1 exit). | M3–M5 |

Phase 1 exit criteria: `dev-roadmap.md` Section 3 (two weeks in one
test group without operator intervention; restart loses no message and
no digest boundary; visible dead-letter rate).
