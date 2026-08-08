# Development Roadmap for the Tamako Group-Pet Bot

Version: 0.1 Draft
Status: For review
Companion documents: `specs.md`, `proposed-graph-database-specs.md`

## 1. Phasing strategy

Two rules govern the phase split:

- F1: Ship a living pet early. The smallest slice that intakes messages, digests them into the graph, wakes up, and replies is one milestone. Memory quality improves later.
- F2: Defer only what sits behind a stable interface. An item may move to a later phase only if its Phase 1 substitute produces the same outputs through the same call sites. Section 5 lists the items that pass this test. Section 6 lists the items that fail it and must be built in Phase 1 even though they are tempting to skip.

## 2. Phase 0 — Scaffolding

No LLM calls in this phase.

1. Repository and crate layout: adapter crate, actor crate, memory crate, persona crate.
2. `store.db` schema and migrations: `messages`, `state`, `injected_memories`, `dead_letter`. Refer to `specs.md` Section 5.2.
3. LadybugDB schema creation and connection management. Refer to `proposed-graph-database-specs.md` Sections 5 and 6.
4. Platform adapter trait with a mock implementation. Refer to `specs.md` Section 4.
5. Per-group actor skeleton: inbox, trigger state, persistence and rebuild of session state. Refer to `specs.md` Section 6.

Exit criterion: a scripted mock adapter replays a recorded chat log; the actor persists the raw log and the session state; a restart rebuilds the identical state.

## 3. Phase 1 — A living pet (MVP)

Scope:

1. teloxide adapter, long polling, mention and reply resolution at intake. Refer to `specs.md` Section 4.
2. Trigger set: message intake, `Wake` with jitter and floor, `Digest` with size thresholds and timeout. Refer to `specs.md` Section 8. Warmup is excluded.
3. Context lifecycle: rolling window with one-chunk lag, append-only between digests. Refer to `specs.md` Section 7.
4. Digest pipeline: rig extraction (completion request plus JSON output schema; rig-core 0.41 has no `Extractor` type) with the `KnowledgeGraph` schema, deterministic identifiers, entity resolution steps 1, 2, and 4 only (mention binding, exact alias match, ambiguity fallback to the Alias node), single-transaction write, `CHECKPOINT`, exponential backoff, dead-letter. Refer to `specs.md` Section 10.
5. Fact storage without invalidation: all predicates behave as multi-value. `valid_at` is written; `invalid_at` stays NULL. Read-side ordering takes the latest edge per subject and predicate.
6. Wake procedure: participation decision with a cheap model, reply generation with the main model, monologue lock. Refer to `specs.md` Sections 8.5 and 9.
7. Shallow recall: exact alias match only. The full injection protocol is built here: format, guardrail, deduplication table, lifecycle. Refer to `specs.md` Section 9. The recall worker's query depth is the only part deferred.
8. Persona service with static configuration. No hot reload.
9. Minimal counters in the state table: participation rate, injection rate, digest failure count. Structured logging only, no metrics backend.

Explicitly out of scope for Phase 1: everything in Section 5.

Exit criteria:

- The bot lives in one test group for two weeks without operator intervention.
- The graph accumulates Person, Alias, and Concept nodes with facts; the bot answers direct questions about recent group events.
- A process restart loses no message and no digest boundary.
- The dead-letter rate is visible; a zero rate is not required.

Known accepted defect: concept fragmentation from the missing vector pre-screen. One concept may exist as several nodes with different surface forms. Phase 2 provides the merge tooling.

## 4. Phase 2 — Memory that remembers

Scope:

1. sqlite-vec sidecar in `store.db`: embedding writes in the digest transaction, name and description embeddings for Person, Alias, and Concept. Refer to `proposed-graph-database-specs.md` Section 7.6.
2. Vector pre-screen in entity resolution, step 3 with thresholds 0.92 and 0.80, plus the LLM confirmation call for the middle band. Refer to Section 7.4 of that document.
3. Merge tool for the fragmented Phase 1 graph: candidate pairs from the vector index, one LLM confirmation per pair, edge re-pointing, node tombstone.
4. Deep recall: graph expansion per the rules of Section 8.2 of that document, the relevance gate, and the "who discussed X" pattern through a full-text sidecar on `edge_text`.
5. Fact validity: the predicate registry, single-value invalidation in one transaction. No data migration is required; the schema columns exist since Phase 1. Refer to Section 7.5 of that document.
6. Manual invalidation command for the owner. Refer to `specs.md` Section 14.
7. Warmup trigger with silence detection, engagement tracking, and the soft backoff. Refer to `specs.md` Sections 8.4 and 8.5.
8. Persona hot reload. Cache invalidation is an accepted, deliberate event. Rule C4 of `specs.md` applies.
9. Metrics backend with the full metric set of `specs.md` Section 12.

Exit criteria:

- The injection rate settles in the healthy band of 20 to 40 percent.
- The fallback attachment rate stops rising after the merge tool runs.
- Warmup messages produce measurable engagement in at least one group.

## 5. Phase 3 — Deferred and open

Ordered by expected value:

1. Vision captioning under the constraints of `specs.md` Section 14. The caption model has no tool access; caption text is data, marked with delimiters in the digest input.
2. Negation detection and the `supersedes` edge. Refer to `proposed-graph-database-specs.md` Section 7.5.
3. The repair tool for dead-letter batches, using the retained raw log.
4. `EditedMessage` semantics. The Phase 1 behavior ignores edits and logs them.
5. The `is_a` concept hierarchy.
6. The Matrix adapter. The contract of `specs.md` Section 4 is the acceptance test.

## 6. Heavy items that decouple cleanly

This section is the rationale for the phase split. Each item is heavy, and its Phase 1 substitute produces identical outputs at identical call sites.

| Item | Why heavy | Phase 1 substitute | Interface that protects the split |
|---|---|---|---|
| Vector entity resolution, `proposed-graph-database-specs.md` 7.4 step 3 | Embedding pipeline, vector index, threshold calibration, confirmation calls | Steps 1, 2, 4 only | The resolution stage returns a node id regardless of the method. Callers do not change. |
| Deep recall | Relevance-gate tuning, graph expansion rules, hub truncation | Exact alias match, direct neighbors only | The wake procedure consumes "zero or more injected memories". The injection protocol exists in Phase 1; only the producer improves. |
| Fact invalidation and predicate registry | Registry maintenance, transactional invalidation, registry update process | All predicates multi-value; read side picks the latest by `valid_at` | The schema carries `valid_at` and `invalid_at` from day one. Enabling invalidation is a write-path change only, no migration. |
| Warmup and engagement feedback | Reaction event processing, backoff state machine, content strategy | Absent | An independent trigger. No coupling to `Wake` or `Digest`. |
| Full-text sidecar on `edge_text` | Second index to maintain and keep consistent | Graph traversal only | Query-side component behind the read path. |
| Metrics backend | Infrastructure, dashboards | Counters in the state table plus structured logs | Metric names and definitions live in `specs.md` Section 12. Only the sink changes. |
| Persona hot reload | Reload coordination across group actors | Static config read at startup | The persona service interface is fixed in Phase 1. |
| Dead-letter repair tool | Reprocessing logic, conflict handling | Manual inspection of the dead-letter table | The raw log is retained; reprocessing is a replay. |

## 7. Items that must not be deferred

These items are cheap now and expensive later. Building them late requires data migration or breaks Rule P1 of `specs.md`.

| Item | Reason |
|---|---|
| Raw message log with the mention and reply map at intake | The log is the only repair source for skipped batches and the only reconstruction source for the context. Mention metadata is not recoverable later. |
| `valid_at` and `invalid_at` as real columns, written from day one | Retrofitting temporal columns onto a live graph is a full-table migration. |
| `injected_memories` table and the deduplication set | The injection lifecycle of `specs.md` Section 9.5 depends on it. Rebuilding the lifecycle later changes digest removal semantics. |
| Per-group directory layout with both files | Moving groups between layouts is operator pain with zero benefit. |
| Idempotent batch writes under deterministic identifiers with a stable batch id across retries | Backoff without idempotency corrupts the graph silently. |
| Bot self-memory: the bot's own messages in the raw log and in the digest input | Rule B1 of `specs.md`. The rolling context removal of Rule C3 assumes it. |
| Monologue lock | Two counters in the state table. Deferring it risks the bot's reputation in its first groups, which no later phase can repair. |

## 8. Dependency order inside each phase

Within Phase 1, the build order is:

1. `store.db` schema, graph schema, actor skeleton (Phase 0 carry-over).
2. teloxide adapter and intake path.
3. Digest pipeline end to end against a replayed log, before any live traffic.
4. Context lifecycle and the wake procedure.
5. Shallow recall with the full injection protocol.
6. Two-week test-group soak, then Phase 2 planning with real calibration data.

The thresholds 0.92 and 0.80, the relevance gate, and the participation gate are calibrated with the data of the Phase 1 soak. Refer to the open items of both specifications.
