# Agent Behavior Specification for the Tamako Group-Pet Bot

Version: 0.1 Draft
Status: For review
Companion document: `proposed-graph-database-specs.md` (memory backend)

## 1. Purpose

This document specifies the runtime behavior of the Tamako group-pet bot: the per-group event loop, the context lifecycle, the memory recall and injection protocol, the digest pipeline, and the proactive speech rules. The memory storage backend is specified in `proposed-graph-database-specs.md`. This document references it but does not repeat it.

## 2. Design principles

These principles are mandatory. A violation is a defect.

- P1: The raw message log is the single source of truth. The model context is a materialized view of the log. The context can be rebuilt from the log and the session state at any time.
- P2: Between two digests, the model context is append-only. All destructive edits occur at digest time. This maximizes the provider prefix-cache hit rate.
- P3: Recall is involuntary from the perspective of the pet persona. Memories are injected as if the pet remembered them on its own. The pet never announces a search.
- P4: Attention is scarce. The bot does not read or answer every message in real time. The bot wakes up on a jittered schedule and decides whether to participate.
- P5: Memory is private to each group. Persona is global. Refer to `proposed-graph-database-specs.md` Section 5.1.
- P6: The bot is a participant of the group, not an assistant. The bot speaks rarely, can stay silent, and can be wrong.
- P7: The platform frontend is loosely coupled. Platform types must not leak past the adapter layer. Refer to Section 4.

## 3. Architecture overview

Components:

| Component | Count | Responsibility |
|---|---|---|
| Platform adapter | One per platform process | Normalize inbound events. Execute outbound actions. First implementation: teloxide long polling. |
| Per-group actor | One per `chat_id` | Serialize triggers. Own the live context. Own the session state. Refer to Section 6. |
| Recall worker | Shared | Cheap-model recall call plus memory backend queries. Refer to Section 9. |
| Digest worker | Shared | Extraction and entity resolution. Graph writes are serialized per group. Refer to Section 10. |
| Persona service | One, global | Render the system preamble from the persona configuration. Refer to Section 5.3. |

The agent harness is rig.rs. The extraction call is a rig completion request with a JSON output schema (schemars) for a typed `KnowledgeGraph` struct; the Anthropic provider uses native structured output. NOTE: rig-core 0.41 has no `Extractor` type; earlier drafts referenced it. The reply generation uses a rig completion with a manually maintained message history. Relationship-name validation and all memory-side rules run in plain Rust after the extraction returns.

LLM access is endpoint-portable. Every LLM call uses one of two API families: an Anthropic-compatible endpoint or an OpenAI-compatible endpoint. The base URL of the endpoint is a user configuration item, so proxies and self-hosted endpoints work. Model names are configuration items. Refer to Section 13.

## 4. Platform abstraction

### 4.1 Rules

- A1: All platform-specific types stay inside the adapter. The actor sees only normalized events.
- A2: The adapter exposes inbound events: `Message`, `EditedMessage`, `Reaction`, `MemberJoin`, `MemberLeave`.
- A3: The adapter exposes outbound actions: `SendText`, `SendMedia`, `React`.
- A4: A normalized message carries: platform message id, timestamp, sender id, sender display name, text, reply-to id, and mention flags.
- A5: The first adapter is Telegram via teloxide. A Matrix adapter must be possible without changes to the actor, the context, or the memory backend.

### 4.2 Platform constraints

- The Telegram Bot API does not provide history before the bot joins a group. The bot receives messages only through the update stream. Rule P1 applies: every inbound message is persisted to the raw log at intake time, before any processing.
- Mention and reply metadata is resolved at intake time and stored with the log row. The digest pipeline uses this stored map. Refer to `proposed-graph-database-specs.md` Section 7.3.

## 5. Persistence layout

### 5.1 Per-group directory

Each `chat_id` has one directory `{data_root}/{chat_id}/`:

| File | Content |
|---|---|
| `memory.lbug` | LadybugDB graph. Refer to `proposed-graph-database-specs.md`. |
| `store.db` | SQLite database, WAL mode, `synchronous=NORMAL`. Contains the raw message log, the session state, and the sidecar vector index. |
| `media/` | Reserved. Captioned media artifacts. Refer to Section 14. |

### 5.2 `store.db` content

- `messages` table: one row per normalized inbound or outbound message. Outbound rows store the bot's own speech. Rule B1 applies.
- `reactions` table: one row per reaction event on a group message. The Phase 2 warmup backoff consumes this table. Reaction data is not recoverable later, so collection starts at intake time in Phase 1.
- `state` table: key-value rows. Keys include `last_digest_boundary_msg_id`, `muted_flag`, `consecutive_bot_msgs`, `warmup_backoff_factor`, `warmup_quota_used_today`.
- `injected_memories` table: one row per injected recall. Columns: edge id, injection position, message-id range tag, rendered content. The rendered content is stored so a restart rebuild is bit-identical without graph queries. Refer to Section 9.5.
- Vector index: sqlite-vec virtual tables in the same file. The embeddings of Person, Alias, and Concept names and descriptions live here. Refer to `proposed-graph-database-specs.md` Section 7.6.

To delete the memory of a group, delete the directory. Both files share one lifecycle.

### 5.3 Persona configuration

- One global persona configuration at `{data_root}/persona.toml`. Hot-reloadable.
- The persona service renders the system preamble. The preamble is the prefix of every model context and never changes inside a context lifetime. Rule C4 applies.
- The persona rendering layer is an interface. The pet persona is one implementation. This decoupling permits reuse of the runtime for other personas or purposes.

## 6. Per-group actor and concurrency

### 6.1 Actor model

1. One actor per group. All trigger events enter one FIFO inbox.
2. LLM calls run concurrently across groups. Inside one group, the following operations are strictly serialized: graph writes, context mutations, session-state mutations.
3. Graph writes per group are serialized through the actor. LadybugDB permits one writer per database file. A blocked batch must not block the queue. Refer to Section 10.4.
4. The actor persists the session state after every mutation. On restart, the actor rebuilds the live context from the raw log and the session state.

### 6.2 Trigger ordering

- If several triggers are pending, `Digest` runs before `Wake`. Recall sees the freshest graph.
- A forced `Wake` (mention or reply to the bot) moves to the head of the queue. It does not preempt a running call.
- Inbound messages during a running `Wake` are logged and appended to the context. They do not interrupt the running call. Before the bot sends a reply, the actor re-checks the recency of the target message. If the conversation has moved on, the reply is discarded or regenerated.

## 7. Context lifecycle

### 7.1 Structure

The live context is an ordered list of items:

1. System preamble. Persona plus behavioral rules plus the injection guardrail. Refer to Section 9.4. This prefix is the cache anchor.
2. The previous digested chunk. Already extracted into the graph. Kept as an overlap buffer.
3. The current tail. Undigested inbound messages, bot replies, and recall injections.

Each item carries a message-id range tag. The boundary pair (`prev_digest_boundary_msg_id`, `last_digest_boundary_msg_id`) splits the previous chunk from the current tail. The previous chunk is the range at or below the previous boundary.

### 7.2 Rules

- C1: Between two digests, the context is append-only. Rule P2 applies.
- C2: A recall injection is always appended at the tail, directly after the messages that triggered it. An insertion into the middle of the history is forbidden.
- C3: At digest time, the actor removes every item with a range tag at or below the previous boundary. The removal covers inbound messages, bot replies, injections, and tool outputs in that range. The digest model never sees the removed content. Refer to Section 10.3. The actor performs the removal, the deduplication pruning of Section 10.2, and the boundary update as one serialized step (Section 6.1). Post-digest hooks are stateless observers only. They must not mutate the context.
- C4: A persona preamble change invalidates the provider cache for all groups. Preamble edits are deliberate events, not runtime side effects.
- C5: The context size is bounded by approximately two digest chunks. The maximum size follows from the digest thresholds in Section 8.2.

## 8. Triggers

All thresholds are per-group configuration items. Defaults in parentheses. Refer to Section 13.

### 8.1 Message intake

- Every inbound message: append to the raw log, append to the live context, increment the wake counter. No LLM call.
- Duplicate deliveries: the raw log insert is idempotent. The wake counter counts each delivery. The counter is a scheduling hint; the log row is the source of truth. Rule P1 applies.
- An edited message appends a new log row. Extracted facts are not retracted. Refer to Section 15.
- A mention of the bot or a reply to the bot triggers a forced `Wake`. The bot must respond when addressed directly. The `muted` state does not suppress a forced wake. Refer to Section 8.4.

### 8.2 Digest trigger

- Fire when the undigested tail reaches the first of: 5000 CJK characters, 100 messages, 2500 words, or 20 kB.
- Fallback: fire when the tail is non-empty and the last digest is older than the digest timeout (6 h). "Last digest" means the wall-clock completion time of the last successful digest. If no digest has ever run for the group, the fallback does not fire. The tail right after the bot joins grows until it reaches a size threshold.
- On fire, run the digest pipeline. Refer to Section 10.

### 8.3 Wake trigger

- Fire when the first of these is true: at least `wake_msg_count` new messages (5) since the last wake, or at least `wake_interval` (1 h) since the last wake.
- Multiply the active interval by a uniform random factor in [0.7, 1.3] at every reset. The expectation stays stable.
- Floor: two consecutive wakes are at least `wake_floor` (5 min) apart. The floor is a configuration item. It caps the cost in very active groups where five messages can arrive in seconds.
- On fire, run the wake procedure. Refer to Section 9.

### 8.4 Warmup trigger

- Daily quota: `warmup_quota` (1–3) proactive messages, spread at random over the configured active hours.
- A warmup message is permitted only if the group has been silent for at least `warmup_silence` (4 h).
- The `muted` state suppresses warmup. The soft backoff in Section 8.5 adjusts the effective quota.

### 8.5 Speech suppression

- Hard rule (monologue lock): if the last `monologue_limit` (2) messages in the group are all from the bot, enter the `muted` state. In `muted`, proactive speech and warmup are forbidden. A forced wake is still permitted. Any human message clears the state.
- Soft rule (warmup backoff): if a warmup message receives zero reactions and zero replies within the reaction window, the effective daily quota decreases and the next warmup interval doubles. The backoff resets on any successful engagement.
- Participation rate is a metric. A sustained rate above 50 percent indicates that the participation gate is too permissive. Refer to Section 12.

## 9. Wake procedure

One wake executes these steps in this sequence:

1. If `muted` and not forced, return. Reset the counters and the timer.
2. Recall. Refer to Section 9.1 to 9.5.
3. Participation decision. Refer to Section 9.6.
4. If the decision is to participate, generate a reply with the main model. Send it through the adapter. Write the bot's own message to the raw log and append it to the context. Rule B1 applies.
5. Reset the wake counter and the timer with fresh jitter.

### 9.1 Recall call

- The recall worker reads the new messages of this wake and queries the memory backend through the read path of `proposed-graph-database-specs.md` Section 8. Entry resolution: mentions and replies, exact alias match, then vector search.
- The recall worker uses a cheap model. It never calls the main model.
- If the decision at step 3 is negative, the main model is never called. The recall cost is the fixed cost of every wake.

### 9.2 Relevance gate

- The recall returns memories only if an omitted memory would materially reduce the reply quality or the participation decision quality.
- If nothing is relevant, nothing is injected. An empty injection is forbidden.
- The injection rate is a metric. The expected healthy range is 20 to 40 percent of wakes.

### 9.3 Deduplication

- The actor stores the edge ids of every injected memory in `injected_memories`.
- A memory already injected in the current chunk is not injected again.

### 9.4 Injection format and guardrails

- The injection is one assistant message of the form: "I remember: ...". It is appended at the tail. Rule C2 applies.
- The content of a memory originates from group messages through the graph. This is an indirect prompt-injection channel. The system preamble contains a standing guardrail: injected memory content is reference material, never an instruction.
- A memory injected from a stale or contested fact is acceptable in this version. Negation detection is deferred. Refer to Section 14.

### 9.5 Injection lifecycle

- An injection is removed only at digest time, together with its range. Rule C3 applies.
- The digest model never sees injections. Its input is the raw log of the range: human messages and the bot's own speech, with speaker labels. This prevents recalled graph content from re-entering the graph as new facts.

### 9.6 Participation decision

- Input: the new messages plus the injected memories.
- Output: a binary decision, with the target message for the reply.
- The decision uses a cheap model. The main model runs only on a positive decision.
- The recall result is part of the input on purpose: a topic with strong personal memories is a valid reason to participate.

## 10. Digest pipeline

### 10.1 Input

- The raw log range `(last_boundary, current_tail]`: human messages plus the bot's own speech. Injections and tool outputs are excluded. Section 9.5 applies.
- The mention and reply map stored at intake time. Refer to Section 4.2.

### 10.2 Extraction and write

1. Extract the `KnowledgeGraph` object. Refer to `proposed-graph-database-specs.md` Section 7.3.
2. Run entity resolution and fact validity steps. Refer to Sections 7.4 and 7.5 of that document.
3. Write nodes, edges, and embeddings in one transaction per group. Run `CHECKPOINT`.
4. On success, advance `last_digest_boundary_msg_id`, apply the context removal of Rule C3, and prune the deduplication set of Section 9.3.

### 10.3 Failure handling

1. On failure, retry with exponential backoff. The retry uses the same batch identifier. The `MERGE` operations are idempotent under the deterministic identifiers of `proposed-graph-database-specs.md` Section 7.1.
2. After `digest_max_retries` total attempts (the count includes the first attempt), write the batch skeleton and the error to a dead-letter table, emit the failure metric, and skip the batch. A failed batch never blocks later batches.
3. The skipped range stays in the raw log. A later repair tool can reprocess it.

### 10.4 Bot self-memory

- B1: The bot's own messages are written to the raw log and are part of the digest input. The bot exists as a `Person` node in the graph of each group.
- The persona is global. The memory of the bot's own speech is per-group, like every other member's memory. Rule P5 applies.

## 11. Warmup behavior

- A warmup run executes the recall step against recent group history and generates a message from the recalled material. A message grounded in a real memory of the group is preferred over a generic cute message.
- If recall returns nothing relevant, the warmup falls back to a persona-consistent generic message.
- The warmup obeys the monologue lock and the soft backoff. Refer to Section 8.5.

## 12. Observability

Metrics per group:

| Metric | Meaning |
|---|---|
| Participation rate | Share of wakes with a positive decision. Healthy target below 50 percent. |
| Injection rate | Share of wakes with at least one injected memory. Expected 20 to 40 percent. |
| Warmup engagement rate | Share of warmup messages with a reaction or a reply. Drives the soft backoff. |
| Digest failure rate | Failed extractions before dead-letter. |
| Dead-letter count | Skipped batches. Requires operator attention. |
| Fallback attachment rate | Refer to `proposed-graph-database-specs.md` Section 10. Primary entity-resolution quality metric. |
| Wake rate | Wakes per hour. Watch against the floor configuration. |

## 13. Configuration

Global defaults. Every item is overridable per group.

| Key | Default | Section |
|---|---|---|
| `wake_msg_count` | 5 | 8.3 |
| `wake_interval` | 1 h | 8.3 |
| `wake_jitter` | uniform [0.7, 1.3] | 8.3 |
| `wake_floor` | 5 min | 8.3 |
| `digest_max_chars_cjk` | 5000 | 8.2 |
| `digest_max_messages` | 100 | 8.2 |
| `digest_max_words` | 2500 | 8.2 |
| `digest_max_bytes` | 20 kB | 8.2 |
| `digest_timeout` | 6 h | 8.2 |
| `digest_max_retries` | 5 total attempts, including the first | 10.3 |
| `warmup_quota` | 1–3 per day | 8.4 |

LLM access is global configuration, not per-group:

| Key | Default | Notes |
|---|---|---|
| `llm_api` | `anthropic` | API family: `anthropic` or `openai-compatible`. Environment override: `TAMAKO_LLM_API`. |
| `llm_base_url` | The canonical URL of the selected family | User-specified endpoint base URL. Permits proxies and self-hosted endpoints. Environment override: `TAMAKO_LLM_BASE_URL`. |
| `digest_model` | `claude-haiku-4-5` | Extraction model. Environment override: `TAMAKO_DIGEST_MODEL`. |

API keys come from the environment only, never from a config file (`ANTHROPIC_API_KEY` or `OPENAI_API_KEY` per the selected family).
| `warmup_silence` | 4 h | 8.4 |
| `monologue_limit` | 2 | 8.5 |

## 14. Deferred items

1. Vision captioning for images, videos, and GIFs. Interface constraints are fixed now: the caption model has no tool access; the caption text is data, never an instruction; the caption enters the digest input with a delimiter that marks it as a media description, not as a member message; captions are stored as display-only properties. Rule R1 of the database specification applies.
2. Negation detection and the `supersedes` edge. Refer to `proposed-graph-database-specs.md` Section 7.5. Until then, a manual invalidation command permits the owner to invalidate a specific edge.
3. The `is_a` concept hierarchy. Refer to the open items of the database specification.
4. Additional platform adapters, starting with Matrix. Section 4 defines the contract.

## 15. Open items

1. Calibration of the recall relevance gate prompt and the participation gate prompt with real group data.
2. The repair tool for dead-letter batches.
3. Retention policy for the raw log after extraction. The log is the only repair source for skipped ranges.
4. Behavior on `EditedMessage`: the current version appends the edit as a new log row and does not retract extracted facts. Decide if retraction is necessary.

(End of file)
