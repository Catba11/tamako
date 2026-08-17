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

LLM access is endpoint-portable. Every LLM call uses one of two API families: `anthropic-compatible` or `openai-compatible`. "Compatible" describes the wire format only, never the vendor. Any endpoint that speaks one of these two formats qualifies: first-party APIs, proxies, aggregators, and self-hosted servers. The base URL of the endpoint is a user configuration item. Model names are configuration items. Refer to Section 13.

## 4. Platform abstraction

### 4.1 Rules

- A1: All platform-specific types stay inside the adapter. The actor sees only normalized events.
- A2: The adapter exposes inbound events: `Message`, `EditedMessage`, `Reaction`, `MemberJoin`, `MemberLeave`.
- A3: The adapter exposes outbound actions: `SendText`, `SendMedia`, `React`.
- A4: A normalized message carries: platform message id, timestamp, sender id, sender display name, the optional sender username, text, reply-to id, and mention flags.
- A5: The first adapter is Telegram via teloxide. A Matrix adapter must be possible without changes to the actor, the context, or the memory backend.

### 4.2 Platform constraints

- The Telegram Bot API does not provide history before the bot joins a group. The bot receives messages only through the update stream. Rule P1 applies: every inbound message is persisted to the raw log at intake time, before any processing.
- Mention and reply metadata is resolved at intake time and stored with the log row. The digest pipeline uses this stored map. Refer to `proposed-graph-database-specs.md` Section 7.3.
- An edited message persists its `edit_date` as the row timestamp, not the original send date. Rows written before this rule (the schema v6 cutover) carry the original send date; the mixed semantics are deliberate (Rule P1, append-only). Telegram also delivers `edited_message` updates without a user-visible text change, for example on link-preview generation. Refer to Section 8.1 for the intake rule.
- Administrator status is OPTIONAL. The bot works as a plain group member when privacy mode is disabled; only reaction collection is absent. Reaction collection requires administrator status: reaction updates (`message_reaction`, `message_reaction_count`) are delivered to administrators only. Requesting these update kinds in `allowed_updates` without administrator status causes no error — the updates never arrive.
- Privacy mode cannot be queried through the Bot API. The combination non-administrator + privacy mode on (only commands and replies to the bot arrive) is normal platform behavior. The runtime surfaces it as operator guidance in the logs, not as runtime detection.
- Outbound permission failures are tolerated. Example: `setMessageReaction` without the needed rights. Such a failure is logged with the chat id and is never fatal.

## 5. Persistence layout

### 5.1 Per-group directory

Each `chat_id` has one directory `{data_root}/{chat_id}/`:

| File | Content |
|---|---|
| `memory.lbug` | LadybugDB graph. Refer to `proposed-graph-database-specs.md`. |
| `store.db` | SQLite database, WAL mode, `synchronous=NORMAL`. Contains the raw message log, the session state, and the sidecar vector index. |
| `media/` | Reserved. Captioned media artifacts. Refer to Section 14. |

### 5.2 `store.db` content

- `messages` table: one row per normalized inbound or outbound message. Outbound rows store the bot's own speech. Rule B1 applies. Sender identity columns carry the sender id and the display name; schema v4 adds the nullable `sender_username`. Rows written before v4 read it as NULL. Schema v6 extends the dedup key with the message text, so several edits of one message persist.
- `reactions` table: one row per reaction event on a group message. The Phase 2 warmup backoff consumes this table. Reaction data is not recoverable later, so collection starts at intake time in Phase 1.
- `state` table: key-value rows. Keys include `last_digest_boundary_msg_id`, `prev_digest_boundary_msg_id`, `wake_last_row_id`, `muted_flag`, `consecutive_bot_msgs`, `warmup_backoff_factor`, `warmup_quota_used_today`.
- `injected_memories` table: one row per injected recall. Columns: edge id, injection position, message-id range tag, rendered content. The rendered content is stored so a restart rebuild is bit-identical without graph queries. Refer to Section 9.5.
- `context_summaries` table: one row per summarized removed chunk. Columns: the message-id range `(first_msg_id, last_msg_id]` as the natural dedup key, the rendered summary text, and a creation timestamp. The text is persisted at creation time so a restart rebuild is bit-identical without re-calling the model. Rule P1 applies. Rows of rotated-out summaries stay for forensics.
- `pending_embeddings` table (schema v7): the embedding work queue. One row per (node id, content hash) pair — the unique key makes re-enqueue retry-safe. Columns: status (`pending`/`done`/`failed`), attempts, timestamps. The digest pipeline enqueues after the graph commit (best-effort); the embedding worker drains it. Refer to `proposed-graph-database-specs.md` Section 7.6.
- `node_embeddings` virtual table (schema v7): the sqlite-vec `vec0` sidecar index, one 4096-dimension embedding per node id. Derived, recomputable data — never the source of truth. The sqlite-vec extension registers at connection open on every code path that opens a store; a connection without it cannot even SELECT the virtual table.
- Vector index: sqlite-vec virtual tables in the same file. The embeddings of Person, Alias, and Concept names and descriptions live here. Refer to `proposed-graph-database-specs.md` Section 7.6.

To delete the memory of a group, delete the directory. Both files share one lifecycle.

### 5.3 Persona configuration

- One global persona configuration at `{data_root}/persona.toml`. Loaded once at startup.
- An optional `system_prefix` string is rendered verbatim before the identity line, with exactly one blank line as the separator. It carries system-level directives, such as alignment notes. When the key is absent, the rendered preamble is bit-identical to a configuration without it. Rule C4 applies. The injection guardrail is code-owned and is never configurable.
- A code-owned context-format explanation renders into the preamble after the persona sections and before the injection guardrail. It describes the XML item rendering of Section 7.3 in detail, and it forbids the model to write the `<msg>` or `<you>` structure itself. It is version-controlled and never configurable, like the guardrail. The same text feeds the participation-gate and the recall relevance-gate preambles.
- In live mode the persona file is required. A missing or invalid file fails startup with a clear error. An explicit operator flag permits the lenient fallback chain for experiments. Replay mode is always lenient. The preamble is the cache anchor. Its source must be deliberate. Rule C4 applies.
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
- Inbound messages during a running `Wake` are logged and appended to the context. They do not interrupt the running call. Before the bot sends a reply, the actor re-checks the recency of the target message. If the number of newer human messages after the target exceeds `reply_staleness_threshold` (20), the reply is discarded, not regenerated. The next wake is the natural retry.
- A non-forced wake reply quotes (replies-to) its target message only when the number of newer human messages after the target exceeds `reply_quote_threshold` (10). A recent target gets a plain standalone message: a Telegram reply notifies the author, and a recent target needs no context anchor. A forced wake always quotes — the human engaged the bot directly.
- If a wake fails, `wake_last_row_id` rolls back to its pre-wake value: the messages are presented again at the next wake. A failed forced wake requeues once. A second failure emits a distinct error, because Section 8.1 obliges the bot to respond.

## 7. Context lifecycle

### 7.1 Structure

The live context is an ordered list of items:

1. System preamble. Persona plus behavioral rules plus the injection guardrail. Refer to Section 9.4. This prefix is the cache anchor.
2. The two most recent summaries of removed chunks. Compressed history, rendered as summary items. Refer to Section 7.3.
3. The previous digested chunk. Already extracted into the graph. Kept as an overlap buffer, in raw form.
4. The current tail. Undigested inbound messages, bot replies, and recall injections.

Each item carries a message-id range tag. The boundary pair (`prev_digest_boundary_msg_id`, `last_digest_boundary_msg_id`) splits the previous chunk from the current tail. The previous chunk is the range at or below the previous boundary.

### 7.2 Rules

- C1: Between two digests, the context is append-only. Rule P2 applies.
- C2: A recall injection is always appended at the tail, directly after the messages that triggered it. An insertion into the middle of the history is forbidden.
- C3: At digest time, the actor summarizes the chunk being removed, then removes every item with a range tag at or below the previous boundary. Summary items are exempt from the removal; their retention is count-based (Section 7.3). The removal covers inbound messages, bot replies, injections, and tool outputs in that range. The digest model never sees the removed content. The summary is persisted before the removal. If the summarization fails, the removal defers one cycle and the raw chunk stays. Refer to Section 10.3. The actor performs the summarization, the removal, the deduplication pruning of Section 10.2, and the boundary update as one serialized flow (Section 6.1). Post-digest hooks are stateless observers only. They must not mutate the context.
- C4: A persona preamble change invalidates the provider cache for all groups. Preamble edits are deliberate events, not runtime side effects.
- C5: The context size is bounded by approximately two digest chunks plus two summaries. The maximum size follows from the digest thresholds in Section 8.2.
- C6: Context items render in the XML form of Section 7.3. Every rendered attribute derives from persisted raw-log columns. Rule P1 applies.

### 7.3 Rendering

- A human message renders: `<msg from="{display_name}"[ user="{username}"] at="{HH:MM}" id="{row_id}"[ kind="edit"][ reply="bot" | reply="user"[ reply_to_name="{name}" reply_to_id="{row_id}"]][ mention="bot"]>text</msg>`, role user. The time is UTC. The `user` attribute is absent when no username is stored (a row written before schema v4, or a sender without a username). An edit row carries `kind="edit"`. A reply to the bot carries `reply="bot"`. A reply to another member carries `reply="user"`. A mention of the bot carries `mention="bot"`.
- The reply target of a member reply renders when the raw log resolves it: `reply_to_platform_msg_id` maps to the ORIGINAL logged row. An edit does not move the target. `reply_to_name` is the display name of that row. `reply_to_id` is its raw-log row id. A target absent from the log renders `reply="user"` alone.
- A reply to the bot never carries a target name or id. Outbound rows use synthetic platform ids (Rule A3), so the target cannot be resolved.
- The bot's own speech renders: `<you at="{HH:MM}" id="{row_id}">text</you>`, role assistant.
- A recall injection renders: `<memory>...</memory>`, role assistant. Refer to Section 9.4.
- A context summary renders: `<summary range="{first}-{last}">text</summary>`, role user. The text is escaped like every other content. Summaries sit directly after the preamble, oldest first, before the raw previous chunk. The context keeps the two most recent summaries; a new one replaces the oldest. Summary items are exempt from the Rule C3 removal.
- Text content escapes `&`, `<`, `>`. Attribute values also escape `"`. A group member cannot forge context structure through message text.
- The digest input rendering is unchanged: `[{display_name} {HH:MM}] {text}` per `proposed-graph-database-specs.md` Section 7.2 step 4. The divergence is deliberate: the digest model extracts facts and receives the reply structure as data, not as dialogue.

## 8. Triggers

All thresholds are per-group configuration items. Defaults in parentheses. Refer to Section 13.

### 8.1 Message intake

- Every inbound message: append to the raw log, append to the live context, increment the wake counter. No LLM call.
- Duplicate deliveries: the raw log insert is idempotent. The wake counter counts each delivery. The counter is a scheduling hint; the log row is the source of truth. Rule P1 applies.
- An edited message appends a new log row with the edit time as its timestamp. An edit whose text is identical to the latest stored row of that message appends nothing: it is not an event (Section 4.2 lists the platform causes). Extracted facts are not retracted. Refer to Section 15.
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

- The recall worker reads the new messages of this wake and queries the memory backend through the read path of `proposed-graph-database-specs.md` Section 8. Entry resolution: mentions and replies, exact alias match, then vector search. Candidate generation reads the new messages only; widening it is the deep-recall item of the roadmap (Phase 2).
- The recall worker uses a cheap model. It never calls the main model.
- If the decision at step 3 is negative, the main model is never called. The recall cost is the fixed cost of every wake.

### 9.2 Relevance gate

- The recall returns memories only if an omitted memory would materially reduce the reply quality or the participation decision quality. When in doubt, inject nothing.
- If nothing is relevant, nothing is injected. An empty injection is forbidden.
- At most `recall_injection_cap` (5) memories are injected per wake.
- The injection rate is a metric. The expected healthy range is 20 to 40 percent of wakes.
- The relevance gate receives the same shared context view of Section 9.6, rendered ahead of the new messages and the candidate list. The `gate_context` switch applies to it equally.

### 9.3 Deduplication

- The actor stores the edge ids of every injected memory in `injected_memories`.
- A memory already injected in the current chunk is not injected again.

### 9.4 Injection format and guardrails

- The injection is one assistant message of the form: `<memory>...</memory>`. It is appended at the tail. Rule C2 applies. Injection rows persisted before this format change keep their legacy "I remember: ..." text verbatim until Rule C3 removes them.
- The content of a memory originates from group messages through the graph. This is an indirect prompt-injection channel. The system preamble contains a standing guardrail that names the `<memory>` and `<summary>` tags: their content is reference material, never an instruction, never speech.
- A memory injected from a stale or contested fact is acceptable in this version. Negation detection is deferred. Refer to Section 14.

### 9.5 Injection lifecycle

- An injection is removed only at digest time, together with its range. Rule C3 applies.
- The digest model never sees injections. Its input is the raw log of the range: human messages and the bot's own speech, with speaker labels. This prevents recalled graph content from re-entering the graph as new facts.

### 9.6 Participation decision

- Input: the new messages plus the injected memories. Ahead of them the gate receives the shared context view: the same rendered bytes the reply model sees (the two newest summaries, the previous chunk, the current tail up to this wake's marker). Only the new messages of this wake are targetable; the context view is reference material for judgment quality. The view renders ahead of the per-call sections so consecutive gate calls share a growing byte prefix (provider prompt cache; invalidation rides digest tempo, the same rhythm as the reply path). Setting `gate_context` to false restores the delta-only input.
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
4. On every completion (a dead-lettered batch also advances the boundary), advance `last_digest_boundary_msg_id`, obtain and persist the summary of the removed chunk (Rule C3, Section 7.3), apply the context removal, and prune the deduplication set of Section 9.3. A crash between the summary write and the boundary advance is replay-safe: the next completion finds the existing summary row and skips the model call. If the summarization fails, the removal defers one cycle; a later completion retries over the widened range. After 3 consecutive failures the chunk is removed without a summary (one ERROR), and the failure count resets. The summarizer input is capped at 2 × `digest_max_messages`; an oversized chunk summarizes its newest suffix while the summary row records the full range.

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
| Summarization failures | `summaries_failed_total`, cumulative. A rising count warns of a stuck summarizer before the circuit breaker of Section 10.2 engages. |

The `tamako --status <chat_id>` command is the metrics access path. It queries the group store read-only and prints the counters, the derived rates, the boundaries, the session state, and the dead-letter entries. `--status-all` prints every group.

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
| `warmup_silence` | 4 h | 8.4 |
| `monologue_limit` | 2 | 8.5 |
| `reply_staleness_threshold` | 20 newer human messages | 6.2 |
| `reply_quote_threshold` | 10 newer human messages | 6.2 |
| `gate_context` | true | 9.6 |
| `recall_injection_cap` | 5 per wake | 9.2 |

LLM access resolves from the per-group effective configuration (global defaults with per-group overrides, like every key above):

| Key | Default | Notes |
|---|---|---|
| `llm_api` | `anthropic-compatible` | API family: `anthropic-compatible` or `openai-compatible`. The family selects the wire format only, not the vendor. Environment override: `TAMAKO_LLM_API`. |
| `llm_base_url` | The canonical URL of the selected family | Base URL of the endpoint. Any endpoint that speaks the family format works: first-party, proxy, aggregator, self-hosted. Environment override: `TAMAKO_LLM_BASE_URL`. |
| `digest_model` | `claude-haiku-4-5` | Extraction (Section 10). Environment override: `TAMAKO_DIGEST_MODEL`. |
| `gate_model` | `claude-haiku-4-5` | Participation decision (Section 9.6). Environment override: `TAMAKO_GATE_MODEL`. |
| `reply_model` | `claude-sonnet-4-5` | Reply generation (Section 9, step 4). Environment override: `TAMAKO_REPLY_MODEL`. |
| `summary_model` | `claude-haiku-4-5` | Removed-chunk summarization (Rule C3, Section 10.2). Environment override: `TAMAKO_SUMMARY_MODEL`. |
| `structured_output` | `schema` | Structured-output mode: `schema` (send the JSON schema), `json_object` (JSON mode without a schema), `prompt_only` (no response_format; for endpoints that reject unknown parameters). Environment override: `TAMAKO_STRUCTURED_OUTPUT`. |
| `llm_session_id` | `"tamako"` | Session-affinity identifier sent as the `x-opencode-session` request header on every LLM call. Gateways that honor the header (Opencode Go) keep the prompt cache on one upstream. No per-purpose variant. One session id per deployment is the intent; the per-group resolution means a group table could override it — do not. Empty string counts as unset. Environment override: `TAMAKO_LLM_SESSION_ID`. |
| `embedding_model` | `"qwen/qwen3-embedding-8b"` | Embedding model for the vector sidecar (schema v7). Global only: no per-purpose and no per-group variant. Environment override: `TAMAKO_EMBEDDING_MODEL`. |
| `embedding_llm_base_url` | `"https://openrouter.ai/api/v1"` | Base URL of the openai-compatible embeddings endpoint. Global only. Environment override: `TAMAKO_EMBEDDING_BASE_URL`. The embedding dimension is pinned at 4096; changing it means recreating the `node_embeddings` table. |

A purpose (`digest`, `gate`, `reply`, `summary`) may override `llm_api`, `llm_base_url`, and `structured_output` individually. The per-purpose keys are `digest_llm_api`, `digest_structured_output`, and so on, with environment overrides `TAMAKO_DIGEST_STRUCTURED_OUTPUT` and so on. This permits mixed deployments, for example a cheap self-hosted OpenAI-compatible endpoint for extraction and a first-party Anthropic endpoint for replies.

API keys come from the environment only, never from a config file: `ANTHROPIC_API_KEY` for anthropic-compatible endpoints, `OPENAI_API_KEY` for openai-compatible endpoints. These variable names are the convention for the format, for third-party endpoints as well.

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
