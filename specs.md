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
- A4: A normalized message carries: platform message id, timestamp, sender id, sender display name, the optional sender username, text, reply-to id, and mention flags. Decision 82 adds an ordered media-part list (kind, caption text): the adapter's intake stage downloads, normalizes, and captions media BEFORE the normalized event exists, so the event's text is final — member text and rendered `<media>` elements interleaved in original order (Section 7.3). IMPLEMENTATION NOTE (decision 82, accepted 2026-08-22): at cutover the adapter assembles caption text + element with the element pinned at the END (`text <media .../>`), not a true positional interleave — Telegram delivers at most one media item per message at cutover; the middle-of-text position is the only information lost. The actor never sees bytes or platform file ids.
- A5: The first adapter is Telegram via teloxide. A Matrix adapter must be possible without changes to the actor, the context, or the memory backend.

### 4.2 Platform constraints

- The Telegram Bot API does not provide history before the bot joins a group. The bot receives messages only through the update stream. Rule P1 applies: every inbound message is persisted to the raw log at intake time, before any processing.
- Mention and reply metadata is resolved at intake time and stored with the log row. The digest pipeline uses this stored map. Refer to `proposed-graph-database-specs.md` Section 7.3.
- Forward metadata is captured at intake time and stored with the log row (decision 108): the forward origin (kind `user`/`hidden_user`/`chat`/`channel`, a display label, the origin id when the platform gives one, the original send date) and the automatic-forward flag (a linked channel's auto-repost into its discussion group). A message that is not forwarded stores NULLs. The digest pipeline uses this stored data for the attribution rule of Section 10.1. Manually forwarded messages cannot be edited by the forwarder; an edited forward row still arrives when a channel-post edit propagates to the auto-forwarded copy of a discussion group — the edit path captures the same fields.
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
| `media.db` (decision 82) | GLOBAL (cross-group) SQLite database at the data root, NOT per group — the first cross-group store. Its own `user_version` chain (currently 1), outside the group MIGRATIONS chain. Holds the `sticker_captions` KV (sticker file_unique_id → caption, first-write-wins; stickers are public platform objects, safe to share across groups). Photos are NOT cached at cutover. Image bytes are never persisted anywhere (decision 82(g)). |

### 5.2 `store.db` content

- `messages` table: one row per normalized inbound or outbound message. Outbound rows store the bot's own speech. Rule B1 applies. Sender identity columns carry the sender id and the display name; schema v4 adds the nullable `sender_username`. Rows written before v4 read it as NULL. Schema v6 extends the dedup key with the message text, so several edits of one message persist. Schema v14 adds the five nullable forward columns of Section 4.2 (`forward_kind`, `forward_label`, `forward_origin_id`, `forward_date`, `forward_automatic`); rows written before v14 read them as NULL (not forwarded).
- `reactions` table: one row per reaction event on a group message. The Phase 2 warmup backoff consumes this table. Reaction data is not recoverable later, so collection starts at intake time in Phase 1.
- `state` table: key-value rows. Keys include `last_digest_boundary_msg_id`, `prev_digest_boundary_msg_id`, `wake_last_row_id`, `muted_flag`, `consecutive_bot_msgs`, `warmup_backoff_factor`, `warmup_quota_used_today`, `warmup_next_at`, `warmup_topic_cooldowns`, and the warmup engagement-watch keys.
- `injected_memories` table: one row per injected recall. Columns: edge id, injection position, message-id range tag, rendered content. The rendered content is stored so a restart rebuild is bit-identical without graph queries. Refer to Section 9.5.
- `context_summaries` table: one row per summarized removed chunk. Columns: the message-id range `(first_msg_id, last_msg_id]` as the natural dedup key, the rendered summary text, and a creation timestamp. The text is persisted at creation time so a restart rebuild is bit-identical without re-calling the model. Rule P1 applies. Rows of rotated-out summaries stay for forensics.
- `pending_embeddings` table (schema v7): the embedding work queue. One row per (node id, content hash) pair — the unique key makes re-enqueue retry-safe. Columns: status (`pending`/`done`/`failed`), attempts, timestamps. The digest pipeline enqueues after the graph commit (best-effort); the embedding worker drains it. Refer to `proposed-graph-database-specs.md` Section 7.6.
- `node_embeddings` virtual table (schema v7): the sqlite-vec `vec0` sidecar index, one embedding per node id. Derived, recomputable data — never the source of truth. The sqlite-vec extension registers at connection open on every code path that opens a store; a connection without it cannot even SELECT the virtual table. Schema v8 recreates the table with `distance_metric=cosine` (dropping the v7 L2 table; the reconciliation pass re-embeds — derived data). Schema v11 recreates it at 3072 dimensions for the decision-81 model switch (`google/gemini-embedding-2` — 3072 is the model's native dimension, so the optional `dimensions` request parameter is behaviorally irrelevant and the hard length check is the guard); the v8/v11 drop-and-recreate-plus-journal-clear pattern is identical, and every threshold of `proposed-graph-database-specs.md` Sections 7.4/7.7 reverts to uncalibrated-provisional against the new model's score distribution.
- `merge_audit` table (schema v9): one append-only row per merge-tool action. Columns: loser id, survivor id, loser kind/name/description, the three-way verdict (`same`/`related`/`different` — only `same` merges), reason, confirmed_by (`llm:<model>` or `operator`), edge counters (moved, self-loops dropped, deduped), the rollback snapshot (JSON: the loser node and its original edges, plus the created edge identifiers; NULL for non-merge verdicts), a rolled_back flag, and a timestamp. Refer to `proposed-graph-database-specs.md` Section 7.7.
- `related_pairs` table (schema v12, decision 83): one row per `related` merge verdict — the "dotted edges". Columns: the unordered pair (`node_a_id` < `node_b_id`, UNIQUE, first-write-wins via INSERT OR IGNORE), reason, confirmed_by, a status (`pending`/`promoted`/`dismissed`), and a timestamp. A `same` merge rewrites the loser's rows to the survivor in the same apply. The decision-106 promotion pass reads the `pending` rows at digest assembly and flips grounded rows to `promoted`; `--dismiss-related-pair` flips a row to `dismissed`. No OTHER read path touches this table (not recall, not resolution, not status).
- `llm_session_keys` table (schema v13, decision 84): one row per (chat_id, purpose) — the persisted random session-id suffix of the cache-affinity headers. Columns: chat id, purpose (`digest`/`gate`/`reply`/`summary`/`caption`/`embedding`), the 16-char base64url suffix, and a creation timestamp. Minted lazily on first use (get-or-mint), never rotated; a restart reuses the stored suffix so provider-side affinity survives restarts. Cutover limitation (decision 84, review-corrected): only the four completion purposes (digest/gate/reply/summary) mint per-group suffixes; embedding and caption providers are built once process-wide with no chat_id in scope and send the bare prefix (per-group affinity for them is an accepted follow-up).
- `edge_texts` table (schema v10): the full-text sidecar over edge descriptions (the "who discussed X" pattern of `proposed-graph-database-specs.md` Section 8.2), scanned with parameterized LIKE. Derived, recomputable; written post-commit at digest time (best-effort) and repaired by the startup reconciliation pass.
- Vector index: sqlite-vec virtual tables in the same file. The embeddings of Person, Alias, and Concept names and descriptions live here. Refer to `proposed-graph-database-specs.md` Section 7.6.

To delete the memory of a group, delete the directory. Both files share one lifecycle.

### 5.3 Persona configuration

- One global persona configuration at `{data_root}/persona.toml`. Loaded at startup; in live mode a filesystem watcher reloads it on deliberate edits (decision 80: the rendered preamble is broadcast to every spawned group actor through its inbox, applied in memory only — the file is the state, the next rebuild renders the same bytes). Replay mode never watches. The watcher's identity gate compares the rendered preamble AND the rendered suffix (decision 94): a suffix-only edit rewrites the reply-suffix slot in place and broadcasts NOTHING — the suffix never enters the preamble, so no actor context changes and no cache anchor moves.
- An optional `system_prefix` string is rendered verbatim before the identity line, with exactly one blank line as the separator. It carries system-level directives, such as alignment notes. When the key is absent, the rendered preamble is bit-identical to a configuration without it. Rule C4 applies. The injection guardrail is code-owned and is never configurable.
- An optional `[[example]]` array (decision 85): few-shot dialogue examples. Each entry carries `context` (a sample of the live XML dialect of Section 7.3, written raw by the operator — the persona file is trusted config, no escaping) and `reply` (the pet's reply as BARE TEXT, no tags — the wrapper is code-rendered, never operator-written, and the example never teaches the `<you>` shape the context-format gloss forbids (the decision-64 no-imitation line — the ban lives in the gloss, not in Section 9.4 as this sentence said pre-2026-09-06; decision 95 later took the own-speech element off the forbidden list). Superseded rationale, decision 93: the "plain text output channel" premise no longer holds — the live output contract is the `<reply>` fence of Section 9.8, the same wrapper the examples render, so the teaching is now deliberate. Decision 95 renames both once more: the wrapper is the pet tag `<{pet}>` of Section 7.3, the same element every own-history item wears, so the fence shape is demonstrated on every turn). The section renders after the context-format gloss and before the injection guardrail, each example wrapped in an `<example>` element with `<context>` and `<reply>` children, under a framing line marking them as examples, not live context. Absent or empty renders NOTHING — the preamble stays bit-identical (Rule C4). Examples ride the decision-80 hot reload. They feed ONLY the reply persona preamble: the gate and recall preambles append the gloss but not the examples; digest and summary stay gloss-free. No length cap: every example is paid in prompt tokens on every wake (cached-prefix pricing applies) — the persona file documents this.
- An optional `suffix` string array (decision 86): high-importance guardrail instructions appended as ONE system-role message STRICTLY LAST in the reply request's message list (after the newest context message) — the lost-in-the-middle mitigation. The body renders as a `<system>` element wrapping one NUMBERED `<rule1>`, `<rule2>`, ... element per entry, so every rule has its own boundary. Entry content is verbatim (trusted config, like `system_prefix`). The suffix is NOT part of the cache anchor: it sits past the cached prefix, so editing it invalidates nothing — it is the zero-cache-cost hot-tuning knob, the complement of preamble edits. Absent or empty appends no message — one exception: with `timezone` set (decision 90), an empty persona suffix still yields the suffix message carrying the code-owned `<now>` element alone. The placement mode is `suffix_mode` (decision 88, Section 13): `system` appends the message strictly last; `append` merges the body into the final user instruction. It rides the decision-80 hot reload — a suffix-only edit applies in place with no actor broadcast (decision 94) — and feeds ONLY the reply request: gate/recall/digest/summary/warmup carry no suffix (short-context structured tasks, no lost-in-the-middle problem).
- An optional PER-GROUP suffix override at `{data_root}/{chat_id}/persona.toml` (decision 98): when — and only when — the override file carries a `suffix` key, that array replaces the GLOBAL suffix for this group's reply requests (`suffix = []` explicitly silences the global rules for the group; the decision-90 `<now>` splice still applies on top). The override supplies the BODY only; the placement mode still comes from the trigger configuration (Section 13). The override loads at actor spawn and has NO hot-reload watcher — edit and restart. A missing file, a file without the key, or a malformed file (one WARN, degrade to the global suffix — a bad override never blocks the group's wake path) all mean the global suffix applies. Any other key in the override file is denied at the schema (`deny_unknown_fields`): the override is a suffix-only contract.
- A code-owned context-format explanation renders into the preamble after the persona sections and before the injection guardrail. It describes the XML item rendering of Section 7.3 in detail, and it forbids the model to write the `<msg>` structure itself. (Decision 95: the own-speech element left the forbidden list — it IS the reply fence of Section 9.8 and the model is required to write exactly one per reply. The gloss still never names the fence CONTRACT — no wrapper sentence — because the same text feeds the JSON-outputting gates.) It is version-controlled and never configurable, like the guardrail. The same text feeds the participation-gate and the recall relevance-gate preambles.
- In live mode the persona file is required. A missing or invalid file fails startup with a clear error. A `name` or `identity` that is empty or whitespace-only is an invalid file too (decision 99): the degenerate preamble it would render shifts the cache anchor, so the load fails loudly; on a watcher reload the current preamble stays (the malformed-file arm). An explicit operator flag permits the lenient fallback chain for experiments. Replay mode is always lenient. The preamble is the cache anchor. Its source must be deliberate. Rule C4 applies. Unknown-key posture (decision 94): an unknown key inside an `[[example]]` element is an INVALID file (a root-level array placed below a `[[example]]` header nests into the last element by TOML scoping rules and would otherwise drop silently — the 2026-09-04 live incident); an unknown ROOT key is tolerated for forward compatibility and earns one curated WARN per key, never applied (the decision-84(e) posture extended to the persona file).
- The persona service renders the system preamble. The preamble is the prefix of every model context and never changes inside a context lifetime — EXCEPT through the deliberate reload event of decision 80 (an operator edit broadcast to all groups; in-flight calls keep their old snapshot, the next call of every purpose pays a cold prefix). Rule C4 applies.
- The persona rendering layer is an interface. The pet persona is one implementation. This decoupling permits reuse of the runtime for other personas or purposes.

## 6. Per-group actor and concurrency

### 6.1 Actor model

1. One actor per group. All trigger events enter one FIFO inbox. The inbox also carries the persona-reload command of decision 80 (live mode only): a reload serializes behind in-flight work and never interrupts a running call.
2. LLM calls run concurrently across groups. Inside one group, the following operations are strictly serialized: graph writes, context mutations, session-state mutations.
3. Graph writes per group are serialized through the actor. LadybugDB permits one writer per database file. A blocked batch must not block the queue. Refer to Section 10.4.
4. The actor persists the session state after every mutation. On restart, the actor rebuilds the live context from the raw log and  the session state. The decode of the persisted state is a total function (decision 99): a missing key decodes to the fresh default silently (first start is the normal case); a present-but-malformed value decodes to the fresh default of THAT field with one WARN per field — the pre-99 silent default hid operator edits and writer bugs.
5. Shutdown drains (decision 114): on the shutdown command the actor closes every dispatch point (the tick path and the completion chains) through a `shutting_down` flag and keeps selecting on the inbox until every tracked detached task has reported. Cancellable long tasks stop cooperatively at LLM-call boundaries; the completion handlers keep running during the drain (state restoration only). Forced wakes are never cancelled.

### 6.2 Trigger ordering

- If several triggers are pending, `Digest` runs before `Wake`. Recall sees the freshest graph.
- A forced `Wake` (mention or reply to the bot) moves to the head of the queue. It does not preempt a running call. A forced wake is suppressed while a `forced_wake_cooldown` is running (Section 8.1): a suppressed forced wake leaves no queue entry.
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
- C3: At digest time, the actor summarizes the chunk being removed, then removes every item with a range tag at or below the previous boundary. Summary items are exempt from the removal; their retention is count-based (Section 7.3). The removal covers inbound messages, bot replies, injections, and tool outputs in that range. The digest model never sees the removed content. The summary is persisted before the removal. If the summarization fails, the removal defers one cycle and the raw chunk stays. Refer to Section 10.2 step 4. The actor performs the summarization, the removal, the deduplication pruning of Section 10.2, and the boundary update as one serialized flow (Section 6.1). Post-digest hooks are stateless observers only. They must not mutate the context.
- C4: A persona preamble change invalidates the provider cache for all groups. Preamble edits are deliberate events, not runtime side effects. The live-mode mechanism is decision 80: a debounced file watcher broadcasts the re-rendered preamble through each actor's inbox; the actor swaps context item 0 in memory (nothing persists — the file is the state); a malformed intermediate file keeps the current preamble with one WARN; a dead or backlogged actor is skipped (its next start reads the file).
- C5: The context size is bounded by approximately two digest chunks plus two summaries. The maximum size follows from the digest thresholds in Section 8.2.
- C6: Context items render in the XML form of Section 7.3. Every rendered attribute derives from persisted raw-log columns. Rule P1 applies.

### 7.3 Rendering

- A human message renders: `<msg from="{display_name}"[ user="{username}"] at="{HH:MM}" id="{row_id}"[ kind="edit"][ reply="bot" | reply="user"[ reply_to_name="{name}" reply_to_id="{row_id}"]][ mention="bot"][ fwd="{kind}:{label}"]>text</msg>`, role user. The `fwd` attribute renders only for a forwarded row (decision 108): kind `user`, `hidden`, `chat`, or `channel`, and `fwd="auto:{label}"` for the automatic forward of a linked channel. A row without forward data renders byte-identical to the pre-v14 shape. The time is UTC. The `user` attribute is absent when no username is stored (a row written before v4, or a sender without a username). An edit row carries `kind="edit"`. A reply to the bot carries `reply="bot"`. A reply to another member carries `reply="user"`. A mention of the bot carries `mention="bot"`. Decision 82: the text may embed `<media type="{image|sticker|video}">{caption}</media>` elements, interleaved with member text in original order; a failed or unsupported caption renders the element with an EMPTY body. The caption text is escaped like member text (a caption containing `</media>` cannot break structure), and inbound member text is escaped before embedding (a member cannot forge a media element). The tag constants are single-sourced beside the MSG/SUMMARY/YOU constants; the outbound parrot filter strips the block (Section 9.4 discipline).
- The reply target of a member reply renders when the raw log resolves it: `reply_to_platform_msg_id` maps to the ORIGINAL logged row. An edit does not move the target. `reply_to_name` is the display name of that row. `reply_to_id` is its raw-log row id. A target absent from the log renders `reply="user"` alone.
- A reply to the bot never carries a target name or id. Outbound rows use synthetic platform ids (decision 35), so the target cannot be resolved.
- The bot's own speech renders: `<{pet} at="{HH:MM}" id="{row_id}">text</{pet}>`, role assistant. `{pet}` is the pet tag (decision 95): the persona name lowercased and reduced to ASCII alphanumerics, `-`, and `_` (a name with no keepers falls back to `you`); `tamako_persona::pet_tag_for_name` is the single derivation, threaded to every renderer and filter. Decision-95 supersession: the element was the FIXED `<you>` before; stored summaries and memories persisted under the old shape can still quote it, so the outbound filter keeps a legacy `<you>` strip region (Section 9.8).
- A recall injection renders: `<memory>...</memory>`, role assistant. Refer to Section 9.4.
- A context summary renders: `<summary range="{first}-{last}">text</summary>`, role user. The text is escaped like every other content. Summaries sit directly after the preamble, oldest first, before the raw previous chunk. The context keeps the two most recent summaries; a new one replaces the oldest. Summary items are exempt from the Rule C3 removal.
- Text content escapes `&`, `<`, `>`. Attribute values also escape `"`. A group member cannot forge context structure through message text.
- The digest input rendering keeps its own shape: `[{display_name} {HH:MM}] {text}` per `proposed-graph-database-specs.md` Section 7.2 step 4, with one addition (decision 108): a forwarded message carries the marker `(fwd {kind}:{label})` between the speaker label and the text — `auto:{label}` for an automatic forward. The divergence is deliberate: the digest model extracts facts and receives the reply structure as data, not as dialogue.

## 8. Triggers

All thresholds are per-group configuration items. Defaults in parentheses. Refer to Section 13.

### 8.1 Message intake

- Every inbound message: append to the raw log, append to the live context, increment the wake counter. No LLM call.
- Duplicate deliveries: the raw log insert is idempotent. The wake counter counts each delivery. The counter is a scheduling hint; the log row is the source of truth. Rule P1 applies.
- An edited message appends a new log row with the edit time as its timestamp. An edit whose text is identical to the latest stored row of that message appends nothing: it is not an event (Section 4.2 lists the platform causes). Extracted facts are not retracted. Refer to Section 15.
- A mention of the bot or a reply to the bot triggers a forced `Wake`. The bot must respond when addressed directly. The `muted` state does not suppress a forced wake. Refer to Section 8.5.
- Forced-wake cooldown: a forced wake that produced a reply starts a `forced_wake_cooldown` (default 10 s, per-group overridable, 0 disables). A new forced wake during the cooldown is suppressed: the mention or reply is still logged to the raw log and the context (it lands in the next wake's presented set, so no information is lost), but no immediate wake fires. The cooldown suppresses the back-to-back reply chains of a mention/reply rally (decision 79).

### 8.2 Digest trigger

- Fire when the undigested tail reaches the first of: 5000 CJK characters, 100 messages, 2500 words, or 20 kB.
- Fallback: fire when the tail is non-empty and the last digest is older than the digest timeout (6 h). "Last digest" means the wall-clock completion time of the last successful digest. If no digest has ever run for the group, the fallback does not fire. The tail right after the bot joins grows until it reaches a size threshold.
- On fire, run the digest pipeline. Refer to Section 10.
- Shutdown drain (decision 114): while the actor drains, the digest trigger dispatches nothing new — the gate covers both the tick-path evaluation and the completion-chain re-evaluation. An in-flight digest finishes or cancels at a call boundary (Section 10.3 item 4).

### 8.3 Wake trigger

- Fire when the first of these is true: at least `wake_msg_count` new messages (5) since the last wake, or at least `wake_interval` (1 h) since the last wake.
- Multiply the active interval by a uniform random factor in [0.7, 1.3] at every reset. The expectation stays stable.
- Floor: two consecutive wakes are at least `wake_floor` (5 min) apart. The floor is a configuration item. It caps the cost in very active groups where five messages can arrive in seconds.
- On fire, run the wake procedure. Refer to Section 9.

### 8.4 Warmup trigger

- Daily quota: `warmup_quota` (default 1; the range is 1–3) proactive messages, spread uniformly at random over `warmup_active_hours` (default "08:00-23:00", host-local time; overnight ranges are unsupported).
- A warmup message is permitted only if the group has been silent for at least `warmup_silence` (4 h).
- The `muted` state suppresses warmup. The soft backoff in Section 8.5 adjusts the effective quota.
- The next scheduled activation persists in the `warmup_next_at` state key: a restart never reshuffles the schedule (Rule P1).
- The master switch `warmup` (default true) disables the trigger entirely when false. The procedure is Section 9.7.

### 8.5 Speech suppression

- Hard rule (monologue lock): if the last `monologue_limit` (2) messages in the group are all from the bot, enter the `muted` state. In `muted`, proactive speech and warmup are forbidden. A forced wake is still permitted. Any human message clears the state.
- Soft rule (warmup backoff): a warmup is ENGAGED when a human reply or a reaction arrives within `warmup_reaction_window` (30 min; reactions reach administrator groups only — Section 4.2 — so non-administrator groups measure replies only). A warmup with zero engagement at window expiry increments `warmup_backoff_factor` by one: the effective daily quota becomes max(1, `warmup_quota` − factor) — the floor of one keeps the engagement reset reachable (decision 79: backoff lengthens spacing, it never silences permanently) — and the warmup interval multiplier becomes 2^factor. Any successful engagement resets the factor to zero.
- Participation rate is a metric. The calibration band is 30 to 60 percent. Refer to Section 12.

## 9. Wake procedure

One wake executes these steps in this sequence:

1. If `muted` and not forced, return. Reset the counters and the timer.
2. Recall. Refer to Section 9.1 to 9.5.
3. Participation decision. Refer to Section 9.6.
4. If the decision is to participate, generate a reply with the main model. The output contract of Section 9.8 applies. Decision 100: a reply longer than the platform limit of 4096 characters is REJECTED before the raw-log write — the SAME wake error as an empty reply: nothing persists, nothing sends, no context append, the marker rolls back, and a forced wake requeues once per the failure handling above (never a truncation: an overlength reply is an incident). Otherwise write the bot's own message to the raw log FIRST, then send it through the adapter, then append it to the context (Rule B1: the log precedes the send, so the log and the group see the same text — the pre-2026-09-06 sentence order of this step read send-then-log; the code was always log-then-send).
5. Reset the wake counter and the timer with fresh jitter.
- Shutdown drain (decision 114): an UNFORCED wake cancelled at an LLM-call boundary (between the serial recall/gate/reply calls) rolls `wake_last_row_id` back to `pre_wake_row_id` and persists — the messages re-present at the next natural trigger's gate judgment. Forcing is memory-only and the cancel path does not re-arm it (no intake arming, no decision-65 requeue): a Section-8.1 intent cancelled mid-wake degrades to best-effort gate judgment. A FORCED wake is never cancelled; it runs to completion or dies with the process.

### 9.1 Recall call

- The recall worker reads the new messages of this wake and queries the memory backend through the read path of `proposed-graph-database-specs.md` Section 8. Candidate terms come from the new messages only. Entry resolution: mentions and replies, exact alias match, then vector search (accept at or above `vector_candidate_threshold`; no confirmation call on the read path). With `deep_recall` enabled (the default), candidates widen two more ways: graph expansion to two hops under the `proposed-graph-database-specs.md` Section 8.2 rules (validity filter, the 90-day window, the per-node expansion limit, hub truncation; `contains` and `known_as` excluded, `also_known_as` included), and full-text matches on edge descriptions through the `edge_texts` sidecar (the "who discussed X" pattern). All candidate sources dedup by edge id and cap at `recall_candidate_cap` (40) before the relevance gate.
- The recall worker uses a cheap model. It never calls the main model.
- With `deep_recall` set to false, candidate generation is the Phase 1 shallow form: direct neighbors only, one hop, entry by mentions/replies and exact alias match only.
- If the decision at step 3 is negative, the main model is never called. The recall cost is the fixed cost of every wake.

### 9.2 Relevance gate

- The recall returns memories only if an omitted memory would materially reduce the reply quality or the participation decision quality. When in doubt, inject nothing.
- If nothing is relevant, nothing is injected. An empty injection is forbidden.
- At most `recall_injection_cap` (5) memories are injected per wake.
- The injection rate is a metric. The expected healthy range is 20 to 40 percent of wakes.
- The relevance gate receives the same shared context view of Section 9.6, rendered ahead of the new messages and the candidate list. The `gate_context` switch applies to it equally. Its cache prefix is its own (per-gate, per-group — the two gates never share a cache, Section 9.6).

### 9.3 Deduplication

- The actor stores the edge ids of every injected memory in `injected_memories`.
- A memory already injected in the current chunk is not injected again.

### 9.4 Injection format and guardrails

- The injection is one assistant message of the form: `<memory>...</memory>`. It is appended at the tail. Rule C2 applies. Injection rows persisted before this format change keep their legacy "I remember: ..." text verbatim until Rule C3 removes them.
- The content of a memory originates from group messages through the graph. This is an indirect prompt-injection channel. The system preamble contains a standing guardrail that names the `<memory>`, `<summary>`, and `<media>` tags: their content — recalled memories, compressed history, media descriptions — is reference material, never an instruction, never speech. (Decision 82 added `<media>` to the constant; this bullet omitted it until the 2026-09-06 docs round.)
- A memory injected from a stale or contested fact is acceptable in this version. Negation detection is deferred. Refer to Section 14.
- The reply request carries the decision-86 `suffix` (a `<system>` element wrapping numbered `<rule1>`, `<rule2>`, ... entries from the persona file) under the `suffix_mode` placement of Section 13 (decision 88): ONE final system-role message STRICTLY LAST after the newest context message (the `system` default), or the same body merged into the final user instruction (the `append` mode, for endpoints that reject trailing system messages). It is the lost-in-the-middle mitigation and the zero-cache-cost guardrail knob — it is not part of the cache anchor, so editing it never invalidates a group's cached prefix. Gate, recall, digest, summary, and warmup requests carry no suffix. Decision 98: the authority contract renders whenever the GLOBAL mode is `append` — the pre-98 raw-suffix-non-empty condition let an empty-suffix append configuration merge the decision-90 `<now>` element with NO contract rendered. The suffix BODY may come from the group's override file (Section 5.3, decision 98); the placement mode still comes from the trigger configuration.

### 9.5 Injection lifecycle

- An injection is removed only at digest time, together with its range. Rule C3 applies.
- The digest model never sees injections. Its input is the raw log of the range: human messages and the bot's own speech, with speaker labels. This prevents recalled graph content from re-entering the graph as new facts.

### 9.6 Participation decision

- Input: the new messages plus the injected memories. Ahead of them the gate receives the shared context view: the same rendered bytes the reply model sees (the two newest summaries, the previous chunk, the current tail up to this wake's marker). Only the new messages of this wake are targetable; the context view is reference material for judgment quality. The view renders ahead of the per-call sections so consecutive calls OF THE SAME GATE across wakes share a growing byte prefix (provider prompt cache; invalidation rides digest tempo, the same rhythm as the reply path). The two gates carry different preambles, so their caches never warm each other — each gate's prefix is cached per gate, per group. Setting `gate_context` to false restores the delta-only input.
- Output: a binary decision, with the target message for the reply.
- The decision uses a cheap model. The main model runs only on a positive decision.
- The recall result is part of the input on purpose: a topic with strong personal memories is a valid reason to participate.

### 9.7 Warmup procedure

One warmup executes these steps in this sequence (independent of the Wake procedure; roadmap Section 6):

1. Check the gates: `warmup` enabled, not `muted`, quota remains for today (after the Section 8.5 backoff), the group silent for `warmup_silence`, and the persisted `warmup_next_at` due. Any failure exits quietly (DEBUG).
2. Pick a topic: sample the group's Concept nodes weighted by edge count × recency decay, excluding topics on their per-topic cooldown (`warmup_topic_cooldown_days`, 3 days) and topics whose normalized name appears in the 50-row raw-log tail (never restart the conversation that just went quiet). No eligible topic means no warmup — forced small talk is worse than silence.
3. Generate with the reply purpose: the persona preamble, the gloss, the guardrail, the shared context view, and a warmup instruction naming the topic. An interest attached to a specific person is framed as an open question to the group, never as "X likes Y" — the Section 9.4 guardrail spirit extended to proactive speech. The decision-59/64 parrot filter applies to the generated text like every reply.
4. Send as a plain standalone message. Proactive speech never quotes a target (Section 6.2's quoting rule governs replies; warmup has no target). Write the outbound row to the raw log first.  Rule B1 applies. The decision-100 overlength reject (4096 characters) applies to the warmup text alike — rejected before the raw-log write, one ERROR, no quota consumed, no engagement watch opened.
5. Emit one curated `warmup` INFO line, persist the engagement-watch state, and schedule the next activation (`warmup_next_at`).

### 9.8 Reply output contract and outbound text hygiene

- The ephemeral reply instruction of step 4 requires the whole reply in exactly one `<{pet}>...</{pet}>` element — the pet tag of Section 7.3, not a fixed `<reply>` (decision 95: unifying the fence tag with the own-speech tag turns every rendered own-history item into a demonstration of the fence; the 2026-09-05 live-replay measured 38/40 fenced under the unified tag against 33/50 under the split tags) — nothing outside it (decision 93). The fence sentence lives in the reply instruction only: the shared context-format gloss also feeds the JSON-outputting gates, so it never carries the fence. The decision-85 examples (Section 5.3) render the same wrapper — the contract formalizes a shape the prompt already teaches (the live model fenced spontaneously before the contract existed).
- Fence extraction runs at the reply validation seam, before the parrot filter: the FIRST complete `<{pet}>...</{pet}>` pair yields the reply text (an attribute-carrying `<{pet} at="..." id="...">` opener is accepted — every own-history item carries attributes, so an imitated attribute opener is an expected variant, measured in up to 6/10 outputs in the 2026-09-05 replay); everything outside the fence drops, with one WARN carrying the dropped byte count. The fence is the allowlist complement of the markup blocklist below: an UNKNOWN future reasoning marker outside the fence drops with no code change. Decision 97 (C2): the scan skips a LOOKALIKE opener token — the `<{pet}` prefix not followed by `>` or whitespace, like `<{pet}ing>` or `<{pet}->` — and keeps looking for a real opener; pre-97 the first lookalike disabled extraction for the whole text, so one injected lookalike silenced both fence layers and the fail-open WARN falsely reported no fence.
- The contract is FAIL-OPEN by design: an absent, unclosed, or malformed fence passes the whole text through with one WARN. A non-compliant model degrades to the pre-contract hygiene level instead of dropping every reply (the deployment runs several endpoints and models; strict fence-or-drop would turn a model swap into silence). The warmup path carries no fence sentence and keeps the pre-contract handling. Decision 96 (C4): the seam WARNs of this section carry the purpose and the resolved model name; the fence-fallback WARN also names the expected opener (decision 95's `fence_open` field). Decision 101 (B6): the seam WARNs also carry the `chat_id` — `ReplyGenerator::generate` and `WarmupGenerator::generate_warmup` gained the `chat_id` parameter for it (the group attribution the 96 ruling deferred; no tracing spans exist in the actor to recover it).
- The decision-59/64 parrot filter then applies to the extracted text. Residual fence tokens are hygiene, not speech: a tag-only `<{pet}>`/`</{pet}>` line drops, an inline pair unwraps to its content, an edge token strips; a mid-line single token survives untouched (quotation protection — members discuss AI glitch output). Decision 96 (F5): a strip raises one WARN at the validation seam — the seam filters first on the live path, so the actor's idempotent second pass is only the net for generators without a seam filter (pre-96 the WARN sat on the actor pass and never fired live, so parrot events were invisible). Decision 95: the tag-only test keys on the opener's first `>` ENDING the line (trimmed) — an attribute-carrying inline pair like `<{pet} at="..">come eat</{pet}>` unwraps instead of dropping (pre-fix it was misread as tag-only and dropped, which on the fence-less warmup path lost the whole output). There is deliberately NO strip region for the pet tag — its residuals are fence hygiene by design — while a legacy `<you>` region stays for stored pre-95 summaries and memories quoting the old shape. Decision 97: the inline-pair unwrap is a single-pass cursor scan repeated to a fixpoint — a lookalike token is copied through and the scan resumes (pre-97 it stopped the unwrap and every later pair survived wrapped), nested pairs still unnest, and the per-pair string rebuild (measured 84.5 ms on 16k pairs / 240 KB) is gone — and the edge-token strip runs to a fixpoint (a doubled `<{pet}><{pet}>` edge no longer leaks one token). The five strip-region openers require a tag delimiter after the prefix (`>`, whitespace, or line end), normalized to the bare prefix: a lookalike opener like `<memorybank…` is ordinary text, not a region (pre-97 it opened a region that ate the tail and erred the wake), and bare `<msg>`/`<you>`/`<media>` openers now strip under the uniform rule.
- Independently of the fence, the endpoint layer strips reasoning markup from the extracted text of EVERY completion purpose (reply, gates, summary, extraction, caption) before purpose-specific handling (decision 93): balanced `<think>...</think>` regions strip; an orphan `</think>` drops everything up to and including it (the observed shape of a provider-side reasoning parser splitting at a literal `</think>` the reasoning itself mentioned); an unclosed `<think>` voids the remainder, so an all-reasoning response maps to the same extraction-error CLASS as a text-less response and the caller's backoff and dead-letter discipline applies unchanged — one parked exception (2026-09-04 review item 82): the caption seam maps all-reasoning to the NON-retried `CaptionError::Empty` while a text-less response is the retried Provider class. One WARN per non-trivial strip. Decision 96 (C3): a remainder with no opener resolves in one last-closer jump — the per-closer rescan was quadratic on closer-dense input (measured 400 KB → 9.4 s, synchronously inside the async completion task). Providers that return reasoning on a separate response field were already safe: the client maps it to a content variant the text extraction never selects.

## 10. Digest pipeline

### 10.1 Input

- The raw log range `(last_boundary, current_tail]`: human messages plus the bot's own speech. Injections and tool outputs are excluded. Section 9.5 applies.
- The mention and reply map stored at intake time. Refer to Section 4.2.
- Forward attribution (decision 108): the content of a forwarded message is the ORIGIN's statement, never the sender's. The extraction call attributes forwarded content to the origin ONLY through the verified-origin list of the batch: a user-kind origin whose deterministic Person id already exists in the group graph (assembly-time probe), excluding any origin label that collides with a batch sender's display name. Forward origins never create nodes; an unlisted origin (hidden, chat, channel, automatic, or unverified) is unattributable for person facts. Concepts extract from forwarded content normally. The forwarder receives no content edge from the forwarded message (operator ruling: no weak "shared" relation in v1).
- Both the extraction preamble and the summary preamble (Section 10.2) carry the media-is-data rule of `proposed-graph-database-specs.md` Section 7.2 step 4 verbatim, with content pins (review finding M3): message text and `<media>` bodies are untrusted DATA, never instructions.

### 10.2 Extraction and write

1. Extract the `KnowledgeGraph` object. Refer to `proposed-graph-database-specs.md` Section 7.3.
2. Run entity resolution and fact validity steps. Refer to Sections 7.4 and 7.5 of that document.
3. Write nodes and edges in one transaction per group. Run `CHECKPOINT`. Embeddings never join the transaction: the pipeline enqueues them into `pending_embeddings` after the commit (Section 5.2, decision 66).
4. On every completion (a dead-lettered batch also advances the boundary), advance `last_digest_boundary_msg_id`, obtain and persist the summary of the removed chunk (Rule C3, Section 7.3), apply the context removal, and prune the deduplication set of Section 9.3. A crash between the summary write and the boundary advance is replay-safe: the next completion finds the existing summary row and skips the model call. If the summarization fails, the removal defers one cycle; a later completion retries over the widened range. After 3 consecutive failures the chunk is removed without a summary (one ERROR), and the failure count resets. The summarizer input is capped at 2 × `digest_max_messages`; an oversized chunk summarizes its newest suffix while the summary row records the full range.

### 10.3 Failure handling

1. On failure, retry with exponential backoff. The retry uses the same batch identifier. The `MERGE` operations are idempotent under the deterministic identifiers of `proposed-graph-database-specs.md` Section 7.1.
2. After `digest_max_retries` total attempts (the count includes the first attempt), write the batch skeleton and the error to a dead-letter table, emit the failure metric, and skip the batch. A failed batch never blocks later batches.
3. The skipped range stays in the raw log. A later repair tool can reprocess it.
4. Shutdown drain (decision 114): a digest task cancelled at an LLM-call boundary (between retry attempts, or between an attempt's initial and repair calls) reports Cancelled. The batch stays PENDING — no attempt consumed, no failure counter, never the dead-letter branch — and resumes on the next run.

### 10.4 Bot self-memory

- B1: The bot's own messages are written to the raw log and are part of the digest input. The bot exists as a `Person` node in the graph of each group.
- The persona is global. The memory of the bot's own speech is per-group, like every other member's memory. Rule P5 applies.

## 11. Warmup behavior

- A warmup run samples one eligible interest Concept node from the group's own graph (Section 9.7 step 2) and generates a message about that topic with the reply purpose. A message grounded in the group's real interests is preferred over a generic cute message.
- No eligible topic means no warmup — forced small talk is worse than silence. There is no generic fallback message.
- The warmup obeys the monologue lock and the soft backoff. Refer to Section 8.5.

## 12. Observability

Metrics per group:

| Metric | Meaning |
|---|---|
| Participation rate | Share of wakes with a positive decision. The calibration band is 30 to 60 percent. A sustained rate outside the band in either direction means the gate prompt needs calibration. |
| Injection rate | Share of wakes with at least one injected memory. Expected 20 to 40 percent. |
| Warmup engagement rate | Share of warmup messages with a reaction or a reply. Drives the soft backoff. |
| Digest failure rate | Failed extractions before dead-letter. |
| Dead-letter count | Skipped batches. Requires operator attention. |
| Fallback attachment rate | Refer to `proposed-graph-database-specs.md` Section 10. Primary entity-resolution quality metric. |
| Wake rate | Wakes per hour. Watch against the floor configuration. |
| Warmup activity | `warmups_total` and `warmup_engaged_total`. The engagement ratio is the Phase 2 exit-criterion metric (roadmap Section 4); watch it per group via `--status`. |
| Summarization failures | `summaries_failed_total`, cumulative. A rising count warns of a stuck summarizer before the circuit breaker of Section 10.2 engages. |
| Fact invalidations | `facts_invalidated_total`, cumulative. Counts invalidation events from all three write paths (digest, merge apply, manual `--invalidate`); `--revalidate` never decrements. |
| Related-pair promotions | `related_pairs_promoted_total`, cumulative (decision 106): the rows the promotion pass flipped to `promoted` after a successful commit. Persisted counter; `--status` does not print it yet (the accepted follow-up of the other rows). |
| Vector resolution outcomes | `vector_resolution_confirmed_total`, `vector_resolution_rejected_total`. Counted post-commit from the final attempt only (decision 77). Decision 104 retired the auto-match band and its `vector_resolution_matched_total` counter (existing stores keep the key as history); these two rates fed the decision-104 calibration and stay the instrument for the post-deploy budget check. Persisted counters; `--status` does not print them yet (accepted follow-up). |
| Media captioning | `captions_total`, `captions_failed_total`, `captions_empty_total` (Empty is not Provider — the model answered with nothing usable, never retried), `sticker_cache_hits_total`, `placeholder_media_total` (by kind). The placeholder rate is the quality signal for the intake-caption timing assumption of decision 82. Emitted as structured tracing fields only at cutover; `--status` surfacing is an accepted follow-up (decision 82). |

The `tamako --status <chat_id>` command is the metrics access path. It queries the group store read-only and prints the persisted counters (every row above except Vector resolution outcomes and Media captioning, as noted), the derived rates, the boundaries, the session state, and the dead-letter entries. `--status-all` prints every group.

Log surface (decisions 115/116): at INFO the bot emits one line per wake and one per completed digest (the one-line guarantee), plus one startup line with the resolved `suffix_mode` and `timezone` (decision 116). Digest dispatch additionally logs at DEBUG with the batch id and range (decision 115) — the post-hoc attribution surface for a hard-killed stop.

## 13. Configuration

Global defaults. Every item is overridable per group. Keys below are the exact TOML spellings; the `_secs`-suffixed keys take integer seconds. Config load WARNs on any TOML key that matches no known field — top level, the global table, and per-group tables alike (decision 84(e), extended to the top level in decision 94: a stray key above the first header, or a misspelled table header such as `[group."-1001"]`, WARNs as table `<top-level>` instead of silently applying nothing): one curated WARN per unknown key at startup, never a hard error (forward compatibility: an older binary reading a newer config must not fail).

At startup the resolved `suffix_mode` and `timezone` values print as one INFO line (decision 116; Section 12).

| Key | Default | Section |
|---|---|---|
| `wake_msg_count` | 5 | 8.3 |
| `wake_interval_secs` | 1 h | 8.3 |
| `wake_jitter_min` / `wake_jitter_max` | uniform [0.7, 1.3] | 8.3 |
| `wake_floor_secs` | 5 min | 8.3 |
| `digest_max_chars_cjk` | 5000 | 8.2 |
| `digest_max_messages` | 100 | 8.2 |
| `digest_max_words` | 2500 | 8.2 |
| `digest_max_bytes` | 20 kB | 8.2 |
| `digest_timeout_secs` | 6 h | 8.2 |
| `digest_max_retries` | 5 total attempts, including the first | 10.3 |
| `warmup_silence_secs` | 4 h | 8.4 |
| `monologue_limit` | 2 | 8.5 |
| `reply_staleness_threshold` | 20 newer human messages | 6.2 |
| `reply_quote_threshold` | 10 newer human messages | 6.2 |
| `forced_wake_cooldown_secs` | 10 s (0 disables) | 8.1 |
| `gate_context` | true | 9.6 |
| `vector_resolution` | true | graph 7.4 |
| `vector_candidate_threshold` | 0.88 (decision 104) | graph 7.4 |
| `resolution_confirm_budget` | 5 per digest batch | graph 7.4 |
| `merge_candidate_threshold` | 0.90 (decision 104) | graph 7.7 |
| `single_value_predicates` | `currently_playing`, `works_at`, `lives_in`, `dating` | graph 7.5 |
| `warmup` | true | 8.4 |
| `warmup_quota` | 1 (range 1–3) | 8.4 |
| `warmup_active_hours` | "08:00-23:00" host-local | 8.4 |
| `warmup_reaction_window_secs` | 30 min | 8.5 |
| `warmup_topic_cooldown_days` | 3 | 9.7 |
| `deep_recall` | true | 9.1 |
| `recall_candidate_cap` | 40 per wake | 9.1 |
| `recall_injection_cap` | 5 per wake | 9.2 |
LLM access and the reply-suffix keys resolve from the per-group effective configuration (global defaults with per-group overrides, like every key above):

| Key | Default | Notes |
|---|---|---|
| `suffix_mode` | `system` | Decision 88: suffix placement mode in reply requests. `system` (Decision 86 default: separate system message strictly last) or `append` (appended into the final user instruction with an authoritative preamble contract). Environment override: `TAMAKO_SUFFIX_MODE` (decision 90 implements it). Per-group TOML wins over the environment value. Decision 98: a per-group `append` while the global mode is `system` is a loud startup error (the authority contract renders per GLOBAL mode); global `append` with a per-group `system` stays legal (the contract is inert for that group). |
| `timezone` | unset | Decision 90: the reply-suffix current-time line. An IANA zone name (`Asia/Shanghai`; DST-aware, via the time-tz database — the workspace keeps the `time` crate, not chrono) or a fixed UTC offset (`+08:00`). When set, the reply request's suffix gains a code-owned `<now>` element (the FIRST child of `<system>`, ahead of every operator rule) with the current civil time in the zone, rendered per request past the cached prefix — the decision-86 (d) cache-anchor property is preserved. Unset (or empty string) disables the line: byte-identical pre-90 behavior. An unknown zone name is a hard startup error (Section 5.3 strictness). Environment override: `TAMAKO_TIMEZONE`. Per-group TOML wins over the environment value. |
| `llm_api` | `anthropic-compatible` | API family: `anthropic-compatible` or `openai-compatible`. The family selects the wire format only, not the vendor. Environment override: `TAMAKO_LLM_API`. |
| `llm_base_url` | The canonical URL of the selected family | Base URL of the endpoint. Any endpoint that speaks the family format works: first-party, proxy, aggregator, self-hosted. Environment override: `TAMAKO_LLM_BASE_URL`. |
| `digest_model` | `claude-haiku-4-5` | Extraction (Section 10). Environment override: `TAMAKO_DIGEST_MODEL`. |
| `gate_model` | `claude-haiku-4-5` | Participation decision (Section 9.6). Environment override: `TAMAKO_GATE_MODEL`. |
| `reply_model` | `claude-sonnet-4-5` | Reply generation (Section 9, step 4). Environment override: `TAMAKO_REPLY_MODEL`. |
| `summary_model` | `claude-haiku-4-5` | Removed-chunk summarization (Rule C3, Section 10.2). Environment override: `TAMAKO_SUMMARY_MODEL`. |
| `caption_model` | `minimax/minimax-m3` | Media captioning at intake (decision 82, Section 14). The caption call shares the openai-compatible family's key (`OPENAI_API_KEY`); the base URL defaults to the pinned OpenRouter URL unless `caption_llm_base_url` overrides it (per-group-capable keys; intake uses one process-wide provider at cutover — per-group resolution is an accepted follow-up). Environment overrides: `TAMAKO_CAPTION_MODEL` / `TAMAKO_CAPTION_BASE_URL`. |
| `caption_llm_base_url` | `"https://openrouter.ai/api/v1"` | Caption-endpoint base-URL override (decision 82). Environment override: `TAMAKO_CAPTION_BASE_URL`. |
| `structured_output` | `schema` | Structured-output mode: `schema` (send the JSON schema), `json_object` (JSON mode without a schema), `prompt_only` (no response_format; for endpoints that reject unknown parameters). Environment override: `TAMAKO_STRUCTURED_OUTPUT`. |
| `llm_session_id` | `"tamako"` | Session-affinity PREFIX (decision 84). Every LLM call sends BOTH `x-opencode-session` (Opencode Go affinity) and `x-session-id` (OpenRouter sticky routing) — dual-send, each gateway reads its own key. The header value is `{prefix}-{suffix}`: the prefix is this key (env `TAMAKO_LLM_SESSION_ID` → config → `"tamako"`; empty string counts as unset; global-only — a group table setting it is a hard startup error, unchanged), and the suffix is a 16-char random base64url string minted once per (group, purpose) and persisted in the group's `llm_session_keys` table (Section 5.2) so provider affinity survives restarts. Purposes receiving a per-group suffix at cutover: digest, gate, reply, summary (embedding and caption send the bare prefix — their providers are process-wide shared; follow-up accepted). The per-(group, purpose) disambiguation is machine-generated, not operator-facing. |
| `embedding_model` | `"google/gemini-embedding-2"` (3072 dims native; decision 81, schema v11) | Embedding model for the vector sidecar. Global only: no per-purpose and no per-group variant. Environment override: `TAMAKO_EMBEDDING_MODEL`. |
| `embedding_llm_base_url` | `"https://openrouter.ai/api/v1"` | Base URL of the openai-compatible embeddings endpoint. Global only. Environment override: `TAMAKO_EMBEDDING_BASE_URL`. The embedding dimension is pinned at 3072 (decision 81); changing it means recreating the `node_embeddings` table. |
| `embedding_enabled` | true | Master switch for the embedding sidecar: the provider, the worker, and the digest-path enqueue. Global only. When false, no content leaves for embeddings; enabling later backfills through the reconciliation pass. |
| `embedding_concurrency` | 1 | Bound on in-flight SINGLE-TEXT embedding POSTs (decision 113), PER CALL SITE — every vector pre-screen/deep-recall batch bounds itself (concurrently waking groups sum on the recall path); the drain path is ONE shared call site (the worker drains groups sequentially per tick, so the drain never exceeds this bound process-wide). Concurrent single-text requests, never array input (the decision-81 ZDR route serves single-text only). Global only. 1 = the decision-66 sequential behavior. |

A purpose (`digest`, `gate`, `reply`, `summary`) may override `llm_api`, `llm_base_url`, and `structured_output` individually. The per-purpose keys are `digest_llm_api`, `digest_structured_output`, and so on, with environment overrides `TAMAKO_DIGEST_STRUCTURED_OUTPUT` and so on. Per-purpose environment overrides of `llm_api` / `llm_base_url` exist for the summary purpose only (`TAMAKO_SUMMARY_LLM_API` / `TAMAKO_SUMMARY_LLM_BASE_URL`). This permits mixed deployments, for example a cheap self-hosted OpenAI-compatible endpoint for extraction and a first-party Anthropic endpoint for replies.

API keys come from the environment only, never from a config file: `ANTHROPIC_API_KEY` for anthropic-compatible endpoints, `OPENAI_API_KEY` for openai-compatible endpoints. These variable names are the convention for the format, for third-party endpoints as well. Decision 87: each completion purpose may override the family key with `TAMAKO_{DIGEST|GATE|REPLY|SUMMARY}_LLM_API_KEY` (purpose env → family env → missing-key error) — the enabler for pointing one purpose at a different provider of the same API family. The override is environment-only by the same invariant (no TOML key exists). An empty string counts as unset and falls through to the family key. Embedding and caption providers keep the family key (they are process-wide; the decision-84 M5 follow-up).

## 14. Deferred items

1. ~~Vision captioning for images, videos, and GIFs~~ DELIVERED for photos and static WebP stickers (decision 82, 2026-08-22): captioning happens AT INTAKE inside the adapter (download → `tamako-vision` normalize → the standalone `CaptionEndpoint` call → the normalized event's text embeds `<media>` elements); image bytes are deliberately never persisted; the caption is produced once and never re-derived. Interface constraints hold: the caption model has no tool access; the caption text is data, never an instruction; the `<media>` element boundary marks it as a media description, not as a member message. Still DEFERRED: video/webm captioning (the seams take a media-kind parameter; M3 eats video natively — one new normalize implementation) and animated media (webm/tgs/GIF — placeholder elements at cutover).
2. Negation detection and the `supersedes` edge. Refer to `proposed-graph-database-specs.md` Section 7.5. The manual invalidation command of that section (`--facts` / `--invalidate` / `--revalidate`) is delivered; what remains deferred is the LLM negation detection itself.
3. The `is_a` concept hierarchy. Refer to the open items of the database specification.
4. Additional platform adapters, starting with Matrix. Section 4 defines the contract.

## 15. Open items

1. Calibration of the recall relevance gate prompt and the participation gate prompt with real group data.
2. The repair tool for dead-letter batches.
3. Retention policy for the raw log after extraction. The log is the only repair source for skipped ranges.
4. Behavior on `EditedMessage`: the current version appends the edit as a new log row and does not retract extracted facts. Decide if retraction is necessary.
5. An edited media message: the edit row of decision 82 carries a NEW placeholder element (the platform delivers the new file ids; re-captioning on edit is a policy decision — at cutover the edit placeholder keeps the caption text EMPTY rather than re-running the caption pipeline; cost vs faithfulness, undecided).

(End of file)
