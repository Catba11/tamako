# Tamako

Tamako is a Telegram group-pet bot with persistent memory. It lives in chat groups, speaks rarely, and remembers facts about group members in a per-group graph database. It is a pet, not an assistant: one global persona, per-group private memories, and a scarce-attention behavior model.

Status: Phase 2 in progress (v0.1.0); Phase 1 shipped as v0.0.1. The bot intakes messages, digests conversations into long-term memory, speaks through the participation gate, and injects relevant memories into its decisions. Refer to `current-state.md` for the exact state.

## Contents

- [Documents](#documents)
- [Prerequisites](#prerequisites)
- [Build and verify](#build-and-verify)
- [Offline demo (no tokens needed)](#offline-demo-no-tokens-needed)
- [Live bring-up (Telegram)](#live-bring-up-telegram)
- [Operator commands](#operator-commands)
- [Verification commands](#verification-commands)

## Documents

| Document | Content |
|---|---|
| `specs.md` | Agent behavior: event loop, triggers, context lifecycle, configuration. |
| `proposed-graph-database-specs.md` | Memory backend: graph schema, write and read paths. |
| `dev-roadmap.md` | Phase plan. |
| `ARCHITECTURE.md` | The implementation as built. |
| `current-state.md` | Current progress, decisions, known gaps. |
| `AGENT.md` | Conventions for LLM coding agents. |

## Prerequisites

- Rust toolchain, rustc ≥ 1.96 (dependency-tree requirement; enforced by the workspace `rust-version`, decision 92).
- A C/C++ toolchain with CMake. The `lbug` crate compiles or downloads the LadybugDB core; refer to `docs/adr-0001-ladybugdb-binding.md`.
- For live operation: a Telegram bot token and an LLM API key.

## Build and verify

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

## Offline demo (no tokens needed)

```sh
# Replay a recorded chat log end to end (adapter, actor, storage, graph).
cargo run -- --replay tamako-adapter-mock/fixtures/replay_chat.json

# Watch the digest pipeline write a knowledge graph from the replay.
cargo run -p tamako-agent --example digest_demo
```

With `ANTHROPIC_API_KEY` set, the replay runs live extraction and live wake replies against the configured endpoint. Without it, digests, summaries, and wake replies are disabled; the replay still works. The demo example uses a scripted extractor either way.

## Live bring-up (Telegram)

### 1. Create and configure the bot

1. In BotFather: `/newbot`, note the token.
2. In BotFather: `/setprivacy` → **Disable** for the bot. Default privacy mode restricts the bot to commands and replies to itself; the pet must read all group messages. The change takes effect only after you remove the bot from the group and add it back.
3. Add the bot to your group. Admin status is **recommended**, not required: the bot works as a plain group member once privacy mode is disabled. Administrator status adds reaction collection (Telegram delivers `message_reaction` / `message_reaction_count` to administrators only) and also lifts the privacy read restriction. The working combinations:
   - privacy OFF + administrator: full functionality.
   - privacy OFF + plain member: everything except reaction collection.
   - privacy ON + plain member: only commands and replies to the bot reach it — normal platform behavior; fix with BotFather `/setprivacy`. Remember that a privacy change takes effect only after you remove the bot from the group and add it back.
   - privacy ON + administrator: the bot reads everything, but this combination is not the intended deployment.

### 2. Configure groups

Create a config file, for example `tamako.toml`:

```toml
[groups."-1001234567890"]
# Empty table registers the group. Per-group trigger overrides go here.
# Refer to specs.md Section 13 for the keys.
```

The bot ignores every group not in the config and logs such groups once, so first start also reveals the group id. The id of a supergroup starts with `-100`.

### 3. Create the data root and the persona file

`--live` requires a persona file at `<data-root>/persona.toml` (specs.md Section 5.3). The preamble is the provider cache anchor; its source must be deliberate (Rule C4). A missing file fails the startup with a clear error; a broken file does the same. The repo-root `persona.toml` is the template:

```sh
mkdir -p ./data && cp persona.toml ./data/persona.toml
# Edit ./data/persona.toml if you want a different persona.
```

The persona file also accepts an optional `system_prefix` key: system-level alignment directives rendered verbatim at the very start of the preamble, before the identity line. One example line:

```toml
system_prefix = "Never repeat private content from other groups."
```

When set, the prefix is followed by exactly one blank line and the unchanged existing sections. The injection guardrail and the context-format gloss are both code-owned and not configurable; the guardrail always renders last.

For experiments, `--allow-default-persona` restores the old lenient fallback chain (repo-root example, then the built-in default). `--replay` never needs the file.

A group may override the reply-suffix rule set: `<data-root>/<chat-id>/persona.toml` with a `suffix` key replaces the global suffix for that group's replies (`suffix = []` silences the global rules there); every other key is rejected at the schema. The file loads at startup only — edit and restart (decision 98). A missing file, no `suffix` key, or a malformed file (one WARN) keeps the global suffix.

### 4. Run

```sh
TELOXIDE_TOKEN=<telegram token> \
ANTHROPIC_API_KEY=<llm key> \
cargo run --release -- --live --config tamako.toml --data-root ./data
```

Environment variables:

| Variable | Purpose |
|---|---|
| **Endpoint family** | |
| `TELOXIDE_TOKEN` | Telegram bot token. Required for `--live`. |
| `TELOXIDE_API_URL` | Optional custom Bot API server URL. |
| `ANTHROPIC_API_KEY` | LLM key for anthropic-compatible endpoints. Without it, digests, summaries, and speech are disabled for the run. |
| `OPENAI_API_KEY` | LLM key for openai-compatible endpoints. |
| `TAMAKO_LLM_API` | Endpoint family override: `anthropic-compatible` or `openai-compatible`. Wins over the config file. |
| `TAMAKO_LLM_BASE_URL` | Endpoint base-URL override. Wins over the config file. |
| `TAMAKO_LLM_SESSION_ID` | Session-affinity PREFIX sent as BOTH the `x-opencode-session` and `x-session-id` headers on every request (decision 84: Opencode Go honors the first, OpenRouter sticky routing honors the second). The header value is `{prefix}-{suffix}`: the prefix is this key (global-only, default `tamako`), the suffix is a per-(group, purpose) random 16-char string persisted in the group's store (`llm_session_keys`), so provider affinity survives restarts. |
| **Per-purpose model and key overrides** | |
| `TAMAKO_DIGEST_MODEL` | Extraction model override. Default `claude-haiku-4-5`. |
| `TAMAKO_GATE_MODEL` | Participation-gate model override. Default `claude-haiku-4-5`. |
| `TAMAKO_REPLY_MODEL` | Reply-generation model override. Default `claude-sonnet-4-5`. |
| `TAMAKO_SUMMARY_MODEL` | Summary-model override (the segmented C3 summarizer). Default `claude-haiku-4-5`. |
| `TAMAKO_DIGEST_LLM_API_KEY`, `TAMAKO_GATE_LLM_API_KEY`, `TAMAKO_REPLY_LLM_API_KEY`, `TAMAKO_SUMMARY_LLM_API_KEY` | Per-purpose LLM-key overrides of the family key (decision 87), env-only — no TOML key. Unset or empty falls through to the family key. |
| `TAMAKO_SUMMARY_LLM_API`, `TAMAKO_SUMMARY_LLM_BASE_URL` | Per-purpose endpoint-family and base-URL overrides for the SUMMARY purpose (the decision-62 summarizer gets its own endpoint, decision 84), env-only. Unset or empty falls through to the family values. |
| **Embeddings and media captions** | |
| `TAMAKO_EMBEDDING_MODEL` | Embedding-model override for the vector sidecar. Default `google/gemini-embedding-2`. Global only. |
| `TAMAKO_EMBEDDING_BASE_URL` | Embedding-endpoint base-URL override. Default `https://openrouter.ai/api/v1`. Global only. |
| `TAMAKO_CAPTION_MODEL` | Media-caption model override. Default `minimax/minimax-m3`. Global only. |
| `TAMAKO_CAPTION_BASE_URL` | Media-caption endpoint base-URL override. Default `https://openrouter.ai/api/v1`. Global only. |
| **Structured output** | |
| `TAMAKO_STRUCTURED_OUTPUT` | Structured-output mode, global fallback: `schema` (default), `json_object`, `prompt_only`. |
| `TAMAKO_DIGEST_STRUCTURED_OUTPUT` | Structured-output mode override of the digest (extraction) purpose. |
| `TAMAKO_GATE_STRUCTURED_OUTPUT` | Structured-output mode override of the gate purpose. |
| `TAMAKO_REPLY_STRUCTURED_OUTPUT` | Structured-output mode override of the reply purpose. |
| `TAMAKO_SUMMARY_STRUCTURED_OUTPUT` | Structured-output mode override of the summary purpose. |
| **Reply suffix** | |
| `TAMAKO_SUFFIX_MODE` | Reply-suffix placement, global fallback: `system` (default) or `append` (decisions 88/90). Overrides the global value only; per-group TOML wins. A per-group `append` under a global `system` mode is a startup error (decision 98). |
| `TAMAKO_TIMEZONE` | Timezone of the reply-suffix `<now>` time line: IANA name or fixed `±HH:MM`; unset/empty disables the line (decision 90). Overrides the global value only; per-group TOML wins. |

Endpoint portability (specs.md Section 13): every LLM call uses one of the two API families; "compatible" describes the wire format, never the vendor. The config keys `llm_api` / `llm_base_url` select any anthropic-compatible or openai-compatible endpoint (proxy, aggregator, self-hosted), each purpose (`digest`, `gate`, `reply`, `summary`) may override them individually, and the same pattern applies to the structured-output mode. Section 13 owns the full key set, the per-purpose env overrides, and the precedence rules. API keys come from the environment only, never from the config file.

Embeddings (specs.md Section 13, schema v7): the vector sidecar embeds Person/Alias/Concept names and descriptions through an openai-compatible `/v1/embeddings` endpoint — by default OpenRouter + `google/gemini-embedding-2`, keyed by `OPENAI_API_KEY`. The sidecar is rebuildable derived data; a missing key degrades embeddings to a startup warning, and the digest pipeline is unaffected. Note the dual-use: embeddings read the SAME `OPENAI_API_KEY` as openai-compatible completions — when the completions endpoint points elsewhere (e.g. Opencode Go), the key must still be valid for the embedding endpoint; startup logs one WARN in that configuration (decision 77).

### Recipe: Opencode Go

Opencode Go serves both API families, split by model family. Chat/completions models (Grok, GLM, Kimi, DeepSeek, MiMo, Hy3) take base `https://opencode.ai/zen/go/v1`; MiniMax/Qwen models speak the Anthropic Messages format at `https://opencode.ai/zen/go/v1/messages` with `llm_api = "anthropic-compatible"`. The mimo recipe (`tamako.example.toml` is a pre-filled variant with commentary):

```toml
[global]
llm_api = "openai-compatible"
llm_base_url = "https://opencode.ai/zen/go/v1"
digest_model = "mimo-v2.5"        # extraction
gate_model = "mimo-v2.5"          # participation gate + recall gate
reply_model = "mimo-v2.5-pro"     # reply generation
summary_model = "mimo-v2.5"       # segmented summarizer
# structured_output = "schema"    # the default; recommended here
```

with `OPENAI_API_KEY` in the environment.

Recommended `structured_output` for Opencode Go: `schema`, the default — endpoint-verified 2026-08-08. The chat/completions models accept and honor `response_format: {type: "json_schema", strict: true}` byte-exactly, both for a trivial probe schema and for the full nested `KnowledgeGraph` schema, and the structured modes suppress reasoning tokens entirely. Free generation on mimo-v2.5 instead burns ~85-300 reasoning tokens per call, may wrap the output in markdown fences, and cost 5x latency on the probe (5.7 s vs 1.1 s). Pick `json_object` when an OpenAI-family endpoint rejects schema mode (sound on Opencode Go; on anthropic-compatible endpoints it degrades to prompt-only — the Messages API has no json_object format). Pick `prompt_only` only as the last resort for endpoints that reject both. `cargo run -p tamako-agent --example probe_endpoint` probes an endpoint before you commit to a mode.

### 5. Expected behavior right now

- Startup logs the bot identity and the configured groups.
- Startup runs a capability check: one `getChatMember` call per configured group.
  - An administrator group logs one INFO line: `bot is an administrator of this group; full functionality (reaction collection active).`
  - A non-administrator group logs one WARN per run: reaction collection is OFF (Telegram delivers reaction updates to administrators only); everything else works normally. The WARN text also covers the privacy-mode interaction of Section 1.
  - If the check fails (the bot may not be a member yet), an INFO line explains that the status is re-checked when the first event of the group arrives.
- The first message in a configured group creates `{data-root}/{chat_id}/store.db` and `memory.lbug`.
- Every message lands in the raw log before any other processing. Reactions land in the `reactions` table; reaction collection is active only in groups where the bot is an administrator.
- Digest triggers fire on their thresholds; extraction writes entities and facts into the graph. At digest completion the chunk the context drops is LLM-summarized first, and the context keeps the two newest `<summary range="first-last">...</summary>` items; after 3 consecutive summarization failures the chunk drops unsummarized with one ERROR line (circuit breaker). Watch the logs for batch outcomes.
- The bot speaks: it answers mentions and replies to itself directly, and it joins the conversation when the participation gate says yes.
  - A reply is a Telegram reply-to of its target only when the target is stale (more than `reply_quote_threshold`, default 10, newer human messages) or the wake was forced; a recent target gets a plain standalone message.
  - Every reply lands in the raw log before it is sent. Two consecutive bot messages engage the monologue lock; any human message unlocks.
  - Without an LLM key for the configured endpoint family the bot stays silent (the wake procedure logs one warning at startup).
- The bot remembers: every wake runs a recall over the group graph — exact alias matches and the people of the new messages, plus deep recall (the default): vector entry over the node embeddings, two-hop graph expansion, and edge-description text search, all gated by the conservative relevance gate.
  - The gate selects at most `recall_injection_cap` (default 5) memories; a non-empty selection enters the context as one `<memory>...</memory>` assistant-role item — visible to the gate and the reply model, present even when the bot stays silent.
  - Injected memories are deduplicated per edge across wakes and removed at digest time; a multi-edge injection appears as one context item (collapse at rebuild).
  - Set `deep_recall = false` to restore the shallow path (direct neighbors only, no fuzzy scans).
- The bot starts conversations on its own (warmup, specs.md Sections 8.4/8.5/9.7):
  - Quota: up to `warmup_quota` (default 1, valid range 1–3) self-initiated messages per host-local day, at a random slot inside `warmup_active_hours` (default "08:00-23:00"); the schedule persists across restarts.
  - Conditions: only after `warmup_silence_secs` (default 14400 = 4 h) of group silence, never while muted, and only when the memory graph holds an eligible topic — a Concept node sampled with weight edge-count × recency-decay, skipping topics used in the last `warmup_topic_cooldown_days` (default 3) days or mentioned in the latest 50 messages; a group with no eligible topic stays silent.
  - The message is a plain standalone message (no reply quote, no ping) generated with the reply model; an interest tied to one member is asked as an open group question, never "X likes Y".
  - Engagement: a human reply or a reaction within `warmup_reaction_window_secs` (default 1800 = 30 min); reactions only reach the bot in administrator groups, so non-administrator groups measure replies only. Ignored warmups back off (2× spacing per ignored warmup, effective quota floored at 1, resetting on engagement).
  - `warmup = false` disables warmups entirely. The counters `warmups_total` / `warmup_engaged_total` show in `--status`.
- Ctrl-c shuts down gracefully and flushes session state; a restart rebuilds identical state.

### 6. Watching the pet

At the default `info` level the bot emits exactly one line per wake and one line per completed digest — the one-line guarantee. Nothing else is needed to follow the pet's behavior:

```text
INFO tamako_core::actor: wake chat_id=-1001234567890 trigger="message_count" injections=0 gate="participate" reason="a direct question" action="reply_sent" reply_to="w3"
INFO tamako_core::actor: wake chat_id=-1001234567890 trigger="forced" injections=0 gate="bypassed_forced" action="reply_sent" reply_to="100004"
INFO tamako_core::actor: digest chat_id=-1001234567890 batch_id=e1ba1491-bf91-5853-b0f1-58b24ba26c98 range=(0,101] outcome="written" nodes=19 edges=30
INFO tamako_core::actor: warmup chat_id=-1001234567890 topic="GRPO" action="sent"
```

The `wake` fields: `trigger` (`message_count`|`interval`|`forced`), `injections` (recall-memory count, 0 allowed), `gate` (`participate`|`silent`|`bypassed_forced`|`muted`|`in_flight_skipped`), `reason` (the gate's own reason — present only when it gives one), `action` (`reply_sent`|`discarded_stale`|`nothing`), `reply_to` (the target's platform message id — present only on `reply_sent`). A failed wake instead emits one ERROR line `wake procedure failed; skipping this wake`. The `digest` fields: `batch_id`, `range` (the `(old,new]` msg-id range), `outcome` (`written` with `nodes`/`edges` counts, or `skeleton`); a dead-lettered batch keeps the pipeline's ERROR line `digest batch dead-lettered after all retries` as its one line. Note: during a fast catch-up replay thousands of `in_flight_skipped` wake lines can appear (one per suppressed fire while an LLM wake runs); at live tempo they are rare. Summarization failures log at WARN; the circuit-breaker drop (3 consecutive failures) logs one ERROR. A sent warmup is its own one INFO line with `topic` (the sampled Concept node) and `action="sent"`; warmup skips stay at DEBUG.

Add `-v` (or `--verbose`, accepted in every mode) to lift every Tamako crate to debug level while dependencies stay quiet — useful when one of the lines above needs its backstory. `RUST_LOG` always wins over the flag; use it as the escape hatch for anything finer:

```sh
RUST_LOG=tamako_core=debug cargo run --release -- --live --config tamako.toml --data-root ./data
# or: RUST_LOG=tamako=trace,reqwest=warn ...
```

## Operator commands

The status modes inspect the group stores read-only. They are safe while the bot runs (the store is a WAL-mode SQLite database), and they never need a persona file, a Telegram token, or an LLM key.

```sh
# One group: counters and rates (specs.md Section 12), digest boundaries,
# muted state, and the most recent dead letters (specs.md Section 10.3).
cargo run -- --status -1001234567890 --data-root ./data

# Every group store under the data root, sorted by chat id.
cargo run -- --status-all --data-root ./data
```

`--status` exits with an error when the group has no `store.db` yet. `--status-all` with no group stores prints a note and exits successfully. Caveat: after a CLEAN shutdown the bot removes the WAL files; a read-only status run then still opens the database, but the first query can fail when the directory is not writable — the error message carries a hint. Make the data-root directory writable, or start the bot once and stop it.

The merge modes are the offline graph-repair tool (current-state.md decision 74, `proposed-graph-database-specs.md` Section 7.7): they deduplicate fragmented Person/Concept nodes. The mutating commands (`--merge-tool --apply`, `--merge`, `--merge-rollback`) take a per-group advisory lock, `{data-root}/{chat_id}/.tamako.lock`, and refuse loudly when another process holds it (`is the bot running? lock held: <path>`); `--live` holds the same lock per served group for its whole lifetime. The `--merge-tool` dry run opens the group's `store.db` READ-ONLY and writes only the plan file. Behind the lock, LadybugDB itself also holds an exclusive OS file lock on `memory.lbug`: a second process that somehow gets that far fails loudly at the open (`Could not set lock on file … Resource temporarily unavailable`) instead of corrupting the graph.

```sh
# Scan the vector index for duplicate candidates, confirm each pair once
# on the digest endpoint (verdicts: same merges, related links
# table, different skips; decision 83), print the plan, and write it to
# {data-root}/{chat_id}/merge_plan.json. DRY RUN (read-only store).
cargo run -- --merge-tool -1001234567890 --data-root ./data --config tamako.toml

# Execute the PLAN FILE of a previous dry run (decision 77, two-step
# apply): the apply re-scans the candidates (cheap, no LLM) and refuses
# loudly when the candidate set changed since the dry run ("plan is
# stale; re-run the dry run"). A matching plan executes the file's
# actions; every action appends a merge_audit row (specs.md Section 5.2)
# and a fully applied plan retires its file.
cargo run -- --merge-tool -1001234567890 --apply --data-root ./data --config tamako.toml

# Manual merge, no LLM: merge loser into survivor as an operator-decided
# 'same' action (confirmed_by "operator"). Prints the audit id. A
# kind-incompatible pair (Person vs Concept) is a hard error unless
# --force is given.
cargo run -- --merge -1001234567890 <loser_id> <survivor_id> --data-root ./data

# Roll one 'same' merge back from its audit snapshot; the row is marked
# rolled back. Refusals (unknown id, non-same row, already rolled back)
# exit non-zero.
cargo run -- --merge-rollback -1001234567890 <audit_id> --data-root ./data
```

Without a digest-endpoint LLM key `--merge-tool` prints only the scan (the candidate pairs above the threshold) with a note that the confirmations were skipped; it writes no plan file. The candidate threshold is the per-group `merge_candidate_threshold` config key (default 0.85); `--max-confirmations N` caps the LLM confirmation calls of one run (default 50). Rollback does not re-embed the restored node itself: the next startup reconciliation of the embedding worker picks it up automatically (decision 66).

The fact modes are the offline fact-validity tool (current-state.md decision 75, `proposed-graph-database-specs.md` Section 7.5): they list, invalidate, and re-validate the edges of one node. No LLM key is needed. `--invalidate` / `--revalidate` WRITE the graph and take the same per-group lock as the merge modes, so they refuse loudly while the bot runs. `--facts` is read-only on the store side, but its graph open still conflicts with a running bot (the LadybugDB file lock), so stop the bot first for all three. The flow: `--facts` prints the edge ids that `--invalidate` / `--revalidate` take.

```sh
# List every edge of the node resolved from <name> (exact alias match),
# both directions, valid AND invalid — with the edge id, the direction,
# the predicate, the other endpoint, a description excerpt, and the
# validity timestamps. An unknown name exits non-zero.
cargo run -- --facts -1001234567890 <name> --data-root ./data

# Set invalid_at on one edge. The edge id is the opaque string that
# --facts prints; a malformed or unknown id exits non-zero. A successful
# invalidation bumps facts_invalidated_total.
cargo run -- --invalidate -1001234567890 <edge_id> --data-root ./data

# Clear invalid_at on one edge (the typo safety net). Does NOT decrement
# facts_invalidated_total: the counter counts invalidation events.
cargo run -- --revalidate -1001234567890 <edge_id> --data-root ./data
```

Invalidation is non-destructive and self-recording: it sets `invalid_at` on the edge row itself (no audit table), and the recall path has filtered invalid edges since M5 — an invalidated fact stops entering wakes immediately. The `facts_invalidated_total` counter (visible in `--status`) counts invalidation events from all three write paths: the digest path, the merge apply path, and the manual `--invalidate` command. Replay note (decision 77): a replayed batch never re-validates an invalidated edge — the stored `invalid_at` survives — but an out-of-order FULL replay can still rewind a fact's other columns (edge text, properties) to the replayed batch's values; in-order replay converges.

## Verification commands

```sh
# Optional live smoke test (needs a token; send a group message within 60 s).
TAMAKO_LIVE_TELEGRAM=1 TELOXIDE_TOKEN=<token> \
  cargo test -p tamako-adapter-teloxide --test live_smoke -- --ignored

# Optional live LLM smoke test.
TAMAKO_LIVE_TEST=1 ANTHROPIC_API_KEY=<key> \
  cargo test -p tamako-agent -- --ignored
```
