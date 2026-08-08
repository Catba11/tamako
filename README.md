# Tamako

Tamako is a Telegram group-pet bot with persistent memory. It lives in chat groups, speaks rarely, and remembers facts about group members in a per-group graph database. It is a pet, not an assistant: one global persona, per-group private memories, and a scarce-attention behavior model.

Status: Phase 1 in progress. The bot intakes messages, stores reactions, digests conversations into long-term memory, and SPEAKS: it answers mentions and replies directly, and it joins conversations when the participation gate says yes (M4). It also REMEMBERS out loud: a wake with relevant graph memories injects them as one "I remember: ..." assistant message before the participation decision (M5). Refer to `current-state.md`.

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

- Rust toolchain, rustc ≥ 1.85 (teloxide requirement).
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

With `ANTHROPIC_API_KEY` set, the replay runs live extraction and live wake replies against the configured endpoint. Without it, digests are disabled and the bot stays silent; the replay still works. The demo example uses a scripted extractor either way.

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

For experiments, `--allow-default-persona` restores the old lenient fallback chain (repo-root example, then the built-in default). `--replay` never needs the file.

### 4. Run

```sh
TELOXIDE_TOKEN=<telegram token> \
ANTHROPIC_API_KEY=<llm key> \
cargo run --release -- --live --config tamako.toml --data-root ./data
```

Environment variables:

| Variable | Purpose |
|---|---|
| `TELOXIDE_TOKEN` | Telegram bot token. Required for `--live`. |
| `TELOXIDE_API_URL` | Optional custom Bot API server URL. |
| `ANTHROPIC_API_KEY` | LLM key for anthropic-compatible endpoints. Without it, digests and speech are disabled for the run. |
| `OPENAI_API_KEY` | LLM key for openai-compatible endpoints. |
| `TAMAKO_DIGEST_MODEL` | Extraction model override. Default `claude-haiku-4-5`. |
| `TAMAKO_GATE_MODEL` | Participation-gate model override. Default `claude-haiku-4-5`. |
| `TAMAKO_REPLY_MODEL` | Reply-generation model override. Default `claude-sonnet-4-5`. |
| `TAMAKO_LLM_API` | Endpoint family override: `anthropic-compatible` or `openai-compatible`. Wins over the config file. |
| `TAMAKO_LLM_BASE_URL` | Endpoint base-URL override. Wins over the config file. |

Endpoint portability (specs.md Section 13): every LLM call uses one of the two API families above; "compatible" describes the wire format, never the vendor. The config file keys `llm_api` and `llm_base_url` select an arbitrary anthropic-compatible or openai-compatible endpoint (proxy, aggregator, self-hosted), and a purpose (`digest`, `gate`, `reply`) may override them individually (`digest_llm_api`, `digest_llm_base_url`, and likewise for `gate_` and `reply_`). API keys come from the environment only, never from the config file.

### 5. Expected behavior right now

- Startup logs the bot identity and the configured groups.
- Startup runs a capability check: one `getChatMember` call per configured group. An administrator group logs at INFO: `bot is an administrator of this group; full functionality (reaction collection active).` A non-administrator group logs one WARN per run: `the bot is not an administrator of this group: Telegram delivers reaction updates to administrators only, so reaction collection is OFF for this group. Everything else works normally. To enable reactions, make the bot a group administrator. Note: privacy mode OFF alone suffices for reading all group messages; if privacy mode is still ON (the BotFather default) the bot receives only commands and replies to itself, which is normal platform behavior.` If the check fails (the bot may not be a member yet), an INFO line explains that the status is re-checked when the first event of the group arrives.
- The first message in a configured group creates `{data-root}/{chat_id}/store.db` and `memory.lbug`.
- Every message lands in the raw log before any other processing. Reactions land in the `reactions` table; reaction collection is active only in groups where the bot is an administrator.
- Digest triggers fire on their thresholds; extraction writes entities and facts into the graph. Watch the logs for batch outcomes.
- The bot speaks: it answers mentions and replies to itself directly, and it joins the conversation when the participation gate says yes. Every reply is a reply-to of its target message and lands in the raw log before it is sent. Two consecutive bot messages engage the monologue lock; any human message unlocks. Without an LLM key for the configured endpoint family the bot stays silent (the wake procedure logs one warning at startup).
- The bot remembers: every wake runs a shallow recall over the group graph (exact alias matches and the people of the new messages; no fuzzy scans). A conservative relevance gate selects at most `recall_injection_cap` (default 5) memories; a non-empty selection enters the context as one "I remember: ..." assistant message — visible to the participation gate and the reply model, and present even when the bot stays silent. Injected memories are deduplicated per digest chunk and removed at digest time.
- Ctrl-c shuts down gracefully and flushes session state; a restart rebuilds identical state.

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

## Verification commands

```sh
# Optional live smoke test (needs a token; send a group message within 60 s).
TAMAKO_LIVE_TELEGRAM=1 TELOXIDE_TOKEN=<token> \
  cargo test -p tamako-adapter-teloxide --test live_smoke -- --ignored

# Optional live LLM smoke test.
TAMAKO_LIVE_TEST=1 ANTHROPIC_API_KEY=<key> \
  cargo test -p tamako-agent -- --ignored
```
