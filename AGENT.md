# AGENT.md — Guidance for LLM Agents in the Tamako Repository

This file tells LLM coding agents how to work in this repository. Read it before any task.

## 1. What this project is

Tamako is a Telegram group-pet bot with persistent memory. It lives in chat groups, speaks rarely, and remembers facts about group members in a per-group graph database. The bot is a pet, not an assistant: it has a global persona, per-group private memories, and a scarce-attention behavior model.

## 2. Governing documents

These documents define the system. Code must follow them. When code and a document disagree, stop and report the conflict; do not improvise.

| Document | Content |
|---|---|
| `proposed-graph-database-specs.md` | Memory backend: LadybugDB schema, storage rules R1–R5, write path, read path. |
| `specs.md` | Agent behavior: principles P1–P7, adapter rules A1–A5, context rules C1–C5, triggers, wake and digest pipelines, configuration defaults. |
| `dev-roadmap.md` | Phase split, phase scope, anti-defer list. |

Rules in these documents have identifiers (example: R3, P5, C2). Reference these identifiers in code comments where the code implements the rule. Example: `// Rule P5: one database file per group`.

## 3. Current phase and scope guard

The repository is **code-complete for Phase 1**; the two-week test-group soak runs on live groups. Refer to `dev-roadmap.md` Section 3 and to `current-state.md` for the exact state.

- rig.rs and teloxide are permitted. They entered in Phase 1.
- Do not add vector search or embeddings. They enter in Phase 2.
- Do not implement warmup, deep recall, or fact invalidation. They enter in Phase 2. Shallow recall (exact alias match) with the full injection protocol is in scope. Refer to `dev-roadmap.md` Section 3 item 7.
- If a task seems to require a deferred item, stop and report. The phase split is deliberate.

## 4. Workspace layout

The workspace is a Cargo workspace at the repository root. Crates:

| Crate | Role |
|---|---|
| `tamako` | Binary. Wiring, configuration loading, CLI. |
| `tamako-core` | Normalized events and actions, the per-group actor, trigger logic, session state, the digest pipeline contract. |
| `tamako-store` | `store.db`: SQLite access, migrations, raw message log, state table. |
| `tamako-memory` | The `MemoryBackend` trait and the real `LbugBackend` implementation (lbug 0.18), plus deterministic identifiers. |
| `tamako-persona` | Global persona configuration and preamble rendering. |
| `tamako-adapter-mock` | Mock platform adapter and the replay fixture for tests. |
| `tamako-adapter-teloxide` | Live Telegram adapter (teloxide). Pure normalization plus polling intake and outbound actions. |
| `tamako-agent` | All LLM concerns: the extraction call (rig) and the digest pipeline. The only crate that depends on rig. |

Dependency direction: `tamako` depends on all crates. `tamako-core` depends on `tamako-store`, `tamako-memory`, and `tamako-persona` through traits. The adapter crates (`tamako-adapter-mock`, `tamako-adapter-teloxide`) depend on `tamako-core` types only. `tamako-agent` depends on `tamako-core` (the digest contract), `tamako-store`, and `tamako-memory`. No cycles.

## 5. Commands

- Build: `cargo build --workspace`
- Test: `cargo test --workspace`
- Lint: `cargo clippy --workspace -- -D warnings`
- Format: `cargo fmt --all`

A task is done when all four commands pass.

## 6. Conventions

### 6.1 Language and documentation

- Documentation and comments use ASD-STE100 Simplified Technical English where practical: short sentences, active voice, one instruction per sentence.
- This rule is soft. Break it when it makes the text less clear or less natural. Clarity wins.
- Identifiers, configuration keys, and log messages use snake_case English.

### 6.2 Architecture rules that apply to all code

- Rule P1: the raw message log is the source of truth. Every inbound message is persisted before any processing.
- Rule P5 / `specs.md` Section 5: one group's data never crosses into another group. All storage APIs take a `chat_id`.
- Rule A1: platform-specific types stay inside adapter crates. `tamako-core` sees normalized events only.
- Synchronous storage calls (`rusqlite`, LadybugDB) run inside `tokio::task::spawn_blocking`. Never block the async runtime.
- All storage writes are idempotent or transactional. Batch identifiers are stable across retries.

### 6.3 Configuration

- Configuration defaults come from `specs.md` Section 13. Every key is overridable per group.
- Do not invent new configuration keys without a matching entry in `specs.md` or a reported deviation.

### 6.4 Error handling

- Library crates return typed errors (`thiserror`). The binary crate uses `anyhow`.
- A failed digest batch must not block later batches. Refer to `specs.md` Section 10.3.

## 7. Definition of done

1. The four commands of Section 5 pass.
2. New behavior has tests. The current phase scope and exit criteria live in `dev-roadmap.md` and `current-state.md`; do not build deferred items (Section 3).
3. Deviations from the governing documents are reported in the pull request description or the task result, not hidden in code.
