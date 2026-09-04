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

The repository is **past Phase 1** (v0.0.1) and **mid-Phase 2**: the embedding sidecar, the vector pre-screen, the merge tool, fact validity with the manual invalidation command, deep recall, the warmup trigger, persona hot reload, media captioning, and the decisions 81–88 batch are all on main. Refer to `dev-roadmap.md` Section 4 and to `current-state.md` for the exact state.

- rig.rs, teloxide, and sqlite-vec are permitted. They entered in Phases 1–2.
- Do not build the Phase 3 deferred set (`dev-roadmap.md` Section 5): negation detection with the `supersedes` edge, the dead-letter repair tool, edit retraction semantics, the `is_a` concept hierarchy, the Matrix adapter, and the metrics backend (parked by the operator, 2026-08-22). `SendMedia` also stays deferred: it returns `Unsupported` until Phase 3 (current-state.md Section 5, M3 row).
- The operator's self-use prompt tuning lives on the `catball-self-use` branch by deliberate divergence (current-state.md decisions 55 and 89). Do not port it to main.
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
| `tamako-vision` | Pure media normalization for intake captioning (decision 82): byte-level allowlist sniffing, JPEG/PNG/WebP dimension extraction, resize and re-encode to the 2048-px JPEG cap, base64 data-URI output. No LLM, no I/O. |
| `tamako-agent` | All LLM concerns: the extraction call (rig) and the digest pipeline. The only crate that depends on rig. |

Dependency direction: `tamako` depends on all crates. `tamako-core` depends on `tamako-store`, `tamako-memory`, and `tamako-persona` through traits. The adapter crates (`tamako-adapter-mock`, `tamako-adapter-teloxide`) depend on `tamako-core` types only. `tamako-agent` depends on `tamako-core` (the digest contract), `tamako-store`, `tamako-memory`, and `tamako-persona` (the shared context-format gloss). `tamako-vision` is a pure leaf: `tamako-adapter-teloxide` (the intake enrichment stage) and the `tamako-agent` caption tests depend on it; `tamako-core` deliberately does not. No cycles.

## 5. Commands

- Build: `cargo build --workspace`
- Test: `cargo test --workspace`
- Lint: `cargo clippy --workspace --all-targets -- -D warnings`
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

### 6.5 Documentation synchronization

Two rules that were oral until decision 92: every behavioral change carries a numbered decision entry in `current-state.md` Section 3, and the documentation commit lands BEFORE the code commit (docs-first).

A change that touches one of these surfaces updates ALL of the listed documents in the same change (or its paired docs commit):

| Change type | Surfaces to update |
|---|---|
| Environment variable (add, rename, default change) | `specs.md` Section 13; the `README.md` environment table |
| TOML key (add, rename, default change) | `specs.md` Section 13; `tamako.example.toml` |
| CLI flag or operator-facing output string | The owning `specs.md` section; `README.md` Operator commands; the USAGE/help text |
| Phase or delivery status | `current-state.md` Section 1; the `dev-roadmap.md` status header; the `README.md` status line; Section 3 of this file |
| Crate added or dependency boundary changed | Section 4 of this file; `current-state.md` Section 2; `Cargo.toml` |
| Observable behavior | The `current-state.md` Section 3 decision entry; the owning `specs.md` section |

Backstop before committing such a change: `grep -n <token> README.md tamako.example.toml specs.md current-state.md dev-roadmap.md AGENT.md` and review every hit.

`README.md` restates reference content only where bring-up usability wins (the environment table). Everywhere else, compress and point to the owning document (decision 92).

## 7. Definition of done

1. The four commands of Section 5 pass.
2. New behavior has tests. The current phase scope and exit criteria live in `dev-roadmap.md` and `current-state.md`; do not build deferred items (Section 3).
3. Deviations from the governing documents are reported in the pull request description or the task result, not hidden in code.
4. The documentation synchronization of Section 6.5 is done (decision 92).
