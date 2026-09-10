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
- Every shape rendered into the context is a shape the model can imitate. A forbidden shape never appears in a high-salience position (examples, tail instructions) — that is how the parrot family recurred (decisions 59–97).
- Parsers over model-visible or model-produced text assume adversarial input: lookalike tokens, truncated structures, nesting (decision 97). The outbound filter is the backstop, not the primary defense. For a new high-risk outbound format, the adversarial test plan is an operator discussion, case by case (decision 110).

### 6.3 Configuration

- Configuration defaults come from `specs.md` Section 13. Every key is overridable per group.
- Do not invent new configuration keys without a matching entry in `specs.md` or a reported deviation.

### 6.4 Error handling

- Library crates return typed errors (`thiserror`). The binary crate uses `anyhow`.
- A failed digest batch must not block later batches. Refer to `specs.md` Section 10.3.
- Failures are loud. Never swallow an error, silently fall back to a default, or degrade to a log line unless a decision entry records the rationale. Warn-only observability has demonstrably failed here: the decision-84(e) unknown-key WARN sat unread for weeks. New configuration surfaces use `deny_unknown_fields` or an equivalent catch-all. WARNs carry attribution fields (chat id, purpose, model, key).
- A live defect with an unknown mechanism is not parked indefinitely: record the candidate mechanisms, the defense-in-depth gaps, a measurable sunset criterion, and the reopen condition in `current-state.md` Section 4 (the K2 pattern, closed 2026-09-09).

### 6.5 Documentation synchronization

Two rules that were oral until decision 92: every behavioral change carries a numbered decision entry in `current-state.md` Section 3, and the documentation commit lands BEFORE the code commit (docs-first).

A change that touches one of these surfaces updates ALL of the listed documents in the same change (or its paired docs commit):

| Change type | Surfaces to update |
|---|---|
| Environment variable (add, rename, default change) | `specs.md` Section 13; the `README.md` environment table |
| TOML key (add, rename, default change) | `specs.md` Section 13; `tamako.example.toml` |
| CLI flag or operator-facing output string | The owning `specs.md` section; `README.md` Operator commands; the USAGE/help text |
| Phase or delivery status | `current-state.md` Section 1; the `dev-roadmap.md` status header; the `README.md` status line; Section 3 of this file |
| Crate added or dependency boundary changed | Section 4 of this file; `current-state.md` Section 2; `ARCHITECTURE.md` Sections 1–2; `Cargo.toml` |
| Observable behavior | The `current-state.md` Section 3 decision entry; the owning `specs.md` section; the owning `ARCHITECTURE.md` section |

Backstop before committing such a change: `grep -n <token> README.md tamako.example.toml specs.md current-state.md dev-roadmap.md AGENT.md ARCHITECTURE.md` and review every hit.

`README.md` restates reference content only where bring-up usability wins (the environment table). Everywhere else, compress and point to the owning document (decision 92).

Search scope convention (operator ruling, 2026-09-06): repo-wide searches EXCLUDE `./data` (live data) and `./target` (build output) — pass explicit paths to the search tool instead of searching the repository root. Search inside those directories only when troubleshooting their contents.

### 6.6 Git branch discipline (operator ruling, 2026-09-07)

- Do ALL work on `main`. `catball-self-use` is the operator's live branch (main plus private prompt-tuning patches); never commit work to it directly.
- Run `git branch --show-current` before EVERY commit. If it does not print `main`, stop and switch. (Violated twice: 2026-09-08 — a docs commit landed on `catball-self-use`, recovered with `git reset --soft HEAD~1`, a stash move, and a recommit on main; 2026-09-10 — the `.gitignore` `*.log` commit, same recovery.)
- After pushing main, rebase the live branch and STAY on it: `git checkout catball-self-use && git rebase main`. Resolve conflicts by keeping both sides faithful (the main change plus the self-use hunks). The worktree rests on the BRANCH, never on main (operator ruling, 2026-09-10, `.omp/rules/build-on-production-branch.md`): main-side work ends by returning to `catball-self-use`.
- The bot's release binary builds from the BRANCH tip. Before ANY build or launch command for the live bot, run `git branch --show-current` standalone; if it prints `main`, switch first and say so. Rebuild after a rebase when a restart is due. (Violated once on 2026-09-10: a rebuild ran with HEAD on main; the main-built bot served ~5 minutes without the branch-only tunings before the stop-rebuild-restart. A main-built binary silently drops the branch tunings and voids any observation window measured against it.)
- NEVER `git add -A`, and never stage a directory: untracked never-commit files live in the worktree (the gitignored live config `data/persona.toml` and `tamako.toml`, review/eval notes, scratch examples). Stage explicit file paths.
- Prompt-tuning content is confidential: it stays on `catball-self-use` and never crosses into a main-bound commit.

### 6.7 Dependencies and external behavior (decision 110)

- Do not extrapolate behavior across models, gateways, or dependency versions. Probe each new model endpoint (`probe_endpoint`) before it serves traffic (decision 56).
- A behavioral assumption about a dependency either carries measured evidence or is labeled UNVERIFIED where the code relies on it.
- A version pin must lock the layer that actually takes effect: a crate pin does not pin the artifacts the crate downloads (decision 109: `LBUG_VERSION` pins the lbug C++ core).
- An uncalibrated default or threshold ships with a stated calibration plan (instrument, window, review point). A placeholder number without a plan is a defect (decisions 81(d) and 104).
- A retrievability claim names durable media: session context and `/tmp` are not archives. "Recorded" or "reproducible" means a committed file or a named operator-held file; anything else is not archived.

### 6.8 Agent operating discipline (decision 110)

- Advisor notes are input, not operator rulings. Changing operator-ruled state (a closed decision entry, a ruled disposition) requires a fresh operator ruling, even when the change is additive.
- Edit anchors are verified byte-for-byte against a fresh read. A rewrite re-emits only the spans its match covered.
- A maintenance command is sized for the actual state (measure first) and accounts for live writers into the target area (example: the rust-analyzer flycheck rebuilds `target/` during a delete).

## 7. Definition of done

1. The four commands of Section 5 pass.
2. New behavior has tests. The current phase scope and exit criteria live in `dev-roadmap.md` and `current-state.md`; do not build deferred items (Section 3).
   - A regression test for a fix first demonstrates that it catches the bug (red-first).
   - At least one test of a hardening change runs with preconditions unmet (the minimal environment). A test that silently satisfies a precondition hides the regression it guards (decision 107: the lock test pre-created the directory and missed the 18-day ENOENT).
   - A test that pins suspect behavior is annotated as suspect.
3. Deviations from the governing documents are reported in the pull request description or the task result, not hidden in code.
4. The documentation synchronization of Section 6.5 is done (decision 92).
