# Watchdog notes

Shared orientation for every advisor in this workspace. Lane-specific
pattern lists live in `WATCHDOG.yml` under each roster entry.

## What this project is

Tamako is a Telegram group-pet bot with persistent memory. Rust workspace,
nine `tamako*` crates at the repository root. The bot lives in chat groups,
speaks rarely, and keeps per-group memories in SQLite (`store.db`) plus a
LadybugDB graph (`memory.lbug`), under a per-group data directory.

## Governing documents

These documents define the system. Code follows them. When code and a
document disagree, the correct outcome is a reported conflict, not an
improvised fix.

- `proposed-graph-database-specs.md` — memory backend schema, storage rules R1–R5.
- `specs.md` — agent behavior: principles P1–P7, adapter rules A1–A5,
  context rules C1–C5, pipelines, configuration defaults (Section 13).
- `dev-roadmap.md` — phase split and the Phase-3 deferred set.
- `current-state.md` — numbered decision log; every behavioral change
  carries an entry (decision 92).
- `AGENT.md` — crate layout, dependency direction, conventions.

Rules have identifiers (P5, A1, C2, R3). Cite the identifier when you
report against a rule.

## Definition of done

`cargo build --workspace`, `cargo test --workspace`,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all`.
A task is done when all four pass. Some suites are env-gated live tests
that skip silently: a green run does not prove those paths ran.

## Search scope

Repo-wide searches exclude `./data` (live data) and `./target` (build
output). Pass explicit paths to search tools.

## How to read your pattern list

The patterns in your roster instructions are classes, not checklists. A
same-class instance on a surface the list does not name still qualifies.
Every report carries evidence — file:line or symbol — and names its class.
No class match means silence.
