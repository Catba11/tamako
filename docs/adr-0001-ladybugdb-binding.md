# ADR 0001: LadybugDB Rust binding

- Status: accepted
- Date: 2026-08-07

## Context

Tamako stores per-group long-term memory in LadybugDB
(`proposed-graph-database-specs.md`). The Rust workspace needs a client
binding for the `tamako-memory` crate. The hard-gate research of
2026-08-07 found:

- LadybugDB has official Rust bindings: the crate `lbug` on crates.io.
  The crate uses cxx FFI into a bundled C++ core, built with CMake. The
  publisher is the LadybugDB lead. The repository is
  https://github.com/LadybugDB/ladybug-rust. First release: 2025-11-01.
  Latest release at check time: 0.19.1. The crate is actively
  maintained. On supported targets the build downloads a precompiled
  static `liblbug` archive and does not compile the C++ source.
- The crates `ladybug` and `ladybugdb` are unrelated or nonexistent.
- The project specification pins Python `ladybug>=0.16.0,<=0.18.2`
  (Section 10). From version 0.18.0 the on-disk storage format is v42
  with magic bytes `LBUG`.
- docs.rs fails to build `lbug >= 0.17`. This is a docs.rs sandbox
  limitation, not a crate defect. Read the API from the local crate
  source or build `cargo doc -p lbug --open`.

## Decision

Use the official `lbug` crate. Pin `lbug = "0.18"` in the workspace
dependencies. The pin keeps storage-format v42 (`LBUG`) compatibility
with the pinned Python `ladybug>=0.16.0,<=0.18.2`. Do not use 0.19.x.

Connection management: `lbug::Database` and `lbug::Connection` are both
`Send + Sync`. `Connection` borrows its `Database`, so the backend
caches `Arc<Database>` per group and opens a fresh `Connection` inside
each `tokio::task::spawn_blocking` closure. Connection creation is
cheap.

## Rejected alternatives

- Stub backend plus deferral. Rejected: the official binding exists and
  the hard gate passed. A stub would defer real MERGE semantics to a
  later phase.
- bindgen against the C API (https://docs.ladybugdb.com/client-apis/c).
  Rejected for now: more unsafe code and more maintenance than the cxx
  crate. This path remains the fallback if the cxx build ever becomes a
  problem.
- The frozen `kuzu` crate. Rejected: LadybugDB is a fork of KuzuDB and
  the storage formats diverge from LadybugDB 0.18. The `kuzu` crate does
  not track LadybugDB releases.

## Consequences

- CMake and a C++ toolchain are required to build the workspace on
  targets without a precompiled `liblbug` archive. The first source
  build takes a long time (the bundled C++ core has about 450 000
  lines). Later builds are incremental.
- API documentation comes from the local crate source, not docs.rs.
- Driver errors map to `MemoryError::Backend`. Synchronous driver calls
  stay inside `tokio::task::spawn_blocking` (AGENT.md Section 6.2).
