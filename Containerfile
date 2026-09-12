# syntax=docker/dockerfile:1
# Tamako all-in-one image (migration-runbook.md Section 5): cargo-chef
# three-stage, tagged localhost/tamako:<git-sha> — the image embeds the
# compiled prompts, inherits branch confidentiality, and never leaves
# operator-controlled hardware (Section 1 item 4).

FROM rust:1-slim-trixie AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# lbug's build script downloads the prebuilt C++ core (curl + bash +
# ca-certificates — missing these silently falls back to the crate's
# bundled 0.18.3 core, a storage-format drift), can source-build
# (cmake + g++), and pkg-configs openssl to dylib-link ssl/crypto.
# The builder is TRIXIE, not bookworm: the 0.19.1 prebuilt's public
# header (lbug.hpp) includes C++20 <format>, which bookworm's g++ 12
# lacks — trixie's g++ 14 compiles it (spike 1(b) finding). Builder
# and runtime stay on the SAME Debian release (the linked binary never
# trips a builder-newer-than-runtime glibc/libssl mismatch).
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl bash ca-certificates cmake g++ pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
# The decision-109 pin must govern the COOK layer too (not just the
# final build): the downloader resolves `releases/latest` without it.
COPY .cargo/config.toml .cargo/config.toml
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
# Decision 109 fail-closed wiring (migration-runbook.md Section 5). The
# LBUG_VERSION=0.19.1 pin arrives via .cargo/config.toml's env table,
# which cargo never echoes to the build log — so the pin is asserted by
# its EFFECTS, never by a log grep for the pin itself:
# (0) WIPE the prebuilt cache AFTER the cook layer: lbug's build.rs
#     reuses an existing cache dir silently (no download, no warning)
#     and the cache key comes only from LBUG_VERSION, so a cache-hit
#     cook snapshot could otherwise smuggle a stale key past both
#     guards below.
RUN rm -rf "$CARGO_HOME"/registry/src/*/lbug-*/.cache/lbug-prebuilt
# cargo caches the build SCRIPT's run across cook → build (inputs
# unchanged), so a wiped cache would NOT be re-downloaded below and
# guard (2) would fail on a missing dir instead of a stale key. Force
# the rerun by deleting lbug's build-script output + fingerprint dirs
# (cargo clean -p lbug removes 0 files in this layout — observed in
# spike 1(b) — so the wipe stays load-bearing via the direct rm).
RUN rm -rf target/release/build/lbug-* target/release/.fingerprint/lbug-*
RUN cargo build --release --locked --bin tamako > /tmp/build.log 2>&1 \
    || { cat /tmp/build.log; exit 1; }
# (1) ANY downloader fallback fails the build — including the SOURCE
#     build. Its "Downloading ladybug source …" marker is a plain
#     println, and cargo hides build-script stdout in normal mode (it
#     lands in target/release/build/lbug-*/output), so the grep covers
#     BOTH the log (cargo:warning= lines) and that file.
RUN if grep -E "download failed.*building from source|Could not run prebuilt liblbug downloader|Downloading ladybug source" \
        /tmp/build.log target/release/build/lbug-*/output; then \
        echo "FAIL-CLOSED: the lbug prebuilt downloader fell back; the linked core is unpinned"; \
        exit 1; \
    fi
# (2) The cache holds EXACTLY ONE key, version-0.19.1, with the static
#     core at its path — a `latest/` entry marks the unpinned
#     pre-decision-109 resolution and fails. The registry index hash is
#     machine-specific, so glob it — but fail closed on zero or many.
RUN set -eu; \
    roots=$(ls -d "$CARGO_HOME"/registry/src/*/lbug-0.18.3/.cache/lbug-prebuilt 2>/dev/null || true); \
    [ -n "$roots" ] || { echo "FAIL-CLOSED: the lbug prebuilt cache is missing"; exit 1; }; \
    n=0; \
    for root in $roots; do \
        keys=$(ls -1A "$root"); \
        [ "$keys" = "version-0.19.1" ] \
            || { echo "FAIL-CLOSED: unexpected prebuilt keys in $root: $keys"; exit 1; }; \
        [ -f "$root/version-0.19.1/lib/liblbug.a" ] \
            || { echo "FAIL-CLOSED: liblbug.a missing under $root/version-0.19.1"; exit 1; }; \
        n=$((n + 1)); \
    done; \
    [ "$n" -eq 1 ] || { echo "FAIL-CLOSED: expected exactly one lbug crate dir, found $n"; exit 1; }

FROM debian:trixie-slim AS runtime
# The lbug core links statically by default but still dylib-links
# ssl/crypto and, on Linux, stdc++. Trixie names the OpenSSL 3
# runtime libssl3t64 (the 64-bit-time_t transition).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3t64 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/tamako /usr/local/bin/tamako
ENTRYPOINT ["tamako"]
