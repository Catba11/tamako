# syntax=docker/dockerfile:1
# Tamako all-in-one image (migration-runbook.md Section 5): cargo-chef
# three-stage, tagged localhost/tamako:<git-sha> — the image embeds the
# compiled prompts, inherits branch confidentiality, and never leaves
# operator-controlled hardware (Section 1 item 4).

FROM rust:1-slim-bookworm AS chef
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
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl bash ca-certificates cmake g++ pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
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
RUN cargo build --release --locked --bin tamako > /tmp/build.log 2>&1 \
    || { cat /tmp/build.log; exit 1; }
# (1) ANY downloader fallback cargo:warning= fails the build.
RUN if grep -E "download failed.*building from source|Could not run prebuilt liblbug downloader" /tmp/build.log; then \
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

FROM debian:bookworm-slim AS runtime
# The lbug core links statically by default but still dylib-links
# ssl/crypto and, on Linux, stdc++.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/tamako /usr/local/bin/tamako
ENTRYPOINT ["tamako"]
