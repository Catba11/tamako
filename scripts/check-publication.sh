#!/usr/bin/env bash
# check-publication.sh — the publication gate (AGENT.md Section 6.9, decision 122).
# Nothing private crosses into main: no live Telegram group id, no bot token,
# no API-key material, no private key block. Sanctioned placeholder group ids:
# -1001234567890 and -1009876543210.
#
# Usage:
#   scripts/check-publication.sh --staged          scan staged added lines (pre-commit)
#   scripts/check-publication.sh --history [ref]   scan full history of ref (pre-push; default: main)
#   scripts/check-publication.sh --tree [ref]      scan every blob of ref (default: HEAD)
#
# Exit 0 = clean, 1 = findings (each finding printed).
set -euo pipefail

mode="${1:---staged}"
ref="${2:-}"
fail=0

# The four pattern classes, defined once so the candidate selection (git log -G)
# and the extraction (git grep) below cannot drift. Each pattern is written so
# this script's own text does not match it.
IDPAT='-100[0-9]{10}'
TOKPAT='[0-9]{9,10}:[A-Za-z0-9_-]{35}'
KEYPAT='sk-[A-Za-z0-9_-]{20,}'
PKPAT='PRIVATE KEY-----'
GATE_PAT="$IDPAT|$TOKPAT|$KEYPAT|$PKPAT"

scan_stream() { # $1 = label; content on stdin
  local label="$1" text hits
  text="$(tr -d '\0')"
  hits="$(printf '%s' "$text" | grep -oE -- "$IDPAT" | grep -vxF -e '-1001234567890' -e '-1009876543210' | sort -u || true)"
  if [ -n "$hits" ]; then echo "GATE HIT ($label): live Telegram group id: $hits"; fail=1; fi
  if printf '%s' "$text" | grep -qE "$TOKPAT"; then
    echo "GATE HIT ($label): Telegram bot token shape"; fail=1
  fi
  hits="$(printf '%s' "$text" | grep -oE "$KEYPAT" | sort -u || true)"
  if [ -n "$hits" ]; then echo "GATE HIT ($label): API key shape: $hits"; fail=1; fi
  if printf '%s' "$text" | grep -qE -- '-----BEGIN [A-Z]+ PRIVATE KEY-----'; then
    echo "GATE HIT ($label): private key block"; fail=1
  fi
}

case "$mode" in
  --staged)
    scan_stream "staged" < <(git diff --cached --unified=0 -- . | grep -E '^\+' || true)
    ;;
  --history)
    ref="${ref:-main}"
    commits="$(git log --format=%H -G"$GATE_PAT" "$ref" -- || true)"
    if [ -n "$commits" ]; then
      # shellcheck disable=SC2086
      scan_stream "history of $ref" < <(git grep -h -oE -- "$GATE_PAT" $commits -- 2>/dev/null || true)
    fi
    ;;
  --tree)
    ref="${ref:-HEAD}"
    scan_stream "tree of $ref" < <(git ls-tree -r --name-only "$ref" | while IFS= read -r f; do git show "$ref:$f" 2>/dev/null || true; done)
    ;;
  *)
    echo "usage: $0 [--staged|--history [ref]|--tree [ref]]" >&2
    exit 2
    ;;
esac

if [ "$fail" = "1" ]; then exit 1; fi
echo "publication gate: clean ($mode ${ref:-})"
