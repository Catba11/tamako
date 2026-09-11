# Migration runbook: macOS operator machine → Fedora Kinoite, containerized

Status: DECIDED (2026-09-11), pre-spike. The six operator decisions are
recorded in Section 1; spike outcomes (Section 3) are filled in when the
spike lands. This runbook covers moving the live bot, its data root, its
secrets, and the branch-authority workflow to a Fedora Kinoite desktop,
containerized for CI/CD. It supersedes the macOS-specific parts of
docs/soak-runbook.md Section 4/5 for the target machine (Section 8).

## 1. Decisions (2026-09-11, operator)

1. Target architecture: **x86_64**.
2. Branch authority: **the desktop**. `catball-self-use` commits happen
   ONLY on the desktop after switchover; the Mac copy is a dormant
   read-only backup and MUST NOT commit on the branch. There is no
   shared remote for the branch — two writable clones would diverge
   silently. Never push the branch to any remote to "enable" anything.
3. Branch-side CI: **a local script** (`ci-local.sh`), not a self-hosted
   runner. The branch never leaves operator-controlled hardware.
4. Image registry: **`localhost/`** (podman local storage). The image
   embeds the compiled prompts; it inherits branch confidentiality and
   never goes to a public registry.
5. Container logs: **journald** (quadlet default; `journalctl --user -u
   tamako`), with journald's own rotation.
6. Reboot cadence: **frequent** (rpm-ostree auto-updates reboot the
   desktop). `loginctl enable-linger` + `Restart=always` are HARD
   requirements, not conveniences.

## 2. Sequencing gate

Do not start before the 2026-09-12 review closes the endpoint-switch
window (decision 111) and the embedding-concurrency calibration
(decision 113). The observation window keeps pure semantics only while
the pre-113 binary keeps serving.

Milestones: 0 → 1 → 2 → 3 → 4. The CD pipeline (Section 7) does not
enable automated deploys until milestone 0 lands.

## 3. Milestone 0: shutdown drain (prerequisite code, on main)

Today `ActorCommand::Shutdown` breaks the actor loop without awaiting
the detached digest/summary/wake/warmup tasks (current-state.md Section
4 item 11); main returns and the runtime aborts any in-flight digest
mid-batch. Rare deliberate restarts made that tolerable; push-to-deploy
makes restarts frequent and automated, so the gap ships with the
pipeline or closes first. It closes first.

- **Drain** (follow-up a): the actor tracks the detached task handles;
  on `Shutdown` it KEEPS SELECTING on the inbox until every tracked task
  has reported, then exits. Do NOT break-then-await: the completion
  path reports through the actor's own bounded inbox, and awaiting with
  the receive loop stopped deadlocks against a full inbox (the
  completion handler also performs the flag resets / boundary advances /
  marker rollbacks, which must run). Main's shutdown side must
  BROADCAST-then-join: send `Shutdown` to every actor first, then await
  the joins. Today's serial `shutdown().await` loop (send + join per
  actor, in turn) leaves groups 2..8 fully alive while group 1 drains —
  their tickers keep DISPATCHING new digests/wakes, the drain
  observation set never converges, the total approaches the SUM of
  per-group drains, and a mid-way `StopTimeout` SIGKILL defeats the
  drain and lands back in the torn-file window. Broadcast-first makes
  the total ≈ the MAX.
- **Digest-start line** (follow-up b): one line at dispatch with batch
  id and range, at **DEBUG** — README.md Section 6 ("Watching the pet")
  guarantees exactly one INFO line per wake and one per completed
  digest, and the guarantee stands (the pipeline's extraction detail
  line already lives at debug for the same reason). Observability only;
  explicitly NOT a "wait-for-idle deploy gate" (check-then-act race — a
  digest can start between the check and the signal), and once the
  drain lands every stop is unconditionally safe anyway.
- **Resolved trigger-config startup line** (new): one INFO line at
  startup printing the resolved `suffix_mode` and `timezone`. No
  existing line shows them — the wake wiring banner prints only the
  gate/reply models and the recall cap, and the persona line prints
  name/pet_tag/preamble_len/suffix_rules — while a missing
  `TAMAKO_SUFFIX_MODE`/`TAMAKO_TIMEZONE` silently keeps the TOML value
  (missing and empty-after-trim both count as UNSET). Without this
  line the Section 6/8 reconciliation has no target for the
  silent-fallback class.
- AGENT.md Section 6.5 surface, docs-first: one numbered decision entry
  per behavior change in current-state.md Section 3, the owning specs.md
  sections, ARCHITECTURE.md ("ctrl-c shuts every actor down gracefully"
  paragraph), README.md Section 5's ctrl-c line, soak-runbook.md, and
  the Section 4 item-11 update.

## 4. Milestone 1: spike (go/no-go)

On the desktop, in a toolbox container with rustup + cmake + g++ +
**curl + bash + ca-certificates** (the lbug prebuilt download script's
hard dependencies; without them the build silently falls back to the
crate's bundled 0.18.3 core — a storage-format drift):

1. Transfer the source (git bundle from the Mac — the branch rides
   along), `cargo build --release --locked`, full workspace test.
2. Generate the quadlet unit (`podman-system-generator --user --dryrun`)
   and inspect it; then EMPIRICALLY verify which signal the
   in-container process receives on `systemctl --user stop`
   (KillMode=mixed sends the [Service] KillSignal — default SIGTERM —
   only to the podman process; whether podman converts that into the
   container's StopSignal=SIGINT is asserted, not yet observed). If the
   app sees SIGTERM, the entire graceful path never triggers and
   milestone 0's drain is dead code — stop and fix the unit before
   proceeding.
3. Confirm the lbug **0.19.1** prebuilt exists for x86_64-linux and is
   what the build linked (Section 5's assertion).
4. Probe the podman `--env-file` parser with
   `podman run --rm --env-file <probe> localhost/tamako:<sha> env`:
   malformed lines, quotes, `#` comments, an `export ` prefix. The
   observed behavior dictates how the hand-merged env file (Section 6)
   is written — the `[Container] EnvironmentFile=` line maps to
   `--env-file`, NOT systemd's EnvironmentFile parser.
5. Record: build time, test total vs the stamped 1158 (9 ignored),
   prebuilt availability, the exact assertion wiring, the signal-test
   and env-file-probe outcomes.

Spike failure pauses the project; do not improvise around the graph
core.

## 5. Milestone 2: image build

- `Containerfile`: cargo-chef three-stage (planner / builder / runtime),
  `cargo build --release --locked --bin tamako`; runtime =
  `debian:bookworm-slim` + ca-certificates + libssl3;
  `ENTRYPOINT ["tamako"]`; builder carries curl + bash + ca-certificates
- `.dockerignore` (NEW, required): `.git/`, `target/`, `data/`, `.env*`,
  `tamako.toml`, `*.log`, `.omp/`, `*decision*.md`, `review-*.md`,
  `eval-*.md` — and the exception `!.cargo/config.toml`. `.git/` goes
  first: the object store holds the confidential branch commits, no
  workspace crate has a build.rs or git-metadata consumer (the image
  tag carries the SHA instead), and excluding it keeps the daemon
  context tarball lean. The decision-109 pin (`LBUG_VERSION = "0.19.1"`)
  lives in `.cargo/config.toml`; dropping it unpins the C++ core (the
  download script falls back to `main`).
- **Fail-closed pin assertion in CI**: the build log must show the
  prebuilt source resolving to the pinned 0.19.1 release; any
  download-fallback warning fails the build. Never trust the warning to
  be noticed. (Exact wiring is a spike output.)
- Tag images `localhost/tamako:<git-sha>`; rollback = retag the quadlet
  + restart. Keep the previous N tags.

- `.env.example` (NEW, tracked, placeholders only): the checked-in
  source of truth for the required env key set — the Section 7
  assertion and the Section 6 hand-merge both diff against it.

## 6. Milestone 3: runtime (rootless quadlet)

`/var/lib/tamako/{data,config,env}` owned by the invoking user (rootless
podman maps container root to that user; or mount `:U`). The env file is
mode 0600 and is HAND-MERGED from `.env` plus the three operator-shell
variables `TAMAKO_GATE_LLM_API_KEY`, `TAMAKO_REPLY_LLM_API_KEY`,
`TAMAKO_SUMMARY_MODEL`. `[Container] EnvironmentFile=` maps to podman's
`--env-file` — NOT systemd's EnvironmentFile parser (that parser only
applies under `[Service]`, where it would inject the podman client
process, not the container); its exact rules (quotes, comments,
malformed lines, `export` prefixes) are verified empirically in spike
item 4 BEFORE the file is written. The `TAMAKO_*` overrides fail SILENT
when absent (missing/empty-after-trim both count as UNSET; only missing
API keys are loud), so the required key set has a checked-in source of
truth: `.env.example` (Section 5) plus the three shell variables.
Reconcile after every start: the wiring banner covers the models, and
the milestone-0 startup line covers the resolved
`suffix_mode`/`timezone`.

`~/.config/containers/systemd/tamako.container`:

```ini
[Container]
Image=localhost/tamako:<git-sha>
EnvironmentFile=/var/lib/tamako/env
Volume=/var/lib/tamako/data:/data:Z
Volume=/var/lib/tamako/config:/config:Z,ro
Exec=--live --config /config/tamako.toml --data-root /data --verbose
StopSignal=SIGINT
StopTimeout=120

[Unit]
StartLimitIntervalSec=300
StartLimitBurst=5

[Service]
Restart=always
RestartSec=5
TimeoutStopSec=150

[Install]
WantedBy=default.target
```

Then: `loginctl enable-linger <user>`, `systemctl --user daemon-reload`,
`systemctl --user start tamako`.

- Stops/restarts are `systemctl --user ...` ONLY. A raw `kill -INT`
  under `Restart=always` is invalid (systemd immediately restarts it).
- `StopSignal=SIGINT` is load-bearing: the default SIGTERM is unhandled
  and hard-kills, skipping the graceful path.
- Two SEPARATE stop budgets: `StopTimeout=120` is quadlet's podman
  `--stop-timeout` (the container's own stop grace), while systemd's
  `[Service] TimeoutStopSec` (default 90s) is the cgroup kill budget —
  quadlet does NOT derive one from the other, so without the explicit
  `TimeoutStopSec=150` systemd would SIGKILL the cgroup at 90s and
  silently truncate the drain budget. TimeoutStopSec must exceed
  StopTimeout. Neither extends process life beyond the drain (that is
  the drain's job, not the timeouts').

## 7. CI/CD

- Public lane (GitHub Actions, main only): fmt + clippy + the full
  workspace suite + image build. Main carries nothing confidential.
- Branch lane (`ci-local.sh` on the desktop): fmt, clippy, tests,
  `podman build`, smoke, retag, `systemctl --user restart`, banner
  assertion. The smoke stage is the Section 8 readback against a fixture
  data root plus an env-key-set assertion against `.env.example` (+ the
  three shell variables);
  CI NEVER runs `--live` (double-poll hazard).

## 8. Milestone 4: switchover

1. `git branch --show-current` on the Mac (must print
   `catball-self-use`); `cargo build --release` only if a host-side
   verification binary is wanted — the container pipeline replaces host
   binaries.
2. Stop: `kill -INT <pid>`; wait for exit (`while kill -0 <pid>
   2>/dev/null; do sleep 1; done`).
3. Worktree: `rsync -a --exclude target/ --exclude data/
   ~/Documents/development/Tamako/ desktop:~/Tamako/` — the whole tree
   INCLUDING `.git` (the branch exists only locally) and the untracked
   live config; `data/` moves separately, AFTER the clean stop (a live
   copy carries in-flight WAL files; a stale second data root beside the
   service root is a silent-wrong-data trap for manual probes).
4. Config dir: `mkdir -p /var/lib/tamako/config && cp
   ~/Tamako/tamako.toml /var/lib/tamako/config/` (the untracked live
   config rides the step-3 worktree copy), owned by the service user.
   `persona.toml` needs NO copy here: it loads from
   `<data-root>/persona.toml` and arrives with step 5.
5. Data root: `rsync -a data/ desktop:/var/lib/tamako/data/`.
6. Env file: merge `.env` + the three shell vars into
   `/var/lib/tamako/env` (0600, service user).
7. **Readback gate** (all 8 groups, via the freshly built image — this
   validates the exact lbug core that will serve, not some host binary):

```bash
IMG=localhost/tamako:<sha>
rsync -a /var/lib/tamako/data/ /tmp/tamako-readback/data/
failed=0
run_probe() { podman run --rm \
    -v /tmp/tamako-readback/data:/data:U,z \
    -v /var/lib/tamako/config:/config:Z,ro \
    "$IMG" "$@" --config /config/tamako.toml --data-root /data 2>&1; }

run_probe --status-all || failed=1
for d in /tmp/tamako-readback/data/*/; do
    [ -f "$d/store.db" ] || continue   # Rule P5 predicate: skips Frameworks/, bugscope/
    cid=$(basename "$d")
    out=$(run_probe --facts "$cid" "__readback_probe__") || {
        grep -q "no node named" <<<"$out" || { echo "READBACK FAIL: $cid"; failed=1; }
    }
done
[ "$failed" = 0 ] || { echo "scratch kept at /tmp/tamako-readback"; exit 1; }
rm -rf /tmp/tamako-readback
```

   `--facts` is the offline read-only graph open (decision 75/77: no
   LLM, no lock, no writes); the never-present probe name makes "no node
   named" the PASS signature and any graph-open error the FAIL
   signature. `--merge-tool` is NOT a substitute (it resolves endpoints,
   spends LLM tokens, and writes `merge_plan.json`). `:U,z` chowns the
   scratch copy into the container namespace (a WAL-mode store needs a
   writable directory even for reads). Failure keeps the scratch and
   blocks the start.
8. Start: `systemctl --user start tamako`. Verify: wiring banner vs
   expectations (env silent-fallback check), first gate usage line, one
   real Telegram reply.
9. Rollback: the Mac stays intact (binary + data) for one week. NEVER
   run both ends at once — concurrent getUpdates pollers steal each
   other's updates and error on conflicts.

## 9. Post-switchover workflow

- Observe logs (`journalctl --user -u tamako`) → tune → commit on the
  branch ON THE DESKTOP → `ci-local.sh` → restart. No iteration hop.
- main pushes to origin may happen from either machine (origin is
  main's shared authority); the branch never pushes anywhere.
- Divergence sentinel: monthly `git bundle` snapshots exchanged between
  the two machines; on suspicion, `git bundle verify` + `rev-list`
  reconciliation.
- The desktop worktree NEVER holds a `data/` directory; every offline
  probe passes `--data-root` explicitly.

## 10. Doc-sync deliverables (switchover, docs-first per Section 6.5)
- README.md: the Section 5 ctrl-c line (shutdown semantics change with
  the drain and with systemctl replacing ctrl-c on the target) and a
  Section 6 re-check that no new INFO-level line leaks into the
  one-line guarantee (the digest-start line is DEBUG).
- docs/soak-runbook.md: Section 4 stop/restart procedure (ctrl-c →
  `systemctl --user stop`), Section 5 backup step 1, the "same launch
  command" line, and the log-location guidance (journald).
- ARCHITECTURE.md: the graceful-shutdown paragraph (milestone 0).
- current-state.md: Section 3 decision entries for the drain and the
  digest-start line; Section 4 item 11 marked DONE with the drain's
  decision reference.
