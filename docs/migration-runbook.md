# Migration runbook: macOS operator machine → Fedora Kinoite, containerized

Status: DECIDED (2026-09-11), pre-spike. The six operator decisions are
recorded in Section 1; spike outcomes (Section 4) are filled in when the
spike lands. This runbook covers moving the live bot, its data root, its
secrets, and the branch-authority workflow to a Fedora Kinoite desktop,
containerized for CI/CD. It supersedes the macOS-specific parts of
docs/soak-runbook.md Section 4/5 for the target machine (doc-sync list
in Section 10).

## 1. Decisions (2026-09-11, operator)

1. Target architecture: **x86_64**.
2. Branch authority: **the desktop** — with a hard handover rule,
   because the branch has no shared remote and two writable clones
   diverge silently. UNTIL switchover the Mac remains the sole writer
   (the live binary builds from it; tuning continues there); the
   desktop worktree from milestone 1 on is READ-ONLY — spike fixes land
   on the Mac and re-transfer. AT switchover (Section 8 step 3's SHA
   check passes) authority flips: `catball-self-use` commits happen
   ONLY on the desktop; the Mac copy is a dormant read-only backup and
   MUST NOT commit on the branch. Never push the branch to any remote
   to "enable" anything.
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
  marker rollbacks, which must run). The drain loop itself must not
  re-arm the dispatch paths either: the actor's main loop is a
  `tokio::select!` over `inbox.recv()` AND `ticker.tick()`, and a Tick
  drives the same warmup/wake/digest dispatch. On entering drain, a
  single `shutting_down` flag gates EVERY dispatch point — the tick path
  AND the completion chains (SummaryCompleted unconditionally re-runs
  digest evaluation; digest completion spawns the summarizer) — so the
  drain only finishes in-flight work and never dispatches new. Precisely:
  the gate suppresses ONLY the re-dispatch calls INSIDE the completion
  handlers (maybe_launch_digest / start_wake / the summarizer spawn);
  the handlers themselves RUN during drain — they carry the mandatory
  state restoration (flag resets, boundary advances, marker rollbacks,
  the Cancelled restoration) and they are the only signal the drain
  waits for. A gate that swallowed completion COMMANDS would deadlock
  the drain into the budget SIGKILL — the exact window this milestone
  exists to close. Gating
  only the ticker arm lets the completion chains spawn NEW tracked work,
  the tracked set never shrinks monotonically, and the budget SIGKILLs a
  non-converging drain. Third non-convergence source, independent of the
  cross-group serialization below. The
  drain waits for task
  COMPLETION within the stop budget (Section 6) — it never cancels a
  task mid-call: dropping the digest future mid-request re-enters the
  UNVERIFIED mid-digest window the drain exists to close. For long
  tasks the drain pairs completion-waiting with a CALL-boundary cancel:
  every tracked task polls a cancellation token between its LLM calls —
  the digest between retry attempts AND between an attempt's initial
  call and its repair call (the two get separate 900s windows), the
  UNFORCED wake between its serial recall/gate/reply calls, the summary
  before its call. FORCED wakes are excluded from the cancel set:
  must-respond IS the forced path and forced bypasses the gate, so a
  cancelled forced wake would re-present the message only to ordinary
  gate judgment — the obligation degraded, not preserved. A forced wake
  in flight at drain entry runs to completion within the budget — or,
  past it, is SIGKILLed into the residual below; the budget is NOT
  sized to the 5400s worst case (forced wakes are rare and
  human-triggered, and sizing for them would tax every rpm-ostree
  reboot). State-safety is by design: nothing lands between calls (the
  wake's dedup/context/outbound rows live in the completion handler; a
  failed digest attempt commits nothing and the boundary only advances
  on commit; a failed repair returns the original error). The cancel is
  NOT the failure path: the task reports a Cancelled outcome and the
  actor's drain handler performs ONLY state restoration — for a digest
  the batch stays PENDING (no attempt consumed, no failure counter,
  never the dead-letter branch — its terminal state has no shipped
  repair tool, specs.md Section 15 item 2); for an UNFORCED wake the handler
  rolls `wake_last_row_id` back to `pre_wake_row_id` and persists —
  REQUIRED, because `start_wake` advances and persists the marker at
  wake START, so a cancel without the rollback silently drops the
  messages. The rollback preserves the MESSAGES (they re-present at the
  next natural trigger's gate judgment); it does NOT preserve any
  forcing — forced entries are memory-only, armed at intake or by the
  decision-65 requeue, and the cancel path arms neither, so any
  Section-8.1 intent would degrade to best-effort gate judgment. That
  case is narrow by construction (a mention arms a FORCED wake at
  intake, and forced wakes are never cancelled), but if milestone 0
  wants zero degradation it must re-arm forcing from the persisted
  raw-log row (mentions_bot/is_reply_to_bot survive); otherwise the
  degradation is documented, not hidden; the decision-65 requeue is NOT
  armed either (the dispatch gate is closed). The one residual
  no cooperative cancel can cover: a HARD kill (budget SIGKILL, crash)
  landing mid-wake before any cancel point leaves the marker advanced
  and the obligation lost — wakes have no replay surface (the
  idempotent-replay and reconciliation nets cover digest batches only);
  this matches every pre-drain stop and stays documented. With the cancel the drain's tail is ONE call window (≤ 900s) plus
  wrap-up FOR CANCELLABLE TASKS — which is what makes the Section 6
  budget honest; without the cancel the worst cases are not waitable
  (5 digest attempts × 900s + backoff ≈ 4530s; a slow wake's serial
  calls at up to 5400s). The uncancellable FORCED wake stands outside
  this arithmetic — its overrun is bounded only by the SIGKILL residual
  above.
  And the shutdown ORDER must keep the outbound pump alive: today
  ctrl-c breaks the intake loop — which hosts the outbound select arm —
  BEFORE the actors are shut down, so a wake completing during the
  drain pushes its reply into a buffered channel nobody serves (the log
  says the bot spoke; Telegram never receives — and the more successful
  the drain, the more such losses). Milestone 0 keeps the pump running
  until every actor has joined AND the outbound channel is empty (the
  replay path already has the flush pattern: `while let Ok(action) =
  outbound_rx.try_recv()`). Main's
  shutdown side must BROADCAST-then-join: send `Shutdown` to every
  actor first, then await the joins. Today's serial `shutdown().await`
  loop (send + join per actor, in turn) leaves groups 2..8 fully alive
  while group 1 drains — their tickers keep DISPATCHING new
  digests/wakes, the drain observation set never converges, the total
  approaches the SUM of per-group drains, and a mid-way `StopTimeout`
  SIGKILL defeats the drain and lands back in the torn-file window.
  Broadcast-first makes the total ≈ the MAX.
- **Digest-start line** (follow-up b): one line at dispatch with batch
  id and range, at **DEBUG** — README.md Section 6 ("Watching the pet")
  guarantees exactly one INFO line per wake and one per completed
  digest, and the guarantee stands (the pipeline's extraction detail
  line already lives at debug for the same reason). Observability only;
  explicitly NOT a "wait-for-idle deploy gate" (check-then-act race — a
  digest can start between the check and the signal), and once the
  drain lands every stop is unconditionally safe anyway.
- **Resolved trigger-config startup line** (new): one INFO line at
  startup printing the resolved `suffix_mode`, `timezone`, AND the
  digest/summary models. No existing line shows any of them — the wake
  wiring banner prints only the gate/reply models and the recall cap,
  and the persona line prints name/pet_tag/preamble_len/suffix_rules —
  while a missing `TAMAKO_SUFFIX_MODE`/`TAMAKO_TIMEZONE`/
  `TAMAKO_DIGEST_MODEL`/`TAMAKO_SUMMARY_MODEL` silently keeps the TOML
  value (missing and empty-after-trim both count as UNSET). Without
  this line the Section 6/8 reconciliation has no target for the
  silent-fallback class.
- AGENT.md Section 6.5 surface, docs-first: one numbered decision entry
  per behavior change in current-state.md Section 3, the owning specs.md
  sections, ARCHITECTURE.md ("ctrl-c shuts every actor down gracefully"
  paragraph), README.md Section 5's ctrl-c line, soak-runbook.md, and
  the Section 4 item-11 update.

## 4. Milestone 1: spike (go/no-go)

On the desktop, in a Fedora toolbox container with rustup + `gcc-c++` +
`cmake` + `pkgconf-pkg-config` + `openssl-devel` + **curl + bash +
ca-certificates** (Fedora package names — the Kinoite toolbox is
Fedora; the openssl probe plus the lbug prebuilt download script's hard
dependencies; without the download trio the build silently falls back
to the crate's bundled 0.18.3 core — a storage-format drift). A
missing-package link failure here is a nuisance to fix, NOT an "lbug
won't build on Linux" verdict — recheck this list before pausing the
project:

1. Transfer the source (git bundle from the Mac — the branch rides
   along) and extract it into `~/Tamako` — the ONE canonical desktop
   source LOCATION, build-only and read-only until switchover (Section
   1 item 2); no second clone of the branch may exist on the machine,
   and any spike scratch outside it is removed when the spike passes. Build and test TWICE, once per
   toolchain: (a) toolbox
   `cargo build --release --locked` + full workspace test; (b) a real
   `podman build` of the Section-5 Containerfile (at least the builder
   stage) — the Containerfile and `.dockerignore` are authored on the
   MAC and committed to the branch BEFORE transfer (the desktop is
   read-only from milestone 1; if the spike already ran, re-cut the
   bundle after they land). The spike's Fedora toolbox is NOT the
   image's Debian
   builder, and the prebuilt static core's link compatibility is
   toolchain-sensitive, so a green (a) without (b) proves nothing about
   milestone 2.
2. Generate the quadlet unit (`podman-system-generator --user --dryrun`)
   and inspect it; then EMPIRICALLY verify which signal the
   in-container process receives on `systemctl --user stop` — with a
   NON-POLLING PROBE, never the real Exec, and THROUGH THE GENERATED
   UNIT, not a bare `podman run` — the question under test is the
   systemd → podman → container chain (KillMode=mixed sends KillSignal
   to the podman process only), which `podman stop` never exercises:
   override the Exec with a drop-in
   (`~/.config/containers/systemd/tamako.container.d/99-probe.conf`
   carrying `Exec=sh -c 'trap "echo GOT-SIGINT; exit 0" INT; trap "echo
   GOT-SIGTERM; exit 0" TERM; while :; do sleep 1; done'`, same
   `Image=`), `systemctl --user daemon-reload && systemctl --user start
   tamako`, then `systemctl --user stop tamako`, and read the captured
   signal name from journald. Two failure readings to distinguish:
   GOT-SIGTERM (StopSignal not converted) or NO line with the stop
   taking ~90s (TimeoutStopSec=1260 not in effect — systemd's default
   budget SIGKILLed first). Either way the graceful path never triggers
   and milestone 0's drain is dead code — stop and fix the unit before
   proceeding. The probe runs WITHOUT launching the bot (the Mac owns
   the Telegram token until Section 8 step 8; a live probe would
   double-poll). The probe drop-in OMITS `EnvironmentFile` (the probe
   needs no secrets and the env file does not exist until Section 8
   step 6 — podman errors on a missing `--env-file`; also create the
   `/var/lib/tamako/{data,config}` skeleton now, since bind sources
   auto-create but env files do not). The drop-in must not outlive the
   test: `rm ~/.config/containers/systemd/tamako.container.d/99-probe.conf`
   + `daemon-reload` immediately after (a leftover probe Exec would
   silently replace the real service at Section 8 step 8, which asserts
   `systemctl --user cat tamako` shows the real ExecStart first).
3. Confirm the lbug **0.19.1** prebuilt exists for x86_64-linux and is
   what the build linked (Section 5's assertion).
4. Probe the podman `--env-file` parser with
   `podman run --rm --entrypoint env --env-file <probe>
   localhost/tamako:<sha>` (the image ENTRYPOINT is `tamako`, so
   appending `env` would run `tamako env` and clap-error out — the
   probe must override the entrypoint):
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
  `cargo build --release --locked --bin tamako`;
  `ENTRYPOINT ["tamako"]`. Builder stage base `rust:1-slim` (Debian —
  hence the Debian package names): curl + bash + ca-certificates +
  cmake + g++ + pkg-config + libssl-dev (download-script deps, the
  cxx/source-build toolchain, and the openssl probe — lbug's build
  script pkg-configs openssl and dylib-links ssl/crypto, so the link
  fails without them). Runtime: `debian:bookworm-slim` +
  ca-certificates + libssl3 + libstdc++6 — the lbug core links
  statically by default but still dylib-links ssl/crypto and, on Linux,
  stdc++.
- `.dockerignore` (NEW, required): `.git/`, `target/`, `data/`, `.env*`,
  `tamako.toml`, `*.log`, `.omp/`, `*decision*.md`, `review-*.md`,
  `eval-*.md` — and the exception `!.cargo/config.toml`. `.git/` goes
  first: the object store holds the confidential branch commits, no
  workspace crate has a build.rs or git-metadata consumer (the image
  tag carries the SHA instead), and excluding it keeps the daemon
  context tarball lean. The decision-109 pin (`LBUG_VERSION = "0.19.1"`)
  lives in `.cargo/config.toml`; dropping it unpins the C++ core (the
  download script falls back to `main`).
- **Fail-closed pin assertion in CI** (two parts — the pin arrives via
  `cargo:rustc-env=`, which cargo does NOT echo to the build log, so a
  log grep for the pin would be perma-green): (1) ANY lbug downloader
  fallback `cargo:warning=` ("download failed … building from source" /
  "Could not run prebuilt liblbug downloader") fails the build; (2) the
  LBUG_VERSION-keyed prebuilt cache artifact must exist post-build at
  `$CARGO_HOME/registry/src/<index>/lbug-0.18.3/.cache/lbug-prebuilt/version-0.19.1/lib/liblbug.a`
  (glob the registry index hash — it is machine-specific). Because build.rs REUSES an existing cache dir
  silently (no download, no warning) and the cache key comes only from
  LBUG_VERSION, the Containerfile builder must (0) WIPE
  `.cache/lbug-prebuilt/` before the build — and the wipe must land
  AFTER any cargo-chef cook layer that caches the registry-src tree,
  or a cache-hit build reuses a layer snapshot still holding a stale
  key and both assertions pass over a drifted core; the spike verifies
  this by building twice (the second build cache-hot) and confirming
  the assertion still bites. The assertion must
  require that post-build the cache holds EXACTLY ONE key,
  `version-0.19.1` — a `latest/` entry marks an unpinned
  pre-decision-109 resolution and fails the build. Exact wiring is a
  spike output (item 5).
- Tag images `localhost/tamako:<git-sha>`; rollback = retag the quadlet
  + restart. Keep the previous N tags.
- `.env.example` (EXTEND, tracked, placeholders only): the file already
  exists as a secrets template (OPENAI_API_KEY plus commented
  ANTHROPIC_API_KEY/TELOXIDE_TOKEN) and documents a `.env.local`
  convention while the live run sources `.env` — add the three
  `TAMAKO_*` variables and reconcile the `.env`/`.env.local` wording,
  ON THE MAC, committed before the transfer (the desktop is read-only
  from milestone 1; the file is tracked, so it lands on main or the
  branch consistently). It is the checked-in source of truth for the
  required key set: the Section 7 assertion and the Section 6
  hand-merge both diff against it.

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
StopTimeout=1200

[Unit]
StartLimitIntervalSec=300
StartLimitBurst=5

[Service]
Restart=always
RestartSec=5
TimeoutStopSec=1260

[Install]
WantedBy=default.target
```

Then: `loginctl enable-linger <user>`, `systemctl --user daemon-reload`,
and STATIC verification ONLY (`podman-system-generator --user
--dryrun`, `systemd-analyze --user verify`). The first `start` is
Section 8 step 8, never here: the Mac bot keeps polling until
switchover step 2, and starting now would double-poll Telegram (both
ends steal each other's getUpdates), answer groups from an EMPTY data
root, and create empty store.db/WAL files that the step-5 rsync would
then interleave with the real ones.

- Stops/restarts are `systemctl --user ...` ONLY. A raw `kill -INT`
  under `Restart=always` is invalid (systemd immediately restarts it).
- `StopSignal=SIGINT` is load-bearing: the default SIGTERM is unhandled
  and hard-kills, skipping the graceful path.
- Two SEPARATE stop budgets, sized against the endpoint contract:
  every completion attempt is bounded by ENDPOINT_TIMEOUT = 900s
  (endpoint.rs; raised 300→900 by operator ruling 2026-08-23, guarding
  stalled — not slow — completions) and digests retry with backoff, so
  a digest already in flight at SIGINT can LEGITIMATELY run for many
  minutes. `StopTimeout=1200` is quadlet's podman `--stop-timeout` and
  covers the COMMON case — one in-flight call window completing
  (≤ 900s) plus slack — and stands ONLY together with milestone 0's
  CALL-boundary cancel (the retry-storm worst case, 5 attempts ×
  900s + backoff ≈ 4530s, or a slow wake's serial recall/gate/reply at
  up to 5400s, is not waitable; the cancel bounds the drain at one
  900s call window — an in-flight FORCED wake is the exception: never
  cancelled; when the budget cannot cover it (a slow one can reach
  5400s ≫ 1260s) it is SIGKILLed into the Section-3 hard-kill residual
  (marker advanced, obligation lost)); systemd's `[Service]
  TimeoutStopSec` (default 90s) is the cgroup kill budget — quadlet
  does NOT derive one from the other, so the explicit
  `TimeoutStopSec=1260` (> StopTimeout) keeps systemd from SIGKILLing
  the cgroup mid-drain. A kill past the budget lands in the
  replay-covered residual (idempotent batch replay + startup
  reconciliation; the lbug mid-call file state there stays UNVERIFIED).
  rpm-ostree AUTO-UPDATE reboots pay this cost directly: each one may
  wait out an in-flight drain, up to the 1260s cap (21 min). That is
  the price attached to the Section-1 frequent-reboot decision, weighed
  against the torn-window alternative — visible, not papered over.

## 7. CI/CD

- Public lane (GitHub Actions, main only): fmt + clippy + the full
  workspace suite + image build. Main carries nothing confidential.
- Branch lane (`ci-local.sh` on the desktop): the branch gate FIRST
  (`git -C ~/Tamako branch --show-current` = `catball-self-use`,
  Section 8 step 7's analogue), then fmt, clippy, tests,
  `podman build`, smoke, retag, `systemctl --user restart`, banner
  assertion. The smoke stage runs the Section 8 gate against a
  CHECKED-IN minimal fixture data root (one group: store.db +
  memory.lbug + media.db — a milestone-2 artifact authored on the Mac;
  replay cannot generate a graph offline, and without a graph-bearing
  fixture the inventory guard fails red by design), plus an env-key-set
  assertion against `.env.example` and the three shell variables. CI
  NEVER runs `--live` (double-poll hazard).

## 8. Milestone 4: switchover

1. `git branch --show-current` on the Mac (must print
   `catball-self-use`); `cargo build --release` only if a host-side
   verification binary is wanted — the container pipeline replaces host
   binaries.
2. Stop: `kill -INT <pid>`; wait for exit (`while kill -0 <pid>
   2>/dev/null; do sleep 1; done`).
3. Worktree: `rsync -a --delete --exclude target/ --exclude data/
   ~/Documents/development/Tamako/ desktop:~/Tamako/` — the whole tree
   INCLUDING `.git` (the branch exists only locally) and the untracked
   live config, MERGING INTO the spike worktree (the canonical path
   since milestone 1). Add `--delete` — the excluded paths survive by
   default, and without it files deleted on the Mac linger invisibly on
   the desktop (leftover `examples/`/`tests/` still compile under
   `--all-targets`). Gate in two parts: (1) PRE-transfer, run the dry run (`rsync -ain
   --delete --exclude target/ --exclude data/ <mac-tree>/
   desktop:~/Tamako/`) and read ONLY its `deleting …` lines — every
   desktop-side deletion must be intended (a file removed on the Mac);
   anything else must first be committed on the Mac (the desktop is
   read-only per Section 1 item 2, so there should be none). Copy lines
   are expected — the Mac tree has advanced since the spike. (2) The
   REAL rsync's exit code is the transfer gate (rsync reports transfer
   errors nonzero; a nonzero exit stops the switchover). Do NOT gate on
   a post-transfer dry run: against a just-copied tree it is trivially
   empty (proves no third-party change, not completeness) and
   mtime drift makes `-ain` list spurious entries. What discriminates a
   truncated transfer is the object store: run `git fsck --no-progress
   --no-dangling` (plus `git cat-file -e
   $(git rev-parse catball-self-use)^{commit}`) on the desktop — the
   only check that reads `.git` objects. The branch's rebase-heavy
   history guarantees dangling objects, so the pass criterion is the
   EXIT CODE, not empty output.
   `git rev-parse` only reads the ref file that rode the same payload
   (vacuously green) and `git status` is permanently dirty here
   (untracked never-commit files, the modified WATCHDOG.yml), so
   neither is a gate. `data/` moves separately, AFTER the clean stop (a live
   copy carries in-flight WAL files; a stale second data root beside the
   service root is a silent-wrong-data trap for manual probes).
4. Config dir: `mkdir -p /var/lib/tamako/config && cp
   ~/Tamako/tamako.toml /var/lib/tamako/config/` (the untracked live
   config rides the step-3 worktree copy), owned by the service user.
   `persona.toml` needs NO copy here: it loads from
   `<data-root>/persona.toml` and arrives with step 5.
5. Data root + graph inventory: on the Mac, AFTER the stop, capture the
   authoritative list of groups that hold a graph —
   `find data -mindepth 2 -maxdepth 2 -name memory.lbug | sed
   's|^data/||; s|/memory.lbug$||' | sort > /tmp/graph-groups.txt`
   (BSD find: no `-printf`) — then `rsync -a data/
   desktop:/var/lib/tamako/data/` and `scp /tmp/graph-groups.txt
   desktop:/var/lib/tamako/`. Without the inventory, a memory.lbug LOST
   in transfer is indistinguishable from a never-digested group and the
   step-7 gate would silently pass. Capture-side anti-vacuum: assert
   the file is non-empty AND holds at most one line per data-root group
   dir (8 today — the dirs holding store.db; tamako.toml configures 7
   and the example-id dir still carries a graph, so config count is the
   WRONG baseline) BEFORE the rsync — an empty capture (wrong CWD, a
   typo) must fail here, not pass green at the gate. Only graph-bearing
   dirs carry inventory lines: a served group that has never digested
   has no memory.lbug and no entry, so the check is an upper bound,
   never equality. The CI smoke derives its fixture inventory the same
   way and applies the same check.
6. Env file: merge `.env` + the three shell vars into
   `/var/lib/tamako/env` (0600, service user), written per the spike
   item-4 probe results.
7. **Build + readback gate** — FIRST the desktop branch gate:
   `git -C ~/Tamako branch --show-current` MUST print
   `catball-self-use` (the AGENT.md gate's desktop analogue: a
   main-built image would silently serve without the branch tunings —
   the 2026-09-10 incident class). Then build the image from the
   post-step-3
   tree and tag it from the same HEAD the gate will validate:
   `podman build -t localhost/tamako:$(git -C ~/Tamako rev-parse
   --short HEAD) ~/Tamako` (the milestone-2 image predates the rsync
   and its tag may name a rebase-rewritten SHA — never reuse it here).
   Then the gate (graph coverage = the step-5 inventory; SQLite
   coverage = every store.db AND the data-root `media.db` — the
   sticker-caption cache, whose loss is silent: `MediaStore::open`
   creates an EMPTY database when the file is missing and a failed open
   only WARNs, so it needs an explicit integrity check):

```bash
IMG=localhost/tamako:<sha>
rsync -a /var/lib/tamako/data/ /tmp/tamako-readback/data/
failed=0
INVENTORY=${INVENTORY:-/var/lib/tamako/graph-groups.txt}
[ -s "$INVENTORY" ] || { echo "READBACK FAIL: empty or missing inventory ($INVENTORY)"; exit 1; }
run_probe() { podman run --rm \
    -v /tmp/tamako-readback/data:/data:U,z \
    -v /var/lib/tamako/config:/config:Z,ro \
    "$IMG" "$@" --config /config/tamako.toml --data-root /data 2>&1; }

run_probe --status-all || failed=1
# media.db: existence FIRST — sqlite3 open-creates a missing path and
# would print "ok" for the empty database it just made. The CLI comes
# from the toolbox (the runtime image has no sqlite3).
if [ ! -f /tmp/tamako-readback/data/media.db ]; then
    echo "READBACK FAIL: media.db missing"; failed=1
else
    sqlite3 /tmp/tamako-readback/data/media.db 'PRAGMA integrity_check;' | grep -qx ok \
        || { echo "READBACK FAIL: media.db"; failed=1; }
fi
while read -r cid; do
    if [ ! -f "/tmp/tamako-readback/data/$cid/memory.lbug" ]; then
        echo "READBACK FAIL: $cid (memory.lbug lost in transfer)"
        failed=1; continue
    fi
    out=$(run_probe --facts "$cid" "__readback_probe__") || {
        grep -q "no node named" <<<"$out" || { echo "READBACK FAIL: $cid"; failed=1; }
    }
done < "$INVENTORY"
[ "$failed" = 0 ] || { echo "scratch kept at /tmp/tamako-readback"; exit 1; }
rm -rf /tmp/tamako-readback
```

   `--facts` is the offline read-only graph open (decision 75/77: no
   LLM, no lock, no writes); the never-present probe name makes "no node
   named" the PASS signature and any graph-open error the FAIL
   signature. `--merge-tool` is NOT a substitute (it resolves endpoints,
   spends LLM tokens, and writes `merge_plan.json`). `:U,z` chowns the
   scratch copy into the container namespace (a WAL-mode store needs a
   writable directory even for reads). Groups without a graph (never
   digested) carry no inventory entry; their SQLite coverage is
   `--status-all`. CI reuses this script against a fixture root with
   INVENTORY derived from the fixture itself (the step-5 find); the
   `-s` guard keeps a missing or empty list from passing green having
   probed nothing. Failure keeps the scratch and blocks the start.
8. Start: first assert the probe drop-in is gone (`systemctl --user cat
   tamako` shows the real ExecStart — no `sleep` loop), then
   `systemctl --user start tamako`. Verify against expectations:
   the wiring banner (models), the milestone-0 startup line (resolved
   `suffix_mode`/`timezone`), the first gate usage line, and one real
   Telegram reply.
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
  the drain and with systemctl replacing ctrl-c on the target), a
  Section 6 re-check that no new INFO-level line leaks into the
  one-line guarantee (the digest-start line is DEBUG), and a Section 6
  mention of the resolved trigger-config startup line.
- docs/soak-runbook.md: Section 4 stop/restart procedure (ctrl-c →
  `systemctl --user stop`), Section 5 backup step 1, the "same launch
  command" line, and the log-location guidance (journald).
- ARCHITECTURE.md: the graceful-shutdown paragraph (milestone 0);
  specs.md: the Section 12 observability surface gains the
  trigger-config startup line and the DEBUG digest-start line.
- current-state.md: Section 3 decision entries for ALL THREE
  milestone-0 items (drain, digest-start line, resolved trigger-config
  line); Section 4 item 11 marked DONE with the drain's decision
  reference.
