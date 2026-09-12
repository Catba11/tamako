> RUNTIME MOVED 2026-09-12 (migration-runbook.md): the bot now runs as
> the systemd --user quadlet `tamako.service` on the Fedora desktop
> (data root `/var/lib/tamako/data`, config `/var/lib/tamako/config`,
> logs `journalctl --user -u tamako`). The `cargo run --release --
> --live` commands below are the pre-container record — running one
> against the live bot starts a SECOND getUpdates poller (forbidden,
> migration-runbook.md Section 8 step 9). Stops/starts are
> `systemctl --user stop|start tamako` (StopSignal=SIGINT = the drain).

# Soak runbook — Phase 1 exit: two weeks in one test group

This runbook tells the operator how to run the two-week Phase 1 soak
(dev-roadmap.md Section 3, exit criteria; Section 8 item 6). The soak
proves: the bot lives in one test group for two weeks without operator
intervention; a restart loses no message and no digest boundary; the
dead-letter rate is visible.

## 1. Before the soak

1. Build the release binary: `cargo build --release`.
2. Create the data root and the persona file (specs.md Section 5.3).
   `--live` fails at startup without the file:

   ```sh
   mkdir -p ./data
   cp persona.toml ./data/persona.toml
   # Edit ./data/persona.toml if wanted. The file is the source of the
   # system preamble, the provider cache anchor (Rule C4).
   ```

   For experiments only, `--allow-default-persona` restores the lenient
   fallback chain. Do not use it for the soak.
3. Create the config file `tamako.toml` with the test group:

   ```toml
   [groups."-1001234567890"]
   # Empty table registers the group. Per-group overrides go here.
   ```

4. Check the bot setup in Telegram (README, "Live bring-up"): privacy
   mode OFF, administrator status recommended.
5. Set the environment: `TELOXIDE_TOKEN`, and `ANTHROPIC_API_KEY` (or
   `OPENAI_API_KEY` for an openai-compatible endpoint).

## 2. Launch

```sh
TELOXIDE_TOKEN=<telegram token> \
ANTHROPIC_API_KEY=<llm key> \
cargo run --release -- --live --config tamako.toml --data-root ./data
```

Expected startup lines:

- `persona configuration loaded path=./data/persona.toml` — the strict
  persona check passed.
- `telegram bot identity resolved`.
- One capability line per configured group. An administrator group logs
  at INFO. A non-administrator group logs one WARN (reaction collection
  off; everything else works).
- The first message of the group creates `./data/<chat_id>/store.db`
  and `./data/<chat_id>/memory.lbug`.

## 3. What to watch

### 3.1 In the logs

The bot curates its INFO output: exactly one `wake` line per wake event
and one `digest` line per completed digest, both on target
`tamako_core::actor`. A healthy group shows a calm rhythm: one wake line
per floor-bounded wake (`wake_floor`, 5 min default), one digest line
per batch. The line formats:

```text
INFO tamako_core::actor: wake chat_id=-1001234567890 trigger="message_count" injections=0 gate="participate" reason="a direct question" action="reply_sent" reply_to="w3"
INFO tamako_core::actor: digest chat_id=-1001234567890 batch_id=e1ba1491-bf91-5853-b0f1-58b24ba26c98 range=(0,101] outcome="written" nodes=19 edges=30
```

- `wake` fields: `chat_id`; `trigger` (`message_count`|`interval`|
  `forced`); `injections` (recall-memory count, 0 allowed); `gate`
  (`participate`|`silent`|`bypassed_forced`|`muted`|`in_flight_skipped`);
  `reason` — the gate's own reason string, ABSENT when it has none;
  `action` (`reply_sent`|`discarded_stale`|`nothing`); `reply_to` — the
  target's platform message id, ABSENT unless `action="reply_sent"`.
- `digest` fields: `chat_id`; `batch_id`; `range` — the rendered
  `(old,new]` msg-id range; `outcome` — `written` with `nodes`/`edges`
  counts, or `skeleton` (no counts).

| Signal | Healthy | Action when not healthy |
|---|---|---|
| Wake rate | One `wake` line per floor-bounded wake (`wake_floor`, 5 min default). | A wake storm below the floor is a bug; capture the log. |
| In-flight skips | `gate="in_flight_skipped" action="nothing"` occasionally, while an LLM wake runs. | Repeated skips with no `reply_sent` completion: LLM calls are stuck; check the endpoint. |
| Wake failures | None; a failed wake emits one ERROR `wake procedure failed; skipping this wake`. | Repeated wake ERRORs: check the endpoint and the model names. |
| Digest batches | One `digest` line per batch, `outcome="written"` (or `skeleton` on an empty extraction). | A batch that retries then dead-letters: see 3.2. |
| Digest retries | WARN `digest attempt failed; retry scheduled` is rare. This is an attempt-level signal, not the digest outcome. | Retry WARNs repeating for one `batch_id`: endpoint trouble; check latency and rate limits. |
| Dead letters | ERROR `digest batch dead-lettered after all retries` with `chat_id`, `batch_id`, `attempts`, `error` (specs.md Section 10.3). This line REPLACES the INFO `digest` line for the batch. | Rare is fine. A rising count needs attention. The skipped range stays in the raw log. |
| Outbound failures | WARN with the chat id, tolerated (specs.md Section 4.2). | Repeated permission failures: check the bot's rights in the group. |

For more detail, `-v` lifts every Tamako crate to debug level;
`RUST_LOG` always wins over the flag (README, "Watching the pet").

### 3.2 With `--status` (the Section 12 metrics)

The counters of specs.md Section 12 live in the state table. Read them
with the operator mode (read-only; safe while the bot runs):

```sh
cargo run --release -- --status -1001234567890 --data-root ./data --config tamako.toml
# or every group:
cargo run --release -- --status-all --data-root ./data --config tamako.toml
```

The output prints the counters, the two rates, the boundaries, the
muted state, and the most recent dead letters. Watch these metrics:

| Metric (specs.md Section 12) | Where in `--status` | Healthy range |
|---|---|---|
| Participation rate | `rates: participation rate` | Below 50 percent. A sustained higher rate means the gate is too permissive. |
| Injection rate | `rates: injection rate` | 20 to 40 percent of wakes. |
| Digest failure rate | `counters: digest_failures_total` vs. boundary advances | Low. |
| Dead-letter count | `dead letters: N total` plus recent entries (batch id, attempts, error, timestamp) | Visible; zero is not required. Every entry needs a glance at its error. |
| Wake rate | `counters: wakes_total` over wall-clock time | Compare against the floor configuration. |

Note: after a CLEAN shutdown of the bot, SQLite removes the WAL files.
A read-only `--status` can then fail on a read-only directory. Make the
directory writable, or run `--status` while the bot is up.

## 4. Restart procedure

A restart loses no message and no digest boundary (Phase 1 exit
criterion). The raw log is the source of truth (Rule P1). The actor
rebuilds the live context and the session state from the raw log, the
`injected_memories` table, and the state table.

1. Stop the bot: `systemctl --user stop tamako` on the desktop (the
   quadlet sets StopSignal=SIGINT, so this IS the ctrl-c drain path;
   pre-container era: ctrl-c). The shutdown drains in-flight
   digest/summary/wake tasks (up to one LLM call window for
   cancellable tasks; forced wakes run out) and flushes the session
   state of every group (decision 114). Wait for the `live run
   summary` line (journald: `journalctl --user -u tamako`; pre-journald
   era: the raw log file — still the Rule P1 source of truth there).
2. Start the bot again: `systemctl --user start tamako` (pre-container
   era: the same launch command).
3. Verify with `--status`: the boundaries and counters continue from
   the pre-restart values.

A crash (SIGKILL, power loss) is also safe: committed rows survive.
Start the bot again. If SQLite reports WAL damage on `store.db`, the
recovery rule of the database spec Section 10 applies: delete the
`store.db-wal` file and open the database again. Committed data is not
in the WAL tail only; the last checkpoint covers the rest.

## 5. Backup procedure

Per the database spec Section 10: checkpoint, then copy the files.

The graph file `memory.lbug` is checkpointed after every digest batch,
so the file on disk is consistent whenever no digest is in flight. A
clean ctrl-c stop ESTABLISHES that condition since decision 114: the
shutdown drain awaits or cancels in-flight digest work before exit,
and the digest-start DEBUG line of decision 115 marks dispatch for
post-hoc attribution. The remaining window is a HARD kill (SIGKILL,
crash, power loss) landing mid-digest: a batch interrupted that way
replays idempotently on the next run, and a backup taken after such a
stop may carry a mid-digest graph file.

The safe procedure (bot stopped):

1. Stop the bot (`systemctl --user stop tamako`; pre-container era:
   ctrl-c). On clean shutdown SQLite checkpoints
   `store.db` and removes the WAL files.
2. Copy the two files of the group:
   `cp ./data/<chat_id>/store.db ./data/<chat_id>/memory.lbug <backup dir>/`
3. Start the bot again.

The recovery point objective is one checkpoint interval. A daily
backup is enough for the soak.

## 6. Feedback loop: what to bring back for Phase 2

The thresholds 0.92 and 0.80, the relevance gate, and the participation
gate are calibrated with the data of this soak (dev-roadmap.md
Section 8). Record these observations during the two weeks:

1. Participation gate quality (specs.md Section 15, open item 1): the
   participation rate from `--status`; examples of bad joins and of
   missed good joins (save the raw-log message ids).
2. Recall relevance gate quality (Section 15, open item 1): the
   injection rate; examples of irrelevant "I remember: ..." injections
   and of relevant memories that were not injected.
3. Dead-letter entries (Section 15, open item 2): the batch id and the
   error of every dead letter. The skipped ranges stay in the raw log;
   the Phase 2 repair tool will reprocess them.
4. Concept fragmentation (the accepted Phase 1 defect): visible cases
   of one concept as several nodes. Phase 2 provides the merge tool.
5. Fallback attachment rate (database spec Section 10): the primary
   entity-resolution quality metric. Count Alias nodes with
   `"attachment": "fallback"` markers.
6. Edited messages (Section 15, open item 4): any case where the
   append-only edit behavior produced a wrong fact in the graph.
7. Warmup and engagement observations, if the group goes quiet:
   reaction and reply patterns around bot speech. They feed the Phase 2
   warmup calibration.
8. Operational notes: restart count, any crash, any unexpected log
   line, endpoint latency problems.

Bring the `--status-all` output of the last day and this list to the
Phase 2 planning session.
