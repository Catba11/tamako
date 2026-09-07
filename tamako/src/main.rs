//! Tamako binary. CLI wiring, the `--replay` demo, and the `--live` mode.
//! The demo replays a recorded chat log through the mock adapter into one
//! group actor and prints a summary. The live mode runs against Telegram
//! through the teloxide adapter (specs.md Section 4.2) and routes the
//! events of every configured group to its own actor. Phase 1 wires the
//! digest pipeline of specs.md Section 10 (M1) and the wake procedure of
//! specs.md Section 9 (M4) with the shallow recall of Sections 9.1-9.5
//! (M5) from the resolved LLM endpoints of Section 13; without the
//! family API key the pipelines degrade to silence. The `--merge-tool` /
//! `--merge` / `--merge-rollback` modes are the OFFLINE merge tool of
//! current-state.md decision 74 (graph-spec Section 7.7), and the
//! `--facts` / `--invalidate` / `--revalidate` modes the OFFLINE fact
//! commands of decision 75 (graph-spec Section 7.5): they never start
//! the event loop. Decision 77 (H5): the MUTATING offline commands
//! (--merge-tool --apply, --merge, --merge-rollback, --invalidate,
//! --revalidate, --dismiss-related-pair) take the per-group advisory lock
//! ({data_root}/{chat_id}/.tamako.lock) and refuse loudly when another
//! process holds it; --live holds the same per-group locks for its whole
//! lifetime, so a mutating tool against a served group fails immediately
//! instead of deep inside the lbug open. The read-only paths
//! (--merge-tool dry run, --facts, --status) take no lock.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, PoisonError, RwLock};

mod persona_watch;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tamako_adapter_mock::MockAdapter;
use tamako_adapter_teloxide::{BotChatStatus, GroupEvent, MediaEnricher, TeloxideAdapter};
use tamako_agent::endpoint::{
    CaptionEndpoint, EmbeddingEndpoint, LlmApi, RigEmbeddingProvider, DEFAULT_EMBEDDING_BASE_URL,
};
use tamako_agent::merge_confirm::EndpointMergeConfirmer;
use tamako_agent::recall::DeepRecallConfig;
use tamako_agent::resolve::{EndpointResolutionConfirmer, VectorResolutionConfig};
use tamako_agent::{
    AgentDigestPipeline, AgentError, EndpointConfig, LlmConfigValues, LlmEndpoints, LlmPurpose,
    PipelineConfig, RetryCaptionProvider, RigCaptionProvider, RigExtractor, RigGate,
    RigRelevanceGate, RigReplyGenerator, RigSummary, RigWarmupGenerator, ShallowRecall,
};
use tamako_core::actor::{
    spawn_group_actor, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::{BotConfig, TriggerConfig};
use tamako_core::digest::DigestPipeline;
use tamako_core::embedding::{EmbeddingError, EmbeddingWorker, GroupEmbeddingTarget};
use tamako_core::event::OutboundAction;
use tamako_core::merge::{
    apply_merge_plan, merge_candidate_set_hash, plan_merges, rollback_merge_action,
    scan_merge_candidates, MergeCandidate, MergeConfirmation, MergeError, MergeNodeInfo, MergePlan,
    MergePlanAction, MergeVerdict, SkipReason,
};
use tamako_core::summary::SummaryProvider;
use tamako_core::wake::{NoopRecall, RecallProvider, WakeServices};
use tamako_core::warmup::WarmupServices;
use tamako_memory::{LbugBackend, MemoryBackend, NodeType};
use tamako_persona::{load_persona, PersonaConfig, PetPreambleRenderer, PreambleRenderer};
use tamako_store::{read_group_status, GroupStatus, MediaStore, Store, StoreError};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
Usage:
  tamako --replay <fixture.json> [--data-root <dir>] [--config <config.toml>]
  tamako --live [--allow-default-persona] [--data-root <dir>] [--config <config.toml>]
  tamako --status <chat_id> [--data-root <dir>] [--config <config.toml>]
  tamako --status-all [--data-root <dir>] [--config <config.toml>]
  tamako --merge-tool <chat_id> [--apply] [--max-confirmations N] [--data-root <dir>] [--config <config.toml>]
  tamako --merge <chat_id> <loser_id> <survivor_id> [--force] [--data-root <dir>]
  tamako --merge-rollback <chat_id> <audit_id> [--data-root <dir>]
  tamako --facts <chat_id> <name> [--data-root <dir>]
  tamako --invalidate <chat_id> <edge_id> [--data-root <dir>]
  tamako --revalidate <chat_id> <edge_id> [--data-root <dir>]
  tamako --related-pairs <chat_id> [--data-root <dir>]
  tamako --dismiss-related-pair <chat_id> <pair_id> [--data-root <dir>]
  tamako --help

Options:
  --replay <fixture.json>  Replay a recorded chat log through the mock adapter.
  --live                   Run live against Telegram. The token comes from the
                           TELOXIDE_TOKEN environment variable. The served
                           groups are the [groups.<chat_id>] tables of the
                           config file. The bot must be a group admin with
                           privacy mode off. Live mode requires a persona
                           file at <data-root>/persona.toml (specs.md
                           Section 5.3).
  --status <chat_id>       Print a read-only status snapshot of one group:
                           the counters of specs.md Section 12, the digest
                           boundaries, the muted state, and the most recent
                           dead letters (specs.md Section 10.3). Opens
                           store.db read-only; safe while the bot runs.
  --status-all             Print the status snapshot of every group store
                           under the data root, sorted by chat id.
  --merge-tool <chat_id>   The offline merge tool (decision 74, graph-spec
                           Section 7.7): scans the group's vector index for
                           duplicate Person/Concept pairs, confirms each
                           candidate once on the digest endpoint (three-way
                           verdict: same merges, related writes a
                           related_pairs row, different skips), prints the
                           plan, and writes it to
                           <data-root>/<chat_id>/merge_plan.json. DRY RUN
                           by default: the group's store.db opens
                           READ-ONLY and nothing else is written without
                           --apply. --apply executes the PLAN FILE of a
                           previous dry run (decision 77): it re-scans the
                           candidates (cheap, no LLM) and refuses loudly
                           when the candidate set changed since the dry
                           run (\"plan is stale; re-run the dry run\").
                           --apply takes the per-group lock
                           (<data-root>/<chat_id>/.tamako.lock) for the
                           whole run and refuses when another process
                           holds it (is the bot running?). Without a
                           digest-endpoint LLM key the confirmations are
                           skipped and only the scan prints; no plan file
                           is written either way.
  --merge <chat_id> <loser_id> <survivor_id>
                           Manual merge without an LLM: merges loser_id
                           into survivor_id as an operator-decided 'same'
                           action (confirmed_by \"operator\") and appends
                           the audit row. A kind-incompatible pair (a
                           Person into a Concept, say) is a hard error
                           unless --force is given; the audit row records
                           the loser kind either way. Takes the per-group
                           lock and refuses when another process holds it
                           (is the bot running?).
  --merge-rollback <chat_id> <audit_id>
                           Rolls one 'same' merge back from its audit
                           snapshot and marks the row rolled back. The
                           restored node's embedding is rebuilt by the next
                           startup reconciliation. Takes the per-group
                           lock and refuses when another process holds it
                           (is the bot running?).
  --facts <chat_id> <name> The offline fact listing (decision 75,
                           graph-spec Section 7.5): every edge of the
                           node resolved from <name> (exact alias
                           match), both directions, valid AND invalid —
                           with the edge id, the direction, the
                           predicate, the other endpoint, a description
                           excerpt, and the validity timestamps. The
                           printed edge ids are the values that
                           --invalidate / --revalidate take. No LLM
                           needed. The store.db opens READ-ONLY
                           (decision 77, M7). The graph open
                           (memory.lbug) still conflicts with a running
                           bot: LadybugDB holds an exclusive OS file
                           lock, so against a running bot the command
                           fails loudly with a lock error — stop the bot
                           first. An unknown name exits non-zero.
  --invalidate <chat_id> <edge_id>
                           Sets invalid_at on one edge (decision 75,
                           graph-spec Section 7.5): the manual
                           invalidation. <edge_id> is the opaque id that
                           --facts prints; a malformed or unknown id
                           exits non-zero. A successful invalidation also
                           bumps the facts_invalidated_total counter.
                           Takes the per-group lock and refuses when
                           another process holds it (is the bot
                           running?). No LLM needed. Replay note
                           (decision 77): a replayed batch never
                           re-validates an invalidated edge (the stored
                           invalid_at survives the replay), but an
                           out-of-order FULL replay can still rewind a
                           fact's other columns (edge text, properties)
                           to the replayed batch's values — in-order
                           replay converges.
  --revalidate <chat_id> <edge_id>
                           Clears invalid_at on one edge (decision 75,
                           graph-spec Section 7.5): the typo safety net
                           that undoes an invalidation. <edge_id> is the
                           opaque id that --facts prints; a malformed or
                           unknown id exits non-zero. Does NOT decrement
                           facts_invalidated_total: the counter counts
                           invalidation events, not invalid edges. Takes
                           the per-group lock and refuses when another
                           process holds it (is the bot running?). No
                           LLM needed.
  --related-pairs <chat_id>
                           Lists the related_pairs side table of one
                           group (decision 83/106): every recorded
                           dotted-edge pair with its row id, status
                           (pending/promoted/dismissed), endpoint
                           names, reason, and timestamp. The row ids
                           are the values --dismiss-related-pair
                           takes. Read-only; the graph open conflicts
                           with a running bot (same as --facts). No
                           LLM needed.
  --dismiss-related-pair <chat_id> <pair_id>
                           Flips one PENDING related_pairs row to
                           'dismissed' (decision 106 (f)): the
                           promotion pass never offers the pair to the
                           digest model again. <pair_id> is the row id
                           --related-pairs prints; an unknown id or a
                           row already terminal exits non-zero. Takes
                           the per-group lock and refuses when another
                           process holds it (is the bot running?). No
                           LLM needed.
  --apply                  Only affects --merge-tool: executes the plan
                           file (merge_plan.json) of a previous dry run
                           instead of running a fresh dry run.
  --force                  Only affects --merge: allows a kind-incompatible
                           pair (a Person into a Concept, say); without it
                           such a merge is a hard error.
  --max-confirmations N    Only affects --merge-tool: caps the LLM
                           confirmation calls of one run. Default: 50.
  --allow-default-persona  Only affects --live: restores the lenient
                           persona fallback chain (repo-root example, then
                           the built-in default) instead of requiring
                           <data-root>/persona.toml. For experiments.
  -v, --verbose            Verbose logging: every tamako crate at debug
                           level (tamako=debug), dependencies stay quiet.
                           Accepted in every mode. Precedence: RUST_LOG
                           always wins over this flag when both are set.
  --data-root <dir>        Data root of the bot. Default: ./data
  --config <config.toml>   Bot configuration file. Optional. A missing file
                           keeps the global defaults. In the status modes it
                           supplies the per-group digest_max_retries of the
                           attempts display; in --merge-tool it supplies the
                           per-group merge_candidate_threshold.
  --help                   Show this text.";

/// The run mode. Exactly one of `--replay` / `--live` / `--status` /
/// `--status-all` / `--merge-tool` / `--merge` / `--merge-rollback` /
/// `--facts` / `--invalidate` / `--revalidate` / `--related-pairs` /
/// `--dismiss-related-pair` is required.
#[derive(Debug)]
enum Mode {
    Replay {
        fixture: PathBuf,
    },
    Live,
    Status {
        chat_id: String,
    },
    StatusAll,
    MergeTool {
        chat_id: String,
    },
    Merge {
        chat_id: String,
        loser_id: String,
        survivor_id: String,
    },
    MergeRollback {
        chat_id: String,
        audit_id: i64,
    },
    Facts {
        chat_id: String,
        name: String,
    },
    Invalidate {
        chat_id: String,
        edge_id: String,
    },
    Revalidate {
        chat_id: String,
        edge_id: String,
    },
    /// Decision 106 (f): the read-only listing of the `related_pairs`
    /// side table (the dismissal operator's overview).
    RelatedPairs {
        chat_id: String,
    },
    /// Decision 106 (f): the operator dismissal — one pending row
    /// flips to 'dismissed'.
    DismissRelatedPair {
        chat_id: String,
        pair_id: i64,
    },
}

/// The parsed command line.
#[derive(Debug)]
struct Cli {
    mode: Mode,
    data_root: PathBuf,
    config: Option<PathBuf>,
    allow_default_persona: bool,
    verbose: bool,
    /// The `--apply` flag. Only affects --merge-tool (the same
    /// accepted-everywhere discipline as --allow-default-persona).
    apply: bool,
    /// The `--force` flag. Only affects --merge (decision 77, M9):
    /// allows a kind-incompatible pair.
    force: bool,
    /// The `--max-confirmations` value of --merge-tool.
    max_confirmations: usize,
}

/// The default LLM-confirmation budget of one `--merge-tool` run
/// (decision 74: one digest-endpoint call per candidate pair).
const DEFAULT_MAX_CONFIRMATIONS: usize = 50;

/// The result of the command-line parse.
#[derive(Debug)]
enum ParseOutcome {
    Run(Cli),
    Help,
}

/// Parses the arguments. An `Err` carries a message for the user.
fn parse_args<I: Iterator<Item = String>>(args: I) -> std::result::Result<ParseOutcome, String> {
    let mut fixture = None;
    let mut live = false;
    let mut status = None;
    let mut status_all = false;
    let mut merge_tool = None;
    let mut merge = None;
    let mut merge_rollback = None;
    let mut facts = None;
    let mut invalidate = None;
    let mut revalidate = None;
    let mut related_pairs = None;
    let mut dismiss_related_pair = None;
    let mut allow_default_persona = false;
    let mut verbose = false;
    let mut apply = false;
    let mut force = false;
    let mut max_confirmations = None;
    let mut data_root = None;
    let mut config = None;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(ParseOutcome::Help),
            "--replay" => {
                let value = args.next().ok_or("the --replay flag needs a value")?;
                fixture = Some(PathBuf::from(value));
            }
            "--live" => live = true,
            "--status" => {
                let value = args.next().ok_or("the --status flag needs a value")?;
                status = Some(value);
            }
            "--status-all" => status_all = true,
            "--merge-tool" => {
                let value = args.next().ok_or("the --merge-tool flag needs a value")?;
                merge_tool = Some(value);
            }
            "--merge" => {
                let missing =
                    "the --merge flag needs three values: <chat_id> <loser_id> <survivor_id>";
                let chat_id = args.next().ok_or(missing)?;
                let loser_id = args.next().ok_or(missing)?;
                let survivor_id = args.next().ok_or(missing)?;
                merge = Some((chat_id, loser_id, survivor_id));
            }
            "--merge-rollback" => {
                let missing = "the --merge-rollback flag needs two values: <chat_id> <audit_id>";
                let chat_id = args.next().ok_or(missing)?;
                let audit_id = args.next().ok_or(missing)?;
                let audit_id = audit_id.parse::<i64>().map_err(|_| {
                    format!("the --merge-rollback audit id must be an integer, got '{audit_id}'")
                })?;
                merge_rollback = Some((chat_id, audit_id));
            }
            "--facts" => {
                let missing = "the --facts flag needs two values: <chat_id> <name>";
                let chat_id = args.next().ok_or(missing)?;
                let name = args.next().ok_or(missing)?;
                facts = Some((chat_id, name));
            }
            "--related-pairs" => {
                let missing = "the --related-pairs flag needs one value: <chat_id>";
                let chat_id = args.next().ok_or(missing)?;
                related_pairs = Some(chat_id);
            }
            "--dismiss-related-pair" => {
                let missing =
                    "the --dismiss-related-pair flag needs two values: <chat_id> <pair_id>";
                let chat_id = args.next().ok_or(missing)?;
                let pair_id = args.next().ok_or(missing)?;
                let pair_id = pair_id.parse::<i64>().map_err(|_| {
                    format!(
                        "the --dismiss-related-pair pair id must be an integer, got '{pair_id}'"
                    )
                })?;
                dismiss_related_pair = Some((chat_id, pair_id));
            }
            "--invalidate" | "--revalidate" => {
                // The edge id is an OPAQUE string (a compact JSON of the
                // edge's natural key, decision 75): the parse accepts
                // any value and the RUNTIME errors loudly on a malformed
                // or unknown id (EdgeId::decode / the backend).
                let missing = format!("the {arg} flag needs two values: <chat_id> <edge_id>");
                let chat_id = args.next().ok_or(missing.clone())?;
                let edge_id = args.next().ok_or(missing)?;
                if arg == "--invalidate" {
                    invalidate = Some((chat_id, edge_id));
                } else {
                    revalidate = Some((chat_id, edge_id));
                }
            }
            "--apply" => apply = true,
            "--force" => force = true,
            "--max-confirmations" => {
                let value = args
                    .next()
                    .ok_or("the --max-confirmations flag needs a value")?;
                max_confirmations = Some(value.parse::<usize>().map_err(|_| {
                    format!(
                        "the --max-confirmations value must be a non-negative integer, got '{value}'"
                    )
                })?);
            }
            "--allow-default-persona" => allow_default_persona = true,
            "-v" | "--verbose" => verbose = true,
            "--data-root" => {
                let value = args.next().ok_or("the --data-root flag needs a value")?;
                data_root = Some(PathBuf::from(value));
            }
            "--config" => {
                let value = args.next().ok_or("the --config flag needs a value")?;
                config = Some(PathBuf::from(value));
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    // Exactly one mode. The candidates collect first so the mutual-
    // exclusion check stays one comparison for seven modes.
    let mut modes: Vec<Mode> = Vec::new();
    if let Some(fixture) = fixture {
        modes.push(Mode::Replay { fixture });
    }
    if live {
        modes.push(Mode::Live);
    }
    if let Some(chat_id) = status {
        modes.push(Mode::Status { chat_id });
    }
    if status_all {
        modes.push(Mode::StatusAll);
    }
    if let Some(chat_id) = merge_tool {
        modes.push(Mode::MergeTool { chat_id });
    }
    if let Some((chat_id, loser_id, survivor_id)) = merge {
        modes.push(Mode::Merge {
            chat_id,
            loser_id,
            survivor_id,
        });
    }
    if let Some((chat_id, audit_id)) = merge_rollback {
        modes.push(Mode::MergeRollback { chat_id, audit_id });
    }
    if let Some((chat_id, name)) = facts {
        modes.push(Mode::Facts { chat_id, name });
    }
    if let Some((chat_id, edge_id)) = invalidate {
        modes.push(Mode::Invalidate { chat_id, edge_id });
    }
    if let Some((chat_id, edge_id)) = revalidate {
        modes.push(Mode::Revalidate { chat_id, edge_id });
    }
    if let Some(chat_id) = related_pairs {
        modes.push(Mode::RelatedPairs { chat_id });
    }
    if let Some((chat_id, pair_id)) = dismiss_related_pair {
        modes.push(Mode::DismissRelatedPair { chat_id, pair_id });
    }
    const MODE_LIST: &str = "--replay <fixture.json>, --live, --status <chat_id>, \
         --status-all, --merge-tool <chat_id>, --merge <chat_id> <loser_id> <survivor_id>, \
         --merge-rollback <chat_id> <audit_id>, --facts <chat_id> <name>, \
         --invalidate <chat_id> <edge_id>, --revalidate <chat_id> <edge_id>, \
         --related-pairs <chat_id>, or --dismiss-related-pair <chat_id> <pair_id>";
    let mode = match modes.len() {
        1 => modes.pop().expect("exactly one mode collected"),
        0 => return Err(format!("one of {MODE_LIST} is required")),
        _ => {
            return Err(format!(
                "the run modes are mutually exclusive: pick exactly one of {MODE_LIST}"
            ))
        }
    };
    Ok(ParseOutcome::Run(Cli {
        mode,
        data_root: data_root.unwrap_or_else(|| PathBuf::from("./data")),
        config,
        allow_default_persona,
        verbose,
        apply,
        force,
        max_confirmations: max_confirmations.unwrap_or(DEFAULT_MAX_CONFIRMATIONS),
    }))
}

/// The tracing filter. Precedence: RUST_LOG (always wins) →
/// -v/--verbose (`tamako=debug`: all Tamako crates at debug,
/// dependencies quiet — the directive matches by target prefix, so it
/// covers tamako, tamako_core, tamako_agent, and so on) → the default
/// `info`.
fn log_filter(verbose: bool) -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(if verbose { "tamako=debug" } else { "info" }))
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match parse_args(std::env::args().skip(1)) {
        Ok(ParseOutcome::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(ParseOutcome::Run(cli)) => cli,
        Err(message) => {
            eprintln!("error: {message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(log_filter(cli.verbose))
        .init();

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Loads the bot configuration. No path, or a path that does not exist,
/// gives the global defaults of specs.md Section 13.
fn load_bot_config(path: Option<&Path>) -> Result<BotConfig> {
    let mut config = match path {
        Some(path) if path.exists() => {
            let text = std::fs::read_to_string(path).with_context(|| {
                format!("failed to read the configuration file {}", path.display())
            })?;
            let config = BotConfig::from_toml_str(&text).with_context(|| {
                format!("failed to parse the configuration file {}", path.display())
            })?;
            info!(path = %path.display(), "bot configuration loaded");
            config
        }
        Some(path) => {
            warn!(path = %path.display(), "configuration file not found; using the global defaults");
            BotConfig::default()
        }
        None => BotConfig::default(),
    };
    // Decision 90: the trigger-key environment overrides
    // (TAMAKO_SUFFIX_MODE, TAMAKO_TIMEZONE) replace the GLOBAL values —
    // they apply over the file-less defaults too, and per-group TOML
    // still wins over them.
    config.apply_trigger_env_overrides()?;
    Ok(config)
}

/// Loads the persona with the fallback chain of tamako-persona: first
/// `{data_root}/persona.toml`, then `./persona.toml` (the repo-root
/// example), then the built-in default.
fn load_persona_with_fallback(data_root: &Path) -> PersonaConfig {
    let candidates = [
        data_root.join("persona.toml"),
        PathBuf::from("./persona.toml"),
    ];
    for path in &candidates {
        if !path.exists() {
            continue;
        }
        match load_persona(path) {
            Ok(persona) => {
                info!(path = %path.display(), "persona configuration loaded");
                return persona;
            }
            Err(error) => {
                warn!(path = %path.display(), %error, "persona file is invalid; trying the next fallback");
            }
        }
    }
    warn!("no persona file found; using the built-in default persona");
    PersonaConfig::default()
}

/// The strict persona policy of --live (specs.md Section 5.3). A silent
/// fallback to a default persona in production hides configuration
/// mistakes, so a missing file is a hard startup error, and a present
/// but unparseable file is a hard startup error with the parse context.
/// Rule C4 context: the preamble is the provider cache anchor; its
/// source must be deliberate. The `--allow-default-persona` escape hatch
/// restores the lenient chain of `load_persona_with_fallback`.
fn load_persona_strict(data_root: &Path) -> Result<PersonaConfig> {
    let path = data_root.join("persona.toml");
    if !path.exists() {
        anyhow::bail!(
            "--live mode requires a persona file at {} (specs.md Section 5.3). \
             The preamble is the provider cache anchor; its source must be deliberate \
             (Rule C4). Create the file from the repo-root example: cp persona.toml {}. \
             For experiments, --allow-default-persona restores the lenient fallback chain.",
            path.display(),
            path.display()
        );
    }
    // A broken persona file in live mode is a configuration mistake;
    // never silently fall back.
    let persona = load_persona(&path)
        .with_context(|| format!("the persona file {} is invalid", path.display()))?;
    info!(path = %path.display(), "persona configuration loaded");
    Ok(persona)
}

/// Maps a `TriggerConfig` to the endpoint-resolution input of
/// tamako-agent (specs.md Section 13). A mechanical field copy.
fn llm_config_values(config: &TriggerConfig) -> LlmConfigValues {
    LlmConfigValues {
        llm_api: config.llm_api.clone(),
        llm_base_url: config.llm_base_url.clone(),
        llm_session_id: config.llm_session_id.clone(),
        digest_model: config.digest_model.clone(),
        gate_model: config.gate_model.clone(),
        reply_model: config.reply_model.clone(),
        summary_model: config.summary_model.clone(),
        digest_llm_api: config.digest_llm_api.clone(),
        digest_llm_base_url: config.digest_llm_base_url.clone(),
        gate_llm_api: config.gate_llm_api.clone(),
        gate_llm_base_url: config.gate_llm_base_url.clone(),
        reply_llm_api: config.reply_llm_api.clone(),
        reply_llm_base_url: config.reply_llm_base_url.clone(),
        summary_llm_api: config.summary_llm_api.clone(),
        summary_llm_base_url: config.summary_llm_base_url.clone(),
        structured_output: config.structured_output.clone(),
        digest_structured_output: config.digest_structured_output.clone(),
        gate_structured_output: config.gate_structured_output.clone(),
        reply_structured_output: config.reply_structured_output.clone(),
        summary_structured_output: config.summary_structured_output.clone(),
        // Decision 66 (global-only): the config always carries the
        // resolved value; TAMAKO_EMBEDDING_MODEL /
        // TAMAKO_EMBEDDING_BASE_URL win inside EmbeddingEndpoint::resolve
        // (the same env-wins idiom as the purpose keys).
        embedding_model: Some(config.embedding_model.clone()),
        embedding_llm_base_url: Some(config.embedding_llm_base_url.clone()),
        // Decision 82 (c) (global-only for construction, the same
        // standing as the embedding pair): the config always carries the
        // resolved value; TAMAKO_CAPTION_MODEL / TAMAKO_CAPTION_BASE_URL
        // win inside CaptionEndpoint::resolve (the same env-wins idiom).
        caption_model: Some(config.caption_model.clone()),
        caption_llm_base_url: Some(config.caption_llm_base_url.clone()),
    }
}

/// Resolves the four LLM endpoints of specs.md Section 13 from the
/// group configuration and the environment. A resolve error (an unknown
/// `llm_api` family string, in the config or in `TAMAKO_LLM_API`) is a
/// HARD startup error: operator misconfiguration must surface, never
/// silently default.
fn resolve_endpoints(config: &TriggerConfig) -> Result<LlmEndpoints> {
    LlmEndpoints::resolve(&llm_config_values(config))
        .map_err(anyhow::Error::new)
        .context("failed to resolve the LLM endpoints (specs.md Section 13)")
}

/// The four per-purpose session-affinity suffixes minted by
/// [`Store::get_or_insert_session_suffix`] (decision 84 (b)), one field
/// per completion purpose. The minting (the I/O) is
/// [`apply_session_suffixes`]'s concern; the struct is the input of the
/// pure splice below, so the per-purpose routing is unit-testable
/// without a store.
struct SessionSuffixes {
    digest: String,
    gate: String,
    reply: String,
    summary: String,
}

/// The pure splice of decision 84 (b): given the resolved (prefix)
/// endpoints and the four per-purpose persisted suffixes, the full
/// session id of each purpose is `{prefix}-{suffix}`. Pure — the
/// minting (the I/O) is [`apply_session_suffixes`]'s concern.
fn splice_session_suffixes(endpoints: LlmEndpoints, suffixes: &SessionSuffixes) -> LlmEndpoints {
    LlmEndpoints {
        digest: endpoints.digest.with_session_suffix(&suffixes.digest),
        gate: endpoints.gate.with_session_suffix(&suffixes.gate),
        reply: endpoints.reply.with_session_suffix(&suffixes.reply),
        summary: endpoints.summary.with_session_suffix(&suffixes.summary),
    }
}

/// Decision 84 (b)/(c): mints the per-(group, purpose) session-affinity
/// suffix LAZILY at the per-group service-build site — here, NOT in
/// `LlmEndpoints::resolve` (group-agnostic; group-less callers like
/// `--status` keep the bare prefix) — and joins it onto each purpose's
/// resolved prefix: the full session id is `{prefix}-{suffix}`, sent as
/// BOTH the `x-opencode-session` and the `x-session-id` header
/// (decision 84 (a)). `Store::get_or_insert_session_suffix` is the ONLY
/// mint (the binary never generates a suffix): the suffix persists in
/// `llm_session_keys` and is NEVER rotated, so provider-side affinity
/// (OpenRouter sticky routing) survives restarts.
///
/// Scope: the four COMPLETION purposes only. The embedding and caption
/// providers are built ONCE, process-wide, from the global config (ONE
/// `Arc` per process, decision 73) — there is no per-group construction
/// site for them at cutover, so they keep the bare prefix (refer to
/// [`build_embedding_provider`]).
///
/// Synchronous like the whole store crate: async call sites run the
/// whole helper (all four mints, one blocking task) on the blocking
/// pool (AGENT.md Section 6.2). A mint failure propagates like the
/// neighboring `resolve_endpoints` error: a broken suffix mint means a
/// broken store, which is fatal for serving the group anyway — the
/// spawn site fails loudly, never silently falling back to the bare
/// prefix (an unpersisted id would re-pin the group to a fresh provider
/// session on every restart).
fn apply_session_suffixes(
    store: &Arc<Store>,
    chat_id: &str,
    endpoints: LlmEndpoints,
) -> Result<LlmEndpoints> {
    let mint = |purpose: LlmPurpose| -> Result<String> {
        store
            .get_or_insert_session_suffix(chat_id, purpose.as_str())
            .with_context(|| {
                format!(
                    "failed to mint the {} session-affinity suffix of group {chat_id} (decision 84 (b))",
                    purpose.as_str()
                )
            })
    };
    let suffixes = SessionSuffixes {
        digest: mint(LlmPurpose::Digest)?,
        gate: mint(LlmPurpose::Gate)?,
        reply: mint(LlmPurpose::Reply)?,
        summary: mint(LlmPurpose::Summary)?,
    };
    Ok(splice_session_suffixes(endpoints, &suffixes))
}

/// Builds the digest pipeline (specs.md Section 10) for the resolved
/// digest endpoint. `Ok(None)` means digests are disabled for this run:
/// the replay still works, the digest trigger stays a stub. A missing
/// family API key (`EndpointClient::build` reports it as
/// `AgentError::ProviderConfig`, either family) degrades to `None` with
/// a warning; every other build error propagates.
///
/// Decision 66: the pipeline's embedding enqueue needs a DEDICATED
/// one-group Store (the chat_id-less embedding helpers require exactly
/// one open group per Store instance — `StoreError::AmbiguousGroup`
/// otherwise — while the shared store opens one group per served
/// chat), opened here through `GroupEmbeddingTarget::open`, the same
/// shape as the embedding worker's per-group target. A failed open
/// degrades the enqueue ALONE to disabled with a WARN — never a
/// startup failure; the worker's startup reconciliation heals the loss
/// through its own stores.
///
/// Decision 73: `embedding_provider` is the shared provider of
/// [`build_embedding_provider`] (ONE `Arc` per process, the same
/// instance the embedding worker uses). `Some` wires the step-3 vector
/// pre-screen with the confirmer built from the SAME digest endpoint
/// the extractor uses (the confirmation's latency lands on the digest
/// path, like the extraction retries); `None` (replay mode, or a
/// missing `OPENAI_API_KEY`) keeps the byte-identical Phase 1
/// resolution. A confirmer build failure degrades the pre-screen
/// ALONE to disabled with a WARN, mirroring the store-open degrade.
/// The four decision-73 keys (`vector_resolution` /
/// `vector_candidate_threshold` / `resolution_confirm_budget`)
/// come from the resolved per-group
/// `trigger_config` (specs.md Section 13).
///
/// Decision 75: the resolved per-group `single_value_predicates` ride
/// the graph commit through `with_single_value_predicates` — the SAME
/// wiring in replay and live mode (replay convergence is a write-path
/// property).
fn build_digest_pipeline(
    store: &Arc<Store>,
    memory: &Arc<LbugBackend>,
    endpoint: &EndpointConfig,
    data_root: &Path,
    chat_id: &str,
    embedding_provider: Option<Arc<dyn tamako_core::embedding::EmbeddingProvider>>,
    trigger_config: &tamako_core::config::TriggerConfig,
) -> Result<Option<Arc<dyn DigestPipeline>>> {
    let model = endpoint.model.clone();
    match RigExtractor::from_endpoint(endpoint) {
        Ok(extractor) => {
            info!(model = %model, "digest pipeline wired (live extraction)");
            let pipeline = AgentDigestPipeline::new(
                Arc::clone(store),
                Arc::clone(memory),
                Arc::new(extractor),
                // specs.md Section 13: max_retries = 5.
                PipelineConfig::default(),
            );
            let pipeline = match GroupEmbeddingTarget::open(data_root, chat_id) {
                Ok(target) => pipeline.with_embedding_store(target.store),
                Err(error) => {
                    warn!(chat_id = %chat_id, %error, "embedding enqueue disabled: the dedicated group store failed to open; the startup reconciliation will repair the loss");
                    pipeline
                }
            };
            // Decision 75: the resolved per-group single-value registry
            // rides the graph commit. The replay path wires it too —
            // replay convergence is a write-path property (a replayed
            // batch converges state-wise), so live and replay must run
            // the SAME invalidation behavior.
            let pipeline = pipeline
                .with_single_value_predicates(trigger_config.single_value_predicates.clone());
            // Decision 73: the vector pre-screen. Both the provider AND
            // the dedicated store must be wired for step 3 to activate
            // (the KNN read rides the one-group store).
            let vector_config = VectorResolutionConfig {
                enabled: trigger_config.vector_resolution,
                candidate_threshold: trigger_config.vector_candidate_threshold,
                confirm_budget: trigger_config.resolution_confirm_budget,
            };
            let pipeline = match embedding_provider {
                Some(provider) => match EndpointResolutionConfirmer::from_endpoint(endpoint) {
                    Ok(confirmer) => {
                        pipeline.with_vector_prescreen(provider, Arc::new(confirmer), vector_config)
                    }
                    Err(error) => {
                        warn!(chat_id = %chat_id, %error, "vector pre-screen disabled: the resolution confirmer failed to build; resolution falls back to the Phase 1 steps");
                        pipeline
                    }
                },
                None => pipeline,
            };
            Ok(Some(Arc::new(pipeline) as Arc<dyn DigestPipeline>))
        }
        Err(AgentError::ProviderConfig(error)) => {
            warn!(%error, "digest pipeline disabled: no provider configuration; digests will not run");
            Ok(None)
        }
        Err(error) => Err(error).context("failed to build the digest pipeline"),
    }
}

/// Builds the Rule C3 summarizer (specs.md Section 10, keep-two
/// summary retention) for the resolved summary endpoint. `Ok(None)`
/// means the summarizer is disabled for this run: the actor keeps the
/// old-style C3 removal, so the removed chunk drops without a summary.
/// A missing family API key (`EndpointClient::build` reports it as
/// `AgentError::ProviderConfig`, either family) degrades to `None`
/// with a warning; every other build error propagates.
fn build_summary_provider(endpoint: &EndpointConfig) -> Result<Option<Arc<dyn SummaryProvider>>> {
    let model = endpoint.model.clone();
    match RigSummary::from_endpoint(endpoint) {
        Ok(summary) => {
            info!(model = %model, "Rule C3 summarizer wired (live segmented summaries)");
            Ok(Some(Arc::new(summary)))
        }
        Err(AgentError::ProviderConfig(error)) => {
            warn!(%error, "Rule C3 summarizer disabled: no provider configuration; removed chunks drop without a summary");
            Ok(None)
        }
        Err(error) => Err(error).context("failed to build the Rule C3 summarizer"),
    }
}

/// Builds the wake-procedure services (specs.md Section 9) for the
/// resolved gate and reply endpoints. `Ok(None)` means the wake
/// procedure is disabled for this run: the actor keeps its stub
/// behavior and the bot stays silent. A missing family API key in
/// EITHER endpoint (reported as `AgentError::ProviderConfig`) degrades
/// to `None` with one warning; every other build error propagates.
///
/// The recall seam (Section 9 step 2, M5) wires the shallow recall
/// worker over the shared store and graph. Its relevance gate runs on
/// the cheap `gate` endpoint (Section 9.1) and the injection cap comes
/// from the group configuration (`recall_injection_cap`, Section 9.2).
/// A missing key for the relevance gate degrades the recall ALONE to
/// the no-op (the wake still runs; the injection list stays empty);
/// every other recall build error propagates.
///
/// Decision 76: when the resolved group configuration has
/// `deep_recall = true` (the default) AND `embedding_provider` is
/// `Some` (live mode; replay passes `None` — replay stays shallow-only,
/// deterministic and network-free, the same discipline as the
/// decision-73 vector pre-screen), the recall widens with
/// [`ShallowRecall::with_deep_recall`]: the shared provider Arc, the
/// group's `vector_candidate_threshold`, and `recall_candidate_cap`.
/// The deep store sources (KNN over `node_embeddings`, LIKE over
/// `edge_texts`) ride the chat_id-less single-open-group helpers, which
/// the SHARED store cannot serve in a multi-group live deployment
/// (`StoreError::AmbiguousGroup` once a second group's actor opened its
/// group): with deep recall on, the recall therefore gets a DEDICATED
/// one-group Store opened here through `GroupEmbeddingTarget::open`
/// (the same shape as the decision-66 digest enqueue and the embedding
/// worker's per-group targets — the same per-group store.db file, so
/// the chat-scoped shallow reads, `injected_memories` dedup included,
/// see the same data). A failed open degrades to the shallow-only form
/// over the shared store with a WARN — never a startup failure,
/// mirroring the digest pipeline's enqueue degrade (decision 66).
/// `deep_recall = false` never opens the dedicated store and never
/// calls `with_deep_recall`: the byte-identical pre-76 shallow path.
#[allow(clippy::too_many_arguments)] // the decision-86 suffix slot joins the per-group wiring
fn build_wake_services(
    store: &Arc<Store>,
    memory: &Arc<LbugBackend>,
    endpoints: &LlmEndpoints,
    data_root: &Path,
    chat_id: &str,
    embedding_provider: Option<Arc<dyn tamako_core::embedding::EmbeddingProvider>>,
    trigger_config: &TriggerConfig,
    // Decision 86: the shared rendered-suffix slot, wired into the reply
    // generator (the ONLY purpose that carries a suffix). Read at
    // request-assembly time; rewritten by the persona hot reload.
    // (unless decision 98 swapped in a group-private slot — the hot
    // reload never touches that one).
    suffix: Arc<RwLock<String>>,
    // Decision 95: the shared pet-tag slot, wired into the gate, reply,
    // and recall generators (every wake purpose whose prompts explain
    // or enforce the speech tag). Same hot-reload discipline.
    pet_tag: Arc<RwLock<String>>,
) -> Result<Option<WakeServices>> {
    // Decision 98: the per-group suffix override (boot-loaded) — a
    // `{data_root}/{chat_id}/persona.toml` with a `suffix` key swaps the
    // shared global slot for a private one the hot reload never touches.
    let suffix = tamako_persona::suffix_slot_for_group(data_root, chat_id, suffix);
    match (
        RigGate::from_endpoint(&endpoints.gate)
            .map(|gate| gate.with_pet_tag_slot(Arc::clone(&pet_tag))),
        RigReplyGenerator::from_endpoint(&endpoints.reply).map(|generator| {
            generator
                .with_suffix_slot(suffix)
                .with_suffix_mode(trigger_config.suffix_mode)
                .with_timezone(trigger_config.timezone)
                .with_pet_tag_slot(Arc::clone(&pet_tag))
        }),
    ) {
        (Ok(gate), Ok(reply)) => {
            // specs.md Section 9 step 2 (M5): the shallow recall worker
            // over the shared store and graph.
            let recall: Arc<dyn RecallProvider> = match RigRelevanceGate::from_endpoint(
                &endpoints.gate,
                // The gate renders the cap into its preamble (decision
                // 65); ShallowRecall enforces the same cap.
                trigger_config.recall_injection_cap,
            )
            .map(|gate| gate.with_pet_tag_slot(pet_tag))
            {
                Ok(relevance_gate) => {
                    // Decision 76: the deep-recall plan of this group.
                    // `Some` carries the dedicated one-group store and
                    // the knobs; `None` is the shallow-only form
                    // (config off, replay mode, or the WARN-degrade).
                    let deep = match (trigger_config.deep_recall, embedding_provider) {
                        (true, Some(provider)) => {
                            deep_recall_store(data_root, chat_id).map(|recall_store| {
                                (
                                    recall_store,
                                    DeepRecallConfig {
                                        provider,
                                        vector_candidate_threshold: trigger_config
                                            .vector_candidate_threshold,
                                        candidate_cap: trigger_config.recall_candidate_cap,
                                    },
                                )
                            })
                        }
                        _ => None,
                    };
                    match deep {
                        Some((recall_store, deep_config)) => Arc::new(
                            ShallowRecall::new(
                                recall_store,
                                Arc::clone(memory),
                                relevance_gate,
                                trigger_config.recall_injection_cap,
                            )
                            .with_deep_recall(deep_config),
                        ),
                        None => Arc::new(ShallowRecall::new(
                            Arc::clone(store),
                            Arc::clone(memory),
                            relevance_gate,
                            trigger_config.recall_injection_cap,
                        )),
                    }
                }
                // The same degrade-to-silence policy as the whole
                // wake build: no provider key, no recall.
                Err(AgentError::ProviderConfig(error)) => {
                    warn!(%error, "recall disabled: no provider configuration; wakes inject nothing");
                    Arc::new(NoopRecall)
                }
                Err(error) => {
                    return Err(error).context("failed to build the recall relevance gate")
                }
            };
            info!(
                gate_model = %endpoints.gate.model,
                reply_model = %endpoints.reply.model,
                recall_injection_cap = trigger_config.recall_injection_cap,
                "wake procedure wired (live recall, gate, and reply)"
            );
            Ok(Some(WakeServices {
                recall,
                gate: Arc::new(gate),
                reply: Arc::new(reply),
            }))
        }
        (Err(AgentError::ProviderConfig(error)), _)
        | (_, Err(AgentError::ProviderConfig(error))) => {
            warn!(%error, "wake procedure disabled: no provider configuration; the bot stays silent this run");
            Ok(None)
        }
        (Err(error), _) | (_, Err(error)) => {
            Err(error).context("failed to build the wake services")
        }
    }
}

/// Builds the warmup-trigger services (specs.md Sections 8.4/8.5/9.7,
/// decision 78) for the resolved REPLY endpoint: the warmup is a
/// reply-purpose generation (decision 78 (a), Section 13), so it rides
/// the same endpoint the wake's reply generator uses. The degrade
/// doctrine mirrors `build_wake_services`: a missing family API key
/// (reported as `AgentError::ProviderConfig`) degrades to `Ok(None)`
/// with one warning — the actor treats `None` as fully inert — and
/// every other build error propagates.
fn build_warmup_services(
    endpoints: &LlmEndpoints,
    pet_tag: Arc<RwLock<String>>,
) -> Result<Option<WarmupServices>> {
    match RigWarmupGenerator::from_endpoint(&endpoints.reply)
        .map(|generator| generator.with_pet_tag_slot(pet_tag))
    {
        Ok(generator) => {
            info!(reply_model = %endpoints.reply.model, "warmup trigger wired (reply endpoint)");
            Ok(Some(WarmupServices {
                generator: Arc::new(generator),
            }))
        }
        Err(AgentError::ProviderConfig(error)) => {
            warn!(%error, "warmup disabled: no provider configuration; the bot never starts conversations this run");
            Ok(None)
        }
        Err(error) => Err(error).context("failed to build the warmup services"),
    }
}

/// Decision 76: opens the DEDICATED one-group Store the deep-recall
/// store sources need (the chat_id-less KNN/LIKE helpers reject a
/// multi-group Store with `StoreError::AmbiguousGroup`), the same
/// `GroupEmbeddingTarget::open` shape as the decision-66 digest
/// enqueue. A failed open degrades the recall ALONE to shallow-only
/// with a WARN — never a startup failure; the wake keeps the pre-76
/// candidate set and the next restart retries the open.
fn deep_recall_store(data_root: &Path, chat_id: &str) -> Option<Arc<Store>> {
    match GroupEmbeddingTarget::open(data_root, chat_id) {
        Ok(target) => Some(target.store),
        Err(error) => {
            warn!(chat_id = %chat_id, %error, "deep recall disabled: the dedicated group store failed to open; recall stays shallow-only this run");
            None
        }
    }
}

/// Adapts the tamako-agent embedding provider onto the tamako-core
/// embedding seam (the contract-in-core pattern of the digest pipeline:
/// tamako-core cannot depend on tamako-agent, AGENT.md Section 4, so
/// the trait lives in tamako-core and the binary bridges it).
struct AgentEmbeddingProvider(RigEmbeddingProvider);

impl tamako_core::embedding::EmbeddingProvider for AgentEmbeddingProvider {
    fn embed<'a>(
        &'a self,
        text: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Vec<f32>, EmbeddingError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            tamako_agent::endpoint::EmbeddingProvider::embed(&self.0, text)
                .await
                .map_err(|error| EmbeddingError::Provider(error.to_string()))
        })
    }

    fn embed_texts<'a>(
        &'a self,
        texts: &'a [String],
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Vec<Vec<f32>>, EmbeddingError>>
                + Send
                + 'a,
        >,
    > {
        // Decision 73: forward to the rig impl's ONE batched HTTP call.
        // The core trait's default would loop `embed` sequentially —
        // one HTTP call per text — which would multiply the digest
        // path's endpoint latency by the unresolved-entity count.
        Box::pin(async move {
            tamako_agent::endpoint::EmbeddingProvider::embed_texts(&self.0, texts)
                .await
                .map_err(|error| EmbeddingError::Provider(error.to_string()))
        })
    }
}

/// Builds the process-wide embedding provider (decision 66), shared by
/// the embedding worker AND the decision-73 vector pre-screen of every
/// group's digest pipeline: ONE `Arc` instance, so the resolver's
/// batched embeddings calls and the worker's drain calls ride the same
/// rig client. The embedding endpoint is global-only (config.rs,
/// decision 66), so resolution uses the GLOBAL trigger config. The
/// degrade mirrors the per-purpose builders: a missing `OPENAI_API_KEY`
/// logs one WARN inside `RigEmbeddingProvider::build` and the result is
/// `None` (the worker is not spawned; the pre-screen stays inert).
/// Decision 84 (b) scope limitation: this provider is built ONCE,
/// process-wide, from the GLOBAL config (ONE `Arc` per process,
/// decision 73, shared by the digest pre-screen, the wake recall, and
/// the embedding worker) — there is NO per-group construction site at
/// cutover, so the embedding endpoint sends the BARE session-id prefix
/// on both affinity headers. The per-(group, purpose) suffix threading
/// of [`apply_session_suffixes`] covers the four completion purposes
/// only (the reply-path cache decision 84 measured); per-group
/// embedding affinity would require restructuring this deliberately
/// shared provider into per-group instances — follow-up, deliberately
/// not done (the same holds for the caption provider of
/// [`build_caption_provider`]).
fn build_embedding_provider(
    setup: &SharedSetup,
) -> Option<Arc<dyn tamako_core::embedding::EmbeddingProvider>> {
    let values = llm_config_values(&setup.bot_config.global);
    let endpoint = EmbeddingEndpoint::resolve(&values);
    // Decision 77 (M6b): the OPENAI_API_KEY dual-use conflation
    // detector. Embeddings ALWAYS read OPENAI_API_KEY (the
    // openai-compatible family key), and openai-compatible COMPLETION
    // endpoints read the same variable. When the embeddings resolve to
    // the DEFAULT OpenRouter base URL while the completions resolve
    // elsewhere (the typical Opencode Go deployment), the operator
    // likely set OPENAI_API_KEY for the completions provider only —
    // embeddings then fail with auth errors against OpenRouter. One
    // WARN at startup; the remedy is embedding_llm_base_url /
    // TAMAKO_EMBEDDING_BASE_URL. The digest endpoint stands in for the
    // completions side (the per-purpose base URLs rarely diverge).
    if endpoint.base_url == DEFAULT_EMBEDDING_BASE_URL {
        if let Ok(endpoints) = LlmEndpoints::resolve(&values) {
            let digest = &endpoints.digest;
            if digest.api == LlmApi::OpenAiCompatible
                && digest.base_url.as_deref() != Some(endpoint.base_url.as_str())
            {
                warn!(
                    embedding_base_url = %endpoint.base_url,
                    completions_base_url = ?digest.base_url,
                    "embeddings resolve to the default OpenRouter base URL but the completions \
                     endpoint resolves to a different base URL; both read OPENAI_API_KEY, so the \
                     key must be valid for the embedding endpoint too — set embedding_llm_base_url \
                     (or TAMAKO_EMBEDDING_BASE_URL) if they should share one provider"
                );
            }
        }
    }
    RigEmbeddingProvider::build(&endpoint).map(|provider| {
        Arc::new(AgentEmbeddingProvider(provider))
            as Arc<dyn tamako_core::embedding::EmbeddingProvider>
    })
}

/// Builds the process-wide caption provider of decision 82 (media
/// captioning AT INTAKE), consumed by the teloxide adapter's
/// `MediaEnricher`: ONE `Arc` per process, the same standing as
/// [`build_embedding_provider`]. The caption endpoint is GLOBAL-only
/// for construction, so resolution uses the GLOBAL trigger config.
/// Decision 82 made `caption_model` / `caption_llm_base_url`
/// per-group-overridable keys, but the intake enrichment is a single
/// process-wide provider at cutover (one adapter, one caption pipeline);
/// resolving per-group caption models is follow-up the config keys
/// already admit. The degrade mirrors the embedding builder: a missing
/// `OPENAI_API_KEY` logs one WARN inside `RigCaptionProvider::build`
/// and the result is `None` — the adapter gets a `None` enricher and
/// media messages behave as before Block 2 (silently skipped when no
/// text). The rig provider rides inside [`RetryCaptionProvider`], the
/// decorator carrying the decision-82 (d) retry policy (three attempts,
/// 30 s/60 s backoff). Decision 84 (b) scope limitation (the same as
/// [`build_embedding_provider`]): process-wide construction means the
/// caption endpoint sends the BARE session-id prefix at cutover; the
/// per-(group, purpose) suffix covers the four completion purposes
/// only.
fn build_caption_provider(
    setup: &SharedSetup,
) -> Option<Arc<dyn tamako_core::caption::CaptionProvider>> {
    let endpoint = CaptionEndpoint::resolve(&llm_config_values(&setup.bot_config.global));
    RigCaptionProvider::build(&endpoint).map(|provider| {
        let inner: Arc<dyn tamako_core::caption::CaptionProvider> = Arc::new(provider);
        Arc::new(RetryCaptionProvider::new(inner)) as Arc<dyn tamako_core::caption::CaptionProvider>
    })
}

/// Builds the [`MediaEnricher`] of decision 82 for the live adapter:
/// the caption provider of [`build_caption_provider`], the global
/// sticker-caption cache (`{data_root}/media.db`, decision 82 (f)),
/// and the media download source (the adapter's
/// [`TeloxideAdapter::media_downloader`], a clone of its Bot handle —
/// Rule A1 keeps every teloxide type inside the adapter crate).
/// `Some` only when BOTH halves are available; `None` keeps the
/// pre-enrichment behavior byte-identical (a media message with no
/// text is skipped). The degrade doctrine mirrors the embedding
/// worker: a missing `OPENAI_API_KEY` (no provider) or a failed
/// `MediaStore::open` logs one WARN and yields `None` — captioning
/// degrades, never a startup failure. `MediaStore::open` is
/// synchronous like the whole store crate, so it runs on the blocking
/// pool (AGENT.md Section 6.2).
///
/// Metrics (specs.md Section 12): the adapter emits the decision-82
/// counters (`captions_total`, `captions_failed_total`,
/// `captions_empty_total`, `sticker_cache_hits_total`,
/// `placeholder_media_total`) as structured tracing fields through
/// this wired path. Surfacing them through `--status` is follow-up
/// work owned by the primary agent.
async fn build_media_enricher(
    setup: &SharedSetup,
    downloader: Arc<dyn tamako_adapter_teloxide::MediaDownloader>,
) -> Option<MediaEnricher> {
    let data_root = setup.store.data_root().to_path_buf();
    let opened = tokio::task::spawn_blocking(move || MediaStore::open(&data_root)).await;
    let media_store = match opened {
        Ok(Ok(store)) => Arc::new(store),
        Ok(Err(error)) => {
            warn!(%error, "media store failed to open; media captioning is disabled for this run");
            return None;
        }
        Err(join_error) => {
            warn!(%join_error, "the media store open task failed; media captioning is disabled for this run");
            return None;
        }
    };
    let caption = build_caption_provider(setup)?;
    Some(MediaEnricher {
        caption,
        media_store,
        downloader,
    })
}

/// Spawns the process-wide embedding worker (decision 66): ONE
/// background interval task drains every group's `pending_embeddings`
/// queue, after a per-group startup reconciliation pass. The provider
/// comes from [`build_embedding_provider`] (shared with the digest
/// pipelines' vector pre-screen, decision 73).
///
/// The groups are enumerated from the group stores on disk (the same
/// Rule P5 enumeration as `--status-all`); each group gets its OWN
/// Store instance because the chat_id-less embedding helpers require
/// exactly one open group per Store (`StoreError::AmbiguousGroup`). A
/// group store that fails to open logs a WARN and is skipped — its
/// actor still works; the group's embeddings wait for the next run. A
/// group first served AFTER startup is picked up on the next restart's
/// reconciliation (the queue rows wait store-side, inspectable).
fn spawn_embedding_worker(
    setup: &SharedSetup,
    provider: Option<Arc<dyn tamako_core::embedding::EmbeddingProvider>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let data_root = setup.store.data_root().to_path_buf();
    let chat_ids = match list_group_chat_ids(&data_root) {
        Ok(chat_ids) => chat_ids,
        Err(error) => {
            warn!(%error, "embedding worker disabled: failed to enumerate the group stores");
            return None;
        }
    };
    let mut targets = Vec::new();
    for chat_id in chat_ids {
        match GroupEmbeddingTarget::open(&data_root, &chat_id) {
            Ok(target) => targets.push(target),
            Err(error) => {
                warn!(chat_id = %chat_id, %error, "embedding worker: the group store failed to open; the group's embeddings wait for the next run");
            }
        }
    }
    EmbeddingWorker::new(provider, Arc::clone(&setup.memory), targets).spawn()
}

/// The run setup shared by both modes: bot configuration, the rendered
/// persona preamble, the persona name (the sender display name of
/// outbound raw-log rows), and the two storage backends.
///
/// Decision 80: `preamble` sits behind an `Arc<RwLock>` — the live
/// persona watcher rewrites it on each accepted reload, so actors
/// spawned after a reload are born current. Both spawn sites read
/// through the lock with the house poison-recovery pattern.
struct SharedSetup {
    bot_config: BotConfig,
    preamble: Arc<RwLock<String>>,
    /// The rendered decision-86 reply suffix body (the `<system>` string
    /// of `tamako_persona::render_suffix`), behind the same hot-reload
    /// lock discipline as `preamble` (decision 80): the persona watcher
    /// rewrites it on each accepted reload, and the reply generator reads
    /// it at request-assembly time. NEVER persisted — the suffix is not
    /// part of the context or the cache anchor (decision 86 (d)). EMPTY
    /// string means no suffix (byte-identical pre-86 behavior).
    suffix: Arc<RwLock<String>>,
    /// The decision-95 pet tag (`tamako_persona::pet_tag_for_name` of
    /// the persona name): ONE slot shared by the gate, reply, warmup,
    /// and recall generators, rewritten by the persona watcher on each
    /// accepted reload. NEVER persisted — the group actors hold their
    /// own copy (`GroupActorParams::pet_tag`) swapped via the
    /// `ReloadPreamble` broadcast.
    pet_tag: Arc<RwLock<String>>,
    bot_name: String,
    store: Arc<Store>,
    memory: Arc<LbugBackend>,
}

/// Builds the shared setup. Rule C4: the preamble is the prefix of every
/// model context; the actor stores it as item 0 of the live context.
fn shared_setup(cli: &Cli) -> Result<SharedSetup> {
    let bot_config = load_bot_config(cli.config.as_deref())?;
    // specs.md Section 5.3 / Rule C4: --live requires a deliberate
    // persona file unless the escape hatch is set. --replay ALWAYS uses
    // the lenient chain, flag or not: offline demos must not require
    // setup.
    let persona = match (&cli.mode, cli.allow_default_persona) {
        (Mode::Live, false) => load_persona_strict(&cli.data_root)?,
        _ => load_persona_with_fallback(&cli.data_root),
    };
    let preamble =
        PetPreambleRenderer.render_preamble_for_mode(&persona, bot_config.global.suffix_mode);
    // Decision 86: the reply suffix body is rendered once here (the
    // startup value of the shared slot) and re-rendered by the persona
    // watcher on each reload. It rides the SAME hot-reload discipline as
    // the preamble but is NEVER persisted and never part of the anchor.
    let suffix = tamako_persona::render_suffix(&persona.suffix);
    // Decision 95: the speech/fence tag derives from the persona name
    // ONCE here; every wake-purpose generator shares the slot.
    let pet_tag = tamako_persona::pet_tag_for_name(&persona.name);
    // Decision 94 (d): the suffix rule count rides the startup line, so
    // a silently empty suffix is one log read away.
    info!(persona = %persona.name, pet_tag = %pet_tag, preamble_len = preamble.len(), suffix_rules = persona.suffix.len(), "persona preamble rendered");
    Ok(SharedSetup {
        bot_config,
        // Decision 80: updatable in live mode; the startup render is the
        // initial value.
        preamble: Arc::new(RwLock::new(preamble)),
        suffix: Arc::new(RwLock::new(suffix)),
        pet_tag: Arc::new(RwLock::new(pet_tag)),
        bot_name: persona.name,
        store: Arc::new(Store::new(cli.data_root.clone())),
        memory: Arc::new(LbugBackend::new(cli.data_root.clone())),
    })
}

/// Dispatches to the selected run mode. The status modes are offline
/// inspection: no persona load, no TELOXIDE_TOKEN, no LLM endpoints, no
/// actor spawn — status NEVER fails for a missing persona file. The
/// merge modes are the offline operator tool of decision 74 and the fact
/// modes the offline operator tool of decision 75: they open one group's
/// store and graph directly (the bot must be STOPPED) and never start
/// the event loop. The run modes share one outbound channel for every
/// actor (Rule A3): the actions carry their chat id, so one pump into
/// the platform adapter is enough.
async fn run(cli: Cli) -> Result<()> {
    match &cli.mode {
        Mode::Status { chat_id } => return run_status(&cli, chat_id),
        Mode::StatusAll => return run_status_all(&cli),
        Mode::MergeTool { chat_id } => return run_merge_tool(&cli, chat_id).await,
        Mode::Merge {
            chat_id,
            loser_id,
            survivor_id,
        } => return run_merge(&cli, chat_id, loser_id, survivor_id).await,
        Mode::MergeRollback { chat_id, audit_id } => {
            return run_merge_rollback(&cli, chat_id, *audit_id).await
        }
        Mode::Facts { chat_id, name } => return run_facts(&cli, chat_id, name).await,
        Mode::Invalidate { chat_id, edge_id } => {
            return run_invalidate(&cli, chat_id, edge_id).await
        }
        Mode::Revalidate { chat_id, edge_id } => {
            return run_revalidate(&cli, chat_id, edge_id).await
        }
        Mode::RelatedPairs { chat_id } => return run_related_pairs(&cli, chat_id).await,
        Mode::DismissRelatedPair { chat_id, pair_id } => {
            return run_dismiss_related_pair(&cli, chat_id, *pair_id).await
        }
        Mode::Replay { .. } | Mode::Live => {}
    }
    let setup = shared_setup(&cli)?;
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel::<OutboundAction>(100);
    match &cli.mode {
        Mode::Replay { fixture } => {
            run_replay(&cli.data_root, &setup, fixture, outbound_tx, outbound_rx).await
        }
        Mode::Live => run_live(&setup, outbound_tx, outbound_rx).await,
        // The offline modes returned above.
        Mode::Status { .. }
        | Mode::StatusAll
        | Mode::MergeTool { .. }
        | Mode::Merge { .. }
        | Mode::MergeRollback { .. }
        | Mode::Facts { .. }
        | Mode::Invalidate { .. }
        | Mode::Revalidate { .. }
        | Mode::RelatedPairs { .. }
        | Mode::DismissRelatedPair { .. } => unreachable!(),
    }
}

/// The `--replay` run. Refer to dev-roadmap.md Section 2.
async fn run_replay(
    data_root: &Path,
    setup: &SharedSetup,
    fixture: &Path,
    outbound_tx: tokio::sync::mpsc::Sender<OutboundAction>,
    mut outbound_rx: tokio::sync::mpsc::Receiver<OutboundAction>,
) -> Result<()> {
    let mut adapter = MockAdapter::from_fixture_path(fixture)
        .with_context(|| format!("failed to load the replay fixture {}", fixture.display()))?;
    let chat_id = adapter.chat_id().to_string();
    info!(chat_id = %chat_id, remaining = adapter.remaining(), "replay fixture loaded");

    let store = Arc::clone(&setup.store);
    let memory = Arc::clone(&setup.memory);
    let group_config = setup.bot_config.for_group(&chat_id);
    // The endpoints, the digest pipeline, and the wake services are
    // built after the chat_id is known and before the actor spawns.
    let endpoints = resolve_endpoints(&group_config)?;
    // Decision 84 (b)/(c): the SAME mint-and-apply as the live spawn
    // site, for uniformity. Replay already writes state through this
    // store (replay convergence is a write-path property), so the
    // local mint breaks no replay discipline; the network-free rule is
    // untouched (the scripted providers never send the headers — the
    // suffix is cosmetic here). First-write-wins persistence keeps the
    // suffix stable across replay runs of one data root.
    let endpoints = {
        let suffix_store = Arc::clone(&store);
        let suffix_chat_id = chat_id.clone();
        tokio::task::spawn_blocking(move || {
            apply_session_suffixes(&suffix_store, &suffix_chat_id, endpoints)
        })
        .await
        .context("the session-affinity suffix mint task failed")??
    };
    // Decision 73: NO embedding provider in replay (same discipline as
    // the decision-66 worker below): replay runs the mock adapter and
    // must stay deterministic and network-free, so the pipeline's
    // vector pre-screen stays inert (Phase 1 resolution behavior).
    let digest = build_digest_pipeline(
        &store,
        &memory,
        &endpoints.digest,
        data_root,
        &chat_id,
        None,
        &group_config,
    )?;
    // Decision 76: NO embedding provider in replay (the decision-73
    // discipline above): the recall stays shallow-only regardless of
    // the `deep_recall` config key — deterministic and network-free.
    let wake = build_wake_services(
        &store,
        &memory,
        &endpoints,
        data_root,
        &chat_id,
        None,
        &group_config,
        // Decision 86: replay carries the suffix too (the persona file is
        // the state; replay reads the startup render). The mock adapter
        // path never sends it.
        Arc::clone(&setup.suffix),
        Arc::clone(&setup.pet_tag),
    )?;
    // The Rule C3 summarizer (decision 62). A missing family API key
    // degrades to the old C3 behavior (drop without a summary) with one
    // startup warning inside `build_summary_provider`.
    let summary_provider = build_summary_provider(&endpoints.summary)?;
    // Decision 66: NO embedding worker in replay. Replay runs the mock
    // adapter and must stay deterministic and network-free; embeddings
    // are a live-mode sidecar (spawn_embedding_worker in run_live).
    // Decision 77 (S1-F7): one startup INFO per group logs the resolved
    // single-value registry.
    info!(chat_id = %chat_id, single_value_registry = %format_single_value_registry(&group_config.single_value_predicates), "single-value registry resolved");
    let handle = spawn_group_actor(GroupActorParams {
        chat_id: chat_id.clone(),
        store: Arc::clone(&store),
        memory,
        config: group_config,
        started_at: OffsetDateTime::now_utc(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        // Rule C4: the rendered preamble seeds item 0 of the live
        // context. Decision 80: read through the shared lock like the
        // live spawn site — but replay never spawns the persona watcher
        // (Rule P1 replay determinism), so it reads the startup value
        // forever.
        preamble: setup
            .preamble
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
        // Decision 95: the speech/fence tag of the startup persona
        // (replay never reloads — the startup value stands).
        pet_tag: setup
            .pet_tag
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
        digest,
        // The actor performs the Rule C3 removal itself (M2); the hook
        // stays a seam for observers that need no actor state.
        post_digest_hook: None,
        wake,
        // Decision 78: NO warmup in replay (the decision-73/76
        // discipline): replay runs the mock adapter and must stay
        // deterministic and network-free. Warmup scheduling is
        // wall-clock host-local and would draw slots against replay
        // wall time, not fixture time. The actor-side test
        // `unwired_warmup_services_are_fully_inert` covers this inert
        // path.
        warmup: None,
        summary_provider,
        outbound: Some(outbound_tx),
        bot_name: Some(setup.bot_name.clone()),
    });

    let mut events_replayed = 0_usize;
    loop {
        tokio::select! {
            result = adapter.next_event() => match result {
                Ok(Some(event)) => {
                    handle
                        .send_event(event)
                        .await
                        .context("the group actor inbox closed during the replay")?;
                    events_replayed += 1;
                }
                Ok(None) => break,
                Err(error) => return Err(error).context("the replay source failed"),
            },
            action = outbound_rx.recv() => match action {
                // The outbound pump: the actor's wake replies execute on
                // the same mock adapter (it records them for the demo).
                // The mock never fails; log and continue if it ever does.
                Some(action) => {
                    if let Err(error) = adapter.execute(action).await {
                        error!(chat_id = %chat_id, %error, "outbound action failed in replay mode");
                    }
                }
                // The local sender is alive for the whole run, so the
                // channel never closes here.
                None => break,
            },
        }
    }

    // The snapshot is a FIFO barrier: when it returns, every replayed
    // event is processed and the session state is persisted.
    let session = handle.snapshot().await.context("the snapshot failed")?;
    // Best-effort drain of the outbound channel: a wake that the last
    // events spawned can still be in flight at the barrier, so this
    // drain is NOT a completeness guarantee for the demo. The
    // integration tests (tamako/tests/wake_replay.rs) assert the wake
    // behavior deterministically.
    while let Ok(action) = outbound_rx.try_recv() {
        if let Err(error) = adapter.execute(action).await {
            error!(chat_id = %chat_id, %error, "outbound action failed in replay mode");
        }
    }
    handle
        .shutdown()
        .await
        .context("the actor reported an error")?;

    // AGENT.md Section 6.2: the synchronous store call runs in spawn_blocking.
    let (rows, dead_letters) = {
        let store = Arc::clone(&store);
        let chat_id = chat_id.clone();
        tokio::task::spawn_blocking(move || {
            let rows = store.list_messages(&chat_id)?;
            let dead_letters = store.list_dead_letters(&chat_id)?;
            Ok::<_, tamako_store::StoreError>((rows, dead_letters))
        })
        .await
        .context("the blocking store task failed to join")?
        .context("failed to read the raw log")?
    };

    let group_dir = data_root.join(&chat_id);
    println!("Replay summary");
    println!("  chat_id:                {chat_id}");
    println!("  events replayed:        {events_replayed}");
    println!("  raw log rows:           {}", rows.len());
    // A boundary above 0 means at least one digest ran.
    println!(
        "  digest boundary msg id: {}",
        session.last_digest_boundary_msg_id
    );
    println!("  dead letters:           {}", dead_letters.len());
    println!("  muted:                  {}", session.muted);
    println!("  consecutive bot msgs:   {}", session.consecutive_bot_msgs);
    println!("  wake msgs since wake:   {}", session.wake.msgs_since_wake);
    println!("  group directory:        {}", group_dir.display());
    Ok(())
}

/// The dead-letter display limit of the status modes: the 5 most recent
/// entries (specs.md Section 10.3).
const STATUS_RECENT_DEAD_LETTERS: usize = 5;

/// The `--status` run: a read-only snapshot of one group store
/// (specs.md Sections 10.3 and 12).
fn run_status(cli: &Cli, chat_id: &str) -> Result<()> {
    // The config supplies the per-group digest_max_retries of the
    // attempts display.
    let bot_config = load_bot_config(cli.config.as_deref())?;
    let status = read_group_status(&cli.data_root, chat_id, STATUS_RECENT_DEAD_LETTERS)
        .map_err(status_read_hint)?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no store.db for group {chat_id} under {} (the group has not been served yet)",
                cli.data_root.display()
            )
        })?;
    let store_path = cli.data_root.join(chat_id).join("store.db");
    print!(
        "{}",
        format_group_status(
            chat_id,
            &store_path,
            &status,
            bot_config.for_group(chat_id).digest_max_retries
        )
    );
    Ok(())
}

/// The group-store enumeration shared by `--status-all` and the
/// embedding-worker wiring: the chat ids of every group store under the
/// data root, sorted by chat id. Rule P5: a group store is a
/// subdirectory that contains store.db. Other files of the data root
/// (persona.toml) are skipped. No stores at all is not an error.
fn list_group_chat_ids(data_root: &Path) -> Result<Vec<String>> {
    let mut chat_ids: Vec<String> = Vec::new();
    if data_root.is_dir() {
        for entry in std::fs::read_dir(data_root)
            .with_context(|| format!("failed to list the data root {}", data_root.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() && path.join("store.db").is_file() {
                chat_ids.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    chat_ids.sort();
    Ok(chat_ids)
}

/// The `--status-all` run: one status block per group store under the
/// data root, sorted by chat id. No stores at all is not an error.
fn run_status_all(cli: &Cli) -> Result<()> {
    let bot_config = load_bot_config(cli.config.as_deref())?;
    let chat_ids = list_group_chat_ids(&cli.data_root)?;
    if chat_ids.is_empty() {
        println!("no group stores under {}", cli.data_root.display());
        return Ok(());
    }
    let mut blocks = Vec::new();
    for chat_id in &chat_ids {
        let status = read_group_status(&cli.data_root, chat_id, STATUS_RECENT_DEAD_LETTERS)
            .map_err(status_read_hint)?
            .expect("the store.db existence was checked above");
        let store_path = cli.data_root.join(chat_id).join("store.db");
        blocks.push(format_group_status(
            chat_id,
            &store_path,
            &status,
            bot_config.for_group(chat_id).digest_max_retries,
        ));
    }
    print!("{}", blocks.join("\n"));
    Ok(())
}

/// Adds the operator hint for the clean-shutdown SQLITE_READONLY case of
/// `read_group_status`: after a clean shutdown the -wal/-shm files are
/// gone, and the first read-only query can fail when the directory is
/// not writable.
fn status_read_hint(error: StoreError) -> anyhow::Error {
    let is_readonly = error.to_string().contains("readonly");
    let error = anyhow::Error::new(error);
    if is_readonly {
        error.context(
            "the bot shut down cleanly and removed the WAL files; the directory must be \
             writable to read a WAL database after a clean shutdown",
        )
    } else {
        error
    }
}

/// Adapts the tamako-agent merge confirmer (decision 74) onto the
/// tamako-core merge seam (the contract-in-core pattern of
/// [`AgentEmbeddingProvider`]: tamako-core cannot depend on
/// tamako-agent, AGENT.md Section 4, so the trait lives in tamako-core
/// and the binary bridges it).
struct AgentMergeConfirmer(EndpointMergeConfirmer);

impl tamako_core::merge::MergeConfirmer for AgentMergeConfirmer {
    fn confirm_merge<'a>(
        &'a self,
        a: &'a MergeNodeInfo,
        b: &'a MergeNodeInfo,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<MergeConfirmation, MergeError>>
                + Send
                + 'a,
        >,
    > {
        let a = tamako_agent::merge_confirm::MergeNode {
            name: a.name.clone(),
            kind: a.kind.as_str().to_string(),
            description: a.description.clone(),
        };
        let b = tamako_agent::merge_confirm::MergeNode {
            name: b.name.clone(),
            kind: b.kind.as_str().to_string(),
            description: b.description.clone(),
        };
        Box::pin(async move {
            let confirmation =
                tamako_agent::merge_confirm::MergeConfirmer::confirm_merge(&self.0, &a, &b)
                    .await
                    .map_err(|error| MergeError::Confirmer(error.to_string()))?;
            let verdict = match confirmation.verdict {
                tamako_agent::merge_confirm::MergeVerdict::Same => MergeVerdict::Same,
                tamako_agent::merge_confirm::MergeVerdict::Related => MergeVerdict::Related,
                tamako_agent::merge_confirm::MergeVerdict::Different => MergeVerdict::Different,
            };
            Ok(MergeConfirmation {
                verdict,
                reason: confirmation.reason,
            })
        })
    }
}

/// The per-group cross-process lock file name (decision 77, H5): one
/// advisory lock file per group, next to the group's store.db and
/// memory.lbug.
const GROUP_LOCK_FILE_NAME: &str = ".tamako.lock";

/// The held per-group cross-process lock (decision 77, H5). The guard
/// releases the OS lock on drop; the CLI modes hold it in their run
/// function for the whole operation, --live holds one guard per served
/// group in a map alongside the actors.
///
/// WHY AN ADVISORY FILE LOCK: the per-group lbug database is NOT
/// cross-process safe. The H5 runtime experiment
/// (tamako/tests/lbug_cross_process.rs, ignored; run it with
/// `cargo test -p tamako --test lbug_cross_process -- --ignored`)
/// opened one group's memory.lbug from a SECOND process while the
/// first process held it: the second process's `Database::new`
/// FAILED LOUDLY with a lock error ("Could not set lock on file ...
/// Resource temporarily unavailable"), so a concurrent --merge against
/// a running bot errors out deep inside the lbug open rather than
/// corrupting the graph — but the failure is late, low-context, and
/// lbug-version-specific. The fd-lock guard turns the same contention
/// into an immediate, deliberate refusal at the CLI boundary (and lets
/// --live declare its groups up front). The store.db side never needed
/// this: it is WAL-mode SQLite with a 2 s busy timeout.
struct GroupLock {
    _guard: fd_lock::RwLockWriteGuard<'static, std::fs::File>,
}

/// Takes the per-group lock (`{data_root}/{chat_id}/.tamako.lock`) in
/// exclusive (write) mode, non-blocking. Contention is a LOUD refusal
/// that names the possibility. The fd-lock guard borrows its lock
/// handle, so the handle is deliberately leaked ('static): the guard
/// lives as long as the operation and the process releases the OS lock
/// on exit anyway.
fn acquire_group_lock(data_root: &Path, chat_id: &str) -> Result<GroupLock> {
    let path = data_root.join(chat_id).join(GROUP_LOCK_FILE_NAME);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("failed to open the group lock file {}", path.display()))?;
    let lock: &'static mut fd_lock::RwLock<std::fs::File> =
        Box::leak(Box::new(fd_lock::RwLock::new(file)));
    match lock.try_write() {
        Ok(guard) => Ok(GroupLock { _guard: guard }),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            anyhow::bail!(
                "the group {chat_id} is locked by another tamako process — is the bot running? \
                 lock held: {}. Stop the bot (or wait for the other tool) and retry.",
                path.display()
            )
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed to lock the group file {}", path.display()))
        }
    }
}

/// Opens the per-group store and graph backend of the merge modes
/// (decision 74) and the fact modes (decision 75): OFFLINE operator
/// tools, one group per run. The mutating modes hold the per-group
/// lock ([`acquire_group_lock`], decision 77 H5) BEFORE this open — a
/// running --live holds the group's lock for its whole lifetime. The
/// store opens ONLY the given group, matching
/// the AmbiguousGroup constraint of the chat_id-less embedding/audit
/// helpers. A group with no store.db OR no memory.lbug is a loud error
/// in the wording of --status (decision 77, S1-F8): `open_group` would
/// CREATE an empty store and mask a mistyped chat id.
fn open_merge_group(data_root: &Path, chat_id: &str) -> Result<(Arc<Store>, LbugBackend)> {
    check_merge_group_exists(data_root, chat_id)?;
    let store = Arc::new(Store::new(data_root.to_path_buf()));
    store
        .open_group(chat_id)
        .with_context(|| format!("failed to open the store of group {chat_id}"))?;
    Ok((store, LbugBackend::new(data_root.to_path_buf())))
}

/// The READ-ONLY variant of [`open_merge_group`] (decision 77, M7) for
/// the inspection paths (--merge-tool dry run, --facts): the store
/// opens through SQLITE_OPEN_READ_ONLY with NO migrations (a read-only
/// connection must never migrate) and the vec0 registration kept
/// (decision 66 rule). A write through this store fails with
/// SQLITE_READONLY — mutating modes use [`open_merge_group`].
fn open_merge_group_read_only(
    data_root: &Path,
    chat_id: &str,
) -> Result<(Arc<Store>, LbugBackend)> {
    check_merge_group_exists(data_root, chat_id)?;
    let store = Arc::new(Store::new(data_root.to_path_buf()));
    store
        .open_group_read_only(chat_id)
        .with_context(|| format!("failed to open the store of group {chat_id} read-only"))?;
    Ok((store, LbugBackend::new(data_root.to_path_buf())))
}

/// The shared existence check of the merge/fact group opens (decision
/// 77, S1-F8): both store.db AND memory.lbug must be present, or the
/// group has not been served yet (a mistyped chat id must never create
/// empty stores or silently read an empty graph).
fn check_merge_group_exists(data_root: &Path, chat_id: &str) -> Result<()> {
    let group_dir = data_root.join(chat_id);
    if !group_dir.join("store.db").is_file() {
        anyhow::bail!(
            "no store.db for group {chat_id} under {} (the group has not been served yet)",
            data_root.display()
        );
    }
    if !group_dir.join("memory.lbug").is_file() {
        anyhow::bail!(
            "no memory.lbug for group {chat_id} under {} (the group has not been served yet; \
             the store exists but the graph was never written)",
            data_root.display()
        );
    }
    Ok(())
}

/// The plan file name of the two-step apply (decision 77, H6a), one
/// per group next to store.db.
const MERGE_PLAN_FILE_NAME: &str = "merge_plan.json";

/// The plan-file format version. A mismatch is a loud refusal.
const MERGE_PLAN_FILE_VERSION: u32 = 1;

/// Decision 77 (H6a): the on-disk shape of the two-step apply. The
/// `--merge-tool` dry run writes the confirmed plan to
/// `{data_root}/{chat_id}/merge_plan.json`; `--apply` REQUIRES the file,
/// re-runs the candidate SCAN (cheap, no LLM — re-running the LLM
/// confirmations to verify staleness would double their cost), and
/// compares [`merge_candidate_set_hash`] of the fresh scan against the
/// stored hash. The hash covers the CANDIDATE SET only (ids + scores,
/// verdict-independent), so the stored actions — verdicts, reasons,
/// survivor/loser choices — are exactly what a matching `--apply`
/// executes: a matching hash certifies the scan the verdicts were
/// confirmed against, and the file's actions are executed rather than a
/// fresh plan. The skipped candidates of the dry run are NOT stored
/// (they carry no action); they ride the hash only.
#[derive(Debug, Serialize, Deserialize)]
struct MergePlanFile {
    version: u32,
    chat_id: String,
    threshold: f64,
    confirmed_by: String,
    candidate_set_hash: String,
    actions: Vec<MergePlanFileAction>,
}

/// One action of [`MergePlanFile`]: the candidate fields plus the
/// verdict, the reason, and the survivor/loser choice. `kind` and
/// `verdict` are the wire strings (NodeType::as_str /
/// MergeVerdict::as_str); unknown values are a loud error at load.
#[derive(Debug, Serialize, Deserialize)]
struct MergePlanFileAction {
    a_id: String,
    b_id: String,
    a_name: String,
    b_name: String,
    a_description: String,
    b_description: String,
    kind: String,
    score: f64,
    verdict: String,
    reason: String,
    survivor_id: String,
    loser_id: String,
}

impl MergePlanFile {
    /// Builds the file of one dry-run plan. The candidate-set hash
    /// covers EVERY scanned candidate (actions and skipped alike).
    fn from_plan(chat_id: &str, threshold: f64, confirmed_by: &str, plan: &MergePlan) -> Self {
        let candidate_set_hash = merge_candidate_set_hash(
            plan.actions
                .iter()
                .map(|action| &action.candidate)
                .chain(plan.skipped.iter().map(|(candidate, _)| candidate)),
        );
        MergePlanFile {
            version: MERGE_PLAN_FILE_VERSION,
            chat_id: chat_id.to_string(),
            threshold,
            confirmed_by: confirmed_by.to_string(),
            candidate_set_hash,
            actions: plan
                .actions
                .iter()
                .map(|action| {
                    let candidate = &action.candidate;
                    MergePlanFileAction {
                        a_id: candidate.a_id.clone(),
                        b_id: candidate.b_id.clone(),
                        a_name: candidate.a_name.clone(),
                        b_name: candidate.b_name.clone(),
                        a_description: candidate.a_description.clone(),
                        b_description: candidate.b_description.clone(),
                        kind: candidate.kind.as_str().to_string(),
                        score: candidate.score,
                        verdict: action.verdict.as_str().to_string(),
                        reason: action.reason.clone(),
                        survivor_id: action.survivor_id.clone(),
                        loser_id: action.loser_id.clone(),
                    }
                })
                .collect(),
        }
    }

    /// Parses the file back into a plan. An unknown verdict or kind
    /// string is a loud error (a hand-edited or corrupt file must not
    /// silently become a different plan).
    fn into_plan(self) -> Result<MergePlan> {
        let mut actions = Vec::with_capacity(self.actions.len());
        for action in self.actions {
            let verdict = MergeVerdict::from_str(&action.verdict).ok_or_else(|| {
                anyhow::anyhow!(
                    "the merge plan file carries an unknown verdict {:?}; re-run the dry run",
                    action.verdict
                )
            })?;
            let kind = NodeType::from_str(&action.kind).ok_or_else(|| {
                anyhow::anyhow!(
                    "the merge plan file carries an unknown node kind {:?}; re-run the dry run",
                    action.kind
                )
            })?;
            actions.push(MergePlanAction {
                candidate: MergeCandidate {
                    a_id: action.a_id,
                    b_id: action.b_id,
                    a_name: action.a_name,
                    b_name: action.b_name,
                    a_description: action.a_description,
                    b_description: action.b_description,
                    kind,
                    score: action.score,
                },
                verdict,
                reason: action.reason,
                survivor_id: action.survivor_id,
                loser_id: action.loser_id,
            });
        }
        Ok(MergePlan {
            actions,
            skipped: Vec::new(),
        })
    }
}

/// The path of the group's merge plan file.
fn merge_plan_file_path(data_root: &Path, chat_id: &str) -> PathBuf {
    data_root.join(chat_id).join(MERGE_PLAN_FILE_NAME)
}

/// Writes the plan file of a dry run (decision 77, H6a). A write
/// failure propagates: without the file the printed plan is not
/// executable by --apply, so the dry run must not pretend otherwise.
fn write_merge_plan_file(
    data_root: &Path,
    chat_id: &str,
    threshold: f64,
    confirmed_by: &str,
    plan: &MergePlan,
) -> Result<PathBuf> {
    let file = MergePlanFile::from_plan(chat_id, threshold, confirmed_by, plan);
    let json = serde_json::to_string_pretty(&file).expect("the plan file struct serializes");
    let path = merge_plan_file_path(data_root, chat_id);
    std::fs::write(&path, format!("{json}\n"))
        .with_context(|| format!("failed to write the merge plan file {}", path.display()))?;
    Ok(path)
}

/// Reads and validates the plan file of a previous dry run (decision
/// 77, H6a). A missing file, an unparseable file, a version mismatch,
/// or a chat-id mismatch is a loud refusal that names the remedy.
fn read_merge_plan_file(data_root: &Path, chat_id: &str) -> Result<MergePlanFile> {
    let path = merge_plan_file_path(data_root, chat_id);
    let text = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "--apply needs the plan file of a previous dry run; none exists at {} \
             — run --merge-tool {chat_id} (the dry run) first",
            path.display()
        )
    })?;
    let file: MergePlanFile = serde_json::from_str(&text).with_context(|| {
        format!(
            "the merge plan file {} is unparseable; re-run the dry run",
            path.display()
        )
    })?;
    if file.version != MERGE_PLAN_FILE_VERSION {
        anyhow::bail!(
            "the merge plan file {} has version {} (this binary writes {}); re-run the dry run",
            path.display(),
            file.version,
            MERGE_PLAN_FILE_VERSION
        );
    }
    if file.chat_id != chat_id {
        anyhow::bail!(
            "the merge plan file {} is for group {}, not {chat_id}; re-run the dry run",
            path.display(),
            file.chat_id
        );
    }
    Ok(file)
}

/// The `--merge-tool` run (decision 74, graph-spec Section 7.7).
/// OFFLINE. The threshold comes from the per-group
/// `merge_candidate_threshold` of the resolved config (default 0.90,
/// decision 104); the confirmation budget from `--max-confirmations`.
///
/// Decision 77 (H6a), the two-step apply: the DRY RUN (default) opens
/// the group's store READ-ONLY (M7, no lock, no migrations), confirms
/// the plan, prints it, and writes `{chat_id}/merge_plan.json`.
/// `--apply` takes the per-group lock (H5), REQUIRES the plan file,
/// re-scans the candidates (no LLM), and refuses loudly when the
/// candidate-set hash changed since the dry run ("plan is stale;
/// re-run the dry run"); a matching hash executes the FILE's actions.
/// The no-key degrade mirrors the per-purpose builders: a missing
/// family API key (`AgentError::ProviderConfig`) prints the scan alone
/// with a note and writes NO plan file; every other confirmer build
/// error propagates.
async fn run_merge_tool(cli: &Cli, chat_id: &str) -> Result<()> {
    let bot_config = load_bot_config(cli.config.as_deref())?;
    let group_config = bot_config.for_group(chat_id);
    let threshold = group_config.merge_candidate_threshold;
    // Decision 77 (S1-F7): the resolved single-value registry is
    // printed once per merge-tool invocation.
    println!(
        "single-value registry: {}",
        format_single_value_registry(&group_config.single_value_predicates)
    );
    if cli.apply {
        return run_merge_tool_apply(cli, chat_id, threshold, &group_config).await;
    }
    let (store, memory) = open_merge_group_read_only(&cli.data_root, chat_id)?;
    let endpoints = resolve_endpoints(&group_config)?;
    let confirmer = match EndpointMergeConfirmer::from_endpoint(&endpoints.digest) {
        Ok(confirmer) => confirmer,
        Err(AgentError::ProviderConfig(error)) => {
            warn!(%error, "merge tool: no provider configuration; confirmations skipped, printing the scan only");
            let candidates = scan_merge_candidates(&store, &memory, chat_id, threshold).await?;
            print!("{}", format_merge_scan(chat_id, threshold, &candidates));
            println!(
                "  note: no LLM key for the digest endpoint; the {n} candidate pair(s) were NOT \
                 confirmed and nothing was planned or written. Set the family API key and re-run.",
                n = candidates.len()
            );
            return Ok(());
        }
        Err(error) => return Err(error).context("failed to build the merge confirmer"),
    };
    let confirmer = AgentMergeConfirmer(confirmer);
    let plan = plan_merges(
        &store,
        &memory,
        chat_id,
        threshold,
        &confirmer,
        cli.max_confirmations,
    )
    .await?;
    let confirmed_by = format!("llm:{}", endpoints.digest.model);
    let plan_path =
        write_merge_plan_file(&cli.data_root, chat_id, threshold, &confirmed_by, &plan)?;
    print!(
        "{}",
        format_merge_plan(chat_id, threshold, cli.max_confirmations, &plan)
    );
    println!(
        "DRY RUN: the plan is written to {}; re-run with --apply to execute it.",
        plan_path.display()
    );
    Ok(())
}

/// The `--merge-tool --apply` run (decision 77, H6a): executes the plan
/// file of a previous dry run after the staleness check. Holds the
/// per-group lock (H5) for the whole run.
async fn run_merge_tool_apply(
    cli: &Cli,
    chat_id: &str,
    threshold: f64,
    group_config: &TriggerConfig,
) -> Result<()> {
    let _lock = acquire_group_lock(&cli.data_root, chat_id)?;
    let (store, memory) = open_merge_group(&cli.data_root, chat_id)?;
    let (plan, confirmed_by) =
        load_applicable_merge_plan(&store, &memory, &cli.data_root, chat_id, threshold).await?;
    // Decision 75 (c): the invariant must hold globally, so the tool
    // path runs it with the resolved per-group registry.
    let report = apply_merge_plan(
        &store,
        &memory,
        chat_id,
        &plan,
        &confirmed_by,
        &group_config.single_value_predicates,
    )
    .await;
    print!(
        "{}",
        format_apply_report(chat_id, &plan, &report, &confirmed_by)
    );
    if !report.failures.is_empty() {
        // Loud exit: the applied actions stand (each carries its audit
        // row), but the operator must see the failure in the exit code.
        // The plan file stays in place so the operator can roll back
        // and re-apply.
        anyhow::bail!(
            "{} merge action(s) failed; the successful actions above applied and carry audit rows",
            report.failures.len()
        );
    }
    // A fully applied plan retires its file: a second --apply of the
    // same file would fail action-by-action (the losers are gone) with
    // a confusing report, so the executed plan is removed.
    std::fs::remove_file(merge_plan_file_path(&cli.data_root, chat_id))
        .context("failed to remove the executed merge plan file")?;
    Ok(())
}

/// Decision 77 (H6a): loads the plan file of a previous dry run and
/// certifies it against a FRESH candidate scan (cheap, no LLM —
/// re-running the LLM confirmations to verify staleness would double
/// their cost). A candidate-set hash mismatch is a loud refusal; the
/// remedy is a fresh dry run (which re-confirms with the LLM). On a
/// match the file's actions are returned verbatim — the apply executes
/// the FILE's plan, not a fresh one. The returned `confirmed_by` is the
/// dry run's attribution (`llm:<model>`), the honest value for an apply
/// that makes no LLM calls.
async fn load_applicable_merge_plan<M: MemoryBackend>(
    store: &Arc<Store>,
    memory: &M,
    data_root: &Path,
    chat_id: &str,
    threshold: f64,
) -> Result<(MergePlan, String)> {
    let plan_file = read_merge_plan_file(data_root, chat_id)?;
    let confirmed_by = plan_file.confirmed_by.clone();
    let current = scan_merge_candidates(store, memory, chat_id, threshold).await?;
    let current_hash = merge_candidate_set_hash(&current);
    if current_hash != plan_file.candidate_set_hash {
        anyhow::bail!(
            "the merge plan is stale; re-run the dry run ({}: the candidate set changed since \
             the dry run — plan hash {}, current scan hash {})",
            merge_plan_file_path(data_root, chat_id).display(),
            plan_file.candidate_set_hash,
            current_hash
        );
    }
    Ok((plan_file.into_plan()?, confirmed_by))
}

/// Renders the resolved single-value registry for the merge-tool
/// output and the startup log (decision 77, S1-F7).
fn format_single_value_registry(registry: &[String]) -> String {
    if registry.is_empty() {
        "(empty)".to_string()
    } else {
        format!("[{}]", registry.join(", "))
    }
}

/// The `--merge` run: the manual, LLM-free form of the merge tool
/// (decision 74 / Section 7.7 step 3: the operator picks the pair AND
/// the survivor, overriding the degree rule). Reuses the core apply
/// path with a one-action 'same' plan so the merge, the sidecar
/// tombstone, and the audit row follow the exact Section 7.7 step 3
/// order. A missing node id (mistyped, or an already-merged loser —
/// Section 7.7 step 5) is a loud error.
async fn run_merge(cli: &Cli, chat_id: &str, loser_id: &str, survivor_id: &str) -> Result<()> {
    if loser_id == survivor_id {
        anyhow::bail!(
            "--merge needs two different node ids (loser and survivor are both '{loser_id}')"
        );
    }
    // Decision 77 (H5): the mutating CLI holds the per-group lock for
    // its whole lifetime; a running bot (or another tool) makes this a
    // loud refusal, not a deep lbug lock error.
    let _lock = acquire_group_lock(&cli.data_root, chat_id)?;
    let (store, memory) = open_merge_group(&cli.data_root, chat_id)?;
    // Decision 75 (c): the invariant must hold globally, so the manual
    // merge path runs it with the resolved per-group registry (the same
    // resolution as --merge-tool).
    let bot_config = load_bot_config(cli.config.as_deref())?;
    let group_config = bot_config.for_group(chat_id);
    // The stored content of both nodes feeds the audit row's loser
    // name/description and the loud unknown-id error.
    let content_of = |node_id: &str| {
        let memory = &memory;
        let node_id = node_id.to_string();
        async move {
            memory
                .node_content(chat_id, &node_id)
                .await
                .with_context(|| format!("failed to read node {node_id} of group {chat_id}"))?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no node {node_id} in group {chat_id} (a mistyped id, or an already-merged node)"
                    )
                })
        }
    };
    let loser = content_of(loser_id).await?;
    let survivor = content_of(survivor_id).await?;
    let kinds = memory
        .node_resolution_infos(chat_id, &[loser_id.to_string(), survivor_id.to_string()])
        .await
        .context("failed to read the node kinds")?;
    let kind_of = |node_id: &str| {
        kinds
            .iter()
            .find(|(id, _)| id == node_id)
            .map(|(_, info)| info.kind)
            .expect("the node content read above proves the node exists")
    };
    let loser_kind = kind_of(loser_id);
    let survivor_kind = kind_of(survivor_id);
    if loser_kind != survivor_kind {
        // Decision 77 (M9): a kind-incompatible pair is a hard error
        // unless the operator forces it; the audit row records the
        // loser kind either way.
        if !cli.force {
            anyhow::bail!(
                "--merge refuses the kind-incompatible pair: {loser_id} is a {} but \
                 {survivor_id} is a {} — pass --force to merge anyway",
                loser_kind.as_str(),
                survivor_kind.as_str()
            );
        }
        warn!(chat_id = %chat_id, loser_id = %loser_id, survivor_id = %survivor_id, "forced manual merge of kind-incompatible nodes; the audit row records the loser kind");
    }
    // MergeCandidate keeps a_id < b_id for a deterministic display
    // order; the operator's loser/survivor choice rides the dedicated
    // fields, not the pair order. The audit row takes the loser
    // name/description from the matching endpoint, so both orders must
    // carry the right content.
    let (a_id, a_name, a_description, b_id, b_name, b_description) = if loser_id < survivor_id {
        (
            loser_id,
            loser.name.clone(),
            loser.description.clone(),
            survivor_id,
            survivor.name.clone(),
            survivor.description.clone(),
        )
    } else {
        (
            survivor_id,
            survivor.name.clone(),
            survivor.description.clone(),
            loser_id,
            loser.name.clone(),
            loser.description.clone(),
        )
    };
    let action = MergePlanAction {
        candidate: MergeCandidate {
            a_id: a_id.to_string(),
            b_id: b_id.to_string(),
            a_name,
            b_name,
            a_description,
            b_description,
            // The audit row records the loser kind. For a
            // kind-incompatible operator merge the loser's kind is the
            // honest value.
            kind: loser_kind,
            // An operator-decided pair has no scan score; the field is
            // display-only here and never persisted.
            score: 1.0,
        },
        verdict: MergeVerdict::Same,
        reason: "operator decision (--merge)".to_string(),
        survivor_id: survivor_id.to_string(),
        loser_id: loser_id.to_string(),
    };
    let plan = MergePlan {
        actions: vec![action],
        skipped: Vec::new(),
    };
    let report = apply_merge_plan(
        &store,
        &memory,
        chat_id,
        &plan,
        "operator",
        &group_config.single_value_predicates,
    )
    .await;
    if let Some(failure) = report.failures.first() {
        anyhow::bail!(
            "the merge of {loser_id} into {survivor_id} in group {chat_id} failed: {}",
            failure.error
        );
    }
    let audit_id = report.audit_ids[0];
    println!("merged \"{}\" ({loser_id}) into \"{}\" ({survivor_id}) in group {chat_id}: audit id {audit_id}", loser.name, survivor.name);
    println!("  roll back with: --merge-rollback {chat_id} {audit_id}");
    Ok(())
}

/// The `--merge-rollback` run: restores one 'same' merge from its audit
/// snapshot (graph-spec Section 7.7 step 4). Refusals (unknown audit
/// id, non-'same' row, already rolled back, no snapshot, a tombstoned
/// survivor) are LOUD: the error propagates and the process exits
/// non-zero. The restored node's vec row is NOT recreated here — the
/// merge deleted it with the done-journal rows, so the next startup
/// reconciliation re-embeds the node automatically (decision 66/74).
async fn run_merge_rollback(cli: &Cli, chat_id: &str, audit_id: i64) -> Result<()> {
    let _lock = acquire_group_lock(&cli.data_root, chat_id)?;
    let (store, memory) = open_merge_group(&cli.data_root, chat_id)?;
    rollback_merge_action(&store, &memory, chat_id, audit_id).await?;
    // Read the row back for the report (decision 77: the point lookup,
    // not a full audit scan). AGENT.md Section 6.2: the synchronous
    // store call runs in spawn_blocking.
    let row = {
        let store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || store.get_merge_audit(audit_id))
            .await
            .context("the blocking store task failed to join")?
            .context("failed to read the merge audit")?
            .expect("the rolled-back audit row exists")
    };
    println!("rolled back merge audit {audit_id} of group {chat_id}:");
    println!(
        "  loser restored:  \"{}\" ({})",
        row.loser_name, row.loser_id
    );
    println!("  survivor kept:   {}", row.survivor_id);
    println!("  the audit row is marked rolled back");
    println!("  the restored node's embedding is rebuilt by the next startup reconciliation");
    Ok(())
}

/// The character cap of one `--facts` description excerpt: longer edge
/// texts are cut at a character boundary and marked with an ellipsis
/// (the full text stays in the graph).
const FACTS_EXCERPT_MAX_CHARS: usize = 60;

/// The `--facts` run (decision 75, graph-spec Section 7.5): the fact
/// listing of one node, resolved through the exact-alias machinery of
/// Section 7.4 step 2. OFFLINE: the bot must be stopped. No LLM needed.
/// Every edge prints with the opaque edge id that `--invalidate` /
/// `--revalidate` take. An unknown name is a loud error (exit 1); a
/// resolved node with zero edges prints an explicit note.
async fn run_facts(cli: &Cli, chat_id: &str, name: &str) -> Result<()> {
    // Decision 77 (M7): the read-only open — no lock, no migrations.
    let (_store, memory) = open_merge_group_read_only(&cli.data_root, chat_id)?;
    let facts = memory
        .node_facts(chat_id, name)
        .await
        .with_context(|| format!("failed to read the facts of \"{name}\" in group {chat_id}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no node named \"{name}\" in group {chat_id} (the entry is the exact alias \
                 match; run --facts with a name the graph knows)"
            )
        })?;
    print!("{}", format_node_facts(&facts));
    Ok(())
}

/// Renders one timestamp of the fact modes as RFC 3339; an
/// unrepresentable value degrades to a marker instead of failing the
/// listing.
fn format_fact_timestamp(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "<invalid timestamp>".to_string())
}

/// Shortens a description excerpt at a character boundary: the first
/// `FACTS_EXCERPT_MAX_CHARS` characters plus an ellipsis. Pure.
fn excerpt_edge_text(text: &str) -> String {
    let char_count = text.chars().count();
    if char_count <= FACTS_EXCERPT_MAX_CHARS {
        return text.to_string();
    }
    let mut excerpt: String = text.chars().take(FACTS_EXCERPT_MAX_CHARS).collect();
    excerpt.push('…');
    excerpt
}

/// Renders the `--facts` listing: one block per edge — the opaque edge
/// id first (the operator copies it), then direction, predicate, the
/// other endpoint, the description excerpt, and the validity
/// timestamps. An edge with `invalid_at` set is marked INVALID. Pure.
fn format_node_facts(facts: &tamako_memory::NodeFacts) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Facts: \"{}\" ({}) in the group graph",
        facts.node_name, facts.node_id
    );
    let _ = writeln!(out, "  edges: {}", facts.edges.len());
    for edge in &facts.edges {
        let direction = if edge.outgoing { "->" } else { "<-" };
        let _ = writeln!(out, "  edge id: {}", edge.edge_id);
        let _ = writeln!(
            out,
            "    {direction} {} \"{}\"",
            edge.relationship_name, edge.other_node_name
        );
        let _ = writeln!(
            out,
            "      text:       {}",
            excerpt_edge_text(&edge.edge_text)
        );
        let _ = writeln!(
            out,
            "      valid_at:   {}",
            format_fact_timestamp(edge.valid_at)
        );
        match edge.invalid_at {
            Some(invalid_at) => {
                let _ = writeln!(
                    out,
                    "      invalid_at: {}  INVALID",
                    format_fact_timestamp(invalid_at)
                );
            }
            None => {
                let _ = writeln!(out, "      invalid_at: (valid)");
            }
        }
    }
    if facts.edges.is_empty() {
        let _ = writeln!(out, "  (no edges: the node has no facts yet)");
    }
    out
}

/// Bumps `facts_invalidated_total` by 1 on the group's store after a
/// SUCCESSFUL manual invalidation (decision 75 (e): the counter covers
/// the digest path, the merge apply path, AND the manual command). Best
/// effort, the same discipline as the pipeline's `bump_counter_by` and
/// the merge apply path: a store failure is one WARN and never fails the
/// command — the invalidation itself already committed. AGENT.md
/// Section 6.2: the synchronous store call runs in spawn_blocking.
async fn bump_facts_invalidated(store: &Arc<Store>, chat_id: &str) {
    let store = Arc::clone(store);
    let chat_id_owned = chat_id.to_string();
    let result = tokio::task::spawn_blocking(move || {
        store.increment_counter(&chat_id_owned, "facts_invalidated_total", 1)
    })
    .await;
    match result {
        Ok(Ok(new_value)) => {
            info!(chat_id = %chat_id, new_value, "facts_invalidated_total incremented (manual invalidation)");
        }
        Ok(Err(error)) => {
            warn!(chat_id = %chat_id, %error, "failed to increment facts_invalidated_total; the invalidation itself succeeded");
        }
        Err(error) => {
            warn!(chat_id = %chat_id, %error, "the blocking store task failed to join; facts_invalidated_total not incremented");
        }
    }
}

/// The shared run of `--invalidate` and `--revalidate` (decision 75,
/// graph-spec Section 7.5). OFFLINE: the bot must be stopped. No LLM
/// needed. The edge id is the opaque string `--facts` prints; a
/// malformed id (bad JSON, a missing field, an unparseable timestamp)
/// or a well-formed id that matches no edge row is a LOUD error — the
/// manual ops never silently target the wrong edge. The result words
/// distinguish the actual mutation from the no-ops (already invalid /
/// already valid).
async fn run_edge_validity(
    cli: &Cli,
    chat_id: &str,
    edge_id: &str,
    invalidate: bool,
) -> Result<()> {
    let _lock = acquire_group_lock(&cli.data_root, chat_id)?;
    let (store, memory) = open_merge_group(&cli.data_root, chat_id)?;
    let now = OffsetDateTime::now_utc();
    let changed = if invalidate {
        memory
            .invalidate_edge(chat_id, edge_id, now)
            .await
            .with_context(|| format!("failed to invalidate the edge in group {chat_id}"))?
    } else {
        memory
            .revalidate_edge(chat_id, edge_id, now)
            .await
            .with_context(|| format!("failed to revalidate the edge in group {chat_id}"))?
    };
    // Decision 75 (e): a SUCCESSFUL manual invalidation feeds the same
    // counter as the digest and merge paths. Revalidation never
    // decrements: the counter counts invalidation events.
    if invalidate && changed {
        bump_facts_invalidated(&store, chat_id).await;
    }
    let verb = match (invalidate, changed) {
        (true, true) => "invalidated",
        (true, false) => "already invalid (no change)",
        (false, true) => "revalidated",
        (false, false) => "already valid (no change)",
    };
    println!("edge {verb}: {edge_id}");
    Ok(())
}

/// The `--invalidate` run: sets `invalid_at` on one edge (decision 75).
async fn run_invalidate(cli: &Cli, chat_id: &str, edge_id: &str) -> Result<()> {
    run_edge_validity(cli, chat_id, edge_id, true).await
}

/// The `--revalidate` run: clears `invalid_at` on one edge — the typo
/// safety net (decision 75).
async fn run_revalidate(cli: &Cli, chat_id: &str, edge_id: &str) -> Result<()> {
    run_edge_validity(cli, chat_id, edge_id, false).await
}

/// The `--related-pairs` run (decision 106 (f)): the dismissal
/// operator's overview of the `related_pairs` side table — every row
/// with its status, endpoint names (hydrated from the graph), reason,
/// and timestamp. OFFLINE read-only (decision 77 M7): no lock, no
/// migrations; the graph open conflicts with a running bot, the same
/// as --facts. No LLM needed.
async fn run_related_pairs(cli: &Cli, chat_id: &str) -> Result<()> {
    let (store, memory) = open_merge_group_read_only(&cli.data_root, chat_id)?;
    let rows = tokio::task::spawn_blocking(move || store.list_related_pairs())
        .await
        .context("the blocking store task failed to join")?
        .with_context(|| format!("failed to read the related pairs of group {chat_id}"))?;
    if rows.is_empty() {
        println!("no related pairs recorded in group {chat_id}");
        return Ok(());
    }
    println!("{} related pair(s) in group {chat_id}:", rows.len());
    for row in rows {
        // An endpoint removed from the graph renders as the raw id:
        // the row stays inspectable. The two resolves serialize (the
        // same backend); the listing is operator-scale, never hot.
        let mut names: Vec<String> = Vec::with_capacity(2);
        for node_id in [&row.node_a_id, &row.node_b_id] {
            let rendered = match memory.node_content(chat_id, node_id).await {
                Ok(Some(content)) => format!("\"{}\"", content.name),
                _ => format!("<gone: {node_id}>"),
            };
            names.push(rendered);
        }
        let (a, b) = (names.remove(0), names.remove(0));
        println!(
            "  #{} [{}] {} ({}) <-> {} ({})\n      reason: {}\n      recorded: {} by {}",
            row.id,
            row.status,
            a,
            row.node_a_id,
            b,
            row.node_b_id,
            row.reason,
            row.created_at,
            row.confirmed_by
        );
    }
    Ok(())
}

/// The `--dismiss-related-pair` run (decision 106 (f)): flips one
/// PENDING row to 'dismissed'. OFFLINE mutating: takes the per-group
/// lock. An unknown id or a row already terminal (promoted/dismissed)
/// is a loud error — the operator never silently targets the wrong
/// row. No LLM needed.
async fn run_dismiss_related_pair(cli: &Cli, chat_id: &str, pair_id: i64) -> Result<()> {
    let _lock = acquire_group_lock(&cli.data_root, chat_id)?;
    let (store, _memory) = open_merge_group(&cli.data_root, chat_id)?;
    let chat_id_owned = chat_id.to_string();
    let changed = tokio::task::spawn_blocking(move || {
        store.set_related_pair_status(&chat_id_owned, pair_id, "dismissed")
    })
    .await
    .context("the blocking store task failed to join")?
    .with_context(|| format!("failed to dismiss related pair {pair_id} in group {chat_id}"))?;
    if changed {
        println!("related pair {pair_id} dismissed (group {chat_id})");
        Ok(())
    } else {
        anyhow::bail!(
            "related pair {pair_id} in group {chat_id} is not pending (unknown id or already \
             terminal); run --related-pairs {chat_id} for the current statuses"
        )
    }
}

/// The display name of one endpoint of a plan action's pair.
fn endpoint_name<'a>(candidate: &'a MergeCandidate, node_id: &str) -> &'a str {
    if candidate.a_id == node_id {
        &candidate.a_name
    } else {
        &candidate.b_name
    }
}

/// Renders one confirmed plan action: pair, score, verdict, effect, and
/// the confirmer's reason. Pure.
fn format_plan_action(action: &MergePlanAction) -> String {
    let candidate = &action.candidate;
    let mut out = String::new();
    let effect = match action.verdict {
        MergeVerdict::Same => format!(
            "\"{}\" ({}) merges into \"{}\" ({})",
            endpoint_name(candidate, &action.loser_id),
            action.loser_id,
            endpoint_name(candidate, &action.survivor_id),
            action.survivor_id
        ),
        MergeVerdict::Related => format!(
            "\"{}\" ({}) --related (no edge)--> \"{}\" ({})",
            endpoint_name(candidate, &action.survivor_id),
            action.survivor_id,
            endpoint_name(candidate, &action.loser_id),
            action.loser_id
        ),
        MergeVerdict::Different => format!(
            "\"{}\" ({}) / \"{}\" ({}) left as-is",
            candidate.a_name, candidate.a_id, candidate.b_name, candidate.b_id
        ),
    };
    let _ = writeln!(
        out,
        "    [{:<9}] score={:.3}  {}  {}",
        action.verdict.as_str(),
        candidate.score,
        candidate.kind.as_str(),
        effect
    );
    let _ = writeln!(out, "               reason: {}", action.reason);
    out
}

/// Renders the `--merge-tool` dry-run plan table (decision 74 point 3):
/// the confirmed actions first, then the skipped candidates with their
/// reasons. Pure.
fn format_merge_plan(
    chat_id: &str,
    threshold: f64,
    max_confirmations: usize,
    plan: &MergePlan,
) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Merge plan: {chat_id} (threshold {threshold}, confirmation budget {max_confirmations})"
    );
    let _ = writeln!(out, "  confirmed actions: {}", plan.actions.len());
    for action in &plan.actions {
        out.push_str(&format_plan_action(action));
    }
    let _ = writeln!(out, "  skipped: {}", plan.skipped.len());
    for (candidate, reason) in &plan.skipped {
        let note = match reason {
            SkipReason::OverConfirmationBudget => {
                "over the confirmation budget (not confirmed)".to_string()
            }
            SkipReason::ConfirmationFailed(error) => format!("confirmation failed: {error}"),
        };
        let _ = writeln!(
            out,
            "    score={:.3}  {} \"{}\" ({}) / \"{}\" ({}): {note}",
            candidate.score,
            candidate.kind.as_str(),
            candidate.a_name,
            candidate.a_id,
            candidate.b_name,
            candidate.b_id
        );
    }
    out
}

/// Renders the no-LLM-key scan output of `--merge-tool`: the candidate
/// pairs above the threshold, nothing confirmed, nothing written. Pure.
fn format_merge_scan(chat_id: &str, threshold: f64, candidates: &[MergeCandidate]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Merge scan: {chat_id} (threshold {threshold})");
    let _ = writeln!(out, "  candidate pairs: {}", candidates.len());
    for candidate in candidates {
        let _ = writeln!(
            out,
            "    score={:.3}  {} \"{}\" ({}) / \"{}\" ({})",
            candidate.score,
            candidate.kind.as_str(),
            candidate.a_name,
            candidate.a_id,
            candidate.b_name,
            candidate.b_id
        );
    }
    out
}

/// Renders the `--apply` outcome: one line per action with its audit
/// id, one per failure, and the summary line. Pure: `report.audit_ids`
/// rides the insertion order of the successful actions
/// ([`tamako_core::merge::ApplyReport`]).
fn format_apply_report(
    chat_id: &str,
    plan: &MergePlan,
    report: &tamako_core::merge::ApplyReport,
    confirmed_by: &str,
) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Merge apply: {chat_id} (confirmed_by {confirmed_by})");
    let mut audit_ids = report.audit_ids.iter();
    for action in &plan.actions {
        let failure = report.failures.iter().find(|failure| {
            failure.loser_id == action.loser_id
                && failure.survivor_id == action.survivor_id
                && failure.verdict == action.verdict
        });
        match failure {
            Some(failure) => {
                let _ = writeln!(
                    out,
                    "    [{:<9}] FAILED  {} -> {}: {}",
                    action.verdict.as_str(),
                    failure.loser_id,
                    failure.survivor_id,
                    failure.error
                );
            }
            None => {
                let audit_id = audit_ids.next().expect("one audit id per applied action");
                let _ = write!(out, "{}", format_plan_action(action));
                let _ = writeln!(out, "               audit id: {audit_id}");
            }
        }
    }
    let _ = writeln!(
        out,
        "  {} action(s): {} applied, {} failed",
        plan.actions.len(),
        report.audit_ids.len(),
        report.failures.len()
    );
    out
}

/// Renders one group status snapshot as aligned text (specs.md Sections
/// 10.3 and 12). Pure: the unit tests assert the exact shape. Counter
/// keys absent from the state table print as 0; the rates print per
/// denominator: the wake rates need wakes_total > 0, the warmup
/// engagement rate (decision 78 (f)) needs warmups_total > 0.
///
/// The per-group capability of specs.md Section 4.2 is NOT printed: it
/// is never persisted by design (current-state.md decision 29) and
/// querying Telegram from an offline inspection tool would be
/// surprising.
fn format_group_status(
    chat_id: &str,
    store_path: &Path,
    status: &GroupStatus,
    digest_max_retries: u32,
) -> String {
    // A state value is a string; a missing or corrupt numeric key
    // reads as 0.
    let counter = |key: &str| -> u64 {
        status
            .state
            .get(key)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let wakes = counter("wakes_total");
    let participations = counter("participations_total");
    let injection_wakes = counter("injection_wakes_total");
    let digest_failures = counter("digest_failures_total");
    let summaries_failed = counter("summaries_failed_total");
    let facts_invalidated = counter("facts_invalidated_total");
    let warmups = counter("warmups_total");
    let warmup_engaged = counter("warmup_engaged_total");
    let dead_letters_counter = counter("dead_letters_total");
    let last_boundary = counter("last_digest_boundary_msg_id");
    let prev_boundary = counter("prev_digest_boundary_msg_id");
    let consecutive = counter("consecutive_bot_msgs");
    let muted = status.state.get("muted_flag").map(String::as_str) == Some("1");

    let mut out = String::new();
    let _ = writeln!(out, "Group status: {chat_id}");
    let _ = writeln!(out, "  {:<29}{}", "store:", store_path.display());
    let _ = writeln!(out, "  counters (specs.md Section 12):");
    let _ = writeln!(out, "    {:<27}{wakes}", "wakes_total:");
    let _ = writeln!(out, "    {:<27}{participations}", "participations_total:");
    let _ = writeln!(out, "    {:<27}{injection_wakes}", "injection_wakes_total:");
    let _ = writeln!(out, "    {:<27}{digest_failures}", "digest_failures_total:");
    let _ = writeln!(
        out,
        "    {:<27}{summaries_failed}",
        "summaries_failed_total:"
    );
    let _ = writeln!(
        out,
        "    {:<27}{facts_invalidated}",
        "facts_invalidated_total:"
    );
    let _ = writeln!(out, "    {:<27}{warmups}", "warmups_total:");
    let _ = writeln!(out, "    {:<27}{warmup_engaged}", "warmup_engaged_total:");
    let _ = writeln!(
        out,
        "    {:<27}{dead_letters_counter}",
        "dead_letters_total:"
    );
    // A rate is meaningful only once its denominator is non-zero: the
    // wake rates need a wake, the warmup engagement rate (specs.md
    // Section 12, decision 78 (f)) needs a warmup — so the rates block
    // opens when EITHER counter moved, and each line keeps its own gate.
    if wakes > 0 || warmups > 0 {
        let _ = writeln!(out, "  rates:");
        if wakes > 0 {
            let participation_rate = participations as f64 * 100.0 / wakes as f64;
            let injection_rate = injection_wakes as f64 * 100.0 / wakes as f64;
            let _ = writeln!(
                out,
                "    {:<27}{participation_rate:.1}% ({participations}/{wakes}) \
                 (healthy band: 30-60%)",
                "participation rate:"
            );
            let _ = writeln!(
                out,
                "    {:<27}{injection_rate:.1}% ({injection_wakes}/{wakes}) \
                 (expected band: 20 to 40%)",
                "injection rate:"
            );
        }
        if warmups > 0 {
            let engagement_rate = warmup_engaged as f64 * 100.0 / warmups as f64;
            let _ = writeln!(
                out,
                "    {:<27}{engagement_rate:.1}% ({warmup_engaged}/{warmups})",
                "warmup engagement rate:"
            );
        }
    }
    let _ = writeln!(out, "  boundaries:");
    let _ = writeln!(
        out,
        "    {:<31}{last_boundary}",
        "last_digest_boundary_msg_id:"
    );
    let _ = writeln!(
        out,
        "    {:<31}{prev_boundary}",
        "prev_digest_boundary_msg_id:"
    );
    let _ = writeln!(out, "  session:");
    let _ = writeln!(out, "    {:<27}{muted}", "muted:");
    let _ = writeln!(out, "    {:<27}{consecutive}", "consecutive_bot_msgs:");
    match status.dead_letter_count {
        0 => {
            let _ = writeln!(out, "  dead letters: 0 total");
        }
        count => {
            let _ = writeln!(out, "  dead letters: {count} total (most recent first)");
            if dead_letters_counter != count {
                // The table count is authoritative; the counter can
                // drift (a manual cleanup, a bug).
                let _ = writeln!(
                    out,
                    "  note: the dead_letters_total counter reads {dead_letters_counter} \
                     but the table holds {count} rows; the table count is shown above"
                );
            }
            for row in &status.recent_dead_letters {
                let at = row
                    .created_at
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| "<invalid timestamp>".to_string());
                // The dead_letter row does not store attempts: a
                // dead-lettered batch BY DEFINITION exhausted
                // digest_max_retries total attempts (specs.md Section
                // 10.3 item 2). The effective per-group value prints.
                let _ = writeln!(
                    out,
                    "    id={}  batch={}  attempts={digest_max_retries} (exhausted)  at {at}",
                    row.id, row.batch_id
                );
                let _ = writeln!(out, "      error: {}", row.error);
            }
        }
    }
    out
}

/// What the bot can do in one group, derived from its membership status
/// (specs.md Section 4.2). Cached in memory per group and re-evaluated
/// on each startup; never persisted, because membership can change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Capability {
    /// Administrator: every feature works, reaction collection included.
    Full,
    /// Member or restricted: everything works except reaction collection.
    NoReactions,
    /// The status could not be determined.
    Indeterminate,
}

/// Maps the membership status of the bot to the group capability.
fn capability_of(status: BotChatStatus) -> Capability {
    match status {
        BotChatStatus::Administrator => Capability::Full,
        BotChatStatus::Member | BotChatStatus::RestrictedOrOther => Capability::NoReactions,
        BotChatStatus::Unknown => Capability::Indeterminate,
    }
}

/// The per-group guidance line for a non-administrator status. `None`
/// for an administrator: full functionality needs no guidance. These
/// texts are the operator contract; the README repeats them verbatim.
fn status_guidance(status: BotChatStatus) -> Option<&'static str> {
    match status {
        BotChatStatus::Administrator => None,
        BotChatStatus::Member | BotChatStatus::RestrictedOrOther => Some(
            "the bot is not an administrator of this group: Telegram delivers reaction updates \
             to administrators only, so reaction collection is OFF for this group. Everything \
             else works normally. To enable reactions, make the bot a group administrator. \
             Note: privacy mode OFF alone suffices for reading all group messages; if privacy \
             mode is still ON (the BotFather default) the bot receives only commands and \
             replies to itself, which is normal platform behavior.",
        ),
        BotChatStatus::Unknown => Some(
            "could not determine the bot's status in this group (the getChatMember call failed \
             — the bot may not be a member yet). Reaction collection needs administrator \
             status; the status is re-checked when the first event of this group arrives.",
        ),
    }
}

/// Logs the capability guidance of one group (specs.md Section 4.2).
/// An administrator logs at info level every time and never warns.
/// A non-administrator status warns once per group per run: `warned`
/// holds the chat ids already warned about. An unknown status logs at
/// info level when the group has no warning yet, but does not consume
/// the warning slot: a later member result still warns.
fn log_chat_status(chat_id: &str, status: BotChatStatus, warned: &mut HashSet<String>) {
    match capability_of(status) {
        Capability::Full => {
            info!(chat_id = %chat_id, "bot is an administrator of this group; full functionality (reaction collection active).");
        }
        Capability::NoReactions => {
            if warned.insert(chat_id.to_string()) {
                let guidance = status_guidance(status).expect("non-admin statuses have guidance");
                warn!(chat_id = %chat_id, "{}", guidance);
            }
        }
        Capability::Indeterminate => {
            if !warned.contains(chat_id) {
                let guidance = status_guidance(status).expect("non-admin statuses have guidance");
                info!(chat_id = %chat_id, "{}", guidance);
            }
        }
    }
}

/// The `--live` run: one group actor per configured group, fed from the
/// Telegram update stream (specs.md Section 4.2).
///
/// Intake tolerance: no code path complains about absent reaction
/// events. For a non-administrator bot the polling stream simply never
/// carries them; the single startup warning is the only notice.
async fn run_live(
    setup: &SharedSetup,
    outbound_tx: tokio::sync::mpsc::Sender<OutboundAction>,
    mut outbound_rx: tokio::sync::mpsc::Receiver<OutboundAction>,
) -> Result<()> {
    // The binary never logs the token.
    let token = std::env::var("TELOXIDE_TOKEN")
        .ok()
        .filter(|token| !token.trim().is_empty())
        .context("set TELOXIDE_TOKEN to the Telegram bot token to run in --live mode")?;
    // Decision 82 (media captioning at intake): the live adapter gets
    // the process-wide MediaEnricher (caption provider + global sticker
    // cache). `None` — a missing OPENAI_API_KEY or a failed media-store
    // open, one WARN each already logged — keeps the pre-enrichment
    // behavior byte-identical: media messages with no text are silently
    // skipped. Replay/mock paths never build an enricher (Rule P1:
    // replay stays deterministic and network-free).
    let adapter = TeloxideAdapter::new(&token)
        .await
        .context("failed to start the Telegram adapter")?;
    // The enricher's download source is the adapter's Bot handle (a
    // cheap clone), handed over as a MediaDownloader so no teloxide
    // type crosses the boundary (Rule A1).
    let downloader = adapter.media_downloader();
    let mut adapter = adapter.with_media_enricher(build_media_enricher(setup, downloader).await);
    let identity = adapter.bot_identity();
    info!(username = %identity.username, id = identity.id, "telegram bot identity resolved");

    // The configured group set: the keys of the [groups.<chat_id>] tables.
    let configured: HashSet<String> = setup.bot_config.overrides.keys().cloned().collect();
    if configured.is_empty() {
        warn!(
            "no [groups.<chat_id>] table in the config file; the bot will ignore every group. \
             The config file is not watched: add a table and restart the bot to serve a group"
        );
    }

    // Capability detection (specs.md Section 4.2): one getChatMember
    // call per configured group. These calls happen once per startup,
    // sequentially, so the pass stays fast and simple. The cache is
    // in-memory only: membership can change, so each startup
    // re-evaluates it. `warned` guarantees one warning per group.
    let mut chat_statuses: HashMap<String, BotChatStatus> = HashMap::new();
    let mut warned: HashSet<String> = HashSet::new();
    for chat_id in &configured {
        let status = adapter.bot_chat_status(chat_id).await;
        chat_statuses.insert(chat_id.clone(), status);
        log_chat_status(chat_id, status, &mut warned);
    }

    let mut actors: HashMap<String, GroupActorHandle> = HashMap::new();

    // Decision 80: the live-mode persona hot-reload watcher (persona
    // watch on {data_root}/persona.toml, debounced, one reload per save
    // burst). The returned watcher MUST stay bound for the loop's
    // lifetime — dropping it stops the watch. `None` (creation failed,
    // one WARN already logged) drops the sender, so the select branch
    // below stays inert. Replay never spawns this watcher (Rule P1).
    let (reload_tx, mut reload_rx) = tokio::sync::mpsc::channel::<persona_watch::ReloadNotice>(4);
    let _persona_watcher = persona_watch::spawn_persona_watcher(
        setup.store.data_root().to_path_buf(),
        setup
            .preamble
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
        setup
            .suffix
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
        setup.bot_config.global.suffix_mode,
        reload_tx,
    );
    // Decision 77 (H5): one held group lock per served group, dropped
    // (releasing the OS locks) at the end of the run.
    let mut group_locks: HashMap<String, GroupLock> = HashMap::new();
    // Chat ids already logged as non-configured; the message logs once each.
    let mut logged_skips: HashSet<String> = HashSet::new();
    let mut events_routed = 0_usize;
    // A fatal error to surface after the shutdown flush below.
    let mut fatal: Option<anyhow::Error> = None;

    // The process-wide embedding worker (decision 66), a live-mode
    // sidecar: one interval task drains every group's embedding queue
    // after the startup reconciliation pass. REPLAY MODE spawns no
    // worker: replay runs the mock adapter and must stay deterministic
    // and network-free; embeddings are a live-mode sidecar. The handle
    // is kept only for readability — dropping it detaches, never
    // cancels, the task. The provider instance is SHARED with the
    // digest pipelines' decision-73 vector pre-screen (ONE Arc per
    // process, built once).
    let embedding_provider = build_embedding_provider(setup);
    let _embedding_worker = spawn_embedding_worker(setup, embedding_provider.clone());

    loop {
        tokio::select! {
            result = adapter.next_group_event() => match result {
                Ok(Some(GroupEvent { chat_id, event })) => {
                    // Rule P5: nothing crosses groups. Events of a group that
                    // is not configured never touch storage.
                    if !configured.contains(&chat_id) {
                        if logged_skips.insert(chat_id.clone()) {
                            info!(chat_id = %chat_id, "ignoring events from a non-configured group");
                        }
                        continue;
                    }
                    // Lazy spawn: the actor starts on the first event of the
                    // group, with the same params shape as the replay.
                    let handle = match actors.entry(chat_id.clone()) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            // Decision 77 (H5): take the per-group lock
                            // BEFORE the group is served; the guard is
                            // held in `group_locks` for the process
                            // lifetime. Contention means another tamako
                            // process serves or edits this group. The
                            // house-consistent choice (the same as every
                            // other spawn-time failure in this branch —
                            // and H4c's split-brain paranoia): fail the
                            // RUN loudly; the supervisor restarts the
                            // process.
                            let lock = match acquire_group_lock(setup.store.data_root(), &chat_id) {
                                Ok(lock) => lock,
                                Err(error) => {
                                    error!(chat_id = %chat_id, %error, "the group lock is held by another process; this group cannot be served");
                                    fatal = Some(error);
                                    break;
                                }
                            };
                            group_locks.insert(chat_id.clone(), lock);
                            // Spawn-time re-check: a missing or unknown
                            // cached status (example: the bot joined the
                            // group after startup) is re-queried now.
                            // The one-warning-per-group rule still holds.
                            let cached = chat_statuses.get(&chat_id).copied();
                            if cached.is_none() || cached == Some(BotChatStatus::Unknown) {
                                let status = adapter.bot_chat_status(&chat_id).await;
                                chat_statuses.insert(chat_id.clone(), status);
                                log_chat_status(&chat_id, status, &mut warned);
                            }
                            let group_config = setup.bot_config.for_group(&chat_id);
                            let endpoints = match resolve_endpoints(&group_config) {
                                Ok(endpoints) => endpoints,
                                Err(error) => {
                                    fatal = Some(error);
                                    break;
                                }
                            };
                            // Decision 84 (b)/(c): mint the per-(group,
                            // purpose) session-affinity suffixes at the
                            // per-group build site (the group lock is
                            // already held) and join them onto the
                            // resolved prefixes. The synchronous store
                            // mints ride the blocking pool (AGENT.md
                            // Section 6.2); a mint failure is fatal,
                            // the same standing as the resolve error
                            // above.
                            let suffix_store = Arc::clone(&setup.store);
                            let suffix_chat_id = chat_id.clone();
                            let endpoints = match tokio::task::spawn_blocking(move || {
                                apply_session_suffixes(&suffix_store, &suffix_chat_id, endpoints)
                            })
                            .await
                            {
                                Ok(Ok(endpoints)) => endpoints,
                                Ok(Err(error)) => {
                                    fatal = Some(error);
                                    break;
                                }
                                Err(join_error) => {
                                    fatal = Some(anyhow::Error::new(join_error).context(
                                        "the session-affinity suffix mint task failed",
                                    ));
                                    break;
                                }
                            };
                            let digest =
                                match build_digest_pipeline(&setup.store, &setup.memory, &endpoints.digest, setup.store.data_root(), &chat_id, embedding_provider.clone(), &group_config) {
                                    Ok(digest) => digest,
                                    Err(error) => {
                                        fatal = Some(error);
                                        break;
                                    }
                                };
                            let wake = match build_wake_services(
                                &setup.store,
                                &setup.memory,
                                &endpoints,
                                setup.store.data_root(),
                                &chat_id,
                                // Decision 76: the shared provider Arc (ONE
                                // per process, the same instance the worker
                                // and the digest pre-screen use). `None`
                                // (no OPENAI_API_KEY) keeps the recall
                                // shallow-only.
                                embedding_provider.clone(),
                                &group_config,
                                // Decision 86: the shared rendered-suffix
                                // slot, hot-reloaded by the persona watcher.
                                Arc::clone(&setup.suffix),
                                Arc::clone(&setup.pet_tag),
                            ) {
                                Ok(wake) => wake,
                                Err(error) => {
                                    fatal = Some(error);
                                    break;
                                }
                            };
                            // Decision 78: the warmup trigger rides the
                            // SAME per-group resolved endpoints the wake
                            // build uses (the warmup is a reply-purpose
                            // call); the degrade doctrine mirrors
                            // build_wake_services (no provider key, no
                            // warmup — the actor stays inert).
                            let warmup =
                                match build_warmup_services(&endpoints, Arc::clone(&setup.pet_tag))
                                {
                                Ok(warmup) => warmup,
                                Err(error) => {
                                    fatal = Some(error);
                                    break;
                                }
                            };
                            // The Rule C3 summarizer (decision 62), as
                            // in the replay path.
                            let summary_provider =
                                match build_summary_provider(&endpoints.summary) {
                                    Ok(summary) => summary,
                                    Err(error) => {
                                        fatal = Some(error);
                                        break;
                                    }
                                };
                            info!(chat_id = %chat_id, "first event of a configured group; spawning the actor");
                            // Decision 77 (S1-F7): one startup INFO per
                            // group logs the resolved single-value
                            // registry.
                            info!(chat_id = %chat_id, single_value_registry = %format_single_value_registry(&group_config.single_value_predicates), "single-value registry resolved");
                            entry.insert(spawn_group_actor(GroupActorParams {
                                chat_id: chat_id.clone(),
                                store: Arc::clone(&setup.store),
                                memory: Arc::clone(&setup.memory),
                                config: group_config,
                                started_at: OffsetDateTime::now_utc(),
                                inbox_capacity: DEFAULT_INBOX_CAPACITY,
                                // Rule C4: the rendered preamble seeds item 0
                                // of the live context. Decision 80: read
                                // through the shared lock at spawn time — an
                                // actor spawned after a persona reload is
                                // born current.
                                preamble: setup
                                    .preamble
                                    .read()
                                    .unwrap_or_else(PoisonError::into_inner)
                                    .clone(),
                                // Decision 95: the CURRENT pet tag — an
                                // actor spawned after a persona reload is
                                // born with the renamed tag.
                                pet_tag: setup
                                    .pet_tag
                                    .read()
                                    .unwrap_or_else(PoisonError::into_inner)
                                    .clone(),
                                digest,
                                post_digest_hook: None,
                                wake,
                                warmup,
                                summary_provider,
                                outbound: Some(outbound_tx.clone()),
                                bot_name: Some(setup.bot_name.clone()),
                            }))
                        }
                    };
                    // A send failure means the actor died. Do not drop the
                    // event silently: stop the loop, flush, and surface it.
                    if let Err(error) = handle.send_event(event).await {
                        fatal = Some(anyhow::Error::new(error)
                            .context(format!("the actor of group {chat_id} died; its inbox closed")));
                        break;
                    }
                    events_routed += 1;
                }
                Ok(None) => {
                    // H4c (decision 65): loud fail-fast. A dead polling
                    // stream is NOT a clean shutdown — the process is
                    // supervisor-managed and an exit 0 defeats
                    // restart-on-failure, so the loop surfaces a fatal
                    // error and main exits non-zero. Respawn is rejected:
                    // two pollers of one group risk split-brain dual
                    // state, so the supervisor restarts the process
                    // instead.
                    error!("the telegram update stream ended; exiting non-zero so the supervisor restarts the bot");
                    fatal = Some(anyhow::anyhow!(
                        "the telegram update stream ended; the bot receives no events until restarted"
                    ));
                    break;
                }
                Err(error) => {
                    // The adapter already skips transient stream errors
                    // internally. An escaping Err is a fatal channel issue;
                    // a continue could spin hot. Fail loud like the
                    // dead-stream path (H4c): a silent exit 0 would defeat
                    // the supervisor's restart-on-failure.
                    error!(%error, "fatal telegram adapter error; stopping the event loop");
                    fatal = Some(
                        anyhow::Error::new(error).context("the telegram adapter stream failed")
                    );
                    break;
                }
            },
            action = outbound_rx.recv() => match action {
                Some(action) => {
                    // specs.md Section 4.2: outbound failures are
                    // tolerated. Warn with the chat id and continue;
                    // never fatal. The raw-log row of the reply is
                    // already persisted actor-side (Rules B1/P1).
                    let action_chat_id = match &action {
                        OutboundAction::SendText { chat_id, .. }
                        | OutboundAction::SendMedia { chat_id, .. }
                        | OutboundAction::React { chat_id, .. } => chat_id.clone(),
                    };
                    if let Err(error) = adapter.execute(action).await {
                        warn!(chat_id = %action_chat_id, %error, "outbound action failed; continuing");
                    }
                }
                // The local sender is alive for the whole run, so the
                // channel never closes here.
                None => break,
            },
            // Decision 80: one applied persona reload. The pattern
            // disables the branch when the watcher never started (the
            // sender dropped at spawn) or exited (shutdown).
            Some(notice) = reload_rx.recv() => {
                // The shared preamble FIRST: actors spawned after this
                // reload are born current (decision 80 (c)).
                *setup
                    .preamble
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = notice.preamble.clone();
                // Decision 86 (h): the reply suffix slot reloads with the
                // SAME broadcast. The reply generator reads it at the next
                // request-assembly; the suffix is never persisted and never
                // part of the context, so this invalidates no cache anchor.
                *setup
                    .suffix
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = notice.suffix.clone();
                // Decision 95: the shared pet-tag slot follows the name
                // of the reloaded persona. Unconditional (cheap and
                // idempotent): a tag change always travels with a
                // preamble change (the name is rendered into it), so the
                // broadcast below carries it to the actors.
                let pet_tag = tamako_persona::pet_tag_for_name(&notice.persona_name);
                *setup
                    .pet_tag
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = pet_tag.clone();
                if notice.preamble_changed {
                    let skipped =
                        persona_watch::broadcast_preamble(&actors, &notice.preamble, &pet_tag);
                    // ONE curated INFO line per applied reload (decision-53
                    // addition, the decision-78 warmup line's class): a
                    // deliberate operator event is exactly the startup-class
                    // kind. The fields mirror the startup "persona preamble
                    // rendered" line and add the skip count.
                    info!(persona = %notice.persona_name, preamble_len = notice.preamble.len(), suffix_rules = notice.suffix_rules, skipped = skipped.len(), "persona preamble reloaded");
                } else {
                    // Decision 94 (b): a suffix-only reload rewrites the
                    // slot WITHOUT the broadcast — the suffix never
                    // enters the preamble (decision 86 (d)), so the
                    // actors' context item 0 is untouched and no cache
                    // anchor moves.
                    info!(persona = %notice.persona_name, suffix_rules = notice.suffix_rules, "persona suffix reloaded");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received; shutting down");
                break;
            }
        }
    }

    let groups_served = actors.len();
    // The shutdown is the session-state flush: the actor persists the
    // session after every mutation, and shutdown lets the FIFO drain and
    // surfaces task errors. A failed shutdown logs at error level; the
    // other actors still shut down.
    let mut shutdown_failures = 0_usize;
    for (chat_id, handle) in actors {
        if let Err(error) = handle.shutdown().await {
            error!(chat_id = %chat_id, %error, "group actor shutdown failed");
            shutdown_failures += 1;
        }
    }
    info!(
        groups_served,
        events_routed, shutdown_failures, "live run summary"
    );

    if let Some(error) = fatal {
        return Err(error);
    }
    // H4c (decision 65): actor task failures surfaced at shutdown are
    // not a clean exit either — fail loud so the supervisor restarts
    // the bot. Mid-run actor death is already fatal at the send site
    // (the inbox closed); a dedicated actor-death watch channel does
    // not exist and is a documented follow-up.
    if shutdown_failures > 0 {
        return Err(anyhow::anyhow!(
            "{shutdown_failures} group actor(s) reported an error at shutdown"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the tests that mutate RUST_LOG. Env mutation is
    /// process-global; the lock keeps the tests hermetic against each
    /// other (the same pattern as tamako-agent's endpoint tests).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Saves and restores RUST_LOG. Hermetic env handling.
    struct RustLogGuard(Option<String>);

    impl RustLogGuard {
        /// Locks the env, removes RUST_LOG, and returns the lock guard
        /// together with the restore guard.
        fn cleared() -> (std::sync::MutexGuard<'static, ()>, Self) {
            let lock = ENV_LOCK.lock().unwrap();
            let guard = RustLogGuard(std::env::var("RUST_LOG").ok());
            std::env::remove_var("RUST_LOG");
            (lock, guard)
        }
    }

    impl Drop for RustLogGuard {
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => std::env::set_var("RUST_LOG", value),
                None => std::env::remove_var("RUST_LOG"),
            }
        }
    }

    /// Parses a fixed argument list.
    fn parse(args: &[&str]) -> std::result::Result<ParseOutcome, String> {
        parse_args(args.iter().map(|arg| (*arg).to_string()))
    }

    #[test]
    fn help_flag_short_and_long() {
        assert!(matches!(parse(&["--help"]), Ok(ParseOutcome::Help)));
        assert!(matches!(parse(&["-h"]), Ok(ParseOutcome::Help)));
    }

    #[test]
    fn replay_only() {
        let outcome = parse(&["--replay", "fixture.json"]).expect("a valid replay command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::Replay { fixture } => assert_eq!(fixture, PathBuf::from("fixture.json")),
            _ => panic!("expected the replay mode"),
        }
        assert_eq!(cli.data_root, PathBuf::from("./data"));
        assert!(cli.config.is_none());
        assert!(!cli.allow_default_persona);
    }

    #[test]
    fn live_only() {
        let outcome = parse(&["--live"]).expect("a valid live command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::Live));
        assert_eq!(cli.data_root, PathBuf::from("./data"));
        assert!(!cli.allow_default_persona);
    }

    #[test]
    fn live_with_allow_default_persona() {
        // The escape hatch is accepted in any mode; it only affects
        // --live.
        let outcome = parse(&["--live", "--allow-default-persona"])
            .expect("a valid live command line with the escape hatch");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::Live));
        assert!(cli.allow_default_persona);
    }

    #[test]
    fn verbose_defaults_to_false_in_every_mode() {
        for args in [
            &["--replay", "fixture.json"][..],
            &["--live"][..],
            &["--status", "-1001"][..],
            &["--status-all"][..],
            &["--merge-tool", "-1001"][..],
            &["--merge", "-1001", "a", "b"][..],
            &["--merge-rollback", "-1001", "7"][..],
            &["--facts", "-1001", "Tama"][..],
            &["--invalidate", "-1001", "edge"][..],
            &["--revalidate", "-1001", "edge"][..],
        ] {
            let ParseOutcome::Run(cli) = parse(args).expect("a valid command line") else {
                panic!("expected the Run outcome");
            };
            assert!(!cli.verbose, "args: {args:?}");
        }
    }

    #[test]
    fn verbose_short_and_long_in_every_mode() {
        // Like --allow-default-persona, the flag is accepted in any
        // mode; both spellings set it.
        for flag in ["-v", "--verbose"] {
            for args in [
                vec!["--replay", "fixture.json"],
                vec!["--live"],
                vec!["--status", "-1001"],
                vec!["--status-all"],
            ] {
                let mut args = args;
                args.push(flag);
                let ParseOutcome::Run(cli) = parse(&args).expect("a valid command line") else {
                    panic!("expected the Run outcome");
                };
                assert!(cli.verbose, "args: {args:?}");
            }
        }
    }

    #[test]
    fn verbose_combines_with_other_flags() {
        let ParseOutcome::Run(cli) =
            parse(&["--replay", "fixture.json", "-v"]).expect("a valid command line")
        else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::Replay { .. }));
        assert!(cli.verbose);

        let ParseOutcome::Run(cli) = parse(&[
            "--live",
            "--verbose",
            "--allow-default-persona",
            "--data-root",
            "/tmp/tamako",
        ])
        .expect("a valid command line") else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::Live));
        assert!(cli.verbose);
        assert!(cli.allow_default_persona);
        assert_eq!(cli.data_root, PathBuf::from("/tmp/tamako"));
    }

    #[test]
    fn log_filter_defaults_to_info() {
        let (_lock, _guard) = RustLogGuard::cleared();
        // EnvFilter renders its directive string verbatim.
        assert_eq!(log_filter(false).to_string(), "info");
    }

    #[test]
    fn deep_recall_store_opens_a_single_group_store() {
        // Decision 76: the dedicated store opens the one group of the
        // recall, so the chat_id-less deep-source helpers (the KNN and
        // edge_texts reads) accept it.
        let dir = tempfile::tempdir().expect("a temporary data root");
        let store = deep_recall_store(dir.path(), "-1001").expect("the store opens");
        assert_eq!(
            store.list_edge_text_ids().expect("a single-group read"),
            Vec::<String>::new(),
            "the single-open-group contract holds on the dedicated store"
        );
    }

    #[test]
    fn deep_recall_store_degrades_to_none_on_a_failed_open() {
        // Decision 76: a failed open degrades the recall ALONE to
        // shallow-only (None) — never a startup failure. A data root
        // that is a FILE makes the per-group create_dir_all fail.
        let dir = tempfile::tempdir().expect("a temporary directory");
        let file_root = dir.path().join("blocker");
        std::fs::write(&file_root, b"not a directory").expect("the blocker file");
        assert!(deep_recall_store(&file_root, "-1001").is_none());
    }

    #[test]
    fn log_filter_verbose_targets_the_tamako_crates_at_debug() {
        let (_lock, _guard) = RustLogGuard::cleared();
        assert_eq!(log_filter(true).to_string(), "tamako=debug");
    }

    #[test]
    fn log_filter_rust_log_always_wins() {
        let (_lock, _guard) = RustLogGuard::cleared();
        std::env::set_var("RUST_LOG", "tamako=trace,teloxide=info");
        // Regardless of the flag, the RUST_LOG directive is used. Note:
        // EnvFilter reorders the directives of a multi-directive value
        // on display (it sorts by target specificity); the semantics
        // are unchanged.
        let expected = "teloxide=info,tamako=trace";
        assert_eq!(log_filter(false).to_string(), expected);
        assert_eq!(log_filter(true).to_string(), expected);
    }

    #[test]
    fn status_only() {
        let outcome = parse(&["--status", "-1001234567890"]).expect("a valid status command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::Status { chat_id } => assert_eq!(chat_id, "-1001234567890"),
            _ => panic!("expected the status mode"),
        }
        assert_eq!(cli.data_root, PathBuf::from("./data"));
    }

    #[test]
    fn status_all_only() {
        let outcome = parse(&["--status-all"]).expect("a valid status-all command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::StatusAll));
    }

    #[test]
    fn status_needs_a_value() {
        let error = parse(&["--status"]).expect_err("a missing --status value must fail");
        assert!(error.contains("--status"));
    }

    #[test]
    fn merge_tool_parses_with_the_dry_run_defaults() {
        // Decision 74: --merge-tool is a DRY RUN without --apply; the
        // confirmation budget defaults to 50.
        let outcome = parse(&["--merge-tool", "-1001"]).expect("a valid merge-tool command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::MergeTool { chat_id } => assert_eq!(chat_id, "-1001"),
            _ => panic!("expected the merge-tool mode"),
        }
        assert!(!cli.apply);
        assert_eq!(cli.max_confirmations, DEFAULT_MAX_CONFIRMATIONS);
        assert_eq!(cli.data_root, PathBuf::from("./data"));
    }

    #[test]
    fn merge_tool_apply_and_max_confirmations_parse() {
        let outcome = parse(&[
            "--merge-tool",
            "-1001",
            "--apply",
            "--max-confirmations",
            "5",
        ])
        .expect("a valid merge-tool command line with --apply");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::MergeTool { .. }));
        assert!(cli.apply);
        assert_eq!(cli.max_confirmations, 5);
    }

    #[test]
    fn merge_tool_needs_a_value() {
        let error = parse(&["--merge-tool"]).expect_err("a missing --merge-tool value must fail");
        assert!(error.contains("--merge-tool"));
    }

    #[test]
    fn merge_parses_chat_loser_survivor() {
        let outcome =
            parse(&["--merge", "-1001", "loser-id", "survivor-id"]).expect("a valid --merge line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::Merge {
                chat_id,
                loser_id,
                survivor_id,
            } => {
                assert_eq!(chat_id, "-1001");
                assert_eq!(loser_id, "loser-id");
                assert_eq!(survivor_id, "survivor-id");
            }
            _ => panic!("expected the merge mode"),
        }
    }

    #[test]
    fn merge_needs_three_values() {
        for args in [
            &["--merge"][..],
            &["--merge", "-1001"][..],
            &["--merge", "-1001", "loser-id"][..],
        ] {
            let error = parse(args).expect_err("an incomplete --merge must fail");
            assert!(
                error.contains("--merge"),
                "args: {args:?}, message: {error}"
            );
        }
    }

    #[test]
    fn merge_rollback_parses_chat_and_audit_id() {
        let outcome =
            parse(&["--merge-rollback", "-1001", "7"]).expect("a valid --merge-rollback line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::MergeRollback { chat_id, audit_id } => {
                assert_eq!(chat_id, "-1001");
                assert_eq!(audit_id, 7);
            }
            _ => panic!("expected the merge-rollback mode"),
        }
    }

    #[test]
    fn merge_rollback_needs_two_values() {
        for args in [
            &["--merge-rollback"][..],
            &["--merge-rollback", "-1001"][..],
        ] {
            let error = parse(args).expect_err("an incomplete --merge-rollback must fail");
            assert!(
                error.contains("--merge-rollback"),
                "args: {args:?}, message: {error}"
            );
        }
    }

    #[test]
    fn merge_rollback_audit_id_must_be_an_integer() {
        let error = parse(&["--merge-rollback", "-1001", "abc"])
            .expect_err("a non-integer audit id must fail");
        assert!(error.contains("audit id"), "message: {error}");
        assert!(error.contains("abc"), "message: {error}");
    }

    #[test]
    fn facts_parses_chat_and_name() {
        let outcome = parse(&["--facts", "-1001", "Tama"]).expect("a valid --facts command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::Facts { chat_id, name } => {
                assert_eq!(chat_id, "-1001");
                assert_eq!(name, "Tama");
            }
            _ => panic!("expected the facts mode"),
        }
        assert_eq!(cli.data_root, PathBuf::from("./data"));
    }

    #[test]
    fn facts_needs_two_values() {
        for args in [&["--facts"][..], &["--facts", "-1001"][..]] {
            let error = parse(args).expect_err("an incomplete --facts must fail");
            assert!(
                error.contains("--facts"),
                "args: {args:?}, message: {error}"
            );
        }
    }

    #[test]
    fn invalidate_parses_chat_and_edge_id() {
        let outcome = parse(&["--invalidate", "-1001", "{\"source_id\":\"a\"}"])
            .expect("a valid --invalidate command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::Invalidate { chat_id, edge_id } => {
                assert_eq!(chat_id, "-1001");
                assert_eq!(edge_id, "{\"source_id\":\"a\"}");
            }
            _ => panic!("expected the invalidate mode"),
        }
    }

    #[test]
    fn revalidate_parses_chat_and_edge_id() {
        let outcome = parse(&["--revalidate", "-1001", "edge-key"])
            .expect("a valid --revalidate command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::Revalidate { chat_id, edge_id } => {
                assert_eq!(chat_id, "-1001");
                assert_eq!(edge_id, "edge-key");
            }
            _ => panic!("expected the revalidate mode"),
        }
    }

    #[test]
    fn related_pairs_parses_chat_id() {
        let outcome =
            parse(&["--related-pairs", "-1001"]).expect("a valid --related-pairs command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::RelatedPairs { chat_id } => assert_eq!(chat_id, "-1001"),
            _ => panic!("expected the related-pairs mode"),
        }
    }

    #[test]
    fn related_pairs_needs_one_value() {
        let error =
            parse(&["--related-pairs"]).expect_err("an incomplete --related-pairs must fail");
        assert!(error.contains("--related-pairs"), "message: {error}");
    }

    #[test]
    fn dismiss_related_pair_parses_chat_and_pair_id() {
        let outcome = parse(&["--dismiss-related-pair", "-1001", "7"])
            .expect("a valid --dismiss-related-pair command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        match cli.mode {
            Mode::DismissRelatedPair { chat_id, pair_id } => {
                assert_eq!(chat_id, "-1001");
                assert_eq!(pair_id, 7);
            }
            _ => panic!("expected the dismiss-related-pair mode"),
        }
    }

    #[test]
    fn dismiss_related_pair_needs_two_values() {
        for args in [
            &["--dismiss-related-pair"][..],
            &["--dismiss-related-pair", "-1001"][..],
        ] {
            let error = parse(args).expect_err("an incomplete --dismiss-related-pair must fail");
            assert!(
                error.contains("--dismiss-related-pair"),
                "args: {args:?}, message: {error}"
            );
        }
    }

    #[test]
    fn dismiss_related_pair_pair_id_must_be_an_integer() {
        let error = parse(&["--dismiss-related-pair", "-1001", "abc"])
            .expect_err("a non-integer pair id must fail");
        assert!(error.contains("pair id"), "message: {error}");
        assert!(error.contains("abc"), "message: {error}");
    }

    #[test]
    fn invalidate_and_revalidate_need_two_values() {
        for args in [
            &["--invalidate"][..],
            &["--invalidate", "-1001"][..],
            &["--revalidate"][..],
            &["--revalidate", "-1001"][..],
        ] {
            let error = parse(args).expect_err("an incomplete edge command must fail");
            assert!(
                error.contains("--invalidate") || error.contains("--revalidate"),
                "args: {args:?}, message: {error}"
            );
        }
    }

    #[test]
    fn edge_id_is_opaque_at_parse_time() {
        // Decision 75: the edge id is an opaque string (a compact JSON
        // of the edge's natural key). The PARSE accepts any value —
        // garbage included — and the RUNTIME errors loudly on a
        // malformed or unknown id. So garbage parses fine here.
        for flag in ["--invalidate", "--revalidate"] {
            for edge_id in ["not-json", "{}", "12345", "{\"source_id\":\"a\"}"] {
                let outcome =
                    parse(&[flag, "-1001", edge_id]).expect("the parse accepts any edge-id string");
                let ParseOutcome::Run(cli) = outcome else {
                    panic!("expected the Run outcome");
                };
                match cli.mode {
                    Mode::Invalidate {
                        edge_id: parsed, ..
                    }
                    | Mode::Revalidate {
                        edge_id: parsed, ..
                    } => {
                        assert_eq!(parsed, edge_id);
                    }
                    _ => panic!("expected an edge mode"),
                }
            }
        }
    }

    #[test]
    fn fact_modes_are_mutually_exclusive_with_every_other_mode() {
        let facts = ["--facts", "-1001", "Tama"];
        let invalidate = ["--invalidate", "-1001", "edge"];
        let revalidate = ["--revalidate", "-1001", "edge"];
        let live = ["--live", "", ""];
        let status = ["--status", "-1001", ""];
        let merge_tool = ["--merge-tool", "-1001", ""];
        for (first, second) in [
            (facts, invalidate),
            (facts, revalidate),
            (invalidate, revalidate),
            (facts, live),
            (facts, status),
            (invalidate, merge_tool),
            (revalidate, live),
            (revalidate, status),
        ] {
            let args: Vec<&str> = first
                .iter()
                .chain(second.iter())
                .copied()
                .filter(|arg| !arg.is_empty())
                .collect();
            let error = parse(&args).expect_err(&format!("the combination {args:?} must fail"));
            assert!(error.contains("mutually exclusive"), "message: {error}");
        }
    }

    #[test]
    fn max_confirmations_must_be_a_non_negative_integer() {
        let error = parse(&["--merge-tool", "-1001", "--max-confirmations", "many"])
            .expect_err("a non-integer budget must fail");
        assert!(error.contains("--max-confirmations"), "message: {error}");
    }

    #[test]
    fn apply_and_max_confirmations_are_accepted_in_other_modes() {
        // The same accepted-everywhere discipline as
        // --allow-default-persona: the flags parse in any mode and only
        // affect --merge-tool.
        let outcome = parse(&["--status", "-1001", "--apply", "--max-confirmations", "3"])
            .expect("the flags are accepted in the status mode");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::Status { .. }));
        assert!(cli.apply);
        assert_eq!(cli.max_confirmations, 3);
    }

    #[test]
    fn merge_modes_are_mutually_exclusive_with_every_other_mode() {
        let merge_tool = ["--merge-tool", "-1001", "", ""];
        let merge = ["--merge", "-1001", "a", "b"];
        let rollback = ["--merge-rollback", "-1001", "7", ""];
        let live = ["--live", "", "", ""];
        let status = ["--status", "-1001", "", ""];
        for (first, second) in [
            (merge_tool, merge),
            (merge_tool, rollback),
            (merge, rollback),
            (merge_tool, live),
            (merge, status),
            (rollback, live),
        ] {
            let args: Vec<&str> = first
                .iter()
                .chain(second.iter())
                .copied()
                .filter(|arg| !arg.is_empty())
                .collect();
            let error = parse(&args).expect_err(&format!("the combination {args:?} must fail"));
            assert!(error.contains("mutually exclusive"), "message: {error}");
        }
    }

    #[test]
    fn both_modes_is_a_usage_error() {
        let error =
            parse(&["--replay", "fixture.json", "--live"]).expect_err("both flags must fail");
        assert!(error.contains("mutually exclusive"));
    }

    #[test]
    fn every_pair_of_modes_is_a_usage_error() {
        // Exactly one mode is required; all combinations of two modes
        // are usage errors.
        let replay = ["--replay", "fixture.json"];
        let live = ["--live", ""];
        let status = ["--status", "-1001"];
        let status_all = ["--status-all", ""];
        for (first, second) in [
            (replay, live),
            (replay, status),
            (replay, status_all),
            (live, status),
            (live, status_all),
            (status, status_all),
        ] {
            let args: Vec<&str> = first
                .iter()
                .chain(second.iter())
                .copied()
                .filter(|arg| !arg.is_empty())
                .collect();
            let error = parse(&args).expect_err(&format!("the combination {args:?} must fail"));
            assert!(error.contains("mutually exclusive"), "message: {error}");
        }
    }

    #[test]
    fn no_mode_is_a_usage_error() {
        let error = parse(&[]).expect_err("no mode must fail");
        assert!(error.contains("--replay") && error.contains("--live"));
    }

    #[test]
    fn unknown_argument_is_a_usage_error() {
        let error = parse(&["--wat"]).expect_err("an unknown argument must fail");
        assert!(error.contains("unknown argument: --wat"));
    }

    #[test]
    fn llm_config_values_maps_the_embedding_keys() {
        // Decision 66: the global-only embedding keys map straight
        // through; the env-wins step lives in tamako-agent's
        // EmbeddingEndpoint::resolve (tested there).
        let config = TriggerConfig {
            embedding_model: "some-embedding-model".to_string(),
            embedding_llm_base_url: "https://embeddings.example/v1".to_string(),
            ..TriggerConfig::default()
        };
        let values = llm_config_values(&config);
        assert_eq!(
            values.embedding_model.as_deref(),
            Some("some-embedding-model")
        );
        assert_eq!(
            values.embedding_llm_base_url.as_deref(),
            Some("https://embeddings.example/v1")
        );
        // The defaults map through as concrete values too (decision 81
        // re-pins the default model).
        let values = llm_config_values(&TriggerConfig::default());
        assert_eq!(
            values.embedding_model.as_deref(),
            Some("google/gemini-embedding-2")
        );
        assert_eq!(
            values.embedding_llm_base_url.as_deref(),
            Some("https://openrouter.ai/api/v1")
        );
    }

    /// A resolved endpoint with a fixed session-id prefix, for the
    /// decision-84 suffix tests. Constructed directly (no
    /// `LlmEndpoints::resolve`) so the tests stay hermetic against the
    /// operator's TAMAKO_* environment.
    fn prefix_endpoint(prefix: &str) -> EndpointConfig {
        EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: None,
            model: "test-model".to_string(),
            structured_output: tamako_agent::StructuredOutputMode::default(),
            session_id: prefix.to_string(),
        }
    }

    /// Four endpoints sharing one prefix, the `resolve` output shape.
    fn prefix_endpoints(prefix: &str) -> LlmEndpoints {
        LlmEndpoints {
            digest: prefix_endpoint(prefix),
            gate: prefix_endpoint(prefix),
            reply: prefix_endpoint(prefix),
            summary: prefix_endpoint(prefix),
        }
    }

    #[test]
    fn splice_joins_each_suffix_onto_the_right_purpose() {
        // The pure half of decision 84 (b): each purpose's endpoint gets
        // ITS OWN suffix joined as `{prefix}-{suffix}` — the per-purpose
        // routing is the correctness point (a crossed wire here would
        // silently re-pin purposes to each other's provider sessions).
        // Distinct models pin that the splice returns the RIGHT endpoint
        // per purpose, not just the right session id.
        let endpoints = LlmEndpoints {
            digest: EndpointConfig {
                model: "digest-model".to_string(),
                ..prefix_endpoint("tamako")
            },
            gate: EndpointConfig {
                model: "gate-model".to_string(),
                ..prefix_endpoint("tamako")
            },
            reply: EndpointConfig {
                model: "reply-model".to_string(),
                ..prefix_endpoint("tamako")
            },
            summary: EndpointConfig {
                model: "summary-model".to_string(),
                ..prefix_endpoint("tamako")
            },
        };
        let suffixes = SessionSuffixes {
            digest: "digest-suffix".to_string(),
            gate: "gate-suffix".to_string(),
            reply: "reply-suffix".to_string(),
            summary: "summary-suffix".to_string(),
        };
        let spliced = splice_session_suffixes(endpoints, &suffixes);
        for (endpoint, model, session_id) in [
            (&spliced.digest, "digest-model", "tamako-digest-suffix"),
            (&spliced.gate, "gate-model", "tamako-gate-suffix"),
            (&spliced.reply, "reply-model", "tamako-reply-suffix"),
            (&spliced.summary, "summary-model", "tamako-summary-suffix"),
        ] {
            assert_eq!(endpoint.model, model, "the purpose keeps its endpoint");
            assert_eq!(endpoint.session_id, session_id, "the prefix-suffix join");
        }
    }

    #[test]
    fn apply_session_suffixes_fails_loudly_when_the_mint_fails() {
        // Decision 84 (b): a broken suffix mint means a broken store,
        // which is FATAL for serving the group — never a silent fallback
        // to the bare prefix (an unpersisted id would re-pin the group
        // to a fresh provider session on every restart). The store below
        // is broken deterministically: its data_root is a FILE, so
        // opening the group db (and with it the mint) always fails.
        let dir = tempfile::tempdir().expect("tempdir");
        let data_root_file = dir.path().join("not-a-directory");
        std::fs::write(&data_root_file, b"").expect("a data_root that cannot hold group dbs");
        let store = Arc::new(Store::new(data_root_file));
        let err = apply_session_suffixes(&store, "g1", prefix_endpoints("tamako"))
            .expect_err("a mint failure is fatal, never a bare-prefix fallback");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("failed to mint the digest session-affinity suffix of group g1"),
            "the mint context names the purpose and group, got: {chain}"
        );
    }

    #[test]
    fn session_suffixes_join_the_prefix_per_purpose() {
        // Decision 84 (b): every completion purpose gets its OWN suffix
        // joined as `{prefix}-{suffix}`; the purpose strings come from
        // tamako-agent (LlmPurpose::as_str), never hardcoded here.
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(Store::new(dir.path().to_path_buf()));
        store.open_group("g1").expect("open group");
        let endpoints =
            apply_session_suffixes(&store, "g1", prefix_endpoints("tamako")).expect("mint");
        for (purpose, endpoint) in [
            (LlmPurpose::Digest, &endpoints.digest),
            (LlmPurpose::Gate, &endpoints.gate),
            (LlmPurpose::Reply, &endpoints.reply),
            (LlmPurpose::Summary, &endpoints.summary),
        ] {
            let suffix = endpoint
                .session_id
                .strip_prefix("tamako-")
                .expect("the suffix joins onto the prefix");
            assert_eq!(suffix.len(), 16, "12 random bytes, base64url");
            let stored = store
                .get_or_insert_session_suffix("g1", purpose.as_str())
                .expect("the minted suffix is persisted");
            assert_eq!(suffix, stored, "purpose {}", purpose.as_str());
        }
        // Four purposes, four DISTINCT suffixes.
        let ids = [
            &endpoints.digest.session_id,
            &endpoints.gate.session_id,
            &endpoints.reply.session_id,
            &endpoints.summary.session_id,
        ];
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 4);
    }

    #[test]
    fn session_suffixes_are_sticky_per_group() {
        // Decision 84 (b): the mint is persisted, so a rebuild (the
        // restart case) re-applies the SAME suffixes; another group
        // mints its own.
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(Store::new(dir.path().to_path_buf()));
        store.open_group("g1").expect("open group g1");
        store.open_group("g2").expect("open group g2");
        let first =
            apply_session_suffixes(&store, "g1", prefix_endpoints("tamako")).expect("first mint");
        let second =
            apply_session_suffixes(&store, "g1", prefix_endpoints("tamako")).expect("re-mint");
        assert_eq!(first, second, "the suffixes survive a rebuild");
        let other =
            apply_session_suffixes(&store, "g2", prefix_endpoints("tamako")).expect("other group");
        assert_ne!(
            first.reply.session_id, other.reply.session_id,
            "the affinity key is per-(group, purpose)"
        );
    }

    #[test]
    fn capability_of_maps_every_status() {
        assert_eq!(
            capability_of(BotChatStatus::Administrator),
            Capability::Full
        );
        assert_eq!(
            capability_of(BotChatStatus::Member),
            Capability::NoReactions
        );
        assert_eq!(
            capability_of(BotChatStatus::RestrictedOrOther),
            Capability::NoReactions
        );
        assert_eq!(
            capability_of(BotChatStatus::Unknown),
            Capability::Indeterminate
        );
    }

    #[test]
    fn status_guidance_matches_the_capability_mapping() {
        // Full functionality needs no guidance.
        assert!(status_guidance(BotChatStatus::Administrator).is_none());
        // Every non-administrator status carries a non-empty guidance
        // line. Member and restricted share the reaction-collection
        // guidance; the unknown status has its own.
        for status in [
            BotChatStatus::Member,
            BotChatStatus::RestrictedOrOther,
            BotChatStatus::Unknown,
        ] {
            assert_ne!(capability_of(status), Capability::Full);
            let guidance = status_guidance(status).expect("non-admin statuses have guidance");
            assert!(!guidance.is_empty());
        }
        assert_eq!(
            status_guidance(BotChatStatus::Member),
            status_guidance(BotChatStatus::RestrictedOrOther)
        );
    }

    #[test]
    fn strict_persona_missing_file_is_a_hard_error_with_the_cp_hint() {
        // specs.md Section 5.3 / Rule C4: --live must not silently fall
        // back to a default persona.
        let dir = tempfile::tempdir().expect("tempdir");
        let error = load_persona_strict(dir.path()).expect_err("a missing persona file must fail");
        let text = format!("{error:#}");
        assert!(text.contains("persona.toml"), "message: {text}");
        assert!(text.contains(&dir.path().join("persona.toml").display().to_string()));
        assert!(text.contains("cp persona.toml"), "message: {text}");
        assert!(text.contains("--allow-default-persona"), "message: {text}");
    }

    #[test]
    fn strict_persona_valid_file_loads() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("persona.toml"),
            "name = \"Mochi\"\nidentity = \"a calm dog who lives in this chat group\"\n",
        )
        .expect("write persona");
        let persona = load_persona_strict(dir.path()).expect("a valid persona file must load");
        assert_eq!(persona.name, "Mochi");
    }

    #[test]
    fn strict_persona_malformed_file_is_a_hard_error() {
        // A broken persona file in live mode is a configuration
        // mistake; there is no silent fallback.
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("persona.toml"), "name = [unclosed").expect("write");
        let error =
            load_persona_strict(dir.path()).expect_err("a malformed persona file must fail");
        let text = format!("{error:#}");
        assert!(text.contains("invalid"), "message: {text}");
    }

    /// Builds a status snapshot with the given state pairs, table count,
    /// and dead-letter rows.
    fn status_fixture(
        pairs: &[(&str, &str)],
        dead_letter_count: u64,
        recent_dead_letters: Vec<tamako_store::DeadLetterRow>,
    ) -> GroupStatus {
        let state = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        GroupStatus {
            state,
            dead_letter_count,
            recent_dead_letters,
        }
    }

    /// A dead-letter row with a fixed timestamp for the render tests.
    fn dead_letter(
        id: i64,
        batch_id: &str,
        error: &str,
        unix_ts: i64,
    ) -> tamako_store::DeadLetterRow {
        tamako_store::DeadLetterRow {
            id,
            batch_id: batch_id.to_string(),
            batch_skeleton: "{\"items\":[]}".to_string(),
            error: error.to_string(),
            created_at: OffsetDateTime::from_unix_timestamp(unix_ts).expect("valid timestamp"),
        }
    }

    #[test]
    fn format_group_status_with_zero_counters_prints_no_rates() {
        let status = status_fixture(&[], 0, Vec::new());
        let text = format_group_status(
            "-1001234567890",
            Path::new("./data/-1001234567890/store.db"),
            &status,
            5,
        );
        // Counter keys absent from the state table print as 0.
        assert!(
            text.contains("summaries_failed_total:    0"),
            "text:\n{text}"
        );
        assert!(
            text.contains("facts_invalidated_total:   0"),
            "text:\n{text}"
        );
        assert!(
            text.contains("warmups_total:             0"),
            "text:\n{text}"
        );
        assert!(
            text.contains("warmup_engaged_total:      0"),
            "text:\n{text}"
        );
        assert!(
            text.contains("dead_letters_total:        0"),
            "text:\n{text}"
        );
        // The rates print only when wakes_total > 0.
        assert!(!text.contains("rates:"), "text:\n{text}");
        assert!(
            text.contains("last_digest_boundary_msg_id:   0"),
            "text:\n{text}"
        );
        assert!(
            text.contains("muted:                     false"),
            "text:\n{text}"
        );
        assert!(text.contains("  dead letters: 0 total\n"), "text:\n{text}");
    }

    #[test]
    fn format_group_status_renders_counters_rates_boundaries_and_dead_letters() {
        let status = status_fixture(
            &[
                ("wakes_total", "30"),
                ("participations_total", "12"),
                ("injection_wakes_total", "6"),
                ("digest_failures_total", "0"),
                ("summaries_failed_total", "3"),
                ("facts_invalidated_total", "4"),
                ("warmups_total", "10"),
                ("warmup_engaged_total", "4"),
                ("dead_letters_total", "2"),
                ("last_digest_boundary_msg_id", "91"),
                ("prev_digest_boundary_msg_id", "82"),
                ("muted_flag", "1"),
                ("consecutive_bot_msgs", "0"),
            ],
            2,
            vec![
                dead_letter(
                    7,
                    "batch:10:20",
                    "provider timeout after 30 s",
                    1_785_528_000,
                ),
                dead_letter(3, "batch:1:9", "schema validation failed", 1_785_348_764),
            ],
        );
        let text = format_group_status(
            "-1001234567890",
            Path::new("./data/-1001234567890/store.db"),
            &status,
            5,
        );
        assert!(
            text.starts_with("Group status: -1001234567890\n"),
            "text:\n{text}"
        );
        assert!(
            text.contains("  store:                       ./data/-1001234567890/store.db\n"),
            "text:\n{text}"
        );
        assert!(
            text.contains("participation rate:        40.0% (12/30) (healthy band: 30-60%)"),
            "text:\n{text}"
        );
        assert!(
            text.contains("injection rate:            20.0% (6/30) (expected band: 20 to 40%)"),
            "text:\n{text}"
        );
        assert!(
            text.contains("last_digest_boundary_msg_id:   91"),
            "text:\n{text}"
        );
        assert!(
            text.contains("prev_digest_boundary_msg_id:   82"),
            "text:\n{text}"
        );
        assert!(
            text.contains("muted:                     true"),
            "text:\n{text}"
        );
        assert!(
            text.contains("summaries_failed_total:    3"),
            "text:\n{text}"
        );
        assert!(
            text.contains("facts_invalidated_total:   4"),
            "text:\n{text}"
        );
        assert!(
            text.contains("warmups_total:             10"),
            "text:\n{text}"
        );
        assert!(
            text.contains("warmup_engaged_total:      4"),
            "text:\n{text}"
        );
        // The warmup engagement rate (specs.md Section 12, decision 78
        // (f)): engaged over sent warmups.
        assert!(
            text.contains("warmup engagement rate:    40.0% (4/10)"),
            "text:\n{text}"
        );
        assert!(
            text.contains("  dead letters: 2 total (most recent first)\n"),
            "text:\n{text}"
        );
        // The attempts display: a dead-lettered batch exhausted
        // digest_max_retries (specs.md Section 10.3 item 2).
        assert!(text.contains("attempts=5 (exhausted)"), "text:\n{text}");
        // Newest first: id=7 before id=3.
        let first = text.find("id=7 ").expect("id=7 line");
        let second = text.find("id=3 ").expect("id=3 line");
        assert!(first < second, "text:\n{text}");
        assert!(
            text.contains("      error: provider timeout after 30 s\n"),
            "text:\n{text}"
        );
        assert!(
            text.contains("      error: schema validation failed\n"),
            "text:\n{text}"
        );
        // The counter and the table count agree: no note.
        assert!(!text.contains("note:"), "text:\n{text}");
    }

    #[test]
    fn format_group_status_prints_the_warmup_rate_without_any_wake() {
        // Regression-pin the rates gate (decision 78 (f)): a group that
        // warmed up but never woke still shows its warmup engagement
        // rate; the wake rates stay gated on wakes_total > 0.
        let status = status_fixture(
            &[("warmups_total", "3"), ("warmup_engaged_total", "1")],
            0,
            Vec::new(),
        );
        let text = format_group_status("-1", Path::new("./data/-1/store.db"), &status, 5);
        assert!(text.contains("  rates:\n"), "text:\n{text}");
        assert!(
            text.contains("warmup engagement rate:    33.3% (1/3)"),
            "text:\n{text}"
        );
        assert!(!text.contains("participation rate:"), "text:\n{text}");
        assert!(!text.contains("injection rate:"), "text:\n{text}");
    }

    #[test]
    fn the_example_config_parses_and_keeps_its_group_table() {
        // Pins tamako.example.toml against drift: the file must always
        // parse through the real config loader and keep its example
        // group table. The six decision-78 warmup keys are commented
        // out in the file (ST3 covers the keys themselves); this guard
        // catches a key spelling that stops parsing or a dropped table.
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tamako.example.toml");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} reads: {error}", path.display()));
        let config = BotConfig::from_toml_str(&text).expect("the example config parses");
        assert!(
            config.overrides.contains_key("-1001234567890"),
            "the example group table exists"
        );
    }

    #[test]
    fn format_group_status_notes_a_counter_table_disagreement() {
        // The table count is authoritative; the note explains the drift.
        let status = status_fixture(&[("dead_letters_total", "5")], 2, Vec::new());
        let text = format_group_status("-1", Path::new("./data/-1/store.db"), &status, 5);
        assert!(
            text.contains("  dead letters: 2 total (most recent first)\n"),
            "text:\n{text}"
        );
        assert!(text.contains("note:"), "text:\n{text}");
        assert!(text.contains("counter reads 5"), "text:\n{text}");
    }

    /// A fact edge fixture with fixed timestamps for the render tests.
    fn fact_edge(
        relationship_name: &str,
        other_node_name: &str,
        outgoing: bool,
        edge_text: &str,
        invalid_at: Option<OffsetDateTime>,
    ) -> tamako_memory::NodeFactEdge {
        tamako_memory::NodeFactEdge {
            edge_id: format!(
                "{{\"source_id\":\"s\",\"relationship_name\":\"{relationship_name}\",\
                 \"target_id\":\"t\",\"valid_at\":\"2026-08-01T00:00:00Z\"}}"
            ),
            relationship_name: relationship_name.to_string(),
            other_node_name: other_node_name.to_string(),
            outgoing,
            edge_text: edge_text.to_string(),
            valid_at: time::macros::datetime!(2026-08-01 00:00:00 UTC),
            invalid_at,
        }
    }

    #[test]
    fn format_node_facts_renders_edges_directions_and_validity() {
        let facts = tamako_memory::NodeFacts {
            node_id: "person:tama".to_string(),
            node_name: "Tama".to_string(),
            edges: vec![
                fact_edge("works_at", "Studio A", true, "Tama works at Studio A", None),
                fact_edge(
                    "likes",
                    "Tama",
                    false,
                    "a long edge text that runs well past the excerpt cap of sixty characters",
                    Some(time::macros::datetime!(2026-08-02 00:00:00 UTC)),
                ),
            ],
        };
        let text = format_node_facts(&facts);
        assert!(
            text.starts_with("Facts: \"Tama\" (person:tama) in the group graph\n"),
            "text:\n{text}"
        );
        assert!(text.contains("  edges: 2\n"), "text:\n{text}");
        // The edge id line comes first: the operator copies it.
        assert!(
            text.contains("  edge id: {\"source_id\":\"s\","),
            "text:\n{text}"
        );
        assert!(
            text.contains("    -> works_at \"Studio A\"\n"),
            "text:\n{text}"
        );
        assert!(text.contains("    <- likes \"Tama\"\n"), "text:\n{text}");
        assert!(
            text.contains("      valid_at:   2026-08-01T00:00:00Z\n"),
            "text:\n{text}"
        );
        assert!(
            text.contains("      invalid_at: 2026-08-02T00:00:00Z  INVALID\n"),
            "text:\n{text}"
        );
        // A valid edge prints the (valid) marker, never INVALID.
        assert!(
            text.contains("      invalid_at: (valid)\n"),
            "text:\n{text}"
        );
        // The long description is excerpted at the character cap.
        let excerpt_line = text
            .lines()
            .find(|line| line.starts_with("      text:       a long edge text"))
            .expect("the excerpt line");
        assert!(excerpt_line.ends_with('…'), "line: {excerpt_line}");
    }

    #[test]
    fn format_node_facts_without_edges_prints_the_note() {
        let facts = tamako_memory::NodeFacts {
            node_id: "alias:mochi".to_string(),
            node_name: "Mochi".to_string(),
            edges: Vec::new(),
        };
        let text = format_node_facts(&facts);
        assert!(text.contains("  edges: 0\n"), "text:\n{text}");
        assert!(
            text.contains("  (no edges: the node has no facts yet)\n"),
            "text:\n{text}"
        );
    }

    #[test]
    fn excerpt_edge_text_keeps_short_texts_and_cuts_long_ones() {
        // Short texts pass through unchanged; the cap counts characters,
        // not bytes, and the cut gets an ellipsis.
        assert_eq!(excerpt_edge_text("short"), "short");
        let exactly: String = "x".repeat(FACTS_EXCERPT_MAX_CHARS);
        assert_eq!(excerpt_edge_text(&exactly), exactly);
        let long: String = "y".repeat(FACTS_EXCERPT_MAX_CHARS + 1);
        let excerpt = excerpt_edge_text(&long);
        assert_eq!(excerpt.chars().count(), FACTS_EXCERPT_MAX_CHARS + 1);
        assert!(excerpt.ends_with('…'));
        // Character-boundary safety: a multibyte character at the cut
        // point never splits.
        let cjk: String = "あ".repeat(FACTS_EXCERPT_MAX_CHARS + 3);
        let excerpt = excerpt_edge_text(&cjk);
        assert_eq!(excerpt.chars().count(), FACTS_EXCERPT_MAX_CHARS + 1);
        assert!(excerpt.ends_with('…'));
    }

    #[test]
    fn merge_force_flag_parses_and_defaults_to_false() {
        // Decision 77 (M9): --force gates the kind-incompatible --merge.
        let ParseOutcome::Run(cli) =
            parse(&["--merge", "-1001", "a", "b"]).expect("a valid --merge line")
        else {
            panic!("expected the Run outcome");
        };
        assert!(!cli.force);
        let ParseOutcome::Run(cli) = parse(&["--merge", "-1001", "a", "b", "--force"])
            .expect("a valid --merge line with --force")
        else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::Merge { .. }));
        assert!(cli.force);
    }

    #[test]
    fn force_is_accepted_in_other_modes_but_only_affects_merge() {
        // The same accepted-everywhere discipline as --apply.
        let ParseOutcome::Run(cli) =
            parse(&["--status", "-1001", "--force"]).expect("the flag parses in the status mode")
        else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::Status { .. }));
        assert!(cli.force);
    }

    /// Decision 77 (H6a) test fixture: a two-node fragmented Concept
    /// pair with near-parallel stored vectors (the merge.rs fixture
    /// discipline), so the scan finds exactly one candidate. Returns
    /// the store, the backend, and the scanned candidate.
    async fn seeded_merge_group(dir: &Path) -> (Arc<Store>, LbugBackend, MergeCandidate) {
        let store = Arc::new(Store::new(dir.to_path_buf()));
        store.open_group("g1").expect("open group");
        let memory = LbugBackend::new(dir.to_path_buf());
        let at = time::macros::datetime!(2026-08-17 10:00 UTC);
        let node = |name: &str, description: &str| tamako_memory::MemoryNode {
            id: tamako_memory::identifiers::concept_id(name),
            name: name.to_string(),
            node_type: NodeType::Concept,
            created_at: at,
            updated_at: at,
            properties: Some(format!(r#"{{"description":"{description}"}}"#)),
        };
        let rust = node("Rust", "the programming language");
        let rustlang = node("Rust Language", "the Rust programming language");
        memory
            .upsert_batch(
                "g1",
                &tamako_memory::MemoryBatch {
                    batch_id: tamako_memory::identifiers::batch_id(1, 10),
                    nodes: vec![rust.clone(), rustlang.clone()],
                    edges: vec![],
                },
            )
            .await
            .expect("seed graph");
        // Near-parallel sparse vectors: cosine ~0.995 (see the merge.rs
        // test fixture note).
        let vector = |jitter: Option<f32>| {
            let mut vector = vec![0.0f32; tamako_store::EMBEDDING_DIM];
            vector[0] = 1.0;
            if let Some(jitter) = jitter {
                vector[1] = jitter;
            }
            vector
        };
        store
            .upsert_node_embedding(&rust.id, &vector(None))
            .expect("vec");
        store
            .upsert_node_embedding(&rustlang.id, &vector(Some(0.1)))
            .expect("vec");
        let candidates = scan_merge_candidates(&store, &memory, "g1", 0.85)
            .await
            .expect("scan");
        assert_eq!(candidates.len(), 1);
        (
            store,
            memory,
            candidates.into_iter().next().expect("one candidate"),
        )
    }

    /// A one-action 'same' plan of the seeded pair (hand-built
    /// verdicts: the tests never call an LLM).
    fn same_plan(candidate: &MergeCandidate) -> MergePlan {
        MergePlan {
            actions: vec![MergePlanAction {
                candidate: candidate.clone(),
                verdict: MergeVerdict::Same,
                reason: "identical concept".to_string(),
                survivor_id: candidate.a_id.clone(),
                loser_id: candidate.b_id.clone(),
            }],
            skipped: Vec::new(),
        }
    }

    #[tokio::test]
    async fn merge_tool_dry_run_writes_the_plan_file_and_apply_executes_it() {
        // Decision 77 (H6a) happy path: the dry run writes
        // merge_plan.json; the apply certifies it against a fresh scan
        // and executes the FILE's actions.
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, memory, candidate) = seeded_merge_group(dir.path()).await;
        let plan = same_plan(&candidate);
        let path = write_merge_plan_file(dir.path(), "g1", 0.85, "llm:test-model", &plan)
            .expect("write the plan file");
        assert!(path.is_file(), "the dry run wrote merge_plan.json");
        assert_eq!(path, dir.path().join("g1").join("merge_plan.json"));
        // The stored hash is the hash of the dry run's candidate set.
        let file = read_merge_plan_file(dir.path(), "g1").expect("read back");
        assert_eq!(
            file.candidate_set_hash,
            merge_candidate_set_hash([&candidate])
        );

        let (loaded, confirmed_by) =
            load_applicable_merge_plan(&store, &memory, dir.path(), "g1", 0.85)
                .await
                .expect("a fresh plan file is applicable");
        assert_eq!(confirmed_by, "llm:test-model");
        assert_eq!(loaded, plan, "the file's actions load verbatim");
        let report = apply_merge_plan(&store, &memory, "g1", &loaded, &confirmed_by, &[]).await;
        assert!(report.failures.is_empty());
        assert_eq!(report.audit_ids.len(), 1);
        assert!(
            memory
                .node_content("g1", &candidate.b_id)
                .await
                .expect("content")
                .is_none(),
            "the loser merged away"
        );
        let row = store
            .get_merge_audit(report.audit_ids[0])
            .expect("get")
            .expect("the audit row");
        assert_eq!(row.verdict, "same");
        assert!(row.snapshot.is_some(), "the rollback source landed");
    }

    #[tokio::test]
    async fn merge_tool_apply_refuses_a_missing_plan_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, memory, _candidate) = seeded_merge_group(dir.path()).await;
        let error = load_applicable_merge_plan(&store, &memory, dir.path(), "g1", 0.85)
            .await
            .expect_err("no plan file was written");
        let text = format!("{error:#}");
        assert!(text.contains("merge_plan.json"), "message: {text}");
        assert!(text.contains("dry run"), "message: {text}");
    }

    #[tokio::test]
    async fn merge_tool_apply_refuses_a_stale_plan_file() {
        // Decision 77 (H6a): the graph changed between the dry run and
        // the apply (the pair got linked, so the scan excludes it) —
        // the candidate-set hash mismatches and the apply refuses.
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, memory, candidate) = seeded_merge_group(dir.path()).await;
        write_merge_plan_file(
            dir.path(),
            "g1",
            0.85,
            "llm:test-model",
            &same_plan(&candidate),
        )
        .expect("write the plan file");
        memory
            .link_also_known_as("g1", &candidate.a_id, &candidate.b_id)
            .await
            .expect("link the pair");
        let error = load_applicable_merge_plan(&store, &memory, dir.path(), "g1", 0.85)
            .await
            .expect_err("the candidate set changed");
        let text = format!("{error:#}");
        assert!(
            text.contains("the merge plan is stale; re-run the dry run"),
            "message: {text}"
        );
        // A hand-edited file (the hash field tampered) refuses the same way.
        let path = merge_plan_file_path(dir.path(), "g1");
        let mut file = read_merge_plan_file(dir.path(), "g1").expect("read");
        file.candidate_set_hash = "0000000000000000".to_string();
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&file).expect("serialize"),
        )
        .expect("rewrite");
        let error = load_applicable_merge_plan(&store, &memory, dir.path(), "g1", 0.85)
            .await
            .expect_err("a tampered hash mismatches");
        assert!(format!("{error:#}").contains("stale"));
    }

    #[test]
    fn merge_plan_file_refuses_version_chat_and_verdict_mismatches() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("g1")).expect("group dir");
        let candidate = MergeCandidate {
            a_id: "a".to_string(),
            b_id: "b".to_string(),
            a_name: "A".to_string(),
            b_name: "B".to_string(),
            a_description: String::new(),
            b_description: String::new(),
            kind: NodeType::Concept,
            score: 0.9,
        };
        let plan = same_plan(&candidate);
        write_merge_plan_file(dir.path(), "g1", 0.85, "llm:test-model", &plan).expect("write");

        // A chat-id mismatch is a loud refusal.
        let error =
            read_merge_plan_file(dir.path(), "g2").expect_err("a missing file for another group");
        assert!(format!("{error:#}").contains("dry run"));
        // A version mismatch is a loud refusal.
        let mut file = read_merge_plan_file(dir.path(), "g1").expect("read");
        file.version = MERGE_PLAN_FILE_VERSION + 1;
        std::fs::write(
            merge_plan_file_path(dir.path(), "g1"),
            serde_json::to_string_pretty(&file).expect("serialize"),
        )
        .expect("rewrite");
        let error = read_merge_plan_file(dir.path(), "g1").expect_err("a version mismatch");
        assert!(format!("{error:#}").contains("version"));
        // An unknown verdict string never silently becomes a plan.
        let mut file = MergePlanFile::from_plan("g1", 0.85, "llm:test-model", &plan);
        file.actions[0].verdict = "maybe".to_string();
        let error = file.into_plan().expect_err("an unknown verdict");
        assert!(format!("{error:#}").contains("unknown verdict"));
    }

    #[test]
    fn group_lock_refuses_a_second_holder_and_is_reacquirable_after_drop() {
        // Decision 77 (H5): the contention refusal names the
        // possibility and the lock path; a dropped guard frees the
        // group again.
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("g1")).expect("group dir");
        let guard = acquire_group_lock(dir.path(), "g1").expect("the first lock");
        let error = acquire_group_lock(dir.path(), "g1")
            .err()
            .expect("contention must refuse");
        let text = format!("{error:#}");
        assert!(text.contains("is the bot running?"), "message: {text}");
        assert!(text.contains(".tamako.lock"), "message: {text}");
        assert!(text.contains("g1"), "message: {text}");
        drop(guard);
        acquire_group_lock(dir.path(), "g1").expect("the lock is free after the drop");
    }

    #[tokio::test]
    async fn open_merge_group_requires_store_db_and_memory_lbug() {
        // Decision 77 (S1-F8): both files must exist; a store-only
        // group (the graph never written) bails with the same
        // not-served-yet family.
        let dir = tempfile::tempdir().expect("tempdir");
        let error = open_merge_group(dir.path(), "g1")
            .err()
            .expect("neither file exists");
        let text = format!("{error:#}");
        assert!(text.contains("no store.db"), "message: {text}");
        assert!(text.contains("has not been served yet"), "message: {text}");

        let store = Store::new(dir.path().to_path_buf());
        store.open_group("g1").expect("create the store only");
        let error = open_merge_group(dir.path(), "g1")
            .err()
            .expect("no graph yet");
        let text = format!("{error:#}");
        assert!(text.contains("no memory.lbug"), "message: {text}");
        assert!(text.contains("has not been served yet"), "message: {text}");
        // The read-only variant checks the same.
        let error = open_merge_group_read_only(dir.path(), "g1")
            .err()
            .expect("no graph yet");
        assert!(format!("{error:#}").contains("no memory.lbug"));
    }
}
