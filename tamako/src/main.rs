//! Tamako binary. CLI wiring, the `--replay` demo, and the `--live` mode.
//! The demo replays a recorded chat log through the mock adapter into one
//! group actor and prints a summary. The live mode runs against Telegram
//! through the teloxide adapter (specs.md Section 4.2) and routes the
//! events of every configured group to its own actor. Phase 1 wires the
//! digest pipeline of specs.md Section 10 (M1) and the wake procedure of
//! specs.md Section 9 (M4) with the shallow recall of Sections 9.1-9.5
//! (M5) from the resolved LLM endpoints of Section 13; without the
//! family API key the pipelines degrade to silence.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use tamako_adapter_mock::MockAdapter;
use tamako_adapter_teloxide::{BotChatStatus, GroupEvent, TeloxideAdapter};
use tamako_agent::{
    AgentDigestPipeline, AgentError, EndpointConfig, LlmConfigValues, LlmEndpoints, PipelineConfig,
    RigExtractor, RigGate, RigRelevanceGate, RigReplyGenerator, ShallowRecall,
};
use tamako_core::actor::{
    spawn_group_actor, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::{BotConfig, TriggerConfig};
use tamako_core::digest::DigestPipeline;
use tamako_core::event::OutboundAction;
use tamako_core::wake::{NoopRecall, RecallProvider, WakeServices};
use tamako_memory::LbugBackend;
use tamako_persona::{load_persona, PersonaConfig, PetPreambleRenderer, PreambleRenderer};
use tamako_store::{read_group_status, GroupStatus, Store, StoreError};
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
  --allow-default-persona  Only affects --live: restores the lenient
                           persona fallback chain (repo-root example, then
                           the built-in default) instead of requiring
                           <data-root>/persona.toml. For experiments.
  --data-root <dir>        Data root of the bot. Default: ./data
  --config <config.toml>   Bot configuration file. Optional. A missing file
                           keeps the global defaults. In the status modes it
                           supplies the per-group digest_max_retries of the
                           attempts display.
  --help                   Show this text.";

/// The run mode. Exactly one of `--replay` / `--live` / `--status` /
/// `--status-all` is required.
#[derive(Debug)]
enum Mode {
    Replay { fixture: PathBuf },
    Live,
    Status { chat_id: String },
    StatusAll,
}

/// The parsed command line.
#[derive(Debug)]
struct Cli {
    mode: Mode,
    data_root: PathBuf,
    config: Option<PathBuf>,
    allow_default_persona: bool,
}

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
    let mut allow_default_persona = false;
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
            "--allow-default-persona" => allow_default_persona = true,
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
    let mode = match (fixture, live, status, status_all) {
        (Some(fixture), false, None, false) => Mode::Replay { fixture },
        (None, true, None, false) => Mode::Live,
        (None, false, Some(chat_id), false) => Mode::Status { chat_id },
        (None, false, None, true) => Mode::StatusAll,
        (None, false, None, false) => {
            return Err(
                "one of --replay <fixture.json>, --live, --status <chat_id>, or --status-all \
                 is required"
                    .to_string(),
            )
        }
        _ => {
            return Err(
                "the run modes are mutually exclusive: pick exactly one of --replay \
                 <fixture.json>, --live, --status <chat_id>, or --status-all"
                    .to_string(),
            )
        }
    };
    Ok(ParseOutcome::Run(Cli {
        mode,
        data_root: data_root.unwrap_or_else(|| PathBuf::from("./data")),
        config,
        allow_default_persona,
    }))
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
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
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
    let Some(path) = path else {
        return Ok(BotConfig::default());
    };
    if !path.exists() {
        warn!(path = %path.display(), "configuration file not found; using the global defaults");
        return Ok(BotConfig::default());
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read the configuration file {}", path.display()))?;
    let config = BotConfig::from_toml_str(&text)
        .with_context(|| format!("failed to parse the configuration file {}", path.display()))?;
    info!(path = %path.display(), "bot configuration loaded");
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
        digest_model: config.digest_model.clone(),
        gate_model: config.gate_model.clone(),
        reply_model: config.reply_model.clone(),
        digest_llm_api: config.digest_llm_api.clone(),
        digest_llm_base_url: config.digest_llm_base_url.clone(),
        gate_llm_api: config.gate_llm_api.clone(),
        gate_llm_base_url: config.gate_llm_base_url.clone(),
        reply_llm_api: config.reply_llm_api.clone(),
        reply_llm_base_url: config.reply_llm_base_url.clone(),
        structured_output: config.structured_output.clone(),
        digest_structured_output: config.digest_structured_output.clone(),
        gate_structured_output: config.gate_structured_output.clone(),
        reply_structured_output: config.reply_structured_output.clone(),
    }
}

/// Resolves the three LLM endpoints of specs.md Section 13 from the
/// group configuration and the environment. A resolve error (an unknown
/// `llm_api` family string, in the config or in `TAMAKO_LLM_API`) is a
/// HARD startup error: operator misconfiguration must surface, never
/// silently default.
fn resolve_endpoints(config: &TriggerConfig) -> Result<LlmEndpoints> {
    LlmEndpoints::resolve(&llm_config_values(config))
        .map_err(anyhow::Error::new)
        .context("failed to resolve the LLM endpoints (specs.md Section 13)")
}

/// Builds the digest pipeline (specs.md Section 10) for the resolved
/// digest endpoint. `Ok(None)` means digests are disabled for this run:
/// the replay still works, the digest trigger stays a stub. A missing
/// family API key (`EndpointClient::build` reports it as
/// `AgentError::ProviderConfig`, either family) degrades to `None` with
/// a warning; every other build error propagates.
fn build_digest_pipeline(
    store: &Arc<Store>,
    memory: &Arc<LbugBackend>,
    endpoint: &EndpointConfig,
) -> Result<Option<Arc<dyn DigestPipeline>>> {
    let model = endpoint.model.clone();
    match RigExtractor::from_endpoint(endpoint) {
        Ok(extractor) => {
            info!(model = %model, "digest pipeline wired (live extraction)");
            Ok(Some(Arc::new(AgentDigestPipeline::new(
                Arc::clone(store),
                Arc::clone(memory),
                Arc::new(extractor),
                // specs.md Section 13: max_retries = 5.
                PipelineConfig::default(),
            )) as Arc<dyn DigestPipeline>))
        }
        Err(AgentError::ProviderConfig(error)) => {
            warn!(%error, "digest pipeline disabled: no provider configuration; digests will not run");
            Ok(None)
        }
        Err(error) => Err(error).context("failed to build the digest pipeline"),
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
fn build_wake_services(
    store: &Arc<Store>,
    memory: &Arc<LbugBackend>,
    endpoints: &LlmEndpoints,
    recall_injection_cap: u32,
) -> Result<Option<WakeServices>> {
    match (
        RigGate::from_endpoint(&endpoints.gate),
        RigReplyGenerator::from_endpoint(&endpoints.reply),
    ) {
        (Ok(gate), Ok(reply)) => {
            // specs.md Section 9 step 2 (M5): the shallow recall worker
            // over the shared store and graph.
            let recall: Arc<dyn RecallProvider> = match RigRelevanceGate::from_endpoint(
                &endpoints.gate,
            ) {
                Ok(relevance_gate) => Arc::new(ShallowRecall::new(
                    Arc::clone(store),
                    Arc::clone(memory),
                    relevance_gate,
                    recall_injection_cap,
                )),
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
                recall_injection_cap,
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

/// The run setup shared by both modes: bot configuration, the rendered
/// persona preamble, the persona name (the sender display name of
/// outbound raw-log rows), and the two storage backends.
struct SharedSetup {
    bot_config: BotConfig,
    preamble: String,
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
    let preamble = PetPreambleRenderer.render_preamble(&persona);
    info!(persona = %persona.name, preamble_len = preamble.len(), "persona preamble rendered");
    Ok(SharedSetup {
        bot_config,
        preamble,
        bot_name: persona.name,
        store: Arc::new(Store::new(cli.data_root.clone())),
        memory: Arc::new(LbugBackend::new(cli.data_root.clone())),
    })
}

/// Dispatches to the selected run mode. The status modes are offline
/// inspection: no persona load, no TELOXIDE_TOKEN, no LLM endpoints, no
/// actor spawn — status NEVER fails for a missing persona file. The run
/// modes share one outbound channel for every actor (Rule A3): the
/// actions carry their chat id, so one pump into the platform adapter is
/// enough.
async fn run(cli: Cli) -> Result<()> {
    match &cli.mode {
        Mode::Status { chat_id } => return run_status(&cli, chat_id),
        Mode::StatusAll => return run_status_all(&cli),
        Mode::Replay { .. } | Mode::Live => {}
    }
    let setup = shared_setup(&cli)?;
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel::<OutboundAction>(100);
    match &cli.mode {
        Mode::Replay { fixture } => {
            run_replay(&cli.data_root, &setup, fixture, outbound_tx, outbound_rx).await
        }
        Mode::Live => run_live(&setup, outbound_tx, outbound_rx).await,
        // The status modes returned above.
        Mode::Status { .. } | Mode::StatusAll => unreachable!(),
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
    let digest = build_digest_pipeline(&store, &memory, &endpoints.digest)?;
    let wake = build_wake_services(
        &store,
        &memory,
        &endpoints,
        group_config.recall_injection_cap,
    )?;
    let handle = spawn_group_actor(GroupActorParams {
        chat_id: chat_id.clone(),
        store: Arc::clone(&store),
        memory,
        config: group_config,
        started_at: OffsetDateTime::now_utc(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        // Rule C4: the rendered preamble seeds item 0 of the live context.
        preamble: setup.preamble.clone(),
        digest,
        // The actor performs the Rule C3 removal itself (M2); the hook
        // stays a seam for observers that need no actor state.
        post_digest_hook: None,
        wake,
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

/// The `--status-all` run: one status block per group store under the
/// data root, sorted by chat id. No stores at all is not an error.
fn run_status_all(cli: &Cli) -> Result<()> {
    let bot_config = load_bot_config(cli.config.as_deref())?;
    let mut chat_ids: Vec<String> = Vec::new();
    if cli.data_root.is_dir() {
        for entry in std::fs::read_dir(&cli.data_root)
            .with_context(|| format!("failed to list the data root {}", cli.data_root.display()))?
        {
            let entry = entry?;
            // Rule P5: a group store is a subdirectory that contains
            // store.db. Other files of the data root (persona.toml) are
            // skipped.
            let path = entry.path();
            if path.is_dir() && path.join("store.db").is_file() {
                chat_ids.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    if chat_ids.is_empty() {
        println!("no group stores under {}", cli.data_root.display());
        return Ok(());
    }
    chat_ids.sort();
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

/// Renders one group status snapshot as aligned text (specs.md Sections
/// 10.3 and 12). Pure: the unit tests assert the exact shape. Counter
/// keys absent from the state table print as 0; the rates print only
/// when wakes_total > 0.
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
        "    {:<27}{dead_letters_counter}",
        "dead_letters_total:"
    );
    // The rates are meaningful only once the bot woke at least once.
    if wakes > 0 {
        let participation_rate = participations as f64 * 100.0 / wakes as f64;
        let injection_rate = injection_wakes as f64 * 100.0 / wakes as f64;
        let _ = writeln!(out, "  rates:");
        let _ = writeln!(
            out,
            "    {:<27}{participation_rate:.1}% ({participations}/{wakes}) \
             (healthy target: below 50%)",
            "participation rate:"
        );
        let _ = writeln!(
            out,
            "    {:<27}{injection_rate:.1}% ({injection_wakes}/{wakes}) \
             (expected band: 20 to 40%)",
            "injection rate:"
        );
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
    let mut adapter = TeloxideAdapter::new(&token)
        .await
        .context("failed to start the Telegram adapter")?;
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
    // Chat ids already logged as non-configured; the message logs once each.
    let mut logged_skips: HashSet<String> = HashSet::new();
    let mut events_routed = 0_usize;
    // A fatal error to surface after the shutdown flush below.
    let mut fatal: Option<anyhow::Error> = None;

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
                            let digest =
                                match build_digest_pipeline(&setup.store, &setup.memory, &endpoints.digest) {
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
                                group_config.recall_injection_cap,
                            ) {
                                Ok(wake) => wake,
                                Err(error) => {
                                    fatal = Some(error);
                                    break;
                                }
                            };
                            info!(chat_id = %chat_id, "first event of a configured group; spawning the actor");
                            entry.insert(spawn_group_actor(GroupActorParams {
                                chat_id: chat_id.clone(),
                                store: Arc::clone(&setup.store),
                                memory: Arc::clone(&setup.memory),
                                config: group_config,
                                started_at: OffsetDateTime::now_utc(),
                                inbox_capacity: DEFAULT_INBOX_CAPACITY,
                                // Rule C4: the rendered preamble seeds item 0
                                // of the live context.
                                preamble: setup.preamble.clone(),
                                digest,
                                post_digest_hook: None,
                                wake,
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
                    info!("the telegram update stream ended");
                    break;
                }
                Err(error) => {
                    // The adapter already skips transient stream errors
                    // internally. An escaping Err is a fatal channel issue;
                    // a continue could spin hot, so the loop breaks instead.
                    warn!(%error, "fatal telegram adapter error; stopping the event loop");
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
            text.contains("wakes_total:               0"),
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
            text.contains("participation rate:        40.0% (12/30) (healthy target: below 50%)"),
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
}
