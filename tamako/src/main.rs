//! Tamako binary. CLI wiring, the `--replay` demo, and the `--live` mode.
//! The demo replays a recorded chat log through the mock adapter into one
//! group actor and prints a summary. The live mode runs against Telegram
//! through the teloxide adapter (specs.md Section 4.2) and routes the
//! events of every configured group to its own actor. Phase 1 wires the
//! digest pipeline of specs.md Section 10 (M1) and the wake procedure of
//! specs.md Section 9 (M4) from the resolved LLM endpoints of Section 13;
//! without the family API key the pipelines degrade to silence.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use tamako_adapter_mock::MockAdapter;
use tamako_adapter_teloxide::{BotChatStatus, GroupEvent, TeloxideAdapter};
use tamako_agent::{
    AgentDigestPipeline, AgentError, EndpointConfig, LlmConfigValues, LlmEndpoints, PipelineConfig,
    RigExtractor, RigGate, RigReplyGenerator,
};
use tamako_core::actor::{
    spawn_group_actor, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::{BotConfig, TriggerConfig};
use tamako_core::digest::DigestPipeline;
use tamako_core::event::OutboundAction;
use tamako_core::wake::{NoopRecall, WakeServices};
use tamako_memory::LbugBackend;
use tamako_persona::{load_persona, PersonaConfig, PetPreambleRenderer, PreambleRenderer};
use tamako_store::Store;
use time::OffsetDateTime;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
Usage:
  tamako --replay <fixture.json> [--data-root <dir>] [--config <config.toml>]
  tamako --live [--data-root <dir>] [--config <config.toml>]
  tamako --help

Options:
  --replay <fixture.json>  Replay a recorded chat log through the mock adapter.
  --live                   Run live against Telegram. The token comes from the
                           TELOXIDE_TOKEN environment variable. The served
                           groups are the [groups.<chat_id>] tables of the
                           config file. The bot must be a group admin with
                           privacy mode off.
  --data-root <dir>        Data root of the bot. Default: ./data
  --config <config.toml>   Bot configuration file. Optional. A missing file
                           keeps the global defaults.
  --help                   Show this text.";

/// The run mode. Exactly one of `--replay` / `--live` is required.
#[derive(Debug)]
enum Mode {
    Replay { fixture: PathBuf },
    Live,
}

/// The parsed command line.
#[derive(Debug)]
struct Cli {
    mode: Mode,
    data_root: PathBuf,
    config: Option<PathBuf>,
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
    let mode = match (fixture, live) {
        (Some(fixture), false) => Mode::Replay { fixture },
        (None, true) => Mode::Live,
        (Some(_), true) => return Err("use either --replay or --live, not both".to_string()),
        (None, false) => {
            return Err("one of --replay <fixture.json> or --live is required".to_string())
        }
    };
    Ok(ParseOutcome::Run(Cli {
        mode,
        data_root: data_root.unwrap_or_else(|| PathBuf::from("./data")),
        config,
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
fn build_wake_services(endpoints: &LlmEndpoints) -> Result<Option<WakeServices>> {
    match (
        RigGate::from_endpoint(&endpoints.gate),
        RigReplyGenerator::from_endpoint(&endpoints.reply),
    ) {
        (Ok(gate), Ok(reply)) => {
            info!(
                gate_model = %endpoints.gate.model,
                reply_model = %endpoints.reply.model,
                "wake procedure wired (live gate and reply)"
            );
            Ok(Some(WakeServices {
                // specs.md Section 9 step 2: the recall seam. M4 wires
                // the no-op; M5 replaces it with shallow recall.
                recall: Arc::new(NoopRecall),
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
    let persona = load_persona_with_fallback(&cli.data_root);
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

/// Dispatches to the selected run mode after the shared setup. One
/// outbound channel serves every actor of the run (Rule A3): the actions
/// carry their chat id, so one pump into the platform adapter is enough.
async fn run(cli: Cli) -> Result<()> {
    let setup = shared_setup(&cli)?;
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel::<OutboundAction>(100);
    match cli.mode {
        Mode::Replay { fixture } => {
            run_replay(&cli.data_root, &setup, &fixture, outbound_tx, outbound_rx).await
        }
        Mode::Live => run_live(&setup, outbound_tx, outbound_rx).await,
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
    let wake = build_wake_services(&endpoints)?;
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
                            let wake = match build_wake_services(&endpoints) {
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
            Mode::Live => panic!("expected the replay mode"),
        }
        assert_eq!(cli.data_root, PathBuf::from("./data"));
        assert!(cli.config.is_none());
    }

    #[test]
    fn live_only() {
        let outcome = parse(&["--live"]).expect("a valid live command line");
        let ParseOutcome::Run(cli) = outcome else {
            panic!("expected the Run outcome");
        };
        assert!(matches!(cli.mode, Mode::Live));
        assert_eq!(cli.data_root, PathBuf::from("./data"));
    }

    #[test]
    fn both_modes_is_a_usage_error() {
        let error =
            parse(&["--replay", "fixture.json", "--live"]).expect_err("both flags must fail");
        assert!(error.contains("not both"));
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
}
