//! Tamako binary. CLI wiring, the `--replay` demo, and the `--live` mode.
//! The demo replays a recorded chat log through the mock adapter into one
//! group actor and prints a summary. The live mode runs against Telegram
//! through the teloxide adapter (specs.md Section 4.2) and routes the
//! events of every configured group to its own actor. Phase 1 (M1) wires
//! the digest pipeline of specs.md Section 10 when `ANTHROPIC_API_KEY` is
//! present; without the key digests simply do not run.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use tamako_adapter_mock::MockAdapter;
use tamako_adapter_teloxide::{GroupEvent, TeloxideAdapter};
use tamako_agent::{
    AgentDigestPipeline, AgentError, ExtractorConfig, PipelineConfig, RigExtractor,
};
use tamako_core::actor::{
    spawn_group_actor, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::{BotConfig, TriggerConfig};
use tamako_core::digest::DigestPipeline;
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

/// Builds the digest pipeline (specs.md Section 10) when
/// `ANTHROPIC_API_KEY` is present. `Ok(None)` means digests are disabled
/// for this run: the replay still works, the digest trigger stays a
/// stub. A provider configuration error degrades to `None` with a
/// warning; every other build error propagates.
fn build_digest_pipeline(
    store: &Arc<Store>,
    memory: &Arc<LbugBackend>,
    config: &TriggerConfig,
) -> Result<Option<Arc<dyn DigestPipeline>>> {
    // rig reads ANTHROPIC_API_KEY itself (Client::from_env); the binary
    // never reads the key. The check only decides whether to try.
    if std::env::var_os("ANTHROPIC_API_KEY").is_none() {
        info!("ANTHROPIC_API_KEY is not set; the digest pipeline is disabled for this run");
        return Ok(None);
    }
    // Override order: TAMAKO_DIGEST_MODEL, then the config-file
    // digest_model (specs.md Section 13), then the default.
    let extractor_config = ExtractorConfig::resolve(config.digest_model.as_deref());
    let model = extractor_config.model.clone();
    match RigExtractor::from_env(extractor_config) {
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

/// The run setup shared by both modes: bot configuration, the rendered
/// persona preamble, and the two storage backends.
struct SharedSetup {
    bot_config: BotConfig,
    preamble: String,
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
        store: Arc::new(Store::new(cli.data_root.clone())),
        memory: Arc::new(LbugBackend::new(cli.data_root.clone())),
    })
}

/// Dispatches to the selected run mode after the shared setup.
async fn run(cli: Cli) -> Result<()> {
    let setup = shared_setup(&cli)?;
    match cli.mode {
        Mode::Replay { fixture } => run_replay(&cli.data_root, &setup, &fixture).await,
        Mode::Live => run_live(&setup).await,
    }
}

/// The `--replay` run. Refer to dev-roadmap.md Section 2.
async fn run_replay(data_root: &Path, setup: &SharedSetup, fixture: &Path) -> Result<()> {
    let mut adapter = MockAdapter::from_fixture_path(fixture)
        .with_context(|| format!("failed to load the replay fixture {}", fixture.display()))?;
    let chat_id = adapter.chat_id().to_string();
    info!(chat_id = %chat_id, remaining = adapter.remaining(), "replay fixture loaded");

    let store = Arc::clone(&setup.store);
    let memory = Arc::clone(&setup.memory);
    let group_config = setup.bot_config.for_group(&chat_id);
    // The digest pipeline is built after the chat_id is known and before
    // the actor spawns (Phase 1, M1).
    let digest = build_digest_pipeline(&store, &memory, &group_config)?;
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
    });

    let mut events_replayed = 0_usize;
    while let Some(event) = adapter
        .next_event()
        .await
        .context("the replay source failed")?
    {
        handle
            .send_event(event)
            .await
            .context("the group actor inbox closed during the replay")?;
        events_replayed += 1;
    }

    // The snapshot is a FIFO barrier: when it returns, every replayed
    // event is processed and the session state is persisted.
    let session = handle.snapshot().await.context("the snapshot failed")?;
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

/// The `--live` run: one group actor per configured group, fed from the
/// Telegram update stream (specs.md Section 4.2).
async fn run_live(setup: &SharedSetup) -> Result<()> {
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
                            let group_config = setup.bot_config.for_group(&chat_id);
                            let digest =
                                match build_digest_pipeline(&setup.store, &setup.memory, &group_config) {
                                    Ok(digest) => digest,
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
}
