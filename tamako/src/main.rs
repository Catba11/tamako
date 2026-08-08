//! Tamako binary. Phase 0 scope (dev-roadmap.md Section 2): CLI wiring
//! and the `--replay` demo. The demo replays a recorded chat log through
//! the mock adapter into one group actor and prints a summary.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use tamako_adapter_mock::MockAdapter;
use tamako_core::actor::{spawn_group_actor, GroupActorParams, DEFAULT_INBOX_CAPACITY};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::BotConfig;
use tamako_memory::LbugBackend;
use tamako_persona::{load_persona, PersonaConfig, PetPreambleRenderer, PreambleRenderer};
use tamako_store::Store;
use time::OffsetDateTime;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
Usage:
  tamako --replay <fixture.json> [--data-root <dir>] [--config <config.toml>]
  tamako --help

Options:
  --replay <fixture.json>  Replay a recorded chat log through the mock adapter.
  --data-root <dir>        Data root of the bot. Default: ./data
  --config <config.toml>   Bot configuration file. Optional. A missing file
                           keeps the global defaults.
  --help                   Show this text.";

/// The parsed command line.
struct Cli {
    fixture: PathBuf,
    data_root: PathBuf,
    config: Option<PathBuf>,
}

/// The result of the command-line parse.
enum ParseOutcome {
    Run(Cli),
    Help,
}

/// Parses the arguments. An `Err` carries a message for the user.
fn parse_args<I: Iterator<Item = String>>(args: I) -> std::result::Result<ParseOutcome, String> {
    let mut fixture = None;
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
    let fixture = fixture.ok_or("the --replay flag is required".to_string())?;
    Ok(ParseOutcome::Run(Cli {
        fixture,
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

/// The `--replay` run. Refer to dev-roadmap.md Section 2.
async fn run(cli: Cli) -> Result<()> {
    let bot_config = load_bot_config(cli.config.as_deref())?;

    // Rule C4: the preamble is the prefix of every model context. Phase 0
    // proves the wiring only; it logs the length of the rendered preamble.
    let persona = load_persona_with_fallback(&cli.data_root);
    let preamble = PetPreambleRenderer.render_preamble(&persona);
    info!(persona = %persona.name, preamble_len = preamble.len(), "persona preamble rendered");

    let mut adapter = MockAdapter::from_fixture_path(&cli.fixture).with_context(|| {
        format!(
            "failed to load the replay fixture {}",
            cli.fixture.display()
        )
    })?;
    let chat_id = adapter.chat_id().to_string();
    info!(chat_id = %chat_id, remaining = adapter.remaining(), "replay fixture loaded");

    let store = Arc::new(Store::new(cli.data_root.clone()));
    let memory = Arc::new(LbugBackend::new(cli.data_root.clone()));
    let handle = spawn_group_actor(GroupActorParams {
        chat_id: chat_id.clone(),
        store: Arc::clone(&store),
        memory,
        config: bot_config.for_group(&chat_id),
        started_at: OffsetDateTime::now_utc(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
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
    let rows = {
        let store = Arc::clone(&store);
        let chat_id = chat_id.clone();
        tokio::task::spawn_blocking(move || store.list_messages(&chat_id))
            .await
            .context("the blocking list_messages task failed to join")?
            .context("failed to read the raw log")?
    };

    let group_dir = cli.data_root.join(&chat_id);
    println!("Replay summary");
    println!("  chat_id:                {chat_id}");
    println!("  events replayed:        {events_replayed}");
    println!("  raw log rows:           {}", rows.len());
    println!(
        "  digest boundary msg id: {}",
        session.last_digest_boundary_msg_id
    );
    println!("  muted:                  {}", session.muted);
    println!("  consecutive bot msgs:   {}", session.consecutive_bot_msgs);
    println!("  wake msgs since wake:   {}", session.wake.msgs_since_wake);
    println!("  group directory:        {}", group_dir.display());
    Ok(())
}
