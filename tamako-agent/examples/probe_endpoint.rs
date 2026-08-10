//! The live endpoint probe for structured-output modes (the robustness
//! fix of the openai-compatible digest extraction).
//!
//! ```text
//! cargo run -p tamako-agent --example probe_endpoint -- --config tamako.toml
//! cargo run -p tamako-agent --example probe_endpoint -- --mode schema
//! ```
//!
//! The probe posts raw HTTP (no rig) so it stays independent of the
//! agent crate internals. It sends one tiny structured request per mode
//! and reports, per mode, the HTTP status, the raw response body, the
//! extracted assistant content, and the local validation result. The
//! three modes replicate what the digest pipeline could ask of an
//! openai-compatible endpoint:
//!
//! - `schema`: `response_format: {type: "json_schema", strict: true}`,
//!   the exact wire shape rig 0.41 emits for `output_schema`.
//! - `graph_schema`: like `schema`, but the json_schema schema is the
//!   real `schemars::schema_for!(KnowledgeGraph)` schema (the complex
//!   nested shape with $defs and an enum that the digest extraction
//!   sends) and the prompt is a small two-message chat batch. This
//!   mode reproduces the conditions of the original live extraction
//!   bug.
//! - `json_object`: `response_format: {type: "json_object"}`.
//! - `none`: no `response_format` key at all (the baseline).
//!
//! The API key comes from `OPENAI_API_KEY` (specs.md Section 13: keys
//! come from the environment only) and is never printed.

use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::Deserialize;
use tamako_agent::KnowledgeGraph;
use tamako_core::config::BotConfig;

/// The default base URL: Opencode Go, chat/completions wire format.
/// The live `tamako.toml` sets this explicitly; the default covers a
/// missing config file.
const DEFAULT_BASE_URL: &str = "https://opencode.ai/zen/go/v1";

/// The default probe model: the cheap digest model of the live config.
const DEFAULT_MODEL: &str = "mimo-v2.5";

/// The cap on the printed raw response body. Large enough to hold one
/// full chat/completions response for a tiny request.
const BODY_PRINT_CAP: usize = 2000;

/// The one user message every mode sends. Kept minimal so the probe
/// stays cheap and the schema-drift signal is unambiguous.
const PROMPT: &str =
    "Output a JSON object describing a pet: name 'Tama', legs 4. Output JSON only.";

/// The chat batch of the `graph_schema` mode: two user messages, like
/// one digest batch of the replay fixture. The prompt states the exact
/// field names, matching the extraction preamble style (prompt.rs).
const GRAPH_PROMPT: &str = "Extract the knowledge graph of this chat batch.\n\
    Output shape (field names exactly as written): \
    {\"nodes\":[{\"name\":\"...\",\"node_type\":\"Person\"|\"Concept\",\"description\":\"...\"}],\
    \"edges\":[{\"source\":\"...\",\"target\":\"...\",\"relationship_name\":\"...\",\"description\":\"...\"}]}\n\
    Output only the JSON object. No commentary.\n\n\
    Batch:\n\
    [1] Alice: I just adopted a corgi named Mochi.\n\
    [2] Bob: Nice! Corgis shed a lot, get a good vacuum.";

/// The `max_tokens` of the `graph_schema` mode: the graph plus the
/// endpoint's reasoning budget needs more headroom than the pet probe.
const GRAPH_MAX_TOKENS: u32 = 262144;

/// The local validation target: what the extraction expects back.
#[derive(Debug, Deserialize)]
struct Pet {
    name: String,
    legs: i64,
}

/// One structured-output mode under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// rig 0.41's wire shape for `output_schema`.
    Schema,
    /// Like `Schema`, with the real KnowledgeGraph schema and chat
    /// batch prompt (the original bug's conditions).
    GraphSchema,
    /// The weaker OpenAI mode without a schema.
    JsonObject,
    /// No `response_format` at all.
    None,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Schema => "schema",
            Mode::GraphSchema => "graph_schema",
            Mode::JsonObject => "json_object",
            Mode::None => "none",
        }
    }
}

/// The resolved probe configuration.
struct ProbeConfig {
    base_url: String,
    model: String,
    api_key: String,
}

/// The parsed CLI arguments.
struct Cli {
    config_path: PathBuf,
    modes: Vec<Mode>,
}

/// Parses `--config <path>` and `--mode schema|json_object|none`. No
/// `--mode` runs all three modes sequentially.
fn parse_cli() -> Result<Cli, String> {
    let mut config_path = PathBuf::from("tamako.toml");
    let mut modes: Vec<Mode> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args.next().ok_or("--config requires a path")?;
                config_path = PathBuf::from(value);
            }
            "--mode" => {
                let value = args.next().ok_or("--mode requires a value")?;
                let mode = match value.as_str() {
                    "schema" => Mode::Schema,
                    "graph_schema" => Mode::GraphSchema,
                    "json_object" => Mode::JsonObject,
                    "none" => Mode::None,
                    other => {
                        return Err(format!(
                        "unknown --mode {other:?}; expected schema|graph_schema|json_object|none"
                    ))
                    }
                };
                modes.push(mode);
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    if modes.is_empty() {
        modes = vec![Mode::Schema, Mode::JsonObject, Mode::None];
    }
    Ok(Cli { config_path, modes })
}

/// Resolves the probe configuration. Resolution matches the app wiring
/// (tamako-agent/src/endpoint.rs): the env vars win over the config
/// file, the config file wins over the built-in defaults.
fn resolve_probe_config(config_path: &Path) -> Result<ProbeConfig, String> {
    let bot_config = if config_path.exists() {
        let text = std::fs::read_to_string(config_path)
            .map_err(|error| format!("failed to read {}: {error}", config_path.display()))?;
        BotConfig::from_toml_str(&text)
            .map_err(|error| format!("failed to parse {}: {error}", config_path.display()))?
    } else {
        println!(
            "config {} not found; using the built-in defaults",
            config_path.display()
        );
        BotConfig::default()
    };
    let global = &bot_config.global;
    let base_url = std::env::var("TAMAKO_LLM_BASE_URL")
        .ok()
        .or_else(|| global.llm_base_url.clone())
        .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
    let model = std::env::var("TAMAKO_DIGEST_MODEL")
        .ok()
        .or_else(|| global.digest_model.clone())
        .unwrap_or_else(|| DEFAULT_MODEL.to_string());
    // The family is informational here: the probe always speaks the
    // chat/completions wire format.
    let family = global.llm_api.as_deref().unwrap_or("(unset)");
    println!("endpoint family (config): {family}");
    let api_key = std::env::var("OPENAI_API_KEY")
        .map_err(|_| "OPENAI_API_KEY is not set; the probe needs an API key".to_string())?;
    Ok(ProbeConfig {
        base_url,
        model,
        api_key,
    })
}

/// Builds the request body of one mode.
fn request_body(model: &str, mode: Mode) -> serde_json::Value {
    let (prompt, max_tokens) = match mode {
        Mode::GraphSchema => (GRAPH_PROMPT, GRAPH_MAX_TOKENS),
        _ => (PROMPT, 512),
    };
    let mut body = serde_json::json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "user", "content": prompt},
        ],
    });
    match mode {
        // The exact shape rig-core 0.41 emits for `output_schema`
        // (strict is hardcoded true there).
        Mode::Schema => {
            body["response_format"] = serde_json::json!({
                "type": "json_schema",
                "json_schema": {
                    "name": "pet",
                    "strict": true,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "legs": {"type": "integer"},
                        },
                        "required": ["name", "legs"],
                        "additionalProperties": false,
                    },
                },
            });
        }
        Mode::GraphSchema => {
            // The same wire shape as `Schema`, with the real schemars
            // schema of the digest extraction (rig_impl.rs).
            let schema = serde_json::to_value(schemars::schema_for!(KnowledgeGraph))
                .expect("KnowledgeGraph schema serializes");
            body["response_format"] = serde_json::json!({
                "type": "json_schema",
                "json_schema": {
                    "name": "knowledge_graph",
                    "strict": true,
                    "schema": schema,
                },
            });
        }
        Mode::JsonObject => {
            body["response_format"] = serde_json::json!({"type": "json_object"});
        }
        Mode::None => {}
    }
    body
}

/// Truncates a body for printing.
fn truncate(text: &str) -> String {
    if text.chars().count() <= BODY_PRINT_CAP {
        return text.to_string();
    }
    let truncated: String = text.chars().take(BODY_PRINT_CAP).collect();
    format!("{truncated}...[truncated]")
}

/// Extracts the assistant content of a chat/completions body.
fn extract_content(body: &serde_json::Value) -> Option<String> {
    body.pointer("/choices/0/message/content")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// Runs one mode once and prints the delimited report.
async fn run_mode(
    client: &reqwest::Client,
    config: &ProbeConfig,
    mode: Mode,
) -> Result<(), String> {
    let url = format!("{}/chat/completions", config.base_url);
    let started = Instant::now();
    let response = client
        .post(&url)
        .bearer_auth(&config.api_key)
        .json(&request_body(&config.model, mode))
        .send()
        .await
        .map_err(|error| format!("mode {}: request failed: {error}", mode.name()))?;
    let latency = started.elapsed();
    let status = response.status();
    let body_text = response
        .text()
        .await
        .map_err(|error| format!("mode {}: failed to read the body: {error}", mode.name()))?;

    println!(
        "==================== mode: {} ====================",
        mode.name()
    );
    println!("HTTP status: {status}");
    println!("latency: {latency:.2?}");
    println!("raw body: {}", truncate(&body_text));

    // Content extraction and validation only make sense on a JSON body.
    match serde_json::from_str::<serde_json::Value>(&body_text) {
        Ok(body) => {
            if let Some(usage) = body.get("usage") {
                println!("usage: {usage}");
            }
            match extract_content(&body) {
                Some(content) => {
                    println!("assistant content: {}", truncate(&content));
                    validate_content(mode, &content);
                }
                None => println!("assistant content: (none found at /choices/0/message/content)"),
            }
        }
        Err(error) => println!("body is not JSON: {error}"),
    }
    println!();
    Ok(())
}

/// Validates the assistant content against the mode's target type.
fn validate_content(mode: Mode, content: &str) {
    match mode {
        Mode::GraphSchema => match serde_json::from_str::<KnowledgeGraph>(content) {
            Ok(graph) => println!(
                "local validation: Ok (nodes={}, edges={})",
                graph.nodes.len(),
                graph.edges.len()
            ),
            Err(error) => println!("local validation: Err ({error})"),
        },
        _ => match serde_json::from_str::<Pet>(content) {
            Ok(pet) => println!(
                "local validation: Ok (name={:?}, legs={})",
                pet.name, pet.legs
            ),
            Err(error) => println!("local validation: Err ({error})"),
        },
    }
}

#[tokio::main]
async fn main() {
    let result: Result<(), String> = async {
        let cli = parse_cli()?;
        let config = resolve_probe_config(&cli.config_path)?;
        println!("base url: {}", config.base_url);
        println!("model: {}", config.model);
        println!();
        let client = reqwest::Client::new();
        for mode in &cli.modes {
            run_mode(&client, &config, *mode).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
