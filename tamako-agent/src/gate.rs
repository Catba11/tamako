//! The participation gate of specs.md Section 9.6: the binary decision
//! whether the bot speaks in this wake, plus the target message of the
//! reply.
//!
//! The gate runs ONLY for unforced wakes. Forced wakes (mention/reply,
//! Section 8.1) bypass the gate: the bot must respond when addressed
//! directly. The actor never calls `decide` for a forced wake.
//!
//! Principles P4 and P6: the group pet has scarce attention. It speaks
//! rarely and it can stay silent. Section 12: a sustained participation
//! rate above 50 percent means the gate is too permissive.
//!
//! The live implementation is [`RigGate`] over the cheap `gate_model`
//! endpoint (specs.md Section 13). Structured output via JSON schema
//! (same pattern as M1's extractor). Tests use [`ScriptedGate`].
//!
//! Never trust the model: the structured output is post-validated in
//! plain Rust (same principle as `validate.rs`).

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use rig::completion::Message;

use tamako_core::actor::CoreError;
use tamako_core::wake::{GateDecision, GateInput, GateMessage, ParticipationGate};
use tamako_persona::CONTEXT_FORMAT_GLOSS;

use crate::endpoint::{EndpointClient, EndpointConfig};
use crate::extract::AgentError;

/// The default max tokens of the gate response. The output is one small
/// JSON object (three fields); 262144 tokens is a generous bound.
pub const GATE_DEFAULT_MAX_TOKENS: u64 = 262144;

/// The structured gate output (Section 9.6). `target_msg_id` is the
/// raw-log row id of the message the reply should target.
///
/// The LLM produces exactly this shape (rig output_schema). The doc
/// comments are part of the prompt: schemars turns them into schema
/// descriptions on the Anthropic structured-output path.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct GateOutput {
    /// True when the pet participates in this wake.
    ///
    /// The `should_participate` alias tolerates field-name drift of
    /// endpoints that ignore the output schema and free-generate.
    /// Aliases affect deserialization only: serialization and the
    /// schemars schema keep the canonical name.
    #[serde(alias = "should_participate")]
    pub participate: bool,
    /// The raw-log row id of the ONE message the reply targets. Must be
    /// the id of one of the presented new messages. Null when
    /// participate is false.
    ///
    /// The `target_message_id`/`target_id` aliases tolerate endpoint
    /// field-name drift.
    #[serde(alias = "target_message_id", alias = "target_id")]
    pub target_msg_id: Option<i64>,
    /// One short reason for the decision.
    ///
    /// The `rationale` alias tolerates endpoint field-name drift.
    #[serde(alias = "rationale")]
    pub reason: String,
}

/// The system preamble of the gate call (Section 9.6; principles P4 and
/// P6; Section 12).
pub const GATE_PREAMBLE: &str = "\
You decide whether a group pet speaks. The pet is a member of a group chat. Attention is scarce: the pet speaks rarely, and it can stay silent.
Output shape (field names exactly as written): {\"participate\":true|false,\"target_msg_id\":<integer or null>,\"reason\":\"...\"}

Rules:
1. Read the new group messages. Decide whether the pet participates in this wake.
2. Participate ONLY when the pet can add something a member would welcome: a direct question to the pet, a topic the pet can genuinely help with, or a moment that fits a pet persona.
3. When in doubt, stay silent. Silence costs nothing. An unwanted reply costs attention. A participation rate above 50 percent means the gate is too permissive.
4. On participation, select the ONE message the reply targets: the most recent message that motivates the participation. Give its message id.
5. On silence, set participate to false and target_msg_id to null.
6. Output only the JSON object of the required schema. Give one short reason. No commentary.";

/// The full system preamble of the gate call: [`GATE_PREAMBLE`] plus
/// the shared context-format gloss of tamako-persona (decision 63, the
/// deliberate preamble event). The gloss is the SINGLE source in
/// tamako-persona: the persona preamble and the recall preamble embed
/// the same constant, so the format explanations can never drift
/// apart. The gate consumes the XML-shaped `GateMessage.content`
/// lines, so it must read the same explanation.
pub fn gate_system_preamble() -> String {
    format!("{GATE_PREAMBLE}\n\n{CONTEXT_FORMAT_GLOSS}")
}

/// Renders the user prompt of the gate call (Section 9.6): one line per
/// new message as `{row_id} {content}` (the content already carries the
/// XML `<msg>` shape of Section 7.2 step 4), then the injected memories
/// of the recall step. M4 has no recall, so the section renders `(none)`;
/// the section exists on purpose: the recall result is part of the gate
/// input (Section 9.6).
pub fn render_gate_prompt(input: &GateInput) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "New messages (id and XML-tagged content):");
    for message in &input.new_messages {
        let _ = writeln!(out, "{} {}", message.row_id, message.content);
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "Injected memories:");
    if input.injections.is_empty() {
        // M4: the recall step is the no-op; an empty injection is
        // forbidden (Section 9.2), so nothing is injected.
        let _ = writeln!(out, "(none)");
    } else {
        for injection in &input.injections {
            let _ = writeln!(out, "- {injection}");
        }
    }
    out
}

/// Post-validation of the structured gate output (never trust the
/// model; same principle as `validate.rs`). Pure function, no I/O.
///
/// Rules:
///
/// - `participate == false` → silence; a target on a negative decision
///   is dropped.
/// - `participate == true` requires a `target_msg_id` that is one of
///   the presented `new_messages` row ids. A target outside the
///   presented set (or none) is a gate malfunction: silence is cheaper
///   than a wrong reply (conservative fallback + debug log).
/// - Otherwise the decision passes through.
fn gate_decision_from_output(output: GateOutput, presented: &[GateMessage]) -> GateDecision {
    // The gate's own reason string rides along on every outcome
    // (telemetry only: the curated wake log line).
    if !output.participate {
        return GateDecision {
            participate: false,
            target_row_id: None,
            reason: Some(output.reason),
        };
    }
    match output.target_msg_id {
        Some(row_id) if presented.iter().any(|message| message.row_id == row_id) => GateDecision {
            participate: true,
            target_row_id: Some(row_id),
            reason: Some(output.reason),
        },
        other => {
            tracing::debug!(
                target_msg_id = ?other,
                "the gate returned a target outside the presented messages; falling back to silence"
            );
            GateDecision {
                participate: false,
                target_row_id: None,
                reason: Some(output.reason),
            }
        }
    }
}

/// The live participation gate: one completion call on the cheap
/// `gate_model` endpoint (specs.md Sections 9.6 and 13) with the
/// `GateOutput` schema.
pub struct RigGate {
    client: EndpointClient,
    max_tokens: u64,
}

// The rig model handles do not implement Debug. A manual impl keeps
// RigGate printable in test failures and logs (same pattern as
// RigExtractor).
impl std::fmt::Debug for RigGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigGate")
            .field("client", &self.client)
            .field("max_tokens", &self.max_tokens)
            .finish_non_exhaustive()
    }
}

impl RigGate {
    /// Builds the gate from an endpoint client.
    pub fn new(client: EndpointClient, max_tokens: u64) -> Self {
        RigGate { client, max_tokens }
    }

    /// Builds the gate for one resolved endpoint (the `gate` purpose,
    /// specs.md Section 13). Returns `AgentError::ProviderConfig` when
    /// the family API key is missing.
    pub fn from_endpoint(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        Ok(RigGate::new(
            EndpointClient::build(endpoint)?,
            GATE_DEFAULT_MAX_TOKENS,
        ))
    }
}

impl ParticipationGate for RigGate {
    fn decide<'a>(
        &'a self,
        input: &'a GateInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<GateDecision, CoreError>> + Send + 'a>,
    > {
        Box::pin(async move {
            // The shared structured flow of the endpoint layer
            // (schema per the resolved mode, one-shot repair retry).
            let output = self
                .client
                .complete_structured::<GateOutput>(
                    // The preamble plus the shared format gloss becomes
                    // the system message.
                    Some(gate_system_preamble()),
                    vec![Message::user(render_gate_prompt(input))],
                    schemars::schema_for!(GateOutput),
                    self.max_tokens,
                    "invalid gate JSON",
                )
                .await
                // A gate failure skips this wake; the next wake is the
                // natural retry (CoreError::Wake docs).
                .map_err(|error| CoreError::Wake(error.to_string()))?;
            Ok(gate_decision_from_output(output, &input.new_messages))
        })
    }
}

/// The response mode of `ScriptedGate`.
enum ScriptedGateMode {
    /// Pops the next decision per call (FIFO). An exhausted queue
    /// returns silence.
    Decisions(VecDeque<GateDecision>),
    /// Every call fails with `CoreError::Wake`.
    Failing(String),
}

/// A scripted gate for tests (same pattern as `ScriptedExtractor`).
/// Two modes:
///
/// - `ScriptedGate::with_decisions(vec_of_decisions)`: pops the next
///   decision per call (FIFO; when exhausted, returns silence);
/// - `ScriptedGate::failing(message)`: every call fails with
///   `CoreError::Wake`.
///
/// Every `GateInput` is recorded for assertions (`inputs()`).
pub struct ScriptedGate {
    mode: Mutex<ScriptedGateMode>,
    inputs: Mutex<Vec<GateInput>>,
}

impl ScriptedGate {
    /// A scripted gate that answers with the given decisions in order.
    pub fn with_decisions(decisions: Vec<GateDecision>) -> Self {
        ScriptedGate {
            mode: Mutex::new(ScriptedGateMode::Decisions(decisions.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// A scripted gate whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedGate {
            mode: Mutex::new(ScriptedGateMode::Failing(message.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// Every input the gate received, in call order.
    pub fn inputs(&self) -> Vec<GateInput> {
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ParticipationGate for ScriptedGate {
    fn decide<'a>(
        &'a self,
        input: &'a GateInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<GateDecision, CoreError>> + Send + 'a>,
    > {
        // Lock, record, and decide synchronously; the future only
        // carries the result. A poisoned mutex is recovered; the
        // recorded inputs stay valid (same policy as ScriptedExtractor).
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(input.clone());
        let result = {
            let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *mode {
                ScriptedGateMode::Decisions(decisions) => {
                    Ok(decisions.pop_front().unwrap_or(GateDecision {
                        participate: false,
                        target_row_id: None,
                        // The scripted path never consulted the gate
                        // model: no reason string.
                        reason: None,
                    }))
                }
                ScriptedGateMode::Failing(message) => Err(CoreError::Wake(message.clone())),
            }
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate_message(row_id: i64, content: &str) -> GateMessage {
        GateMessage {
            row_id,
            platform_msg_id: format!("m{row_id}"),
            content: content.to_string(),
            // The M5 recall fields; the gate prompt reads `content`
            // only, so plausible stand-ins suffice here.
            sender_id: format!("u{row_id}"),
            reply_to_platform_msg_id: None,
            text: content.to_string(),
        }
    }

    fn sample_input() -> GateInput {
        GateInput {
            new_messages: vec![
                gate_message(
                    41,
                    r#"<msg from="Alice" at="13:01" id="41">has anyone tried the new cafe?</msg>"#,
                ),
                gate_message(
                    42,
                    r#"<msg from="Bob" at="13:02" id="42">the espresso is great</msg>"#,
                ),
            ],
            injections: vec![],
            forced: false,
        }
    }

    #[test]
    fn the_prompt_renders_row_ids_and_xml_tagged_content() {
        let prompt = render_gate_prompt(&sample_input());
        // The header names the new XML shape; one line per new message
        // as `{row_id} {content}` with the XML <msg> content of
        // Section 7.2 step 4.
        assert!(prompt.starts_with("New messages (id and XML-tagged content):\n"));
        assert!(prompt.contains(
            r#"41 <msg from="Alice" at="13:01" id="41">has anyone tried the new cafe?</msg>"#
        ));
        assert!(
            prompt.contains(r#"42 <msg from="Bob" at="13:02" id="42">the espresso is great</msg>"#)
        );
    }

    #[test]
    fn empty_injections_render_as_none() {
        // M4: the recall result is part of the gate input on purpose
        // (Section 9.6); an empty set renders explicitly.
        let prompt = render_gate_prompt(&sample_input());
        assert!(prompt.contains("Injected memories:"));
        assert!(prompt.contains("(none)"));
    }

    #[test]
    fn non_empty_injections_render_as_a_list() {
        let mut input = sample_input();
        input.injections = vec!["<memory>Alice likes espresso.</memory>".to_string()];
        let prompt = render_gate_prompt(&input);
        assert!(prompt.contains("- <memory>Alice likes espresso.</memory>"));
        assert!(!prompt.contains("(none)"));
    }

    #[test]
    fn the_gate_output_round_trips_through_json_and_schema() {
        let output = GateOutput {
            participate: true,
            target_msg_id: Some(42),
            reason: "a direct question".to_string(),
        };
        let json = serde_json::to_string(&output).expect("to json");
        let back: GateOutput = serde_json::from_str(&json).expect("from json");
        assert_eq!(back, output);
        // The schema is part of the prompt on the structured-output
        // path; the three fields must appear in it.
        let schema = schemars::schema_for!(GateOutput);
        let schema_json = serde_json::to_string(&schema).expect("schema to json");
        assert!(schema_json.contains("participate"));
        assert!(schema_json.contains("target_msg_id"));
        assert!(schema_json.contains("reason"));
    }

    #[test]
    fn field_name_aliases_deserialize_into_the_canonical_struct() {
        // Endpoints that ignore the output schema free-generate; the
        // aliases tolerate the observed field-name drift
        // (deserialization only).
        let output: GateOutput = serde_json::from_str(
            r#"{"should_participate":false,"target_id":null,"rationale":"r"}"#,
        )
        .expect("aliases");
        assert_eq!(
            output,
            GateOutput {
                participate: false,
                target_msg_id: None,
                reason: "r".to_string(),
            }
        );
        let output: GateOutput =
            serde_json::from_str(r#"{"participate":true,"target_message_id":42,"reason":"r"}"#)
                .expect("target_message_id alias");
        assert_eq!(output.target_msg_id, Some(42));
    }

    #[test]
    fn serialization_keeps_the_canonical_field_names() {
        // Aliases affect deserialization only: the serialized JSON
        // keeps the canonical names, so downstream readers never see
        // the drift spellings.
        let output = GateOutput {
            participate: true,
            target_msg_id: Some(42),
            reason: "r".to_string(),
        };
        let value = serde_json::to_value(&output).expect("to value");
        assert!(value.get("participate").is_some());
        assert!(value.get("target_msg_id").is_some());
        assert!(value.get("reason").is_some());
        assert!(value.get("should_participate").is_none());
        assert!(value.get("target_message_id").is_none());
        assert!(value.get("target_id").is_none());
        assert!(value.get("rationale").is_none());
    }

    #[test]
    fn the_preamble_states_the_scarce_attention_rules() {
        // Principles P4 and P6; Section 12's 50 percent bound.
        assert!(GATE_PREAMBLE.contains("speaks rarely"));
        assert!(GATE_PREAMBLE.contains("stay silent"));
        assert!(GATE_PREAMBLE.contains("50 percent"));
        assert!(GATE_PREAMBLE.contains("the most recent message"));
        assert!(GATE_PREAMBLE.contains("the JSON object of the required schema"));
        // The preamble states the exact output field names (a minimal
        // skeleton): field names must not rely on schema enforcement.
        assert!(GATE_PREAMBLE.contains("\"participate\":true|false"));
        assert!(GATE_PREAMBLE.contains("\"target_msg_id\""));
        assert!(GATE_PREAMBLE.contains("\"reason\""));
    }

    #[test]
    fn the_gate_system_preamble_appends_the_shared_format_gloss() {
        // Decision 61: the gate system message includes the SAME XML
        // format explanation as the persona preamble (single source in
        // tamako-persona). The scarce-attention text stays first; the
        // gloss appends as a clearly separated section.
        let preamble = gate_system_preamble();
        assert!(preamble.starts_with(GATE_PREAMBLE));
        assert!(preamble.ends_with(&format!("\n\n{CONTEXT_FORMAT_GLOSS}")));
        assert!(preamble.contains(CONTEXT_FORMAT_GLOSS));
        // The scarce-attention rules are unchanged and still present.
        assert!(preamble.contains("speaks rarely"));
        assert!(preamble.contains("50 percent"));
    }

    #[test]
    fn post_validation_drops_the_target_of_a_negative_decision() {
        let output = GateOutput {
            participate: false,
            target_msg_id: Some(42),
            reason: "nothing to add".to_string(),
        };
        let decision = gate_decision_from_output(output, &sample_input().new_messages);
        assert_eq!(
            decision,
            GateDecision {
                participate: false,
                target_row_id: None,
                reason: Some("nothing to add".to_string()),
            }
        );
    }

    #[test]
    fn post_validation_passes_a_valid_target_through() {
        let output = GateOutput {
            participate: true,
            target_msg_id: Some(42),
            reason: "Bob asked for a recommendation".to_string(),
        };
        let decision = gate_decision_from_output(output, &sample_input().new_messages);
        assert_eq!(
            decision,
            GateDecision {
                participate: true,
                target_row_id: Some(42),
                reason: Some("Bob asked for a recommendation".to_string()),
            }
        );
    }

    #[test]
    fn post_validation_rejects_a_target_outside_the_presented_set() {
        // A target outside the presented set is a gate malfunction;
        // silence is cheaper than a wrong reply.
        let output = GateOutput {
            participate: true,
            target_msg_id: Some(99),
            reason: "hallucinated id".to_string(),
        };
        let decision = gate_decision_from_output(output, &sample_input().new_messages);
        assert_eq!(
            decision,
            GateDecision {
                participate: false,
                target_row_id: None,
                reason: Some("hallucinated id".to_string()),
            }
        );
    }

    #[test]
    fn post_validation_rejects_a_missing_target_on_participation() {
        let output = GateOutput {
            participate: true,
            target_msg_id: None,
            reason: "no target".to_string(),
        };
        let decision = gate_decision_from_output(output, &sample_input().new_messages);
        assert_eq!(
            decision,
            GateDecision {
                participate: false,
                target_row_id: None,
                reason: Some("no target".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn scripted_gate_pops_fifo_then_returns_silence() {
        let gate = ScriptedGate::with_decisions(vec![GateDecision {
            participate: true,
            target_row_id: Some(41),
            reason: None,
        }]);
        let first = gate.decide(&sample_input()).await.expect("first");
        assert_eq!(
            first,
            GateDecision {
                participate: true,
                target_row_id: Some(41),
                reason: None,
            }
        );
        let second = gate.decide(&sample_input()).await.expect("second");
        assert_eq!(
            second,
            GateDecision {
                participate: false,
                target_row_id: None,
                reason: None,
            }
        );
        assert_eq!(gate.inputs().len(), 2);
        assert_eq!(gate.inputs()[0], sample_input());
    }

    #[tokio::test]
    async fn scripted_gate_failing_mode_fails_every_call_and_records_inputs() {
        let gate = ScriptedGate::failing("boom");
        for _ in 0..2 {
            match gate.decide(&sample_input()).await {
                Err(CoreError::Wake(message)) => assert_eq!(message, "boom"),
                other => panic!("expected Wake error, got {other:?}"),
            }
        }
        assert_eq!(gate.inputs().len(), 2);
    }

    #[test]
    fn from_endpoint_without_an_api_key_is_a_provider_config_error() {
        // The test environment must not carry a key for this assertion.
        use crate::endpoint::env_lock::ENV_LOCK;
        let _lock = ENV_LOCK.lock().unwrap();
        let saved_key = std::env::var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR).ok();
        let saved_family = std::env::var(crate::endpoint::LLM_API_ENV_VAR).ok();
        std::env::remove_var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR);
        std::env::remove_var(crate::endpoint::LLM_API_ENV_VAR);
        let endpoint = EndpointConfig {
            api: crate::endpoint::LlmApi::AnthropicCompatible,
            base_url: None,
            model: "claude-haiku-4-5".to_string(),
            structured_output: crate::endpoint::StructuredOutputMode::Schema,
            session_id: crate::endpoint::DEFAULT_SESSION_ID.to_string(),
        };
        let result = RigGate::from_endpoint(&endpoint);
        if let Some(key) = saved_key {
            std::env::set_var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR, key);
        }
        if let Some(value) = saved_family {
            std::env::set_var(crate::endpoint::LLM_API_ENV_VAR, value);
        }
        match result {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig error, got {other:?}"),
        }
    }
}
