//! Warmup generation: specs.md Section 9.7 step 3 (decision 78). On a
//! warmup trigger, the bot generates one casual opener with the REPLY
//! purpose over the live context plus the sampled topic.
//!
//! A sibling seam of `reply.rs`, NOT a mode of it: the warmup has no
//! reply target, so the decision-59 reply path stays byte-identical.
//! The rig conversion seam stays single — this module imports
//! `context_messages_to_rig` from `reply.rs` (the M2-documented ONLY
//! conversion point).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use rig::completion::Message;

use tamako_core::actor::CoreError;
use tamako_core::wake::{filter_reply_parrot_lines, ReplyFence, ReplyFilterOutcome};
use tamako_core::warmup::{WarmupGenerator, WarmupRequest};

use crate::endpoint::{EndpointClient, EndpointConfig, LlmPurpose};
use crate::extract::AgentError;
use crate::reply::{context_messages_to_rig, REPLY_DEFAULT_MAX_TOKENS};

/// The F2 sentence of the warmup instruction — VERBATIM the sentence
/// of `crate::reply::render_reply_instruction` (decision 59 F2 +
/// decision 64). Duplicated rather than shared: reply.rs renders it
/// inline in its format string and sharing a const would churn that
/// renderer. Keep the two in sync; the test
/// `the_f2_sentence_matches_the_reply_instruction_verbatim` asserts
/// the duplication never drifts.
const F2_SENTENCE: &str = "Never write \"I remember:\" lines, <memory> blocks, <summary> blocks, <msg> blocks, or a memory list: recalled memories are context, never speech.";

/// Renders the trailing ephemeral user message of the warmup call
/// (specs.md Section 9.7 step 3; decision 78). It names the topic in
/// PLAIN QUOTES (the decision-64 analog): embedding any XML context
/// shape at the highest-salience position of the call would teach the
/// model the exact shapes the F2 sentence forbids.
///
/// The framing rule of decision 69 rides along: an interest attached
/// to a specific person is framed as an OPEN QUESTION TO THE WHOLE
/// GROUP — never "X likes Y", never an attribution to a specific
/// member. The topic is reference material, not a claim about a
/// person.
///
/// This instruction is part of the warmup CALL input only. It is never
/// appended to the live context: the live context holds group speech
/// and bot speech (Section 7.1), not per-call scaffolding (the same
/// discipline as `render_reply_instruction`).
///
/// The F2 tail sentence stays in sync with the outbound parrot filter
/// of tamako-core (decision 59 F1 + decision 64): the filter strips
/// `I remember:` lines, `<memory>` blocks, and `<summary>` blocks from
/// the warmup text exactly as from a reply (decision 59/64: the parrot
/// filter applies like every reply).
pub fn render_warmup_instruction(topic: &str) -> String {
    format!(
        "Start a casual conversation with the group about this topic: \"{topic}\"\n\
         Ask an open question the whole group can answer. The topic is reference \
         material, not a claim about a person: never attribute the interest to a \
         specific member and never write \"X likes Y\" or name who brought it up. \
         Write only the message text: one message, no speaker label, no quotes. \
         {F2_SENTENCE}"
    )
}

/// Trims the model output and applies the parrot filter
/// (`tamako_core::wake::filter_reply_parrot_lines`, decision 59 F1) at
/// the warmup generator's own validation seam — the actor filters
/// every generator output again (the same double-seam discipline as
/// `reply::trimmed_reply_or_error`). Decision 59/64: the parrot filter
/// applies to the warmup like every reply. Decision 96 (F5/C4): the
/// parrot-strip WARN fires AT THIS SEAM, carrying the purpose and the
/// resolved model name — pre-96 it sat on the actor's idempotent
/// second pass, which sees the already-filtered text on the live
/// path, so it never fired and live parrot events were invisible.
///
/// An empty or whitespace-only remainder — including a warmup that was
/// ONLY a parrot block — is a `CoreError::Warmup`: log, skip this
/// warmup, no crash (the next scheduled slot is the natural retry).
/// Pure function, no I/O.
fn trimmed_warmup_or_error(
    text: &str,
    fence: &ReplyFence,
    purpose: &str,
    model: &str,
    chat_id: &str,
) -> Result<ReplyFilterOutcome, CoreError> {
    // Warmup stays hygiene-only (no fence extraction — decision 93's
    // scope): the fence of the pet tag drives the residual-token rules,
    // which is also what cleans up the R1 attribute-carrying pairs the
    // unified tag invites (decision 95, the C1 fix).
    let filtered = filter_reply_parrot_lines(text, fence);
    if filtered.stripped_parrot {
        tracing::warn!(
            purpose,
            model,
            chat_id = %chat_id,
            "the warmup text carries imitated structure or fence debris: the parrot filter stripped the affected lines"
        );
    }
    if filtered.text.is_empty() {
        Err(CoreError::Warmup(
            "the warmup generator returned an empty message".to_string(),
        ))
    } else {
        Ok(filtered)
    }
}

/// The live warmup generator: one completion call on the resolved
/// `reply` endpoint (specs.md Section 9.7 step 3 and Section 13;
/// decision 78 (a)). Plain text output; no output schema.
pub struct RigWarmupGenerator {
    client: EndpointClient,
    max_tokens: u64,
    /// The decision-95 pet tag, behind the same shared-lock discipline
    /// as `RigReplyGenerator::pet_tag`: a persona `name` change swaps
    /// it on the next request. Default `"you"` (the legacy fallback);
    /// production always wires the real derivation.
    pet_tag: Arc<RwLock<String>>,
}

// The rig model handles do not implement Debug. A manual impl keeps
// RigWarmupGenerator printable in test failures and logs (same pattern
// as RigReplyGenerator).
impl std::fmt::Debug for RigWarmupGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigWarmupGenerator")
            .field("client", &self.client)
            .field("max_tokens", &self.max_tokens)
            .finish_non_exhaustive()
    }
}

impl RigWarmupGenerator {
    /// Builds the generator from an endpoint client.
    pub fn new(client: EndpointClient, max_tokens: u64) -> Self {
        RigWarmupGenerator {
            client,
            max_tokens,
            pet_tag: Arc::new(RwLock::new("you".to_string())),
        }
    }

    /// Wires the shared decision-95 pet-tag slot (same contract as
    /// `RigReplyGenerator::with_pet_tag_slot`).
    pub fn with_pet_tag_slot(mut self, pet_tag: Arc<RwLock<String>>) -> Self {
        self.pet_tag = pet_tag;
        self
    }

    /// Builds the generator for one resolved endpoint (the `reply`
    /// purpose, specs.md Section 13 — the warmup is a reply-purpose
    /// call). Returns `AgentError::ProviderConfig` when the family API
    /// key is missing.
    pub fn from_endpoint(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        Ok(RigWarmupGenerator::new(
            // The warmup is a reply-purpose call (the doc comment
            // above); stamp it for the curated usage line.
            EndpointClient::build_for_purpose(endpoint, LlmPurpose::Reply)?,
            REPLY_DEFAULT_MAX_TOKENS,
        ))
    }
}

impl WarmupGenerator for RigWarmupGenerator {
    fn generate_warmup<'a>(
        &'a self,
        chat_id: &'a str,
        request: &'a WarmupRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, CoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            let (preamble, mut messages) = context_messages_to_rig(&request.messages);
            // The trailing ephemeral instruction names the topic. It
            // is part of the warmup call only, never of the live
            // context.
            messages.push(Message::user(render_warmup_instruction(&request.topic)));
            let text = self
                .client
                .complete(
                    preamble,
                    messages,
                    // No schema: the warmup is plain text.
                    None,
                    self.max_tokens,
                )
                .await
                // A warmup failure skips this trigger; the next
                // scheduled slot is the natural retry
                // (CoreError::Warmup docs).
                .map_err(|error| CoreError::Warmup(error.to_string()))?;
            // The parrot filter runs here, at the warmup-text
            // validation seam of the live generator (decision 59, F1)
            // — with its WARN at the seam (decision 96, F5).
            let pet_tag = self
                .pet_tag
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            Ok(trimmed_warmup_or_error(
                &text,
                &ReplyFence::for_pet_tag(&pet_tag),
                self.client.purpose(),
                self.client.model_name(),
                chat_id,
            )?
            .text)
        })
    }
}

/// The response mode of `ScriptedWarmupGenerator`.
enum ScriptedWarmupMode {
    /// Pops the next reply per call (FIFO). An exhausted queue fails
    /// with `CoreError::Warmup`.
    Replies(VecDeque<String>),
    /// Every call fails with `CoreError::Warmup`.
    Failing(String),
}

/// A scripted warmup generator for tests and the tamako integration
/// suite (same pattern as `ScriptedReplyGenerator`). Two modes:
///
/// - `ScriptedWarmupGenerator::with_replies(vec_of_replies)`: pops the
///   next reply per call (FIFO; when exhausted, every call fails with
///   `CoreError::Warmup("scripted warmup replies exhausted")`);
/// - `ScriptedWarmupGenerator::failing(message)`: every call fails
///   with `CoreError::Warmup`.
///
/// Every `WarmupRequest` is recorded for assertions (`requests()`).
pub struct ScriptedWarmupGenerator {
    mode: Mutex<ScriptedWarmupMode>,
    requests: Mutex<Vec<WarmupRequest>>,
}

impl ScriptedWarmupGenerator {
    /// A scripted generator that answers with the given replies in
    /// order.
    pub fn with_replies(replies: Vec<String>) -> Self {
        ScriptedWarmupGenerator {
            mode: Mutex::new(ScriptedWarmupMode::Replies(replies.into())),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// A scripted generator whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedWarmupGenerator {
            mode: Mutex::new(ScriptedWarmupMode::Failing(message.into())),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// Every request the generator received, in call order.
    pub fn requests(&self) -> Vec<WarmupRequest> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl WarmupGenerator for ScriptedWarmupGenerator {
    fn generate_warmup<'a>(
        &'a self,
        _chat_id: &'a str,
        request: &'a WarmupRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, CoreError>> + Send + 'a>>
    {
        // Lock, record, and decide synchronously; the future only
        // carries the result. A poisoned mutex is recovered; the
        // recorded requests stay valid (same policy as
        // ScriptedReplyGenerator).
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.clone());
        let result = {
            let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *mode {
                ScriptedWarmupMode::Replies(replies) => replies.pop_front().ok_or_else(|| {
                    CoreError::Warmup("scripted warmup replies exhausted".to_string())
                }),
                ScriptedWarmupMode::Failing(message) => Err(CoreError::Warmup(message.clone())),
            }
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tamako_core::context::{ContextMessage, ContextRole};
    use tamako_core::wake::GateMessage;
    /// The fence built from the legacy tag name `reply` (decision 95;
    /// same discipline as the reply.rs test helper).
    fn test_fence() -> ReplyFence {
        ReplyFence::for_pet_tag("reply")
    }

    fn context_message(role: ContextRole, content: &str) -> ContextMessage {
        ContextMessage {
            role,
            content: content.to_string(),
        }
    }

    fn sample_request() -> WarmupRequest {
        WarmupRequest {
            messages: vec![
                context_message(ContextRole::System, "You are the group pet."),
                context_message(ContextRole::User, "[Alice 13:01] hungry"),
                context_message(ContextRole::Assistant, "the cafe on main street"),
            ],
            topic: "Graph Database".to_string(),
        }
    }

    /// A reply target for the F2 sync test (the shape of reply.rs's
    /// `sample_target`).
    fn reply_target() -> GateMessage {
        GateMessage {
            row_id: 42,
            platform_msg_id: "m42".to_string(),
            content: r#"<msg from="Bob" at="13:02" id="42">what should we eat?</msg>"#.to_string(),
            sender_id: "u2".to_string(),
            reply_to_platform_msg_id: None,
            text: "what should we eat?".to_string(),
        }
    }

    #[test]
    fn the_warmup_instruction_names_the_topic_verbatim_in_plain_quotes() {
        let instruction = render_warmup_instruction("Graph Database");
        // The topic embed is the plain-quotes form (the decision-64
        // analog).
        assert!(instruction.contains("about this topic: \"Graph Database\""));
        // The F2 sentence names the shapes by tag, so assert against
        // the full WRAPPER forms only (the embed must not teach them).
        assert!(!instruction.contains("<msg from="));
        assert!(!instruction.contains("<you at="));
    }

    #[test]
    fn the_warmup_instruction_carries_the_decision_69_framing_rule() {
        // Decision 69: an interest attached to a specific person is an
        // open question to the WHOLE group, never "X likes Y".
        let instruction = render_warmup_instruction("Graph Database");
        assert!(instruction.contains("Ask an open question the whole group can answer."));
        assert!(instruction.contains("never attribute the interest to a specific member"));
        assert!(instruction.contains("never write \"X likes Y\""));
        assert!(instruction.contains("not a claim about a person"));
    }

    #[test]
    fn the_f2_sentence_matches_the_reply_instruction_verbatim() {
        // Single-source discipline by test (see the sync comment on
        // F2_SENTENCE): the warmup instruction must end with EXACTLY
        // the F2 sentence of the reply instruction.
        let reply_instruction = crate::reply::render_reply_instruction(&reply_target(), "tamako");
        assert!(reply_instruction.ends_with(F2_SENTENCE));
        assert!(render_warmup_instruction("Graph Database").ends_with(F2_SENTENCE));
    }

    #[test]
    fn the_warmup_instruction_renders_byte_exact() {
        // The full ephemeral instruction, byte-exact: the plain-quotes
        // topic embed (the decision-64 analog), the decision-69 framing
        // rule, and the F2 sentence naming every shape the outbound
        // parrot filter strips.
        let expected = concat!(
            "Start a casual conversation with the group about this topic: \"Graph Database\"",
            "\n",
            "Ask an open question the whole group can answer. ",
            "The topic is reference material, not a claim about a person: ",
            "never attribute the interest to a specific member and never write \"X likes Y\" or name who brought it up. ",
            "Write only the message text: one message, no speaker label, no quotes. ",
            "Never write \"I remember:\" lines, <memory> blocks, <summary> blocks, <msg> blocks, or a memory list: ",
            "recalled memories are context, never speech.",
        );
        assert_eq!(render_warmup_instruction("Graph Database"), expected);
    }

    #[test]
    fn an_empty_or_whitespace_warmup_is_a_warmup_error() {
        for empty in ["", "   ", "\n\t "] {
            match trimmed_warmup_or_error(empty, &test_fence(), "warmup", "test-model", "c1") {
                Err(CoreError::Warmup(message)) => {
                    assert_eq!(message, "the warmup generator returned an empty message")
                }
                other => panic!("expected Warmup error for {empty:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_warmup_of_only_a_parrot_block_is_the_empty_message_error() {
        // Decision 59, F1: nothing remains after the strip, so the
        // warmup follows the same skip path as an empty warmup — the
        // same CoreError::Warmup, nothing persisted, nothing sent.
        for only in [
            "I remember: Alice likes tea.",
            "  I remember: Alice likes tea.\nI remember：小明喜欢吃辣。 ",
            "<memory>\nAlice likes tea.\n</memory>",
        ] {
            match trimmed_warmup_or_error(only, &test_fence(), "warmup", "test-model", "c1") {
                Err(CoreError::Warmup(message)) => {
                    assert_eq!(message, "the warmup generator returned an empty message")
                }
                other => panic!("expected Warmup error for {only:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_leading_parrot_line_is_stripped() {
        // Decision 59, F1: the model echoed the injection format before
        // its real speech (the live-soak failure shape).
        let warmup = trimmed_warmup_or_error(
            "I remember: Alice likes tea.\nhas anyone tried the new cafe?",
            &test_fence(),
            "warmup",
            "test-model",
            "c1",
        )
        .expect("warmup");
        assert_eq!(warmup.text, "has anyone tried the new cafe?");
        assert!(warmup.stripped_parrot);
    }

    #[test]
    fn normal_text_passes_byte_identical() {
        // False-positive control: an innocuous mid-line "I remember"
        // mention and multiline text survive untouched (line-start
        // anchored only) — the reply.rs control shapes.
        for normal in [
            "I remember when we tried that place",
            "在的",
            "one\ntwo\nthree",
            "hungry? I remember: not a line start",
        ] {
            let warmup =
                trimmed_warmup_or_error(normal, &test_fence(), "warmup", "test-model", "c1")
                    .expect("warmup");
            assert_eq!(warmup.text, normal.trim());
            assert!(!warmup.stripped_parrot, "false positive on {normal:?}");
        }
    }

    #[tokio::test]
    async fn scripted_warmup_replies_pop_fifo_then_fail() {
        let generator = ScriptedWarmupGenerator::with_replies(vec!["tea anyone?".to_string()]);
        let first = generator
            .generate_warmup("c1", &sample_request())
            .await
            .expect("first");
        assert_eq!(first, "tea anyone?");
        match generator.generate_warmup("c1", &sample_request()).await {
            Err(CoreError::Warmup(message)) => {
                assert_eq!(message, "scripted warmup replies exhausted")
            }
            other => panic!("expected Warmup error, got {other:?}"),
        }
        assert_eq!(generator.requests().len(), 2);
        // The WarmupRequest round-trips through the recording seam.
        assert_eq!(generator.requests()[0], sample_request());
    }

    #[tokio::test]
    async fn scripted_failing_mode_fails_every_call_and_records_requests() {
        let generator = ScriptedWarmupGenerator::failing("boom");
        for _ in 0..2 {
            match generator.generate_warmup("c1", &sample_request()).await {
                Err(CoreError::Warmup(message)) => assert_eq!(message, "boom"),
                other => panic!("expected Warmup error, got {other:?}"),
            }
        }
        assert_eq!(generator.requests().len(), 2);
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
            model: "claude-sonnet-4-5".to_string(),
            structured_output: crate::endpoint::StructuredOutputMode::Schema,
            session_id: crate::endpoint::DEFAULT_SESSION_ID.to_string(),
        };
        let result = RigWarmupGenerator::from_endpoint(&endpoint);
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
