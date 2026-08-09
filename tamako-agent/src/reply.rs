//! Reply generation: specs.md Section 9, step 4. On a participate
//! decision, the bot generates one reply with the main `reply_model`
//! over the live context (`LiveContext::messages_for_llm()`).
//!
//! This module is the M2-documented conversion seam: tamako-core stays
//! model-agnostic (`ContextMessage`); this is the ONLY place that maps
//! core context messages to rig completion messages.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use rig::completion::Message;

use tamako_core::actor::CoreError;
use tamako_core::context::{ContextMessage, ContextRole};
use tamako_core::wake::{GateMessage, ReplyGenerator, ReplyRequest};

use crate::endpoint::{EndpointClient, EndpointConfig};
use crate::extract::AgentError;

/// The default max tokens of the reply. A pet reply is short: one chat
/// message, one or two sentences. 1024 tokens is a generous bound.
pub const REPLY_DEFAULT_MAX_TOKENS: u64 = 1024;

/// Converts core context messages to the rig call shape (the M2 seam;
/// the ONLY conversion point — tamako-core stays model-agnostic).
///
/// The first message (always the preamble item 0, role System, Rule C4)
/// becomes the rig preamble. The rest map `ContextRole::User` →
/// `Message::user` and `ContextRole::Assistant` → `Message::assistant`.
/// A defensive late System maps to a user message: only item 0 is
/// System by construction, so a late System is malformed input; mapping
/// it to a user message keeps the content instead of dropping it.
///
/// A leading non-System message means there is no preamble (defensive;
/// Rule C4 guarantees one) — the preamble comes back `None`.
pub fn context_messages_to_rig(messages: &[ContextMessage]) -> (Option<String>, Vec<Message>) {
    let (preamble, rest) = match messages.first() {
        Some(first) if first.role == ContextRole::System => {
            (Some(first.content.clone()), &messages[1..])
        }
        _ => (None, messages),
    };
    let rig_messages = rest
        .iter()
        .map(|message| match message.role {
            ContextRole::User => Message::user(message.content.clone()),
            ContextRole::Assistant => Message::assistant(message.content.clone()),
            // Defensive: only item 0 is System by construction (Rule C4).
            ContextRole::System => Message::user(message.content.clone()),
        })
        .collect();
    (preamble, rig_messages)
}

/// Renders the trailing ephemeral user message of the reply call
/// (Section 9 step 4). It names the target: the raw-log row id plus the
/// target content verbatim.
///
/// This instruction is part of the reply CALL input only. It is never
/// appended to the live context: the live context holds group speech
/// and bot speech (Section 7.1), not per-call scaffolding.
pub fn render_reply_instruction(target: &GateMessage) -> String {
    format!(
        "Reply to THIS message (id {}): {}\n\
         Reply as the group pet persona. Write only the reply text: \
         one message, no speaker label, no quotes.",
        target.row_id, target.content
    )
}

/// Trims the model output. An empty or whitespace-only reply is a wake
/// error: the bot never sends an empty message (Section 9 step 4: log,
/// skip this wake, no crash). Pure function, no I/O.
fn trimmed_reply_or_error(text: &str) -> Result<String, CoreError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        Err(CoreError::Wake(
            "the reply model returned an empty reply".to_string(),
        ))
    } else {
        Ok(trimmed.to_string())
    }
}

/// The live reply generator: one completion call on the main
/// `reply_model` endpoint (specs.md Section 9 step 4 and Section 13).
/// Plain text output; no output schema.
pub struct RigReplyGenerator {
    client: EndpointClient,
    max_tokens: u64,
}

// The rig model handles do not implement Debug. A manual impl keeps
// RigReplyGenerator printable in test failures and logs (same pattern
// as RigExtractor).
impl std::fmt::Debug for RigReplyGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigReplyGenerator")
            .field("client", &self.client)
            .field("max_tokens", &self.max_tokens)
            .finish_non_exhaustive()
    }
}

impl RigReplyGenerator {
    /// Builds the generator from an endpoint client.
    pub fn new(client: EndpointClient, max_tokens: u64) -> Self {
        RigReplyGenerator { client, max_tokens }
    }

    /// Builds the generator for one resolved endpoint (the `reply`
    /// purpose, specs.md Section 13). Returns
    /// `AgentError::ProviderConfig` when the family API key is missing.
    pub fn from_endpoint(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        Ok(RigReplyGenerator::new(
            EndpointClient::build(endpoint)?,
            REPLY_DEFAULT_MAX_TOKENS,
        ))
    }
}

impl ReplyGenerator for RigReplyGenerator {
    fn generate<'a>(
        &'a self,
        request: &'a ReplyRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, CoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            let (preamble, mut messages) = context_messages_to_rig(&request.messages);
            // The trailing ephemeral instruction names the target. It
            // is part of the reply call only, never of the live context.
            messages.push(Message::user(render_reply_instruction(&request.target)));
            let text = self
                .client
                .complete(
                    preamble,
                    messages,
                    // No schema: the reply is plain text.
                    None,
                    self.max_tokens,
                )
                .await
                // A reply failure skips this wake; the next wake is the
                // natural retry (CoreError::Wake docs).
                .map_err(|error| CoreError::Wake(error.to_string()))?;
            trimmed_reply_or_error(&text)
        })
    }
}

/// The response mode of `ScriptedReplyGenerator`.
enum ScriptedReplyMode {
    /// Pops the next reply per call (FIFO). An exhausted queue fails
    /// with `CoreError::Wake`.
    Replies(VecDeque<String>),
    /// Every call fails with `CoreError::Wake`.
    Failing(String),
}

/// A scripted reply generator for tests (same pattern as
/// `ScriptedExtractor`). Two modes:
///
/// - `ScriptedReplyGenerator::with_replies(vec_of_replies)`: pops the
///   next reply per call (FIFO; when exhausted, every call fails with
///   `CoreError::Wake("scripted replies exhausted")`);
/// - `ScriptedReplyGenerator::failing(message)`: every call fails with
///   `CoreError::Wake`.
///
/// Every `ReplyRequest` is recorded for assertions (`requests()`).
pub struct ScriptedReplyGenerator {
    mode: Mutex<ScriptedReplyMode>,
    requests: Mutex<Vec<ReplyRequest>>,
}

impl ScriptedReplyGenerator {
    /// A scripted generator that answers with the given replies in
    /// order.
    pub fn with_replies(replies: Vec<String>) -> Self {
        ScriptedReplyGenerator {
            mode: Mutex::new(ScriptedReplyMode::Replies(replies.into())),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// A scripted generator whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedReplyGenerator {
            mode: Mutex::new(ScriptedReplyMode::Failing(message.into())),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// Every request the generator received, in call order.
    pub fn requests(&self) -> Vec<ReplyRequest> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ReplyGenerator for ScriptedReplyGenerator {
    fn generate<'a>(
        &'a self,
        request: &'a ReplyRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, CoreError>> + Send + 'a>>
    {
        // Lock, record, and decide synchronously; the future only
        // carries the result. A poisoned mutex is recovered; the
        // recorded requests stay valid (same policy as
        // ScriptedExtractor).
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.clone());
        let result = {
            let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *mode {
                ScriptedReplyMode::Replies(replies) => replies
                    .pop_front()
                    .ok_or_else(|| CoreError::Wake("scripted replies exhausted".to_string())),
                ScriptedReplyMode::Failing(message) => Err(CoreError::Wake(message.clone())),
            }
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context_message(role: ContextRole, content: &str) -> ContextMessage {
        ContextMessage {
            role,
            content: content.to_string(),
        }
    }

    fn sample_target() -> GateMessage {
        GateMessage {
            row_id: 42,
            platform_msg_id: "m42".to_string(),
            content: "[Bob 13:02] what should we eat?".to_string(),
            // The M5 recall fields; the reply path reads `content` and
            // `platform_msg_id` only, so plausible stand-ins suffice.
            sender_id: "u2".to_string(),
            reply_to_platform_msg_id: None,
            text: "what should we eat?".to_string(),
        }
    }

    fn sample_request() -> ReplyRequest {
        ReplyRequest {
            messages: vec![
                context_message(ContextRole::System, "You are the group pet."),
                context_message(ContextRole::User, "[Alice 13:01] hungry"),
                context_message(ContextRole::Assistant, "the cafe on main street"),
                context_message(ContextRole::User, "[Bob 13:02] what should we eat?"),
            ],
            target: sample_target(),
        }
    }

    #[test]
    fn the_conversion_extracts_the_item_0_preamble() {
        // Rule C4: item 0 is the system preamble.
        let (preamble, messages) = context_messages_to_rig(&sample_request().messages);
        assert_eq!(preamble.as_deref(), Some("You are the group pet."));
        assert_eq!(messages.len(), 3);
    }

    /// The (role, text) view of one rig message for assertions. rig's
    /// `Message` carries `OneOrMany` content items; the conversion
    /// produces single text items only.
    fn role_and_text(message: &Message) -> (&'static str, String) {
        use rig::completion::message::{AssistantContent, UserContent};
        match message {
            Message::User { content } => match content.first_ref() {
                UserContent::Text(text) => ("user", text.text.clone()),
                other => panic!("expected user text, got {other:?}"),
            },
            Message::Assistant { content, .. } => match content.first_ref() {
                AssistantContent::Text(text) => ("assistant", text.text.clone()),
                other => panic!("expected assistant text, got {other:?}"),
            },
            Message::System { content } => ("system", content.clone()),
        }
    }

    #[test]
    fn the_conversion_maps_user_and_assistant_roles() {
        let (_, messages) = context_messages_to_rig(&sample_request().messages);
        let rendered: Vec<_> = messages.iter().map(role_and_text).collect();
        assert_eq!(
            rendered,
            vec![
                ("user", "[Alice 13:01] hungry".to_string()),
                ("assistant", "the cafe on main street".to_string()),
                ("user", "[Bob 13:02] what should we eat?".to_string()),
            ]
        );
    }

    #[test]
    fn a_late_system_maps_to_a_user_message() {
        // Defensive: only item 0 is System by construction (Rule C4).
        let messages = vec![
            context_message(ContextRole::System, "preamble"),
            context_message(ContextRole::User, "first"),
            context_message(ContextRole::System, "malformed late system"),
        ];
        let (preamble, rig_messages) = context_messages_to_rig(&messages);
        assert_eq!(preamble.as_deref(), Some("preamble"));
        let rendered: Vec<_> = rig_messages.iter().map(role_and_text).collect();
        assert_eq!(
            rendered,
            vec![
                ("user", "first".to_string()),
                ("user", "malformed late system".to_string()),
            ]
        );
    }

    #[test]
    fn a_leading_non_system_message_means_no_preamble() {
        // Defensive: Rule C4 guarantees a preamble, but the conversion
        // must not eat a user message as the preamble.
        let messages = vec![context_message(ContextRole::User, "orphan")];
        let (preamble, rig_messages) = context_messages_to_rig(&messages);
        assert_eq!(preamble, None);
        assert_eq!(rig_messages.len(), 1);
    }

    #[test]
    fn the_reply_instruction_names_the_target_verbatim() {
        let instruction = render_reply_instruction(&sample_target());
        assert!(instruction.contains("id 42"));
        assert!(instruction.contains("[Bob 13:02] what should we eat?"));
        assert!(instruction.contains("no speaker label"));
        assert!(instruction.contains("no quotes"));
    }

    #[test]
    fn an_empty_or_whitespace_reply_is_a_wake_error() {
        for empty in ["", "   ", "\n\t "] {
            match trimmed_reply_or_error(empty) {
                Err(CoreError::Wake(message)) => {
                    assert_eq!(message, "the reply model returned an empty reply")
                }
                other => panic!("expected Wake error for {empty:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_reply_is_trimmed() {
        let reply = trimmed_reply_or_error("  the cafe on main street \n").expect("reply");
        assert_eq!(reply, "the cafe on main street");
    }

    #[tokio::test]
    async fn scripted_replies_pop_fifo_then_fail() {
        let generator = ScriptedReplyGenerator::with_replies(vec!["pizza".to_string()]);
        let first = generator.generate(&sample_request()).await.expect("first");
        assert_eq!(first, "pizza");
        match generator.generate(&sample_request()).await {
            Err(CoreError::Wake(message)) => assert_eq!(message, "scripted replies exhausted"),
            other => panic!("expected Wake error, got {other:?}"),
        }
        assert_eq!(generator.requests().len(), 2);
        assert_eq!(generator.requests()[0], sample_request());
    }

    #[tokio::test]
    async fn scripted_failing_mode_fails_every_call_and_records_requests() {
        let generator = ScriptedReplyGenerator::failing("boom");
        for _ in 0..2 {
            match generator.generate(&sample_request()).await {
                Err(CoreError::Wake(message)) => assert_eq!(message, "boom"),
                other => panic!("expected Wake error, got {other:?}"),
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
        };
        let result = RigReplyGenerator::from_endpoint(&endpoint);
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
