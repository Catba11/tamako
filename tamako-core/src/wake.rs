//! The wake-procedure contracts. specs.md Section 9, steps 1-5. M5
//! wires the recall step (Sections 9.1-9.5): the recall result carries
//! the planned injections of the wake.
//!
//! The gate and reply implementations live in the tamako-agent crate
//! (Phase 1, M4); the M5 recall worker follows them. tamako-core
//! defines the contracts so the actor can drive the wake
//! procedure without a dependency on the agent crate (no dependency
//! cycles, AGENT.md Section 4). Same pattern as `digest.rs`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::actor::CoreError;
use crate::context::ContextMessage;

/// One message presented to the participation gate (Section 9.6 input).
/// `content` is the rendered speaker-label form
/// `[{display_name} {HH:MM}] {text}` (Section 7.2 step 4) — the same
/// render helper as the live context, so the gate input stays consistent
/// with what the reply model sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateMessage {
    /// The raw-log row id of this message.
    pub row_id: i64,
    /// The platform-side message id.
    pub platform_msg_id: String,
    /// The rendered speaker-label content (Section 7.2 step 4).
    pub content: String,
    /// The sender id of the message (recall entry resolution,
    /// proposed-graph-database-specs.md Section 8.1 step 1).
    pub sender_id: String,
    /// The platform id of the reply target, when the message is a reply.
    pub reply_to_platform_msg_id: Option<String>,
    /// The raw message text (recall term tokenization). `content`
    /// stays the rendered speaker-label form of Section 7.2 step 4.
    pub text: String,
}

/// One planned recall injection (Section 9.4): the rendered
/// "I remember: ..." assistant message plus the edge ids of the
/// memories it carries (Section 9.3 dedup).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedInjection {
    /// The dedup keys of the injected edges (the natural-key
    /// string of tamako-memory's NeighborEdge::edge_id).
    pub edge_ids: Vec<String>,
    /// The rendered injection text: "I remember: ...".
    pub content: String,
}

/// The recall result of one wake (Section 9 step 2). M5 produces
/// at most one injection per wake; the Vec keeps the seam open.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecallOutcome {
    pub injections: Vec<PlannedInjection>,
}

/// The gate input (Section 9.6): the new messages of this wake plus the
/// injected memories of the recall step (Section 9.6: the recall result
/// is gate input on purpose).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateInput {
    /// The new messages of this wake: the raw-log rows above
    /// `wake_last_row_id`, rendered as gate messages.
    pub new_messages: Vec<GateMessage>,
    /// The rendered injection texts of the recall step ("I remember:
    /// ..."). Empty means no injection (Section 9.2: an empty
    /// injection is forbidden — nothing is injected).
    pub injections: Vec<String>,
    /// True for a forced wake (mention/reply, Section 8.1).
    pub forced: bool,
}

/// The gate output (Section 9.6): a binary decision with the target
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateDecision {
    /// True when the bot participates in this wake.
    pub participate: bool,
    /// The raw-log row id of the message the reply targets. `None` when
    /// `participate` is false.
    pub target_row_id: Option<i64>,
}

/// The recall seam (Section 9 step 2; Sections 9.1-9.5). The wake
/// calls recall before the gate. The injections of the result enter the
/// gate input (Section 9.6) and the context (Section 9.4, Rule C2).
/// An empty `injections` vec of the `RecallOutcome` means no injection
/// (Section 9.2: an empty injection is forbidden — nothing is
/// injected).
pub trait RecallProvider: Send + Sync {
    fn recall<'a>(
        &'a self,
        chat_id: &'a str,
        new_messages: &'a [GateMessage],
    ) -> Pin<Box<dyn Future<Output = Result<RecallOutcome, CoreError>> + Send + 'a>>;
}

/// The no-op recall: no injections. The binary wires it when no LLM
/// key is configured.
pub struct NoopRecall;

impl RecallProvider for NoopRecall {
    fn recall<'a>(
        &'a self,
        _chat_id: &'a str,
        _new_messages: &'a [GateMessage],
    ) -> Pin<Box<dyn Future<Output = Result<RecallOutcome, CoreError>> + Send + 'a>> {
        Box::pin(async { Ok(RecallOutcome::default()) })
    }
}

/// The participation decision (Section 9.6). The live rig implementation
/// and the scripted test implementation live in tamako-agent (same
/// pattern as M1's KnowledgeExtractor). Structured output via JSON
/// schema, over the cheap `gate_model`.
///
/// Forced wakes (mention/reply, Section 8.1) BYPASS this gate: the actor
/// never calls `decide` for a forced wake.
pub trait ParticipationGate: Send + Sync {
    fn decide<'a>(
        &'a self,
        input: &'a GateInput,
    ) -> Pin<Box<dyn Future<Output = Result<GateDecision, CoreError>> + Send + 'a>>;
}

/// The reply request: the live context as LLM-facing messages (preamble
/// first, from `LiveContext::messages_for_llm`) plus the chosen target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyRequest {
    /// Preamble first; tamako-agent converts these to rig completion
    /// messages (the M2 seam).
    pub messages: Vec<ContextMessage>,
    /// The target message of the reply (the gate's choice).
    pub target: GateMessage,
}

/// Reply generation with the main model (`reply_model`), Section 9
/// step 4.
pub trait ReplyGenerator: Send + Sync {
    fn generate<'a>(
        &'a self,
        request: &'a ReplyRequest,
    ) -> Pin<Box<dyn Future<Output = Result<String, CoreError>> + Send + 'a>>;
}

/// The bundle the actor needs to run the wake procedure. `None` in
/// `GroupActorParams` keeps the stub behavior (a later subtask wires
/// this).
pub struct WakeServices {
    /// The recall seam (Section 9 step 2). The binary wires
    /// `NoopRecall` when no LLM key is configured.
    pub recall: Arc<dyn RecallProvider>,
    /// The participation gate (Section 9.6). Runs only for unforced
    /// wakes (Section 8.1).
    pub gate: Arc<dyn ParticipationGate>,
    /// The reply generator (Section 9 step 4).
    pub reply: Arc<dyn ReplyGenerator>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_gate_message() -> GateMessage {
        GateMessage {
            row_id: 7,
            platform_msg_id: "m7".to_string(),
            content: "[Alice 13:07] hello".to_string(),
            sender_id: "u1".to_string(),
            reply_to_platform_msg_id: None,
            text: "hello".to_string(),
        }
    }

    #[tokio::test]
    async fn the_noop_recall_returns_no_injections() {
        // Section 9.2: an empty injection is forbidden — nothing is
        // injected. The no-op always returns the empty outcome.
        let recall = NoopRecall;
        let messages = vec![sample_gate_message()];
        let outcome = recall
            .recall("chat", &messages)
            .await
            .expect("the no-op recall never fails");
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[test]
    fn the_traits_are_object_safe() {
        // The actor holds Arc<dyn ...> of each trait (see WakeServices).
        // This assertion keeps the traits object-safe.
        fn assert_object_safe(
            _: Option<Arc<dyn RecallProvider>>,
            _: Option<Arc<dyn ParticipationGate>>,
            _: Option<Arc<dyn ReplyGenerator>>,
        ) {
        }
        assert_object_safe(None, None, None);
    }
}
