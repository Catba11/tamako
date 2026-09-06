//! Live-API smoke test of the wake LLM calls: the participation gate
//! (specs.md Section 9.6) and reply generation (specs.md Section 9
//! step 4). Ignored by default; run with `TAMAKO_LIVE_TEST=1` and
//! `ANTHROPIC_API_KEY` set:
//!
//! ```sh
//! TAMAKO_LIVE_TEST=1 cargo test -p tamako-agent --test live_wake -- --ignored
//! ```

use tamako_agent::{LlmConfigValues, LlmEndpoints, RigGate, RigReplyGenerator};
use tamako_core::context::{ContextMessage, ContextRole};
use tamako_core::wake::{GateInput, GateMessage, ParticipationGate, ReplyGenerator, ReplyRequest};

fn gate_message(row_id: i64, content: &str) -> GateMessage {
    GateMessage {
        row_id,
        platform_msg_id: format!("m{row_id}"),
        content: content.to_string(),
        // The M5 recall fields; the gate and the reply path read
        // `content` only, so plausible stand-ins suffice here.
        sender_id: format!("u{row_id}"),
        reply_to_platform_msg_id: None,
        text: content.to_string(),
    }
}

#[tokio::test]
#[ignore = "live API test; run with TAMAKO_LIVE_TEST=1 and ANTHROPIC_API_KEY set"]
async fn the_gate_returns_a_structurally_valid_decision() {
    if std::env::var("TAMAKO_LIVE_TEST").as_deref() != Ok("1") {
        return;
    }
    let endpoints =
        LlmEndpoints::resolve(&LlmConfigValues::default()).expect("endpoint resolution");
    let gate = RigGate::from_endpoint(&endpoints.gate).expect("provider config");
    let input = GateInput {
        new_messages: vec![
            gate_message(
                41,
                "[Alice 13:01] does anyone know a good espresso place downtown?",
            ),
            gate_message(
                42,
                "[Bob 13:02] tamako, you always know the cafes — any idea?",
            ),
        ],
        injections: vec![],
        forced: false,
    };
    let decision = gate.decide(&input).await.expect("gate decision");
    // Live output is not deterministic; keep the assertions structural.
    // A participate=true decision must target one of the presented ids.
    if decision.participate {
        let target = decision
            .target_row_id
            .expect("a participate decision has a target");
        assert!(
            input
                .new_messages
                .iter()
                .any(|message| message.row_id == target),
            "the target {target} must be one of the presented messages"
        );
    } else {
        assert_eq!(decision.target_row_id, None);
    }
}

#[tokio::test]
#[ignore = "live API test; run with TAMAKO_LIVE_TEST=1 and ANTHROPIC_API_KEY set"]
async fn the_reply_generator_returns_non_empty_text() {
    if std::env::var("TAMAKO_LIVE_TEST").as_deref() != Ok("1") {
        return;
    }
    let endpoints =
        LlmEndpoints::resolve(&LlmConfigValues::default()).expect("endpoint resolution");
    let generator = RigReplyGenerator::from_endpoint(&endpoints.reply).expect("provider config");
    let request = ReplyRequest {
        messages: vec![
            ContextMessage {
                role: ContextRole::System,
                content: "You are tamako, a quiet cat-like group pet. You speak rarely, \
                          in one short casual message."
                    .to_string(),
            },
            ContextMessage {
                role: ContextRole::User,
                content: "[Alice 13:01] does anyone know a good espresso place downtown?"
                    .to_string(),
            },
            ContextMessage {
                role: ContextRole::User,
                content: "[Bob 13:02] tamako, you always know the cafes — any idea?".to_string(),
            },
        ],
        target: gate_message(
            42,
            "[Bob 13:02] tamako, you always know the cafes — any idea?",
        ),
    };
    let reply = generator
        .generate("live_wake", &request)
        .await
        .expect("reply generation");
    // Live output is not deterministic; keep the assertion structural.
    assert!(!reply.trim().is_empty(), "expected a non-empty reply");
}
