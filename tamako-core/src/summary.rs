//! The summary provider contract: the segmented summarizer of the
//! Rule C3 removed chunk (specs.md Section 10, keep-two summary
//! retention).
//!
//! The live implementation belongs in the tamako-agent crate over the
//! endpoint layer; tamako-core defines the contract so the actor can
//! drive summarization without a dependency on the agent crate (no
//! dependency cycles, AGENT.md Section 4). Same pattern as
//! `digest.rs` and `wake.rs`. The scripted double in this module keeps
//! the actor subtask and its tests hermetic.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};

use tamako_store::MessageRow;

/// Errors of the summary provider.
#[derive(Debug, thiserror::Error)]
pub enum SummaryError {
    /// Cooperative cancellation of the decision-114 shutdown drain:
    /// the summarizer observed the drain token before its LLM call.
    /// NEVER a failure — the handler clears `summary_pending` and does
    /// nothing else: the deferred C3 mutation replays on the next run
    /// (the boundary never advanced).
    #[error("cancelled by the shutdown drain")]
    Cancelled,
    /// The summarizer provider failed (endpoint, model, or transport).
    /// Mirrors the `CoreError::Digest(String)` / `CoreError::Wake(String)`
    /// philosophy: a domain variant carrying the provider's message.
    #[error("summary provider error: {0}")]
    Provider(String),
    /// The provider returned an empty or whitespace-only summary. An
    /// empty summary is an error and is NEVER persisted (Rule P1: the
    /// summary is not derivable from persisted state — persisting an
    /// empty stand-in would fabricate history).
    #[error("the summary provider returned an empty summary")]
    Empty,
}

/// The summarizer of the Rule C3 removed chunk. Implemented in
/// tamako-agent over the endpoint layer; scripted double for hermetic
/// tests. Input is the raw-log range (first_msg_id, last_msg_id]
/// (human + bot rows; injections never enter the raw log); the
/// implementation renders the digest-style flat-label dialect, NOT the
/// XML dialogue dialect (extraction-like task, decision 61 divergence).
///
/// Object-safe; the actor holds an `Arc<dyn SummaryProvider>`.
pub trait SummaryProvider: Send + Sync {
    fn summarize<'a>(
        &'a self,
        chat_id: &'a str,
        first_msg_id: i64,
        last_msg_id: i64,
        rows: &'a [MessageRow],
    ) -> Pin<Box<dyn Future<Output = Result<String, SummaryError>> + Send + 'a>>;
}

/// The response mode of `ScriptedSummary`.
enum ScriptedSummaryMode {
    /// Pops the next summary text per call (FIFO). An exhausted queue
    /// fails with `SummaryError::Provider` (an empty summary is an
    /// error, so exhaustion must never masquerade as a valid summary).
    Summaries(VecDeque<String>),
    /// Every call fails with `SummaryError::Provider`.
    Failing(String),
    /// The first `failures` calls fail with `SummaryError::Provider`;
    /// the rest pop the summary queue (the failure-deferral retry
    /// tests: the summarization of decision 62 retries at the next
    /// digest completion).
    FailThen {
        failures: usize,
        summaries: VecDeque<String>,
    },
}

/// One recorded call of `ScriptedSummary`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptedSummaryInput {
    pub chat_id: String,
    pub first_msg_id: i64,
    pub last_msg_id: i64,
    /// The raw-log rows of the summarized range, in id order.
    pub rows: Vec<MessageRow>,
}

/// A scripted summarizer for tests and the offline demo (same pattern
/// as the tamako-agent scripted doubles). Returns fixed summary texts.
/// Two modes:
///
/// - `ScriptedSummary::with_summaries(vec_of_texts)`: pops the next
///   text per call (FIFO; when exhausted, every call fails with
///   `SummaryError::Provider`);
/// - `ScriptedSummary::failing(message)`: every call fails with
///   `SummaryError::Provider`.
///
/// Every call is recorded for assertions (`inputs()`).
pub struct ScriptedSummary {
    mode: Mutex<ScriptedSummaryMode>,
    inputs: Mutex<Vec<ScriptedSummaryInput>>,
}

impl ScriptedSummary {
    /// A scripted summarizer that answers with the given texts in order.
    pub fn with_summaries(summaries: Vec<String>) -> Self {
        ScriptedSummary {
            mode: Mutex::new(ScriptedSummaryMode::Summaries(summaries.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// A scripted summarizer whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedSummary {
            mode: Mutex::new(ScriptedSummaryMode::Failing(message.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// A scripted summarizer whose first `failures` calls fail, then
    /// pops the given texts in order.
    pub fn failing_then(failures: usize, summaries: Vec<String>) -> Self {
        ScriptedSummary {
            mode: Mutex::new(ScriptedSummaryMode::FailThen {
                failures,
                summaries: summaries.into(),
            }),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// Every call the summarizer received, in call order.
    pub fn inputs(&self) -> Vec<ScriptedSummaryInput> {
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl SummaryProvider for ScriptedSummary {
    fn summarize<'a>(
        &'a self,
        chat_id: &'a str,
        first_msg_id: i64,
        last_msg_id: i64,
        rows: &'a [MessageRow],
    ) -> Pin<Box<dyn Future<Output = Result<String, SummaryError>> + Send + 'a>> {
        // Lock, record, and decide synchronously; the future only
        // carries the result. A poisoned mutex is recovered; the
        // recorded inputs stay valid (same policy as the tamako-agent
        // scripted doubles and tamako-store).
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(ScriptedSummaryInput {
                chat_id: chat_id.to_string(),
                first_msg_id,
                last_msg_id,
                rows: rows.to_vec(),
            });
        let result = {
            let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *mode {
                ScriptedSummaryMode::Summaries(summaries) => {
                    summaries.pop_front().ok_or_else(|| {
                        SummaryError::Provider("scripted summaries exhausted".to_string())
                    })
                }
                ScriptedSummaryMode::Failing(message) => {
                    Err(SummaryError::Provider(message.clone()))
                }
                ScriptedSummaryMode::FailThen {
                    failures,
                    summaries,
                } => {
                    if *failures > 0 {
                        *failures -= 1;
                        Err(SummaryError::Provider("scripted failure".to_string()))
                    } else {
                        summaries.pop_front().ok_or_else(|| {
                            SummaryError::Provider("scripted summaries exhausted".to_string())
                        })
                    }
                }
            }
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64, text: &str) -> MessageRow {
        MessageRow {
            id,
            platform_msg_id: format!("p{id}"),
            direction: tamako_store::Direction::Inbound,
            event_type: tamako_store::EventType::Message,
            timestamp: time::OffsetDateTime::UNIX_EPOCH,
            sender_id: format!("u{id}"),
            sender_display_name: format!("user {id}"),
            sender_username: None,
            text: text.to_string(),
            reply_to_platform_msg_id: None,
            mentions_bot: false,
            is_reply_to_bot: false,
            forward: None,
        }
    }

    #[tokio::test]
    async fn the_scripted_summary_pops_fifo_and_fails_when_exhausted() {
        let summary = ScriptedSummary::with_summaries(vec![
            "chunk one digest".to_string(),
            "chunk two digest".to_string(),
        ]);
        let rows = vec![row(1, "one"), row(2, "two")];
        assert_eq!(
            summary
                .summarize("chat", 0, 2, &rows)
                .await
                .expect("the first call succeeds"),
            "chunk one digest"
        );
        assert_eq!(
            summary
                .summarize("chat", 3, 5, &rows)
                .await
                .expect("the second call succeeds"),
            "chunk two digest"
        );
        // Exhaustion is an error, never an empty (valid-looking) summary.
        let exhausted = summary.summarize("chat", 6, 8, &rows).await;
        assert!(matches!(
            exhausted,
            Err(SummaryError::Provider(message))
                if message == "scripted summaries exhausted"
        ));
    }

    #[tokio::test]
    async fn the_failing_scripted_summary_fails_every_call() {
        let summary = ScriptedSummary::failing("the model is down");
        let rows = vec![row(1, "one")];
        for _ in 0..2 {
            let outcome = summary.summarize("chat", 0, 1, &rows).await;
            assert!(matches!(
                outcome,
                Err(SummaryError::Provider(message)) if message == "the model is down"
            ));
        }
    }

    #[tokio::test]
    async fn the_scripted_summary_records_every_call() {
        let summary = ScriptedSummary::with_summaries(vec!["digest".to_string()]);
        let rows = vec![row(4, "four"), row(5, "five")];
        summary
            .summarize("chat-a", 3, 5, &rows)
            .await
            .expect("the call succeeds");

        let inputs = summary.inputs();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].chat_id, "chat-a");
        assert_eq!(inputs[0].first_msg_id, 3);
        assert_eq!(inputs[0].last_msg_id, 5);
        assert_eq!(inputs[0].rows, rows);
    }

    #[test]
    fn the_trait_is_object_safe() {
        // The actor holds Arc<dyn SummaryProvider> (same pattern as
        // DigestPipeline and the wake traits). This assertion keeps the
        // trait object-safe.
        fn assert_object_safe(_: Option<std::sync::Arc<dyn SummaryProvider>>) {}
        assert_object_safe(None);
    }

    #[test]
    fn the_summary_errors_are_typed() {
        // Library crates return typed errors (AGENT.md Section 6.4).
        assert_eq!(
            SummaryError::Provider("boom".to_string()).to_string(),
            "summary provider error: boom"
        );
        assert_eq!(
            SummaryError::Empty.to_string(),
            "the summary provider returned an empty summary"
        );
    }
}
