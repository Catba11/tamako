//! The extraction stage: the input types of Section 7.3 of the database
//! spec, the `KnowledgeExtractor` trait, the crate error type, and the
//! scripted extractor for tests and the offline demo.
//!
//! The live rig implementation is `rig_impl::RigExtractor`.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};

use crate::graph::KnowledgeGraph;

/// One message of the batch with its speaker label
/// (Section 7.2 step 4: `[{display_name} {HH:MM}] {text}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchMessage {
    pub display_name: String,
    /// HH:MM in UTC from the log-row timestamp.
    pub time_hhmm: String,
    pub text: String,
}

/// How a display name got bound to a tg_user_id at intake time
/// (specs.md Section 10.1; mention/reply map of Section 7.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingSource {
    /// The sender of a message of the batch.
    Sender,
    /// The sender of a message that a batch message replies to.
    ReplyTarget,
}

impl BindingSource {
    /// The rendering used in the extraction prompt.
    pub fn as_str(self) -> &'static str {
        match self {
            BindingSource::Sender => "sender",
            BindingSource::ReplyTarget => "reply_target",
        }
    }
}

/// One entry of the mention/reply map: a display name bound to a
/// Telegram user id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MentionBinding {
    pub display_name: String,
    pub tg_user_id: String,
    pub source: BindingSource,
}

/// The structured extraction input of one batch (Section 7.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionInput {
    pub batch_id: String,
    pub messages: Vec<BatchMessage>,
    /// The stored mention/reply map: display name -> tg_user_id.
    pub mention_map: Vec<MentionBinding>,
    /// Decision 106: the `related_pairs` rows the promotion pass offers
    /// the extraction call for grounding, pre-filtered to pairs whose
    /// two endpoint names both appear in the batch text. EMPTY renders
    /// the byte-identical pre-106 prompt (replay safety).
    pub related_pairs: Vec<RelatedPairCandidate>,
}

/// One `related_pairs` row offered to the extraction call (decision
/// 106): the merge tool judged the pair RELATED without graph context
/// (decision 83); the digest model sees the pair with the full batch
/// text and may ground one specific relationship edge. The grounded
/// edge binds DIRECTLY on the stored node ids — entity resolution
/// never touches a promotion edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelatedPairCandidate {
    /// The `related_pairs` row id (the status-flip key).
    pub row_id: i64,
    /// The stored node ids of the pair (the deterministic binding).
    pub node_a_id: String,
    pub node_b_id: String,
    /// The stored endpoint names and descriptions (the prompt payload;
    /// an empty description means the node carries none).
    pub a_name: String,
    pub a_description: String,
    pub b_name: String,
    pub b_description: String,
    /// The merge tool's reason text (why the pair was judged related).
    pub reason: String,
}

/// Errors of tamako-agent.
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("store error: {0}")]
    Store(#[from] tamako_store::StoreError),
    #[error("memory error: {0}")]
    Memory(#[from] tamako_memory::MemoryError),
    #[error("extraction failed: {0}")]
    Extraction(String),
    #[error("provider configuration error: {0}")]
    ProviderConfig(String),
    #[error("task join error: {0}")]
    Join(String),
}

/// The extraction stage. Tests use a scripted implementation; the live
/// rig implementation is `RigExtractor`. Object-safe.
pub trait KnowledgeExtractor: Send + Sync {
    fn extract<'a>(
        &'a self,
        input: &'a ExtractionInput,
    ) -> Pin<Box<dyn Future<Output = Result<KnowledgeGraph, AgentError>> + Send + 'a>>;
}

/// The response mode of `ScriptedExtractor`.
enum ScriptedMode {
    /// Pops the next graph per call (FIFO). An exhausted queue returns an
    /// empty graph.
    Graphs(VecDeque<KnowledgeGraph>),
    /// Every call fails with `AgentError::Extraction`.
    Failing(String),
}

/// A scripted extractor for tests and the offline demo. Returns fixed
/// `KnowledgeGraph` responses. Two modes:
///
/// - `ScriptedExtractor::with_graphs(vec_of_graphs)`: pops the next
///   graph per call (FIFO; when exhausted, returns an empty graph);
/// - `ScriptedExtractor::failing(message)`: every call fails with
///   `AgentError::Extraction`.
///
/// Every `ExtractionInput` is recorded for assertions (`inputs()`).
pub struct ScriptedExtractor {
    mode: Mutex<ScriptedMode>,
    inputs: Mutex<Vec<ExtractionInput>>,
}

impl ScriptedExtractor {
    /// A scripted extractor that answers with the given graphs in order.
    pub fn with_graphs(graphs: Vec<KnowledgeGraph>) -> Self {
        ScriptedExtractor {
            mode: Mutex::new(ScriptedMode::Graphs(graphs.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// A scripted extractor whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedExtractor {
            mode: Mutex::new(ScriptedMode::Failing(message.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// Every input the extractor received, in call order.
    pub fn inputs(&self) -> Vec<ExtractionInput> {
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl KnowledgeExtractor for ScriptedExtractor {
    fn extract<'a>(
        &'a self,
        input: &'a ExtractionInput,
    ) -> Pin<Box<dyn Future<Output = Result<KnowledgeGraph, AgentError>> + Send + 'a>> {
        // Lock, record, and decide synchronously; the future only
        // carries the result. A poisoned mutex is recovered; the
        // recorded inputs stay valid (same policy as tamako-store).
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(input.clone());
        let result = {
            let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *mode {
                ScriptedMode::Graphs(graphs) => Ok(graphs.pop_front().unwrap_or_default()),
                ScriptedMode::Failing(message) => Err(AgentError::Extraction(message.clone())),
            }
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{ExtractedNode, ExtractedNodeType};

    fn sample_input() -> ExtractionInput {
        ExtractionInput {
            batch_id: "b1".to_string(),
            messages: vec![BatchMessage {
                display_name: "Alice".to_string(),
                time_hhmm: "09:12".to_string(),
                text: "hello".to_string(),
            }],
            mention_map: vec![MentionBinding {
                display_name: "Alice".to_string(),
                tg_user_id: "1001".to_string(),
                source: BindingSource::Sender,
            }],
            related_pairs: vec![],
        }
    }

    fn sample_graph() -> KnowledgeGraph {
        KnowledgeGraph {
            nodes: vec![ExtractedNode {
                name: "Alice".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "A group member.".to_string(),
            }],
            edges: vec![],
        }
    }

    #[tokio::test]
    async fn with_graphs_pops_fifo_then_returns_empty_graphs() {
        let extractor = ScriptedExtractor::with_graphs(vec![sample_graph()]);
        let first = extractor.extract(&sample_input()).await.expect("first");
        assert_eq!(first, sample_graph());
        let second = extractor.extract(&sample_input()).await.expect("second");
        assert_eq!(second, KnowledgeGraph::default());
        assert_eq!(extractor.inputs().len(), 2);
    }

    #[tokio::test]
    async fn failing_mode_fails_every_call_and_records_inputs() {
        let extractor = ScriptedExtractor::failing("boom");
        for _ in 0..2 {
            match extractor.extract(&sample_input()).await {
                Err(AgentError::Extraction(message)) => assert_eq!(message, "boom"),
                other => panic!("expected Extraction error, got {other:?}"),
            }
        }
        assert_eq!(extractor.inputs().len(), 2);
    }

    #[test]
    fn the_trait_is_object_safe() {
        fn assert_object_safe(_: Option<std::sync::Arc<dyn KnowledgeExtractor>>) {}
        assert_object_safe(None);
    }
}
