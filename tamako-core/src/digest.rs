//! The digest pipeline contract. specs.md Section 10.
//!
//! The implementation lives in the tamako-agent crate (Phase 1, M1).
//! tamako-core defines the contract so the actor can drive the pipeline
//! without a dependency on the agent crate (no dependency cycles,
//! AGENT.md Section 4).

use std::future::Future;
use std::pin::Pin;

use crate::actor::CoreError;

/// The result of one digest run over the range (boundary, tail].
/// In every variant the boundary advances: a dead-lettered batch is
/// SKIPPED, a failed batch never blocks later batches (specs.md
/// Section 10.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DigestOutcome {
    /// Extraction ran; the graph write succeeded.
    Extracted {
        batch_id: String,
        new_boundary: i64,
        node_count: usize,
        edge_count: usize,
    },
    /// Emoji/greeting-only batch: the MessageBatch skeleton was stored
    /// without an extraction call (Section 7.2 rule 5 of the database
    /// spec).
    Skeleton { batch_id: String, new_boundary: i64 },
    /// All retries failed; the skeleton and the error were written to the
    /// dead_letter table (specs.md Section 10.3).
    DeadLettered {
        batch_id: String,
        new_boundary: i64,
        error: String,
    },
}

impl DigestOutcome {
    /// The boundary after this digest. Every variant advances it.
    pub fn new_boundary(&self) -> i64 {
        match self {
            DigestOutcome::Extracted { new_boundary, .. }
            | DigestOutcome::Skeleton { new_boundary, .. }
            | DigestOutcome::DeadLettered { new_boundary, .. } => *new_boundary,
        }
    }
}

/// The digest pipeline. Object-safe; the actor holds an
/// `Arc<dyn DigestPipeline>`. Returns `Ok(None)` when the tail is empty
/// (nothing to digest).
pub trait DigestPipeline: Send + Sync {
    fn run_digest<'a>(
        &'a self,
        chat_id: &'a str,
        last_digest_boundary_msg_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DigestOutcome>, CoreError>> + Send + 'a>>;
}

/// A seam for post-digest observers that need no actor state. The
/// context is actor-owned state (specs.md Section 6.1), so the actor
/// itself performs the Rule C3 context removal and the
/// `injected_memories` prune (specs.md Section 10.2 step 4) in its
/// `DigestCompleted` handler BEFORE it calls this hook. The actor calls
/// the hook after every successful digest run.
pub trait PostDigestHook: Send + Sync {
    fn after_digest<'a>(
        &'a self,
        chat_id: &'a str,
        outcome: &'a DigestOutcome,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// The no-op hook. The Rule C3 context removal lives in the actor (the
/// context is actor-owned state, specs.md Section 6.1); this hook stays
/// a seam for observers that need no actor state.
pub struct NoopPostDigestHook;

impl PostDigestHook for NoopPostDigestHook {
    fn after_digest<'a>(
        &'a self,
        _chat_id: &'a str,
        _outcome: &'a DigestOutcome,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_reports_its_new_boundary() {
        // specs.md Section 10.3: every variant advances the boundary.
        let extracted = DigestOutcome::Extracted {
            batch_id: "b1".to_string(),
            new_boundary: 10,
            node_count: 2,
            edge_count: 1,
        };
        let skeleton = DigestOutcome::Skeleton {
            batch_id: "b2".to_string(),
            new_boundary: 20,
        };
        let dead_lettered = DigestOutcome::DeadLettered {
            batch_id: "b3".to_string(),
            new_boundary: 30,
            error: "boom".to_string(),
        };
        assert_eq!(extracted.new_boundary(), 10);
        assert_eq!(skeleton.new_boundary(), 20);
        assert_eq!(dead_lettered.new_boundary(), 30);
    }

    #[tokio::test]
    async fn the_noop_hook_returns_and_does_not_panic() {
        let hook = NoopPostDigestHook;
        let outcome = DigestOutcome::Skeleton {
            batch_id: "b".to_string(),
            new_boundary: 1,
        };
        hook.after_digest("chat", &outcome).await;
    }

    #[test]
    fn the_traits_are_object_safe() {
        // The actor holds Arc<dyn DigestPipeline> and Arc<dyn
        // PostDigestHook>. This assertion keeps the traits object-safe.
        fn assert_object_safe(
            _: Option<std::sync::Arc<dyn DigestPipeline>>,
            _: Option<std::sync::Arc<dyn PostDigestHook>>,
        ) {
        }
        assert_object_safe(None, None);
    }
}
