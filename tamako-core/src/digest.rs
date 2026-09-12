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
///
/// `cancel` is the decision-114 drain token: the implementation polls
/// it between retry attempts, between an attempt's initial call and
/// its repair call, and during the retry backoff; a fired token returns
/// [`CoreError::Cancelled`] with the batch left PENDING (no attempt
/// consumed, no failure counter, no dead letter).
pub trait DigestPipeline: Send + Sync {
    fn run_digest<'a>(
        &'a self,
        chat_id: &'a str,
        last_digest_boundary_msg_id: i64,
        cancel: tokio_util::sync::CancellationToken,
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

/// The content hash of one embedding-queue row (decision 66). FNV-1a,
/// 64-bit, over the byte layout `name ++ b"\n" ++ description` (the
/// pipeline-known candidate content of one graph node), hex-encoded as
/// 16 lowercase zero-padded characters.
///
/// Stability contract: the offset basis and prime below are fixed, the
/// input is the UTF-8 byte sequence only, and the arithmetic wraps, so
/// the hash is stable across builds, platforms, and process restarts.
/// It is NOT cryptographic; it is a change detector. Any change to the
/// candidate name or description changes the hash, and the new hash
/// re-queues the node for embedding (the queue dedups on the
/// (node_id, content_hash) pair). A candidate name containing '\n' can
/// alias a different (name, description) split; extracted entity names
/// are single-line surface forms, so the ambiguity is accepted for a
/// change detector.
pub fn embedding_content_hash(name: &str, description: &str) -> String {
    // The FNV-1a 64-bit constants, fixed forever by the doc above.
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for byte in name
        .as_bytes()
        .iter()
        .chain(b"\n")
        .chain(description.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    format!("{hash:016x}")
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
    fn the_embedding_content_hash_matches_a_pinned_vector() {
        // Pinned: FNV-1a 64-bit over the UTF-8 bytes of
        // "Alice\nA group member who deploys." (name + '\n' +
        // description). A change here is a breaking change of the queue
        // semantics: every stored hash re-embeds.
        assert_eq!(
            embedding_content_hash("Alice", "A group member who deploys."),
            "81cb73b05134403d"
        );
    }

    #[test]
    fn the_embedding_content_hash_is_sensitive_to_both_fields() {
        let base = embedding_content_hash("Alice", "deploys nightly");
        assert_ne!(embedding_content_hash("Bob", "deploys nightly"), base);
        assert_ne!(embedding_content_hash("Alice", "deploys weekly"), base);
        assert_ne!(embedding_content_hash("Alic", "e deploys nightly"), base);
        // Empty fields hash fine; only the enqueue side skips them.
        assert_eq!(
            embedding_content_hash("", "").len(),
            16,
            "the hash is always 16 hex chars"
        );
    }

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
