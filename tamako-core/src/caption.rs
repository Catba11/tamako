//! The caption provider contract: media captioning at intake (decision
//! 82).
//!
//! The live implementation belongs in the tamako-agent crate over the
//! endpoint layer (`LlmPurpose::CaptionMedia`); tamako-core defines the
//! contract so the adapter can drive captioning without a dependency on
//! the agent crate (no dependency cycles, AGENT.md Section 4). Same
//! pattern as `summary.rs`, `digest.rs`, and `embedding.rs`. The
//! scripted double in this module keeps the adapter subtask and its
//! tests hermetic.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};

use crate::context::MediaKindName;

/// Errors of the caption provider.
#[derive(Debug, thiserror::Error)]
pub enum CaptionError {
    /// The caption provider call failed (endpoint, model, or
    /// transport). Mirrors the `SummaryError::Provider` philosophy: a
    /// domain variant carrying the provider's message. This is the
    /// variant that, in Block 2, drives the retry/backoff policy
    /// (30s/60s/120s, 3 attempts, decision 82(d)) INSIDE the
    /// tamako-agent implementation — the contract only carries it.
    #[error("caption provider error: {0}")]
    Provider(String),
    /// The provider returned an empty or whitespace-only caption.
    /// Deliberately different from `SummaryError::Empty`: a failed or
    /// empty caption is NOT a hard failure at the ADAPTER level
    /// (decision 82(d): it becomes the placeholder
    /// `<media type="image"></media>` and the message is NEVER
    /// dropped). At the CONTRACT level a whitespace-only provider
    /// return is an `Empty` error so the scripted double can exercise
    /// the placeholder path distinctly from a transport failure. The
    /// Block-2 adapter maps BOTH `Provider` (after the retries
    /// exhaust) and `Empty` to the empty-placeholder element;
    /// `captions_failed_total` / `placeholder_media_total` distinguish
    /// them.
    #[error("the caption provider returned an empty caption")]
    Empty,
}

/// The media captioner of decision 82. Implemented in tamako-agent over
/// the endpoint layer (`LlmPurpose::CaptionMedia`); scripted double for
/// hermetic tests.
///
/// `jpeg_data_uri` is the `data:image/jpeg;base64,...` string produced
/// by `tamako_vision::to_base64_data_uri`. tamako-core does NOT depend
/// on tamako-vision: the data-URI string simply arrives as `&str`, and
/// the consumer's serialization of it lives in tamako-agent. `kind` is
/// the media kind of the attachment (decision 82(h): the caption
/// request is parameterized by media kind).
///
/// The retry/backoff policy (30s/60s/120s, 3 attempts, decision 82(d))
/// is the IMPLEMENTATION's concern, not the contract's — the trait
/// reports one `Result` per call.
///
/// Object-safe; the adapter/actor holds an `Arc<dyn CaptionProvider>`.
pub trait CaptionProvider: Send + Sync {
    fn caption_image<'a>(
        &'a self,
        jpeg_data_uri: &'a str,
        kind: MediaKindName,
    ) -> Pin<Box<dyn Future<Output = Result<String, CaptionError>> + Send + 'a>>;
}

/// The response mode of `ScriptedCaption`.
enum ScriptedCaptionMode {
    /// Pops the next caption text per call (FIFO). An exhausted queue
    /// fails with `CaptionError::Provider` (exhaustion must never
    /// masquerade as a valid caption).
    Captions(VecDeque<String>),
    /// Every call fails with `CaptionError::Provider`.
    Failing(String),
    /// The first `failures` calls fail with `CaptionError::Provider`;
    /// the rest pop the caption queue (the shape of the Block-2
    /// retry/placeholder tests: the retry/backoff policy of decision
    /// 82(d) recovers inside the tamako-agent implementation).
    FailThen {
        failures: usize,
        captions: VecDeque<String>,
    },
}

/// One recorded call of `ScriptedCaption`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptedCaptionInput {
    /// The full data-URI string of the call (a test double records it
    /// verbatim so tests can assert on it).
    pub jpeg_data_uri: String,
    pub kind: MediaKindName,
}

/// A scripted captioner for tests (same pattern as `ScriptedSummary`
/// and the tamako-agent scripted doubles). Returns fixed caption texts.
/// Three modes:
///
/// - `ScriptedCaption::with_captions(vec_of_texts)`: pops the next
///   text per call (FIFO; when exhausted, every call fails with
///   `CaptionError::Provider`);
/// - `ScriptedCaption::failing(message)`: every call fails with
///   `CaptionError::Provider`;
/// - `ScriptedCaption::failing_then(failures, vec_of_texts)`: the
///   first `failures` calls fail with `CaptionError::Provider`, then
///   pops the given texts in order.
///
/// Every call is recorded for assertions (`inputs()`).
pub struct ScriptedCaption {
    mode: Mutex<ScriptedCaptionMode>,
    inputs: Mutex<Vec<ScriptedCaptionInput>>,
}

impl ScriptedCaption {
    /// A scripted captioner that answers with the given texts in order.
    pub fn with_captions(captions: Vec<String>) -> Self {
        ScriptedCaption {
            mode: Mutex::new(ScriptedCaptionMode::Captions(captions.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// A scripted captioner whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedCaption {
            mode: Mutex::new(ScriptedCaptionMode::Failing(message.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// A scripted captioner whose first `failures` calls fail, then
    /// pops the given texts in order.
    pub fn failing_then(failures: usize, captions: Vec<String>) -> Self {
        ScriptedCaption {
            mode: Mutex::new(ScriptedCaptionMode::FailThen {
                failures,
                captions: captions.into(),
            }),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// Every call the captioner received, in call order.
    pub fn inputs(&self) -> Vec<ScriptedCaptionInput> {
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl CaptionProvider for ScriptedCaption {
    fn caption_image<'a>(
        &'a self,
        jpeg_data_uri: &'a str,
        kind: MediaKindName,
    ) -> Pin<Box<dyn Future<Output = Result<String, CaptionError>> + Send + 'a>> {
        // Lock, record, and decide synchronously; the future only
        // carries the result. A poisoned mutex is recovered; the
        // recorded inputs stay valid (same policy as ScriptedSummary,
        // the tamako-agent scripted doubles, and tamako-store).
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(ScriptedCaptionInput {
                jpeg_data_uri: jpeg_data_uri.to_string(),
                kind,
            });
        let result = {
            let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *mode {
                ScriptedCaptionMode::Captions(captions) => captions.pop_front().ok_or_else(|| {
                    CaptionError::Provider("scripted captions exhausted".to_string())
                }),
                ScriptedCaptionMode::Failing(message) => {
                    Err(CaptionError::Provider(message.clone()))
                }
                ScriptedCaptionMode::FailThen { failures, captions } => {
                    if *failures > 0 {
                        *failures -= 1;
                        Err(CaptionError::Provider("scripted failure".to_string()))
                    } else {
                        captions.pop_front().ok_or_else(|| {
                            CaptionError::Provider("scripted captions exhausted".to_string())
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

    const DATA_URI: &str = "data:image/jpeg;base64,QUJD";

    #[tokio::test]
    async fn the_scripted_caption_pops_fifo_and_fails_when_exhausted() {
        let caption = ScriptedCaption::with_captions(vec![
            "a cat on the sofa".to_string(),
            "a dog in the yard".to_string(),
        ]);
        assert_eq!(
            caption
                .caption_image(DATA_URI, MediaKindName::Image)
                .await
                .expect("the first call succeeds"),
            "a cat on the sofa"
        );
        assert_eq!(
            caption
                .caption_image(DATA_URI, MediaKindName::Sticker)
                .await
                .expect("the second call succeeds"),
            "a dog in the yard"
        );
        // Exhaustion is an error, never an empty (valid-looking)
        // caption.
        let exhausted = caption.caption_image(DATA_URI, MediaKindName::Video).await;
        assert!(matches!(
            exhausted,
            Err(CaptionError::Provider(message))
                if message == "scripted captions exhausted"
        ));
    }

    #[tokio::test]
    async fn the_failing_scripted_caption_fails_every_call() {
        let caption = ScriptedCaption::failing("the model is down");
        for _ in 0..2 {
            let outcome = caption.caption_image(DATA_URI, MediaKindName::Image).await;
            assert!(matches!(
                outcome,
                Err(CaptionError::Provider(message)) if message == "the model is down"
            ));
        }
    }

    #[tokio::test]
    async fn the_failing_then_scripted_caption_recovers() {
        let caption = ScriptedCaption::failing_then(2, vec!["a cat".to_string()]);
        // The shape of the Block-2 retry tests: the first two calls
        // fail, the third succeeds.
        for _ in 0..2 {
            let outcome = caption.caption_image(DATA_URI, MediaKindName::Image).await;
            assert!(matches!(
                outcome,
                Err(CaptionError::Provider(message)) if message == "scripted failure"
            ));
        }
        assert_eq!(
            caption
                .caption_image(DATA_URI, MediaKindName::Image)
                .await
                .expect("the third call succeeds"),
            "a cat"
        );
    }

    #[tokio::test]
    async fn the_scripted_caption_records_every_call() {
        let caption = ScriptedCaption::with_captions(vec!["a cat".to_string()]);
        caption
            .caption_image(DATA_URI, MediaKindName::Animated)
            .await
            .expect("the call succeeds");

        let inputs = caption.inputs();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].jpeg_data_uri, DATA_URI);
        assert_eq!(inputs[0].kind, MediaKindName::Animated);
    }

    #[test]
    fn the_trait_is_object_safe() {
        // The adapter/actor holds Arc<dyn CaptionProvider> (same
        // pattern as SummaryProvider, DigestPipeline, and the wake
        // traits). This assertion keeps the trait object-safe.
        fn assert_object_safe(_: Option<std::sync::Arc<dyn CaptionProvider>>) {}
        assert_object_safe(None);
    }

    #[test]
    fn the_caption_errors_are_typed() {
        // Library crates return typed errors (AGENT.md Section 6.4).
        assert_eq!(
            CaptionError::Provider("boom".to_string()).to_string(),
            "caption provider error: boom"
        );
        assert_eq!(
            CaptionError::Empty.to_string(),
            "the caption provider returned an empty caption"
        );
    }
}
