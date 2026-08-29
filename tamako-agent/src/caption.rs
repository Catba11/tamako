//! The live media caption provider of decision 82 (media captioning
//! AT INTAKE, Phase 3 item 1), over the endpoint layer's
//! [`CaptionEndpoint`].
//!
//! Decision 82 (a): the caption LLM call lives in tamako-agent — the
//! only rig consumer (AGENT.md); tamako-core carries the contract
//! ([`tamako_core::caption::CaptionProvider`]) so the adapter drives
//! captioning without a rig dependency. Two types live here:
//!
//! - [`RigCaptionProvider`]: the live provider over rig's
//!   openai-compatible completion surface (the SAME
//!   `openai::CompletionsClient` family the endpoint layer builds for
//!   openai-compatible completions and embeddings), sending the
//!   image-bearing message of decision 82 (c).
//! - [`RetryCaptionProvider`]: a DECORATOR over
//!   `Arc<dyn CaptionProvider>` carrying the retry/backoff policy of
//!   decision 82 (d). Keeping the policy in a decorator holds the rig
//!   part thin and lets the retry tests script the inner provider
//!   with [`tamako_core::caption::ScriptedCaption`]. The binary wraps
//!   `RigCaptionProvider` in `RetryCaptionProvider`; the adapter only
//!   ever sees `Arc<dyn CaptionProvider>`.
//!
//! The request assembly follows the VERIFIED spike
//! (`tests/caption_spike.rs`) exactly: `UserContent::image_url` (the
//! data URI passes through verbatim — `image_base64` would
//! double-wrap it and the endpoint 400s) with `ImageDetail::Auto`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rig::client::CompletionClient as _;
use rig::completion::message::{ImageDetail, UserContent};
use rig::completion::{AssistantContent, CompletionModel as _, Message};
use rig::providers::openai;
use rig::OneOrMany;

use tamako_core::caption::{CaptionError, CaptionProvider};
use tamako_core::context::MediaKindName;

use crate::endpoint::{env_value, session_header_map, CaptionEndpoint, OPENAI_API_KEY_ENV_VAR};
use crate::extract::AgentError;

/// The PER-ATTEMPT timeout of one caption call (decision 82 (d), M1
/// review fix): captioning is a short-output task, so 120 s is a
/// generous bound — far below the 900 s ENDPOINT_TIMEOUT the
/// completion purposes inherit. With the RetryCaptionProvider's 3
/// attempts + 30 s/60 s backoff and the ~60 s download bound, one
/// media message's worst-case intake stall is ~8.5 min (bounded),
/// not ~46.5 min. Applied per attempt; retry/backoff is the
/// decorator's concern, unchanged.
pub const CAPTION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(120);

/// The fixed caption prompt template of decision 82 (c): faithful
/// description; describe people only as "a person" — never guess an
/// identity; transcribe any prominent in-image text (meme text is
/// semantic content); concise. The exact text the verified spike
/// (`tests/caption_spike.rs`) ran against the live endpoint.
pub const CAPTION_PROMPT: &str = "Please caption the following image, describing its content faithfully. Describe people only as \"a person\" — never guess an identity. Transcribe any prominent text visible in the image. Be concise.";

/// Assembles the caption request message (decision 82 (c)): the fixed
/// [`CAPTION_PROMPT`] plus ONE image part, the first multimodal
/// payload of the endpoint layer. Pure, so the request assembly is
/// unit-testable without a network or a client.
///
/// CRITICAL (the verified spike): `UserContent::image_url` passes the
/// `data:image/jpeg;base64,...` URI of `tamako_vision::to_base64_data_uri`
/// through VERBATIM; `image_base64` builds a data URI itself and would
/// DOUBLE-WRAP this one (the first spike run 400'd: "payload is not
/// base64-encoded data"). `media_type` is unused by the Url variant;
/// `ImageDetail::Auto` lets the provider pick the resolution.
fn build_caption_message(data_uri: &str) -> Message {
    Message::User {
        content: OneOrMany::many(vec![
            UserContent::text(CAPTION_PROMPT),
            UserContent::image_url(data_uri.to_string(), None, Some(ImageDetail::Auto)),
        ])
        .expect("two content parts"),
    }
}

/// The live [`CaptionProvider`] over rig's openai-compatible
/// completion surface (decision 82 (c)): the SAME
/// `openai::CompletionsClient` family the endpoint layer builds,
/// with `completion_model(endpoint.model)`. The
/// `x-opencode-session` and `x-session-id` default headers carry
/// over via [`session_header_map`] (decision 84 (a)).
/// Retry/backoff is NOT this type's concern
/// — [`RetryCaptionProvider`] carries the decision-82 (d) policy.
pub struct RigCaptionProvider {
    model: openai::completion::CompletionModel,
}

// The rig model handle does not implement Debug. A manual impl keeps
// RigCaptionProvider printable in test failures and logs (the same
// pattern as RigEmbeddingProvider). The API key is never printed.
impl std::fmt::Debug for RigCaptionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigCaptionProvider")
            .field("model", &self.model.model)
            .finish_non_exhaustive()
    }
}

impl RigCaptionProvider {
    /// Builds the provider for the resolved caption endpoint. Reads
    /// `OPENAI_API_KEY` from the environment (specs.md Section 13:
    /// API keys come from the environment only; the caption endpoint
    /// is openai-compatible — the same dual-use of the key as the
    /// embedding endpoint). Every build failure is
    /// `AgentError::ProviderConfig` (the same shape as
    /// [`RigEmbeddingProvider::from_endpoint`](crate::endpoint::RigEmbeddingProvider)):
    /// a missing or empty key, an invalid session-id header value,
    /// or a rig client-build error.
    pub fn from_endpoint(endpoint: &CaptionEndpoint) -> Result<Self, AgentError> {
        let api_key = env_value(OPENAI_API_KEY_ENV_VAR).ok_or_else(|| {
            AgentError::ProviderConfig(format!(
                "missing API key: set {OPENAI_API_KEY_ENV_VAR} for openai-compatible endpoints"
            ))
        })?;
        // The same builder + session-affinity header as
        // RigEmbeddingProvider::from_endpoint.
        let client = openai::CompletionsClient::builder()
            .api_key(api_key)
            .base_url(&endpoint.base_url)
            .http_headers(session_header_map(&endpoint.session_id)?)
            .build()
            .map_err(|error| AgentError::ProviderConfig(error.to_string()))?;
        Ok(RigCaptionProvider {
            model: client.completion_model(&endpoint.model),
        })
    }

    /// The degrade seam (decision 82, mirroring the decision-66
    /// embedding degrade): `None` means captioning is disabled for
    /// this run. Every build failure is logged once at WARN and the
    /// caller wires `None` — a missing provider configuration is
    /// never a hard startup error.
    pub fn build(endpoint: &CaptionEndpoint) -> Option<Self> {
        match Self::from_endpoint(endpoint) {
            Ok(provider) => Some(provider),
            Err(error) => {
                tracing::warn!(%error, "caption provider disabled: no provider configuration; captions will not run");
                None
            }
        }
    }
}

impl CaptionProvider for RigCaptionProvider {
    fn caption_image<'a>(
        &'a self,
        jpeg_data_uri: &'a str,
        kind: MediaKindName,
    ) -> Pin<Box<dyn Future<Output = Result<String, CaptionError>> + Send + 'a>> {
        Box::pin(async move {
            let request = self
                .model
                .completion_request(build_caption_message(jpeg_data_uri))
                .build();
            // The caption-specific per-attempt bound
            // (CAPTION_ATTEMPT_TIMEOUT, the M1 review fix): a stalled
            // caption is bounded well below the long-output
            // completion purposes' ENDPOINT_TIMEOUT; a
            // slow-but-progressing response under 120 s is untouched.
            // Retrying a timed-out attempt is RetryCaptionProvider's
            // concern, not this layer's.
            let response =
                tokio::time::timeout(CAPTION_ATTEMPT_TIMEOUT, self.model.completion(request))
                    .await
                    .map_err(|_| {
                        CaptionError::Provider(format!(
                            "endpoint timeout after {CAPTION_ATTEMPT_TIMEOUT:?}: no caption response from the endpoint"
                        ))
                    })?
                .map_err(|error| {
                    CaptionError::Provider(format!("caption completion failed: {error}"))
                })?;
            // Per-call usage at DEBUG (decision 57), mirrored from
            // complete_with. The MissingUsage watch item of decision
            // 66 extends to the caption call (decision 82 (c)): rig's
            // openai completions path carries usage as OPTIONAL (a
            // missing usage object maps to zero counters, never an
            // error), so an omitting provider shows up here as zeros,
            // not as a failed caption.
            tracing::debug!(
                input_tokens = response.usage.input_tokens,
                cached_input_tokens = response.usage.cached_input_tokens,
                cache_creation_input_tokens = response.usage.cache_creation_input_tokens,
                output_tokens = response.usage.output_tokens,
                media_kind = kind.as_str(),
                "llm caption completion usage"
            );
            let caption = response
                .choice
                .iter()
                .find_map(|content| match content {
                    AssistantContent::Text(text) => Some(text.text.clone()),
                    _ => None,
                })
                .ok_or_else(|| {
                    CaptionError::Provider("no text content in the caption response".to_string())
                })?;
            // An empty or whitespace-only reply is CaptionError::Empty
            // — the placeholder path of decision 82 (d), deliberately
            // distinct from a transport failure so the retry decorator
            // never retries it.
            if caption.trim().is_empty() {
                return Err(CaptionError::Empty);
            }
            Ok(caption)
        })
    }
}

/// The attempts budget of decision 82 (d): three attempts total.
const CAPTION_ATTEMPTS: usize = 3;

/// The backoff AFTER attempts 1 and 2 (decision 82 (d)): 30 s, then
/// 60 s. The decision's next step, 120 s, never fires at three
/// attempts — the third failure returns its error.
const CAPTION_BACKOFFS: [Duration; 2] = [Duration::from_secs(30), Duration::from_secs(60)];

/// The retry decorator of decision 82 (d) over
/// `Arc<dyn CaptionProvider>`: three attempts total with 30 s/60 s
/// backoff between them. Only [`CaptionError::Provider`] retries — a
/// transient transport/endpoint failure. [`CaptionError::Empty`]
/// returns immediately: an empty caption is a definitive model
/// answer (the provider replied; the reply was empty), so retrying
/// the same image would burn the backoff budget to reach the same
/// empty reply. The adapter maps BOTH outcomes to the placeholder
/// element downstream; `captions_failed_total` /
/// `placeholder_media_total` distinguish them there.
///
/// The decorator keeps the rig part thin and lets the retry tests
/// script the inner provider. `kind` passes straight through to the
/// inner provider on every attempt.
pub struct RetryCaptionProvider {
    inner: Arc<dyn CaptionProvider>,
}

// Arc<dyn CaptionProvider> is not Debug; a manual impl keeps the
// decorator printable without exposing the inner provider.
impl std::fmt::Debug for RetryCaptionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryCaptionProvider")
            .field("attempts", &CAPTION_ATTEMPTS)
            .field("backoffs", &CAPTION_BACKOFFS)
            .finish_non_exhaustive()
    }
}

impl RetryCaptionProvider {
    /// Wraps the inner provider with the decision-82 (d) retry policy.
    pub fn new(inner: Arc<dyn CaptionProvider>) -> Self {
        RetryCaptionProvider { inner }
    }
}

impl CaptionProvider for RetryCaptionProvider {
    fn caption_image<'a>(
        &'a self,
        jpeg_data_uri: &'a str,
        kind: MediaKindName,
    ) -> Pin<Box<dyn Future<Output = Result<String, CaptionError>> + Send + 'a>> {
        Box::pin(async move {
            for attempt in 0..CAPTION_ATTEMPTS {
                match self.inner.caption_image(jpeg_data_uri, kind).await {
                    Ok(caption) => return Ok(caption),
                    // No retry: an empty caption is a definitive model
                    // answer, not a transient failure (see the type
                    // docs).
                    Err(CaptionError::Empty) => return Err(CaptionError::Empty),
                    Err(error @ CaptionError::Provider(_)) => {
                        match CAPTION_BACKOFFS.get(attempt) {
                            Some(backoff) => {
                                tracing::warn!(
                                    %error,
                                    attempt = attempt + 1,
                                    backoff_seconds = backoff.as_secs(),
                                    "caption attempt failed; retrying after the decision-82 (d) backoff"
                                );
                                tokio::time::sleep(*backoff).await;
                            }
                            // The final attempt returns its error.
                            None => return Err(error),
                        }
                    }
                }
            }
            unreachable!("the final attempt returns its error")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rig::completion::message::DocumentSourceKind;
    use rig::providers::openai::completion::UserContent as OpenAiUserContent;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tamako_core::caption::ScriptedCaption;

    /// A data URI in the exact shape of
    /// `tamako_vision::to_base64_data_uri`'s output.
    const DATA_URI: &str = "data:image/jpeg;base64,QUJD";

    /// Saves, clears, and restores ONE env var (the caption tests only
    /// touch `OPENAI_API_KEY`). Hermetic env handling, the same shape
    /// as the endpoint layer's EnvGuard; the shared lock serializes
    /// against the endpoint tests.
    struct EnvGuard {
        saved: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn cleared() -> Self {
            let lock = crate::endpoint::env_lock::ENV_LOCK.lock().unwrap();
            let saved = std::env::var(OPENAI_API_KEY_ENV_VAR).ok();
            std::env::remove_var(OPENAI_API_KEY_ENV_VAR);
            EnvGuard { saved, _lock: lock }
        }

        fn set(&self, value: &str) {
            std::env::set_var(OPENAI_API_KEY_ENV_VAR, value);
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.saved {
                Some(value) => std::env::set_var(OPENAI_API_KEY_ENV_VAR, value),
                None => std::env::remove_var(OPENAI_API_KEY_ENV_VAR),
            }
        }
    }

    /// Extracts the content parts of the assembled caption message.
    fn caption_parts() -> Vec<UserContent> {
        let message = build_caption_message(DATA_URI);
        match message {
            Message::User { content } => content.iter().cloned().collect(),
            other => panic!("expected Message::User, got {other:?}"),
        }
    }

    // --- Request assembly (pure; decision 82 (c)) ---

    #[test]
    fn the_caption_message_carries_the_data_uri_verbatim() {
        let parts = caption_parts();
        let image = parts
            .iter()
            .find_map(|part| match part {
                UserContent::Image(image) => Some(image),
                _ => None,
            })
            .expect("one image part");
        // The Url variant carries the string through UNCHANGED: not
        // re-wrapped, not base64-re-encoded (the image_base64
        // double-wrap trap of the first spike run).
        assert_eq!(image.data, DocumentSourceKind::Url(DATA_URI.to_string()));
        assert_eq!(image.detail, Some(ImageDetail::Auto));
        // media_type is unused by the Url variant.
        assert_eq!(image.media_type, None);
    }

    #[test]
    fn the_caption_message_carries_the_fixed_prompt() {
        let parts = caption_parts();
        let prompt = parts
            .iter()
            .find_map(|part| match part {
                UserContent::Text(text) => Some(text.text.clone()),
                _ => None,
            })
            .expect("one text part");
        assert_eq!(prompt, CAPTION_PROMPT);
        // Exactly two parts: the prompt and the image (the spike
        // shape; OneOrMany::many would have failed otherwise).
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn the_caption_image_part_serializes_on_the_wire_with_auto_detail() {
        // Assert on the ACTUAL openai-compatible wire shape: rig's
        // provider converts the generic UserContent into its
        // openai::completion::UserContent, which serde-serializes as
        // the standard `image_url` content part. This proves the
        // verbatim URI and the lowercase "auto" detail on the wire,
        // not just in the in-memory parts.
        let wire_parts: Vec<serde_json::Value> = caption_parts()
            .into_iter()
            .map(|part| {
                let wire = OpenAiUserContent::try_from(part).expect("converts to the wire part");
                serde_json::to_value(&wire).expect("serializes")
            })
            .collect();
        assert_eq!(
            wire_parts[0],
            serde_json::json!({ "type": "text", "text": CAPTION_PROMPT })
        );
        assert_eq!(
            wire_parts[1],
            serde_json::json!({
                "type": "image_url",
                "image_url": { "url": DATA_URI, "detail": "auto" }
            })
        );
    }

    // --- CAPTION_ATTEMPT_TIMEOUT (decision 82 (d), M1 review fix) ---

    #[test]
    fn caption_attempt_timeout_is_the_m1_per_attempt_bound() {
        // The pin: one caption attempt is bounded at 120 s. The
        // timeout ERROR PATH itself is not unit-testable without a
        // live model (the timeout wraps `model.completion(request)`;
        // a refused local endpoint fails fast as "caption completion
        // failed", never as the timeout), so the constant pin plus
        // the call-site use of CAPTION_ATTEMPT_TIMEOUT carries the
        // fix. The error message interpolates this same constant, so
        // it reports "endpoint timeout after 120s".
        assert_eq!(CAPTION_ATTEMPT_TIMEOUT, Duration::from_secs(120));
        assert_eq!(
            format!("endpoint timeout after {CAPTION_ATTEMPT_TIMEOUT:?}: no caption response from the endpoint"),
            "endpoint timeout after 120s: no caption response from the endpoint"
        );
    }

    #[test]
    fn caption_attempt_timeout_is_deliberately_below_the_shared_endpoint_bound() {
        // The caption path bounds EACH ATTEMPT tighter than the
        // long-output completion purposes: captioning is a
        // short-output task, so inheriting the 900 s ENDPOINT_TIMEOUT
        // would let one media message stall the serial intake loop
        // ~46.5 min across the retry wrapper's 3 attempts + backoff.
        use crate::endpoint::ENDPOINT_TIMEOUT;
        assert!(CAPTION_ATTEMPT_TIMEOUT < ENDPOINT_TIMEOUT);
    }

    // --- RigCaptionProvider construction ---

    #[test]
    fn caption_build_without_an_api_key_degrades_to_none() {
        // The degrade seam: a missing OPENAI_API_KEY is a WARN + None,
        // never a hard error (the embedding degrade shape).
        let env = EnvGuard::cleared();
        let endpoint = CaptionEndpoint::resolve(&crate::endpoint::LlmConfigValues::default());
        match RigCaptionProvider::from_endpoint(&endpoint) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        assert!(RigCaptionProvider::build(&endpoint).is_none());
        // An empty key counts as missing.
        env.set("");
        assert!(RigCaptionProvider::build(&endpoint).is_none());
    }

    #[test]
    fn caption_build_constructs_a_client_with_a_key() {
        // Client construction performs no I/O; no network call here.
        let env = EnvGuard::cleared();
        env.set("test-openai-key");
        let endpoint = CaptionEndpoint {
            base_url: "http://localhost:9998/v1".to_string(),
            model: "local-caption-model".to_string(),
            session_id: "test-session-caption".to_string(),
        };
        let provider = RigCaptionProvider::build(&endpoint).expect("the provider builds");
        let debug = format!("{provider:?}");
        assert!(debug.contains("local-caption-model"));
        // The API key is never printed.
        assert!(!debug.contains("test-openai-key"));
    }

    #[test]
    fn caption_build_with_an_invalid_session_id_degrades_to_none() {
        // The same ProviderConfig class as the embedding provider: a
        // newline is never a valid header value.
        let env = EnvGuard::cleared();
        env.set("test-openai-key");
        let endpoint = CaptionEndpoint {
            base_url: crate::endpoint::DEFAULT_CAPTION_BASE_URL.to_string(),
            model: crate::endpoint::DEFAULT_CAPTION_MODEL.to_string(),
            session_id: "bad\nsession".to_string(),
        };
        match RigCaptionProvider::from_endpoint(&endpoint) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        assert!(RigCaptionProvider::build(&endpoint).is_none());
    }

    // --- RetryCaptionProvider (decision 82 (d)) ---

    /// Paused-time idiom (the same as tamako-core's embedding.rs /
    /// actor.rs tests): `start_paused = true` freezes the clock; when
    /// the test task parks on the retry sleep and no other task is
    /// runnable, tokio AUTO-ADVANCES the clock to the timer deadline.
    /// The 30 s/60 s decision-82 (d) sleeps therefore complete
    /// instantly and the test never waits real time.
    #[tokio::test(start_paused = true)]
    async fn two_failures_then_success_recovers_on_the_third_attempt() {
        let inner = Arc::new(ScriptedCaption::failing_then(2, vec!["a cat".to_string()]));
        let provider = RetryCaptionProvider::new(inner.clone());

        let caption = provider
            .caption_image(DATA_URI, MediaKindName::Sticker)
            .await
            .expect("the third attempt succeeds");

        assert_eq!(caption, "a cat");
        // Three calls total (fail, fail, succeed); the kind passes
        // through unchanged on every attempt.
        let inputs = inner.inputs();
        assert_eq!(inputs.len(), 3);
        for input in &inputs {
            assert_eq!(input.jpeg_data_uri, DATA_URI);
            assert_eq!(input.kind, MediaKindName::Sticker);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn all_failures_return_the_error_after_exactly_three_attempts() {
        let inner = Arc::new(ScriptedCaption::failing("down"));
        let provider = RetryCaptionProvider::new(inner.clone());

        let outcome = provider.caption_image(DATA_URI, MediaKindName::Image).await;

        assert!(
            matches!(&outcome, Err(CaptionError::Provider(message)) if message == "down"),
            "expected the inner Provider error, got {outcome:?}"
        );
        // Exactly three attempts; the 120 s step never fires.
        assert_eq!(inner.inputs().len(), 3);
    }

    /// A captioner whose every reply is `CaptionError::Empty`
    /// (ScriptedCaption has no empty mode; the tiny double lives here
    /// — tamako-core is NOT modified for this).
    struct EmptyCaption {
        calls: AtomicUsize,
    }

    impl CaptionProvider for EmptyCaption {
        fn caption_image<'a>(
            &'a self,
            _jpeg_data_uri: &'a str,
            _kind: MediaKindName,
        ) -> Pin<Box<dyn Future<Output = Result<String, CaptionError>> + Send + 'a>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(CaptionError::Empty) })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_caption_is_never_retried() {
        let inner = Arc::new(EmptyCaption {
            calls: AtomicUsize::new(0),
        });
        let provider = RetryCaptionProvider::new(inner.clone());

        let outcome = provider.caption_image(DATA_URI, MediaKindName::Image).await;

        assert!(matches!(outcome, Err(CaptionError::Empty)));
        // One call, no backoff: an empty caption is a definitive model
        // answer, not a transient failure.
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }
}
