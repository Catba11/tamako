//! Live-API spike: OpenRouter embeddings round-trip for the Phase 2
//! embedding sidecar (current-state.md decision 81). Ignored by
//! default; run with `TAMAKO_LIVE_TEST=1` and `OPENAI_API_KEY` set:
//!
//! ```sh
//! TAMAKO_LIVE_TEST=1 cargo test -p tamako-agent --test embedding_spike -- --ignored --nocapture
//! ```
//!
//! Ground truth (decision 81): embeddings go through the
//! openai-compatible `/v1/embeddings` endpoint at OpenRouter with model
//! `google/gemini-embedding-2` (served by google-vertex, ZDR),
//! dimension pinned at 3072 — the model's NATIVE dimension (the top of
//! the Matryoshka ladder), so the response is 3072 whether or not the
//! `dimensions` request parameter is honored and the hard length pin
//! is the guard. This spike is the pre-deploy smoke for the
//! decision-81 switch (decision 66's pair was
//! `qwen/qwen3-embedding-8b` at 4096). The spike uses rig-core 0.41's
//! embedding surface — the SAME client family tamako-agent's endpoint
//! layer already builds for openai-compatible completions
//! (`rig::providers::openai::CompletionsClient`), extended with
//! `rig::client::EmbeddingsClient::embedding_model_with_ndims`. The
//! requests go out one text per call — the production seam
//! (endpoint.rs `embed_text`; multi-element input 404s under the
//! account's ZDR-only policy, decision 81 addendum). No
//! production code changes; this file only proves the round-trip.

use rig::client::EmbeddingsClient as _;
use rig::embeddings::EmbeddingModel as _;
use tamako_agent::endpoint::EMBEDDING_DIMS;

/// decision 81 (unchanged from decision 66): openai-compatible base
/// URL of the embedding provider. rig uses the base URL verbatim, so
/// the request lands on `{base}/embeddings` =
/// `https://openrouter.ai/api/v1/embeddings`.
const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// decision 81: the embedding model.
const EMBEDDING_MODEL: &str = "google/gemini-embedding-2";

/// Cosine similarity of two equal-length vectors.
fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    assert_eq!(a.len(), b.len(), "cosine similarity needs equal lengths");
    let mut dot = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    dot / (norm_a.sqrt() * norm_b.sqrt())
}

#[tokio::test]
#[ignore = "live API test; run with TAMAKO_LIVE_TEST=1 and OPENAI_API_KEY set"]
async fn openrouter_gemini_embedding_round_trip() {
    if std::env::var("TAMAKO_LIVE_TEST").as_deref() != Ok("1") {
        eprintln!("skipping: set TAMAKO_LIVE_TEST=1 (and OPENAI_API_KEY) to run the live spike");
        return;
    }
    let api_key = match std::env::var("OPENAI_API_KEY") {
        Ok(key) if !key.is_empty() => key,
        _ => {
            eprintln!("skipping: OPENAI_API_KEY is not set");
            return;
        }
    };

    // The exact client seam of tamako-agent's endpoint layer
    // (src/endpoint.rs): explicit builder, api key from the env, custom
    // base URL, and the x-opencode-session default header. The header
    // map replaces the client defaults; build() adds the bearer auth
    // header when the map does not carry it.
    let mut headers = rig::http_client::HeaderMap::new();
    headers.insert(
        "x-opencode-session",
        rig::http_client::HeaderValue::from_static("tamako-embedding-spike"),
    );
    let client = rig::providers::openai::CompletionsClient::builder()
        .api_key(api_key)
        .base_url(OPENROUTER_BASE_URL)
        .http_headers(headers)
        .build()
        .expect("client build");
    let model = client.embedding_model_with_ndims(EMBEDDING_MODEL, EMBEDDING_DIMS);

    // Two Chinese paraphrases and one unrelated English sentence.
    let inputs = [
        "今天天气真好，正适合出门走走。",
        "今天天气很不错，很适合出去散步。",
        "The compiler rejected the borrow because the referenced value does not live long enough.",
    ];
    let start = std::time::Instant::now();
    // Production's shape (endpoint.rs:1533): ONE single-text call per
    // input. rig serializes every request as `{"input":["…"]}`; the
    // one-element route is served by the ZDR-compliant google-vertex
    // endpoints, while multi-element input is google-ai-studio only
    // and 404s under the account's ZDR-only policy (decision 81
    // addendum; verified 2026-08-22, re-verified 2026-09-12: the
    // 3-element form 404s, the one-element form 200s).
    let mut vectors = Vec::with_capacity(inputs.len());
    for input in &inputs {
        let vec = match model.embed_text(*input).await {
            Ok(vec) => vec,
            Err(error) => panic!(
                "embedding request failed: {error}; \
                 provider status: {:?}; provider body: {:?}",
                error.provider_response_status(),
                error.provider_response_body()
            ),
        };
        vectors.push(vec);
    }
    let latency = start.elapsed();
    eprintln!(
        "round-trip latency: {latency:?} ({} single-text calls)",
        inputs.len()
    );

    // HTTP 200 is implied per call (rig maps non-success statuses to
    // Err above); the hard length pin is the guard.
    for vec in &vectors {
        assert_eq!(
            vec.vec.len(),
            EMBEDDING_DIMS,
            "decision 81 pins the dimension at {EMBEDDING_DIMS}"
        );
    }

    // Smoke-level sanity: the two Chinese paraphrases must be closer
    // than a paraphrase and an unrelated English sentence.
    let sim_zh_zh = cosine_similarity(&vectors[0].vec, &vectors[1].vec);
    let sim_zh_en = cosine_similarity(&vectors[0].vec, &vectors[2].vec);
    eprintln!("cosine(zh1, zh2) = {sim_zh_zh:.6}");
    eprintln!("cosine(zh1, en)  = {sim_zh_en:.6}");
    assert!(
        sim_zh_zh > sim_zh_en,
        "paraphrase pair should outscore the unrelated pair: {sim_zh_zh} vs {sim_zh_en}"
    );
}
