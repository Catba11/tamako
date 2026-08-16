//! Live-API spike: OpenRouter embeddings round-trip for the Phase 2
//! embedding sidecar (current-state.md decision 66). Ignored by
//! default; run with `TAMAKO_LIVE_TEST=1` and `OPENAI_API_KEY` set:
//!
//! ```sh
//! TAMAKO_LIVE_TEST=1 cargo test -p tamako-agent --test embedding_spike -- --ignored --nocapture
//! ```
//!
//! Ground truth (decision 66): embeddings go through the
//! openai-compatible `/v1/embeddings` endpoint at OpenRouter with model
//! `qwen/qwen3-embedding-8b`, dimension pinned at 4096. The spike uses
//! rig-core 0.41's embedding surface — the SAME client family
//! tamako-agent's endpoint layer already builds for openai-compatible
//! completions (`rig::providers::openai::CompletionsClient`), extended
//! with `rig::client::EmbeddingsClient::embedding_model_with_ndims`.
//! No production code changes; this file only proves the round-trip.

use rig::client::EmbeddingsClient as _;
use rig::embeddings::EmbeddingModel as _;

/// decision 66: openai-compatible base URL of the embedding provider.
/// rig uses the base URL verbatim, so the request lands on
/// `{base}/embeddings` = `https://openrouter.ai/api/v1/embeddings`.
const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// decision 66: the embedding model.
const EMBEDDING_MODEL: &str = "qwen/qwen3-embedding-8b";

/// decision 66: the pinned dimension. rig sends it as the
/// openai-compatible `dimensions` request field
/// (`embedding_model_with_ndims`).
const EMBEDDING_DIMS: usize = 4096;

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
async fn openrouter_qwen3_embedding_round_trip() {
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
    let result = model
        .embed_texts_with_usage(inputs.iter().map(|text| (*text).to_string()))
        .await;
    let latency = start.elapsed();
    // rig only returns Ok on a success status; a non-2xx surfaces as an
    // EmbeddingError that preserves the provider status and body. Print
    // both on failure so a live failure is diagnosable.
    let response = match result {
        Ok(response) => response,
        Err(error) => panic!(
            "embedding request failed after {latency:?}: {error}; \
             provider status: {:?}; provider body: {:?}",
            error.provider_response_status(),
            error.provider_response_body()
        ),
    };
    eprintln!(
        "round-trip latency: {latency:?}; usage: {} input / {} total tokens",
        response.usage.input_tokens, response.usage.total_tokens
    );

    // Exactly one embedding per input (HTTP 200 is implied: rig maps
    // non-success statuses to Err above).
    assert_eq!(
        response.embeddings.len(),
        inputs.len(),
        "expected one embedding per input"
    );
    for (input, embedding) in inputs.iter().zip(response.embeddings.iter()) {
        assert_eq!(embedding.document, *input, "document echoes the input");
        assert_eq!(
            embedding.vec.len(),
            EMBEDDING_DIMS,
            "decision 66 pins the dimension at {EMBEDDING_DIMS}"
        );
    }

    // Smoke-level sanity: the two Chinese paraphrases must be closer
    // than a paraphrase and an unrelated English sentence.
    let sim_zh_zh = cosine_similarity(&response.embeddings[0].vec, &response.embeddings[1].vec);
    let sim_zh_en = cosine_similarity(&response.embeddings[0].vec, &response.embeddings[2].vec);
    eprintln!("cosine(zh1, zh2) = {sim_zh_zh:.6}");
    eprintln!("cosine(zh1, en)  = {sim_zh_en:.6}");
    assert!(
        sim_zh_zh > sim_zh_en,
        "paraphrase pair should outscore the unrelated pair: {sim_zh_zh} vs {sim_zh_en}"
    );
}
