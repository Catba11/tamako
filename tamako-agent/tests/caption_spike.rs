//! Live-API spike: MiniMax M3 vision caption round-trip for the media
//! captioning at intake (current-state.md decision 82). Ignored by
//! default; run with `TAMAKO_LIVE_TEST=1` and `OPENAI_API_KEY` set:
//!
//! ```sh
//! TAMAKO_LIVE_TEST=1 cargo test -p tamako-agent --test caption_spike -- --ignored --nocapture
//! ```
//!
//! This spike proves the Block-2 wiring assumptions BEFORE the adapter
//! is wired:
//! 1. rig 0.42's `UserContent::image_base64` serializes correctly
//!    through the openai-compatible `CompletionsClient` (the SAME
//!    client family tamako-agent's endpoint layer already builds);
//! 2. OpenRouter serves `minimax/minimax-m3` with an image-bearing
//!    chat completion under the account's ZDR-only policy (the
//!    first-party MiniMax endpoint is NOT ZDR — the policy must route
//!    to a third-party ZDR provider);
//! 3. The model returns a usable caption for a normalized JPEG (the
//!    exact output of `tamako_vision::normalize_image` + `to_base64_data_uri`).
//!
//! No production code changes; this file only proves the round-trip.

use rig::client::CompletionClient as _;
use rig::completion::message::{ImageDetail, Message, UserContent};
use rig::completion::CompletionModel as _;
use rig::OneOrMany;

/// decision 82(c): openai-compatible base URL of the caption provider.
const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// decision 82(c): the caption model.
const CAPTION_MODEL: &str = "minimax/minimax-m3";

/// The fixed caption prompt template (decision 82(c)): faithful
/// description, no face identification, transcribe prominent in-image
/// text, concise. Kept minimal here; the production prompt template
/// lives in tamako-agent in Block 2.
const CAPTION_PROMPT: &str = "Please caption the following image, describing its content faithfully. Describe people only as \"a person\" — never guess an identity. Transcribe any prominent text visible in the image. Be concise.";

#[tokio::test]
#[ignore = "live API test; run with TAMAKO_LIVE_TEST=1 and OPENAI_API_KEY set"]
async fn openrouter_m3_caption_round_trip() {
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

    // Build a small deterministic test image through the REAL
    // tamako-vision pipeline (a red square on a white background,
    // 256x256). This proves the normalize→base64 path end-to-end.
    let mut rgba = image::ImageBuffer::from_pixel(256, 256, image::Rgba([255, 255, 255, 255]));
    for y in 64..192 {
        for x in 64..192 {
            rgba.put_pixel(x, y, image::Rgba([220, 30, 30, 255]));
        }
    }
    let mut png_bytes = Vec::new();
    image::DynamicImage::ImageRgba8(rgba)
        .write_to(
            &mut std::io::Cursor::new(&mut png_bytes),
            image::ImageFormat::Png,
        )
        .expect("in-code PNG fixture encodes");
    let normalized = tamako_vision::normalize_image(&png_bytes, tamako_vision::MediaKind::Image)
        .expect("normalize must succeed");
    assert_eq!((normalized.width, normalized.height), (256, 256));
    let data_uri = tamako_vision::to_base64_data_uri(&normalized.jpeg_bytes);
    eprintln!(
        "normalized: {}x{}, {} bytes jpeg, {} bytes data-uri",
        normalized.width,
        normalized.height,
        normalized.jpeg_bytes.len(),
        data_uri.len()
    );

    // The exact client seam of tamako-agent's endpoint layer
    // (src/endpoint.rs): explicit builder, api key from the env, custom
    // base URL, and the x-opencode-session default header.
    let mut headers = rig::http_client::HeaderMap::new();
    headers.insert(
        "x-opencode-session",
        rig::http_client::HeaderValue::from_static("tamako-caption-spike"),
    );
    let client = rig::providers::openai::CompletionsClient::builder()
        .api_key(api_key)
        .base_url(OPENROUTER_BASE_URL)
        .http_headers(headers)
        .build()
        .expect("client build");
    let model = client.completion_model(CAPTION_MODEL);

    // Assemble the image-bearing message: text prompt + one image part.
    // rig 0.42 has TWO image constructors with a subtle split:
    // `image_base64(body, media_type, ...)` expects the RAW base64 body
    // and builds the data URI itself (`data:{mime};base64,{body}`) —
    // passing a full data URI here DOUBLE-WRAPS it (the first spike run
    // 400'd: "payload is not base64-encoded data"). `image_url(uri,
    // ...)` passes the string through VERBATIM, which is exactly what
    // the decision-82 contract carries (`to_base64_data_uri` output).
    // media_type is unused by the Url variant.
    let message = Message::User {
        content: OneOrMany::many(vec![
            UserContent::text(CAPTION_PROMPT),
            UserContent::image_url(data_uri, None, Some(ImageDetail::Auto)),
        ])
        .expect("two content parts"),
    };
    let request = model.completion_request(message).build();

    let start = std::time::Instant::now();
    let result = model.completion(request).await;
    let latency = start.elapsed();
    let response = match result {
        Ok(response) => response,
        Err(error) => panic!("caption request failed after {latency:?}: {error:?}"),
    };
    eprintln!("caption latency: {latency:?}");

    // Extract the assistant's text reply.
    let choice = response.choice.into_iter().next().expect("one choice");
    let caption = match choice {
        rig::completion::message::AssistantContent::Text(text) => text.text,
        other => panic!("expected a text reply, got {other:?}"),
    };
    eprintln!("caption: {caption}");

    // Smoke-level sanity: the caption must be non-empty, and for a
    // red square on white it should mention red (or a synonym) or
    // square/shape. This is a SMOKE test, not a quality gate.
    assert!(!caption.trim().is_empty(), "caption must not be empty");
    let lower = caption.to_lowercase();
    let mentions_red = lower.contains("red") || lower.contains("红");
    let mentions_shape = lower.contains("square")
        || lower.contains("shape")
        || lower.contains("方块")
        || lower.contains("方形");
    assert!(
        mentions_red || mentions_shape,
        "caption should mention the red square: {caption}"
    );
}
