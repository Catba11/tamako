//! Media enrichment at intake (decision 82): download -> normalize ->
//! caption -> render.
//!
//! The pure half (classification, PhotoSize selection, text assembly)
//! lives in `crate::normalize`; this module is the ASYNC half. It runs
//! inside `TeloxideAdapter::next_group_event` when a `MediaEnricher` is
//! configured. Every failure — download, normalization, caption —
//! degrades to a PLACEHOLDER element (`<media type="..."></media>`,
//! empty caption): a media message is NEVER dropped (decision 82(d)).
//!
//! Metrics (specs.md Section 12): the adapter has no counter
//! infrastructure today. The exact counter names of decision 82 are
//! emitted as structured `tracing` fields (`counter = "captions_total"`,
//! ...) so a later collector can scrape them from the log stream.
//! Surfacing these counters through `--status` is follow-up work owned
//! by the primary agent. Counters: `captions_total`,
//! `captions_failed_total`, `captions_empty_total`,
//! `sticker_cache_hits_total`, `placeholder_media_total`.

use std::sync::Arc;
use std::time::Duration;

use tamako_core::caption::{CaptionError, CaptionProvider};
use tamako_core::context::{render_media_element, MediaKindName};
use tamako_core::event::NormalizedMessage;
use tamako_store::MediaStore;
use teloxide::net::Download as _;
use teloxide::requests::Requester as _;
use teloxide::types::Message;
use teloxide::Bot;
use tracing::{info, warn};

use crate::normalize::{self, BotIdentity, MediaClass};

/// The bound of one media download (Bot API getFile plus the byte
/// download together). A timeout degrades to a placeholder like every
/// other download failure.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);

/// The media enrichment bundle of decision 82: the caption provider (the
/// core contract; the live implementation is tamako-agent's provider
/// over the endpoint layer) and the global sticker-caption cache.
///
/// The adapter holds an `Option<MediaEnricher>`; `None` keeps the
/// pre-enrichment behavior byte-identical (a photo/sticker with no text
/// is skipped by the pure dispatch).
pub struct MediaEnricher {
    pub caption: Arc<dyn CaptionProvider>,
    pub media_store: Arc<MediaStore>,
}

/// Builds the normalized message of a media-carrying update. The caller
/// (`TeloxideAdapter::next_group_event`) classified the message and
/// knows whether it arrived as an edit.
///
/// Edited media messages (specs.md Section 15 open item): NEVER
/// re-captioned — no download, no caption call. The element is the
/// placeholder with an EMPTY caption, followed by the new caption text,
/// and the timestamp is the edit date.
pub(crate) async fn enrich_media_message(
    bot: &Bot,
    identity: &BotIdentity,
    enricher: &MediaEnricher,
    msg: &Message,
    class: &MediaClass,
    edited: bool,
) -> NormalizedMessage {
    let element = if edited {
        placeholder(class.kind())
    } else {
        media_element_for(bot, enricher, class).await
    };
    let text = normalize::assemble_media_text(class.caption_text(), &element);
    if edited {
        normalize::normalize_edited_message_with_text(msg, identity, text)
    } else {
        normalize::normalize_message_with_text(msg, identity, text)
    }
}

/// The `<media>` element of one classified message, branches by kind
/// (decision 82).
async fn media_element_for(bot: &Bot, enricher: &MediaEnricher, class: &MediaClass) -> String {
    match class {
        MediaClass::Photo { file_id, .. } => {
            caption_media_file(
                bot,
                enricher,
                file_id,
                tamako_vision::MediaKind::Image,
                MediaKindName::Image,
            )
            .await
        }
        // Animated/video stickers short-circuit to a placeholder WITHOUT
        // a download and WITHOUT a caption call (decision 82(h)).
        MediaClass::Sticker {
            is_animated: true, ..
        } => placeholder(MediaKindName::Animated),
        MediaClass::Sticker { is_video: true, .. } => placeholder(MediaKindName::Video),
        MediaClass::Sticker {
            file_id,
            file_unique_id,
            ..
        } => sticker_element(bot, enricher, file_id, file_unique_id).await,
        // Videos, video notes, and animations are placeholders at
        // cutover: no download, no caption call (decision 82(h)).
        MediaClass::Video { .. } => placeholder(MediaKindName::Video),
        MediaClass::Animated { .. } => placeholder(MediaKindName::Animated),
    }
}

/// The static-sticker path: the global cache first (a hit costs ZERO
/// model calls, decision 82(f)); a miss downloads, normalizes, captions,
/// and best-effort caches the result.
async fn sticker_element(
    bot: &Bot,
    enricher: &MediaEnricher,
    file_id: &str,
    file_unique_id: &str,
) -> String {
    // MediaStore is synchronous (AGENT.md Section 6.2): the blocking pool.
    let store = Arc::clone(&enricher.media_store);
    let uid = file_unique_id.to_string();
    let cached = tokio::task::spawn_blocking(move || store.get_sticker_caption(&uid)).await;
    match cached {
        Ok(Ok(Some(caption))) => {
            info!(
                counter = "sticker_cache_hits_total",
                "sticker caption cache hit; no model call"
            );
            render_media_element(MediaKindName::Sticker, &caption)
        }
        Ok(Ok(None)) => sticker_cache_miss(bot, enricher, file_id, file_unique_id).await,
        Ok(Err(error)) => {
            warn!(error = %error, "sticker caption cache read failed; treating as a miss");
            sticker_cache_miss(bot, enricher, file_id, file_unique_id).await
        }
        Err(join_error) => {
            warn!(error = %join_error, "sticker caption cache read task failed; treating as a miss");
            sticker_cache_miss(bot, enricher, file_id, file_unique_id).await
        }
    }
}

/// A sticker cache miss: download -> normalize -> caption, then a
/// best-effort cache write of a successful caption.
async fn sticker_cache_miss(
    bot: &Bot,
    enricher: &MediaEnricher,
    file_id: &str,
    file_unique_id: &str,
) -> String {
    let kind = MediaKindName::Sticker;
    match caption_of_media_file(
        bot,
        enricher,
        file_id,
        tamako_vision::MediaKind::Sticker,
        kind,
    )
    .await
    {
        Some(caption) => {
            cache_sticker_caption(enricher, file_unique_id, &caption).await;
            render_media_element(kind, &caption)
        }
        None => placeholder(kind),
    }
}

/// The best-effort sticker caption cache write (decision 82(f)): a
/// failure is a warn, never fatal. `MediaStore::put_sticker_caption` is
/// INSERT OR IGNORE, so a concurrent or repeated write keeps the first
/// caption.
async fn cache_sticker_caption(enricher: &MediaEnricher, file_unique_id: &str, caption: &str) {
    let store = Arc::clone(&enricher.media_store);
    let uid = file_unique_id.to_string();
    let caption = caption.to_string();
    let put = tokio::task::spawn_blocking(move || store.put_sticker_caption(&uid, &caption)).await;
    match put {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(error = %error, "sticker caption cache write failed; continuing")
        }
        Err(join_error) => {
            warn!(error = %join_error, "sticker caption cache write task failed; continuing")
        }
    }
}

/// Downloads, normalizes, and captions one media file, rendering the
/// result. Every failure logs, counts, and yields the placeholder; the
/// message itself is never dropped (decision 82(d)).
async fn caption_media_file(
    bot: &Bot,
    enricher: &MediaEnricher,
    file_id: &str,
    vision_kind: tamako_vision::MediaKind,
    kind: MediaKindName,
) -> String {
    match caption_of_media_file(bot, enricher, file_id, vision_kind, kind).await {
        Some(caption) => render_media_element(kind, &caption),
        None => placeholder(kind),
    }
}

/// The fallible core of the caption pipeline, split out so the sticker
/// cache-miss path can persist a successful caption. `Some(caption)` has
/// already counted `captions_total`; `None` has already logged and
/// counted the specific failure — the caller still owes the placeholder.
async fn caption_of_media_file(
    bot: &Bot,
    enricher: &MediaEnricher,
    file_id: &str,
    vision_kind: tamako_vision::MediaKind,
    kind: MediaKindName,
) -> Option<String> {
    let bytes = match download_media(bot, file_id).await {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(media_kind = kind.as_str(), error = %error, "media download failed; placeholder");
            return None;
        }
    };
    // Normalization is CPU-bound and synchronous (decision 82(a)); the
    // runtime rule of AGENT.md Section 6.2 puts it on the blocking pool.
    let normalized = match tokio::task::spawn_blocking(move || {
        tamako_vision::normalize_image(&bytes, vision_kind)
    })
    .await
    {
        Ok(Ok(image)) => image,
        Ok(Err(error)) => {
            warn!(media_kind = kind.as_str(), error = %error, "media normalization failed; placeholder");
            return None;
        }
        Err(join_error) => {
            warn!(media_kind = kind.as_str(), error = %join_error, "media normalization task failed; placeholder");
            return None;
        }
    };
    let data_uri = tamako_vision::to_base64_data_uri(&normalized.jpeg_bytes);
    caption_data_uri(&enricher.caption, &data_uri, kind).await
}

/// Calls the caption provider on one normalized data URI. Ok counts
/// `captions_total` and returns the caption. The error variants count
/// their DISTINCT counters (decision 82(d), acceptance finding 2):
/// `captions_failed_total` for `CaptionError::Provider` (the provider's
/// internal retry/backoff already exhausted) and `captions_empty_total`
/// for `CaptionError::Empty`. Both return None — the caller emits the
/// placeholder; the message is never dropped.
async fn caption_data_uri(
    caption: &Arc<dyn CaptionProvider>,
    data_uri: &str,
    kind: MediaKindName,
) -> Option<String> {
    match caption.caption_image(data_uri, kind).await {
        Ok(text) => {
            info!(
                counter = "captions_total",
                media_kind = kind.as_str(),
                "media captioned"
            );
            Some(text)
        }
        Err(CaptionError::Empty) => {
            info!(
                counter = "captions_empty_total",
                media_kind = kind.as_str(),
                "caption provider returned an empty caption; placeholder"
            );
            None
        }
        Err(CaptionError::Provider(error)) => {
            warn!(counter = "captions_failed_total", media_kind = kind.as_str(), error = %error, "caption provider failed after its internal retries; placeholder");
            None
        }
    }
}

/// Bot API getFile + download into memory, bounded by DOWNLOAD_TIMEOUT.
/// `Vec<u8>` implements tokio's `AsyncWrite`, so it is the in-memory
/// sink of teloxide's `Download::download_file` — no file touches disk.
async fn download_media(bot: &Bot, file_id: &str) -> Result<Vec<u8>, String> {
    let fetch = async {
        let file = bot
            .get_file(teloxide::types::FileId(file_id.to_string()))
            .await
            .map_err(|error| format!("get_file failed: {error}"))?;
        let mut bytes = Vec::new();
        bot.download_file(&file.path, &mut bytes)
            .await
            .map_err(|error| format!("download failed: {error}"))?;
        Ok(bytes)
    };
    match tokio::time::timeout(DOWNLOAD_TIMEOUT, fetch).await {
        Ok(result) => result,
        Err(_) => Err(format!(
            "download timed out after {}s",
            DOWNLOAD_TIMEOUT.as_secs()
        )),
    }
}

/// The placeholder element of decision 82(d)/(h): an empty-caption
/// `<media>` element. Every placeholder emission counts
/// `placeholder_media_total`; this function is the single counting
/// point.
fn placeholder(kind: MediaKindName) -> String {
    info!(
        counter = "placeholder_media_total",
        media_kind = kind.as_str(),
        "media placeholder emitted"
    );
    render_media_element(kind, "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::future::Future;
    use std::pin::Pin;
    use tamako_core::caption::ScriptedCaption;

    const BOT_ID: u64 = 777_000;
    const DATE: i64 = 1_700_000_000;

    fn identity() -> BotIdentity {
        BotIdentity {
            id: BOT_ID,
            username: "tamako_bot".to_string(),
        }
    }

    fn message(extra: serde_json::Value) -> Message {
        let mut base = json!({
            "message_id": 1,
            "from": { "id": 42, "is_bot": false, "first_name": "Alice" },
            "chat": { "id": -1001234567890_i64, "type": "supergroup", "title": "Test Group" },
            "date": DATE,
        });
        base.as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        serde_json::from_value(base).expect("a valid Message")
    }

    fn sticker_json(is_animated: bool, is_video: bool) -> serde_json::Value {
        json!({
            "file_id": "sticker-file",
            "file_unique_id": "sticker-unique",
            "width": 512,
            "height": 512,
            "type": "regular",
            "is_animated": is_animated,
            "is_video": is_video,
        })
    }

    /// A dummy bot: builds requests without network (verified in
    /// teloxide-core 0.13.0 `bot.rs`: `Bot::new` only builds a reqwest
    /// client). The tests below never reach the download: animated/video
    /// stickers, video/animation placeholders, edited media, and sticker
    /// cache HITS all complete without a Bot API call by construction.
    fn dummy_bot() -> Bot {
        Bot::new("dummy-token")
    }

    fn temp_enricher(caption: Arc<dyn CaptionProvider>) -> (tempfile::TempDir, MediaEnricher) {
        let dir = tempfile::tempdir().expect("tempdir");
        let enricher = MediaEnricher {
            caption,
            media_store: Arc::new(MediaStore::open(dir.path()).expect("media store")),
        };
        (dir, enricher)
    }

    async fn enrich(enricher: &MediaEnricher, msg: &Message, edited: bool) -> NormalizedMessage {
        let class = normalize::classify_media(msg).expect("a media message");
        enrich_media_message(&dummy_bot(), &identity(), enricher, msg, &class, edited).await
    }

    /// A caption provider whose every call returns `CaptionError::Empty`
    /// (ScriptedCaption has no Empty mode).
    struct EmptyCaption;

    impl CaptionProvider for EmptyCaption {
        fn caption_image<'a>(
            &'a self,
            _jpeg_data_uri: &'a str,
            _kind: MediaKindName,
        ) -> Pin<Box<dyn Future<Output = Result<String, CaptionError>> + Send + 'a>> {
            Box::pin(async { Err(CaptionError::Empty) })
        }
    }

    #[tokio::test]
    async fn an_animated_sticker_is_a_placeholder_without_a_model_call() {
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone());
        let msg = message(json!({ "sticker": sticker_json(true, false) }));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(normalized.text, r#"<media type="animated"></media>"#);
        // Zero model calls: the format flag short-circuits before any
        // download or caption (decision 82(h)).
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn a_video_sticker_is_a_placeholder_without_a_model_call() {
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone());
        let msg = message(json!({ "sticker": sticker_json(false, true) }));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(normalized.text, r#"<media type="video"></media>"#);
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn a_static_sticker_cache_hit_costs_zero_model_calls() {
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone());
        enricher
            .media_store
            .put_sticker_caption("sticker-unique", "a waving dog")
            .expect("seed the cache");
        let msg = message(json!({ "sticker": sticker_json(false, false) }));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(
            normalized.text,
            r#"<media type="sticker">a waving dog</media>"#
        );
        // Zero model calls (decision 82(f)); zero downloads (the dummy
        // bot would have failed against the network).
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn a_video_message_is_a_placeholder_with_the_member_caption() {
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone());
        let msg = message(json!({
            "video": {
                "file_id": "video-file",
                "file_unique_id": "video-unique",
                "width": 640,
                "height": 360,
                "duration": 3,
                // Option<Mime> with a custom deserializer: the key must
                // be present (null) for the untagged MediaKind to match.
                "mime_type": null,
            },
            "caption": "watch this",
        }));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(
            normalized.text,
            r#"watch this <media type="video"></media>"#
        );
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn an_animation_message_is_an_animated_placeholder() {
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone());
        let msg = message(json!({
            "animation": {
                "file_id": "anim-file",
                "file_unique_id": "anim-unique",
                "width": 320,
                "height": 240,
                "duration": 2,
                "mime_type": null,
            },
        }));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(normalized.text, r#"<media type="animated"></media>"#);
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn an_edited_media_message_is_never_recaptioned() {
        // specs.md Section 15 open item: the placeholder carries an
        // EMPTY caption, the new caption text follows it, the timestamp
        // is the edit date, and no download or caption call happens.
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone());
        let msg = message(json!({
            "photo": [{
                "file_id": "photo-file",
                "file_unique_id": "photo-unique",
                "width": 100,
                "height": 100,
            }],
            "caption": "new caption",
            "edit_date": DATE + 5,
        }));
        let normalized = enrich(&enricher, &msg, true).await;
        assert_eq!(
            normalized.text,
            r#"new caption <media type="image"></media>"#
        );
        assert_eq!(
            normalized.timestamp,
            time::OffsetDateTime::from_unix_timestamp(DATE + 5).expect("valid timestamp")
        );
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn caption_success_counts_and_returns_the_caption() {
        let caption: Arc<dyn CaptionProvider> =
            Arc::new(ScriptedCaption::with_captions(vec!["a cat".to_string()]));
        let result = caption_data_uri(
            &caption,
            "data:image/jpeg;base64,QUJD",
            MediaKindName::Image,
        )
        .await;
        assert_eq!(result.as_deref(), Some("a cat"));
    }

    #[tokio::test]
    async fn a_provider_failure_maps_to_none_for_the_placeholder() {
        // captions_failed_total: the provider's internal retries already
        // exhausted (decision 82(d)).
        let caption: Arc<dyn CaptionProvider> = Arc::new(ScriptedCaption::failing("model down"));
        let result = caption_data_uri(
            &caption,
            "data:image/jpeg;base64,QUJD",
            MediaKindName::Image,
        )
        .await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn an_empty_caption_maps_to_none_distinctly() {
        // captions_empty_total: distinct from captions_failed_total
        // (acceptance finding 2).
        let caption: Arc<dyn CaptionProvider> = Arc::new(EmptyCaption);
        let result = caption_data_uri(
            &caption,
            "data:image/jpeg;base64,QUJD",
            MediaKindName::Sticker,
        )
        .await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn the_sticker_cache_write_round_trips_through_the_blocking_pool() {
        let caption: Arc<dyn CaptionProvider> = Arc::new(ScriptedCaption::failing("unused"));
        let (_dir, enricher) = temp_enricher(caption);
        cache_sticker_caption(&enricher, "sticker-unique", "a smiling cat").await;
        let stored = enricher
            .media_store
            .get_sticker_caption("sticker-unique")
            .expect("get");
        assert_eq!(stored.as_deref(), Some("a smiling cat"));
    }
}
