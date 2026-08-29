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

use std::future::Future;
use std::pin::Pin;
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
/// over the endpoint layer), the global sticker-caption cache, and the
/// download source.
///
/// The adapter holds an `Option<MediaEnricher>`; `None` keeps the
/// pre-enrichment behavior byte-identical (a photo/sticker with no text
/// is skipped by the pure dispatch).
pub struct MediaEnricher {
    pub caption: Arc<dyn CaptionProvider>,
    pub media_store: Arc<MediaStore>,
    /// The media byte source. The live implementation is
    /// [`TeloxideDownloader`] (Bot API getFile + byte download); tests
    /// inject a fake so every download-carrying path is drivable
    /// offline.
    pub downloader: Arc<dyn MediaDownloader>,
}

/// The download source of the enrichment pipeline: one file_id in, the
/// media bytes out. Seamed out of the pipeline so the download-carrying
/// paths (the only network touch in this module) are testable offline.
/// Object-safe with the same `Pin<Box>` convention as
/// `CaptionProvider`; the enricher holds an `Arc<dyn MediaDownloader>`.
pub trait MediaDownloader: Send + Sync {
    fn download<'a>(
        &'a self,
        file_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;
}

/// The live [`MediaDownloader`]: Bot API getFile + download into memory,
/// bounded by [`DOWNLOAD_TIMEOUT`]. Owns the `Bot` (teloxide's `Bot` is
/// a cheap Arc-backed clone), so the enrichment chain no longer threads
/// a `&Bot` separately from the enricher. Rule A1 holds: the binary
/// receives this through `TeloxideAdapter::media_downloader`, never a
/// teloxide type.
pub struct TeloxideDownloader {
    bot: Bot,
}

impl TeloxideDownloader {
    pub fn new(bot: Bot) -> Self {
        Self { bot }
    }
}

impl MediaDownloader for TeloxideDownloader {
    /// Bot API getFile + download into memory, bounded by
    /// DOWNLOAD_TIMEOUT. `Vec<u8>` implements tokio's `AsyncWrite`, so
    /// it is the in-memory sink of teloxide's `Download::download_file`
    /// — no file touches disk.
    fn download<'a>(
        &'a self,
        file_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        Box::pin(async move {
            let fetch = async {
                let file = self
                    .bot
                    .get_file(teloxide::types::FileId(file_id.to_string()))
                    .await
                    .map_err(|error| format!("get_file failed: {error}"))?;
                let mut bytes = Vec::new();
                self.bot
                    .download_file(&file.path, &mut bytes)
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
        })
    }
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
    identity: &BotIdentity,
    enricher: &MediaEnricher,
    msg: &Message,
    class: &MediaClass,
    edited: bool,
) -> NormalizedMessage {
    let element = if edited {
        placeholder(class.kind())
    } else {
        media_element_for(enricher, class).await
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
async fn media_element_for(enricher: &MediaEnricher, class: &MediaClass) -> String {
    match class {
        MediaClass::Photo { file_id, .. } => {
            caption_media_file(
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
        } => sticker_element(enricher, file_id, file_unique_id).await,
        // Videos, video notes, and animations are placeholders at
        // cutover: no download, no caption call (decision 82(h)).
        MediaClass::Video { .. } => placeholder(MediaKindName::Video),
        MediaClass::Animated { .. } => placeholder(MediaKindName::Animated),
    }
}

/// The static-sticker path: the global cache first (a hit costs ZERO
/// model calls, decision 82(f)); a miss downloads, normalizes, captions,
/// and best-effort caches the result.
async fn sticker_element(enricher: &MediaEnricher, file_id: &str, file_unique_id: &str) -> String {
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
        Ok(Ok(None)) => sticker_cache_miss(enricher, file_id, file_unique_id).await,
        Ok(Err(error)) => {
            warn!(error = %error, "sticker caption cache read failed; treating as a miss");
            sticker_cache_miss(enricher, file_id, file_unique_id).await
        }
        Err(join_error) => {
            warn!(error = %join_error, "sticker caption cache read task failed; treating as a miss");
            sticker_cache_miss(enricher, file_id, file_unique_id).await
        }
    }
}

/// A sticker cache miss: download -> normalize -> caption, then a
/// best-effort cache write of a successful caption.
async fn sticker_cache_miss(
    enricher: &MediaEnricher,
    file_id: &str,
    file_unique_id: &str,
) -> String {
    let kind = MediaKindName::Sticker;
    match caption_of_media_file(enricher, file_id, tamako_vision::MediaKind::Sticker, kind).await {
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
    enricher: &MediaEnricher,
    file_id: &str,
    vision_kind: tamako_vision::MediaKind,
    kind: MediaKindName,
) -> String {
    match caption_of_media_file(enricher, file_id, vision_kind, kind).await {
        Some(caption) => render_media_element(kind, &caption),
        None => placeholder(kind),
    }
}

/// The fallible core of the caption pipeline, split out so the sticker
/// cache-miss path can persist a successful caption. `Some(caption)` has
/// already counted `captions_total`; `None` has already logged and
/// counted the specific failure — the caller still owes the placeholder.
async fn caption_of_media_file(
    enricher: &MediaEnricher,
    file_id: &str,
    vision_kind: tamako_vision::MediaKind,
    kind: MediaKindName,
) -> Option<String> {
    let bytes = match enricher.downloader.download(file_id).await {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    /// A downloader double returning a scripted result and counting
    /// calls, so tests can drive the download-carrying paths offline
    /// and assert how far the pipeline got.
    struct FakeDownloader {
        result: Result<Vec<u8>, String>,
        calls: AtomicUsize,
    }

    impl FakeDownloader {
        fn ok(bytes: Vec<u8>) -> Self {
            Self {
                result: Ok(bytes),
                calls: AtomicUsize::new(0),
            }
        }

        fn failing(message: &str) -> Self {
            Self {
                result: Err(message.to_string()),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl MediaDownloader for FakeDownloader {
        fn download<'a>(
            &'a self,
            _file_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let result = self.result.clone();
            Box::pin(async move { result })
        }
    }

    /// A downloader that panics if called: the short-circuit tests
    /// (animated/video stickers, video/animation placeholders, edited
    /// media, sticker cache hits) must complete without ANY download.
    struct NoDownload;

    impl MediaDownloader for NoDownload {
        fn download<'a>(
            &'a self,
            _file_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
            panic!("must not be called")
        }
    }

    /// A tiny valid image encoded in-code (the workspace `image` crate
    /// has PNG encode + decode; `normalize_image` accepts any sniffable
    /// image format and re-encodes to JPEG).
    fn tiny_image_bytes() -> Vec<u8> {
        let rgba = image::ImageBuffer::from_pixel(8, 8, image::Rgba([200, 30, 30, 255]));
        let mut bytes = Vec::new();
        image::DynamicImage::ImageRgba8(rgba)
            .write_to(
                &mut std::io::Cursor::new(&mut bytes),
                image::ImageFormat::Png,
            )
            .expect("PNG encode of in-code fixture must succeed");
        bytes
    }

    fn temp_enricher(
        caption: Arc<dyn CaptionProvider>,
        downloader: Arc<dyn MediaDownloader>,
    ) -> (tempfile::TempDir, MediaEnricher) {
        let dir = tempfile::tempdir().expect("tempdir");
        let enricher = MediaEnricher {
            caption,
            media_store: Arc::new(MediaStore::open(dir.path()).expect("media store")),
            downloader,
        };
        (dir, enricher)
    }

    async fn enrich(enricher: &MediaEnricher, msg: &Message, edited: bool) -> NormalizedMessage {
        let class = normalize::classify_media(msg).expect("a media message");
        enrich_media_message(&identity(), enricher, msg, &class, edited).await
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

    /// A caption provider whose every call returns `CaptionError::Empty`
    /// (ScriptedCaption has no Empty mode) and counts its calls, so a
    /// test can assert the adapter captions a media item EXACTLY ONCE
    /// (the retry discipline is the tamako-agent decorator's, not the
    /// adapter's).
    struct CountingEmptyCaption {
        calls: AtomicUsize,
    }

    impl CountingEmptyCaption {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl CaptionProvider for CountingEmptyCaption {
        fn caption_image<'a>(
            &'a self,
            _jpeg_data_uri: &'a str,
            _kind: MediaKindName,
        ) -> Pin<Box<dyn Future<Output = Result<String, CaptionError>> + Send + 'a>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(CaptionError::Empty) })
        }
    }

    #[tokio::test]
    async fn an_animated_sticker_is_a_placeholder_without_a_model_call() {
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone(), Arc::new(NoDownload));
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
        let (_dir, enricher) = temp_enricher(caption.clone(), Arc::new(NoDownload));
        let msg = message(json!({ "sticker": sticker_json(false, true) }));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(normalized.text, r#"<media type="video"></media>"#);
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn a_static_sticker_cache_hit_costs_zero_model_calls() {
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone(), Arc::new(NoDownload));
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
        // Zero model calls (decision 82(f)); zero downloads (NoDownload
        // panics if the cache hit path ever reaches the downloader).
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn a_video_message_is_a_placeholder_with_the_member_caption() {
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let (_dir, enricher) = temp_enricher(caption.clone(), Arc::new(NoDownload));
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
        let (_dir, enricher) = temp_enricher(caption.clone(), Arc::new(NoDownload));
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
        let (_dir, enricher) = temp_enricher(caption.clone(), Arc::new(NoDownload));
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
        let (_dir, enricher) = temp_enricher(caption, Arc::new(NoDownload));
        cache_sticker_caption(&enricher, "sticker-unique", "a smiling cat").await;
        let stored = enricher
            .media_store
            .get_sticker_caption("sticker-unique")
            .expect("get");
        assert_eq!(stored.as_deref(), Some("a smiling cat"));
    }

    fn photo_message(extra: serde_json::Value) -> Message {
        let mut photo = json!({
            "photo": [{
                "file_id": "photo-file",
                "file_unique_id": "photo-unique",
                "width": 100,
                "height": 100,
            }],
        });
        photo
            .as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        message(photo)
    }

    #[tokio::test]
    async fn a_download_failure_is_a_placeholder_and_the_message_is_not_dropped() {
        // Decision 82(d): a failed download degrades to the
        // empty-caption placeholder; the message is NEVER dropped and
        // the caption provider is never reached.
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let downloader = Arc::new(FakeDownloader::failing("boom"));
        let (_dir, enricher) = temp_enricher(caption.clone(), downloader.clone());
        let msg = photo_message(json!({}));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(normalized.text, r#"<media type="image"></media>"#);
        assert_eq!(normalized.sender_id, "42");
        assert_eq!(downloader.calls(), 1);
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn an_empty_caption_is_a_placeholder_captioned_exactly_once() {
        // captions_empty_total: `CaptionError::Empty` degrades to the
        // placeholder. The adapter calls the provider EXACTLY ONCE per
        // media item — the retry discipline lives in the tamako-agent
        // decorator, not here.
        let caption = Arc::new(CountingEmptyCaption {
            calls: AtomicUsize::new(0),
        });
        let downloader = Arc::new(FakeDownloader::ok(tiny_image_bytes()));
        let (_dir, enricher) = temp_enricher(caption.clone(), downloader.clone());
        let msg = photo_message(json!({}));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(normalized.text, r#"<media type="image"></media>"#);
        assert_eq!(downloader.calls(), 1);
        assert_eq!(caption.calls(), 1);
    }

    #[tokio::test]
    async fn a_provider_failure_is_a_placeholder_after_one_caption_call() {
        // captions_failed_total: the provider's internal retries already
        // exhausted (decision 82(d)); the adapter makes ONE call and
        // degrades to the placeholder.
        let caption = Arc::new(ScriptedCaption::failing("model down"));
        let downloader = Arc::new(FakeDownloader::ok(tiny_image_bytes()));
        let (_dir, enricher) = temp_enricher(caption.clone(), downloader.clone());
        let msg = photo_message(json!({}));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(normalized.text, r#"<media type="image"></media>"#);
        assert_eq!(downloader.calls(), 1);
        assert_eq!(caption.inputs().len(), 1);
    }

    #[tokio::test]
    async fn a_sticker_cache_miss_captions_once_then_the_hit_costs_nothing() {
        // Decision 82(f): a cache miss downloads, captions ONCE, and
        // writes the caption; a second identical sticker is served from
        // the cache with NO further download or caption call.
        let caption = Arc::new(ScriptedCaption::with_captions(vec![
            "a cat sticker".to_string()
        ]));
        let downloader = Arc::new(FakeDownloader::ok(tiny_image_bytes()));
        let (_dir, enricher) = temp_enricher(caption.clone(), downloader.clone());
        let msg = message(json!({ "sticker": sticker_json(false, false) }));
        let first = enrich(&enricher, &msg, false).await;
        assert_eq!(first.text, r#"<media type="sticker">a cat sticker</media>"#);
        assert_eq!(caption.inputs().len(), 1);
        assert_eq!(downloader.calls(), 1);
        let stored = enricher
            .media_store
            .get_sticker_caption("sticker-unique")
            .expect("get");
        assert_eq!(stored.as_deref(), Some("a cat sticker"));

        // The second identical sticker: a cache hit, zero further model
        // calls, zero further downloads.
        let second = enrich(&enricher, &msg, false).await;
        assert_eq!(
            second.text,
            r#"<media type="sticker">a cat sticker</media>"#
        );
        assert_eq!(caption.inputs().len(), 1);
        assert_eq!(downloader.calls(), 1);
    }

    #[tokio::test]
    async fn garbage_bytes_fail_normalization_to_a_placeholder() {
        // A successful download of undecodable bytes: normalization
        // fails, the placeholder renders, the message is not dropped,
        // and the caption provider is never reached.
        let caption = Arc::new(ScriptedCaption::failing("must not be called"));
        let downloader = Arc::new(FakeDownloader::ok(b"not an image at all".to_vec()));
        let (_dir, enricher) = temp_enricher(caption.clone(), downloader.clone());
        let msg = photo_message(json!({}));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(normalized.text, r#"<media type="image"></media>"#);
        assert_eq!(downloader.calls(), 1);
        assert!(caption.inputs().is_empty());
    }

    #[tokio::test]
    async fn a_photo_enriches_end_to_end_with_the_member_caption() {
        // The happy path through the FULL enrich_media_message: download
        // ok -> normalize ok -> caption ok -> the captioned element
        // follows the member's caption text.
        let caption = Arc::new(ScriptedCaption::with_captions(vec![
            "a cat on the sofa".to_string()
        ]));
        let downloader = Arc::new(FakeDownloader::ok(tiny_image_bytes()));
        let (_dir, enricher) = temp_enricher(caption.clone(), downloader.clone());
        let msg = photo_message(json!({ "caption": "look at this" }));
        let normalized = enrich(&enricher, &msg, false).await;
        assert_eq!(
            normalized.text,
            r#"look at this <media type="image">a cat on the sofa</media>"#
        );
        assert_eq!(downloader.calls(), 1);
        assert_eq!(caption.inputs().len(), 1);
        assert_eq!(caption.inputs()[0].kind, MediaKindName::Image);
    }
}
