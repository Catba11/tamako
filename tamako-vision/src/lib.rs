//! tamako-vision: the pure image-normalization library of media
//! captioning at intake (current-state.md decision 82).
//!
//! Bytes in, bytes out. No network, no LLM client, no platform
//! adapter, no config plumbing — this crate is a leaf that turns raw
//! inbound image/sticker bytes into a baseline JPEG suitable for a
//! vision caption model, plus a base64 data-URI helper for the wire.
//!
//! The normalize step is CPU-bound and SYNC by design (decision
//! 82(a)/(b)): callers on an async runtime MUST wrap it in
//! `tokio::task::spawn_blocking`. This crate never pulls in tokio.
//!
//! Media-kind-parameterized from day one (decision 82(h)):
//! [`MediaKind`] enumerates every inbound media shape, but only
//! `Image` and `Sticker` are in scope at cutover. `Video` and
//! `Animated` are rejected with a typed
//! [`VisionError::Unsupported`] BEFORE any bytes are touched.

use image::imageops::FilterType;
use image::{DynamicImage, ImageBuffer, RgbImage, RgbaImage};

/// The long-edge ceiling of the normalized output (decision 82): the
/// vision caption model's baseline input. Aspect ratio is preserved;
/// smaller inputs are NEVER upscaled.
pub const MAX_LONG_EDGE: u32 = 2048;

/// The baseline JPEG quality of the normalized output.
pub const JPEG_QUALITY: u8 = 85;

/// The media kind of an inbound attachment (decision 82(h)).
/// Parameterized from day one so the pipeline never has to re-thread
/// kind information later; only `Image` and `Sticker` are normalized
/// at cutover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    /// A plain image attachment (photo, document-as-image).
    Image,
    /// A static sticker (typically WebP/PNG with alpha).
    Sticker,
    /// A video attachment. Out of scope at cutover; rejected.
    Video,
    /// An animated sticker/GIF. Out of scope at cutover; rejected.
    Animated,
}

/// The media type of a normalized image, as seen by the agent
/// boundary. Kept minimal and extensible: the adapter maps this to
/// its provider's wire type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageMediaType {
    /// Baseline JPEG (`image/jpeg`).
    Jpeg,
}

/// The normalized output: a baseline JPEG plus its FINAL
/// (post-resize) dimensions.
#[derive(Debug, Clone)]
pub struct NormalizedImage {
    /// The re-encoded baseline JPEG bytes.
    pub jpeg_bytes: Vec<u8>,
    /// The final width in pixels (post-resize).
    pub width: u32,
    /// The final height in pixels (post-resize).
    pub height: u32,
    /// The media type of `jpeg_bytes` (always `Jpeg` at cutover).
    pub media_type: ImageMediaType,
}

/// Errors of the normalization pipeline.
#[derive(Debug, thiserror::Error)]
pub enum VisionError {
    /// The media kind is out of scope at cutover (decision 82(h)).
    /// Raised BEFORE the bytes are touched.
    #[error("media kind {kind:?} is not supported at intake (decision 82(h))")]
    Unsupported {
        /// The rejected kind.
        kind: MediaKind,
    },
    /// The inbound bytes did not sniff/decode as a supported image.
    /// Garbage input is an ordinary typed error, never a panic.
    #[error("failed to decode image bytes: {0}")]
    Decode(#[source] image::ImageError),
    /// The decoded image failed to re-encode as baseline JPEG.
    #[error("failed to encode normalized JPEG: {0}")]
    Encode(#[source] image::ImageError),
}

/// Normalizes inbound image/sticker bytes to a baseline JPEG
/// (decision 82): sniff the format, decode, composite alpha onto a
/// WHITE canvas, downscale so the long edge is at most
/// [`MAX_LONG_EDGE`] (never upscale), and re-encode at
/// [`JPEG_QUALITY`].
///
/// CPU-bound and synchronous: async callers must use
/// `tokio::task::spawn_blocking`.
///
/// # Errors
///
/// - [`VisionError::Unsupported`] for [`MediaKind::Video`] /
///   [`MediaKind::Animated`], raised before the bytes are read.
/// - [`VisionError::Decode`] for bytes that do not decode as an
///   image, or for degenerate zero-dimension images.
/// - [`VisionError::Encode`] if the JPEG encoder fails.
pub fn normalize_image(bytes: &[u8], kind: MediaKind) -> Result<NormalizedImage, VisionError> {
    // 1. Kind gate first: Video/Animated never reach the decoder.
    match kind {
        MediaKind::Image | MediaKind::Sticker => {}
        MediaKind::Video | MediaKind::Animated => {
            return Err(VisionError::Unsupported { kind });
        }
    }

    // 2. Sniff, then 3. decode. Garbage bytes surface as a typed
    //    Decode error, never a panic.
    let format = image::guess_format(bytes).map_err(VisionError::Decode)?;
    let decoded =
        image::load_from_memory_with_format(bytes, format).map_err(VisionError::Decode)?;
    if decoded.width() == 0 || decoded.height() == 0 {
        // Degenerate input: nothing meaningful to composite or resize.
        return Err(VisionError::Decode(image::ImageError::Parameter(
            image::error::ParameterError::from_kind(
                image::error::ParameterErrorKind::DimensionMismatch,
            ),
        )));
    }

    // 4. Alpha compositing: overlay the RGBA frame onto an opaque
    //    WHITE canvas, THEN drop the (now uniformly opaque) alpha
    //    channel. A bare `.into_rgb8()` would drop alpha FIRST and
    //    leave black/garbage fringes on transparent stickers.
    let rgba: RgbaImage = decoded.into_rgba8();
    let mut canvas: RgbaImage = ImageBuffer::from_pixel(
        rgba.width(),
        rgba.height(),
        image::Rgba([255, 255, 255, 255]),
    );
    image::imageops::overlay(&mut canvas, &rgba, 0, 0);
    let rgb: RgbImage = DynamicImage::ImageRgba8(canvas).into_rgb8();

    // 5. Downscale the long edge to MAX_LONG_EDGE, aspect preserved,
    //    never upscale. Integer math, clamped away from zero.
    let (width, height) = (rgb.width(), rgb.height());
    let rgb = if width.max(height) > MAX_LONG_EDGE {
        let (target_w, target_h) = scaled_dimensions(width, height);
        image::imageops::resize(&rgb, target_w, target_h, FilterType::Triangle)
    } else {
        rgb
    };
    let (width, height) = (rgb.width(), rgb.height());

    // 6. Re-encode as baseline JPEG.
    let mut jpeg_bytes = Vec::new();
    let mut encoder =
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg_bytes, JPEG_QUALITY);
    encoder.encode_image(&rgb).map_err(VisionError::Encode)?;

    Ok(NormalizedImage {
        jpeg_bytes,
        width,
        height,
        media_type: ImageMediaType::Jpeg,
    })
}

/// The aspect-preserving target dimensions with the long edge exactly
/// [`MAX_LONG_EDGE`]. Callers must only invoke this when the long edge
/// exceeds the cap; both outputs are clamped to at least 1.
fn scaled_dimensions(width: u32, height: u32) -> (u32, u32) {
    let long = u64::from(width.max(height));
    debug_assert!(long > u64::from(MAX_LONG_EDGE));
    // u64 math: width * MAX_LONG_EDGE cannot overflow (u32 * small).
    let scale =
        |dim: u32| -> u32 { ((u64::from(dim) * u64::from(MAX_LONG_EDGE)) / long).max(1) as u32 };
    (scale(width), scale(height))
}

/// Encodes normalized JPEG bytes as a base64 data URI
/// (`data:image/jpeg;base64,...`) for the vision caption model. The
/// `image/jpeg` media-type string is single-sourced here.
pub fn to_base64_data_uri(jpeg_bytes: &[u8]) -> String {
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD.encode(jpeg_bytes);
    format!("data:image/jpeg;base64,{body}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageBuffer, Rgba};
    use std::io::Cursor;

    /// Encodes an RGBA buffer as PNG bytes (the `png` feature covers
    /// encode as well as decode).
    fn to_png_bytes(rgba: &RgbaImage) -> Vec<u8> {
        let mut bytes = Vec::new();
        DynamicImage::ImageRgba8(rgba.clone())
            .write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::Png)
            .expect("PNG encode of in-code fixture must succeed");
        bytes
    }

    /// A solid-color RGBA fixture of the given dimensions.
    fn solid_rgba(width: u32, height: u32, pixel: Rgba<u8>) -> RgbaImage {
        ImageBuffer::from_pixel(width, height, pixel)
    }

    /// A tiny deterministic PRNG (xorshift32) — `rand` is NOT a
    /// dependency of this crate.
    struct XorShift(u32);

    impl XorShift {
        fn next(&mut self) -> u32 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 17;
            self.0 ^= self.0 << 5;
            self.0
        }
    }

    /// Deterministic garbage bytes of the given length.
    fn garbage(len: usize, seed: u32) -> Vec<u8> {
        let mut rng = XorShift(seed.max(1));
        (0..len).map(|_| (rng.next() & 0xff) as u8).collect()
    }

    // (a) Alpha compositing: transparent regions must become WHITE,
    //     not black/garbage.
    #[test]
    fn transparent_pixels_composite_to_white() {
        // 4x4: left half fully transparent, right half opaque red.
        let mut rgba = ImageBuffer::from_pixel(4, 4, Rgba([200, 0, 0, 255]));
        for y in 0..4 {
            for x in 0..2 {
                rgba.put_pixel(x, y, Rgba([0, 0, 255, 0])); // fully transparent blue
            }
        }
        let png = to_png_bytes(&rgba);

        let out = normalize_image(&png, MediaKind::Image).expect("normalize must succeed");
        let decoded =
            image::load_from_memory_with_format(&out.jpeg_bytes, image::ImageFormat::Jpeg)
                .expect("output must decode as JPEG")
                .into_rgb8();

        // The transparent half must be (near-)white. JPEG is lossy;
        // allow a small tolerance but reject anything remotely black.
        for y in 0..out.height {
            for x in 0..(out.width / 2) {
                let p = decoded.get_pixel(x, y);
                assert!(
                    p[0] >= 250 && p[1] >= 250 && p[2] >= 250,
                    "transparent region must composite to white, got {p:?} at ({x},{y})"
                );
            }
        }
        // And the opaque half must stay reddish, proving the canvas
        // did not wash out real content.
        let p = decoded.get_pixel(out.width - 1, out.height - 1);
        assert!(
            p[0] > 150 && p[1] < 80 && p[2] < 80,
            "opaque red must survive, got {p:?}"
        );
    }

    // (b) Resize: 4096x1024 -> 2048x512 (long edge capped, aspect
    //     preserved); 100x100 -> 100x100 (no upscale).
    #[test]
    fn long_edge_is_capped_and_aspect_preserved() {
        let big = to_png_bytes(&solid_rgba(4096, 1024, Rgba([10, 200, 30, 255])));
        let out = normalize_image(&big, MediaKind::Image).expect("normalize must succeed");
        assert_eq!((out.width, out.height), (2048, 512));
    }

    #[test]
    fn small_image_is_never_upscaled() {
        let small = to_png_bytes(&solid_rgba(100, 100, Rgba([10, 200, 30, 255])));
        let out = normalize_image(&small, MediaKind::Image).expect("normalize must succeed");
        assert_eq!((out.width, out.height), (100, 100));
    }

    #[test]
    fn portrait_long_edge_is_capped() {
        let tall = to_png_bytes(&solid_rgba(1024, 4096, Rgba([10, 200, 30, 255])));
        let out = normalize_image(&tall, MediaKind::Image).expect("normalize must succeed");
        assert_eq!((out.width, out.height), (512, 2048));
    }

    // (c) The output is a valid baseline JPEG that sniffs as Jpeg.
    #[test]
    fn output_is_valid_jpeg() {
        let png = to_png_bytes(&solid_rgba(64, 48, Rgba([1, 2, 3, 255])));
        let out = normalize_image(&png, MediaKind::Sticker).expect("normalize must succeed");
        assert_eq!(out.media_type, ImageMediaType::Jpeg);
        assert_eq!(
            image::guess_format(&out.jpeg_bytes).expect("output must sniff"),
            image::ImageFormat::Jpeg
        );
        let decoded =
            image::load_from_memory_with_format(&out.jpeg_bytes, image::ImageFormat::Jpeg)
                .expect("output must decode as JPEG");
        assert_eq!((decoded.width(), decoded.height()), (out.width, out.height));
    }

    // (d) Video/Animated are rejected with the typed error, before
    //     the bytes are touched (garbage bytes prove the gate fires
    //     first).
    #[test]
    fn unsupported_kinds_are_rejected_before_decoding() {
        for kind in [MediaKind::Video, MediaKind::Animated] {
            match normalize_image(&garbage(64, 7), kind) {
                Err(VisionError::Unsupported { kind: got }) => assert_eq!(got, kind),
                other => panic!("expected Unsupported for {kind:?}, got {other:?}"),
            }
        }
    }

    // (e) Fuzz-lite: garbage bytes of varying lengths must return a
    //     typed Err and NEVER panic.
    #[test]
    fn garbage_bytes_error_without_panicking() {
        for len in [0usize, 1, 7, 32, 255, 1024, 4096] {
            for seed in 1..=40u32 {
                let bytes = garbage(len, seed.wrapping_mul(0x9e37_79b9).wrapping_add(len as u32));
                match normalize_image(&bytes, MediaKind::Image) {
                    Err(VisionError::Decode(_)) => {}
                    Err(other) => panic!("expected Decode error for garbage, got {other:?}"),
                    Ok(_) => panic!("garbage bytes must not normalize (len={len}, seed={seed})"),
                }
            }
        }
    }

    // WebP: a static WebP with alpha (the sticker case) must decode
    // and composite. The fixture is checked in: the `webp` feature of
    // the image crate is DECODE-only, so an in-code encoder is not
    // available.
    #[test]
    fn webp_sticker_decodes_and_composites() {
        let webp = include_bytes!("../tests/fixtures/sticker-alpha.webp");
        assert_eq!(
            image::guess_format(webp).expect("fixture must sniff as WebP"),
            image::ImageFormat::WebP
        );
        let out = normalize_image(webp, MediaKind::Sticker).expect("webp must normalize");
        let decoded =
            image::load_from_memory_with_format(&out.jpeg_bytes, image::ImageFormat::Jpeg)
                .expect("output must decode as JPEG")
                .into_rgb8();
        // The fixture's transparent corner must be white.
        let p = decoded.get_pixel(0, 0);
        assert!(
            p[0] >= 250 && p[1] >= 250 && p[2] >= 250,
            "webp alpha must composite to white, got {p:?}"
        );
    }

    // to_base64_data_uri: exact prefix + base64 round-trip.
    #[test]
    fn data_uri_round_trips() {
        let payload = b"\xff\xd8\xff\xe0 pretend jpeg \xff\xd9";
        let uri = to_base64_data_uri(payload);
        assert!(uri.starts_with("data:image/jpeg;base64,"));
        use base64::Engine;
        let body = &uri["data:image/jpeg;base64,".len()..];
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(body)
            .expect("base64 body must decode");
        assert_eq!(decoded, payload);
    }

    // The resize math itself: exact cap on the long edge, clamped
    // away from zero on extreme aspect ratios.
    #[test]
    fn scaled_dimensions_hit_the_cap_exactly() {
        assert_eq!(scaled_dimensions(4096, 1024), (2048, 512));
        assert_eq!(scaled_dimensions(1024, 4096), (512, 2048));
        assert_eq!(scaled_dimensions(2049, 2049), (2048, 2048));
        // Extreme aspect ratio: short edge clamps to at least 1.
        let (w, h) = scaled_dimensions(1_000_000, 1);
        assert_eq!(w, 2048);
        assert_eq!(h, 1);
    }
}
