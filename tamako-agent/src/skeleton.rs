//! The skeleton-batch detector. Section 7.2 rule 5 of the database spec:
//! a batch that contains only emoji or greetings skips the extraction and
//! stores the MessageBatch skeleton only.
//!
//! The detector is deterministic and CONSERVATIVE: when in doubt, the
//! batch is extractable (this module returns false). A false negative
//! costs one extraction call; a false positive loses real content.

/// Greeting and acknowledgment forms, English and Chinese. Compared
/// against the lowercase text with leading and trailing punctuation
/// removed. A message in this set counts as insubstantial.
const GREETINGS: [&str; 34] = [
    "hi",
    "hello",
    "hey",
    "good morning",
    "good night",
    "morning",
    "night",
    "gm",
    "gn",
    "lol",
    "haha",
    "ok",
    "okay",
    "yes",
    "no",
    "thanks",
    "thank you",
    "thx",
    "你好",
    "您好",
    "在吗",
    "早上好",
    "晚上好",
    "晚安",
    "哈哈",
    "哈哈哈",
    "好的",
    "嗯",
    "嗯嗯",
    "哦",
    "谢谢",
    "感谢",
    "收到",
    "+1",
];

/// Section 7.2 rule 5: returns true only when EVERY message of the batch
/// is emoji-only or a greeting. One substantial message makes the batch
/// extractable. An empty batch is trivially a skeleton; the pipeline
/// returns Ok(None) before it reaches this check, so the empty case is
/// only defensive. When in doubt, extract.
pub fn is_skeleton_batch(texts: &[&str]) -> bool {
    texts.iter().all(|text| is_insubstantial(text))
}

/// One text counts as insubstantial when (a) it is emoji-only, or
/// (b) it is a greeting or acknowledgment.
fn is_insubstantial(text: &str) -> bool {
    is_emoji_only(text) || is_greeting(text)
}

/// A text is emoji-only when it contains no alphanumeric characters at
/// all. `char::is_alphanumeric` covers ascii, accented letters, and CJK
/// ideographs, so every writing system counts as substance. Everything
/// else (emoji of U+1F000..=U+1FAFF, U+2600..=U+27BF, U+2B00..=U+2BFF,
/// the U+FE0F variation selector, the U+200D zero-width joiner, the
/// U+20E3 keycap mark, skin-tone modifiers inside U+1F000..=U+1FAFF, and
/// plain punctuation) does not. Pure punctuation such as "???" therefore
/// also counts as insubstantial. That is deliberate: it carries no fact.
fn is_emoji_only(text: &str) -> bool {
    !text.chars().any(|c| c.is_alphanumeric())
}

/// A text is a greeting when its lowercase form, trimmed of leading and
/// trailing punctuation, is in the greeting set. The raw form is checked
/// too: "+1" is trimmed to "1" by the punctuation rule, so the raw form
/// keeps "+1" an acknowledgment.
fn is_greeting(text: &str) -> bool {
    let lowered = text.trim().to_lowercase();
    if lowered.is_empty() {
        return false;
    }
    if GREETINGS.contains(&lowered.as_str()) {
        return true;
    }
    let trimmed = lowered.trim_matches(|c: char| !c.is_alphanumeric());
    !trimmed.is_empty() && GREETINGS.contains(&trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pure_emoji_batch_is_a_skeleton() {
        assert!(is_skeleton_batch(&["🎉🎉", "👍", "😂❤️", "🐱‍👤"]));
    }

    #[test]
    fn pure_punctuation_counts_as_insubstantial() {
        assert!(is_skeleton_batch(&["???", "…", "!!!"]));
    }

    #[test]
    fn greetings_only_english_and_chinese_is_a_skeleton() {
        assert!(is_skeleton_batch(&[
            "good morning",
            "hi!",
            "thanks",
            "早上好",
            "晚安",
            "哈哈哈",
            "嗯嗯",
            "收到",
            "+1",
            "Ok.",
        ]));
    }

    #[test]
    fn one_real_sentence_makes_the_batch_extractable() {
        assert!(!is_skeleton_batch(&["hi", "let's deploy the fix tonight"]));
        assert!(!is_skeleton_batch(&["你好", "今天晚饭吃什么"]));
    }

    #[test]
    fn an_empty_batch_is_trivially_a_skeleton() {
        // Defensive: the pipeline returns Ok(None) for an empty tail
        // before this check runs.
        assert!(is_skeleton_batch(&[]));
    }

    #[test]
    fn a_sentence_with_an_acknowledgment_word_is_extractable() {
        // "ok let me check the deploy logs" is not the greeting "ok".
        assert!(!is_skeleton_batch(&["ok let me check the deploy logs"]));
    }

    #[test]
    fn emoji_mixed_with_text_is_extractable() {
        assert!(!is_skeleton_batch(&["deploy done 🎉"]));
    }
}
