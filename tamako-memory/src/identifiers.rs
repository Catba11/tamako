//! Deterministic node identifiers of proposed-graph-database-specs.md
//! Section 7.1. Rule R3 applies: primary keys are short and stable, and
//! primary keys never change.

use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

// Private UUID5 namespace for all Tamako identifiers. Generated once as a
// random UUID v4 on 2026-08-07. Rule R3: this constant must never change.
// A change would give every entity a new identifier and would split the
// graph of every existing database.
const NAMESPACE: Uuid = Uuid::from_u128(0x9068d01a_4a83_4ef0_971b_751eafe829b0);

fn uuid5(key: &str) -> String {
    Uuid::new_v5(&NAMESPACE, key.as_bytes())
        .hyphenated()
        .to_string()
}

/// Normalizes a surface form or a canonical name. Section 7.1
/// (decision 105): Unicode NFKC, every `@` stripped (after NFKC, which
/// folds the fullwidth U+FF20 to `@`), lowercase, no leading or
/// trailing spaces, one space between words, and NO space at an ASCII
/// letter<->digit boundary in either order ("Qwen 3.8" and "Qwen3.8"
/// carry one identifier).
pub fn normalize(text: &str) -> String {
    let nfkc: String = text.nfkc().collect();
    let mut out = String::with_capacity(nfkc.len());
    let mut after_space = true;
    for ch in nfkc
        .chars()
        .filter(|ch| *ch != '@')
        .flat_map(char::to_lowercase)
    {
        if ch.is_whitespace() {
            if !after_space {
                out.push(' ');
                after_space = true;
            }
        } else {
            out.push(ch);
            after_space = false;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    fold_letter_digit_boundaries(&out)
}

/// Decision 105 (b): removes a space at an ASCII letter<->digit
/// boundary in either order. The input carries single spaces only (the
/// collapse above), so the byte neighbors of a space are the token
/// boundaries; non-ASCII bytes are never ASCII alphanumeric, so the
/// byte check is UTF-8 safe.
fn fold_letter_digit_boundaries(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    for (index, ch) in text.char_indices() {
        let fold = ch == ' '
            && index > 0
            && index + 1 < bytes.len()
            && (bytes[index - 1].is_ascii_alphabetic() && bytes[index + 1].is_ascii_digit()
                || bytes[index - 1].is_ascii_digit() && bytes[index + 1].is_ascii_alphabetic());
        if !fold {
            out.push(ch);
        }
    }
    out
}

/// Person identifier: `uuid5("tg_user:{user_id}")`. Section 7.1.
pub fn person_id(user_id: &str) -> String {
    uuid5(&format!("tg_user:{user_id}"))
}

/// Alias identifier: `uuid5("alias:{normalized_surface_form}")`. Section 7.1.
pub fn alias_id(surface_form: &str) -> String {
    uuid5(&format!("alias:{}", normalize(surface_form)))
}

/// Concept identifier: `uuid5("concept:{normalized_canonical_name}")`.
/// Section 7.1.
pub fn concept_id(canonical_name: &str) -> String {
    uuid5(&format!("concept:{}", normalize(canonical_name)))
}

/// MessageBatch identifier: `uuid5("batch:{first_msg_id}:{last_msg_id}")`.
/// Section 7.1. The identifier is stable across retries (specs.md
/// Section 10.3).
pub fn batch_id(first_msg_id: i64, last_msg_id: i64) -> String {
    uuid5(&format!("batch:{first_msg_id}:{last_msg_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_stable_across_calls() {
        assert_eq!(person_id("12345"), person_id("12345"));
        assert_eq!(alias_id("Tama"), alias_id("Tama"));
        assert_eq!(concept_id("Graph Database"), concept_id("Graph Database"));
        assert_eq!(batch_id(1, 50), batch_id(1, 50));
    }

    #[test]
    fn identifiers_have_uuid_shape() {
        let id = person_id("12345");
        assert_eq!(id.len(), 36);
        assert_eq!(id.chars().filter(|c| *c == '-').count(), 4);
    }

    #[test]
    fn normalize_applies_nfkc_case_and_whitespace_rules() {
        // Decision 105: `@` stripping and the letter<->digit fold.
        assert_eq!(normalize("@Tama"), "tama");
        // NFKC folds the fullwidth U+FF20 to `@` first, so it strips.
        assert_eq!(normalize("＠tama"), "tama");
        assert_eq!(normalize("Qwen 3.8 27B"), "qwen3.8 27b");
        assert_eq!(normalize("Qwen3.8 27B"), "qwen3.8 27b");
        assert_eq!(normalize("Windows 11"), "windows11");
        assert_eq!(normalize("gemini 3.7 flash"), "gemini3.7flash");
        // A letter-letter or digit-digit boundary keeps its space.
        assert_eq!(normalize("graph database"), "graph database");
        assert_eq!(normalize("version 3 8"), "version3 8");
        assert_eq!(normalize("a @ b"), "a b");
    }

    #[test]
    fn normalize_rules_pre_decision_105_still_hold() {
        // Full-width characters fold to ASCII under NFKC.
        assert_eq!(normalize("Ｔａｍａｋｏ"), "tamako");
        assert_eq!(normalize("  Graph   Database  "), "graph database");
        // Ideographic space U+3000 folds to a normal space under NFKC.
        assert_eq!(normalize("GRPO　Algorithm"), "grpo algorithm");
        assert_eq!(normalize("a\t\n b"), "a b");
        assert_eq!(normalize(""), "");
    }

    #[test]
    fn normalized_inputs_give_equal_identifiers() {
        assert_eq!(alias_id("  TAMAKO "), alias_id("tamako"));
        assert_eq!(concept_id("Ｅｎｔｒｏｐｙ"), concept_id("entropy"));
        // Decision 105: mention-style and spacing variants fold.
        assert_eq!(alias_id("@tama"), alias_id("tama"));
        assert_eq!(concept_id("Qwen 3.8 27B"), concept_id("qwen3.8 27b"));
    }

    #[test]
    fn distinct_inputs_give_distinct_identifiers() {
        assert_ne!(person_id("1"), person_id("2"));
        assert_ne!(alias_id("tama"), concept_id("tama"));
        assert_ne!(batch_id(1, 50), batch_id(1, 51));
    }
}
