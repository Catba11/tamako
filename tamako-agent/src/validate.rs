//! Post-validation of extracted relationship names in plain Rust.
//! Section 6.3 of the database spec. The prompt is never trusted: every
//! name the LLM produces is checked here before the write.

/// The reserved system relationship names of Section 6.3. The LLM must
/// not generate them.
pub const RESERVED_RELATIONSHIP_NAMES: [&str; 5] = [
    "contains",
    "known_as",
    "also_known_as",
    "is_a",
    "supersedes",
];

/// The fallback name for an invalid or reserved relationship name
/// (Section 6.3). The original name goes into the edge properties.
pub const FALLBACK_RELATIONSHIP_NAME: &str = "related_to";

/// snake_case identifier check: lowercase ascii letters, digits,
/// underscores; starts with a letter; length 1..=64.
pub fn is_snake_case_identifier(name: &str) -> bool {
    if name.is_empty() || name.len() > 64 {
        return false;
    }
    let mut chars = name.chars();
    // The first character is a lowercase ascii letter. The length check
    // above guarantees that `next` returns Some.
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// The outcome of validating one extracted relationship name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipName {
    /// A valid open-vocabulary snake_case name. Section 6.3.
    Valid(String),
    /// An invalid or reserved name. The edge is written as
    /// `related_to`; the original name goes into the edge properties.
    Fallback { original: String },
}

/// Section 6.3: a name that is not a snake_case identifier, or that is a
/// reserved system name, becomes `related_to`; the original name is
/// preserved for the edge properties.
pub fn validate_relationship_name(name: &str) -> RelationshipName {
    if is_snake_case_identifier(name) && !RESERVED_RELATIONSHIP_NAMES.contains(&name) {
        RelationshipName::Valid(name.to_string())
    } else {
        RelationshipName::Fallback {
            original: name.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_snake_case_names_pass() {
        for name in ["likes", "works_at", "a", "z9_", "currently_playing"] {
            assert!(is_snake_case_identifier(name), "expected valid: {name}");
            assert_eq!(
                validate_relationship_name(name),
                RelationshipName::Valid(name.to_string())
            );
        }
    }

    #[test]
    fn invalid_names_fall_back() {
        for name in [
            "Likes",
            "has space",
            "contains!",
            "1abc",
            "",
            "_lead",
            "RELATED",
        ] {
            assert!(!is_snake_case_identifier(name), "expected invalid: {name}");
            assert_eq!(
                validate_relationship_name(name),
                RelationshipName::Fallback {
                    original: name.to_string()
                }
            );
        }
    }

    #[test]
    fn names_longer_than_64_chars_are_invalid() {
        let long = "a".repeat(65);
        assert!(!is_snake_case_identifier(&long));
        let exact = "a".repeat(64);
        assert!(is_snake_case_identifier(&exact));
    }

    #[test]
    fn all_reserved_system_names_fall_back() {
        // Section 6.3: the reserved names are valid snake_case but the
        // LLM must not generate them.
        for name in RESERVED_RELATIONSHIP_NAMES {
            assert!(is_snake_case_identifier(name));
            assert_eq!(
                validate_relationship_name(name),
                RelationshipName::Fallback {
                    original: name.to_string()
                }
            );
        }
    }

    #[test]
    fn the_fallback_name_itself_is_valid_snake_case() {
        assert_eq!(
            validate_relationship_name(FALLBACK_RELATIONSHIP_NAME),
            RelationshipName::Valid(FALLBACK_RELATIONSHIP_NAME.to_string())
        );
    }
}
