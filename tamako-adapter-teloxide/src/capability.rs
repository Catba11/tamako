//! Bot capability detection and outbound permission classification.
//!
//! Rule A1: no teloxide type crosses the boundary. `BotChatStatus` is the
//! only type that leaves this module; the teloxide `ChatMemberKind` and the
//! `RequestError` stay inside.
//!
//! Verified in the teloxide-core 0.13.0 source:
//!
//! - `types/chat_member.rs`: `ChatMemberKind` has the variants `Owner`
//!   (serde name "creator"), `Administrator`, `Member`, `Restricted`,
//!   `Left`, and `Banned` (serde name "kicked").
//! - `errors.rs`: the permission and access `ApiError` variants are the
//!   `NotEnoughRights*` family, `BotBlocked`, `BotKicked`,
//!   `BotKickedFromSupergroup`, `BotKickedFromChannel`, and `ChatNotFound`.
//!   `ApiError::Unknown(String)` is the catch-all: Telegram serves many
//!   rights errors as `Bad Request: not enough rights to ...` text that
//!   teloxide does not enumerate.

use teloxide::types::ChatMemberKind;
use teloxide::{ApiError, RequestError};

/// The bot's membership status in one chat. Normalized; no teloxide type
/// crosses the boundary (Rule A1). Not persisted: membership can change,
/// so consumers re-evaluate on each startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotChatStatus {
    Administrator,
    Member,
    RestrictedOrOther,
    Unknown,
}

/// Maps a teloxide `ChatMemberKind` to the normalized status. Pure.
///
/// The chat owner counts as an administrator: the owner has every right an
/// administrator has. `Unknown` is never produced here; only a failed or
/// impossible query gives `Unknown` (see `TeloxideAdapter::bot_chat_status`).
pub(crate) fn classify_member_kind(kind: &ChatMemberKind) -> BotChatStatus {
    match kind {
        ChatMemberKind::Owner(_) | ChatMemberKind::Administrator(_) => BotChatStatus::Administrator,
        ChatMemberKind::Member(_) => BotChatStatus::Member,
        ChatMemberKind::Restricted(_) | ChatMemberKind::Left | ChatMemberKind::Banned(_) => {
            BotChatStatus::RestrictedOrOther
        }
    }
}

/// True when a request error means the bot lacks the permission or access
/// for the action. False for everything else (network errors, invalid ids,
/// ...).
///
/// The enumerated variants are the rights and access failures verified in
/// teloxide-core 0.13.0 `errors.rs`. `ChatNotFound` is included: Telegram
/// answers with it when the bot is not a member of the chat. The `Unknown`
/// text match is a documented pragmatic catch for the rights errors
/// teloxide does not enumerate (verified: `ApiError::Unknown` is the
/// catch-all in `errors.rs`).
pub(crate) fn is_permission_error(err: &RequestError) -> bool {
    let RequestError::Api(api) = err else {
        return false;
    };
    match api {
        ApiError::NotEnoughRightsToPinMessage
        | ApiError::NotEnoughRightsToManagePins
        | ApiError::NotEnoughRightsToChangeChatPermissions
        | ApiError::NotEnoughRightsToRestrict
        | ApiError::NotEnoughRightsToPostMessages
        | ApiError::BotBlocked
        | ApiError::BotKicked
        | ApiError::BotKickedFromSupergroup
        | ApiError::BotKickedFromChannel
        | ApiError::ChatNotFound => true,
        ApiError::Unknown(text) => {
            let text = text.to_lowercase();
            text.contains("not enough rights") || text.contains("administrator rights")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use super::*;
    use serde_json::json;
    use teloxide::types::ChatMember;

    /// A ChatMember from JSON, like the normalize tests build Updates. The
    /// serde tag is `status` (verified in `types/chat_member.rs`).
    fn member(extra: serde_json::Value) -> ChatMember {
        let mut base = json!({
            "user": { "id": 777_000, "is_bot": true, "first_name": "Tamako" },
        });
        base.as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        serde_json::from_value(base).expect("a valid ChatMember")
    }

    #[test]
    fn an_owner_is_an_administrator() {
        let member = member(json!({ "status": "creator", "is_anonymous": false }));
        assert_eq!(
            classify_member_kind(&member.kind),
            BotChatStatus::Administrator
        );
    }

    #[test]
    fn an_administrator_is_an_administrator() {
        let member = member(json!({
            "status": "administrator",
            "is_anonymous": false,
            "can_be_edited": false,
            "can_manage_chat": true,
            "can_change_info": true,
            "can_delete_messages": true,
            "can_manage_video_chats": true,
            "can_invite_users": true,
            "can_restrict_members": true,
            "can_promote_members": false,
        }));
        assert_eq!(
            classify_member_kind(&member.kind),
            BotChatStatus::Administrator
        );
    }

    #[test]
    fn a_plain_member_is_a_member() {
        let member = member(json!({ "status": "member" }));
        assert_eq!(classify_member_kind(&member.kind), BotChatStatus::Member);
    }

    #[test]
    fn a_restricted_member_is_restricted_or_other() {
        let member = member(json!({
            "status": "restricted",
            "is_member": true,
            "can_send_messages": false,
            "can_send_audios": false,
            "can_send_documents": false,
            "can_send_photos": false,
            "can_send_videos": false,
            "can_send_video_notes": false,
            "can_send_voice_notes": false,
            "can_send_other_messages": false,
            "can_add_web_page_previews": false,
            "can_change_info": false,
            "can_invite_users": false,
            "can_pin_messages": false,
            "can_manage_topics": false,
            "can_send_polls": false,
            "until_date": 1_700_000_000,
        }));
        assert_eq!(
            classify_member_kind(&member.kind),
            BotChatStatus::RestrictedOrOther
        );
    }

    #[test]
    fn a_left_member_is_restricted_or_other() {
        let member = member(json!({ "status": "left" }));
        assert_eq!(
            classify_member_kind(&member.kind),
            BotChatStatus::RestrictedOrOther
        );
    }

    #[test]
    fn a_banned_member_is_restricted_or_other() {
        let member = member(json!({ "status": "kicked", "until_date": 0 }));
        assert_eq!(
            classify_member_kind(&member.kind),
            BotChatStatus::RestrictedOrOther
        );
    }

    fn request(api: &ApiError) -> RequestError {
        RequestError::Api(api.clone())
    }

    #[test]
    fn the_rights_variants_are_permission_errors() {
        for api in [
            ApiError::NotEnoughRightsToPinMessage,
            ApiError::NotEnoughRightsToManagePins,
            ApiError::NotEnoughRightsToChangeChatPermissions,
            ApiError::NotEnoughRightsToRestrict,
            ApiError::NotEnoughRightsToPostMessages,
        ] {
            assert!(is_permission_error(&request(&api)), "{api:?}");
        }
    }

    #[test]
    fn the_access_variants_are_permission_errors() {
        for api in [
            ApiError::BotBlocked,
            ApiError::BotKicked,
            ApiError::BotKickedFromSupergroup,
            ApiError::BotKickedFromChannel,
            ApiError::ChatNotFound,
        ] {
            assert!(is_permission_error(&request(&api)), "{api:?}");
        }
    }

    #[test]
    fn a_rights_text_unknown_is_a_permission_error() {
        // Telegram serves many rights errors as plain text; teloxide does
        // not enumerate them (verified in errors.rs).
        let err = request(&ApiError::Unknown(
            "Bad Request: not enough rights to set message reaction".to_string(),
        ));
        assert!(is_permission_error(&err));
        let err = request(&ApiError::Unknown(
            "Bad Request: need administrator rights in the group chat".to_string(),
        ));
        assert!(is_permission_error(&err));
    }

    #[test]
    fn a_networkish_error_is_not_a_permission_error() {
        // RequestError::Network needs a reqwest::Error (not constructible
        // without network); Io is the other non-Api variant that is
        // constructible (verified in errors.rs).
        let err = RequestError::Io(Arc::new(io::Error::other("boom")));
        assert!(!is_permission_error(&err));
    }

    #[test]
    fn an_unrelated_unknown_text_is_not_a_permission_error() {
        let err = request(&ApiError::Unknown(
            "Bad Request: REACTION_INVALID".to_string(),
        ));
        assert!(!is_permission_error(&err));
        let err = request(&ApiError::MessageToReplyNotFound);
        assert!(!is_permission_error(&err));
    }
}
