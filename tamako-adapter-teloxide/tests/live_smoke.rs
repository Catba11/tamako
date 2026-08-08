//! Live smoke test against the real Telegram Bot API. Ignored by default;
//! it needs a real bot and a real group.
//!
//! Run it with:
//!
//! ```sh
//! TAMAKO_LIVE_TELEGRAM=1 TELOXIDE_TOKEN=<the bot token> \
//!   cargo test -p tamako-adapter-teloxide --test live_smoke -- --ignored --nocapture
//! ```
//!
//! BotFather checklist:
//!
//! - The bot must be a member of the test group (`/setjoingroups` allows it
//!   to be added).
//! - Privacy mode must be off (`/setprivacy` -> Disable) so the bot sees
//!   all group messages.
//! - The bot SHOULD be a group administrator. Reaction updates require it.
//!   A plain member also works; reaction collection is then off (specs.md
//!   Section 4.2). Set TAMAKO_LIVE_EXPECT_ADMIN=1 to assert administrator
//!   status explicitly.
//!
//! Send a message in the group while the test waits (60 s timeout).

use std::time::Duration;

use tamako_adapter_teloxide::{BotChatStatus, TeloxideAdapter};

#[tokio::test]
#[ignore = "live test: needs TAMAKO_LIVE_TELEGRAM=1 and TELOXIDE_TOKEN"]
async fn live_smoke() {
    if std::env::var("TAMAKO_LIVE_TELEGRAM").as_deref() != Ok("1") {
        eprintln!("skipping: set TAMAKO_LIVE_TELEGRAM=1 to run");
        return;
    }
    let Ok(token) = std::env::var("TELOXIDE_TOKEN") else {
        eprintln!("skipping: set TELOXIDE_TOKEN to run");
        return;
    };

    let mut adapter = TeloxideAdapter::new(&token)
        .await
        .expect("the adapter starts");
    assert!(!adapter.bot_identity().username.is_empty());

    let event = tokio::time::timeout(Duration::from_secs(60), adapter.next_group_event())
        .await
        .expect("an event within 60 s")
        .expect("no stream error")
        .expect("the stream is open");
    assert!(!event.chat_id.is_empty());
    eprintln!("received: {event:?}");

    // Capability detection smoke path. Both administrator and plain member
    // are supported deployment shapes (specs.md Section 4.2): the test
    // verifies that DETECTION works, not that a specific status holds.
    // TAMAKO_LIVE_EXPECT_ADMIN=1 tightens the check for reaction testing.
    let status = adapter.bot_chat_status(&event.chat_id).await;
    eprintln!("bot chat status: {status:?}");
    if std::env::var("TAMAKO_LIVE_EXPECT_ADMIN").as_deref() == Ok("1") {
        assert_eq!(
            status,
            BotChatStatus::Administrator,
            "TAMAKO_LIVE_EXPECT_ADMIN=1 but the bot is not an administrator; \
             reaction updates require admin status"
        );
    } else {
        assert!(
            matches!(status, BotChatStatus::Administrator | BotChatStatus::Member),
            "unexpected bot chat status: {status:?}"
        );
        match status {
            BotChatStatus::Administrator => {
                eprintln!("reaction collection is ON (administrator)")
            }
            BotChatStatus::Member => {
                eprintln!("reaction collection is OFF (plain member); this is supported")
            }
            other => unreachable!("covered by the assertion above: {other:?}"),
        }
    }
}
