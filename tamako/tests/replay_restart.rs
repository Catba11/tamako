//! The Phase 0 exit criterion of dev-roadmap.md Section 2: the scripted
//! mock adapter replays the shipped chat log; the actor persists the raw
//! log and the session state; a restart rebuilds the identical state.
//!
//! The test drives the full Phase 0 stack: the mock adapter, the actor,
//! the real SQLite store, and the real LadybugDB backend.

use std::path::Path;
use std::sync::Arc;

use tamako_adapter_mock::fixture::load_fixture;
use tamako_adapter_mock::{FixtureEvent, MockAdapter, ReplayFixture};
use tamako_core::actor::{
    spawn_group_actor, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::TriggerConfig;
use tamako_memory::LbugBackend;
use tamako_store::Store;
use time::OffsetDateTime;

/// The shipped demo fixture.
const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../tamako-adapter-mock/fixtures/replay_chat.json"
);
/// Part 1 replays the events before this index; part 2 the rest.
const SPLIT_AT: usize = 8;

/// One fixed start time for both runs. It sits AFTER every fixture
/// timestamp, so the wake floor blocks every timer fire and the wake
/// counter counts the messages exactly.
fn started_at() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("a valid unix timestamp")
}

/// Counts the events that increment the wake counter (specs.md 8.1).
fn message_count(events: &[FixtureEvent]) -> u32 {
    events
        .iter()
        .filter(|event| matches!(event, FixtureEvent::Message(_)))
        .count() as u32
}

/// Counts the events that become rows of the raw log: messages and edits.
fn log_row_count(events: &[FixtureEvent]) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                event,
                FixtureEvent::Message(_) | FixtureEvent::EditedMessage(_)
            )
        })
        .count()
}

/// Replays the events through a MockAdapter into the actor. The adapter
/// path stays in the loop: the test exercises the normalized contract of
/// Rule A1/A5 end to end.
async fn replay(handle: &GroupActorHandle, chat_id: &str, events: Vec<FixtureEvent>) {
    let mut adapter = MockAdapter::from_fixture(ReplayFixture {
        chat_id: chat_id.to_string(),
        events,
    });
    while let Some(event) = adapter
        .next_event()
        .await
        .expect("the mock adapter never fails")
    {
        handle
            .send_event(event)
            .await
            .expect("the actor inbox is open");
    }
}

/// Spawns one actor on the given storage handles.
fn spawn_on(store: &Arc<Store>, memory: &Arc<LbugBackend>, chat_id: &str) -> GroupActorHandle {
    spawn_group_actor(GroupActorParams {
        chat_id: chat_id.to_string(),
        store: Arc::clone(store),
        memory: Arc::clone(memory),
        config: TriggerConfig::default(),
        started_at: started_at(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: "test preamble".to_string(),
        // Phase 0 replay: no digest pipeline (specs.md Section 8.2 stub).
        digest: None,
        post_digest_hook: None,
    })
}

/// Reads the raw log through a blocking call, like the actor does
/// (AGENT.md Section 6.2).
async fn raw_log_len(store: &Arc<Store>, chat_id: &str) -> usize {
    let store = Arc::clone(store);
    let chat_id = chat_id.to_string();
    tokio::task::spawn_blocking(move || store.list_messages(&chat_id))
        .await
        .expect("the blocking task joins")
        .expect("list_messages succeeds")
        .len()
}

#[tokio::test(flavor = "current_thread")]
async fn replay_restart_rebuilds_identical_state() {
    let fixture = load_fixture(Path::new(FIXTURE_PATH)).expect("the shipped fixture loads");
    assert!(fixture.events.len() > SPLIT_AT);
    let chat_id = fixture.chat_id.clone();
    let part1: Vec<_> = fixture.events[..SPLIT_AT].to_vec();
    let part2: Vec<_> = fixture.events[SPLIT_AT..].to_vec();
    // Part 1 holds the mentions_bot message (index 4) and the
    // is_reply_to_bot message (index 7). Their intake must not fail: the
    // forced-wake path is a stub in Phase 0. The row-count assertions
    // below prove both landed.
    assert!(part1.iter().any(|event| matches!(
        event,
        FixtureEvent::Message(message) if message.mentions_bot
    )));
    assert!(part1.iter().any(|event| matches!(
        event,
        FixtureEvent::Message(message) if message.is_reply_to_bot
    )));

    let dir = tempfile::tempdir().expect("a temporary data root");
    let data_root = dir.path().to_path_buf();

    // --- Part 1: the first run on a fresh data root. ---
    let store = Arc::new(Store::new(data_root.clone()));
    let memory = Arc::new(LbugBackend::new(data_root.clone()));
    let handle = spawn_on(&store, &memory, &chat_id);
    replay(&handle, &chat_id, part1.clone()).await;

    // The snapshot is a FIFO barrier: part 1 is fully processed.
    let s1 = handle.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(raw_log_len(&store, &chat_id).await, log_row_count(&part1));
    assert_eq!(s1.wake.msgs_since_wake, message_count(&part1));
    handle.shutdown().await.expect("the actor reports no error");

    // Rule P5: the per-group files exist after the first run.
    let group_dir = data_root.join(&chat_id);
    assert!(
        group_dir.join("store.db").exists(),
        "store.db must exist at {}",
        group_dir.display()
    );
    assert!(
        group_dir.join("memory.lbug").exists(),
        "memory.lbug must exist at {}",
        group_dir.display()
    );

    // A real restart drops every handle and opens the files again.
    drop(store);
    drop(memory);

    // --- Restart: a new actor on the same data root, same started_at. ---
    let store = Arc::new(Store::new(data_root.clone()));
    let memory = Arc::new(LbugBackend::new(data_root.clone()));
    let restarted = spawn_on(&store, &memory, &chat_id);
    let s2 = restarted.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(s1, s2, "the restart must rebuild the identical state");

    // --- Part 2: the rest of the recording. ---
    replay(&restarted, &chat_id, part2.clone()).await;
    let s3 = restarted.snapshot().await.expect("the snapshot succeeds");
    restarted
        .shutdown()
        .await
        .expect("the actor reports no error");

    assert_eq!(
        raw_log_len(&store, &chat_id).await,
        log_row_count(&fixture.events)
    );
    assert_eq!(s3.wake.msgs_since_wake, message_count(&fixture.events));
    assert!(!s3.muted);
}
