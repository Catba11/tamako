//! End-to-end tests of the warmup trigger (specs.md Sections
//! 8.4/8.5/9.7, decision 78) against the REAL SQLite store and the REAL
//! LadybugDB backend. The topic graph is built through the REAL digest
//! path (a scripted extractor over actor-driven raw-log rows — the
//! digest_replay.rs harness), so the weighted topic pick of Section 9.7
//! step 2 reads a graph it did not script. Warmup generation runs
//! through tamako-agent's scripted double.
//!
//! The deterministic seam: the actor evaluates the warmup trigger on
//! its timer tick ONLY, and the automatic ticker's cadence is five
//! minutes at the default `wake_interval` (trigger.rs
//! `timer_cadence`) — these tests send `ActorCommand::Tick` by hand and
//! finish long before an automatic tick could land. TZ-proof: the
//! active-hours window is the full day ("00:00-23:59"), so no assertion
//! depends on the host offset.

use std::sync::Arc;
use std::time::Duration;

use tamako_agent::{
    AgentDigestPipeline, ExtractedEdge, ExtractedNode, ExtractedNodeType, KnowledgeGraph,
    PipelineConfig, ScriptedExtractor, ScriptedWarmupGenerator,
};
use tamako_core::actor::{
    spawn_group_actor, ActorCommand, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::config::{ActiveHours, TriggerConfig};
use tamako_core::digest::DigestPipeline;
use tamako_core::event::{InboundEvent, NormalizedMessage};
use tamako_core::warmup::WarmupServices;
use tamako_memory::LbugBackend;
use tamako_store::{Direction, MessageRow, Store};
use time::OffsetDateTime;

const CHAT_ID: &str = "-1001234567890";

/// The bounded waits of this suite (the digest_replay.rs pattern): the
/// scripted doubles return immediately, so a warmup round trip is
/// milliseconds; five seconds is generous enough for a loaded CI
/// machine.
const WARMUP_TIMEOUT: Duration = Duration::from_secs(5);
/// The poll interval of the wait helpers.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Fixed base time for the digest-phase messages (the digest_replay.rs
/// pattern). The warmup send path stamps its own row with real
/// wall-clock time; nothing here asserts a host-local date or offset.
fn t0() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
}

/// The trigger config of this suite: the digest fires every 3 tail
/// rows; the warmup trigger is on with a TZ-proof full-day active
/// window and no silence floor (the digest-phase rows are t0-based).
/// `warmup_reaction_window` keeps its 30-minute default; the
/// engagement phase derives the expiry from the actor's snapshot.
fn suite_config() -> TriggerConfig {
    TriggerConfig {
        digest_max_messages: 3,
        warmup: true,
        warmup_quota: 1,
        warmup_active_hours: ActiveHours::parse("00:00-23:59").expect("the test window parses"),
        warmup_silence: Duration::ZERO,
        ..TriggerConfig::default()
    }
}

/// A digest-phase message. The texts of this suite NEVER name the
/// scripted Concept: the tail exclusion of Section 9.7 step 2 skips a
/// topic whose normalized name appears in the 50 newest raw-log rows.
fn message(id: &str, seconds: i64, sender_id: &str, name: &str, text: &str) -> NormalizedMessage {
    NormalizedMessage {
        platform_msg_id: id.to_string(),
        timestamp: t0() + time::Duration::seconds(seconds),
        sender_id: sender_id.to_string(),
        sender_display_name: name.to_string(),
        username: None,
        text: text.to_string(),
        reply_to_platform_msg_id: None,
        mentions_bot: false,
        is_reply_to_bot: false,
    }
}

/// A human message with an explicit timestamp (the engagement watch is
/// wall-clock: the warmup's send path stamps it with
/// `OffsetDateTime::now_utc()`, so the reply lands relative to the
/// snapshot's `warmup_watch_sent_at`).
fn message_at(id: &str, at: OffsetDateTime, text: &str) -> NormalizedMessage {
    NormalizedMessage {
        platform_msg_id: id.to_string(),
        timestamp: at,
        sender_id: "u2".to_string(),
        sender_display_name: "Bob".to_string(),
        username: None,
        text: text.to_string(),
        reply_to_platform_msg_id: None,
        mentions_bot: false,
        is_reply_to_bot: false,
    }
}

/// The scripted extraction of the digest phase (the digest_replay.rs
/// graph): Alice (bound to her sender id by the mention map, Section
/// 7.4 step 1) likes GRPO — the ONLY Concept of the graph, so the
/// weighted pick of Section 9.7 step 2 is deterministic in outcome.
fn alice_grpo_graph() -> KnowledgeGraph {
    KnowledgeGraph {
        nodes: vec![
            ExtractedNode {
                name: "Alice".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "Alice talked about training methods.".to_string(),
            },
            ExtractedNode {
                name: "GRPO".to_string(),
                node_type: ExtractedNodeType::Concept,
                description: "A training method Alice finds unstable.".to_string(),
            },
        ],
        edges: vec![ExtractedEdge {
            source: "Alice".to_string(),
            target: "GRPO".to_string(),
            relationship_name: "likes".to_string(),
            description: "Alice likes GRPO despite its instability.".to_string(),
        }],
    }
}

struct Fixture {
    // The TempDir must outlive the store and the backend.
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    memory: Arc<LbugBackend>,
}

fn make_fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("a temporary data root");
    let store = Arc::new(Store::new(dir.path().to_path_buf()));
    let memory = Arc::new(LbugBackend::new(dir.path().to_path_buf()));
    Fixture {
        _dir: dir,
        store,
        memory,
    }
}

/// Spawns the real actor over the fixture. `digest` and `warmup` are
/// the seams under test; the wake stays unwired (the warmup trigger is
/// a sibling of the wake, not a mode of it).
fn spawn_actor(
    fixture: &Fixture,
    config: TriggerConfig,
    digest: Option<Arc<dyn DigestPipeline>>,
    warmup: Option<WarmupServices>,
) -> GroupActorHandle {
    spawn_group_actor(GroupActorParams {
        pet_tag: "tamako".to_string(),
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config,
        started_at: t0(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: "test preamble".to_string(),
        digest,
        post_digest_hook: None,
        wake: None,
        warmup,
        summary_provider: None,
        outbound: None,
        bot_name: None,
    })
}

/// Writes state-table rows directly BEFORE the actor spawns (the
/// actor-test `seed_state` pattern): the actor decodes the persisted
/// state at startup, so the test SEEDS the warmup schedule instead of
/// reaching into a live actor.
async fn seed_state(fixture: &Fixture, pairs: &[(&str, String)]) {
    let store = Arc::clone(&fixture.store);
    let pairs: Vec<(String, String)> = pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect();
    tokio::task::spawn_blocking(move || {
        store.open_group(CHAT_ID)?;
        store.set_state_many(CHAT_ID, &pairs)
    })
    .await
    .expect("the blocking task joins")
    .expect("the seed succeeds");
}

/// The RFC 3339 encoding of a seeded instant (the session encoding of
/// `warmup_next_at`). Whole seconds: the seeded value round-trips
/// through the store byte-identically.
fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&time::format_description::well_known::Rfc3339)
        .expect("a test timestamp formats")
}

/// Reads one state-table key through the real store.
async fn state(fixture: &Fixture, key: &str) -> Option<String> {
    let store = Arc::clone(&fixture.store);
    let key = key.to_string();
    tokio::task::spawn_blocking(move || store.get_state(CHAT_ID, &key))
        .await
        .expect("the blocking task joins")
        .expect("get_state succeeds")
}

/// Reads the whole raw log through the real store.
async fn list_messages(fixture: &Fixture) -> Vec<MessageRow> {
    let store = Arc::clone(&fixture.store);
    tokio::task::spawn_blocking(move || store.list_messages(CHAT_ID))
        .await
        .expect("the blocking task joins")
        .expect("list_messages succeeds")
}

/// Polls until the state-table counter reads `want` (bounded; the
/// counter bumps inside the actor's completion handler, so reaching
/// `want` proves the handler ran).
async fn wait_for_counter(fixture: &Fixture, key: &str, want: u64) {
    let deadline = std::time::Instant::now() + WARMUP_TIMEOUT;
    loop {
        let value = state(fixture, key)
            .await
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        if value == want {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WARMUP_TIMEOUT:?} waiting for {key} to reach {want} \
             (current: {value})"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls `snapshot()` until the digest boundary reaches `min` (the
/// digest_replay.rs pattern: the snapshot is a FIFO barrier, so a
/// boundary in the snapshot proves the whole completion handler ran).
async fn wait_for_boundary(handle: &GroupActorHandle, min: i64) -> i64 {
    let deadline = std::time::Instant::now() + WARMUP_TIMEOUT;
    loop {
        let session = handle.snapshot().await.expect("the snapshot succeeds");
        if session.last_digest_boundary_msg_id >= min {
            return session.last_digest_boundary_msg_id;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WARMUP_TIMEOUT:?} waiting for the digest boundary to reach {min} \
             (current boundary: {})",
            session.last_digest_boundary_msg_id
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The full decision-78 loop end to end: the REAL digest path builds
/// the topic graph, the REAL backend feeds the Section 9.7 step-2
/// topic pick, the scripted generator answers, the send path persists
/// the outbound row (Rule B1) and bumps `warmups_total`, and a human
/// reply inside the window bumps `warmup_engaged_total` (Section 8.5)
/// — every counter through the real store (Section 12).
#[tokio::test]
async fn the_warmup_fires_end_to_end_through_the_real_graph() {
    let fixture = make_fixture();
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![alice_grpo_graph()]));
    let digest = Arc::new(AgentDigestPipeline::new(
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.memory),
        extractor,
        PipelineConfig::default(),
    ));
    let generator = Arc::new(ScriptedWarmupGenerator::with_replies(vec![
        "anyone else fighting unstable training runs lately?".to_string(),
    ]));
    // A DUE slot, seeded before the actor spawns (RFC 3339, the
    // actor-test seeding). Whole seconds keep the persisted string an
    // exact round-trip; real wall clock keeps the rescheduled slot a
    // day out, so no later tick of this test refires.
    let due = OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp())
        .expect("a valid unix timestamp");
    seed_state(&fixture, &[("warmup_next_at", rfc3339(due))]).await;
    let handle = spawn_actor(
        &fixture,
        suite_config(),
        Some(digest),
        Some(WarmupServices {
            generator: generator.clone(),
        }),
    );

    // Phase 1: the real digest path builds the graph. The batch texts
    // never name "GRPO" (the tail exclusion would skip the topic).
    for msg in [
        message("m1", 1, "u1", "Alice", "morning all"),
        message("m2", 2, "u1", "Alice", "my training run looks unstable"),
        message("m3", 3, "u2", "Bob", "really? ours converged fine"),
    ] {
        handle
            .send_event(InboundEvent::Message(msg))
            .await
            .expect("the actor inbox is open");
    }
    handle.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(wait_for_boundary(&handle, 3).await, 3);

    // Phase 2: the due slot fires on the tick. `warmups_total` bumps
    // inside the completion handler, so the counter is the barrier.
    handle
        .send(ActorCommand::Tick(due))
        .await
        .expect("send succeeds");
    wait_for_counter(&fixture, "warmups_total", 1).await;
    let session = handle.snapshot().await.expect("the snapshot succeeds");

    // The real `sample_interest_topics` fed the pick: the request names
    // the graph's only Concept.
    let requests = generator.requests();
    assert_eq!(requests.len(), 1, "exactly one warmup call");
    assert_eq!(requests[0].topic, "GRPO");
    // Rule B1 through the real store: the outbound row persisted FIRST;
    // Section 9.7 step 4: proactive speech never quotes a target.
    let outbound: Vec<MessageRow> = list_messages(&fixture)
        .await
        .into_iter()
        .filter(|row| row.direction == Direction::Outbound)
        .collect();
    assert_eq!(outbound.len(), 1, "exactly one outbound row");
    assert_eq!(
        outbound[0].text,
        "anyone else fighting unstable training runs lately?"
    );
    assert_eq!(outbound[0].reply_to_platform_msg_id, None);
    assert_eq!(state(&fixture, "warmups_total").await.as_deref(), Some("1"));

    // Phase 3: the engagement loop (Section 8.5). The watch timestamps
    // are REAL (the send path stamps `OffsetDateTime::now_utc()`); the
    // reply lands inside the window and the tick at expiry resolves the
    // watch.
    let sent_at = session
        .warmup_watch_sent_at
        .expect("the watch has a send time");
    let expires = session
        .warmup_watch_expires_at
        .expect("the watch has an expiry");
    assert!(session.warmup_watch_pending);
    handle
        .send_event(InboundEvent::Message(message_at(
            "m4",
            sent_at + time::Duration::minutes(5),
            "yes, constantly",
        )))
        .await
        .expect("the actor inbox is open");
    handle
        .send(ActorCommand::Tick(expires))
        .await
        .expect("send succeeds");
    wait_for_counter(&fixture, "warmup_engaged_total", 1).await;
    let session = handle.snapshot().await.expect("the snapshot succeeds");
    assert!(
        !session.warmup_watch_pending,
        "the watch resolved at expiry"
    );
    assert_eq!(
        session.warmup_backoff_factor, 0,
        "engagement resets the Section 8.5 soft backoff"
    );
    handle.shutdown().await.expect("the actor reports no error");
}

/// Decision 78 (the replay discipline of decisions 73/76): replay wires
/// `warmup: None` and must stay deterministic and network-free.
/// Warmup scheduling is wall-clock host-local and would draw slots
/// against replay wall time, not fixture time — so a replayed tick
/// touches NOTHING without the services: there is no generator to call
/// (none is wired anywhere in this test), no outbound row lands, and
/// the seeded `warmup_next_at` survives byte-identically. The
/// actor-side `unwired_warmup_services_are_fully_inert` pins the same
/// inert path; this test pins it at integration level over the real
/// store and backend.
#[tokio::test]
async fn replay_discipline_warmup_services_none_stays_inert() {
    let fixture = make_fixture();
    let due = OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp())
        .expect("a valid unix timestamp");
    let seeded = rfc3339(due);
    seed_state(&fixture, &[("warmup_next_at", seeded.clone())]).await;
    let handle = spawn_actor(&fixture, suite_config(), None, None);

    handle
        .send(ActorCommand::Tick(due))
        .await
        .expect("send succeeds");
    // The snapshot is the FIFO barrier: the tick is fully processed.
    let session = handle.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(
        session.warmup_next_at,
        Some(due),
        "the slot was not consumed"
    );
    assert!(list_messages(&fixture).await.is_empty());
    assert_eq!(
        state(&fixture, "warmup_next_at").await.as_deref(),
        Some(seeded.as_str()),
        "the persisted slot is untouched"
    );
    assert_eq!(state(&fixture, "warmups_total").await, None);
    handle.shutdown().await.expect("the actor reports no error");
}
