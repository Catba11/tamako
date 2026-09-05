//! The soak-watch contract, captured end to end: exactly one `info`
//! line per wake (message string exactly `wake`) and exactly one per
//! completed digest (message string exactly `digest`), with the curated
//! structured fields. A scoped `tracing_subscriber::fmt` subscriber
//! writes into a shared buffer; the current-thread runtime polls the
//! actor task and its spawned wake/digest tasks on the same thread, so
//! their events land in the capture.
//!
//! This suite lives in its own test binary on purpose: `tracing`'s
//! callsite interest cache is process-global, and in a shared binary a
//! sibling test that fires the same callsites without a subscriber can
//! cache a NEVER interest mid-run, silently dropping this test's events.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tamako_agent::{ScriptedGate, ScriptedReplyGenerator};
use tamako_core::actor::{
    spawn_group_actor, CoreError, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::config::TriggerConfig;
use tamako_core::digest::{DigestOutcome, DigestPipeline};
use tamako_core::event::{InboundEvent, NormalizedMessage, OutboundAction};
use tamako_core::wake::{GateDecision, NoopRecall, WakeServices};
use tamako_memory::{MemoryBackend, MemoryBatch};
use tamako_store::Store;
use time::OffsetDateTime;
use tokio::sync::mpsc;

const CHAT_ID: &str = "-1001234567890";

/// The bound of every wait of this suite. The scripted doubles return
/// immediately, so a wake/digest round trip is milliseconds.
const TIMEOUT: Duration = Duration::from_secs(5);
/// The poll interval of every bounded wait.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A near-infinite wake interval: only the message count drives wakes.
const HUGE_INTERVAL: Duration = Duration::from_secs(u64::MAX / 4);

/// A `MakeWriter` that appends every written line to a shared buffer.
/// The scoped subscriber of the capture test writes through it.
#[derive(Clone, Default)]
struct LogCapture {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl LogCapture {
    /// The captured log output so far.
    fn contents(&self) -> String {
        let bytes = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        String::from_utf8(bytes).expect("the log output is UTF-8")
    }

    /// The captured `info` lines whose event message is exactly
    /// `message` on the actor target (the default fmt rendering puts
    /// the message right after the target).
    fn lines_for(&self, message: &str) -> Vec<String> {
        let prefix = format!("INFO tamako_core::actor: {message}");
        self.contents()
            .lines()
            .filter(|line| line.contains(&prefix))
            .map(str::to_string)
            .collect()
    }
}

impl std::io::Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A memory backend double. All calls succeed; nothing is recorded.
struct NoopMemory;

impl MemoryBackend for NoopMemory {
    async fn ensure_schema(&self, _chat_id: &str) -> tamako_memory::Result<()> {
        Ok(())
    }

    async fn upsert_batch(
        &self,
        _chat_id: &str,
        _batch: &MemoryBatch,
    ) -> tamako_memory::Result<()> {
        Ok(())
    }

    async fn checkpoint(&self, _chat_id: &str) -> tamako_memory::Result<()> {
        Ok(())
    }

    async fn alias_targets(
        &self,
        _chat_id: &str,
        _alias_node_id: &str,
    ) -> tamako_memory::Result<Vec<tamako_memory::AliasTarget>> {
        Ok(vec![])
    }

    async fn neighbors(
        &self,
        _chat_id: &str,
        _node_id: &str,
    ) -> tamako_memory::Result<Vec<tamako_memory::NeighborEdge>> {
        Ok(vec![])
    }

    async fn close(&self, _chat_id: &str) -> tamako_memory::Result<()> {
        Ok(())
    }
}

struct Fixture {
    // The TempDir must outlive the store.
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    memory: Arc<NoopMemory>,
}

fn make_fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("a temporary data root");
    // Open the group store BEFORE the actor spawns: a concurrent
    // `open_group` of the same store.db races the WAL pragma of the
    // actor startup (the wake_replay.rs note).
    let store = Arc::new(Store::new(dir.path().to_path_buf()));
    store.open_group(CHAT_ID).expect("open_group succeeds");
    Fixture {
        _dir: dir,
        store,
        memory: Arc::new(NoopMemory),
    }
}

/// A plain message with a fixed sender and timestamp.
fn message(id: &str, at: OffsetDateTime) -> NormalizedMessage {
    NormalizedMessage {
        platform_msg_id: id.to_string(),
        timestamp: at,
        sender_id: "u1".to_string(),
        sender_display_name: "Alice".to_string(),
        username: None,
        text: format!("text of {id}"),
        reply_to_platform_msg_id: None,
        mentions_bot: false,
        is_reply_to_bot: false,
    }
}

/// The outcome templates of the scripted digest double, in call order.
enum DigestTemplate {
    Extracted { nodes: usize, edges: usize },
    Skeleton,
}

/// A scripted digest pipeline (the `ScriptedDigest` pattern of the
/// actor tests): store-backed, pops one outcome template per run. An
/// empty tail returns `Ok(None)`.
struct ScriptedDigest {
    store: Arc<Store>,
    outcomes: Mutex<VecDeque<DigestTemplate>>,
}

impl DigestPipeline for ScriptedDigest {
    fn run_digest<'a>(
        &'a self,
        chat_id: &'a str,
        last_digest_boundary_msg_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DigestOutcome>, CoreError>> + Send + 'a>> {
        Box::pin(async move {
            let store = Arc::clone(&self.store);
            let chat_id = chat_id.to_string();
            let rows = tokio::task::spawn_blocking(move || {
                store.list_messages_after(&chat_id, last_digest_boundary_msg_id)
            })
            .await
            .map_err(|error| CoreError::Join(error.to_string()))?
            .map_err(CoreError::Store)?;
            let Some(last) = rows.last() else {
                return Ok(None);
            };
            let template = self
                .outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front();
            let batch_id = format!("batch-{}-{}", last_digest_boundary_msg_id, last.id);
            let outcome = match template {
                Some(DigestTemplate::Extracted { nodes, edges }) => DigestOutcome::Extracted {
                    batch_id,
                    new_boundary: last.id,
                    node_count: nodes,
                    edge_count: edges,
                },
                Some(DigestTemplate::Skeleton) | None => DigestOutcome::Skeleton {
                    batch_id,
                    new_boundary: last.id,
                },
            };
            Ok(Some(outcome))
        })
    }
}

/// Spawns an actor with the scripted wake services; returns the
/// outbound receiver.
fn spawn_with_wake(
    fixture: &Fixture,
    config: TriggerConfig,
    started_at: OffsetDateTime,
    gate: Arc<ScriptedGate>,
    reply: Arc<ScriptedReplyGenerator>,
) -> (GroupActorHandle, mpsc::Receiver<OutboundAction>) {
    let (outbound_tx, outbound_rx) = mpsc::channel(64);
    let handle = spawn_group_actor(GroupActorParams {
        pet_tag: "tamako".to_string(),
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config,
        started_at,
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: "test preamble".to_string(),
        digest: None,
        post_digest_hook: None,
        wake: Some(WakeServices {
            recall: Arc::new(NoopRecall),
            gate,
            reply,
        }),
        warmup: None,
        summary_provider: None,
        outbound: Some(outbound_tx),
        bot_name: None,
    });
    (handle, outbound_rx)
}

/// Spawns an actor with the scripted digest pipeline.
fn spawn_with_digest(
    fixture: &Fixture,
    config: TriggerConfig,
    started_at: OffsetDateTime,
    digest: Arc<ScriptedDigest>,
) -> GroupActorHandle {
    spawn_group_actor(GroupActorParams {
        pet_tag: "tamako".to_string(),
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config,
        started_at,
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: "test preamble".to_string(),
        digest: Some(digest),
        post_digest_hook: None,
        wake: None,
        warmup: None,
        summary_provider: None,
        outbound: None,
        bot_name: None,
    })
}

/// Polls the capture until `message` has `count` lines or the deadline
/// passes (the `wait_for_boundary` pattern: bounded).
async fn wait_for_lines(capture: &LogCapture, message: &str, count: usize) {
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        if capture.lines_for(message).len() >= count {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {TIMEOUT:?} waiting for {count} `{message}` lines; captured:\n{}",
            capture.contents()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls `snapshot()` until the digest boundary reaches `min` (the
/// `wait_for_boundary` pattern of digest_replay.rs: the boundary proves
/// the whole completion handler ran).
async fn wait_for_boundary(handle: &GroupActorHandle, min: i64) {
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let session = handle.snapshot().await.expect("the snapshot succeeds");
        if session.last_digest_boundary_msg_id >= min {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {TIMEOUT:?} waiting for the digest boundary to reach {min}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The soak-watch contract, captured. Wake 1 participates (the reply
/// goes out with the gate's reason); wake 2 is a gate-no (the silent
/// outcome without a reason field). Digest 1 is written with the
/// node/edge counts; digest 2 is a skeleton.
#[test]
fn one_curated_info_line_per_wake_and_per_digest() {
    let capture = LogCapture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_ansi(false)
        .without_time()
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the current-thread runtime builds");
        runtime.block_on(async {
            let t0 = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid timestamp");

            // --- Two wakes ---
            let fixture = make_fixture();
            let wake_config = TriggerConfig {
                wake_msg_count: 3,
                wake_floor: Duration::ZERO,
                wake_interval: HUGE_INTERVAL,
                ..TriggerConfig::default()
            };
            let gate = Arc::new(ScriptedGate::with_decisions(vec![
                GateDecision {
                    participate: true,
                    target_row_id: Some(3),
                    reason: Some("a direct question".to_string()),
                },
                GateDecision {
                    participate: false,
                    target_row_id: None,
                    reason: None,
                },
            ]));
            let reply = Arc::new(ScriptedReplyGenerator::with_replies(vec!["r1".to_string()]));
            let (handle, mut outbound) = spawn_with_wake(&fixture, wake_config, t0, gate, reply);
            // Wake 1: the count threshold fires at w3; the gate
            // participates. The send proves the wake completed before
            // the next messages go in (an unpaced feed would produce a
            // legitimate in-flight skip instead of wake 2).
            for (index, id) in ["w1", "w2", "w3"].iter().enumerate() {
                handle
                    .send_event(InboundEvent::Message(message(
                        id,
                        t0 + time::Duration::seconds(index as i64 + 1),
                    )))
                    .await
                    .expect("the actor inbox is open");
            }
            tokio::time::timeout(TIMEOUT, outbound.recv())
                .await
                .expect("the reply action arrives")
                .expect("the outbound channel stays open");
            // Wake 2: the count fires again at w6; the gate says no.
            for (index, id) in ["w4", "w5", "w6"].iter().enumerate() {
                handle
                    .send_event(InboundEvent::Message(message(
                        id,
                        t0 + time::Duration::seconds(index as i64 + 4),
                    )))
                    .await
                    .expect("the actor inbox is open");
            }
            wait_for_lines(&capture, "wake", 2).await;
            handle.shutdown().await.expect("the actor reports no error");

            // --- Two digests ---
            let fixture = make_fixture();
            let digest_config = TriggerConfig {
                digest_max_messages: 2,
                ..TriggerConfig::default()
            };
            let digest = Arc::new(ScriptedDigest {
                store: Arc::clone(&fixture.store),
                outcomes: Mutex::new(VecDeque::from([
                    DigestTemplate::Extracted { nodes: 3, edges: 2 },
                    DigestTemplate::Skeleton,
                ])),
            });
            let handle = spawn_with_digest(&fixture, digest_config, t0, digest);
            // Digest 1 (boundary 0 -> 2): extracted.
            for (index, id) in ["d1", "d2"].iter().enumerate() {
                handle
                    .send_event(InboundEvent::Message(message(
                        id,
                        t0 + time::Duration::seconds(index as i64 + 1),
                    )))
                    .await
                    .expect("the actor inbox is open");
            }
            wait_for_boundary(&handle, 2).await;
            // Digest 2 (boundary 2 -> 4): skeleton.
            for (index, id) in ["d3", "d4"].iter().enumerate() {
                handle
                    .send_event(InboundEvent::Message(message(
                        id,
                        t0 + time::Duration::seconds(index as i64 + 3),
                    )))
                    .await
                    .expect("the actor inbox is open");
            }
            wait_for_boundary(&handle, 4).await;
            handle.shutdown().await.expect("the actor reports no error");
        });
    });

    // --- The wake lines ---
    let wake_lines = capture.lines_for("wake");
    assert_eq!(
        wake_lines.len(),
        2,
        "exactly one wake line per wake; captured:\n{}",
        capture.contents()
    );
    // Wake 1: gate participate, the reply went out with the gate's
    // reason and the target's platform id.
    let first = &wake_lines[0];
    for field in [
        "chat_id=-1001234567890",
        "trigger=\"message_count\"",
        "injections=0",
        "gate=\"participate\"",
        "reason=\"a direct question\"",
        "action=\"reply_sent\"",
        "reply_to=\"w3\"",
    ] {
        assert!(first.contains(field), "missing `{field}` in: {first}");
    }
    // Wake 2: gate silent, nothing sent; a scripted decision carries no
    // reason field and a silent wake has no reply target.
    let second = &wake_lines[1];
    for field in [
        "chat_id=-1001234567890",
        "trigger=\"message_count\"",
        "injections=0",
        "gate=\"silent\"",
        "action=\"nothing\"",
    ] {
        assert!(second.contains(field), "missing `{field}` in: {second}");
    }
    assert!(
        !second.contains("reason="),
        "a scripted gate-no carries no reason field: {second}"
    );
    assert!(
        !second.contains("reply_to="),
        "a gate-no wake has no reply target: {second}"
    );

    // --- The digest lines ---
    let digest_lines = capture.lines_for("digest");
    assert_eq!(
        digest_lines.len(),
        2,
        "exactly one digest line per digest; captured:\n{}",
        capture.contents()
    );
    // Digest 1: written with the range and the node/edge counts.
    let first = &digest_lines[0];
    for field in [
        "chat_id=-1001234567890",
        "batch_id=batch-0-2",
        "range=(0,2]",
        "outcome=\"written\"",
        "nodes=3",
        "edges=2",
    ] {
        assert!(first.contains(field), "missing `{field}` in: {first}");
    }
    // Digest 2: the skeleton outcome carries no node/edge counts.
    let second = &digest_lines[1];
    for field in ["batch_id=batch-2-4", "range=(2,4]", "outcome=\"skeleton\""] {
        assert!(second.contains(field), "missing `{field}` in: {second}");
    }
    assert!(
        !second.contains("nodes="),
        "a skeleton line carries no node count: {second}"
    );
    // The superseded lines are gone from the info level.
    let contents = capture.contents();
    assert!(
        !contents.contains("digest completed"),
        "the old digest line is replaced by the curated one:\n{contents}"
    );
    assert!(
        !contents.contains("(stub)"),
        "the stub lines are gone from the info level:\n{contents}"
    );
}
