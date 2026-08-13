//! The per-group actor. Refer to specs.md Section 6.
//!
//! Message intake appends to the raw log and updates the counters. The
//! digest trigger (specs.md Section 8.2) is wired in Phase 1 (M1): when
//! the trigger fires, the actor launches the digest pipeline as a
//! spawned task and the result returns through the FIFO inbox.
//!
//! The wake procedure (specs.md Section 9, steps 1-5) is live since
//! Phase 1 M4. A wake failure is logged and skipped, never retried
//! inline and never fatal. The counters `wakes_total` and
//! `participations_total` (Section 12) are best effort. The steps:
//! 1. The monologue lock (Section 8.5): a muted, unforced wake resets
//!    and returns. `wake_last_row_id` stays put, so messages of the
//!    muted period remain "new" for the next real wake.
//! 2. Recall (M5, Sections 9.1-9.5): the wake calls the
//!    `RecallProvider` before the gate. The rendered injections enter
//!    the gate input (Section 9.6) and the reply-model snapshot (the
//!    injection is part of the context from step 2 on, so the reply
//!    model of step 4 sees it). The completion handler records the
//!    Section 9.3 dedup rows and appends the injections to the context
//!    (Rule C2) BEFORE the participation outcome is applied: a gate-no
//!    wake still injects (the memory was genuinely remembered; a silent
//!    pet can still remember). `injection_wakes_total` (Section 12)
//!    counts the wakes with at least one injection; it is best effort
//!    like the other counters. `NoopRecall` keeps the no-injection
//!    behavior when no LLM key is configured.
//! 3. The participation decision (Section 9.6) over the new messages of
//!    this wake. Forced wakes (mention/reply, Section 8.1) bypass the
//!    gate. Documented decision: the resets of spec steps 1 and 5
//!    collapse into ONE reset at wake START, so messages that arrive
//!    during a running wake count toward the next wake.
//! 4. On participate, the reply model generates over a snapshot of the
//!    live context. Before the send, the recency re-check (Section 6.2)
//!    DISCARDS a stale reply (documented: discard, not regenerate; the
//!    next wake is the natural retry). The send path persists the
//!    outbound raw-log row FIRST (Rules B1/P1), then sends through the
//!    outbound channel, appends the bot speech to the context, and
//!    records the bot message for the monologue lock.
//! 5. Done at start (refer to step 3).
//!
//! The timer driver (M4, known gap 2): a tokio interval inside the
//! actor task evaluates the triggers on cadence ticks (`timer_cadence`;
//! `MissedTickBehavior::Delay` — a delayed tick loses at most cadence
//! time, while Burst could storm the FIFO evaluation). This closes the
//! M1-known silent-group digest-timeout gap: a group with no traffic
//! still gets its Section 8.2 timeout fallback evaluated on cadence
//! ticks. Shutdown is structural: the ticker lives and dies inside the
//! actor task.
//!
//! Reaction intake (Phase 1, M3) is passive collection: the actor
//! persists one reaction row per event (specs.md Section 5.2) and
//! touches nothing else — no context item, no counter, no session
//! mutation.
//!
//! The actor owns the live context of specs.md Section 7: a
//! materialized view of the raw log (Rule P1). Intake appends items
//! (Rule C1), a completed digest removes the previous chunk with the
//! one-chunk lag (Rule C3), and startup rebuilds the context from the
//! persisted rows (specs.md Section 6.1, rule 4).
//!
//! User-reply targets resolve through `Store::find_reply_target` on
//! every path — intake, the wake gate input, and the startup rebuild —
//! over the append-only raw log, never an in-context index, so intake
//! and rebuild renders are bit-identical (a target can be pruned from
//! the live context while the replying item stays). Rules A3/B1:
//! replies to the bot render `reply="bot"` and never resolve — the
//! synthetic `bot-out:{nanos}` outbound ids cannot name a stored row.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::SeedableRng;
use tamako_memory::{MemoryBackend, MemoryError};
use tamako_store::{
    Direction, EventType, InsertOutcome, MessageRow, NewMessage, NewReaction, ReplyTargetRow,
    Store, StoreError,
};
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info};

use crate::config::TriggerConfig;
use crate::context::{
    render_human_content, ContextItem, ContextMessage, ContextRole, LiveContext, RangeTag,
    ReplyRender,
};
use crate::digest::{DigestOutcome, DigestPipeline, PostDigestHook};
use crate::event::{InboundEvent, NormalizedMessage, OutboundAction, ReactionEvent};
use crate::session::{round_to_millis, SessionState};
use crate::summary::{SummaryError, SummaryProvider};
use crate::trigger::{digest_should_fire, tail_stats, timer_cadence, WakeScheduler};
use crate::wake::{
    filter_reply_parrot_lines, GateDecision, GateInput, GateMessage, ParticipationGate,
    PlannedInjection, RecallProvider, ReplyGenerator, ReplyRequest, WakeServices,
};

/// Errors of tamako-core.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("memory error: {0}")]
    Memory(#[from] MemoryError),
    #[error("actor task join error: {0}")]
    Join(String),
    #[error("actor inbox closed")]
    InboxClosed,
    /// A failure of the digest pipeline (specs.md Section 10). A failed
    /// batch never blocks later batches (Section 10.3).
    #[error("digest error: {0}")]
    Digest(String),
    /// A failure of the wake procedure (specs.md Section 9). Log and skip
    /// this wake; the next wake is the natural retry.
    #[error("wake error: {0}")]
    Wake(String),
}

/// The default inbox capacity when the caller has no preference.
pub const DEFAULT_INBOX_CAPACITY: usize = 256;

/// The result of one wake procedure run, reported back through the
/// FIFO inbox (specs.md Section 6.1, rule 1).
#[derive(Debug)]
pub struct WakeReport {
    /// True for a forced wake (mention/reply, Section 8.1).
    pub forced: bool,
    /// The resolved target of the reply. `None` means the gate said no
    /// (or named a target outside the presented set — treated as
    /// no-participation).
    pub target: Option<GateMessage>,
    /// The generated reply text. `Some` only when `target` is `Some`.
    pub reply_text: Option<String>,
    /// The planned recall injections of this wake (Section 9.4). The
    /// completion handler applies them (Section 9.3 dedup rows, Rule C2
    /// context append) regardless of the gate outcome: the injection
    /// happens as part of recall (step 2), BEFORE the participation
    /// decision consumes the memories.
    pub injections: Vec<PlannedInjection>,
    /// The injection position: the tail raw-log row id computed at
    /// wake start (Rule C2: the injection directly follows this row).
    pub injection_position: i64,
    /// The trigger that started this wake (telemetry for the curated
    /// wake log line): `"forced"`, or the `FireReason` spelling of the
    /// fire site.
    pub trigger: &'static str,
    /// The gate outcome of this wake (telemetry for the curated wake
    /// log line): `"participate"`, `"silent"`, or `"bypassed_forced"`.
    pub gate: &'static str,
    /// The gate's own reason string, when the gate ran (telemetry for
    /// the curated wake log line). `None` for a forced wake.
    pub gate_reason: Option<String>,
}

/// Commands of the per-group actor inbox. specs.md Section 6.1, rule 1:
/// all trigger events enter one FIFO inbox.
pub enum ActorCommand {
    Inbound(InboundEvent),
    /// Evaluates the triggers at the given instant. The M4 timer driver
    /// emits these on cadence with `OffsetDateTime::now_utc()`; tests
    /// drive time explicitly.
    Tick(OffsetDateTime),
    /// Returns a snapshot of the session state (tests, restart checks).
    Snapshot(oneshot::Sender<SessionState>),
    /// Returns a clone of the live context items (tests; M4 reads the
    /// context through the actor too). A FIFO barrier like `Snapshot`.
    ContextSnapshot(oneshot::Sender<Vec<ContextItem>>),
    /// The spawned digest task reports its result through this command
    /// (internal plumbing). Every session mutation stays serialized in
    /// the actor loop — specs.md Section 6.1, rule 2.
    DigestCompleted(std::result::Result<Option<DigestOutcome>, CoreError>),
    /// The spawned summary task reports its result through this command
    /// (internal plumbing, the same pattern as `DigestCompleted`). The
    /// Rule C3 mutation of the digest completion is DEFERRED until this
    /// report arrives: the summary row must persist BEFORE the chunk it
    /// replaces is dropped (Rule P1 — the summary text is not derivable
    /// from persisted state). Carries the deferred completion context.
    SummaryCompleted {
        /// The digest outcome being finalized (post-digest hook input).
        outcome: DigestOutcome,
        /// The removed chunk's range `(first_msg_id, last_msg_id]` —
        /// the natural dedup key of the summary row. `last_msg_id` is
        /// the removal cutoff (the old last boundary).
        first_msg_id: i64,
        last_msg_id: i64,
        /// The digest batch's new boundary (deferred boundary update).
        new_boundary: i64,
        /// The summary text, or the provider failure.
        result: std::result::Result<String, SummaryError>,
    },
    /// The spawned wake task reports its result through this command
    /// (internal plumbing, the same pattern as `DigestCompleted`).
    WakeCompleted(std::result::Result<WakeReport, CoreError>),
    Shutdown,
}

/// The handle of a running group actor.
pub struct GroupActorHandle {
    chat_id: String,
    inbox: mpsc::Sender<ActorCommand>,
    join: JoinHandle<Result<(), CoreError>>,
}

impl GroupActorHandle {
    /// The group this actor serves.
    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    /// Sends one command to the FIFO inbox.
    pub async fn send(&self, cmd: ActorCommand) -> Result<(), CoreError> {
        self.inbox
            .send(cmd)
            .await
            .map_err(|_| CoreError::InboxClosed)
    }

    /// Sends one inbound event to the FIFO inbox.
    pub async fn send_event(&self, ev: InboundEvent) -> Result<(), CoreError> {
        self.send(ActorCommand::Inbound(ev)).await
    }

    /// Returns a snapshot of the current session state.
    pub async fn snapshot(&self) -> Result<SessionState, CoreError> {
        let (tx, rx) = oneshot::channel();
        self.send(ActorCommand::Snapshot(tx)).await?;
        rx.await.map_err(|_| CoreError::InboxClosed)
    }

    /// Returns a clone of the current live context items.
    pub async fn context_snapshot(&self) -> Result<Vec<ContextItem>, CoreError> {
        let (tx, rx) = oneshot::channel();
        self.send(ActorCommand::ContextSnapshot(tx)).await?;
        rx.await.map_err(|_| CoreError::InboxClosed)
    }

    /// Sends Shutdown and awaits the task. Graceful.
    ///
    /// A startup failure aborts the actor task before the loop runs. Such a
    /// failure surfaces here through the JoinHandle: the send fails (the
    /// receiver is gone) and the join returns the startup error.
    pub async fn shutdown(self) -> Result<(), CoreError> {
        // A failed send means the task already ended; the join below still
        // reports the outcome.
        let _ = self.inbox.send(ActorCommand::Shutdown).await;
        match self.join.await {
            Ok(result) => result,
            Err(error) => Err(CoreError::Join(error.to_string())),
        }
    }
}

/// Parameters of `spawn_group_actor`.
pub struct GroupActorParams<M: MemoryBackend> {
    pub chat_id: String,
    pub store: Arc<Store>,
    pub memory: Arc<M>,
    pub config: TriggerConfig,
    /// Start time of the process run. Used for fresh jitter and as the
    /// fallback "now" for a brand-new group.
    pub started_at: OffsetDateTime,
    /// Inbox capacity. Refer to `DEFAULT_INBOX_CAPACITY`.
    pub inbox_capacity: usize,
    /// The rendered persona preamble. The caller renders it through
    /// tamako-persona's `PreambleRenderer`; the actor stores it as
    /// item 0 of the live context (Rule C4).
    pub preamble: String,
    /// The digest pipeline. `None` keeps the Phase 0 stub behavior
    /// (the trigger logs only). The tamako binary wires the live
    /// implementation; tests wire a scripted one.
    pub digest: Option<Arc<dyn DigestPipeline>>,
    /// A seam for post-digest observers that need no actor state.
    /// `None` = no-op. The actor itself performs the Rule C3 context
    /// removal BEFORE it calls this hook (M2).
    pub post_digest_hook: Option<Arc<dyn PostDigestHook>>,
    /// The wake-procedure services (M4). `None` keeps the stub behavior
    /// EXACTLY: an unforced fire logs and resets, a forced wake logs
    /// only. `Some` runs the wake procedure of specs.md Section 9.
    pub wake: Option<WakeServices>,
    /// The Rule C3 chunk summarizer (segmented summarization, keep-two
    /// retention). `None` keeps the old C3 behavior EXACTLY: the removed
    /// chunk drops without a summary. The tamako binary wires the live
    /// implementation (tamako-agent); tests wire a scripted one.
    pub summary_provider: Option<Arc<dyn SummaryProvider>>,
    /// The outbound action sink (Rule A3). The binary owns the platform
    /// adapter and pumps this channel into `PlatformAdapter::execute`.
    /// `None` drops actions with a debug log. The actor never blocks on
    /// the sink: a full or closed channel degrades to a logged drop
    /// (Section 4.2 tolerates outbound failures; the raw-log row — the
    /// source of truth — is already persisted at that point).
    pub outbound: Option<mpsc::Sender<OutboundAction>>,
    /// The sender display name of outbound raw-log rows (the persona
    /// name). `None` falls back to "Tamako".
    pub bot_name: Option<String>,
}

/// Spawns the actor task and returns the handle immediately.
///
/// Design note (one of the two options of the task spec): the startup
/// sequence runs INSIDE the spawned task. `spawn_group_actor` is
/// synchronous and never blocks the caller. A startup failure aborts the
/// task with the error; `GroupActorHandle::shutdown` surfaces it through
/// the JoinHandle. Callers that must know the startup outcome await one
/// `snapshot()` (FIFO order guarantees startup finished first) or call
/// `shutdown()`.
///
/// Startup sequence:
/// 1. `store.open_group` — creates `{data_root}/{chat_id}/store.db`
///    (Rule P5). Runs in `spawn_blocking` (AGENT.md Section 6.2).
/// 2. `memory.ensure_schema(chat_id)` — one graph file per group
///    (specs.md Section 5.1).
/// 3. `store.load_all_state` → `SessionState::decode` — the rebuild of
///    specs.md Section 6.1, rule 4.
/// 4. `store.list_messages_after` + `store.list_injected_memories` →
///    `LiveContext::rebuild` — the Rule P1 rebuild of the live context.
///
/// The `MemoryBackend` trait promises `Send` futures, so the actor task
/// runs under any tokio runtime flavor.
pub fn spawn_group_actor<M: MemoryBackend + 'static>(
    params: GroupActorParams<M>,
) -> GroupActorHandle {
    let (tx, rx) = mpsc::channel(params.inbox_capacity);
    let chat_id = params.chat_id.clone();
    // The spawned digest task posts `DigestCompleted` back through this
    // sender (specs.md Section 6.1, rule 1: one FIFO inbox).
    let join = tokio::spawn(run_actor(params, tx.clone(), rx));
    GroupActorHandle {
        chat_id,
        inbox: tx,
        join,
    }
}

/// Maps a `JoinError` of a blocking store call into `CoreError`.
fn join_error(error: tokio::task::JoinError) -> CoreError {
    CoreError::Join(error.to_string())
}

/// Runs one blocking store call. AGENT.md Section 6.2: synchronous storage
/// calls never block the async runtime.
async fn blocking_store<T, F>(store: &Arc<Store>, call: F) -> Result<T, CoreError>
where
    T: Send + 'static,
    F: FnOnce(Arc<Store>) -> Result<T, StoreError> + Send + 'static,
{
    let store = Arc::clone(store);
    let outcome = tokio::task::spawn_blocking(move || call(store))
        .await
        .map_err(join_error)?;
    Ok(outcome?)
}

/// Builds the raw-log row for one normalized inbound message.
/// Rule A4: the normalized message carries every field the row needs.
fn to_new_message(msg: &NormalizedMessage, event_type: EventType) -> NewMessage {
    NewMessage {
        platform_msg_id: msg.platform_msg_id.clone(),
        direction: Direction::Inbound,
        event_type,
        timestamp: msg.timestamp,
        sender_id: msg.sender_id.clone(),
        sender_display_name: msg.sender_display_name.clone(),
        sender_username: msg.username.clone(),
        text: msg.text.clone(),
        reply_to_platform_msg_id: msg.reply_to_platform_msg_id.clone(),
        mentions_bot: msg.mentions_bot,
        is_reply_to_bot: msg.is_reply_to_bot,
    }
}

/// Resolves the `ReplyRender` of one inbound row at intake time (the
/// message path and the edit path share this helper). Rules A3/B1: a
/// reply to the bot renders `ToBot` with NO target lookup — outbound
/// rows carry synthetic `bot-out:{nanos}` platform ids, so resolution
/// is impossible by construction; do not fake one. A user-reply target
/// resolves through `Store::find_reply_target` (one indexed SELECT per
/// reply message; acceptable). A store failure logs at debug and falls
/// back to `target: None` — a failed lookup must never fail intake
/// (Rule P1: the row is already persisted; the reply renders as an
/// unresolved user reply).
async fn resolve_reply_render(
    store: &Arc<Store>,
    chat_id: &str,
    is_reply_to_bot: bool,
    reply_to_platform_msg_id: Option<&str>,
) -> ReplyRender {
    if is_reply_to_bot {
        return ReplyRender::ToBot;
    }
    let Some(pid) = reply_to_platform_msg_id else {
        return ReplyRender::None;
    };
    let lookup_chat_id = chat_id.to_string();
    let pid_owned = pid.to_string();
    let resolved = blocking_store(store, move |store| {
        store.find_reply_target(&lookup_chat_id, &pid_owned)
    })
    .await;
    match resolved {
        Ok(target) => ReplyRender::ToUser { target },
        Err(error) => {
            debug!(
                chat_id = %chat_id,
                platform_msg_id = %pid,
                %error,
                "reply target lookup failed; rendering without target attributes"
            );
            ReplyRender::ToUser { target: None }
        }
    }
}

/// Resolves the reply-target map of one batch of raw-log rows in ONE
/// blocking pass (the wake gate input and the startup rebuild share
/// this helper, so both render exactly like the intake appends — Rule
/// P1 bit-identity). Rules A3/B1: replies to the bot are excluded —
/// the synthetic `bot-out:{nanos}` outbound ids never resolve by
/// construction. Duplicate platform ids (several replies to one
/// target) resolve once. A store failure propagates like any other
/// store failure of the calling path.
async fn resolve_reply_targets(
    store: &Arc<Store>,
    chat_id: &str,
    rows: &[MessageRow],
) -> Result<HashMap<String, ReplyTargetRow>, CoreError> {
    let pids: HashSet<String> = rows
        .iter()
        .filter(|row| row.direction == Direction::Inbound && !row.is_reply_to_bot)
        .filter_map(|row| row.reply_to_platform_msg_id.clone())
        .collect();
    if pids.is_empty() {
        return Ok(HashMap::new());
    }
    let lookup_chat_id = chat_id.to_string();
    blocking_store(store, move |store| {
        let mut map = HashMap::new();
        for pid in &pids {
            if let Some(target) = store.find_reply_target(&lookup_chat_id, pid)? {
                map.insert(pid.clone(), target);
            }
        }
        Ok(map)
    })
    .await
}

/// The `ReplyRender` of one raw-log row over the already-resolved
/// target map (the wake gate input). Rules A3/B1: replies to the bot
/// render `ToBot` with no target name/id. The map lookup mirrors
/// `LiveContext::rebuild` exactly, so a gate message renders
/// byte-identically to the context item of the same row.
fn reply_render_from_row(
    row: &MessageRow,
    targets: &HashMap<String, ReplyTargetRow>,
) -> ReplyRender {
    if row.is_reply_to_bot {
        ReplyRender::ToBot
    } else {
        match &row.reply_to_platform_msg_id {
            Some(pid) => ReplyRender::ToUser {
                target: targets.get(pid).cloned(),
            },
            None => ReplyRender::None,
        }
    }
}

/// Builds the reaction row for one reaction event (specs.md
/// Section 5.2). The event carries every field the row needs.
fn to_new_reaction(reaction: &ReactionEvent) -> NewReaction {
    NewReaction {
        platform_msg_id: reaction.platform_msg_id.clone(),
        reactor_user_id: reaction.reactor_id.clone(),
        anonymous: reaction.anonymous,
        aggregated: reaction.aggregated,
        old_emojis: reaction.old_emojis.clone(),
        new_emojis: reaction.new_emojis.clone(),
        timestamp: reaction.timestamp,
    }
}

/// Persists the session state. specs.md Section 6.1, rule 4: the actor
/// persists the session state after every mutation.
async fn persist_session(
    store: &Arc<Store>,
    chat_id: &str,
    session: &SessionState,
) -> Result<(), CoreError> {
    let pairs = session.encode();
    let chat_id = chat_id.to_string();
    blocking_store(store, move |store| store.set_state_many(&chat_id, &pairs)).await
}

/// The counter increment of specs.md Section 12. Best effort (the M1
/// counter style): a failure is logged and swallowed, never propagated —
/// the wake does not depend on its metrics.
async fn bump_counter(store: &Arc<Store>, chat_id: &str, key: &str) {
    let chat_id_owned = chat_id.to_string();
    let key_owned = key.to_string();
    let result = blocking_store(store, move |store| {
        store.increment_counter(&chat_id_owned, &key_owned, 1)
    })
    .await;
    if let Err(error) = result {
        tracing::warn!(%error, counter = key, "failed to increment a wake counter");
    }
}

/// The period of the M4 timer driver. `timer_cadence` can return zero
/// (only when `wake_floor` is zero), and `tokio::time::interval` panics
/// on a zero period, so a zero cadence falls back to one second. With a
/// zero floor the intake path drives nearly every wake anyway; the
/// interval trigger fires at most one second late.
fn timer_period(config: &TriggerConfig) -> Duration {
    let cadence = timer_cadence(config);
    if cadence.is_zero() {
        Duration::from_secs(1)
    } else {
        cadence
    }
}

/// The curated wake log line: exactly one `info` line per wake, message
/// string exactly `wake`, emitted once at wake completion (or at the
/// point the wake was suppressed/skipped). `reason` (the gate's own
/// reason string) and `reply_to` (the target's platform message id)
/// appear only when present.
#[allow(clippy::too_many_arguments)]
fn log_wake_line(
    chat_id: &str,
    trigger: &'static str,
    injections: usize,
    gate: &'static str,
    reason: Option<&str>,
    action: &'static str,
    reply_to: Option<&str>,
) {
    info!(
        chat_id = %chat_id,
        trigger,
        injections,
        gate,
        reason = reason,
        action,
        reply_to = reply_to,
        "wake"
    );
}

/// Evaluates the digest trigger (specs.md Section 8.2). On fire, spawns
/// the pipeline task; the result returns through the FIFO inbox as
/// `DigestCompleted`. No-op when no pipeline is wired or a digest is
/// already in flight (one digest at a time per group; Section 6.1
/// rule 2 serializes the graph writes of one group).
///
/// Cost note: one tail scan (`list_messages_after`) per evaluation. The
/// tail is bounded by the size thresholds in practice — a never-digested
/// tail grows until a size threshold fires (Section 8.2). Phase 1
/// accepts this.
#[allow(clippy::too_many_arguments)]
async fn maybe_launch_digest(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &SessionState,
    digest: Option<&Arc<dyn DigestPipeline>>,
    digest_in_flight: &mut bool,
    summary_pending: bool,
    inbox_sender: &mpsc::Sender<ActorCommand>,
    now: OffsetDateTime,
) -> Result<(), CoreError> {
    let Some(pipeline) = digest else {
        return Ok(());
    };
    if *digest_in_flight {
        return Ok(());
    }
    // A pending summary holds the deferred C3 mutation of the last
    // digest completion: `last_digest_boundary_msg_id` has NOT advanced
    // yet, so a new digest would redo the same batch range. Suppress
    // the trigger until the SummaryCompleted report lands (decision
    // 62). The next evaluation point after it retries naturally.
    if summary_pending {
        return Ok(());
    }
    let tail_chat_id = chat_id.to_string();
    let boundary = session.last_digest_boundary_msg_id;
    let rows = blocking_store(store, move |store| {
        store.list_messages_after(&tail_chat_id, boundary)
    })
    .await?;
    // The timeout fallback of Section 8.2 needs the time of the last
    // digest; it comes from the persisted session state.
    let stats = tail_stats(&rows, session.last_digest_at);
    if !digest_should_fire(&stats, now, config) {
        return Ok(());
    }
    // Section 6.1, rule 3: a blocked batch must not block the queue. The
    // extraction and its backoff run in a spawned task; the inbox keeps
    // moving while the digest is in flight.
    *digest_in_flight = true;
    let pipeline = Arc::clone(pipeline);
    let chat_id = chat_id.to_string();
    let sender = inbox_sender.clone();
    tokio::spawn(async move {
        let result = pipeline.run_digest(&chat_id, boundary).await;
        // A failed send means the actor is shutting down. The result is
        // dropped; the boundary did not advance, so the next run redoes
        // the batch (the MERGEs are idempotent, Section 10.3).
        let _ = sender.send(ActorCommand::DigestCompleted(result)).await;
    });
    Ok(())
}

/// The Rule C3 mutation of a digest completion (decision 16): context
/// removal at the cutoff, dedup prune, boundary update, session persist,
/// keep-two summary upsert — ONE serialized step inside the actor loop
/// (specs.md Section 6.1, rule 2). Runs on the immediate path (no
/// summarization needed) and on the deferred path after the summary row
/// persisted (decision 62). The post-digest hook runs AFTER the
/// mutation; it stays a seam for stateless observers.
///
/// The upsert consumes the SAME store query as the startup rebuild
/// (`list_newest_context_summaries(chat_id, 2)`, oldest first), so the
/// live placement and the rebuild placement are bit-identical (Rule
/// P1). Summary items are exempt from `remove_at_or_below`; their
/// retention is count-based through `upsert_summaries`.
#[allow(clippy::too_many_arguments)]
async fn finalize_digest_completion(
    store: &Arc<Store>,
    chat_id: &str,
    session: &mut SessionState,
    context: &mut LiveContext,
    cutoff: i64,
    b_new: i64,
    outcome: &DigestOutcome,
    post_digest_hook: &Option<Arc<dyn PostDigestHook>>,
) -> Result<(), CoreError> {
    // Rule C3 context removal with the one-chunk lag. Only items at or
    // below the PREVIOUS chunk's boundary go; the chunk just digested,
    // (cutoff, b_new], stays as the new overlap buffer (specs.md
    // Section 7.1). This runs for EVERY outcome variant, matching the
    // boundary advancement: a dead-lettered batch is skipped (specs.md
    // Section 10.3) — the skipped range stays in the raw log and its
    // content lags out of the context mechanically at the next digest.
    context.remove_at_or_below(cutoff);
    // Prune the dedup set (specs.md Section 10.2 step 4) at the same
    // cutoff.
    let prune_chat_id = chat_id.to_string();
    let deleted = blocking_store(store, move |store| {
        store.delete_injected_memories_up_to(&prune_chat_id, cutoff)
    })
    .await?;
    debug!(chat_id = %chat_id, deleted, "injected_memories pruned");
    // The session boundaries. Read `last_digest_at` BEFORE it is
    // overwritten below: the FIRST completed digest has no previous
    // chunk, so prev stays None; from the second digest on, prev is
    // the boundary that was current before this digest.
    session.prev_digest_boundary_msg_id = if session.last_digest_at.is_some() {
        Some(cutoff)
    } else {
        None
    };
    session.last_digest_boundary_msg_id = b_new;
    // The wall-clock completion time: the timeout fallback of Section
    // 8.2 measures real time since the last digest.
    session.last_digest_at = Some(OffsetDateTime::now_utc());
    persist_session(store, chat_id, session).await?;
    // Keep-two summary retention (decision 62): refresh the summary
    // block from the persisted rows (the two newest, oldest first).
    let summaries_chat_id = chat_id.to_string();
    let summaries = blocking_store(store, move |store| {
        store.list_newest_context_summaries(&summaries_chat_id, 2)
    })
    .await?;
    context.upsert_summaries(&summaries);
    if let Some(hook) = post_digest_hook {
        hook.after_digest(chat_id, outcome).await;
    }
    Ok(())
}

/// The actor task. Owns the session state and the live wake scheduler.
/// specs.md Section 6.1, rule 2: session-state mutations are strictly
/// serialized inside this loop.
async fn run_actor<M: MemoryBackend>(
    params: GroupActorParams<M>,
    inbox_sender: mpsc::Sender<ActorCommand>,
    mut inbox: mpsc::Receiver<ActorCommand>,
) -> Result<(), CoreError> {
    let GroupActorParams {
        chat_id,
        store,
        memory,
        config,
        started_at,
        preamble,
        digest,
        post_digest_hook,
        wake: wake_services,
        summary_provider,
        outbound,
        bot_name,
        ..
    } = params;
    let bot_name = bot_name.unwrap_or_else(|| "Tamako".to_string());

    // --- Startup (see the docstring of spawn_group_actor) ---
    let startup_chat_id = chat_id.clone();
    blocking_store(&store, move |store| store.open_group(&startup_chat_id)).await?;
    memory.ensure_schema(&chat_id).await?;
    let persisted_chat_id = chat_id.clone();
    let persisted = blocking_store(&store, move |store| {
        store.load_all_state(&persisted_chat_id)
    })
    .await?;

    // One RNG per actor task, seeded from entropy. Tests need deterministic
    // STATE, not deterministic jitter; encode/decode gives them the state.
    let mut rng = StdRng::from_rng(&mut rand::rng());
    let mut session = SessionState::decode(&persisted, &config, started_at, &mut rng);
    let mut wake = WakeScheduler::from_state(session.wake.clone());

    // --- Startup rebuild of the live context (Rule P1) ---
    // specs.md Section 6.1, rule 4: the rebuild is bit-identical to the
    // pre-restart context. The removal cutoff R is the one-chunk-lag
    // cutoff: every row above the PREVIOUS boundary is still live.
    let removal_cutoff = session.prev_digest_boundary_msg_id.unwrap_or(0);
    let rebuild_chat_id = chat_id.clone();
    let rows = blocking_store(&store, move |store| {
        store.list_messages_after(&rebuild_chat_id, removal_cutoff)
    })
    .await?;
    let injections_chat_id = chat_id.clone();
    let mut injections = blocking_store(&store, move |store| {
        store.list_injected_memories(&injections_chat_id)
    })
    .await?;
    // Defensive filter: the Rule C3 prune normally already deleted the
    // rows at or below the cutoff.
    injections.retain(|row| row.injection_position > removal_cutoff);
    // Rule P1 bit-identity: `LiveContext::rebuild` takes the reply
    // targets as the map the store resolution produces, so rebuild ==
    // incremental. The intake appends resolved through the SAME store
    // function and helper (one blocking pass over the unique
    // reply-target platform ids of the rebuilt inbound rows; Rules
    // A3/B1: replies to the bot are excluded — the synthetic outbound
    // ids never resolve by construction).
    let reply_targets = resolve_reply_targets(&store, &chat_id, &rows).await?;
    // Keep-two summary rebuild (S2a stub for S2b): the startup rebuild
    // consumes the SAME store query the live digest-completion window
    // uses (`Store::list_newest_context_summaries(chat_id, 2)`, oldest
    // first), so live placement and rebuild placement are bit-identical
    // (Rule P1). S2b owns the digest-completion side of the flow.
    let summaries_chat_id = chat_id.clone();
    let summaries = blocking_store(&store, move |store| {
        store.list_newest_context_summaries(&summaries_chat_id, 2)
    })
    .await?;
    let mut context =
        LiveContext::rebuild(preamble, &rows, &injections, &reply_targets, &summaries);

    // One digest at a time per group (Section 6.1, rule 2).
    let mut digest_in_flight = false;
    // One chunk summary at a time per group (decision 62): while a
    // summary task is in flight, the C3 mutation of its digest
    // completion is deferred and the digest trigger is suppressed.
    let mut summary_pending = false;
    // One wake at a time per group (Section 6.2: a forced Wake queues
    // behind a running wake; it does not preempt it). The queued entry
    // carries the intake time of the forcing message, so the queued
    // wake stays on the deterministic replay clock.
    let mut wake_in_flight = false;
    let mut forced_pending: Option<(GateMessage, OffsetDateTime)> = None;

    // --- The M4 timer driver (known gap 2) ---
    // `MissedTickBehavior::Delay`: a delayed tick loses at most cadence
    // time; Burst could storm the FIFO evaluation after a long blockage.
    let mut ticker = tokio::time::interval(timer_period(&config));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first tick of `interval` completes immediately; consume it so
    // startup does not cause an instant evaluation.
    ticker.tick().await;

    // --- Inbox loop ---
    loop {
        let command = tokio::select! {
            received = inbox.recv() => {
                // A closed inbox means every handle is gone; end the task
                // like Shutdown does.
                match received {
                    Some(command) => command,
                    None => break,
                }
            }
            _ = ticker.tick() => ActorCommand::Tick(OffsetDateTime::now_utc()),
        };
        match command {
            ActorCommand::Inbound(InboundEvent::Message(msg)) => {
                handle_message(
                    &store,
                    &chat_id,
                    &config,
                    &mut session,
                    &mut wake,
                    &mut rng,
                    &mut context,
                    digest.as_ref(),
                    &mut digest_in_flight,
                    summary_pending,
                    wake_services.as_ref(),
                    &mut wake_in_flight,
                    &mut forced_pending,
                    &inbox_sender,
                    msg,
                )
                .await?;
            }
            ActorCommand::Inbound(InboundEvent::EditedMessage(msg)) => {
                // specs.md Section 15, open item 4: an edit is appended as a
                // new log row with event_type Edit. It is never a
                // retraction. No other processing in Phase 0.
                let row = to_new_message(&msg, EventType::Edit);
                let edit_chat_id = chat_id.clone();
                let outcome = blocking_store(&store, move |store| {
                    store.insert_message(&edit_chat_id, &row)
                })
                .await?;
                // Rule C1: every new raw-log row enters the context. An
                // edit is just a new row; `LiveContext::rebuild` renders
                // edits with kind="edit", so this append is uniform with
                // the restart rebuild. The reply render resolves like
                // the message path (Rules A3/B1: a reply to the bot
                // never resolves by construction), after the persist
                // (Rule P1), so the rebuild renders bit-identically.
                if let InsertOutcome::Inserted(id) = outcome {
                    let reply_render = resolve_reply_render(
                        &store,
                        &chat_id,
                        msg.is_reply_to_bot,
                        msg.reply_to_platform_msg_id.as_deref(),
                    )
                    .await;
                    context.append_human_message(
                        id,
                        &msg.sender_display_name,
                        msg.username.as_deref(),
                        msg.timestamp,
                        true,
                        msg.mentions_bot,
                        reply_render,
                        &msg.text,
                    );
                }
            }
            ActorCommand::Inbound(InboundEvent::Reaction(reaction)) => {
                handle_reaction(&store, &chat_id, reaction).await?;
            }
            // Member join/leave events have no consumer yet (Phase 1
            // collects reactions only). They stay debug-only.
            ActorCommand::Inbound(
                event @ (InboundEvent::MemberJoin(_) | InboundEvent::MemberLeave(_)),
            ) => {
                debug!(chat_id = %chat_id, event = ?event, "member event ignored (no consumer yet)");
            }
            ActorCommand::Tick(now) => {
                // One shared handler for the explicit Tick command and
                // the timer-driver tick, so the two cannot diverge.
                handle_tick(
                    &store,
                    &chat_id,
                    &config,
                    &mut session,
                    &mut wake,
                    &mut rng,
                    &context,
                    digest.as_ref(),
                    &mut digest_in_flight,
                    summary_pending,
                    wake_services.as_ref(),
                    &mut wake_in_flight,
                    &inbox_sender,
                    now,
                )
                .await?;
            }
            ActorCommand::WakeCompleted(result) => {
                wake_in_flight = false;
                match result {
                    Err(error) => {
                        // Log, skip, no crash, NO inline retry: the next
                        // wake is the natural retry (specs.md Section 9
                        // failure handling).
                        tracing::error!(chat_id = %chat_id, %error, "wake procedure failed; skipping this wake");
                    }
                    Ok(report) => {
                        handle_wake_report(
                            &store,
                            &chat_id,
                            &config,
                            &mut session,
                            &mut context,
                            outbound.as_ref(),
                            &bot_name,
                            report,
                        )
                        .await?;
                    }
                }
                // Section 6.2: a queued forced Wake moves to the head of
                // the queue; it starts immediately after the current
                // wake completes. The intake time of the forcing message
                // is its `now` (deterministic replay).
                if let Some(services) = wake_services.as_ref() {
                    if let Some((forcing, forced_at)) = forced_pending.take() {
                        start_wake(
                            &store,
                            &chat_id,
                            &config,
                            &mut session,
                            &mut wake,
                            &mut rng,
                            &context,
                            services,
                            &mut wake_in_flight,
                            &inbox_sender,
                            Some(forcing),
                            forced_at,
                            "forced",
                        )
                        .await?;
                    }
                }
            }
            ActorCommand::DigestCompleted(result) => {
                digest_in_flight = false;
                match result {
                    Ok(Some(outcome)) => {
                        // The boundaries of this digest's range, read
                        // BEFORE the session mutation below: the curated
                        // line renders the range `(b_old, b_new]`.
                        let b_old = session.last_digest_boundary_msg_id;
                        let b_new = outcome.new_boundary();
                        // The curated digest line: exactly one `info`
                        // line per completed digest, message string
                        // exactly `digest`. A dead-lettered batch keeps
                        // the pipeline's ERROR line (specs.md Section
                        // 10.3) as its one line; the actor line drops
                        // to debug.
                        let range = format!("({b_old},{b_new}]");
                        match &outcome {
                            DigestOutcome::Extracted {
                                batch_id,
                                node_count,
                                edge_count,
                                ..
                            } => info!(
                                chat_id = %chat_id,
                                batch_id = %batch_id,
                                range = %range,
                                outcome = "written",
                                nodes = node_count,
                                edges = edge_count,
                                "digest"
                            ),
                            DigestOutcome::Skeleton { batch_id, .. } => info!(
                                chat_id = %chat_id,
                                batch_id = %batch_id,
                                range = %range,
                                outcome = "skeleton",
                                "digest"
                            ),
                            DigestOutcome::DeadLettered { batch_id, .. } => debug!(
                                chat_id = %chat_id,
                                batch_id = %batch_id,
                                range = %range,
                                outcome = "dead_lettered",
                                "digest"
                            ),
                        }
                        // Segmented summarization (decision 62, the
                        // Rule C3 amendment): the chunk being removed,
                        // (lower, b_old] with lower = the previous
                        // removal cutoff, is summarized BEFORE the
                        // removal, and the summary row persists before
                        // the chunk drops (Rule P1). The FIRST digest
                        // completion has b_old == 0: nothing is removed
                        // and nothing is summarized. Without a wired
                        // summarizer the old C3 behavior applies
                        // EXACTLY (drop without a summary).
                        let mut defer_to_summary = false;
                        if b_old > 0 {
                            if let Some(provider) = summary_provider.as_ref() {
                                let lower = session.prev_digest_boundary_msg_id.unwrap_or(0);
                                // Check-before-call (replay idempotency): a
                                // handler re-run after a crash finds the
                                // persisted row and SKIPS the LLM call.
                                let existing_chat_id = chat_id.clone();
                                let existing = blocking_store(&store, move |store| {
                                    store.find_context_summary(&existing_chat_id, lower, b_old)
                                })
                                .await?;
                                if existing.is_none() {
                                    // The summarizer input is the raw-log
                                    // range (human + bot rows; injections
                                    // never enter the raw log — the specs.md
                                    // Section 10.1/9.5 analog).
                                    let rows_chat_id = chat_id.clone();
                                    let rows = blocking_store(&store, move |store| {
                                        store.list_messages_in_range(&rows_chat_id, lower, b_old)
                                    })
                                    .await?;
                                    // Section 6.1, rule 3 analog: the FIFO
                                    // never blocks on the LLM call. The C3
                                    // mutation defers to the SummaryCompleted
                                    // report; the digest trigger is
                                    // suppressed while the summary is
                                    // pending (the batch range derives from
                                    // the not-yet-advanced last boundary).
                                    summary_pending = true;
                                    defer_to_summary = true;
                                    let provider = Arc::clone(provider);
                                    let summary_chat_id = chat_id.clone();
                                    let sender = inbox_sender.clone();
                                    let report_outcome = outcome.clone();
                                    tokio::spawn(async move {
                                        let result = provider
                                            .summarize(&summary_chat_id, lower, b_old, &rows)
                                            .await;
                                        // A failed send means the actor is
                                        // shutting down. Nothing was
                                        // persisted; the restart replays the
                                        // digest completion (the digest
                                        // itself is idempotent, Section
                                        // 10.3) and re-summarizes.
                                        let _ = sender
                                            .send(ActorCommand::SummaryCompleted {
                                                outcome: report_outcome,
                                                first_msg_id: lower,
                                                last_msg_id: b_old,
                                                new_boundary: b_new,
                                                result,
                                            })
                                            .await;
                                    });
                                }
                            }
                        }
                        if !defer_to_summary {
                            finalize_digest_completion(
                                &store,
                                &chat_id,
                                &mut session,
                                &mut context,
                                b_old,
                                b_new,
                                &outcome,
                                &post_digest_hook,
                            )
                            .await?;
                            // Re-evaluate once: the tail can still
                            // exceed the thresholds (it grew during a
                            // long extraction).
                            maybe_launch_digest(
                                &store,
                                &chat_id,
                                &config,
                                &session,
                                digest.as_ref(),
                                &mut digest_in_flight,
                                summary_pending,
                                &inbox_sender,
                                OffsetDateTime::now_utc(),
                            )
                            .await?;
                        }
                    }
                    // The tail was empty; no state change.
                    Ok(None) => {}
                    Err(error) => {
                        // The pipeline dead-letters extraction failures
                        // itself; an escaping Err is an infrastructure
                        // failure (example: the store read failed). Do
                        // NOT advance the boundary and do NOT retry here:
                        // the next evaluation point retries naturally.
                        tracing::error!(chat_id = %chat_id, %error, "digest pipeline failed");
                    }
                }
            }
            ActorCommand::SummaryCompleted {
                outcome,
                first_msg_id,
                last_msg_id,
                new_boundary,
                result,
            } => {
                // The deferred C3 mutation of decision 62 lands here.
                summary_pending = false;
                match result {
                    Ok(text) => {
                        // Rule P1 order: the summary row persists BEFORE
                        // the chunk it replaces is dropped. The insert is
                        // idempotent on the range key (replay safety).
                        let insert_chat_id = chat_id.clone();
                        let insert_text = text.clone();
                        blocking_store(&store, move |store| {
                            store.insert_context_summary(
                                &insert_chat_id,
                                first_msg_id,
                                last_msg_id,
                                &insert_text,
                            )
                        })
                        .await?;
                        debug!(
                            chat_id = %chat_id,
                            range = %format!("({first_msg_id},{last_msg_id}]"),
                            bytes = text.len(),
                            "context summary stored"
                        );
                        finalize_digest_completion(
                            &store,
                            &chat_id,
                            &mut session,
                            &mut context,
                            last_msg_id,
                            new_boundary,
                            &outcome,
                            &post_digest_hook,
                        )
                        .await?;
                    }
                    Err(error) => {
                        // Failure semantics (decision 62): never drop the
                        // chunk silently. The removal is DEFERRED one
                        // digest cycle: `last` advances (the digest
                        // itself completed), `prev` stays, so the next
                        // completion retries over the WIDENED range
                        // `(prev, new_last]` (the deferred chunk plus
                        // the newly digested one; the natural-key
                        // check-before-call still makes an exact-match
                        // replay cheap), and the raw chunk stays in the
                        // context. The C5 bound stretches by one cycle
                        // on this path; the restart rebuild cutoff
                        // `prev.unwrap_or(0)` stays consistent with the
                        // live view.
                        tracing::warn!(
                            chat_id = %chat_id,
                            %error,
                            range = %format!("({first_msg_id},{last_msg_id}]"),
                            "context summarization failed; the raw chunk stays one more digest cycle"
                        );
                        session.last_digest_boundary_msg_id = new_boundary;
                        session.last_digest_at = Some(OffsetDateTime::now_utc());
                        persist_session(&store, &chat_id, &session).await?;
                        if let Some(hook) = &post_digest_hook {
                            // The digest completed; only the
                            // summarization failed. The hook observes
                            // the digest outcome as usual.
                            hook.after_digest(&chat_id, &outcome).await;
                        }
                    }
                }
                // Re-evaluate: the pending gate is open again and the
                // tail can still exceed the thresholds.
                maybe_launch_digest(
                    &store,
                    &chat_id,
                    &config,
                    &session,
                    digest.as_ref(),
                    &mut digest_in_flight,
                    summary_pending,
                    &inbox_sender,
                    OffsetDateTime::now_utc(),
                )
                .await?;
            }
            ActorCommand::Snapshot(reply) => {
                // A dropped receiver means the caller went away. That is not
                // an actor failure.
                let _ = reply.send(session.clone());
            }
            ActorCommand::ContextSnapshot(reply) => {
                // Same rule as Snapshot: a dropped receiver is not an
                // actor failure.
                let _ = reply.send(context.items().to_vec());
            }
            ActorCommand::Shutdown => break,
        }
    }
    Ok(())
}

/// Reaction intake. specs.md Section 5.2: reaction data is not
/// recoverable later, so collection starts at intake time. Passive
/// collection: no context item, no wake-counter advance, no session
/// mutation (no session persist — nothing mutated).
async fn handle_reaction(
    store: &Arc<Store>,
    chat_id: &str,
    reaction: ReactionEvent,
) -> Result<(), CoreError> {
    // Rule P1: persist the reaction before anything else.
    let row = to_new_reaction(&reaction);
    let reaction_chat_id = chat_id.to_string();
    let outcome = blocking_store(store, move |store| {
        store.insert_reaction(&reaction_chat_id, &row)
    })
    .await?;
    if let InsertOutcome::Duplicate = outcome {
        // Idempotent intake (AGENT.md Section 6.2): a reconnect can
        // redeliver the same reaction update. The dedup index makes the
        // second insert a Duplicate. That is not an error.
        debug!(chat_id = %chat_id, platform_msg_id = %reaction.platform_msg_id, "duplicate reaction delivery");
    }
    Ok(())
}

/// Message intake. specs.md Section 8.1.
#[allow(clippy::too_many_arguments)]
async fn handle_message(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    wake: &mut WakeScheduler,
    rng: &mut StdRng,
    context: &mut LiveContext,
    digest: Option<&Arc<dyn DigestPipeline>>,
    digest_in_flight: &mut bool,
    summary_pending: bool,
    wake_services: Option<&WakeServices>,
    wake_in_flight: &mut bool,
    forced_pending: &mut Option<(GateMessage, OffsetDateTime)>,
    inbox_sender: &mpsc::Sender<ActorCommand>,
    msg: NormalizedMessage,
) -> Result<(), CoreError> {
    // Rule P1 (specs.md Section 8.1): FIRST persist to the raw log. The
    // insert completes before anything else runs.
    let row = to_new_message(&msg, EventType::Message);
    let intake_chat_id = chat_id.to_string();
    let outcome = blocking_store(store, move |store| {
        store.insert_message(&intake_chat_id, &row)
    })
    .await?;
    // Resolve the reply render AFTER the persist (Rule P1) and BEFORE
    // the context append: the row is already in the log, so a user
    // reply resolves its target against the full log — the same view
    // the startup rebuild looks up, which keeps the intake render and
    // the rebuild render bit-identical (a target can be pruned from
    // the live context while the replying item stays).
    let reply_render = resolve_reply_render(
        store,
        chat_id,
        msg.is_reply_to_bot,
        msg.reply_to_platform_msg_id.as_deref(),
    )
    .await;
    let inserted_id = match outcome {
        InsertOutcome::Inserted(id) => {
            // Rule C1: the log row exists first (Rule P1), then the
            // materialized view gets the same item.
            context.append_human_message(
                id,
                &msg.sender_display_name,
                msg.username.as_deref(),
                msg.timestamp,
                false,
                msg.mentions_bot,
                reply_render.clone(),
                &msg.text,
            );
            Some(id)
        }
        InsertOutcome::Duplicate => {
            // Idempotent intake (AGENT.md Section 6.2): a replay after a
            // crash can re-see a message. The row exists already; the
            // counters below still update, so the wake counter can count
            // one delivery twice. The log row — the source of truth — is
            // not duplicated, and the view must not duplicate either.
            debug!(chat_id = %chat_id, platform_msg_id = %msg.platform_msg_id, "duplicate delivery");
            None
        }
    };

    // Update the session in memory.
    session.record_human_message();
    wake.record_message();
    session.wake = wake.snapshot();
    persist_session(store, chat_id, session).await?;

    // Trigger evaluation. `msg.timestamp` is `now`: deterministic
    // replay. specs.md Section 6.2: Digest runs BEFORE Wake, so the
    // digest trigger is evaluated first.
    let now = msg.timestamp;
    maybe_launch_digest(
        store,
        chat_id,
        config,
        session,
        digest,
        digest_in_flight,
        summary_pending,
        inbox_sender,
        now,
    )
    .await?;
    let Some(services) = wake_services else {
        // The wake services are disabled (no LLM key wired): an
        // unforced fire logs and resets, a forced wake logs only. The
        // startup warning already covers the disabled state, so these
        // lines stay at debug.
        if msg.mentions_bot || msg.is_reply_to_bot {
            // specs.md Section 8.1: the bot must respond when addressed
            // directly. The muted state does not suppress a forced wake.
            debug!(chat_id = %chat_id, "wake services disabled; forced wake ignored");
        } else if wake.should_fire(now, config) {
            debug!(chat_id = %chat_id, "wake services disabled; wake trigger ignored");
            reset_wake(config, session, wake, rng, now);
            persist_session(store, chat_id, session).await?;
        }
        return Ok(());
    };
    // specs.md Section 8.1: a mention of the bot or a reply to the bot
    // forces a wake. Only a NEWLY INSERTED row forces one: a Duplicate
    // redelivery must not force a second wake.
    let forcing = inserted_id
        .filter(|_| msg.mentions_bot || msg.is_reply_to_bot)
        .map(|row_id| GateMessage {
            row_id,
            platform_msg_id: msg.platform_msg_id.clone(),
            content: render_human_content(
                row_id,
                &msg.sender_display_name,
                msg.username.as_deref(),
                msg.timestamp,
                false,
                msg.mentions_bot,
                // The SAME resolved render as the context append
                // above: the forcing gate message and the context
                // item of this row render identically.
                reply_render.clone(),
                &msg.text,
            ),
            sender_id: msg.sender_id.clone(),
            reply_to_platform_msg_id: msg.reply_to_platform_msg_id.clone(),
            text: msg.text.clone(),
        });
    if let Some(forcing) = forcing {
        if *wake_in_flight {
            // Section 6.2: a forced Wake moves to the head of the queue;
            // it does not preempt a running call. It starts immediately
            // after the current wake completes. A second forced wake
            // replaces the queued one (the newest address wins). The
            // queued wake gets its own curated line at completion.
            debug!(chat_id = %chat_id, "a wake is in flight; the forced wake is queued");
            *forced_pending = Some((forcing, now));
        } else {
            start_wake(
                store,
                chat_id,
                config,
                session,
                wake,
                rng,
                context,
                services,
                wake_in_flight,
                inbox_sender,
                Some(forcing),
                now,
                "forced",
            )
            .await?;
        }
    } else if let Some(reason) = wake.fire_reason(now, config) {
        if *wake_in_flight {
            // Section 6.2: inbound messages during a running wake do not
            // interrupt the call. Thanks to reset-at-start their counts
            // already go toward the next wake, so this fire is a no-op;
            // the curated line reports the skip.
            log_wake_line(
                chat_id,
                reason.as_str(),
                0,
                "in_flight_skipped",
                None,
                "nothing",
                None,
            );
        } else {
            start_wake(
                store,
                chat_id,
                config,
                session,
                wake,
                rng,
                context,
                services,
                wake_in_flight,
                inbox_sender,
                None,
                now,
                reason.as_str(),
            )
            .await?;
        }
    }
    Ok(())
}

/// The shared tick handling of the explicit `ActorCommand::Tick` and the
/// M4 timer driver, so the two paths cannot diverge. specs.md Section
/// 6.2: Digest runs BEFORE Wake. A tick never forces a wake (forcing
/// needs a mention/reply, Section 8.1), so there is no `forced_pending`
/// parameter here.
#[allow(clippy::too_many_arguments)]
async fn handle_tick(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    wake: &mut WakeScheduler,
    rng: &mut StdRng,
    context: &LiveContext,
    digest: Option<&Arc<dyn DigestPipeline>>,
    digest_in_flight: &mut bool,
    summary_pending: bool,
    wake_services: Option<&WakeServices>,
    wake_in_flight: &mut bool,
    inbox_sender: &mpsc::Sender<ActorCommand>,
    now: OffsetDateTime,
) -> Result<(), CoreError> {
    maybe_launch_digest(
        store,
        chat_id,
        config,
        session,
        digest,
        digest_in_flight,
        summary_pending,
        inbox_sender,
        now,
    )
    .await?;
    let Some(services) = wake_services else {
        // The wake services are disabled (no LLM key wired); the
        // behavior matches the intake path exactly.
        if wake.should_fire(now, config) {
            debug!(chat_id = %chat_id, "wake services disabled; wake trigger ignored");
            reset_wake(config, session, wake, rng, now);
            persist_session(store, chat_id, session).await?;
        }
        return Ok(());
    };
    if let Some(reason) = wake.fire_reason(now, config) {
        if *wake_in_flight {
            // Same rule as the intake path: the counts already go toward
            // the next wake (reset-at-start), so this fire is a no-op;
            // the curated line reports the skip.
            log_wake_line(
                chat_id,
                reason.as_str(),
                0,
                "in_flight_skipped",
                None,
                "nothing",
                None,
            );
        } else {
            start_wake(
                store,
                chat_id,
                config,
                session,
                wake,
                rng,
                context,
                services,
                wake_in_flight,
                inbox_sender,
                None,
                now,
                reason.as_str(),
            )
            .await?;
        }
    }
    Ok(())
}

/// The wake procedure of specs.md Section 9, steps 1-5, actor side.
/// Runs inside the actor loop; only the LLM calls leave the loop (they
/// run in a spawned task over owned data and report back through the
/// FIFO inbox as `WakeCompleted`). `trigger` is the telemetry spelling
/// of what started this wake (`"forced"` or a `FireReason`); it lands
/// on the curated wake log line.
#[allow(clippy::too_many_arguments)]
async fn start_wake(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    wake: &mut WakeScheduler,
    rng: &mut StdRng,
    context: &LiveContext,
    services: &WakeServices,
    wake_in_flight: &mut bool,
    inbox_sender: &mpsc::Sender<ActorCommand>,
    forced: Option<GateMessage>,
    now: OffsetDateTime,
    trigger: &'static str,
) -> Result<(), CoreError> {
    // Step 1 (Sections 9 and 8.5): the monologue lock suppresses an
    // unforced wake. A forced wake is never suppressed (Section 8.1).
    // `wake_last_row_id` is deliberately NOT advanced here: messages
    // that arrive during the muted period stay "new" for the next real
    // wake (the Section 9.6 input).
    if session.muted && forced.is_none() {
        bump_counter(store, chat_id, "wakes_total").await;
        reset_wake(config, session, wake, rng, now);
        persist_session(store, chat_id, session).await?;
        log_wake_line(chat_id, trigger, 0, "muted", None, "nothing", None);
        return Ok(());
    }

    // Step 2 (Section 9.6): the new messages of this wake are the
    // inbound raw-log rows above `wake_last_row_id`, rendered with the
    // same helper as the live context so the gate input stays
    // consistent with what the reply model sees.
    let after_id = session.wake_last_row_id;
    let gather_chat_id = chat_id.to_string();
    let rows = blocking_store(store, move |store| {
        store.list_messages_after(&gather_chat_id, after_id)
    })
    .await?;
    // The tail row of ANY direction (the bot's own rows included)
    // becomes the new `wake_last_row_id`; an empty range keeps the old
    // value.
    let tail_id = rows.last().map(|row| row.id).unwrap_or(after_id);
    // Rules A3/B1 + Rule P1 bit-identity: resolve the reply targets of
    // the new inbound rows with the SAME store function the intake
    // path uses (one blocking pass over the unique target platform
    // ids), so the gate messages render byte-identically to the
    // context items of the same rows.
    let reply_targets = resolve_reply_targets(store, chat_id, &rows).await?;
    let new_messages: Vec<GateMessage> = rows
        .iter()
        .filter(|row| row.direction == Direction::Inbound)
        .map(|row| GateMessage {
            row_id: row.id,
            platform_msg_id: row.platform_msg_id.clone(),
            content: render_human_content(
                row.id,
                &row.sender_display_name,
                row.sender_username.as_deref(),
                row.timestamp,
                row.event_type == EventType::Edit,
                row.mentions_bot,
                reply_render_from_row(row, &reply_targets),
                &row.text,
            ),
            // The recall worker needs the raw fields for deterministic
            // candidate extraction (proposed-graph-database-specs.md
            // Section 8.1 step 1); the gate prompt keeps `content`.
            sender_id: row.sender_id.clone(),
            reply_to_platform_msg_id: row.reply_to_platform_msg_id.clone(),
            text: row.text.clone(),
        })
        .collect();

    // Step 3, documented decision: the resets of spec steps 1 and 5
    // collapse into ONE reset at wake START, so messages that arrive
    // during a running wake count toward the next wake instead of being
    // zeroed at completion (Section 6.2). The counter is best effort: a
    // counter failure logs and never aborts the wake.
    reset_wake(config, session, wake, rng, now);
    session.wake_last_row_id = tail_id;
    bump_counter(store, chat_id, "wakes_total").await;
    persist_session(store, chat_id, session).await?;

    // Step 4: Section 9.6 decides over the new messages; none exist
    // here. An interval wake over a silent group must not burn an LLM
    // call. The counter/timer reset already happened in step 3.
    if forced.is_none() && new_messages.is_empty() {
        log_wake_line(chat_id, trigger, 0, "silent", None, "nothing", None);
        return Ok(());
    }

    // Step 5: the LLM calls run in a spawned task (the Section 6.1
    // rule 3 analog). The context is actor-owned (Section 6.1 rule 2),
    // so the task gets a SNAPSHOT taken NOW; it touches NO actor state
    // at all. Inbound messages during the call are logged and appended;
    // they do not interrupt it (Section 6.2).
    let snapshot = context.messages_for_llm();
    *wake_in_flight = true;
    let recall = Arc::clone(&services.recall);
    let gate = Arc::clone(&services.gate);
    let reply = Arc::clone(&services.reply);
    let task_chat_id = chat_id.to_string();
    let sender = inbox_sender.clone();
    tokio::spawn(async move {
        let result = run_wake_calls(
            &task_chat_id,
            recall,
            gate,
            reply,
            new_messages,
            snapshot,
            forced,
            tail_id,
            trigger,
        )
        .await;
        // A failed send means the actor is shutting down; the result is
        // dropped (same rule as the digest task).
        let _ = sender.send(ActorCommand::WakeCompleted(result)).await;
    });
    Ok(())
}

/// The LLM calls of one wake: recall (step 2), the participation
/// decision (step 3), and the reply generation (step 4). Runs in a
/// spawned task over owned data; touches NO actor state (specs.md
/// Section 6.1, rule 2). `injection_position` is the tail raw-log row
/// id computed at wake start; the completion handler uses it as the
/// Rule C2 position of the injections. `trigger` is the telemetry
/// spelling of what started this wake; it passes through to the report
/// for the curated wake log line.
#[allow(clippy::too_many_arguments)]
async fn run_wake_calls(
    chat_id: &str,
    recall: Arc<dyn RecallProvider>,
    gate: Arc<dyn ParticipationGate>,
    reply: Arc<dyn ReplyGenerator>,
    new_messages: Vec<GateMessage>,
    mut snapshot: Vec<ContextMessage>,
    forced: Option<GateMessage>,
    injection_position: i64,
    trigger: &'static str,
) -> Result<WakeReport, CoreError> {
    // Step 2 (Sections 9.1-9.5): recall before the gate. The rendered
    // injection texts enter the gate input (Section 9.6: the recall
    // result is gate input on purpose) AND the reply-model snapshot:
    // the injection is part of the context from step 2 on, so the
    // reply model of step 4 sees it (Section 9.4, Rule C2 tail
    // position). The dedup rows and the context append happen in the
    // completion handler (Section 9.3), regardless of the gate
    // outcome.
    let recall_outcome = recall.recall(chat_id, &new_messages).await?;
    let injections = recall_outcome.injections;
    let injection_texts: Vec<String> = injections
        .iter()
        .map(|injection| injection.content.clone())
        .collect();
    for text in &injection_texts {
        snapshot.push(ContextMessage {
            role: ContextRole::Assistant,
            content: text.clone(),
        });
    }
    let forced_flag = forced.is_some();
    // Step 3 (Section 9.6). Forced wakes BYPASS the gate (Section 8.1):
    // `decide` is never called for them.
    let decision = match &forced {
        Some(forcing) => GateDecision {
            participate: true,
            target_row_id: Some(forcing.row_id),
            // A forced wake never consulted the gate: no reason string.
            reason: None,
        },
        None => {
            gate.decide(&GateInput {
                new_messages: new_messages.clone(),
                injections: injection_texts,
                forced: false,
            })
            .await?
        }
    };
    // The telemetry of the curated wake log line: the forced path
    // reports the bypass; an unforced wake reports the decision and
    // carries the gate's own reason string.
    let (gate_outcome, gate_reason) = if forced_flag {
        ("bypassed_forced", None)
    } else {
        (
            if decision.participate {
                "participate"
            } else {
                "silent"
            },
            decision.reason.clone(),
        )
    };
    // Target resolution. An id outside the presented set is treated as
    // no-participation (the gate named a message the wake never saw).
    let target = if !decision.participate {
        None
    } else if let Some(forcing) = &forced {
        Some(forcing.clone())
    } else {
        let resolved = decision
            .target_row_id
            .and_then(|row_id| new_messages.iter().find(|msg| msg.row_id == row_id))
            .cloned();
        if resolved.is_none() {
            debug!(chat_id = %chat_id, "the gate named a target outside the presented set; treated as no-participation");
        }
        resolved
    };
    // Step 4: the reply generation with the reply model over the context
    // snapshot. Skipped when the decision is no-participation.
    let reply_text = match &target {
        Some(target) => {
            let raw_text = reply
                .generate(&ReplyRequest {
                    messages: snapshot,
                    target: target.clone(),
                })
                .await?;
            // Decision 59, F1: the parrot filter guards EVERY reply
            // text here, so no generator (the live one included, and
            // every scripted double of tests) can put a confabulated
            // "I remember: ..." line into the WakeReport. The filtered
            // text is what the completion handler persists (Rule B1)
            // and sends: the log and the group see the same text.
            let filtered = filter_reply_parrot_lines(&raw_text);
            if filtered.stripped_parrot {
                tracing::warn!(chat_id = %chat_id, "the parrot filter stripped recall-injection echo lines from the reply");
            }
            if filtered.text.is_empty() {
                // Nothing remains: the SAME wake error as an empty
                // reply today — nothing persisted, nothing sent.
                return Err(CoreError::Wake(
                    "the reply model returned an empty reply".to_string(),
                ));
            }
            Some(filtered.text)
        }
        None => None,
    };
    Ok(WakeReport {
        forced: forced_flag,
        target,
        reply_text,
        injections,
        injection_position,
        trigger,
        gate: gate_outcome,
        gate_reason,
    })
}

/// The completion side of the wake procedure (specs.md Section 9 step 4
/// send path). Runs inside the actor loop on `WakeCompleted(Ok(..))`.
/// Applies the recall injections FIRST (Section 9 step 2): a gate-no
/// wake still injects — the memory was genuinely remembered; a silent
/// pet can still remember.
#[allow(clippy::too_many_arguments)]
async fn handle_wake_report(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    context: &mut LiveContext,
    outbound: Option<&mpsc::Sender<OutboundAction>>,
    bot_name: &str,
    report: WakeReport,
) -> Result<(), CoreError> {
    // The injection lifecycle of Section 9. The injections were planned
    // in step 2 (recall), BEFORE the participation decision, so they
    // are applied here regardless of the gate outcome.
    let position = report.injection_position;
    // The range tag string is uniform with the context item tags
    // (RangeTag::single(position)).
    let range_tag = RangeTag::single(position).as_string();
    for injection in &report.injections {
        // Section 9.3: the dedup is per edge id — one `injected_memories`
        // row per injected edge (specs.md Section 5.2: one row per
        // injected recall).
        for edge_id in &injection.edge_ids {
            let insert_chat_id = chat_id.to_string();
            let edge_id_owned = edge_id.clone();
            let row_range_tag = range_tag.clone();
            let content = injection.content.clone();
            let result = blocking_store(store, move |store| {
                store.insert_injected_memory(
                    &insert_chat_id,
                    &edge_id_owned,
                    position,
                    &row_range_tag,
                    &content,
                )
            })
            .await;
            if let Err(error) = result {
                // The same tolerance as the send path: an insert failure
                // logs an error and continues with the next injection;
                // it is never fatal.
                tracing::error!(chat_id = %chat_id, %error, edge_id, "failed to record an injected memory; continuing");
            }
        }
        // Rule C2: the injection is appended at the tail, directly
        // after the messages that triggered it.
        context.append_recall_injection(position, injection.content.clone());
    }
    if !report.injections.is_empty() {
        // Section 12: the injection-rate metric — wakes with at least
        // one injection. Best effort like the other counters.
        bump_counter(store, chat_id, "injection_wakes_total").await;
    }
    let (Some(target), Some(text)) = (report.target, report.reply_text) else {
        // The gate said no: nothing is sent. Only `wakes_total` (and
        // possibly `injection_wakes_total` above) was counted.
        log_wake_line(
            chat_id,
            report.trigger,
            report.injections.len(),
            report.gate,
            report.gate_reason.as_deref(),
            "nothing",
            None,
        );
        return Ok(());
    };
    // The recency re-check (Section 6.2): when too many newer human
    // messages arrived after the target, DISCARD the reply (documented
    // decision: discard, NOT regenerate — the next wake is the natural
    // retry).
    let recheck_chat_id = chat_id.to_string();
    let target_row_id = target.row_id;
    let newer = blocking_store(store, move |store| {
        store.count_inbound_after(&recheck_chat_id, target_row_id)
    })
    .await?;
    if newer > config.reply_staleness_threshold {
        // The detail fields of the discard stay at debug; the curated
        // line below is the one info line of this wake.
        debug!(
            chat_id = %chat_id,
            newer,
            threshold = config.reply_staleness_threshold,
            "wake reply discarded: the conversation moved on"
        );
        log_wake_line(
            chat_id,
            report.trigger,
            report.injections.len(),
            report.gate,
            report.gate_reason.as_deref(),
            "discarded_stale",
            None,
        );
        return Ok(());
    }
    // a. Rules B1/P1: persist the outbound raw-log row FIRST — the log
    // is the source of truth; never speak without logging. The
    // synthetic id: the adapter contract (Rule A3) returns no platform
    // id for a sent message, so the row carries a local synthetic id;
    // nanosecond time keeps the idempotency key unique.
    let now = OffsetDateTime::now_utc();
    let row = NewMessage {
        platform_msg_id: format!("bot-out:{}", now.unix_timestamp_nanos()),
        direction: Direction::Outbound,
        event_type: EventType::Message,
        timestamp: now,
        sender_id: "bot".to_string(),
        sender_display_name: bot_name.to_string(),
        sender_username: None,
        text: text.clone(),
        reply_to_platform_msg_id: Some(target.platform_msg_id.clone()),
        mentions_bot: false,
        is_reply_to_bot: false,
    };
    let insert_chat_id = chat_id.to_string();
    let outcome = blocking_store(store, move |store| {
        store.insert_message(&insert_chat_id, &row)
    })
    .await;
    let row_id = match outcome {
        Ok(InsertOutcome::Inserted(row_id)) => row_id,
        // An insert failure (or an impossible duplicate of the synthetic
        // id) aborts this wake's send path with an error log. The error
        // is NOT propagated: the actor must not die over one reply.
        Ok(InsertOutcome::Duplicate) => {
            tracing::error!(chat_id = %chat_id, "outbound raw-log insert returned Duplicate; the reply is not sent");
            return Ok(());
        }
        Err(error) => {
            tracing::error!(chat_id = %chat_id, %error, "failed to persist the outbound raw-log row; the reply is not sent");
            return Ok(());
        }
    };
    // b. The outbound action (Section 4.2: outbound failures are
    // tolerated; M3 handles the platform-level ones). `try_send`: the
    // actor never blocks on the sink; a full or closed channel degrades
    // to a logged drop — the raw-log row above is already the source of
    // truth.
    let action = OutboundAction::SendText {
        chat_id: chat_id.to_string(),
        text: text.clone(),
        reply_to_platform_msg_id: Some(target.platform_msg_id.clone()),
    };
    match outbound {
        Some(sink) => {
            if let Err(error) = sink.try_send(action) {
                tracing::error!(chat_id = %chat_id, %error, "outbound action dropped (channel full or closed); the raw-log row is persisted");
            }
        }
        None => debug!(chat_id = %chat_id, "outbound action dropped: no outbound sink wired"),
    }
    // c. The live context gets the same item (Rule C1).
    context.append_bot_speech(row_id, now, &text);
    // d. The Section 8.5 monologue lock, wired for live speech.
    session.record_bot_message(config);
    // e. Best-effort counter + the session persist of Section 6.1
    // rule 4.
    bump_counter(store, chat_id, "participations_total").await;
    persist_session(store, chat_id, session).await?;
    // The curated line of this wake: the reply went out.
    log_wake_line(
        chat_id,
        report.trigger,
        report.injections.len(),
        report.gate,
        report.gate_reason.as_deref(),
        "reply_sent",
        Some(&target.platform_msg_id),
    );
    Ok(())
}

/// Resets the wake scheduler and mirrors the result into the session.
/// The fresh interval is normalized to whole milliseconds so the persisted
/// encoding stays lossless (refer to `round_to_millis`).
fn reset_wake(
    config: &TriggerConfig,
    session: &mut SessionState,
    wake: &mut WakeScheduler,
    rng: &mut StdRng,
    now: OffsetDateTime,
) {
    wake.reset(now, config, rng);
    let mut snapshot = wake.snapshot();
    snapshot.current_interval = round_to_millis(snapshot.current_interval);
    *wake = WakeScheduler::from_state(snapshot.clone());
    session.wake = snapshot;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use tamako_memory::MemoryBatch;
    use tempfile::TempDir;

    use crate::context::{render_bot_content, ContextItemKind, ContextRole, RangeTag};
    use crate::summary::ScriptedSummary;

    /// A memory backend double. All calls succeed; `ensure_schema` records
    /// the chat_id values it receives.
    struct NoopMemory {
        ensured: Mutex<Vec<String>>,
    }

    impl NoopMemory {
        fn new() -> Self {
            Self {
                ensured: Mutex::new(Vec::new()),
            }
        }

        fn ensured_chat_ids(&self) -> Vec<String> {
            self.ensured
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl MemoryBackend for NoopMemory {
        async fn ensure_schema(&self, chat_id: &str) -> tamako_memory::Result<()> {
            self.ensured
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(chat_id.to_string());
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
            // Records nothing; an empty result means the alias is unknown
            // (Section 7.4 step 2).
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

    const CHAT_ID: &str = "-1001234567890";

    /// The preamble of every actor spawned in this module.
    const TEST_PREAMBLE: &str = "You are Tamako, a test pet.";

    /// A scripted digest pipeline for the M2 context tests. It is
    /// store-backed: `run_digest` lists the tail above the given
    /// boundary and returns an `Extracted` outcome whose new boundary
    /// is the last row id. An empty tail returns `Ok(None)`.
    struct ScriptedDigest {
        store: Arc<Store>,
    }

    impl DigestPipeline for ScriptedDigest {
        fn run_digest<'a>(
            &'a self,
            chat_id: &'a str,
            last_digest_boundary_msg_id: i64,
        ) -> Pin<Box<dyn Future<Output = Result<Option<DigestOutcome>, CoreError>> + Send + 'a>>
        {
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
                Ok(Some(DigestOutcome::Extracted {
                    batch_id: format!("batch-{}", last.id),
                    new_boundary: last.id,
                    node_count: 0,
                    edge_count: 0,
                }))
            })
        }
    }

    /// The trigger config of the digest tests: the trigger fires every
    /// two messages (specs.md Section 8.2).
    fn digest_config() -> TriggerConfig {
        TriggerConfig {
            digest_max_messages: 2,
            ..TriggerConfig::default()
        }
    }

    /// Polls `snapshot()` until the digest boundary reaches `min` or the
    /// 5 s deadline passes (the pattern of tamako/tests/digest_replay.rs).
    /// The snapshot is a FIFO barrier, and the spawned digest task reports
    /// through the inbox: a snapshot that shows the boundary proves the
    /// whole `DigestCompleted` handler already ran.
    async fn wait_for_boundary(handle: &GroupActorHandle, min: i64) -> i64 {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let session = handle.snapshot().await.expect("snapshot succeeds");
            if session.last_digest_boundary_msg_id >= min {
                return session.last_digest_boundary_msg_id;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for the digest boundary to reach {min} \
                 (current boundary: {})",
                session.last_digest_boundary_msg_id
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Runs one blocking store call, like the actor does (AGENT.md
    /// Section 6.2).
    async fn blocking_store_call<T, F>(store: &Arc<Store>, call: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(Arc<Store>) -> Result<T, StoreError> + Send + 'static,
    {
        let store = Arc::clone(store);
        tokio::task::spawn_blocking(move || call(store))
            .await
            .expect("the blocking task joins")
            .expect("the store call succeeds")
    }

    /// Fixed base time for deterministic replay.
    fn t0() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
    }

    fn message(id: &str, seconds_after_t0: i64, mentions_bot: bool) -> NormalizedMessage {
        NormalizedMessage {
            platform_msg_id: id.to_string(),
            timestamp: t0() + time::Duration::seconds(seconds_after_t0),
            sender_id: "u1".to_string(),
            sender_display_name: "Alice".to_string(),
            username: None,
            text: format!("text of {id}"),
            reply_to_platform_msg_id: None,
            mentions_bot,
            is_reply_to_bot: false,
        }
    }

    /// A message by a second sender ("Bob") that replies to
    /// `reply_to` (any platform id — real, synthetic, or unknown) and
    /// optionally to the bot.
    fn reply_message(
        id: &str,
        seconds_after_t0: i64,
        reply_to: Option<&str>,
        is_reply_to_bot: bool,
    ) -> NormalizedMessage {
        NormalizedMessage {
            platform_msg_id: id.to_string(),
            timestamp: t0() + time::Duration::seconds(seconds_after_t0),
            sender_id: "u2".to_string(),
            sender_display_name: "Bob".to_string(),
            username: None,
            text: format!("text of {id}"),
            reply_to_platform_msg_id: reply_to.map(str::to_string),
            mentions_bot: false,
            is_reply_to_bot,
        }
    }

    /// A named reaction event on message `platform_msg_id` (reactor
    /// identity known, one second after t0).
    fn reaction(platform_msg_id: &str) -> ReactionEvent {
        ReactionEvent {
            platform_msg_id: platform_msg_id.to_string(),
            timestamp: t0() + time::Duration::seconds(1),
            reactor_id: Some("u1".to_string()),
            anonymous: false,
            aggregated: false,
            old_emojis: vec![],
            new_emojis: vec!["👍".to_string()],
        }
    }

    struct Fixture {
        // The TempDir must outlive the store.
        _dir: TempDir,
        store: Arc<Store>,
        memory: Arc<NoopMemory>,
    }

    fn make_fixture() -> Fixture {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let store = Arc::new(Store::new(dir.path().join("data")));
        let memory = Arc::new(NoopMemory::new());
        Fixture {
            _dir: dir,
            store,
            memory,
        }
    }

    fn spawn_on(fixture: &Fixture, config: TriggerConfig) -> GroupActorHandle {
        spawn_group_actor(GroupActorParams {
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::clone(&fixture.memory),
            config,
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            preamble: TEST_PREAMBLE.to_string(),
            digest: None,
            post_digest_hook: None,
            wake: None,
            summary_provider: None,
            outbound: None,
            bot_name: None,
        })
    }

    /// Spawns an actor with the scripted digest pipeline of the M2
    /// context tests.
    fn spawn_with_scripted_digest(fixture: &Fixture, config: TriggerConfig) -> GroupActorHandle {
        let digest = Arc::new(ScriptedDigest {
            store: Arc::clone(&fixture.store),
        });
        spawn_group_actor(GroupActorParams {
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::clone(&fixture.memory),
            config,
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            preamble: TEST_PREAMBLE.to_string(),
            digest: Some(digest),
            post_digest_hook: None,
            wake: None,
            summary_provider: None,
            outbound: None,
            bot_name: None,
        })
    }

    fn spawn_fixture(config: TriggerConfig) -> (Fixture, GroupActorHandle) {
        let fixture = make_fixture();
        let handle = spawn_on(&fixture, config);
        (fixture, handle)
    }

    /// Lists the raw log through a blocking call, like the actor does.
    async fn list_messages(store: &Arc<Store>) -> Vec<tamako_store::MessageRow> {
        let store = Arc::clone(store);
        tokio::task::spawn_blocking(move || store.list_messages(CHAT_ID))
            .await
            .expect("the blocking task joins")
            .expect("list_messages succeeds")
    }

    /// Lists the reaction rows through a blocking call, like the actor
    /// does.
    async fn list_reactions(store: &Arc<Store>) -> Vec<tamako_store::ReactionRow> {
        let store = Arc::clone(store);
        tokio::task::spawn_blocking(move || store.list_reactions(CHAT_ID))
            .await
            .expect("the blocking task joins")
            .expect("list_reactions succeeds")
    }

    #[tokio::test]
    async fn intake_persists_the_message_before_anything_else() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m2", 2, false)))
            .await
            .expect("send succeeds");
        // A snapshot is a FIFO barrier: when it returns, both messages are
        // processed.
        handle.snapshot().await.expect("snapshot succeeds");

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].platform_msg_id, "m1");
        assert_eq!(rows[1].platform_msg_id, "m2");
        for row in &rows {
            assert_eq!(row.direction, Direction::Inbound);
            assert_eq!(row.event_type, EventType::Message);
        }
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn duplicate_delivery_does_not_double_count() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        for _ in 0..2 {
            handle
                .send_event(InboundEvent::Message(message("m1", 1, false)))
                .await
                .expect("send succeeds");
        }
        let session = handle.snapshot().await.expect("snapshot succeeds");

        // Idempotent intake: the raw log holds ONE row.
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        // Documented choice: the wake counter reflects both deliveries. The
        // counter is a scheduling hint; the log row is the source of truth.
        assert_eq!(session.wake.msgs_since_wake, 2);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn edit_appends_a_new_row() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        let mut edited = message("m1", 1, false);
        edited.text = "edited text".to_string();
        handle
            .send_event(InboundEvent::EditedMessage(edited))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].event_type, EventType::Message);
        assert_eq!(rows[1].event_type, EventType::Edit);
        assert_eq!(rows[1].text, "edited text");
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn restart_rebuilds_identical_state() {
        // The Phase 0 exit criterion (dev-roadmap.md Section 2).
        let config = TriggerConfig::default();
        let (fixture, handle) = spawn_fixture(config.clone());
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        let first = handle.snapshot().await.expect("snapshot succeeds");
        handle.shutdown().await.expect("shutdown succeeds");

        // A NEW actor on the same store and the same started_at.
        let restarted = spawn_group_actor(GroupActorParams {
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::new(NoopMemory::new()),
            config,
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            preamble: TEST_PREAMBLE.to_string(),
            digest: None,
            post_digest_hook: None,
            wake: None,
            summary_provider: None,
            outbound: None,
            bot_name: None,
        });
        let rebuilt = restarted.snapshot().await.expect("snapshot succeeds");
        assert_eq!(first, rebuilt);

        // The rebuilt actor keeps working on the same log.
        for index in 4..=5 {
            restarted
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        let advanced = restarted.snapshot().await.expect("snapshot succeeds");
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 5);
        assert!(advanced.wake.msgs_since_wake > rebuilt.wake.msgs_since_wake);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn forced_wake_does_not_panic_when_muted() {
        let config = TriggerConfig {
            monologue_limit: 2,
            ..TriggerConfig::default()
        };
        let fixture = make_fixture();

        // Craft the muted state: monologue_limit consecutive bot messages
        // engage the monologue lock (specs.md Section 8.5). Seed the store
        // BEFORE the actor spawns: a concurrent open_group of the same
        // store.db races the WAL pragma of the actor startup.
        let mut rng = StdRng::seed_from_u64(23);
        let mut seeded = SessionState::new(&config, t0(), &mut rng);
        seeded.record_bot_message(&config);
        seeded.record_bot_message(&config);
        assert!(seeded.muted);
        let pairs = seeded.encode();
        let store = Arc::clone(&fixture.store);
        tokio::task::spawn_blocking(move || {
            store.open_group(CHAT_ID)?;
            store.set_state_many(CHAT_ID, &pairs)
        })
        .await
        .expect("the blocking task joins")
        .expect("seeding succeeds");

        // The actor rebuilds the seeded (muted) state at startup.
        let handle = spawn_on(&fixture, config.clone());

        // A direct mention requests a forced wake. specs.md Section 8.1:
        // the muted state does not suppress a forced wake.
        handle
            .send_event(InboundEvent::Message(message("m1", 1, true)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");

        // The human message clears the monologue lock (Section 8.5), and
        // the session is persisted.
        assert!(!session.muted);
        let persisted = {
            let store = Arc::clone(&fixture.store);
            tokio::task::spawn_blocking(move || store.load_all_state(CHAT_ID))
                .await
                .expect("the blocking task joins")
                .expect("load_all_state succeeds")
        };
        let persisted_map: std::collections::HashMap<String, String> =
            session.encode().into_iter().collect();
        assert_eq!(persisted, persisted_map);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn tick_fires_the_timer() {
        let config = TriggerConfig {
            wake_interval: std::time::Duration::from_millis(10),
            wake_floor: std::time::Duration::ZERO,
            ..TriggerConfig::default()
        };
        let (_fixture, handle) = spawn_fixture(config);
        handle
            .send(ActorCommand::Tick(t0() + time::Duration::hours(1)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");

        assert_eq!(session.wake.msgs_since_wake, 0);
        assert_eq!(session.wake.last_wake_at, t0() + time::Duration::hours(1));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn startup_ensures_the_memory_schema_for_the_group() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        // The snapshot is a FIFO barrier: startup completed before it.
        handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(fixture.memory.ensured_chat_ids(), vec![CHAT_ID.to_string()]);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn intake_appends_human_messages_to_the_context() {
        // Rule C1: every new raw-log row enters the live context.
        let (_fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m2", 2, false)))
            .await
            .expect("send succeeds");
        // A FIFO barrier: when it returns, both messages are processed.
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        assert_eq!(items.len(), 3);
        let preamble = &items[0];
        assert_eq!(preamble.kind, ContextItemKind::Preamble);
        assert_eq!(preamble.role, ContextRole::System);
        assert_eq!(preamble.content, TEST_PREAMBLE);
        assert_eq!(preamble.range_tag, None);

        let first = &items[1];
        assert_eq!(first.kind, ContextItemKind::HumanMessage);
        assert_eq!(first.role, ContextRole::User);
        assert_eq!(
            first.content,
            r#"<msg from="Alice" at="22:13" id="1">text of m1</msg>"#
        );
        assert_eq!(first.range_tag, Some(RangeTag::single(1)));

        let second = &items[2];
        assert_eq!(second.kind, ContextItemKind::HumanMessage);
        assert_eq!(second.role, ContextRole::User);
        assert_eq!(
            second.content,
            r#"<msg from="Alice" at="22:13" id="2">text of m2</msg>"#
        );
        assert_eq!(second.range_tag, Some(RangeTag::single(2)));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn duplicate_delivery_does_not_append_twice() {
        // The log row is not duplicated; the view must not duplicate
        // either (Rule P1).
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        for _ in 0..2 {
            handle
                .send_event(InboundEvent::Message(message("m1", 1, false)))
                .await
                .expect("send succeeds");
        }
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(items.len(), 2);
        assert_eq!(items[1].kind, ContextItemKind::HumanMessage);
        assert_eq!(items[1].range_tag, Some(RangeTag::single(1)));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn edit_appends_a_context_item() {
        // specs.md Section 15, open item 4: an edit is just a new row.
        let (_fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        let mut edited = message("m1", 1, false);
        edited.text = "edited text".to_string();
        handle
            .send_event(InboundEvent::EditedMessage(edited))
            .await
            .expect("send succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        assert_eq!(items.len(), 3);
        assert_eq!(
            items[1].content,
            r#"<msg from="Alice" at="22:13" id="1">text of m1</msg>"#
        );
        assert_eq!(items[1].range_tag, Some(RangeTag::single(1)));
        assert_eq!(
            items[2].content,
            r#"<msg from="Alice" at="22:13" id="2" kind="edit">edited text</msg>"#
        );
        assert_eq!(items[2].range_tag, Some(RangeTag::single(2)));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn restart_rebuilds_a_bit_identical_context() {
        // Rule P1: the startup rebuild from the persisted rows is
        // bit-identical to the pre-restart context.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        handle.snapshot().await.expect("snapshot succeeds");
        // M5 shallow recall inserts injection rows through the actor; the
        // test drives the store directly. The actor owns the live
        // context, so the injected row becomes visible only through the
        // rebuild of a NEW actor below.
        blocking_store_call(&fixture.store, move |store| {
            store.insert_injected_memory(
                CHAT_ID,
                "edge-1",
                2,
                "1-2",
                "I remember: Alice likes GRPO",
            )
        })
        .await;
        handle.shutdown().await.expect("shutdown succeeds");

        let restarted = spawn_on(&fixture, TriggerConfig::default());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        // The hand-computed rebuild over the same persisted rows. The
        // rows of this test carry no reply targets, so the empty map
        // matches them. The summaries query mirrors the startup rebuild
        // (same store function; the fixture holds no summary rows yet —
        // S2b lands the digest-completion writer).
        let (rows, injections, summaries) = blocking_store_call(&fixture.store, move |store| {
            let rows = store.list_messages_after(CHAT_ID, 0)?;
            let injections = store.list_injected_memories(CHAT_ID)?;
            let summaries = store.list_newest_context_summaries(CHAT_ID, 2)?;
            Ok((rows, injections, summaries))
        })
        .await;
        let expected = LiveContext::rebuild(
            TEST_PREAMBLE.to_string(),
            &rows,
            &injections,
            &HashMap::new(),
            &summaries,
        );
        assert_eq!(rebuilt, expected.items());

        // Rule C2 placement: the injection sits directly after row 2.
        assert_eq!(rebuilt.len(), 5);
        assert_eq!(rebuilt[2].kind, ContextItemKind::HumanMessage);
        assert_eq!(rebuilt[2].range_tag, Some(RangeTag::single(2)));
        assert_eq!(rebuilt[3].kind, ContextItemKind::RecallInjection);
        assert_eq!(rebuilt[3].content, "I remember: Alice likes GRPO");
        assert_eq!(rebuilt[3].range_tag, Some(RangeTag::single(2)));
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn digest_removes_the_previous_chunk_with_one_chunk_lag() {
        // Rule C3, specs.md Section 7.1: at a digest only the chunk at or
        // below the PREVIOUS boundary leaves the context.
        let fixture = make_fixture();
        let handle = spawn_with_scripted_digest(&fixture, digest_config());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m2", 2, false)))
            .await
            .expect("send succeeds");
        // Digest 1: the boundary advances 0 -> 2.
        wait_for_boundary(&handle, 2).await;
        // Nothing is at or below the old boundary 0: the full tail stays.
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 3);
        assert_eq!(items[1].range_tag, Some(RangeTag::single(1)));
        assert_eq!(items[2].range_tag, Some(RangeTag::single(2)));
        // The FIRST completed digest has no previous chunk: prev is None.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.prev_digest_boundary_msg_id, None);

        handle
            .send_event(InboundEvent::Message(message("m3", 3, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m4", 4, false)))
            .await
            .expect("send succeeds");
        // Digest 2: the boundary advances 2 -> 4.
        wait_for_boundary(&handle, 4).await;
        // Chunk (0, 2] leaves; chunk (2, 4] stays as the overlap buffer.
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].kind, ContextItemKind::Preamble);
        assert_eq!(items[1].range_tag, Some(RangeTag::single(3)));
        assert_eq!(
            items[1].content,
            r#"<msg from="Alice" at="22:13" id="3">text of m3</msg>"#
        );
        assert_eq!(items[2].range_tag, Some(RangeTag::single(4)));
        assert_eq!(
            items[2].content,
            r#"<msg from="Alice" at="22:13" id="4">text of m4</msg>"#
        );
        // From the second digest on, prev is the boundary that was
        // current before this digest.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.prev_digest_boundary_msg_id, Some(2));
        assert_eq!(session.last_digest_boundary_msg_id, 4);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn digest_prunes_injected_memories_at_or_below_the_previous_boundary() {
        // specs.md Section 10.2 step 4: the dedup set is pruned at the
        // same cutoff as the Rule C3 context removal.
        let fixture = make_fixture();
        let handle = spawn_with_scripted_digest(&fixture, digest_config());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m2", 2, false)))
            .await
            .expect("send succeeds");
        wait_for_boundary(&handle, 2).await;

        // Injections at the boundary (2) and above it (3).
        blocking_store_call(&fixture.store, move |store| {
            store.insert_injected_memory(CHAT_ID, "edge-1", 2, "1-2", "memory at 2")?;
            store.insert_injected_memory(CHAT_ID, "edge-2", 3, "1-3", "memory at 3")?;
            Ok(())
        })
        .await;

        handle
            .send_event(InboundEvent::Message(message("m3", 3, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m4", 4, false)))
            .await
            .expect("send succeeds");
        // Digest 2 (boundary 4) prunes at the previous boundary 2.
        wait_for_boundary(&handle, 4).await;

        let injections = blocking_store_call(&fixture.store, move |store| {
            store.list_injected_memories(CHAT_ID)
        })
        .await;
        let positions: Vec<i64> = injections
            .iter()
            .map(|row| row.injection_position)
            .collect();
        assert_eq!(positions, vec![3]);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn restart_after_digest_rebuilds_the_lagged_view() {
        // Rule P1 after the Rule C3 removal: the startup rebuild is
        // bit-identical to the pre-shutdown (already lagged) context.
        let fixture = make_fixture();
        let handle = spawn_with_scripted_digest(&fixture, digest_config());
        // Two digests, driven in steps: the scripted pipeline reads the
        // live store, so the second pair goes in only after digest 1
        // completed (otherwise digest 1 could swallow the whole tail).
        for index in 1..=2 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        wait_for_boundary(&handle, 2).await;
        for index in 3..=4 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        // Boundary 0 -> 2 -> 4.
        wait_for_boundary(&handle, 4).await;
        let before = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        handle.shutdown().await.expect("shutdown succeeds");

        let restarted = spawn_with_scripted_digest(&fixture, digest_config());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(rebuilt, before);

        // The rebuilt session keeps the lagged boundaries.
        let session = restarted.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.prev_digest_boundary_msg_id, Some(2));
        assert_eq!(session.last_digest_boundary_msg_id, 4);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    // --- Segmented summarization tests (decision 62) ---

    /// Spawns an actor with the scripted digest pipeline AND a scripted
    /// summarizer.
    fn spawn_with_digest_and_summary(
        fixture: &Fixture,
        config: TriggerConfig,
        summary: Arc<dyn SummaryProvider>,
    ) -> GroupActorHandle {
        let digest = Arc::new(ScriptedDigest {
            store: Arc::clone(&fixture.store),
        });
        spawn_group_actor(GroupActorParams {
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::clone(&fixture.memory),
            config,
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            preamble: TEST_PREAMBLE.to_string(),
            digest: Some(digest),
            post_digest_hook: None,
            wake: None,
            summary_provider: Some(summary),
            outbound: None,
            bot_name: None,
        })
    }

    /// Sends one pair of messages (one digest batch of digest_config).
    async fn send_pair(handle: &GroupActorHandle, first_index: i64) {
        for index in first_index..first_index + 2 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
    }

    #[tokio::test]
    async fn the_second_digest_summarizes_the_removed_chunk_before_its_removal() {
        // Decision 62: the chunk that Rule C3 removes is summarized
        // BEFORE the removal; the summary row persists first (Rule P1).
        // The FIRST digest completion removes nothing and never calls
        // the summarizer.
        let fixture = make_fixture();
        let summary = Arc::new(ScriptedSummary::with_summaries(vec![
            "Alice planned a hike.".to_string(),
        ]));
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary.clone());

        send_pair(&handle, 1).await;
        // Digest 1: the boundary advances 0 -> 2. b_old == 0: no
        // removal, no summarization.
        wait_for_boundary(&handle, 2).await;
        assert!(summary.inputs().is_empty());

        send_pair(&handle, 3).await;
        // Digest 2: the chunk (0, 2] leaves the context; its summary
        // enters directly after the preamble.
        wait_for_boundary(&handle, 4).await;

        // The summarizer saw exactly the raw-log rows of the range.
        let inputs = summary.inputs();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].first_msg_id, 0);
        assert_eq!(inputs[0].last_msg_id, 2);
        let row_ids: Vec<i64> = inputs[0].rows.iter().map(|row| row.id).collect();
        assert_eq!(row_ids, vec![1, 2]);

        // Persisted BEFORE the removal: the row exists after the
        // completion, and the raw chunk items are gone from the context.
        let row = blocking_store_call(&fixture.store, move |store| {
            store.find_context_summary(CHAT_ID, 0, 2)
        })
        .await
        .expect("the summary row exists");
        assert_eq!(row.content, "Alice planned a hike.");

        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].kind, ContextItemKind::Preamble);
        assert_eq!(items[1].kind, ContextItemKind::Summary);
        assert_eq!(items[1].role, ContextRole::User);
        assert_eq!(
            items[1].content,
            r#"<summary range="0-2">Alice planned a hike.</summary>"#
        );
        assert_eq!(
            items[1].range_tag,
            Some(RangeTag {
                first_msg_id: 0,
                last_msg_id: 2
            })
        );
        // The previous chunk (2, 4] stays RAW (the one-chunk lag is
        // unchanged).
        assert_eq!(items[2].range_tag, Some(RangeTag::single(3)));
        assert_eq!(items[3].range_tag, Some(RangeTag::single(4)));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_context_keeps_the_two_newest_summaries() {
        // Decision 62: keep-two retention. Three summarizing digest
        // completions; the context holds exactly the two newest
        // summaries (oldest first, after the preamble, before the raw
        // previous chunk); the store keeps all three (forensics — the
        // rows are not pruned).
        let fixture = make_fixture();
        let summary = Arc::new(ScriptedSummary::with_summaries(vec![
            "chunk one.".to_string(),
            "chunk two.".to_string(),
            "chunk three.".to_string(),
        ]));
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary);

        send_pair(&handle, 1).await;
        wait_for_boundary(&handle, 2).await;
        send_pair(&handle, 3).await;
        wait_for_boundary(&handle, 4).await;
        send_pair(&handle, 5).await;
        // Digest 3 summarizes (2, 4]; the context holds S(0-2) + S(2-4).
        wait_for_boundary(&handle, 6).await;
        send_pair(&handle, 7).await;
        // Digest 4 summarizes (4, 6]; S(0-2) rotates OUT of the context.
        wait_for_boundary(&handle, 8).await;

        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 5);
        assert_eq!(items[0].kind, ContextItemKind::Preamble);
        assert_eq!(
            items[1].content,
            r#"<summary range="2-4">chunk two.</summary>"#
        );
        assert_eq!(
            items[2].content,
            r#"<summary range="4-6">chunk three.</summary>"#
        );
        assert_eq!(items[3].range_tag, Some(RangeTag::single(7)));
        assert_eq!(items[4].range_tag, Some(RangeTag::single(8)));

        // Forensics: rotated-out rows stay in the table.
        let rows = blocking_store_call(&fixture.store, move |store| {
            store.list_newest_context_summaries(CHAT_ID, 10)
        })
        .await;
        assert_eq!(rows.len(), 3);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn an_existing_summary_row_skips_the_llm_call() {
        // Rule P1 replay idempotency: a handler re-run (crash between
        // the summary persist and the session persist) finds the row
        // and NEVER calls the provider again.
        let fixture = make_fixture();
        let summary = Arc::new(ScriptedSummary::with_summaries(vec![
            "must never be used.".to_string()
        ]));
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary.clone());

        send_pair(&handle, 1).await;
        wait_for_boundary(&handle, 2).await;
        // The row exists before the second digest completes (the
        // crash-recovery twin of the check-before-call path).
        blocking_store_call(&fixture.store, move |store| {
            store.insert_context_summary(CHAT_ID, 0, 2, "pre-existing summary")
        })
        .await;

        send_pair(&handle, 3).await;
        wait_for_boundary(&handle, 4).await;

        assert!(summary.inputs().is_empty());
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(
            items[1].content,
            r#"<summary range="0-2">pre-existing summary</summary>"#
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_failed_summarization_defers_the_removal_and_retries_next_cycle() {
        // Decision 62 failure semantics: the chunk is NEVER dropped
        // silently. The removal defers one digest cycle; `last`
        // advances, `prev` stays; the next completion retries the
        // removed range — which then covers the failed chunk AND the
        // chunk that was the overlap buffer (the uniform
        // removed-range rule; documented in decision 62).
        let fixture = make_fixture();
        let summary = Arc::new(ScriptedSummary::failing_then(
            1,
            vec!["recovered summary.".to_string()],
        ));
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary.clone());

        send_pair(&handle, 1).await;
        wait_for_boundary(&handle, 2).await;
        send_pair(&handle, 3).await;
        // Digest 2 completes, the summary FAILS: last advances to 4,
        // prev stays None, the raw chunk stays intact.
        wait_for_boundary(&handle, 4).await;
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.last_digest_boundary_msg_id, 4);
        assert_eq!(session.prev_digest_boundary_msg_id, None);
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        // Nothing was removed, nothing was summarized.
        assert_eq!(items.len(), 5);
        assert!(items
            .iter()
            .all(|item| item.kind != ContextItemKind::Summary));
        let missing = blocking_store_call(&fixture.store, move |store| {
            store.find_context_summary(CHAT_ID, 0, 4)
        })
        .await;
        assert!(missing.is_none());

        send_pair(&handle, 5).await;
        // Digest 3 retries: the removed range is (0, 4] (prev stayed
        // None). The summary succeeds; the chunk leaves.
        wait_for_boundary(&handle, 6).await;
        let inputs = summary.inputs();
        assert_eq!(inputs.len(), 2);
        assert_eq!((inputs[0].first_msg_id, inputs[0].last_msg_id), (0, 2));
        assert_eq!((inputs[1].first_msg_id, inputs[1].last_msg_id), (0, 4));

        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 4);
        assert_eq!(
            items[1].content,
            r#"<summary range="0-4">recovered summary.</summary>"#
        );
        assert_eq!(items[2].range_tag, Some(RangeTag::single(5)));
        assert_eq!(items[3].range_tag, Some(RangeTag::single(6)));
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.prev_digest_boundary_msg_id, Some(4));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn restart_rebuilds_the_context_with_summaries_bit_identically() {
        // Rule P1 with summary items: the startup rebuild loads the two
        // newest persisted summary rows and reproduces the live
        // placement exactly.
        let fixture = make_fixture();
        let summary = Arc::new(ScriptedSummary::with_summaries(vec![
            "chunk one.".to_string(),
            "chunk two.".to_string(),
        ]));
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary);

        send_pair(&handle, 1).await;
        wait_for_boundary(&handle, 2).await;
        send_pair(&handle, 3).await;
        wait_for_boundary(&handle, 4).await;
        send_pair(&handle, 5).await;
        // Digest 3: the context holds S(0-2), S(2-4), and the raw tail.
        wait_for_boundary(&handle, 6).await;
        let before = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(before[1].kind, ContextItemKind::Summary);
        assert_eq!(before[2].kind, ContextItemKind::Summary);
        handle.shutdown().await.expect("shutdown succeeds");

        let restarted_summary = Arc::new(ScriptedSummary::with_summaries(vec![]));
        let restarted =
            spawn_with_digest_and_summary(&fixture, digest_config(), restarted_summary.clone());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(rebuilt, before);
        // The restart made no LLM call.
        assert!(restarted_summary.inputs().is_empty());
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    /// A summarizer double that blocks until released (the
    /// pending-gating test).
    struct GatedSummary {
        started: std::sync::atomic::AtomicBool,
        release: tokio::sync::Notify,
    }

    impl SummaryProvider for GatedSummary {
        fn summarize<'a>(
            &'a self,
            _chat_id: &'a str,
            _first_msg_id: i64,
            _last_msg_id: i64,
            _rows: &'a [MessageRow],
        ) -> Pin<Box<dyn Future<Output = Result<String, SummaryError>> + Send + 'a>> {
            Box::pin(async move {
                // Only the FIRST call blocks; later calls return
                // immediately (the completion after the release
                // re-evaluates the digest trigger, whose own
                // summarization must not block again).
                if !self.started.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    self.release.notified().await;
                }
                Ok("gated summary.".to_string())
            })
        }
    }

    #[tokio::test]
    async fn a_digest_trigger_does_not_fire_while_a_summary_is_pending() {
        // Decision 62 gating: while the summary task of one digest
        // completion is in flight, the digest trigger is suppressed —
        // the batch range derives from the not-yet-advanced last
        // boundary, so a new digest would redo the same range.
        let fixture = make_fixture();
        let summary = Arc::new(GatedSummary {
            started: std::sync::atomic::AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        });
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary.clone());

        send_pair(&handle, 1).await;
        wait_for_boundary(&handle, 2).await;
        send_pair(&handle, 3).await;
        // Digest 2 completes; its summary call starts and blocks.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !summary.started.load(std::sync::atomic::Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "the summary never started"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // More messages arrive: the digest threshold is exceeded, but
        // the summary is pending. The boundary must NOT advance (the
        // deferred mutation holds last at 2) and no new digest spawns.
        send_pair(&handle, 5).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.last_digest_boundary_msg_id, 2);

        // Release the summary: the deferred mutation lands (last = 4),
        // then the re-evaluation digests the grown tail (5, 6].
        summary.release.notify_one();
        wait_for_boundary(&handle, 4).await;
        wait_for_boundary(&handle, 6).await;
        handle.shutdown().await.expect("shutdown succeeds");
    }

    // --- Reply-target resolution tests (the amendment-2 rendering) ---

    #[tokio::test]
    async fn reply_to_a_target_pruned_below_the_cutoff_rebuilds_identically() {
        // The amendment-2 trap, Rule P1 bit-identity: the intake-time
        // render and the post-restart rebuild render of a reply MUST
        // produce the same string, resolved through the append-only
        // raw log — never an in-context index. Here the target X is
        // pruned from the live context by the Rule C3 removal while
        // the reply Y stays; the restart rebuild must still render Y
        // with X's display name and row id.
        let fixture = make_fixture();
        let handle = spawn_with_scripted_digest(&fixture, digest_config());
        // Digest 1: the boundary advances 0 -> 2.
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m2", 2, false)))
            .await
            .expect("send succeeds");
        wait_for_boundary(&handle, 2).await;
        // Digest 2: the boundary advances 2 -> 4; the C3 removal at
        // the previous boundary 2 prunes m1 (the future target) from
        // the live context.
        handle
            .send_event(InboundEvent::Message(message("m3", 3, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m4", 4, false)))
            .await
            .expect("send succeeds");
        wait_for_boundary(&handle, 4).await;
        // The reply Y to the already-pruned m1 arrives; intake
        // resolves its target through the raw log, not the context.
        handle
            .send_event(InboundEvent::Message(reply_message(
                "y",
                5,
                Some("m1"),
                false,
            )))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m6", 6, false)))
            .await
            .expect("send succeeds");
        // Digest 3: the boundary advances 4 -> 6; rows 3-4 leave, Y
        // (row 5) stays.
        wait_for_boundary(&handle, 6).await;
        let before = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(before.len(), 3);
        assert_eq!(before[1].range_tag, Some(RangeTag::single(5)));
        // The live render of Y carries the target's display name and
        // row id (row 1, "Alice" fixed at intake).
        assert_eq!(
            before[1].content,
            r#"<msg from="Bob" at="22:13" id="5" reply="user" reply_to_name="Alice" reply_to_id="1">text of y</msg>"#
        );
        handle.shutdown().await.expect("shutdown succeeds");

        // Restart. The rebuild cutoff is the previous boundary (4):
        // the target row 1 is NOT among the rebuilt rows, yet the
        // full-log lookup still resolves it — bit-identical to the
        // intake render.
        let restarted = spawn_with_scripted_digest(&fixture, digest_config());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(rebuilt, before);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_reply_to_an_edited_message_renders_the_original_row_id_and_display_name() {
        // `find_reply_target` pins the ORIGINAL logged row (MIN(id))
        // and the display name fixed at intake: an edit row that
        // arrives before the reply must not move the reply target
        // identity.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        let mut edited = message("m1", 2, false);
        edited.sender_display_name = "Alicia".to_string();
        edited.text = "edited text".to_string();
        handle
            .send_event(InboundEvent::EditedMessage(edited))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(reply_message(
                "y",
                3,
                Some("m1"),
                false,
            )))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        // The reply renders the ORIGINAL row id (1) and the ORIGINAL
        // intake display name ("Alice"), not the edit row (2,
        // "Alicia").
        assert_eq!(items.len(), 4);
        assert_eq!(
            items[3].content,
            r#"<msg from="Bob" at="22:13" id="3" reply="user" reply_to_name="Alice" reply_to_id="1">text of y</msg>"#
        );
        handle.shutdown().await.expect("shutdown succeeds");

        // Rebuild parity: the restart renders the same reply
        // attributes.
        let restarted = spawn_on(&fixture, TriggerConfig::default());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(rebuilt, items);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn an_edit_row_that_is_itself_a_reply_renders_reply_attributes() {
        // The edit path resolves the reply render exactly like the
        // message path: an edit whose normalized message carries a
        // reply target renders kind="edit" plus the resolved reply
        // attributes (flag precedence: kind, then reply).
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        let mut edit_reply = message("e1", 2, false);
        edit_reply.reply_to_platform_msg_id = Some("m1".to_string());
        edit_reply.text = "edit that replies".to_string();
        handle
            .send_event(InboundEvent::EditedMessage(edit_reply))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        assert_eq!(
            items[2].content,
            r#"<msg from="Alice" at="22:13" id="2" kind="edit" reply="user" reply_to_name="Alice" reply_to_id="1">edit that replies</msg>"#
        );
        handle.shutdown().await.expect("shutdown succeeds");

        let restarted = spawn_on(&fixture, TriggerConfig::default());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(rebuilt, items);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_reply_to_the_bot_renders_no_target_attributes() {
        // Rules A3/B1: outbound rows carry synthetic `bot-out:{nanos}`
        // ids; a reply to the bot renders `reply="bot"` with NO target
        // name/id and never attempts a lookup.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(reply_message(
                "r1",
                1,
                Some("bot-out:1700000000000000000"),
                true,
            )))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        assert_eq!(
            items[1].content,
            r#"<msg from="Bob" at="22:13" id="1" reply="bot">text of r1</msg>"#
        );
        handle.shutdown().await.expect("shutdown succeeds");

        let restarted = spawn_on(&fixture, TriggerConfig::default());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(rebuilt, items);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_reply_to_an_unknown_platform_id_renders_a_plain_user_reply() {
        // The target is absent from the log (it predates the bot or
        // never arrived): the lookup returns None and the reply
        // renders `reply="user"` with no target attributes.
        // Deterministic; rebuild-identical.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(reply_message(
                "r1",
                1,
                Some("missing-pid"),
                false,
            )))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        assert_eq!(
            items[1].content,
            r#"<msg from="Bob" at="22:13" id="1" reply="user">text of r1</msg>"#
        );
        handle.shutdown().await.expect("shutdown succeeds");

        let restarted = spawn_on(&fixture, TriggerConfig::default());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(rebuilt, items);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn username_renders_and_rebuilds_with_and_without_the_attribute() {
        // The intake append renders `user="..."` only when the
        // platform provides a username; the rebuild renders the same
        // from the persisted `sender_username`.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        let mut with_username = message("u1", 1, false);
        with_username.username = Some("alice_tg".to_string());
        handle
            .send_event(InboundEvent::Message(with_username))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("u2", 2, false)))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        assert_eq!(
            items[1].content,
            r#"<msg from="Alice" user="alice_tg" at="22:13" id="1">text of u1</msg>"#
        );
        assert_eq!(
            items[2].content,
            r#"<msg from="Alice" at="22:13" id="2">text of u2</msg>"#
        );
        handle.shutdown().await.expect("shutdown succeeds");

        let restarted = spawn_on(&fixture, TriggerConfig::default());
        let rebuilt = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(rebuilt, items);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn reaction_intake_persists_the_row() {
        // specs.md Section 5.2: reaction data is not recoverable later,
        // so intake collects it. Rule P1: the row persists first.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Reaction(reaction("m1")))
            .await
            .expect("send succeeds");
        // An aggregated count update: no reactor identity.
        let mut aggregated = reaction("m1");
        aggregated.reactor_id = None;
        aggregated.aggregated = true;
        aggregated.old_emojis = vec!["👍".to_string()];
        aggregated.new_emojis = vec!["👍".to_string(), "❤️".to_string()];
        aggregated.timestamp = t0() + time::Duration::seconds(2);
        handle
            .send_event(InboundEvent::Reaction(aggregated))
            .await
            .expect("send succeeds");
        // A FIFO barrier: when it returns, both reactions are processed.
        handle.snapshot().await.expect("snapshot succeeds");

        let rows = list_reactions(&fixture.store).await;
        assert_eq!(rows.len(), 2);

        // The named reaction: every field round-trips.
        let named = &rows[0];
        assert_eq!(named.platform_msg_id, "m1");
        assert_eq!(named.reactor_user_id, Some("u1".to_string()));
        assert!(!named.anonymous);
        assert!(!named.aggregated);
        assert_eq!(named.old_emojis, Vec::<String>::new());
        assert_eq!(named.new_emojis, vec!["👍".to_string()]);
        assert_eq!(named.timestamp, t0() + time::Duration::seconds(1));

        // The aggregated reaction: the reactor stays None.
        let aggregate = &rows[1];
        assert_eq!(aggregate.platform_msg_id, "m1");
        assert_eq!(aggregate.reactor_user_id, None);
        assert!(!aggregate.anonymous);
        assert!(aggregate.aggregated);
        assert_eq!(aggregate.old_emojis, vec!["👍".to_string()]);
        assert_eq!(
            aggregate.new_emojis,
            vec!["👍".to_string(), "❤️".to_string()]
        );
        assert_eq!(aggregate.timestamp, t0() + time::Duration::seconds(2));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn reaction_redelivery_is_idempotent() {
        // AGENT.md Section 6.2: a reconnect can redeliver the same
        // update. The dedup index makes the second insert a Duplicate;
        // intake does not fail.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        for _ in 0..2 {
            handle
                .send_event(InboundEvent::Reaction(reaction("m1")))
                .await
                .expect("send succeeds");
        }
        handle.snapshot().await.expect("snapshot succeeds");

        let rows = list_reactions(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn reaction_intake_is_passive_collection() {
        // Reactions are passive: no context item and no wake-counter
        // advance. Only the message counts.
        let (_fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Reaction(reaction("m1")))
            .await
            .expect("send succeeds");
        // FIFO barriers: both events are processed when these return.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        // The reaction did not advance the wake counter.
        assert_eq!(session.wake.msgs_since_wake, 1);
        // Preamble + the message item; the reaction appended nothing.
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].kind, ContextItemKind::Preamble);
        assert_eq!(items[1].kind, ContextItemKind::HumanMessage);
        assert_eq!(items[1].range_tag, Some(RangeTag::single(1)));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    // --- M4 wake-procedure tests ---

    use crate::wake::NoopRecall;
    use tokio::sync::Notify;

    /// Which message of the presented set the scripted gate targets.
    #[derive(Debug, Clone, Copy)]
    enum GateTarget {
        First,
        Last,
    }

    /// A scripted participation gate (the `ScriptedDigest` pattern). The
    /// double runs inside the spawned wake task, so it computes its
    /// decision from the input: participate and target the first/last
    /// new message. Every `decide` call is recorded.
    struct ScriptedGate {
        participate: bool,
        target: GateTarget,
        calls: Mutex<Vec<GateInput>>,
    }

    impl ScriptedGate {
        fn yes(target: GateTarget) -> Arc<Self> {
            Arc::new(Self {
                participate: true,
                target,
                calls: Mutex::new(Vec::new()),
            })
        }

        fn no() -> Arc<Self> {
            Arc::new(Self {
                participate: false,
                target: GateTarget::Last,
                calls: Mutex::new(Vec::new()),
            })
        }

        fn call_count(&self) -> usize {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
        }
    }

    impl ParticipationGate for ScriptedGate {
        fn decide<'a>(
            &'a self,
            input: &'a GateInput,
        ) -> Pin<Box<dyn Future<Output = Result<GateDecision, CoreError>> + Send + 'a>> {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(input.clone());
            let target_row_id = if self.participate {
                match self.target {
                    GateTarget::First => input.new_messages.first(),
                    GateTarget::Last => input.new_messages.last(),
                }
                .map(|msg| msg.row_id)
            } else {
                None
            };
            let participate = self.participate;
            Box::pin(async move {
                Ok(GateDecision {
                    participate,
                    target_row_id,
                    // The scripted double never consulted the gate
                    // model: no reason string.
                    reason: None,
                })
            })
        }
    }

    /// A scripted reply generator. With `hold` set, the FIRST `generate`
    /// call waits on the notify (the in-flight hold of the queueing
    /// tests). Every call is counted and every request recorded.
    struct ScriptedReply {
        text: String,
        calls: Mutex<usize>,
        requests: Mutex<Vec<ReplyRequest>>,
        hold: Option<Arc<Notify>>,
    }

    impl ScriptedReply {
        fn new(text: &str) -> Arc<Self> {
            Arc::new(Self {
                text: text.to_string(),
                calls: Mutex::new(0),
                requests: Mutex::new(Vec::new()),
                hold: None,
            })
        }

        fn held(text: &str, hold: Arc<Notify>) -> Arc<Self> {
            Arc::new(Self {
                text: text.to_string(),
                calls: Mutex::new(0),
                requests: Mutex::new(Vec::new()),
                hold: Some(hold),
            })
        }

        fn call_count(&self) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }

        fn requests(&self) -> Vec<ReplyRequest> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl ReplyGenerator for ScriptedReply {
        fn generate<'a>(
            &'a self,
            request: &'a ReplyRequest,
        ) -> Pin<Box<dyn Future<Output = Result<String, CoreError>> + Send + 'a>> {
            let call = {
                let mut calls = self
                    .calls
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *calls += 1;
                *calls
            };
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request.clone());
            Box::pin(async move {
                if call == 1 {
                    if let Some(hold) = &self.hold {
                        hold.notified().await;
                    }
                }
                Ok(self.text.clone())
            })
        }
    }

    /// A scripted recall provider (the `ScriptedGate` pattern). Each
    /// `recall` call pops one queued `RecallOutcome` (an empty queue
    /// yields the empty outcome) and records its input.
    struct ScriptedRecall {
        outcomes: Mutex<std::collections::VecDeque<crate::wake::RecallOutcome>>,
        calls: Mutex<Vec<(String, Vec<GateMessage>)>>,
    }

    impl ScriptedRecall {
        fn with_outcomes(outcomes: Vec<crate::wake::RecallOutcome>) -> Arc<Self> {
            Arc::new(Self {
                outcomes: Mutex::new(outcomes.into()),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<(String, Vec<GateMessage>)> {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl RecallProvider for ScriptedRecall {
        fn recall<'a>(
            &'a self,
            chat_id: &'a str,
            new_messages: &'a [GateMessage],
        ) -> Pin<Box<dyn Future<Output = Result<crate::wake::RecallOutcome, CoreError>> + Send + 'a>>
        {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((chat_id.to_string(), new_messages.to_vec()));
            let outcome = self
                .outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or_default();
            Box::pin(async move { Ok(outcome) })
        }
    }

    /// The trigger config of the wake tests: `count` messages fire the
    /// threshold, no floor, and a 1 h interval whose jittered minimum
    /// (42 min) stays above every elapsed time these tests use, so only
    /// the count (or an explicit tick) fires.
    fn wake_config(count: u32) -> TriggerConfig {
        TriggerConfig {
            wake_msg_count: count,
            wake_floor: Duration::ZERO,
            wake_interval: Duration::from_secs(60 * 60),
            ..TriggerConfig::default()
        }
    }

    /// Spawns an actor with the M5 wake services over the scripted
    /// doubles. Returns the outbound receiver for the sent actions.
    fn spawn_with_wake(
        fixture: &Fixture,
        config: TriggerConfig,
        recall: Arc<dyn RecallProvider>,
        gate: Arc<ScriptedGate>,
        reply: Arc<ScriptedReply>,
    ) -> (GroupActorHandle, mpsc::Receiver<OutboundAction>) {
        let (outbound_tx, outbound_rx) = mpsc::channel(64);
        let services = WakeServices {
            recall,
            gate,
            reply,
        };
        let handle = spawn_group_actor(GroupActorParams {
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::clone(&fixture.memory),
            config,
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            preamble: TEST_PREAMBLE.to_string(),
            digest: None,
            post_digest_hook: None,
            wake: Some(services),
            summary_provider: None,
            outbound: Some(outbound_tx),
            bot_name: None,
        });
        (handle, outbound_rx)
    }

    /// Reads one counter of the state table (None when the key does not
    /// exist).
    async fn counter_value(store: &Arc<Store>, key: &str) -> Option<i64> {
        let key = key.to_string();
        blocking_store_call(store, move |store| store.get_state(CHAT_ID, &key))
            .await
            .map(|value| value.parse().expect("a counter value is decimal"))
    }

    /// Polls until the counter reaches `want` or the deadline passes
    /// (the `wait_for_boundary` pattern: bounded, deterministic).
    async fn wait_for_counter(store: &Arc<Store>, key: &str, want: i64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if counter_value(store, key).await == Some(want) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for counter {key} to reach {want}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Polls until the gate double records `want` calls (bounded).
    async fn wait_for_gate_calls(gate: &ScriptedGate, want: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if gate.call_count() >= want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {want} gate calls"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Polls until the reply double records `want` calls (bounded).
    async fn wait_for_reply_calls(reply: &ScriptedReply, want: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if reply.call_count() >= want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {want} reply calls"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Polls `snapshot()` until `muted` has the wanted value (bounded).
    async fn wait_for_muted(handle: &GroupActorHandle, want: bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let session = handle.snapshot().await.expect("snapshot succeeds");
            if session.muted == want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for muted == {want}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Receives the next outbound action with a bounded wait.
    async fn next_action(outbound: &mut mpsc::Receiver<OutboundAction>) -> OutboundAction {
        tokio::time::timeout(Duration::from_secs(5), outbound.recv())
            .await
            .expect("an outbound action arrives within 5 s")
            .expect("the outbound channel stays open")
    }

    /// Asserts that no outbound action arrives within `window` (a
    /// bounded negative check).
    async fn assert_no_action(outbound: &mut mpsc::Receiver<OutboundAction>, window: Duration) {
        assert!(
            tokio::time::timeout(window, outbound.recv()).await.is_err(),
            "an unexpected outbound action arrived"
        );
    }

    /// Destructures a SendText action; panics on any other variant.
    fn expect_send_text(action: OutboundAction) -> (String, String, Option<String>) {
        let OutboundAction::SendText {
            chat_id,
            text,
            reply_to_platform_msg_id,
        } = action
        else {
            panic!("the action is a SendText, got {action:?}");
        };
        (chat_id, text, reply_to_platform_msg_id)
    }

    #[tokio::test]
    async fn threshold_wake_participates_end_to_end() {
        // specs.md Section 9 end to end in-core: three messages reach
        // wake_msg_count; the gate says yes and targets the last new
        // message (row 3).
        let fixture = make_fixture();
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("a thoughtful reply");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }

        // The reply goes out as a SendText with reply-to the target.
        let (chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(chat_id, CHAT_ID);
        assert_eq!(text, "a thoughtful reply");
        assert_eq!(reply_to, Some("m3".to_string()));

        // participations_total lands at the END of the completion
        // handler, so this wait covers the whole send path.
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(1));

        // Rule B1: the outbound row is in the raw log.
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 4);
        let bot_row = &rows[3];
        assert_eq!(bot_row.direction, Direction::Outbound);
        assert_eq!(bot_row.event_type, EventType::Message);
        assert_eq!(bot_row.text, "a thoughtful reply");
        assert_eq!(bot_row.reply_to_platform_msg_id, Some("m3".to_string()));
        // bot_name None falls back to "Tamako".
        assert_eq!(bot_row.sender_display_name, "Tamako");

        // Rule C1: the context carries the bot speech at the tail.
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 5);
        let last = items.last().expect("items exist");
        assert_eq!(last.kind, ContextItemKind::BotSpeech);
        assert_eq!(last.role, ContextRole::Assistant);
        // The `<you>` item of the row (the row timestamp is the same
        // `now` the append used).
        assert_eq!(
            last.content,
            render_bot_content(bot_row.id, bot_row.timestamp, "a thoughtful reply")
        );
        assert_eq!(last.range_tag, Some(RangeTag::single(bot_row.id)));

        // The wake advanced the tail marker.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 3);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn gate_no_sends_nothing() {
        // Section 9.6: a negative decision sends nothing and counts no
        // participation. Only wakes_total (counted at wake start) moves.
        let fixture = make_fixture();
        let gate = ScriptedGate::no();
        let reply = ScriptedReply::new("never used");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        wait_for_gate_calls(&gate, 1).await;
        // The reply model never runs on a negative decision (Section 9.6).
        assert_eq!(reply.call_count(), 0);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 3);
        assert_eq!(
            counter_value(&fixture.store, "participations_total").await,
            None
        );
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(1));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn forced_wake_bypasses_the_gate() {
        // Section 8.1: a mention forces a wake. The gate double RECORDS
        // its calls; the forced path must record ZERO calls and target
        // the mention message itself.
        let fixture = make_fixture();
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("forced reply");
        // A high count: only the mention can fire the wake.
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(100),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        handle
            .send_event(InboundEvent::Message(message("m1", 1, true)))
            .await
            .expect("send succeeds");

        let (_chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "forced reply");
        assert_eq!(reply_to, Some("m1".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 1).await;

        assert_eq!(gate.call_count(), 0);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(1));
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].direction, Direction::Outbound);
        assert_eq!(rows[1].reply_to_platform_msg_id, Some("m1".to_string()));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_monologue_lock_suppresses_an_unforced_wake() {
        // Section 8.5 with the live speech path (Section 9 step 1).
        // Counter design asserted below: wakes_total counts EVERY wake
        // invocation — the two forced wakes, the suppressed wake, and
        // the final wake = 4. participations_total counts the sent
        // replies = 3.
        //
        // Note on the scenario: intake of ANY human message clears the
        // lock (Section 8.5), so the muted unforced wake is driven by an
        // explicit Tick, not by a message threshold — a threshold wake
        // can never observe the muted state.
        let fixture = make_fixture();
        let hold = Arc::new(Notify::new());
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::held("monologue reply", Arc::clone(&hold));
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );

        // Wake 1 (forced) holds inside the reply generation; mention m2
        // queues behind it (Section 6.2). Both mentions land BEFORE any
        // bot reply, so the two replies are consecutive in the log and
        // engage the lock (monologue_limit 2).
        handle
            .send_event(InboundEvent::Message(message("m1", 1, true)))
            .await
            .expect("send succeeds");
        wait_for_reply_calls(&reply, 1).await;
        handle
            .send_event(InboundEvent::Message(message("m2", 2, true)))
            .await
            .expect("send succeeds");
        hold.notify_one();

        let (_, _, first_reply_to) = expect_send_text(next_action(&mut outbound).await);
        let (_, _, second_reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(first_reply_to, Some("m1".to_string()));
        assert_eq!(second_reply_to, Some("m2".to_string()));
        wait_for_muted(&handle, true).await;
        assert_eq!(gate.call_count(), 0);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));
        assert_eq!(
            counter_value(&fixture.store, "participations_total").await,
            Some(2)
        );

        // The muted check (Section 9 step 1): the unforced tick wake is
        // suppressed. wake_last_row_id is NOT advanced (messages of the
        // muted period stay "new"); here it keeps the value wake 2's
        // gather produced (row 3, the first bot reply row).
        handle
            .send(ActorCommand::Tick(t0() + time::Duration::hours(10)))
            .await
            .expect("send succeeds");
        wait_for_counter(&fixture.store, "wakes_total", 3).await;
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert_eq!(gate.call_count(), 0);
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 3);

        // One human message clears the lock (Section 8.5); three
        // messages reach the threshold and the wake runs.
        for (id, seconds) in [("c1", 36001), ("c2", 36002), ("c3", 36003)] {
            handle
                .send_event(InboundEvent::Message(message(id, seconds, false)))
                .await
                .expect("send succeeds");
        }
        let (_, _, third_reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(third_reply_to, Some("c3".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 3).await;
        assert_eq!(gate.call_count(), 1);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(4));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn stale_reply_is_discarded_by_the_recency_recheck() {
        // Section 6.2 recency re-check: threshold 0, so any newer human
        // message after the target discards the reply. The gate targets
        // the FIRST new message (row 1); rows 2-3 are newer. Documented
        // decision: discard, NOT regenerate.
        let fixture = make_fixture();
        let mut config = wake_config(3);
        config.reply_staleness_threshold = 0;
        let gate = ScriptedGate::yes(GateTarget::First);
        let reply = ScriptedReply::new("stale reply");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            config,
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        wait_for_gate_calls(&gate, 1).await;
        // The reply WAS generated (the generation is not the waste the
        // re-check guards against; the SEND is), then discarded.
        assert_eq!(reply.call_count(), 1);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 3);
        assert_eq!(
            counter_value(&fixture.store, "participations_total").await,
            None
        );
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(1));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test(start_paused = true)]
    async fn the_timer_driver_evaluates_a_silent_group() {
        // The M4 timer driver end to end: only the interval can fire
        // (count 100; the message is timestamped exactly at started_at,
        // so intake cannot fire). wake_interval 10 ms gives cadence 0
        // (floor 0), which `timer_period` guards to 1 s; the paused
        // runtime auto-advances to the first tick. The tick runs the
        // SAME handler as an explicit Tick with
        // `OffsetDateTime::now_utc()`, so the interval condition fires.
        let config = TriggerConfig {
            wake_interval: Duration::from_millis(10),
            wake_floor: Duration::ZERO,
            wake_msg_count: 100,
            ..TriggerConfig::default()
        };
        let fixture = make_fixture();
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("timer reply");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            config,
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        handle
            .send_event(InboundEvent::Message(message("m1", 0, false)))
            .await
            .expect("send succeeds");

        // wakes_total moves only when a wake starts: the tick must have
        // driven the evaluation (intake could not). The first cadence
        // tick fires at 1 s virtual (the zero-cadence guard of
        // `timer_period`); the paused runtime auto-advances to it during
        // this one virtual sleep. A 10 ms poll loop would need ~100 real
        // sqlite round trips to get there — one virtual jump instead.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        wait_for_counter(&fixture.store, "wakes_total", 1).await;
        let (_, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        // The live wake path ran (the stub path sends nothing and never
        // participates).
        assert_eq!(text, "timer reply");
        assert_eq!(reply_to, Some("m1".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        assert_eq!(gate.call_count(), 1);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn forced_wake_queues_behind_a_running_wake() {
        // Section 6.2: a forced Wake moves to the head of the queue; it
        // does not preempt a running call. The reply double holds the
        // first wake in flight until the test releases it.
        let fixture = make_fixture();
        let hold = Arc::new(Notify::new());
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::held("queued reply", Arc::clone(&hold));
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        // The threshold fires wake 1; its reply generation blocks, so
        // the wake stays in flight.
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("p{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        wait_for_reply_calls(&reply, 1).await;
        // The mention queues behind the running wake.
        handle
            .send_event(InboundEvent::Message(message("m4", 4, true)))
            .await
            .expect("send succeeds");
        hold.notify_one();

        // BOTH replies arrive, in order: the running wake first, the
        // forced one second.
        let (_, _, first_reply_to) = expect_send_text(next_action(&mut outbound).await);
        let (_, _, second_reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(first_reply_to, Some("p3".to_string()));
        assert_eq!(second_reply_to, Some("m4".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 2).await;
        // The gate ran exactly once: the forced wake bypassed it
        // (Section 8.1).
        assert_eq!(gate.call_count(), 1);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    // --- M5 injection-protocol tests ---

    /// Lists the injected_memories rows through a blocking call, like
    /// the actor does.
    async fn list_injected_memories(store: &Arc<Store>) -> Vec<tamako_store::InjectedMemoryRow> {
        blocking_store_call(store, move |store| store.list_injected_memories(CHAT_ID)).await
    }

    /// Opens the group store BEFORE the actor spawns: a concurrent
    /// lazy `open_group` (from a test-side store call) of the same
    /// store.db races the migration of the actor startup. The same
    /// note as `forced_wake_does_not_panic_when_muted`.
    async fn pre_open_group(store: &Arc<Store>) {
        blocking_store_call(store, |store| store.open_group(CHAT_ID)).await;
    }

    #[tokio::test]
    async fn gate_no_still_applies_the_planned_injection() {
        // Section 9 step 2: the injection happens as part of recall,
        // BEFORE the participation decision (the decision itself
        // consumes the memories). A gate-no wake still injects: the
        // memory was genuinely remembered; a silent pet can still
        // remember.
        let fixture = make_fixture();
        pre_open_group(&fixture.store).await;
        let recall = ScriptedRecall::with_outcomes(vec![crate::wake::RecallOutcome {
            injections: vec![PlannedInjection {
                edge_ids: vec!["edge-1".to_string(), "edge-2".to_string()],
                content: "I remember: Alice likes GRPO".to_string(),
            }],
        }]);
        let gate = ScriptedGate::no();
        let reply = ScriptedReply::new("never used");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::clone(&recall) as Arc<dyn RecallProvider>,
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        // The counter lands at the END of the injection application in
        // the completion handler, so this wait covers it (Section 12).
        wait_for_counter(&fixture.store, "injection_wakes_total", 1).await;

        // The gate said no: nothing is sent, the reply model never ran.
        assert_eq!(reply.call_count(), 0);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert_eq!(
            counter_value(&fixture.store, "participations_total").await,
            None
        );
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(1));

        // Rule C2: the injection is appended to the context at the tail
        // anyway, at the position of the tail row id at wake start (row
        // 3, the last new message).
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 5);
        let injection = items.last().expect("items exist");
        assert_eq!(injection.kind, ContextItemKind::RecallInjection);
        assert_eq!(injection.role, ContextRole::Assistant);
        assert_eq!(injection.content, "I remember: Alice likes GRPO");
        assert_eq!(injection.range_tag, Some(RangeTag::single(3)));

        // Section 9.3: one dedup row per edge id, with the position,
        // the range tag string, and the rendered content.
        let rows = list_injected_memories(&fixture.store).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].edge_id, "edge-1");
        assert_eq!(rows[1].edge_id, "edge-2");
        for row in &rows {
            assert_eq!(row.injection_position, 3);
            assert_eq!(row.range_tag, "3-3");
            assert_eq!(row.content, "I remember: Alice likes GRPO");
        }

        // Section 9.6: the recall result is gate input on purpose.
        let gate_calls = gate.calls.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(gate_calls.len(), 1);
        assert_eq!(
            gate_calls[0].injections,
            vec!["I remember: Alice likes GRPO".to_string()]
        );

        // The recall worker received the new messages with the raw
        // fields populated (deterministic candidate extraction,
        // proposed-graph-database-specs.md Section 8.1 step 1).
        let recall_calls = recall.calls();
        assert_eq!(recall_calls.len(), 1);
        assert_eq!(recall_calls[0].0, CHAT_ID);
        let presented = &recall_calls[0].1;
        assert_eq!(presented.len(), 3);
        assert_eq!(presented[0].sender_id, "u1");
        assert_eq!(presented[0].text, "text of m1");
        assert_eq!(presented[0].reply_to_platform_msg_id, None);
        assert_eq!(
            presented[0].content,
            r#"<msg from="Alice" at="22:13" id="1">text of m1</msg>"#
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn gate_yes_reply_request_includes_the_injection_text() {
        // The injection is part of the context from step 2 on, so the
        // reply model of step 4 sees it as an assistant message
        // (Section 9.4, Rule C2 tail position).
        let fixture = make_fixture();
        pre_open_group(&fixture.store).await;
        let recall = ScriptedRecall::with_outcomes(vec![crate::wake::RecallOutcome {
            injections: vec![PlannedInjection {
                edge_ids: vec!["edge-1".to_string()],
                content: "I remember: Alice likes espresso".to_string(),
            }],
        }]);
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("a reply with context");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            recall,
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        let (_, text, _) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "a reply with context");

        // The reply request: preamble + the three new messages + the
        // injection as an assistant message at the tail.
        let requests = reply.requests();
        assert_eq!(requests.len(), 1);
        let messages = &requests[0].messages;
        assert_eq!(messages.len(), 5);
        let injection = messages.last().expect("messages exist");
        assert_eq!(injection.role, ContextRole::Assistant);
        assert_eq!(injection.content, "I remember: Alice likes espresso");

        // The normal injection bookkeeping still ran.
        wait_for_counter(&fixture.store, "injection_wakes_total", 1).await;
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        let rows = list_injected_memories(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].edge_id, "edge-1");
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn empty_recall_outcome_injects_nothing() {
        // Section 9.2: an empty injection is forbidden — nothing is
        // injected. No context item, no dedup rows, no counter.
        let fixture = make_fixture();
        pre_open_group(&fixture.store).await;
        let recall = ScriptedRecall::with_outcomes(vec![crate::wake::RecallOutcome::default()]);
        let gate = ScriptedGate::no();
        let reply = ScriptedReply::new("never used");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::clone(&recall) as Arc<dyn RecallProvider>,
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        wait_for_gate_calls(&gate, 1).await;
        // The recall provider ran and returned the empty outcome.
        assert_eq!(recall.calls().len(), 1);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;

        // No injection item in the context (preamble + 3 human items).
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 4);
        assert!(items
            .iter()
            .all(|item| item.kind != ContextItemKind::RecallInjection));
        // No dedup rows.
        assert!(list_injected_memories(&fixture.store).await.is_empty());
        // The counter key is absent (the same counter assertion style
        // as `participations_total` in `gate_no_sends_nothing`).
        assert_eq!(
            counter_value(&fixture.store, "injection_wakes_total").await,
            None
        );
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(1));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    // --- Reply-target resolution tests (the amendment-2 rendering) ---

    #[tokio::test]
    async fn wake_gate_messages_carry_resolved_reply_targets() {
        // `start_wake` resolves the reply targets of the new inbound
        // rows in ONE blocking pass (the same store function as
        // intake), so the gate input renders byte-identically to the
        // context items — the amendment-2 attention fix reaches the
        // gate.
        let fixture = make_fixture();
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("gate reply");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m2", 2, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(reply_message(
                "m3",
                3,
                Some("m1"),
                false,
            )))
            .await
            .expect("send succeeds");
        wait_for_gate_calls(&gate, 1).await;

        // The third new message presented to the gate renders the
        // resolved target attributes of m1 (row 1, "Alice").
        let gate_calls = gate.calls.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(gate_calls.len(), 1);
        let presented = &gate_calls[0].new_messages;
        assert_eq!(presented.len(), 3);
        assert_eq!(presented[2].row_id, 3);
        assert_eq!(
            presented[2].content,
            r#"<msg from="Bob" at="22:13" id="3" reply="user" reply_to_name="Alice" reply_to_id="1">text of m3</msg>"#
        );

        // The reply goes out targeting the last new message.
        let (_, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "gate reply");
        assert_eq!(reply_to, Some("m3".to_string()));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_forcing_reply_mention_renders_the_resolved_target_in_the_gate_message() {
        // The forcing gate message reuses the SAME resolved
        // `ReplyRender` as the context append (computed once): the
        // reply model's target view and the live context item of the
        // forcing row render identically.
        let fixture = make_fixture();
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("forced reply");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(100),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        let mut forcing = reply_message("m2", 2, Some("m1"), false);
        forcing.mentions_bot = true;
        handle
            .send_event(InboundEvent::Message(forcing))
            .await
            .expect("send succeeds");
        wait_for_reply_calls(&reply, 1).await;

        // The reply model saw the forcing gate message with the
        // resolved target AND the mention flag.
        let requests = reply.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].target.content,
            r#"<msg from="Bob" at="22:13" id="2" reply="user" reply_to_name="Alice" reply_to_id="1" mention="bot">text of m2</msg>"#
        );
        let (_, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "forced reply");
        assert_eq!(reply_to, Some("m2".to_string()));

        // The context item of the forcing row renders identically.
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items[2].content, requests[0].target.content);
        handle.shutdown().await.expect("shutdown succeeds");
    }
}
