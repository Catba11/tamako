//! The per-group actor. Refer to specs.md Section 6.
//!
//! Message intake appends to the raw log and updates the counters. The
//! digest trigger (specs.md Section 8.2) is wired in Phase 1 (M1): when
//! the trigger fires, the actor launches the digest pipeline as a
//! spawned task and the result returns through the FIFO inbox.
//!
//! The wake procedure (specs.md Section 9, steps 1-5) is live since
//! Phase 1 M4. A wake failure is logged and skipped, never fatal. On a
//! failure `wake_last_row_id` rolls back to its pre-wake value
//! (decision 65, amending decision 33's reset-at-start — decision-log +
//! spec-backfill note for Sections 6.2/9): the failed wake's messages
//! re-present at the next NATURAL trigger, and the reset-at-start
//! floor/threshold still gates it, so there is no immediate retry
//! storm. A failed FORCED wake requeues into `forced_pending` ONCE
//! (bounded, marked entry); a second failure drops it with a distinct
//! ERROR naming the unmet Section 8.1 must-respond obligation. The
//! counters `wakes_total` and
//! `participations_total` (Section 12) are best effort. Decision 65
//! also fixes the two latent marker bugs: at startup a missing or
//! malformed `wake_last_row_id` of a group WITH prior log rows is
//! repaired to the raw-log tail ("start from now" — scheduling state,
//! P1-safe to recompute), and the disabled-services path (no
//! gate/reply provider wired) still advances the marker past the
//! messages a wake would have presented, so the presented range stays
//! bounded while the services are off. The steps:
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
//! The forced-wake cooldown (specs.md Sections 8.1/6.2, decision 79
//! (c)): a forced wake that produced a reply starts an in-memory
//! cooldown (`forced_wake_cooldown`, default 10 s, 0 disables)
//! anchored at the forcing message's intake timestamp — the same
//! deterministic replay clock the intake path compares on. A forcing
//! arriving inside the window is suppressed: the row is persisted and
//! enters the context as today (Rule P1 untouched — it lands in the
//! next wake's presented set, so no information is lost), but no wake
//! fires, nothing queues, and `forced_pending`/`wake_last_row_id` stay
//! untouched. The cooldown suppresses the back-to-back reply chains of
//! a mention/reply rally. It is a rate limiter, NOT rebuild state: on
//! a restart (Rule P1) it is simply not running, and the rebuild stays
//! bit-identical.
//!
//! The warmup trigger (specs.md Sections 8.4/8.5/9.7, decision 78) is
//! live since Phase 2. It is evaluated ONLY on the timer tick, AFTER the
//! wake evaluation (the tick order is Digest → Wake → Warmup); intake
//! never evaluates it. The steps of Section 9.7: resolve the engagement
//! watch of the previous warmup first (a reply or reaction inside
//! `warmup_reaction_window` resets the Section 8.5 soft backoff; an
//! expired silent watch increments it), roll the quota-day counter over
//! on a new host-local day, then — when the persisted `warmup_next_at`
//! is due — run the step-1 gates (muted, effective quota, group
//! silence, an open watch, a warmup in flight; any failure consumes the
//! slot quietly at DEBUG and reschedules), pick a topic (step 2; no
//! eligible topic means no warmup — forced small talk is worse than
//! silence), and fire: the slot is consumed and persisted BEFORE the
//! generation task spawns (the Section 6.1 rule-3 analog — the FIFO
//! never blocks on an LLM call), so a generation failure needs no
//! reschedule (the next slot is the natural retry). The send path
//! mirrors the wake's: the parrot filter (decisions 59/64), the
//! outbound raw-log row FIRST (Rule B1), a plain standalone
//! `SendText` (proactive speech never quotes a target), the Rule C1
//! context append, and the monologue lock. The counters `warmups_total`
//! and `warmup_engaged_total` (Section 12) are best effort. Decision 78
//! adds ONE new curated line kind: exactly one INFO `warmup` line per
//! sent warmup; gate skips stay DEBUG.
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
//! Persona hot reload (decision 80, live mode only): the binary's
//! debounced watcher broadcasts the freshly rendered preamble as
//! `ActorCommand::ReloadPreamble` through the FIFO inbox (specs.md
//! Section 6.1 rule 1 — it serializes behind in-flight work, never
//! interrupts a running call). The handler swaps context item 0 in
//! memory (Rule C4); nothing persists — the persona file is the state,
//! a restart rebuild renders the same bytes (Rule P1 untouched). The
//! broadcast is best-effort (`GroupActorHandle::try_send`): a dead or
//! backlogged actor is skipped; its next start reads the file.
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
use time::{OffsetDateTime, UtcOffset};
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
    PlannedInjection, RecallProvider, ReplyFence, ReplyGenerator, ReplyRequest, WakeServices,
};
use crate::warmup::{
    effective_quota, local_date_string, pick_topic, prune_cooldowns, schedule_next, WarmupServices,
    TAIL_EXCLUSION_ROWS, TOPIC_SAMPLE_LIMIT,
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
    /// A failure of the warmup generation (specs.md Section 9.7 step 3,
    /// decision 78). Log and skip this warmup; the next scheduled slot is
    /// the natural retry.
    #[error("warmup error: {0}")]
    Warmup(String),
}

/// The default inbox capacity when the caller has no preference.
pub const DEFAULT_INBOX_CAPACITY: usize = 256;

/// The M3 summarization circuit breaker (decision 65): after this many
/// CONSECUTIVE summarization failures, the failing chunk falls back to
/// the sanctioned old-C3 drop (the removal proceeds without a summary,
/// exactly like the None-provider behavior) with one ERROR, and the
/// breaker resets. Justification of 3: a brief endpoint flap self-heals
/// within one or two digest cycles (the decision-62 deferral already
/// retries those), so three consecutive failures across three digest
/// completions mean the endpoint is durably broken — and wedging the
/// context growth forever is worse than losing one summary. After a
/// circuit-break drop the next chunk tries summarization again (a
/// transient outage self-heals); a permanently broken endpoint pays one
/// probe summarization every 4th chunk, the accepted probe cadence.
const SUMMARY_MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// The summarizer input cap, in multiples of the digest row threshold
/// `digest_max_messages` (decision 65). The decision-62 failure
/// deferral widens the chunk by one digest range per failed cycle;
/// without a cap the summarizer input grows unboundedly. Two multiples
/// cover one natural chunk plus one deferred widening; the circuit
/// breaker bounds the widening anyway, so a chunk past the cap is
/// always the final attempt before the drop.
const SUMMARY_INPUT_CAP_DIGEST_MULTIPLE: usize = 2;

/// The platform's outbound text limit in CHARACTERS (Telegram: 4096).
/// Decision 100 (B1): a generated reply or warmup text longer than
/// this is an incident, never a truncation candidate — the actor
/// rejects it before the Rule B1 raw-log write; nothing persists,
/// nothing sends.
const MAX_OUTBOUND_TEXT_CHARS: usize = 4096;

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

/// One forced wake (mention/reply, Section 8.1): the queue entry of
/// `forced_pending` AND the failure context a `WakeCompleted` report
/// carries back, so the completion handler can apply the decision-65
/// requeue-once rule. In-memory only; nothing of it persists. `pub`
/// because the public `ActorCommand::WakeCompleted` names it; the
/// fields stay crate-internal.
#[derive(Debug, Clone)]
pub struct ForcedWakeEntry {
    /// The forcing message (the mention/reply that Section 8.1 obliges
    /// the bot to answer).
    forcing: GateMessage,
    /// The intake time of the forcing message: the queued wake's `now`,
    /// so it stays on the deterministic replay clock.
    forced_at: OffsetDateTime,
    /// True when this entry is the ONE bounded retry of a failed forced
    /// wake (decision 65): a second failure does NOT requeue again; the
    /// entry drops with a distinct ERROR naming the unmet Section 8.1
    /// must-respond obligation.
    retried: bool,
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
    /// The live-mode persona hot reload (decision 80, amending specs.md
    /// Section 6.1 rule 1): carries the freshly rendered preamble,
    /// broadcast by the binary's debounced watcher. It serializes
    /// through the FIFO inbox like every other command — it never
    /// interrupts in-flight work. The handler swaps context item 0 in
    /// memory (Rule C4); nothing persists (the persona file is the
    /// state). Decision 95: carries the unified speech tag alongside —
    /// a persona-name change swaps both the preamble and the tag.
    ReloadPreamble {
        preamble: String,
        pet_tag: String,
    },
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
    /// (internal plumbing, the same pattern as `DigestCompleted`). The
    /// report carries the failure context of decision 65 alongside the
    /// result, so a failed wake can roll `wake_last_row_id` back and a
    /// failed forced wake can requeue once.
    WakeCompleted {
        /// The wake outcome. Boxed: `WakeReport` is by far the largest
        /// payload of the inbox enum (clippy::large_enum_variant).
        result: Box<std::result::Result<WakeReport, CoreError>>,
        /// The `wake_last_row_id` value at wake START. A failed wake
        /// rolls the marker back to it, so the failed wake's messages
        /// re-present at the next natural trigger (decision 65).
        pre_wake_row_id: i64,
        /// The forced entry of this wake, when forced (Section 8.1).
        /// Its `retried` mark bounds the requeue-once rule.
        forced: Option<ForcedWakeEntry>,
    },
    /// The spawned warmup generation task reports its result through
    /// this command (internal plumbing, the same pattern as
    /// `WakeCompleted`; specs.md Section 9.7 step 3, decision 78). The
    /// slot was already consumed at fire time, so the handler never
    /// reschedules.
    WarmupCompleted {
        /// The generated warmup text, or the generation failure.
        /// Boxed: a `Result<String, CoreError>` payload must not widen
        /// the inbox enum (clippy::large_enum_variant).
        result: Box<std::result::Result<String, CoreError>>,
        /// The display name of the topic sampled at fire time (the
        /// curated warmup line and the cooldown entry name it).
        topic: String,
    },
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

    /// Non-blocking send (decision 80): the persona-reload broadcast
    /// skips a dead or backlogged actor instead of waiting (best-effort;
    /// the file is the state — the actor's next start reads it). The
    /// error collapses `TrySendError::Full` and `TrySendError::Closed`
    /// into `CoreError::InboxClosed` deliberately — the caller only
    /// needs skip-vs-ok (the same collapse `send` applies to its send
    /// error).
    pub fn try_send(&self, cmd: ActorCommand) -> Result<(), CoreError> {
        self.inbox.try_send(cmd).map_err(|_| CoreError::InboxClosed)
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
    /// The warmup-trigger services (Phase 2, decision 78). `None`
    /// disables the trigger entirely (replay mode, or no provider key —
    /// the same discipline as `wake: Option<WakeServices>`). `Some`
    /// runs the warmup procedure of specs.md Section 9.7 on the timer
    /// tick.
    pub warmup: Option<WarmupServices>,
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
    /// The unified speech tag of decision 95 (derived from the persona
    /// name by the caller via `tamako_persona::pet_tag_for_name`): the
    /// live context renders the bot's own speech with it, and the
    /// parrot-filter seams build the reply fence from it.
    pub pet_tag: String,
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

/// Runs one spawned LLM task body to completion, containing a panic
/// (the H4a supervision rule): a panicking digest/summary/wake task
/// reports a synthetic FAILURE through the inbox instead of vanishing
/// silently, so the completion handler ALWAYS runs and the in-flight
/// flag (`digest_in_flight` / `summary_pending` / `wake_in_flight`)
/// ALWAYS resets. The panic becomes the same failure class as a
/// provider error; the returned message carries the `task panicked`
/// marker, so the logs distinguish a panic from a provider failure.
///
/// Mechanism: `futures::FutureExt::catch_unwind` is the textbook
/// shape, but `futures` is not a dependency of tamako-core (nor of the
/// workspace) and the std covers the same ground: `poll_fn` +
/// `std::panic::catch_unwind`, no new dependency. The
/// `AssertUnwindSafe` is sound here: after a caught panic the inner
/// future is NEVER polled again — the wrapper resolves with the error
/// and drops it — which is exactly the contract `futures`' own
/// `CatchUnwind` relies on.
async fn contain_task_panic<F, T>(body: F) -> Result<T, String>
where
    F: std::future::Future<Output = T>,
{
    let mut body = Box::pin(body);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(output)) => std::task::Poll::Ready(Ok(output)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(payload) => std::task::Poll::Ready(Err(format!(
                "task panicked: {}",
                panic_payload_text(payload.as_ref())
            ))),
        }
    })
    .await
}

/// Renders the payload of a caught panic: the `&str` or `String` of a
/// `panic!` message; anything else (a `panic_any` payload) reports its
/// kind, since the payload is opaque.
fn panic_payload_text(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "a non-string panic payload".to_string()
    }
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

/// True when the persisted state has no usable `wake_last_row_id`: the
/// key is absent or its value is malformed. The key string is session.rs
/// `KEY_WAKE_LAST_ROW_ID` (private there); the startup repair spells it
/// out because it must distinguish an absent/malformed key from a
/// PERSISTED zero, which `SessionState::decode` maps to the same fresh
/// default.
fn wake_last_row_id_missing_or_malformed(persisted: &HashMap<String, String>) -> bool {
    persisted
        .get("wake_last_row_id")
        .and_then(|value| value.parse::<i64>().ok())
        .is_none()
}

/// The disabled-services marker advance (decision 65, the K2-latent
/// marker fix): a wake WOULD start here, but no gate/reply provider is
/// wired. Advance `wake_last_row_id` past the messages the wake would
/// have presented — the same tail rule as `start_wake` (the last
/// raw-log row above the marker, any direction) — so the presented
/// range cannot grow without bound while the services are off and
/// re-present as one huge batch when they are re-enabled. One tail
/// scan per suppressed fire: the same scan `start_wake` would have
/// done. The caller resets the scheduler (when the fire is a trigger
/// fire) and persists the session.
async fn advance_wake_marker_without_services(
    store: &Arc<Store>,
    chat_id: &str,
    session: &mut SessionState,
) -> Result<(), CoreError> {
    let after_id = session.wake_last_row_id;
    let gather_chat_id = chat_id.to_string();
    let rows = blocking_store(store, move |store| {
        store.list_messages_after(&gather_chat_id, after_id)
    })
    .await?;
    if let Some(tail_id) = rows.last().map(|row| row.id) {
        session.wake_last_row_id = tail_id;
    }
    Ok(())
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
        // H4a panic containment: a panicking pipeline reports a
        // synthetic failure through the inbox (the same failure class
        // as an infrastructure error), so the completion handler runs
        // and `digest_in_flight` ALWAYS resets.
        let result =
            contain_task_panic(async move { pipeline.run_digest(&chat_id, boundary).await })
                .await
                .unwrap_or_else(|message| Err(CoreError::Digest(message)));
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
        warmup: warmup_services,
        summary_provider,
        outbound,
        bot_name,
        pet_tag,
        ..
    } = params;
    let bot_name = bot_name.unwrap_or_else(|| "Tamako".to_string());

    // Decision 78 (specs.md Section 8.4): the host-local offset of the
    // warmup active-hours window and the quota-day boundary, resolved
    // ONCE at startup. The process never setenvs at runtime, so the
    // offset cannot change under the actor (the `local-offset`
    // soundness note of the time crate); tests pass explicit times and
    // use TZ-proof configs. A resolution failure is a startup-class
    // operator anomaly: one WARN naming the chat, then UTC.
    let local_offset = match UtcOffset::current_local_offset() {
        Ok(offset) => offset,
        Err(error) => {
            tracing::warn!(
                chat_id = %chat_id,
                %error,
                "the host-local UTC offset is unavailable; the warmup active-hours window falls back to UTC"
            );
            UtcOffset::UTC
        }
    };

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
    let mut session = SessionState::decode(&chat_id, &persisted, &config, started_at, &mut rng);
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

    // Decision 65 (the K2-latent marker fix): `SessionState::decode`
    // is a total function — a missing or malformed `wake_last_row_id`
    // key decodes to the fresh default 0. For a group with PRIOR log
    // rows that silently re-presents the ENTIRE raw log at the next
    // wake (an unbounded gate prompt; true duplicate responses are
    // possible). Scheduling state is P1-safe to recompute, so repair
    // it here: "start from now" — the marker moves to the raw-log
    // tail. The rebuild rows above are every row above the removal
    // cutoff of the append-only log, so their last id IS the log
    // tail; no extra store read is needed. A group with no rows keeps
    // 0, and a present, well-formed marker (even 0) is kept as
    // persisted. One WARN line with the chat id: the repair means
    // persisted scheduling state was lost or never written — an
    // operator-visible anomaly reported once per restart, not a
    // routine event (the state repair of a healthy group never logs).
    if wake_last_row_id_missing_or_malformed(&persisted) {
        if let Some(tail) = rows.last() {
            session.wake_last_row_id = tail.id;
            persist_session(&store, &chat_id, &session).await?;
            tracing::warn!(
                chat_id = %chat_id,
                wake_last_row_id = tail.id,
                "wake_last_row_id missing or malformed; repaired to the raw-log tail"
            );
        }
    }
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
    let mut context = LiveContext::rebuild(
        preamble,
        pet_tag,
        &rows,
        &injections,
        &reply_targets,
        &summaries,
    );

    // One digest at a time per group (Section 6.1, rule 2).
    let mut digest_in_flight = false;
    // One chunk summary at a time per group (decision 62): while a
    // summary task is in flight, the C3 mutation of its digest
    // completion is deferred and the digest trigger is suppressed.
    let mut summary_pending = false;
    // The M3 summarization circuit breaker (decision 65): CONSECUTIVE
    // summarization failures. In-memory on purpose (no new session
    // key): a restart re-probes the endpoint anyway, and the worst
    // case of a lost count is one extra deferral cycle. Resets on a
    // success and after a circuit-break drop.
    let mut summary_failures: u32 = 0;
    // One wake at a time per group (Section 6.2: a forced Wake queues
    // behind a running wake; it does not preempt it). The queued entry
    // carries the intake time of the forcing message, so the queued
    // wake stays on the deterministic replay clock, and the
    // requeue-once mark of decision 65.
    let mut wake_in_flight = false;
    let mut forced_pending: Option<ForcedWakeEntry> = None;
    // The forced-wake cooldown (specs.md Sections 8.1/6.2, decision 79
    // (c)): a forced wake that produced a reply suppresses new forcings
    // until this instant. An in-memory rate limiter, NOT persisted —
    // on a rebuild (Rule P1) the cooldown is simply not running, which
    // is acceptable; nothing about the rebuild changes (bit-identical).
    let mut forced_cooldown_until: Option<OffsetDateTime> = None;
    // One warmup generation at a time per group (decision 78, the
    // Section 6.1 rule-3 analog): the flag lives in memory next to
    // `wake_in_flight`; the spawned task's `WarmupCompleted` report
    // ALWAYS resets it.
    let mut warmup_in_flight = false;

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
                    forced_cooldown_until,
                    &inbox_sender,
                    msg,
                )
                .await?;
            }
            ActorCommand::Inbound(InboundEvent::EditedMessage(msg)) => {
                // specs.md Section 15, open item 4: an edit is appended as a
                // new log row with event_type Edit. It is never a
                // retraction. No other processing in Phase 0.
                //
                // Decision 65 (invisible-edit intake filter), the
                // identical-text exception: an edit whose text is
                // byte-identical to the LATEST persisted row of the same
                // platform_msg_id is not an event. The comparison target
                // is the newest row (highest id), not the original: edit
                // chains (A→B→A) treat each edit as a delta against the
                // current state, which is exactly the "did anything
                // change" question. The filter runs BEFORE the persist
                // step (Rule P1 order: persist → context append →
                // session → triggers): a dropped edit produces no log
                // row, no context item, no session mutation, and no
                // wake-counter advance. Telegram sends the full text, so
                // the comparison is byte-exact (no trimming).
                let lookup_chat_id = chat_id.clone();
                let lookup_pid = msg.platform_msg_id.clone();
                let latest = blocking_store(&store, move |store| {
                    store.find_latest_message_by_platform_msg_id(&lookup_chat_id, &lookup_pid)
                })
                .await;
                match latest {
                    Ok(Some(row)) if row.text == msg.text => {
                        debug!(
                            chat_id = %chat_id,
                            platform_msg_id = %msg.platform_msg_id,
                            "text-identical edit dropped (not an event)"
                        );
                        continue;
                    }
                    // No persisted row: the edit predates the bot's view
                    // (target unknown); keep the current behavior.
                    Ok(_) => {}
                    // Fail open: a lookup failure never loses data — treat
                    // the edit as a real edit. Not covered by an actor
                    // test: the harness holds a concrete `Arc<Store>` (no
                    // trait seam), so a store error is not injectable.
                    Err(error) => {
                        tracing::warn!(
                            chat_id = %chat_id,
                            platform_msg_id = %msg.platform_msg_id,
                            %error,
                            "latest-row lookup failed; treating the edit as a real edit (fail open)"
                        );
                    }
                }
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
                    &memory,
                    &context,
                    digest.as_ref(),
                    &mut digest_in_flight,
                    summary_pending,
                    wake_services.as_ref(),
                    &mut wake_in_flight,
                    warmup_services.as_ref(),
                    &mut warmup_in_flight,
                    &inbox_sender,
                    now,
                    local_offset,
                )
                .await?;
            }
            ActorCommand::WakeCompleted {
                result,
                pre_wake_row_id,
                forced,
            } => {
                wake_in_flight = false;
                match *result {
                    Err(error) => {
                        // Decision 65 (amending decision 33's
                        // reset-at-start): roll `wake_last_row_id` back
                        // to its pre-wake value, so the failed wake's
                        // messages re-present at the next NATURAL
                        // trigger. The wake scheduler stays reset (the
                        // floor/threshold still gates), so there is no
                        // immediate retry storm. NO inline retry of an
                        // unforced wake (specs.md Section 9 failure
                        // handling). A SUCCESSFUL wake keeps the
                        // reset-at-start advance (nothing changes).
                        session.wake_last_row_id = pre_wake_row_id;
                        persist_session(&store, &chat_id, &session).await?;
                        match forced {
                            Some(entry) if !entry.retried => {
                                // The Section 8.1 must-respond
                                // obligation gets ONE bounded retry:
                                // the forced wake requeues (marked), so
                                // a second failure cannot requeue again.
                                // The requeue honors the Section 6.2
                                // "newest address wins" rule: a forcing
                                // queued DURING the failed wake is
                                // newer, so it supersedes the retry
                                // (the rollback above means its wake
                                // re-presents the failed wake's
                                // messages anyway).
                                if forced_pending.is_some() {
                                    tracing::warn!(chat_id = %chat_id, %error, "wake procedure failed; a newer forced wake is already queued — the newest address wins, no requeue");
                                } else {
                                    tracing::error!(chat_id = %chat_id, %error, "wake procedure failed; the forced wake requeues once (specs.md Section 8.1)");
                                    forced_pending = Some(ForcedWakeEntry {
                                        retried: true,
                                        ..entry
                                    });
                                }
                            }
                            Some(entry) => {
                                // The requeued forced wake failed
                                // again: drop it with a DISTINCT error
                                // naming the unmet obligation.
                                tracing::error!(
                                    chat_id = %chat_id,
                                    %error,
                                    forcing_row_id = entry.forcing.row_id,
                                    "the requeued forced wake failed again; dropping it — the must-respond obligation of specs.md Section 8.1 is unmet"
                                );
                            }
                            None => {
                                tracing::error!(chat_id = %chat_id, %error, "wake procedure failed; skipping this wake");
                            }
                        }
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
                            forced.as_ref().map(|entry| entry.forced_at),
                            &mut forced_cooldown_until,
                        )
                        .await?;
                    }
                }
                // Section 6.2: a queued forced Wake moves to the head of
                // the queue; it starts immediately after the current
                // wake completes. The intake time of the forcing message
                // is its `now` (deterministic replay). A forced wake
                // requeued by the failure path above starts here too.
                // Decision 79 (c) extension: when the completion JUST
                // started the forced-wake cooldown (the reply went out),
                // a queued forcing inside the window is dropped with the
                // suppression DEBUG — otherwise it would fire immediately
                // and produce the back-to-back replies the cooldown
                // rules against. The drop leaves no queue entry
                // (specs.md Section 6.2); the mention is already in the
                // log/context and lands in the next natural wake's
                // presented set.
                if let Some(services) = wake_services.as_ref() {
                    if let Some(entry) = forced_pending.take() {
                        if forced_cooldown_until.is_some_and(|until| entry.forced_at < until) {
                            debug!(chat_id = %chat_id, "forced wake suppressed: the forced-wake cooldown is running");
                        } else {
                            let forced_at = entry.forced_at;
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
                                Some(entry),
                                forced_at,
                                "forced",
                            )
                            .await?;
                        }
                    }
                }
            }
            ActorCommand::WarmupCompleted { result, topic } => {
                // The flag resets ALWAYS (the H4a containment guarantee:
                // every spawned task reports back, even a panicking
                // one).
                warmup_in_flight = false;
                match *result {
                    Err(error) => {
                        // The slot was consumed at fire time, so there is
                        // nothing to reschedule: the next slot is the
                        // natural retry. Nothing sent, nothing persisted.
                        tracing::error!(chat_id = %chat_id, %error, "warmup generation failed; the slot is consumed — the next slot is the natural retry");
                    }
                    Ok(raw_text) => {
                        handle_warmup_report(
                            &store,
                            &chat_id,
                            &config,
                            &mut session,
                            &mut context,
                            outbound.as_ref(),
                            &bot_name,
                            raw_text,
                            &topic,
                            local_offset,
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
                                    let mut rows = blocking_store(&store, move |store| {
                                        store.list_messages_in_range(&rows_chat_id, lower, b_old)
                                    })
                                    .await?;
                                    // Decision 65 input cap: the
                                    // decision-62 failure deferral
                                    // widens the chunk by one digest
                                    // range per failed cycle, so without
                                    // a cap the summarizer input grows
                                    // unboundedly. Cap the INPUT at
                                    // twice the digest row threshold
                                    // (one natural chunk plus one
                                    // deferred widening). An oversized
                                    // chunk summarizes only its NEWEST
                                    // cap-sized suffix (the oldest
                                    // context is the least valuable),
                                    // while the summary row is recorded
                                    // against the FULL removed range:
                                    // the row's (first,last) key covers
                                    // the removal, the content covers
                                    // the suffix — asymmetric on
                                    // purpose; partial memory beats
                                    // none.
                                    let cap = (SUMMARY_INPUT_CAP_DIGEST_MULTIPLE
                                        * config.digest_max_messages as usize)
                                        .max(1);
                                    let rows = if rows.len() > cap {
                                        debug!(
                                            chat_id = %chat_id,
                                            range = %format!("({lower},{b_old}]"),
                                            rows = rows.len(),
                                            cap,
                                            "the summarizer input exceeds the cap; summarizing the newest suffix only"
                                        );
                                        rows.split_off(rows.len() - cap)
                                    } else {
                                        rows
                                    };
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
                                        // H4a panic containment (the
                                        // digest-task pattern): a
                                        // panicking summarizer reports a
                                        // synthetic failure, so
                                        // `summary_pending` ALWAYS
                                        // resets and the decision-62
                                        // failure deferral applies.
                                        let result = contain_task_panic(async move {
                                            provider
                                                .summarize(&summary_chat_id, lower, b_old, &rows)
                                                .await
                                        })
                                        .await
                                        .unwrap_or_else(|message| {
                                            Err(SummaryError::Provider(message))
                                        });
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
                        // A success resets the decision-65 circuit
                        // breaker's consecutive-failure count.
                        summary_failures = 0;
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
                        // The Section 12-style counter (spec-backfill
                        // note): EVERY summarization failure counts, so
                        // `--status` surfaces a stuck group. Best
                        // effort like the other counters; incremented
                        // before the circuit-break check.
                        summary_failures += 1;
                        bump_counter(&store, &chat_id, "summaries_failed_total").await;
                        if summary_failures >= SUMMARY_MAX_CONSECUTIVE_FAILURES {
                            // The M3 circuit breaker (decision 65):
                            // three consecutive failures across three
                            // digest completions mean the endpoint is
                            // durably broken, and wedging the context
                            // growth forever is worse than losing the
                            // summary. Fall back to the sanctioned
                            // old-C3 drop FOR THIS CHUNK (removal
                            // proceeds without a summary, exactly like
                            // the None-provider behavior) with one
                            // ERROR naming the chat and the dropped
                            // range. The breaker then RESETS: the next
                            // chunk tries summarization again, so a
                            // transient outage self-heals and a
                            // permanently broken endpoint pays one probe
                            // summarization every 4th chunk.
                            tracing::error!(
                                chat_id = %chat_id,
                                %error,
                                range = %format!("({first_msg_id},{last_msg_id}]"),
                                consecutive_failures = summary_failures,
                                "context summarization failed too many times in a row; dropping the chunk without a summary (the old-C3 fallback)"
                            );
                            summary_failures = 0;
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
                        } else {
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
            ActorCommand::ReloadPreamble { preamble, pet_tag } => {
                // Decision 80 (specs.md Section 6.1 rule 1 as amended):
                // the live-mode persona hot reload. IN-MEMORY ONLY: the
                // Rule C4 item-0 swap mutates no session state and
                // persists nothing — the persona file is the state; a
                // restart rebuild renders the same bytes (Rule P1
                // untouched). Decision 53: DEBUG only — the curated INFO
                // line lives at the binary's watcher site, not here.
                let preamble_len = preamble.len();
                context.reload_preamble(preamble);
                context.set_pet_tag(pet_tag);
                debug!(
                    chat_id = %chat_id,
                    preamble_len,
                    "persona preamble reloaded (in-memory item-0 swap)"
                );
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
    forced_pending: &mut Option<ForcedWakeEntry>,
    // The forced-wake cooldown (decision 79 (c)); intake only READS it
    // (a copy — the completion handler owns the mutation).
    forced_cooldown_until: Option<OffsetDateTime>,
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
        // lines stay at debug. Decision 65: BOTH branches still
        // advance `wake_last_row_id` past the messages the suppressed
        // wake would have presented (the Section 9.6 gather range), so
        // the range cannot grow without bound while the services are
        // off. No gate/reply call, no outbound — only the scheduling
        // marker moves.
        if msg.mentions_bot || msg.is_reply_to_bot {
            // specs.md Section 8.1: the bot must respond when addressed
            // directly. The muted state does not suppress a forced wake.
            debug!(chat_id = %chat_id, "wake services disabled; forced wake ignored");
            advance_wake_marker_without_services(store, chat_id, session).await?;
            persist_session(store, chat_id, session).await?;
        } else if wake.should_fire(now, config) {
            debug!(chat_id = %chat_id, "wake services disabled; wake trigger ignored");
            advance_wake_marker_without_services(store, chat_id, session).await?;
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
        if forced_cooldown_until.is_some_and(|until| now < until) {
            // Suppressed (specs.md Sections 8.1/6.2, decision 79 (c)):
            // the row is persisted and in the context (Rule P1
            // untouched — it lands in the next wake's presented set);
            // NO wake fires, NO queue entry, `forced_pending`
            // untouched, `wake_last_row_id` untouched.
            debug!(chat_id = %chat_id, "forced wake suppressed: the forced-wake cooldown is running");
        } else if *wake_in_flight {
            // Section 6.2: a forced Wake moves to the head of the queue;
            // it does not preempt a running call. It starts immediately
            // after the current wake completes. A second forced wake
            // replaces the queued one (the newest address wins). The
            // queued wake gets its own curated line at completion.
            debug!(chat_id = %chat_id, "a wake is in flight; the forced wake is queued");
            *forced_pending = Some(ForcedWakeEntry {
                forcing,
                forced_at: now,
                // A fresh forcing has its full requeue budget (decision
                // 65); only a failure-requeued entry is marked.
                retried: false,
            });
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
                Some(ForcedWakeEntry {
                    forcing,
                    forced_at: now,
                    retried: false,
                }),
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
/// 6.2: Digest runs BEFORE Wake. Decision 78: the warmup trigger
/// (specs.md Section 9.7) evaluates LAST, so the tick order is Digest →
/// Wake → Warmup. A tick never forces a wake (forcing needs a
/// mention/reply, Section 8.1), so there is no `forced_pending`
/// parameter here.
#[allow(clippy::too_many_arguments)]
async fn handle_tick<M: MemoryBackend>(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    wake: &mut WakeScheduler,
    rng: &mut StdRng,
    memory: &Arc<M>,
    context: &LiveContext,
    digest: Option<&Arc<dyn DigestPipeline>>,
    digest_in_flight: &mut bool,
    summary_pending: bool,
    wake_services: Option<&WakeServices>,
    wake_in_flight: &mut bool,
    warmup_services: Option<&WarmupServices>,
    warmup_in_flight: &mut bool,
    inbox_sender: &mpsc::Sender<ActorCommand>,
    now: OffsetDateTime,
    local_offset: UtcOffset,
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
    // Decision 78: NO early return on the disabled-wake path — the
    // warmup evaluation below runs on every tick regardless of the
    // wake wiring (the triggers are independent).
    if let Some(services) = wake_services {
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
    } else {
        // The wake services are disabled (no LLM key wired); the
        // behavior matches the intake path exactly, marker advance
        // included (decision 65).
        if wake.should_fire(now, config) {
            debug!(chat_id = %chat_id, "wake services disabled; wake trigger ignored");
            advance_wake_marker_without_services(store, chat_id, session).await?;
            reset_wake(config, session, wake, rng, now);
            persist_session(store, chat_id, session).await?;
        }
    }
    // Decision 78: the warmup trigger evaluates LAST (Digest → Wake →
    // Warmup) and ONLY here — intake never evaluates it.
    handle_warmup_tick(
        store,
        chat_id,
        config,
        session,
        memory,
        rng,
        context,
        warmup_services,
        warmup_in_flight,
        inbox_sender,
        now,
        local_offset,
    )
    .await?;
    Ok(())
}

/// The Section 9.7 step-1 failure path (decision 78): CONSUME the due
/// slot — `warmup_next_at` reschedules from the spent slot (`prev =
/// Some(due)`, so the Section 8.5 backoff spacing applies), never a
/// same-slot retry — and persist. The caller logs the one DEBUG line
/// naming the failed gate first. A `None` schedule is a defensive
/// invariant: decision 79 (a) floored the effective quota at 1, so
/// only the pathological-multiplier bound of `schedule_next` can yield
/// `None`; it is retried on a later tick, costing nothing.
#[allow(clippy::too_many_arguments)]
async fn consume_warmup_slot(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    rng: &mut StdRng,
    due: OffsetDateTime,
    now: OffsetDateTime,
    local_offset: UtcOffset,
) -> Result<(), CoreError> {
    session.warmup_next_at = schedule_next(
        config,
        session.warmup_backoff_factor,
        Some(due),
        now,
        local_offset,
        rng,
    );
    persist_session(store, chat_id, session).await
}

/// The warmup trigger of specs.md Section 9.7 (decision 78), evaluated
/// on the actor's timer tick ONLY, after the wake evaluation. Runs
/// inside the actor loop; only the LLM call leaves it (a spawned task
/// reports `WarmupCompleted` back through the FIFO inbox).
#[allow(clippy::too_many_arguments)]
async fn handle_warmup_tick<M: MemoryBackend>(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    memory: &Arc<M>,
    rng: &mut StdRng,
    context: &LiveContext,
    services: Option<&WarmupServices>,
    warmup_in_flight: &mut bool,
    inbox_sender: &mpsc::Sender<ActorCommand>,
    now: OffsetDateTime,
    local_offset: UtcOffset,
) -> Result<(), CoreError> {
    // Unwired services (replay mode, or no provider key) disable the
    // trigger entirely; the startup WARN of the binary covers the
    // unwired state, so this stays silent. The `warmup` master switch
    // (Section 8.4) is silent too. Neither path consumes or touches
    // state.
    let Some(services) = services else {
        return Ok(());
    };
    if !config.warmup {
        return Ok(());
    }

    // The engagement watch of the previous warmup resolves FIRST
    // (Section 8.5, decision 78 (d)): at window expiry, engagement
    // resets the soft backoff and silence increments it. A missing
    // expiry self-heals as expired.
    if session.warmup_watch_pending {
        let expires = session.warmup_watch_expires_at.unwrap_or(now);
        if now >= expires {
            resolve_warmup_watch(store, chat_id, session, expires).await?;
        }
    }

    // The quota-day rollover (decision 78 (b)): the counter belongs to
    // one host-local day. Persists only when the day changed.
    let today = local_date_string(now, local_offset);
    if session.warmup_quota_day.as_deref() != Some(today.as_str()) {
        session.warmup_quota_day = Some(today.clone());
        session.warmup_quota_used_today = 0;
        persist_session(store, chat_id, session).await?;
    }

    // Scheduling (Section 8.4, Rule P1): a fresh group draws its first
    // slot once and persists it; a restart never reshuffles. The fresh
    // slot is in the future by construction, so this tick is done
    // either way; a `None` (defensive: decision 79 (a) floored the
    // effective quota at 1, so only the pathological-multiplier bound
    // can yield it) is retried on a later tick, costing nothing.
    if session.warmup_next_at.is_none() {
        session.warmup_next_at = schedule_next(
            config,
            session.warmup_backoff_factor,
            None,
            now,
            local_offset,
            rng,
        );
        if session.warmup_next_at.is_some() {
            persist_session(store, chat_id, session).await?;
        }
        return Ok(());
    }
    let due = session
        .warmup_next_at
        .expect("the scheduling branch above guarantees Some");
    // Not due: the normal state — NO log.
    if due > now {
        return Ok(());
    }

    // The Section 9.7 step-1 gates, in order. On ANY failure: one DEBUG
    // line naming the failed gate, then consume the slot and return.
    if session.muted {
        debug!(chat_id = %chat_id, gate = "muted", "warmup slot skipped; the slot is consumed and the next slot is scheduled");
        return consume_warmup_slot(store, chat_id, config, session, rng, due, now, local_offset)
            .await;
    }
    if session.warmup_quota_used_today
        >= effective_quota(config.warmup_quota, session.warmup_backoff_factor)
    {
        debug!(chat_id = %chat_id, gate = "quota", "warmup slot skipped; the slot is consumed and the next slot is scheduled");
        return consume_warmup_slot(store, chat_id, config, session, rng, due, now, local_offset)
            .await;
    }
    // The silence gate (Section 8.4): the newest raw-log row of ANY
    // direction must be at least `warmup_silence` old — the bot's own
    // speech also breaks group silence. An EMPTY log passes the gate:
    // the topic pick below suppresses a topicless group anyway.
    let tail_chat_id = chat_id.to_string();
    let newest = blocking_store(store, move |store| {
        store.list_latest_messages(&tail_chat_id, 1)
    })
    .await?;
    let silence = time::Duration::try_from(config.warmup_silence).unwrap_or(time::Duration::MAX);
    let silent = match newest.last() {
        Some(row) => now - row.timestamp >= silence,
        None => true,
    };
    if !silent {
        debug!(chat_id = %chat_id, gate = "silence", "warmup slot skipped; the slot is consumed and the next slot is scheduled");
        return consume_warmup_slot(store, chat_id, config, session, rng, due, now, local_offset)
            .await;
    }
    if session.warmup_watch_pending {
        // A warmup is already out (the watch has not expired yet).
        debug!(chat_id = %chat_id, gate = "watch", "warmup slot skipped; the slot is consumed and the next slot is scheduled");
        return consume_warmup_slot(store, chat_id, config, session, rng, due, now, local_offset)
            .await;
    }
    if *warmup_in_flight {
        debug!(chat_id = %chat_id, gate = "in_flight", "warmup slot skipped; the slot is consumed and the next slot is scheduled");
        return consume_warmup_slot(store, chat_id, config, session, rng, due, now, local_offset)
            .await;
    }

    // Step 2: the topic pick. A sampling failure is one ERROR, never
    // propagated — the actor must not die over a warmup.
    let candidates = match memory
        .sample_interest_topics(chat_id, TOPIC_SAMPLE_LIMIT)
        .await
    {
        Ok(candidates) => candidates,
        Err(error) => {
            tracing::error!(chat_id = %chat_id, %error, "warmup topic sampling failed; the slot is consumed — the next slot is the natural retry");
            return consume_warmup_slot(
                store,
                chat_id,
                config,
                session,
                rng,
                due,
                now,
                local_offset,
            )
            .await;
        }
    };
    let tail_chat_id = chat_id.to_string();
    let tail = blocking_store(store, move |store| {
        store.list_latest_messages(&tail_chat_id, TAIL_EXCLUSION_ROWS)
    })
    .await?;
    let tail_texts: Vec<String> = tail.into_iter().map(|row| row.text).collect();
    prune_cooldowns(
        &mut session.warmup_topic_cooldowns,
        &today,
        config.warmup_topic_cooldown_days,
    );
    let Some(topic) = pick_topic(
        &candidates,
        &session.warmup_topic_cooldowns,
        &tail_texts,
        &today,
        config.warmup_topic_cooldown_days,
        now,
        rng,
    ) else {
        // Section 9.7 step 2: no eligible topic means no warmup —
        // forced small talk is worse than silence.
        debug!(chat_id = %chat_id, "warmup slot skipped: no eligible topic; the slot is consumed and the next slot is scheduled");
        return consume_warmup_slot(store, chat_id, config, session, rng, due, now, local_offset)
            .await;
    };

    // FIRE. FIRST consume the slot and persist (this also persists the
    // pruned cooldowns and the rolled-over quota day): a generation
    // failure below then needs no reschedule — the slot is spent and
    // the next slot is the natural retry. Then the generation runs in a
    // spawned task (the Section 6.1 rule-3 analog): the FIFO never
    // blocks on an LLM call. The task gets a SNAPSHOT of the live
    // context taken NOW; it touches NO actor state.
    session.warmup_next_at = schedule_next(
        config,
        session.warmup_backoff_factor,
        Some(due),
        now,
        local_offset,
        rng,
    );
    persist_session(store, chat_id, session).await?;
    let request = crate::warmup::WarmupRequest {
        messages: context.messages_for_llm(),
        topic: topic.name.clone(),
    };
    *warmup_in_flight = true;
    let generator = Arc::clone(&services.generator);
    let topic_name = topic.name;
    let sender = inbox_sender.clone();
    let chat_id_owned = chat_id.to_string();
    tokio::spawn(async move {
        // H4a panic containment (the wake-task pattern): a panicking
        // generator reports a synthetic `CoreError::Warmup` failure, so
        // the completion handler runs and `warmup_in_flight` ALWAYS
        // resets.
        let result =
            contain_task_panic(
                async move { generator.generate_warmup(&chat_id_owned, &request).await },
            )
            .await
            .unwrap_or_else(|message| Err(CoreError::Warmup(message)));
        // A failed send means the actor is shutting down; the result is
        // dropped (same rule as the wake task).
        let _ = sender
            .send(ActorCommand::WarmupCompleted {
                result: Box::new(result),
                topic: topic_name,
            })
            .await;
    });
    Ok(())
}

/// Resolves the engagement watch of the last sent warmup at window
/// expiry (specs.md Section 8.5, decision 78 (d)). Engagement resets the
/// soft backoff to zero; silence increments it (saturating). One DEBUG
/// line carries the verdict and the channel (`reply` / `reaction` /
/// `none`; when both a reply and a reaction qualify, `reply` wins). The
/// watch then closes — the row id, sent, and expiry timestamps stay
/// persisted for forensics — and the session persists.
async fn resolve_warmup_watch(
    store: &Arc<Store>,
    chat_id: &str,
    session: &mut SessionState,
    expires: OffsetDateTime,
) -> Result<(), CoreError> {
    // Engaged-by-reply: any INBOUND row above the watched warmup row
    // (id > watch row implies it arrived after the warmup row) whose
    // timestamp is at or before the expiry — the timestamp bound keeps
    // the window honest when the cadence evaluates late.
    let watch_row_id = session.warmup_watch_row_id.unwrap_or(0);
    let replies_chat_id = chat_id.to_string();
    let rows = blocking_store(store, move |store| {
        store.list_messages_after(&replies_chat_id, watch_row_id)
    })
    .await?;
    let engaged_by_reply = rows
        .iter()
        .any(|row| row.direction == Direction::Inbound && row.timestamp <= expires);

    // Engaged-by-reaction: a bounded recent window of reaction rows
    // whose timestamp lies in (sent_at, expires]. A candidate counts IFF
    // its platform_msg_id is ABSENT from the messages table — the
    // documented approximation of Section 8.5 "a reaction arrives within
    // the window": outbound rows carry synthetic `bot-out:{nanos}` ids
    // and the adapter returns no real sent id (Rule A3), so a reaction
    // to an UNLOGGED message inside the window is treated as a reaction
    // to the warmup, while a reaction to a LOGGED human message never
    // counts. Reactions reach administrator groups only (Section 4.2).
    // A missing `sent_at` yields an empty candidate window (only
    // replies can engage); the state self-heals at the next fire.
    let sent_at = session.warmup_watch_sent_at.unwrap_or(expires);
    let reactions_chat_id = chat_id.to_string();
    let reactions = blocking_store(store, move |store| {
        store.list_latest_reactions(&reactions_chat_id, 100)
    })
    .await?;
    let candidates: HashSet<String> = reactions
        .iter()
        .filter(|reaction| sent_at < reaction.timestamp && reaction.timestamp <= expires)
        .map(|reaction| reaction.platform_msg_id.clone())
        .collect();
    let mut engaged_by_reaction = false;
    if !candidates.is_empty() {
        // The distinct target ids resolve in ONE blocking pass (the
        // `resolve_reply_targets` pattern).
        let lookup = candidates.clone();
        let lookup_chat_id = chat_id.to_string();
        let logged: HashSet<String> = blocking_store(store, move |store| {
            let mut logged = HashSet::new();
            for pid in &lookup {
                if store
                    .find_latest_message_by_platform_msg_id(&lookup_chat_id, pid)?
                    .is_some()
                {
                    logged.insert(pid.clone());
                }
            }
            Ok(logged)
        })
        .await?;
        engaged_by_reaction = candidates.iter().any(|pid| !logged.contains(pid));
    }

    let channel = if engaged_by_reply {
        "reply"
    } else if engaged_by_reaction {
        "reaction"
    } else {
        "none"
    };
    let engaged = engaged_by_reply || engaged_by_reaction;
    if engaged {
        bump_counter(store, chat_id, "warmup_engaged_total").await;
        session.warmup_backoff_factor = 0;
    } else {
        session.warmup_backoff_factor = session.warmup_backoff_factor.saturating_add(1);
    }
    debug!(chat_id = %chat_id, channel, engaged, "warmup engagement watch resolved");
    session.warmup_watch_pending = false;
    persist_session(store, chat_id, session).await
}

/// The completion side of the warmup trigger (specs.md Section 9.7
/// steps 3-5, decision 78). Runs inside the actor loop on
/// `WarmupCompleted(Ok(..))`. The send path mirrors the wake's: the
/// parrot filter, the outbound raw-log row FIRST (Rule B1), a plain
/// standalone send, the Rule C1 context append, the monologue lock —
/// then the quota/cooldown/watch session state and the ONE curated
/// INFO line of decision 78.
#[allow(clippy::too_many_arguments)]
async fn handle_warmup_report(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    context: &mut LiveContext,
    outbound: Option<&mpsc::Sender<OutboundAction>>,
    bot_name: &str,
    raw_text: String,
    topic: &str,
    local_offset: UtcOffset,
) -> Result<(), CoreError> {
    // The parrot filter (Section 9.7 step 3): decisions 59/64 apply to
    // the warmup text like every reply. Decision 96 (F5): on the live
    // path the generator seam already filtered AND warned, so this
    // idempotent second pass fires its WARN only for generators
    // without a seam filter (the scripted doubles).
    let filtered =
        filter_reply_parrot_lines(&raw_text, &ReplyFence::for_pet_tag(context.pet_tag()));
    if filtered.stripped_parrot {
        tracing::warn!(chat_id = %chat_id, "the warmup text carries imitated structure or fence debris: the parrot filter stripped the affected lines");
    }
    if filtered.text.is_empty() {
        // The wake empty-reply analog: nothing persisted, nothing sent.
        tracing::error!(chat_id = %chat_id, topic = %topic, "the warmup text is empty after the parrot filter; nothing is sent");
        return Ok(());
    }
    // Decision 100 (B1): the overlength reject of the wake path applies
    // to warmup speech alike — nothing persists, nothing sends, no
    // quota consumed, no engagement watch opened.
    let chars = filtered.text.chars().count();
    if chars > MAX_OUTBOUND_TEXT_CHARS {
        tracing::error!(chat_id = %chat_id, topic = %topic, chars, limit = MAX_OUTBOUND_TEXT_CHARS, "the generated warmup exceeds the platform character limit; the warmup is rejected before persist");
        return Ok(());
    }
    let text = filtered.text;

    // Rule B1 FIRST (the wake send path's exact pattern): the outbound
    // raw-log row persists BEFORE the send — the log is the source of
    // truth; never speak without logging. The synthetic id: the adapter
    // contract (Rule A3) returns no platform id for a sent message, so
    // the row carries a local synthetic id; nanosecond time keeps the
    // idempotency key unique.
    let now_utc = OffsetDateTime::now_utc();
    let row = NewMessage {
        platform_msg_id: format!("bot-out:{}", now_utc.unix_timestamp_nanos()),
        direction: Direction::Outbound,
        event_type: EventType::Message,
        timestamp: now_utc,
        sender_id: "bot".to_string(),
        sender_display_name: bot_name.to_string(),
        sender_username: None,
        text: text.clone(),
        // Section 9.7 step 4: proactive speech never quotes a target.
        reply_to_platform_msg_id: None,
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
        // id) aborts this warmup's send path with an error log. The
        // error is NOT propagated: the actor must not die over one
        // warmup.
        Ok(InsertOutcome::Duplicate) => {
            tracing::error!(chat_id = %chat_id, "outbound raw-log insert returned Duplicate; the warmup is not sent");
            return Ok(());
        }
        Err(error) => {
            tracing::error!(chat_id = %chat_id, %error, "failed to persist the outbound raw-log row; the warmup is not sent");
            return Ok(());
        }
    };
    // The outbound action (Section 4.2: outbound failures are
    // tolerated). `try_send`: the actor never blocks on the sink; a
    // full or closed channel degrades to a logged drop — the raw-log
    // row above is already the source of truth. A PLAIN standalone
    // message: a warmup has no target to quote.
    let action = OutboundAction::SendText {
        chat_id: chat_id.to_string(),
        text: text.clone(),
        reply_to_platform_msg_id: None,
    };
    match outbound {
        Some(sink) => {
            if let Err(error) = sink.try_send(action) {
                tracing::error!(chat_id = %chat_id, %error, "outbound action dropped (channel full or closed); the raw-log row is persisted");
            }
        }
        None => debug!(chat_id = %chat_id, "outbound action dropped: no outbound sink wired"),
    }
    // Rule C1: the live context gets the same item. The Section 8.5
    // monologue lock covers warmup speech. The counter is best effort
    // (Section 12).
    context.append_bot_speech(row_id, now_utc, &text);
    session.record_bot_message(config);
    bump_counter(store, chat_id, "warmups_total").await;

    // The Section 9.7 step-5 session state. The quota counts on the
    // COMPLETION-time host-local day (a midnight straddle between fire
    // and completion counts on the completion day — immaterial). The
    // cooldown entry keys on the NORMALIZED topic name (the pick's
    // exclusion key). The engagement watch opens for
    // `warmup_reaction_window` (Section 8.5).
    session.warmup_quota_used_today = session.warmup_quota_used_today.saturating_add(1);
    let today = local_date_string(now_utc, local_offset);
    session.warmup_quota_day = Some(today.clone());
    session
        .warmup_topic_cooldowns
        .insert(tamako_memory::identifiers::normalize(topic), today);
    session.warmup_watch_row_id = Some(row_id);
    session.warmup_watch_sent_at = Some(now_utc);
    session.warmup_watch_expires_at = Some(now_utc + config.warmup_reaction_window);
    session.warmup_watch_pending = true;
    persist_session(store, chat_id, session).await?;

    // The curated line (decision 53 discipline; decision 78 adds this
    // ONE new curated line kind): exactly one INFO `warmup` line per
    // sent warmup. Gate skips stay DEBUG; there are no other INFO-level
    // warmup lines.
    info!(chat_id = %chat_id, topic = %topic, action = "sent", "warmup");
    Ok(())
}

/// The wake procedure of specs.md Section 9, steps 1-5, actor side.
/// Runs inside the actor loop; only the LLM calls leave the loop (they
/// run in a spawned task over owned data and report back through the
/// FIFO inbox as `WakeCompleted`). `trigger` is the telemetry spelling
/// of what started this wake (`"forced"` or a `FireReason`); it lands
/// on the curated wake log line. `forced` carries the full forced entry
/// (not just the forcing message), so the failure report can apply the
/// decision-65 requeue-once rule.
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
    forced: Option<ForcedWakeEntry>,
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
    // counter failure logs and never aborts the wake. Decision 65
    // amends this on the FAILURE path only: the completion handler
    // rolls `wake_last_row_id` back to `pre_wake_row_id`, so a failed
    // wake loses no messages (they re-present at the next natural
    // trigger). On success the advance below stands unchanged.
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

    // Decision 72 (specs.md Sections 9.2 and 9.6): the shared context
    // view, rendered ONCE per wake. The bound is `after_id` — the
    // PRE-ADVANCE marker read at step 2 (the step-3 advance above
    // already moved `session.wake_last_row_id`): the wake's new
    // messages stay OUT of the view and render in the gates' per-call
    // sections. The SAME bytes reach both gate calls of this wake (the
    // recall relevance gate and the participation gate), but the two
    // gates carry DISJOINT preambles and never warm each other — the
    // provider-cache win is per-gate and cross-WAKE: each gate's own
    // prefix grows across consecutive wakes (the append-only tail,
    // Rules C1/C2). The `gate_context` kill switch passes None and
    // restores the pre-72 delta-only input.
    let context_view = if config.gate_context {
        Some(context.gate_context_view(after_id))
    } else {
        None
    };

    // Step 5: the LLM calls run in a spawned task (the Section 6.1
    // rule 3 analog). The context is actor-owned (Section 6.1 rule 2),
    // so the task gets a SNAPSHOT taken NOW; it touches NO actor state
    // at all. Inbound messages during the call are logged and appended;
    // they do not interrupt it (Section 6.2).
    let snapshot = context.messages_for_llm();
    // Decision 95: the reply fence of the wake task derives from the
    // CURRENT speech tag; it travels as an owned value (the task
    // touches no actor state).
    let pet_tag = context.pet_tag().to_string();
    *wake_in_flight = true;
    let recall = Arc::clone(&services.recall);
    let gate = Arc::clone(&services.gate);
    let reply = Arc::clone(&services.reply);
    let task_chat_id = chat_id.to_string();
    let sender = inbox_sender.clone();
    // The decision-65 failure context: the pre-wake marker (the gather
    // floor of this wake) and the forced entry travel with the report,
    // so a failure can roll the marker back and requeue a forced wake
    // once. The wake calls themselves consume only the forcing message.
    let pre_wake_row_id = after_id;
    let failure_forced = forced.clone();
    let run_forced = forced.map(|entry| entry.forcing);
    tokio::spawn(async move {
        // H4a panic containment (the digest-task pattern): a panicking
        // recall/gate/reply call reports a synthetic failure, so the
        // wake-skip handler runs and `wake_in_flight` ALWAYS resets.
        let result = contain_task_panic(async move {
            run_wake_calls(
                &task_chat_id,
                recall,
                gate,
                reply,
                new_messages,
                snapshot,
                run_forced,
                tail_id,
                trigger,
                context_view,
                pet_tag,
            )
            .await
        })
        .await
        .unwrap_or_else(|message| Err(CoreError::Wake(message)));
        // A failed send means the actor is shutting down; the result is
        // dropped (same rule as the digest task).
        let _ = sender
            .send(ActorCommand::WakeCompleted {
                result: Box::new(result),
                pre_wake_row_id,
                forced: failure_forced,
            })
            .await;
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
/// for the curated wake log line. `context_view` is the decision-72
/// shared context view (`Some`) rendered once at wake start from the
/// pre-advance marker, or `None` (the `gate_context` kill switch); the
/// SAME bytes go to the recall relevance gate and the participation
/// gate, but the two gates have disjoint preambles and never warm each
/// other — the provider-cache win is per-gate across wakes (each gate
/// re-sees its own growing prefix).
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
    context_view: Option<String>,
    // Decision 95: the wake task runs over owned data and never touches
    // actor state, so the unified speech tag travels as a value; the
    // parrot filter below builds the reply fence from it.
    pet_tag: String,
) -> Result<WakeReport, CoreError> {
    // Step 2 (Sections 9.1-9.5): recall before the gate. The rendered
    // injection texts enter the gate input (Section 9.6: the recall
    // result is gate input on purpose) AND the reply-model snapshot:
    // the injection is part of the context from step 2 on, so the
    // reply model of step 4 sees it (Section 9.4, Rule C2 tail
    // position). The dedup rows and the context append happen in the
    // completion handler (Section 9.3), regardless of the gate
    // outcome. Decision 72: the relevance gate receives the shared
    // context view ahead of its per-call sections (Section 9.2).
    let recall_outcome = recall
        .recall_with_context(chat_id, &new_messages, context_view.as_deref())
        .await?;
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
            // Decision 72: the SAME view bytes the recall gate received
            // — one wake, one prefix.
            gate.decide_with_context(
                &GateInput {
                    new_messages: new_messages.clone(),
                    injections: injection_texts,
                    forced: false,
                },
                context_view.as_deref(),
            )
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
    // Decision 72: a target naming a CONTEXT-VIEW message (a row id at
    // or below the pre-advance marker) is outside the presented set by
    // construction, so this check already covers it.
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
            let fence = ReplyFence::for_pet_tag(&pet_tag);
            let raw_text = reply
                .generate(
                    chat_id,
                    &ReplyRequest {
                        messages: snapshot,
                        target: target.clone(),
                    },
                )
                .await?;
            // Decision 59, F1: the parrot filter guards EVERY reply
            // text here, so no generator (the live one included, and
            // every scripted double of tests) can put a confabulated
            // "I remember: ..." line or an imitated `<msg>`/`<you>`
            // context element into the WakeReport. The filtered
            // text is what the completion handler persists (Rule B1)
            // and sends: the log and the group see the same text.
            // Decision 96 (F5): the seam warns first on the live path;
            // this idempotent second pass is the net for generators
            // without a seam filter, so its WARN fires only there.
            let filtered = filter_reply_parrot_lines(&raw_text, &fence);
            if filtered.stripped_parrot {
                tracing::warn!(chat_id = %chat_id, "the reply carries imitated structure or fence debris: the parrot filter stripped the affected lines");
            }
            if filtered.text.is_empty() {
                // Nothing remains: the SAME wake error as an empty
                // reply today — nothing persisted, nothing sent.
                return Err(CoreError::Wake(
                    "the reply model returned an empty reply".to_string(),
                ));
            }
            // Decision 100 (B1): an overlength reply is an incident,
            // never a truncation candidate — reject it with the SAME
            // wake error as the empty remainder: nothing persists,
            // nothing sends, and a forced wake requeues once
            // (decision 65). The limit counts CHARACTERS, not bytes.
            let chars = filtered.text.chars().count();
            if chars > MAX_OUTBOUND_TEXT_CHARS {
                return Err(CoreError::Wake(format!(
                    "the reply model returned {chars} characters, over the {MAX_OUTBOUND_TEXT_CHARS}-character platform limit; the reply is rejected before persist"
                )));
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
    // The anchor of the forced-wake cooldown (decision 79 (c)): the
    // forcing message's intake timestamp of this completion's forced
    // entry, `None` for an unforced wake.
    forced_reply_at: Option<OffsetDateTime>,
    forced_cooldown_until: &mut Option<OffsetDateTime>,
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
    // Decision 70 (specs.md Section 6.2): the send-time quote decision
    // reuses the SAME distance the recency re-check computed. A forced
    // wake always quotes (the human engaged the bot directly); a
    // non-forced wake quotes only when the conversation moved past the
    // target (MORE than `reply_quote_threshold` newer human messages).
    // A recent target gets a plain standalone message: a Telegram reply
    // notifies the author, and a recent target needs no context anchor.
    let quote_target = if report.forced || newer > config.reply_quote_threshold {
        Some(target.platform_msg_id.clone())
    } else {
        None
    };
    // a. Rules B1/P1: persist the outbound raw-log row FIRST — the log
    // is the source of truth; never speak without logging. The
    // synthetic id: the adapter contract (Rule A3) returns no platform
    // id for a sent message, so the row carries a local synthetic id;
    // nanosecond time keeps the idempotency key unique. The row keeps
    // naming the INTERNAL reply target whether or not the platform
    // send quotes it (the same rule as the curated wake line's
    // `reply_to`, decision 53 freeze): the quote decision touches only
    // the outbound action.
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
        reply_to_platform_msg_id: quote_target,
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
    // Decision 79 (c): the reply of a FORCED wake went out — start the
    // forced-wake cooldown. The anchor is the FORCING MESSAGE's intake
    // timestamp (`ForcedWakeEntry.forced_at`), NOT the wall-clock send
    // time: the intake path compares on the deterministic replay clock
    // (`msg.timestamp`), where a wall-clock anchor would be incoherent
    // (replay fixtures carry historical timestamps — every later forced
    // wake would read as inside a window anchored at the real now).
    // Anchoring at `forced_at` keeps live and replay on one clock; in
    // live, message timestamps are wall-clock, so the window matches
    // the ruled seconds of wall time. A zero cooldown disables the
    // suppression. The early-return arms above (gate-no,
    // discarded-stale, outbound-row failure) produce NO send, so they
    // start NO cooldown; a failed wake (the `Err` arm of
    // `WakeCompleted`) starts nothing either.
    if report.forced && !config.forced_wake_cooldown.is_zero() {
        // A forced completion always carries its forced entry; the
        // `if let` is the defensive invariant.
        if let Some(forced_at) = forced_reply_at {
            let span = time::Duration::try_from(config.forced_wake_cooldown)
                .unwrap_or(time::Duration::MAX);
            *forced_cooldown_until = Some(forced_at.saturating_add(span));
        }
    }
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

    use tamako_memory::{MemoryBatch, TopicCandidate};
    use tempfile::TempDir;

    use crate::config::ActiveHours;
    use crate::context::{render_bot_content, ContextItemKind, ContextRole, RangeTag};
    use crate::summary::ScriptedSummary;
    use crate::warmup::{WarmupGenerator, WarmupRequest};

    /// A memory backend double. All calls succeed; `ensure_schema` records
    /// the chat_id values it receives. `topics` backs the decision-78
    /// `sample_interest_topics` override of the warmup tests (the trait
    /// default returns empty; this double returns the scripted set).
    struct NoopMemory {
        ensured: Mutex<Vec<String>>,
        topics: Mutex<Vec<TopicCandidate>>,
    }

    impl NoopMemory {
        fn new() -> Self {
            Self {
                ensured: Mutex::new(Vec::new()),
                topics: Mutex::new(Vec::new()),
            }
        }

        fn ensured_chat_ids(&self) -> Vec<String> {
            self.ensured
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        /// Scripts the warmup topic sampling (decision 78).
        fn set_topics(&self, topics: Vec<TopicCandidate>) {
            *self
                .topics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = topics;
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

        async fn sample_interest_topics(
            &self,
            _chat_id: &str,
            _limit: u32,
        ) -> tamako_memory::Result<Vec<TopicCandidate>> {
            Ok(self
                .topics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone())
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

    /// A digest pipeline double whose first `panics` calls panic INSIDE
    /// the returned future (the H4a containment tests: an LLM task
    /// panics mid-flight, not at call time); later calls delegate to
    /// the store-backed scripted pipeline. Every call is counted.
    struct PanicThenDigest {
        fallback: ScriptedDigest,
        panics_remaining: Mutex<usize>,
        calls: Mutex<usize>,
    }

    impl PanicThenDigest {
        fn new(store: Arc<Store>, panics: usize) -> Arc<Self> {
            Arc::new(Self {
                fallback: ScriptedDigest { store },
                panics_remaining: Mutex::new(panics),
                calls: Mutex::new(0),
            })
        }

        fn call_count(&self) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl DigestPipeline for PanicThenDigest {
        fn run_digest<'a>(
            &'a self,
            chat_id: &'a str,
            last_digest_boundary_msg_id: i64,
        ) -> Pin<Box<dyn Future<Output = Result<Option<DigestOutcome>, CoreError>> + Send + 'a>>
        {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
            let panic_now = {
                let mut remaining = self
                    .panics_remaining
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            };
            if panic_now {
                return Box::pin(async move { panic!("the digest extractor exploded") });
            }
            self.fallback
                .run_digest(chat_id, last_digest_boundary_msg_id)
        }
    }

    /// Spawns an actor with an arbitrary digest pipeline double.
    fn spawn_with_digest(
        fixture: &Fixture,
        config: TriggerConfig,
        digest: Arc<dyn DigestPipeline>,
    ) -> GroupActorHandle {
        spawn_group_actor(GroupActorParams {
            pet_tag: "tamako".to_string(),
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
            warmup: None,
            summary_provider: None,
            outbound: None,
            bot_name: None,
        })
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

    /// A `tracing` subscriber that records the rendered fields of every
    /// event. The failure errors of the spawned LLM tasks surface
    /// through the actor's WARN/ERROR log lines (their only reporting
    /// surface), so the H4a tests assert the `task panicked` marker in
    /// the captured events. tamako-core has no tracing-subscriber
    /// dependency, so the capture is a minimal hand-rolled subscriber;
    /// `set_default` is thread-local and the `#[tokio::test]` runtime is
    /// single-threaded, so the capture sees every actor event.
    #[derive(Clone, Default)]
    struct EventCapture {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl EventCapture {
        fn contains(&self, needle: &str) -> bool {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .any(|event| event.contains(needle))
        }

        /// Counts the captured events containing EVERY needle (the
        /// one-curated-line assertions match on the field set: the
        /// event message renders FIRST and unquoted, and `%`-fields
        /// render unquoted, so the curated warmup line matches
        /// `["message=warmup ", "action=\"sent\"", "topic=Tea"]`).
        fn count_matching(&self, needles: &[&str]) -> usize {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .filter(|event| needles.iter().all(|needle| event.contains(needle)))
                .count()
        }
    }

    impl tracing::Subscriber for EventCapture {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            struct FieldText(String);

            impl tracing::field::Visit for FieldText {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write;
                    let _ = write!(self.0, " {}={:?}", field.name(), value);
                }
            }

            let mut text = FieldText(String::new());
            event.record(&mut text);
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(text.0);
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    /// Polls the capture until an event contains `needle` or the
    /// deadline passes (the `wait_for_boundary` pattern).
    async fn wait_for_event(capture: &EventCapture, needle: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if capture.contains(needle) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for a log event containing {needle:?}"
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
            pet_tag: "tamako".to_string(),
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
            warmup: None,
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
            pet_tag: "tamako".to_string(),
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
            warmup: None,
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
            pet_tag: "tamako".to_string(),
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
            warmup: None,
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
    async fn reload_preamble_swaps_item_zero_and_touches_nothing_else() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
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
        // A snapshot is a FIFO barrier: when it returns, both messages are
        // processed.
        let session_before = handle.snapshot().await.expect("snapshot succeeds");
        let context_before = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        let persisted_before =
            blocking_store_call(&fixture.store, |store| store.load_all_state(CHAT_ID)).await;

        handle
            .send(ActorCommand::ReloadPreamble {
                preamble: "new preamble".to_string(),
                pet_tag: "tamako".to_string(),
            })
            .await
            .expect("send succeeds");
        // FIFO barrier: when this snapshot returns, the reload ran.
        let session_after = handle.snapshot().await.expect("snapshot succeeds");
        let context_after = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        let persisted_after =
            blocking_store_call(&fixture.store, |store| store.load_all_state(CHAT_ID)).await;

        // Item 0 is exactly the new preamble (kind Preamble); every other
        // item is byte-identical to before.
        assert_eq!(context_after.len(), context_before.len());
        assert_eq!(context_after[0].kind, ContextItemKind::Preamble);
        assert_eq!(context_after[0].content, "new preamble");
        assert_eq!(context_after[1..], context_before[1..]);
        // Nothing persisted: the reload is in-memory only, so the session
        // snapshot before/after is EQUAL (encode derives from the
        // PartialEq state). No state-table write happened by construction
        // — the handler calls only `LiveContext::reload_preamble` — and
        // the persisted state rows prove it.
        assert_eq!(session_before, session_after);
        assert_eq!(persisted_before, persisted_after);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn reload_preamble_serializes_behind_in_flight_work() {
        // FIFO serialization is STRUCTURAL: one inbox, one loop, one
        // command at a time (specs.md Section 6.1 rule 1) — a reload can
        // never interrupt in-flight work; no gating machinery is needed
        // to prove it. The cheap assertion: a context snapshot enqueued
        // BEFORE the reload observes the old preamble, one enqueued
        // AFTER it observes the new preamble — FIFO order of application.
        let (_fixture, handle) = spawn_fixture(TriggerConfig::default());
        let before = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        handle
            .send(ActorCommand::ReloadPreamble {
                preamble: "hot preamble".to_string(),
                pet_tag: "tamako".to_string(),
            })
            .await
            .expect("send succeeds");
        let after = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(before[0].content, TEST_PREAMBLE);
        assert_eq!(after[0].content, "hot preamble");
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn try_send_reports_a_dead_actor() {
        let (_fixture, mut handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send(ActorCommand::Shutdown)
            .await
            .expect("send succeeds");
        // Await the task WITHOUT consuming the handle (`shutdown()` would
        // take the inbox sender with it): the JoinHandle is Unpin, so a
        // `&mut` awaits it.
        (&mut handle.join)
            .await
            .expect("the actor task joins")
            .expect("shutdown succeeds");
        let result = handle.try_send(ActorCommand::ReloadPreamble {
            preamble: "x".to_string(),
            pet_tag: "tamako".to_string(),
        });
        assert!(matches!(result, Err(CoreError::InboxClosed)));
        // The full-inbox arm (`TrySendError::Full`) is not tested: it
        // needs 256 pending commands, and the collapsed error makes the
        // dead-actor arm the deterministic cheap proof — both arms share
        // the same `map_err` code path.
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
    async fn text_identical_edit_drops_without_a_trace() {
        // Decision 65: an edit whose text equals the LATEST persisted row
        // is not an event — no log row, no context item, no session
        // mutation, no wake-counter advance.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        // Byte-exact same text as the persisted original row.
        let edited = message("m1", 1, false);
        handle
            .send_event(InboundEvent::EditedMessage(edited))
            .await
            .expect("send succeeds");
        // A snapshot is a FIFO barrier: both events are processed.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_type, EventType::Message);
        // Preamble + the original message; no edit item.
        assert_eq!(items.len(), 2);
        // The edit path never advanced the wake counter; the drop must
        // not change that.
        assert_eq!(session.wake.msgs_since_wake, 1);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn edit_chain_a_b_a_persists_each_delta_and_a_repeat_drops() {
        // Decision 65: the comparison target is the LATEST persisted row,
        // so a chain A→B→A persists each real delta (the second A differs
        // from the current state B), while a no-op repeat A→A drops.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        // A → B: a real delta, persists.
        let mut edit_b = message("m1", 1, false);
        edit_b.text = "text B".to_string();
        handle
            .send_event(InboundEvent::EditedMessage(edit_b))
            .await
            .expect("send succeeds");
        // B → A: back to the original text; differs from the latest row
        // (B), so it persists.
        let edit_a = message("m1", 1, false);
        handle
            .send_event(InboundEvent::EditedMessage(edit_a))
            .await
            .expect("send succeeds");
        // A → A: identical to the latest row, drops.
        let repeat_a = message("m1", 1, false);
        handle
            .send_event(InboundEvent::EditedMessage(repeat_a))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].event_type, EventType::Message);
        assert_eq!(rows[1].event_type, EventType::Edit);
        assert_eq!(rows[1].text, "text B");
        assert_eq!(rows[2].event_type, EventType::Edit);
        assert_eq!(rows[2].text, "text of m1");
        // Preamble + original + two edit items; the repeat appended none.
        assert_eq!(items.len(), 4);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn edit_of_an_unknown_target_persists() {
        // Decision 65: no persisted row for the platform_msg_id (the edit
        // predates the bot's view) keeps the current behavior — the edit
        // is persisted like any other new row.
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        let edited = message("m-unknown", 1, false);
        handle
            .send_event(InboundEvent::EditedMessage(edited))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_type, EventType::Edit);
        assert_eq!(rows[0].text, "text of m-unknown");
        handle.shutdown().await.expect("shutdown succeeds");
    }
    // Note: the fail-open branch of the decision-65 filter (a store error
    // on the latest-row lookup) is not covered here — the actor harness
    // holds a concrete `Arc<Store>` with no trait seam, so a store error
    // is not injectable. The branch is documented at the intake site.

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
            "tamako".to_string(),
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

    // --- H4a panic-containment tests (batch 2, decision 65 package) ---

    #[tokio::test]
    async fn contain_task_panic_converts_a_panic_into_a_marked_failure() {
        // The `&str` payload of `panic!`.
        let outcome: Result<(), String> =
            contain_task_panic(async { panic!("the model exploded") }).await;
        assert_eq!(
            outcome,
            Err("task panicked: the model exploded".to_string())
        );
        // An owned `String` payload renders too.
        let outcome: Result<(), String> =
            contain_task_panic(async { panic!("{}", "owned payload".to_string()) }).await;
        assert_eq!(outcome, Err("task panicked: owned payload".to_string()));
        // A `panic_any` payload has no message; the marker still
        // distinguishes the panic from a provider failure.
        let outcome: Result<(), String> =
            contain_task_panic(async { std::panic::panic_any(42) }).await;
        assert_eq!(
            outcome,
            Err("task panicked: a non-string panic payload".to_string())
        );
        // No panic: the output passes through unchanged.
        let outcome: Result<u32, String> = contain_task_panic(async { 41 + 1 }).await;
        assert_eq!(outcome, Ok(42));
    }

    #[tokio::test]
    async fn a_panicking_digest_task_reports_a_failure_and_the_trigger_recovers() {
        // H4a: the spawned digest task panics; the containment reports a
        // synthetic DigestCompleted(Err) through the inbox, so
        // `digest_in_flight` resets and a later digest fires again. The
        // failure semantics are EXACTLY those of an infrastructure
        // error: the boundary does not advance and the next evaluation
        // point retries.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let digest = PanicThenDigest::new(Arc::clone(&fixture.store), 1);
        let handle = spawn_with_digest(&fixture, digest_config(), digest.clone());

        send_pair(&handle, 1).await;
        // Digest 1 panics. The failure log line proves the completion
        // handler ran (the flag is already reset at that point: it
        // resets BEFORE the match on the result).
        wait_for_event(&capture, "task panicked: the digest extractor exploded").await;
        // Same semantics as a provider error: no boundary advance.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.last_digest_boundary_msg_id, 0);

        // The trigger recovered: the next evaluation digests the whole
        // tail above boundary 0.
        send_pair(&handle, 3).await;
        wait_for_boundary(&handle, 4).await;
        assert_eq!(digest.call_count(), 2);
        handle.shutdown().await.expect("shutdown succeeds");
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
            pet_tag: "tamako".to_string(),
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
            warmup: None,
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
    async fn three_consecutive_summary_failures_drop_the_chunk_and_the_breaker_resets() {
        // Decision 65, the M3 circuit breaker: on the THIRD consecutive
        // summarization failure THAT chunk drops without a summary row
        // (the sanctioned old-C3 fallback) with one ERROR, and the
        // breaker resets — the next chunk tries summarization again.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let summary = Arc::new(ScriptedSummary::failing_then(
            3,
            vec!["recovered summary.".to_string()],
        ));
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary.clone());

        send_pair(&handle, 1).await;
        wait_for_boundary(&handle, 2).await;
        send_pair(&handle, 3).await;
        // Failure 1 (chunk (0, 2]): the decision-62 deferral.
        wait_for_boundary(&handle, 4).await;
        send_pair(&handle, 5).await;
        // Failure 2 (chunk (0, 4]): deferred again; the context still
        // keeps every raw row.
        wait_for_boundary(&handle, 6).await;
        send_pair(&handle, 7).await;
        // Failure 3 (chunk (0, 6]): the breaker fires — the removal
        // proceeds WITHOUT a summary row (prev = 6), one ERROR lands.
        wait_for_boundary(&handle, 8).await;
        wait_for_event(&capture, "dropping the chunk without a summary").await;
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.last_digest_boundary_msg_id, 8);
        assert_eq!(session.prev_digest_boundary_msg_id, Some(6));
        let dropped = blocking_store_call(&fixture.store, move |store| {
            store.find_context_summary(CHAT_ID, 0, 6)
        })
        .await;
        assert!(dropped.is_none());
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        // The dropped chunk left the context; the overlap buffer
        // (6, 8] stays raw.
        assert_eq!(items.len(), 3);
        assert!(items
            .iter()
            .all(|item| item.kind != ContextItemKind::Summary));
        // The Section 12-style counter counted all three failures.
        assert_eq!(
            counter_value(&fixture.store, "summaries_failed_total").await,
            Some(3)
        );

        send_pair(&handle, 9).await;
        // The breaker reset: digest 5 summarizes the next chunk (6, 8]
        // again — and succeeds.
        wait_for_boundary(&handle, 10).await;
        let inputs = summary.inputs();
        assert_eq!(inputs.len(), 4);
        assert_eq!((inputs[3].first_msg_id, inputs[3].last_msg_id), (6, 8));
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 4);
        assert_eq!(
            items[1].content,
            r#"<summary range="6-8">recovered summary.</summary>"#
        );

        send_pair(&handle, 11).await;
        // The success reset the count: ONE new failure (the scripted
        // queue is exhausted) defers again instead of dropping.
        wait_for_boundary(&handle, 12).await;
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.last_digest_boundary_msg_id, 12);
        assert_eq!(session.prev_digest_boundary_msg_id, Some(8));
        // No drop: the deferred chunk (8, 10] stays raw in the context.
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 6);
        let missing = blocking_store_call(&fixture.store, move |store| {
            store.find_context_summary(CHAT_ID, 8, 10)
        })
        .await;
        assert!(missing.is_none());
        assert_eq!(
            counter_value(&fixture.store, "summaries_failed_total").await,
            Some(4)
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn an_oversized_chunk_summarizes_only_the_newest_suffix_of_the_full_range() {
        // Decision 65 input cap (2 × digest_max_messages = 4 rows
        // here): the deferred-retry widening grows the chunk past the
        // cap; the summarizer receives only the NEWEST cap-sized
        // suffix, while the summary row is recorded against the FULL
        // removed range.
        let fixture = make_fixture();
        let summary = Arc::new(ScriptedSummary::failing_then(
            2,
            vec!["suffix summary.".to_string()],
        ));
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary.clone());

        send_pair(&handle, 1).await;
        wait_for_boundary(&handle, 2).await;
        send_pair(&handle, 3).await;
        // Failure 1 (chunk (0, 2]): deferred.
        wait_for_boundary(&handle, 4).await;
        send_pair(&handle, 5).await;
        // Failure 2 (chunk (0, 4]): four rows — exactly AT the cap, no
        // truncation.
        wait_for_boundary(&handle, 6).await;
        send_pair(&handle, 7).await;
        // Attempt 3 (chunk (0, 6]): six rows EXCEED the cap; the
        // provider receives only the newest four (rows 3-6), and the
        // summary row is recorded against the full range (0, 6].
        wait_for_boundary(&handle, 8).await;

        let inputs = summary.inputs();
        assert_eq!(inputs.len(), 3);
        let untruncated: Vec<i64> = inputs[1].rows.iter().map(|row| row.id).collect();
        assert_eq!(untruncated, vec![1, 2, 3, 4]);
        assert_eq!((inputs[2].first_msg_id, inputs[2].last_msg_id), (0, 6));
        let suffix: Vec<i64> = inputs[2].rows.iter().map(|row| row.id).collect();
        assert_eq!(suffix, vec![3, 4, 5, 6]);

        let row = blocking_store_call(&fixture.store, move |store| {
            store.find_context_summary(CHAT_ID, 0, 6)
        })
        .await
        .expect("the summary row exists");
        assert_eq!(row.content, "suffix summary.");
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 4);
        assert_eq!(
            items[1].content,
            r#"<summary range="0-6">suffix summary.</summary>"#
        );
        assert_eq!(items[2].range_tag, Some(RangeTag::single(7)));
        assert_eq!(items[3].range_tag, Some(RangeTag::single(8)));
        // Two failures, no circuit-break drop (N = 3).
        assert_eq!(
            counter_value(&fixture.store, "summaries_failed_total").await,
            Some(2)
        );
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

    /// A summarizer double whose first `panics` calls panic INSIDE the
    /// returned future (the H4a containment tests: an LLM task panics
    /// mid-flight, not at call time); later calls delegate to the
    /// scripted summarizer. Every call is counted.
    struct PanicThenSummary {
        fallback: ScriptedSummary,
        panics_remaining: Mutex<usize>,
        calls: Mutex<usize>,
    }

    impl PanicThenSummary {
        fn new(panics: usize, summaries: Vec<String>) -> Arc<Self> {
            Arc::new(Self {
                fallback: ScriptedSummary::with_summaries(summaries),
                panics_remaining: Mutex::new(panics),
                calls: Mutex::new(0),
            })
        }

        fn call_count(&self) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl SummaryProvider for PanicThenSummary {
        fn summarize<'a>(
            &'a self,
            chat_id: &'a str,
            first_msg_id: i64,
            last_msg_id: i64,
            rows: &'a [MessageRow],
        ) -> Pin<Box<dyn Future<Output = Result<String, SummaryError>> + Send + 'a>> {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
            let panic_now = {
                let mut remaining = self
                    .panics_remaining
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            };
            if panic_now {
                return Box::pin(async move { panic!("the summarizer exploded") });
            }
            self.fallback
                .summarize(chat_id, first_msg_id, last_msg_id, rows)
        }
    }

    #[tokio::test]
    async fn a_panicking_summary_task_reports_a_failure_and_defers_the_removal() {
        // H4a + decision 62: the spawned summary task panics; the
        // containment reports a synthetic SummaryCompleted(Err), so
        // `summary_pending` resets and the EXISTING failure semantics
        // apply unchanged — the removal defers one digest cycle and the
        // next completion retries the summarization over the WIDENED
        // range.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let summary = PanicThenSummary::new(1, vec!["chunk one.".to_string()]);
        let handle = spawn_with_digest_and_summary(&fixture, digest_config(), summary.clone());

        send_pair(&handle, 1).await;
        // Digest 1: the boundary advances 0 -> 2; b_old == 0 summarizes
        // nothing.
        wait_for_boundary(&handle, 2).await;
        assert_eq!(summary.call_count(), 0);

        send_pair(&handle, 3).await;
        // Digest 2 completes; the summary task of the (0,2] chunk
        // PANICS. The failure semantics of decision 62: `last`
        // advances to 4, the chunk stays one more cycle, and the
        // digest trigger is free again (summary_pending reset).
        wait_for_boundary(&handle, 4).await;
        wait_for_event(&capture, "task panicked: the summarizer exploded").await;
        assert_eq!(summary.call_count(), 1);
        // No summary row persisted (a panic fabricates no history).
        let missing = blocking_store_call(&fixture.store, move |store| {
            store.find_context_summary(CHAT_ID, 0, 2)
        })
        .await;
        assert!(missing.is_none());

        send_pair(&handle, 5).await;
        // Digest 3 completes; its deferred summarization retries over
        // the widened range (0,4] and SUCCEEDS through the fallback.
        wait_for_boundary(&handle, 6).await;
        assert_eq!(summary.call_count(), 2);
        let row = blocking_store_call(&fixture.store, move |store| {
            store.find_context_summary(CHAT_ID, 0, 4)
        })
        .await
        .expect("the retried summary row exists");
        assert_eq!(row.content, "chunk one.");

        // The deferred removal landed: the summary item replaced the
        // raw chunk; the newest chunk (4,6] stays raw (one-chunk lag).
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].kind, ContextItemKind::Preamble);
        assert_eq!(items[1].kind, ContextItemKind::Summary);
        assert_eq!(
            items[1].range_tag,
            Some(RangeTag {
                first_msg_id: 0,
                last_msg_id: 4
            })
        );
        assert_eq!(items[2].range_tag, Some(RangeTag::single(5)));
        assert_eq!(items[3].range_tag, Some(RangeTag::single(6)));
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
    /// new message. Every `decide` call is recorded, together with the
    /// decision-72 context view of each call (`views`; `None` = the
    /// pre-72 delta-only shape).
    struct ScriptedGate {
        participate: bool,
        target: GateTarget,
        calls: Mutex<Vec<GateInput>>,
        views: Mutex<Vec<Option<String>>>,
    }

    impl ScriptedGate {
        fn yes(target: GateTarget) -> Arc<Self> {
            Arc::new(Self {
                participate: true,
                target,
                calls: Mutex::new(Vec::new()),
                views: Mutex::new(Vec::new()),
            })
        }

        fn no() -> Arc<Self> {
            Arc::new(Self {
                participate: false,
                target: GateTarget::Last,
                calls: Mutex::new(Vec::new()),
                views: Mutex::new(Vec::new()),
            })
        }

        fn call_count(&self) -> usize {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
        }

        /// Every recorded gate input, in call order (the forced-wake
        /// cooldown tests inspect the presented set).
        fn calls(&self) -> Vec<GateInput> {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        /// Every context view the gate received, in call order
        /// (decision 72).
        fn views(&self) -> Vec<Option<String>> {
            self.views
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
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

        fn decide_with_context<'a>(
            &'a self,
            input: &'a GateInput,
            context_view: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = Result<GateDecision, CoreError>> + Send + 'a>> {
            // Decision 72: record the view, then the SAME decision
            // logic as `decide`.
            self.views
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(context_view.map(str::to_string));
            self.decide(input)
        }
    }

    /// A gate double whose first `panics` `decide` calls panic INSIDE
    /// the returned future (the H4a containment tests: an LLM task
    /// panics mid-flight, not at call time); later calls delegate to
    /// the scripted gate. Every call is counted.
    struct PanicThenGate {
        fallback: ScriptedGate,
        panics_remaining: Mutex<usize>,
        calls: Mutex<usize>,
    }

    impl PanicThenGate {
        fn yes_after(panics: usize, target: GateTarget) -> Arc<Self> {
            Arc::new(Self {
                fallback: ScriptedGate {
                    participate: true,
                    target,
                    calls: Mutex::new(Vec::new()),
                    views: Mutex::new(Vec::new()),
                },
                panics_remaining: Mutex::new(panics),
                calls: Mutex::new(0),
            })
        }

        fn call_count(&self) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl ParticipationGate for PanicThenGate {
        fn decide<'a>(
            &'a self,
            input: &'a GateInput,
        ) -> Pin<Box<dyn Future<Output = Result<GateDecision, CoreError>> + Send + 'a>> {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
            let panic_now = {
                let mut remaining = self
                    .panics_remaining
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            };
            if panic_now {
                return Box::pin(async move { panic!("the gate model exploded") });
            }
            self.fallback.decide(input)
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
            _chat_id: &'a str,
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

    /// A reply generator double whose first `failures` calls fail with a
    /// wake error; later calls answer with `text` (the decision-65
    /// forced-requeue tests: a forced wake BYPASSES the gate, so the
    /// wake failure must come from the reply model). Every call is
    /// counted.
    struct FailThenReply {
        text: String,
        failures_remaining: Mutex<usize>,
        calls: Mutex<usize>,
    }

    impl FailThenReply {
        fn new(failures: usize, text: &str) -> Arc<Self> {
            Arc::new(Self {
                text: text.to_string(),
                failures_remaining: Mutex::new(failures),
                calls: Mutex::new(0),
            })
        }

        fn call_count(&self) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl ReplyGenerator for FailThenReply {
        fn generate<'a>(
            &'a self,
            _chat_id: &'a str,
            _request: &'a ReplyRequest,
        ) -> Pin<Box<dyn Future<Output = Result<String, CoreError>> + Send + 'a>> {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
            let fail_now = {
                let mut remaining = self
                    .failures_remaining
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            };
            let text = self.text.clone();
            Box::pin(async move {
                if fail_now {
                    Err(CoreError::Wake("the reply endpoint is down".to_string()))
                } else {
                    Ok(text)
                }
            })
        }
    }

    /// A scripted recall provider (the `ScriptedGate` pattern). Each
    /// `recall` call pops one queued `RecallOutcome` (an empty queue
    /// yields the empty outcome) and records its input, together with
    /// the decision-72 context view of each call (`views`; `None` =
    /// the pre-72 delta-only shape).
    struct ScriptedRecall {
        outcomes: Mutex<std::collections::VecDeque<crate::wake::RecallOutcome>>,
        calls: Mutex<Vec<(String, Vec<GateMessage>)>>,
        views: Mutex<Vec<Option<String>>>,
    }

    impl ScriptedRecall {
        fn with_outcomes(outcomes: Vec<crate::wake::RecallOutcome>) -> Arc<Self> {
            Arc::new(Self {
                outcomes: Mutex::new(outcomes.into()),
                calls: Mutex::new(Vec::new()),
                views: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<(String, Vec<GateMessage>)> {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        /// Every context view the recall received, in call order
        /// (decision 72).
        fn views(&self) -> Vec<Option<String>> {
            self.views
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

        fn recall_with_context<'a>(
            &'a self,
            chat_id: &'a str,
            new_messages: &'a [GateMessage],
            context_view: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = Result<crate::wake::RecallOutcome, CoreError>> + Send + 'a>>
        {
            // Decision 72: record the view, then the SAME recall
            // logic as `recall`.
            self.views
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(context_view.map(str::to_string));
            self.recall(chat_id, new_messages)
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
            pet_tag: "tamako".to_string(),
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
            warmup: None,
            summary_provider: None,
            outbound: Some(outbound_tx),
            bot_name: None,
        });
        (handle, outbound_rx)
    }

    /// Spawns an actor with arbitrary wake doubles: the trait-object
    /// variant of `spawn_with_wake`, for the doubles that are not a
    /// `ScriptedGate`/`ScriptedReply` pair (PanicThenGate,
    /// FailThenReply).
    fn spawn_with_wake_doubles(
        fixture: &Fixture,
        config: TriggerConfig,
        recall: Arc<dyn RecallProvider>,
        gate: Arc<dyn ParticipationGate>,
        reply: Arc<dyn ReplyGenerator>,
    ) -> (GroupActorHandle, mpsc::Receiver<OutboundAction>) {
        let (outbound_tx, outbound_rx) = mpsc::channel(64);
        let handle = spawn_group_actor(GroupActorParams {
            pet_tag: "tamako".to_string(),
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::clone(&fixture.memory),
            config,
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            preamble: TEST_PREAMBLE.to_string(),
            digest: None,
            post_digest_hook: None,
            wake: Some(WakeServices {
                recall,
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

    /// Inserts messages directly into the raw log WITHOUT an actor run
    /// (the marker-repair tests: log rows exist, but no session state
    /// row was ever written — the old-session/pre-repair shape). The
    /// group store is opened BEFORE the actor spawns (the
    /// curated_log_replay.rs WAL-pragma race note).
    async fn insert_messages_without_session(fixture: &Fixture, messages: &[NormalizedMessage]) {
        let rows: Vec<NewMessage> = messages
            .iter()
            .map(|msg| to_new_message(msg, EventType::Message))
            .collect();
        blocking_store_call(&fixture.store, move |store| {
            store.open_group(CHAT_ID)?;
            for row in &rows {
                store.insert_message(CHAT_ID, row)?;
            }
            Ok(())
        })
        .await;
    }

    /// Writes one state-table row directly (the malformed-marker repair
    /// test corrupts the key this way).
    async fn set_state_directly(fixture: &Fixture, key: &str, value: &str) {
        let key = key.to_string();
        let value = value.to_string();
        blocking_store_call(&fixture.store, move |store| {
            store.open_group(CHAT_ID)?;
            store.set_state(CHAT_ID, &key, &value)
        })
        .await;
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

        // The reply goes out as a SendText. Decision 70: the target is
        // the LAST new message (0 newer human messages ≤ the quote
        // threshold), so the send is a plain standalone message with NO
        // reply target.
        let (chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(chat_id, CHAT_ID);
        assert_eq!(text, "a thoughtful reply");
        assert_eq!(reply_to, None);

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
        // The `<tamako>` item of the row (the row timestamp is the same
        // `now` the append used).
        assert_eq!(
            last.content,
            render_bot_content(
                bot_row.id,
                bot_row.timestamp,
                "a thoughtful reply",
                "tamako"
            )
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
    async fn the_wake_passes_the_same_context_view_to_both_gates() {
        // Decision 72 (specs.md Sections 9.2 and 9.6): with
        // `gate_context` on (the default), the actor renders the
        // shared context view ONCE per wake from the PRE-ADVANCE
        // marker and hands the SAME bytes to the recall relevance
        // gate and the participation gate (one wake, one provider
        // prefix). The wake's new messages stay OUT of the view.
        let fixture = make_fixture();
        let recall = ScriptedRecall::with_outcomes(vec![]);
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("a reply");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::clone(&recall) as Arc<dyn RecallProvider>,
            Arc::clone(&gate),
            Arc::clone(&reply),
        );

        // Wake 1 over rows 1-3: the pre-advance marker is 0, so no
        // context item qualifies — the view is the EMPTY string (the
        // gates render the explicit empty marker; the prompt shape is
        // stable). The gate participates; the reply completes the
        // wake (participations_total lands at the END of the
        // completion handler, so the wait covers wake_in_flight).
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
        let _ = expect_send_text(next_action(&mut outbound).await);
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        assert_eq!(gate.views(), vec![Some(String::new())]);
        assert_eq!(recall.views(), vec![Some(String::new())]);

        // Wake 2 over rows 5-7 (row 4 is the bot's own reply; the
        // outbound row is never a gate message): the pre-advance
        // marker is 3, so the view is exactly the rows 1-3 items —
        // byte-identical to `gate_context_view(3)` — and EXCLUDES
        // both the bot's reply (row 4, above the marker) and the new
        // messages (rows 5-7).
        for index in 5..=7 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        let _ = expect_send_text(next_action(&mut outbound).await);
        wait_for_counter(&fixture.store, "participations_total", 2).await;

        let expected_view = [
            render_human_content(
                1,
                "Alice",
                None,
                t0() + time::Duration::seconds(1),
                false,
                false,
                ReplyRender::None,
                "text of m1",
            ),
            render_human_content(
                2,
                "Alice",
                None,
                t0() + time::Duration::seconds(2),
                false,
                false,
                ReplyRender::None,
                "text of m2",
            ),
            render_human_content(
                3,
                "Alice",
                None,
                t0() + time::Duration::seconds(3),
                false,
                false,
                ReplyRender::None,
                "text of m3",
            ),
        ]
        .join("\n");
        // Both gates of wake 2 received the SAME view bytes.
        assert_eq!(gate.views()[1], Some(expected_view.clone()));
        assert_eq!(recall.views()[1], Some(expected_view.clone()));
        assert_eq!(gate.views().len(), 2);
        assert_eq!(recall.views().len(), 2);
        // The presented (targetable) set of wake 2 is its NEW
        // messages only — the context view changed nothing about the
        // GateInput.
        let calls = gate
            .calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let row_ids: Vec<i64> = calls[1].new_messages.iter().map(|msg| msg.row_id).collect();
        assert_eq!(row_ids, vec![5, 6, 7]);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_gate_context_switch_restores_the_delta_only_input() {
        // Decision 72 kill switch (specs.md Sections 9.2/9.6/13):
        // `gate_context = false` passes None to both gates — the
        // pre-72 delta-only prompt shape.
        let fixture = make_fixture();
        let recall = ScriptedRecall::with_outcomes(vec![]);
        let gate = ScriptedGate::no();
        let reply = ScriptedReply::new("never used");
        let mut config = wake_config(3);
        config.gate_context = false;
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            config,
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
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        // The recall call ran before the gate call, so both views are
        // recorded by now.
        assert_eq!(gate.views(), vec![None]);
        assert_eq!(recall.views(), vec![None]);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_panicking_wake_gate_reports_a_failure_and_the_next_wake_runs() {
        // H4a: the gate panics inside the spawned wake task; the
        // containment reports a synthetic WakeCompleted(Err), so the
        // wake is skipped EXACTLY like a provider failure (log, no
        // outbound, no participation) and `wake_in_flight` resets.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let gate = PanicThenGate::yes_after(1, GateTarget::Last);
        let reply = ScriptedReply::new("recovered reply");
        let (outbound_tx, mut outbound) = mpsc::channel(64);
        let handle = spawn_group_actor(GroupActorParams {
            pet_tag: "tamako".to_string(),
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::clone(&fixture.memory),
            config: wake_config(3),
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            preamble: TEST_PREAMBLE.to_string(),
            digest: None,
            post_digest_hook: None,
            wake: Some(WakeServices {
                recall: Arc::new(NoopRecall),
                gate: gate.clone(),
                reply: reply.clone(),
            }),
            warmup: None,
            summary_provider: None,
            outbound: Some(outbound_tx),
            bot_name: None,
        });

        // Wake 1 fires on the count threshold; its gate call panics.
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
        // The failure log line proves the completion handler ran (the
        // flag is already reset at that point: it resets BEFORE the
        // match on the result).
        wait_for_event(&capture, "task panicked: the gate model exploded").await;
        assert_eq!(gate.call_count(), 1);
        // The wake-skip semantics of a provider failure: nothing sent,
        // no participation counted.
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert_eq!(
            counter_value(&fixture.store, "participations_total").await,
            None
        );

        // A later wake runs. A mention forces one; had it arrived
        // while wake 1 was still in flight, the forced queue would
        // start it right after the failure — either path proves the
        // flag reset.
        handle
            .send_event(InboundEvent::Message(message("m4", 4, true)))
            .await
            .expect("send succeeds");
        let (chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(chat_id, CHAT_ID);
        assert_eq!(text, "recovered reply");
        assert_eq!(reply_to, Some("m4".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));
        // The forced wake bypassed the gate: still one gate call.
        assert_eq!(gate.call_count(), 1);
        assert_eq!(reply.call_count(), 1);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_failed_wake_rolls_back_and_the_next_natural_wake_re_presents_the_messages() {
        // Decision 65 rollback: the failed wake's messages are NOT
        // lost. `wake_last_row_id` rolls back to its pre-wake value;
        // the next NATURAL trigger (the count threshold — the
        // reset-at-start floor/threshold still gates, so no immediate
        // retry storm) re-presents the same messages.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let gate = PanicThenGate::yes_after(1, GateTarget::Last);
        let reply = ScriptedReply::new("second wake reply");
        let (handle, mut outbound) = spawn_with_wake_doubles(
            &fixture,
            wake_config(3),
            Arc::new(NoopRecall),
            gate.clone(),
            reply.clone(),
        );

        // Wake 1 fires on the count threshold; its gate call panics.
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
        // The failure log proves the completion handler ran — the
        // rollback lands BEFORE that log line.
        wait_for_event(&capture, "task panicked: the gate model exploded").await;
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 0);
        // The wake-skip semantics are unchanged: nothing sent, no
        // participation, the start-time counter bump stands.
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert_eq!(
            counter_value(&fixture.store, "participations_total").await,
            None
        );
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(1));

        // Three more messages reach the threshold again (the count was
        // reset at wake 1's start): the natural retry. The gate input
        // RE-PRESENTS the failed wake's messages — rows 1-3 again, plus
        // the new rows 4-6.
        for index in 4..=6 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        let (_chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "second wake reply");
        // Decision 70: m6 is the last new message (recent target), so
        // the send is a plain standalone message.
        assert_eq!(reply_to, None);
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        // The fallback gate recorded exactly one call (the first call
        // panicked before delegating); its input is the re-presented
        // full range.
        let inputs = gate
            .fallback
            .calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(inputs.len(), 1);
        let row_ids: Vec<i64> = inputs[0]
            .new_messages
            .iter()
            .map(|msg| msg.row_id)
            .collect();
        assert_eq!(row_ids, vec![1, 2, 3, 4, 5, 6]);
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 6);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_failed_forced_wake_requeues_once_and_responds() {
        // Decision 65: a failed FORCED wake requeues into
        // `forced_pending` ONCE and starts immediately after the
        // failure (the Section 8.1 must-respond obligation gets one
        // bounded retry). A forced wake bypasses the gate, so the
        // failure comes from the reply model. The retry responds to the
        // SAME forcing message.
        let fixture = make_fixture();
        let gate = ScriptedGate::no();
        let reply = FailThenReply::new(1, "obliged reply");
        // A high count: only the mention can fire the wake.
        let (handle, mut outbound) = spawn_with_wake_doubles(
            &fixture,
            wake_config(100),
            Arc::new(NoopRecall),
            gate.clone(),
            reply.clone(),
        );
        handle
            .send_event(InboundEvent::Message(message("m1", 1, true)))
            .await
            .expect("send succeeds");

        // The first reply call fails; the requeued forced wake's second
        // call succeeds.
        let (_chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "obliged reply");
        assert_eq!(reply_to, Some("m1".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        assert_eq!(reply.call_count(), 2);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));
        // The forced wake bypassed the gate on both runs (Section 8.1).
        assert_eq!(gate.call_count(), 0);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_twice_failed_forced_wake_drops_with_a_distinct_error() {
        // Decision 65: the requeued forced wake fails AGAIN — the entry
        // drops (NO second requeue) with a distinct ERROR naming the
        // unmet Section 8.1 must-respond obligation.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let gate = ScriptedGate::no();
        let reply = FailThenReply::new(2, "late reply");
        let (handle, mut outbound) = spawn_with_wake_doubles(
            &fixture,
            wake_config(100),
            Arc::new(NoopRecall),
            gate.clone(),
            reply.clone(),
        );
        handle
            .send_event(InboundEvent::Message(message("m1", 1, true)))
            .await
            .expect("send succeeds");

        // The distinct drop line proves both failures ran their course.
        wait_for_event(
            &capture,
            "the must-respond obligation of specs.md Section 8.1 is unmet",
        )
        .await;
        // Exactly two reply calls (the failure plus the one requeue),
        // nothing sent, no participation.
        assert_eq!(reply.call_count(), 2);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert_eq!(
            counter_value(&fixture.store, "participations_total").await,
            None
        );
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));

        // The queue is clean: a fresh mention forces a normal wake with
        // a full retry budget.
        handle
            .send_event(InboundEvent::Message(message("m2", 2, true)))
            .await
            .expect("send succeeds");
        let (_chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "late reply");
        assert_eq!(reply_to, Some("m2".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        assert_eq!(reply.call_count(), 3);
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
        // Decision 79 (c): this test drives two forced wakes 1 s apart
        // and needs BOTH replies (its subject is the monologue lock,
        // not the cooldown), so the forced-wake cooldown is disabled;
        // the default 10 s would suppress the queued second forcing.
        let config = TriggerConfig {
            forced_wake_cooldown: Duration::ZERO,
            ..wake_config(3)
        };
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            config,
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
        // Decision 70: c3 is the last new message (recent target) of
        // this unforced wake, so the send is a plain standalone message.
        assert_eq!(third_reply_to, None);
        wait_for_counter(&fixture.store, "participations_total", 3).await;
        assert_eq!(gate.call_count(), 1);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(4));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn startup_repairs_a_missing_wake_marker_to_the_raw_log_tail() {
        // Decision 65 (the K2-latent marker fix), the ABSENT-key shape:
        // rows land in the raw log WITHOUT any actor run, so no state
        // row was ever written (wake services previously disabled, or
        // an old-session group). The startup repair moves
        // `wake_last_row_id` to the raw-log tail — "start from now" —
        // so the next wake presents only post-restart messages instead
        // of the ENTIRE log.
        let fixture = make_fixture();
        let prior: Vec<_> = (1..=3)
            .map(|index| message(&format!("old{index}"), index, false))
            .collect();
        insert_messages_without_session(&fixture, &prior).await;

        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("post-restart reply");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(2),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );

        // The repair ran at startup (the snapshot is a FIFO barrier):
        // the marker sits at the tail (row 3), one WARN line reported
        // the repair, and the repaired value is persisted.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 3);
        assert!(
            capture.contains("wake_last_row_id missing or malformed; repaired to the raw-log tail")
        );
        assert_eq!(
            counter_value(&fixture.store, "wake_last_row_id").await,
            Some(3)
        );

        // Two post-restart messages reach the count threshold: the
        // gate input is ONLY the post-restart rows 4 and 5 — the
        // repair stopped the full-log re-presentation.
        for index in 4..=5 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        let (_chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "post-restart reply");
        // Decision 70: m5 is the last new message (recent target), so
        // the send is a plain standalone message.
        assert_eq!(reply_to, None);
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        let inputs = gate
            .calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(inputs.len(), 1);
        let row_ids: Vec<i64> = inputs[0]
            .new_messages
            .iter()
            .map(|msg| msg.row_id)
            .collect();
        assert_eq!(row_ids, vec![4, 5]);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn startup_repairs_a_malformed_wake_marker_to_the_raw_log_tail() {
        // The MALFORMED-key shape of the same repair: decode cannot
        // tell a malformed value from an absent key (both fall back to
        // the fresh default), so the startup repair treats the two
        // shapes identically. Wake services stay unwired here: the
        // repair is independent of the disabled/enabled path.
        let fixture = make_fixture();
        let prior: Vec<_> = (1..=2)
            .map(|index| message(&format!("old{index}"), index, false))
            .collect();
        insert_messages_without_session(&fixture, &prior).await;
        set_state_directly(&fixture, "wake_last_row_id", "not-a-number").await;

        let handle = spawn_on(&fixture, TriggerConfig::default());
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 2);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn startup_keeps_a_valid_wake_marker() {
        // No regression of the normal restart path: a PRESENT,
        // well-formed marker (here 3, advanced by a live wake) is kept
        // as persisted — the startup repair must not move it and must
        // not log.
        let fixture = make_fixture();
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("reply one");
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
        let _ = expect_send_text(next_action(&mut outbound).await);
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        handle.shutdown().await.expect("shutdown succeeds");

        // The restart on the same store: the marker key exists and
        // parses, so the repair stays out.
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let restarted = spawn_on(&fixture, TriggerConfig::default());
        let session = restarted.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 3);
        assert!(!capture.contains("repaired to the raw-log tail"));
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn an_empty_group_keeps_the_wake_marker_at_zero() {
        // The repair needs a tail: a group with NO log rows keeps the
        // fresh default 0 and nothing is logged or written.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let handle = spawn_on(&fixture, TriggerConfig::default());
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 0);
        assert!(!capture.contains("repaired to the raw-log tail"));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn disabled_wake_services_still_advance_the_wake_marker() {
        // Decision 65: with no gate/reply provider wired (wake: None),
        // a wake that WOULD fire still advances `wake_last_row_id`
        // past the messages it would have presented, so the presented
        // range cannot grow without bound while the services are off.
        // The scheduler reset of the suppressed trigger fire is
        // unchanged; no gate/reply call and no outbound exist on this
        // path by construction.
        let fixture = make_fixture();
        let handle = spawn_on(&fixture, wake_config(2));

        // m1: below the threshold; the marker stays put.
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 0);

        // m2: the count threshold fires; the suppressed wake advances
        // the marker past m1-m2 (the Section 9.6 gather range).
        handle
            .send_event(InboundEvent::Message(message("m2", 2, false)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 2);
        assert_eq!(
            counter_value(&fixture.store, "wake_last_row_id").await,
            Some(2),
            "the suppressed wake persisted the advanced marker"
        );

        // m3: counting again toward the next wake; the marker stays.
        handle
            .send_event(InboundEvent::Message(message("m3", 3, false)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 2);

        // m4: the threshold fires again; the marker covers m3-m4.
        handle
            .send_event(InboundEvent::Message(message("m4", 4, false)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 4);

        // A mention is a wake that WOULD be forced (Section 8.1); the
        // disabled path advances the marker past it too.
        handle
            .send_event(InboundEvent::Message(message("m5", 5, true)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 5);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn disabled_wake_services_advance_the_marker_on_a_tick_wake() {
        // The tick half of the disabled path: an interval fire with
        // wake: None advances the marker exactly like the intake fire.
        // Count 100 so only the interval can fire; the tick sits far
        // past every jittered interval.
        let fixture = make_fixture();
        let config = TriggerConfig {
            wake_msg_count: 100,
            wake_floor: Duration::ZERO,
            wake_interval: Duration::from_secs(60),
            ..TriggerConfig::default()
        };
        let handle = spawn_on(&fixture, config);
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 0);

        handle
            .send(ActorCommand::Tick(t0() + time::Duration::hours(2)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.wake_last_row_id, 1);
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

    #[tokio::test]
    async fn a_recent_target_sends_a_plain_standalone_message() {
        // Decision 70 (specs.md Section 6.2): a NON-forced wake reply
        // quotes its target only when MORE than `reply_quote_threshold`
        // (default 10) newer human messages arrived after it. The gate
        // targets the LAST new message (distance 0), so the send is a
        // plain standalone message: a Telegram reply notifies the
        // author, and a recent target needs no context anchor.
        let fixture = make_fixture();
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("standalone reply");
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

        let (chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(chat_id, CHAT_ID);
        assert_eq!(text, "standalone reply");
        assert_eq!(reply_to, None);
        wait_for_counter(&fixture.store, "participations_total", 1).await;

        // The raw-log row still names the INTERNAL target (the same
        // rule as the curated wake line's `reply_to`, decision 53
        // freeze): the quote decision touches only the platform send.
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[3].direction, Direction::Outbound);
        assert_eq!(rows[3].reply_to_platform_msg_id, Some("m3".to_string()));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_stale_target_within_the_staleness_threshold_is_quoted() {
        // Decision 70 with the SAME distance metric as the Section 6.2
        // staleness re-check. The gate targets the FIRST new message
        // (m1); eleven newer human messages arrive while the wake is in
        // flight — past `reply_quote_threshold` (10) but within
        // `reply_staleness_threshold` (20): the reply is SENT (not
        // discarded) and it QUOTES the target — a stale target needs
        // the context anchor.
        let fixture = make_fixture();
        let hold = Arc::new(Notify::new());
        let gate = ScriptedGate::yes(GateTarget::First);
        let reply = ScriptedReply::held("quoted reply", Arc::clone(&hold));
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(3),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        // The count threshold fires the wake; the reply generation
        // blocks on the hold, so the wake stays in flight.
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
        wait_for_reply_calls(&reply, 1).await;
        // Eleven newer human messages after the target. Their count
        // fires are in-flight no-ops (Section 6.2: inbound messages
        // during a running wake do not interrupt the call).
        for index in 4..=14 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        hold.notify_one();

        let (chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(chat_id, CHAT_ID);
        assert_eq!(text, "quoted reply");
        assert_eq!(reply_to, Some("m1".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        // No further event arrives, so the pending count fires of the
        // in-flight period never turn into a second wake.
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
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
        // participates). Decision 70: m1 is the only new message
        // (recent target), so the send is a plain standalone message.
        assert_eq!(text, "timer reply");
        assert_eq!(reply_to, None);
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
        // forced one second. Decision 70: the first wake is unforced
        // and its target p3 is recent (one newer human message ≤ the
        // quote threshold), so it sends a plain standalone message; the
        // forced wake always quotes.
        let (_, _, first_reply_to) = expect_send_text(next_action(&mut outbound).await);
        let (_, _, second_reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(first_reply_to, None);
        assert_eq!(second_reply_to, Some("m4".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 2).await;
        // The gate ran exactly once: the forced wake bypassed it
        // (Section 8.1).
        assert_eq!(gate.call_count(), 1);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    // --- Decision 79 (c) forced-wake cooldown tests ---

    #[tokio::test]
    async fn a_forced_reply_starts_the_cooldown_and_suppresses_the_next_forcing() {
        // specs.md Sections 8.1/6.2, decision 79 (c): a forced wake
        // that produced a reply starts the 10 s cooldown anchored at
        // the forcing message's intake timestamp (m1 at t0+1 s, so the
        // window runs to t0+11 s). A forcing inside the window is
        // suppressed — the row is persisted and in the context, but NO
        // wake fires and NO queue entry forms; a forcing past the
        // window fires normally.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let gate = ScriptedGate::no();
        let reply = ScriptedReply::new("cooldown reply");
        // A high count: only a mention can fire the wake.
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
        let (_, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "cooldown reply");
        assert_eq!(reply_to, Some("m1".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 1).await;

        // The second mention lands INSIDE the window: suppressed. The
        // one DEBUG line names the suppression (decision 53: no
        // curated `wake` line — no wake happened).
        handle
            .send_event(InboundEvent::Message(message("m2", 6, true)))
            .await
            .expect("send succeeds");
        wait_for_event(
            &capture,
            "forced wake suppressed: the forced-wake cooldown is running",
        )
        .await;
        // No wake, no reply, no new outbound row, no queued entry
        // (a queued entry would fire its own wake later — the totals
        // below prove none exists).
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert_eq!(reply.call_count(), 1);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(1));
        assert_eq!(list_messages(&fixture.store).await.len(), 3);

        // Past the window a forcing fires normally — and the suppressed
        // mention never produced its own wake (2 wakes total, one per
        // fired mention).
        handle
            .send_event(InboundEvent::Message(message("m3", 12, true)))
            .await
            .expect("send succeeds");
        let (_, _, third_reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(third_reply_to, Some("m3".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 2).await;
        assert_eq!(reply.call_count(), 2);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_zero_cooldown_disables_the_suppression() {
        // Decision 79 (c): `forced_wake_cooldown = 0` disables the
        // suppression — two mentions 1 s apart produce two replies
        // (the pre-79 rally shape).
        let fixture = make_fixture();
        let gate = ScriptedGate::no();
        let reply = ScriptedReply::new("rally reply");
        let config = TriggerConfig {
            forced_wake_cooldown: Duration::ZERO,
            ..wake_config(100)
        };
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            config,
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        for (id, seconds) in [("m1", 1), ("m2", 2)] {
            handle
                .send_event(InboundEvent::Message(message(id, seconds, true)))
                .await
                .expect("send succeeds");
            let (_, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
            assert_eq!(text, "rally reply");
            assert_eq!(reply_to, Some(id.to_string()));
        }
        wait_for_counter(&fixture.store, "participations_total", 2).await;
        assert_eq!(reply.call_count(), 2);
        assert_eq!(counter_value(&fixture.store, "wakes_total").await, Some(2));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_suppressed_mention_lands_in_the_next_wakes_presented_set() {
        // Decision 79 (c), Rule P1: the suppression touches the TRIGGER
        // only — the suppressed mention is persisted and enters the
        // live context exactly as today, so the NEXT wake presents it
        // and no information is lost.
        let fixture = make_fixture();
        let gate = ScriptedGate::yes(GateTarget::Last);
        let reply = ScriptedReply::new("presented reply");
        // Count threshold 1: after the suppression a Tick fires an
        // unforced wake over the suppressed mention.
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(1),
            Arc::new(NoopRecall),
            Arc::clone(&gate),
            Arc::clone(&reply),
        );
        handle
            .send_event(InboundEvent::Message(message("m1", 1, true)))
            .await
            .expect("send succeeds");
        expect_send_text(next_action(&mut outbound).await);
        wait_for_counter(&fixture.store, "participations_total", 1).await;

        // Inside the window (t0+6 s < t0+11 s): suppressed.
        handle
            .send_event(InboundEvent::Message(message("m2", 6, true)))
            .await
            .expect("send succeeds");
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;

        // The intake was untouched: the raw log holds both mentions
        // (m1, the bot reply, m2) and the live context carries the
        // suppressed one.
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|row| row.text == "text of m2"));
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert!(items.iter().any(|item| item.content.contains("text of m2")));

        // The next (unforced, tick) wake presents the suppressed
        // mention: its row is in the gate input, and the wake answers
        // it.
        handle
            .send(ActorCommand::Tick(t0() + time::Duration::seconds(100)))
            .await
            .expect("send succeeds");
        wait_for_gate_calls(&gate, 1).await;
        let presented = &gate.calls()[0].new_messages;
        assert!(
            presented.iter().any(|msg| msg.text == "text of m2"),
            "the suppressed mention is in the presented set: {presented:?}"
        );
        let (_, text, _) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "presented reply");
        wait_for_counter(&fixture.store, "participations_total", 2).await;
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_cooldown_does_not_survive_a_restart_and_the_rebuild_is_identical() {
        // Decision 79 (c): the cooldown is IN-MEMORY actor state, NOT
        // P1 rebuild state — on a restart it is simply not running
        // (accepted), and the rebuilt session/context is bit-identical
        // to a no-cooldown restart (the cooldown left no persisted
        // trace).
        let fixture = make_fixture();
        let gate = ScriptedGate::no();
        let reply = ScriptedReply::new("restart reply");
        let (handle, mut outbound) = spawn_with_wake(
            &fixture,
            wake_config(100),
            Arc::new(NoopRecall),
            gate,
            reply,
        );
        handle
            .send_event(InboundEvent::Message(message("m1", 1, true)))
            .await
            .expect("send succeeds");
        expect_send_text(next_action(&mut outbound).await);
        wait_for_counter(&fixture.store, "participations_total", 1).await;
        // The reply started the cooldown (window: t0+1 s .. t0+11 s).
        let first = handle.snapshot().await.expect("snapshot succeeds");
        let first_context = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        handle.shutdown().await.expect("shutdown succeeds");

        // A NEW actor on the same store: the rebuild is bit-identical
        // (the `restart_rebuilds_identical_state` pattern).
        let (restarted, mut restarted_outbound) = spawn_with_wake(
            &fixture,
            wake_config(100),
            Arc::new(NoopRecall),
            ScriptedGate::no(),
            ScriptedReply::new("after restart"),
        );
        let rebuilt = restarted.snapshot().await.expect("snapshot succeeds");
        assert_eq!(first, rebuilt);
        let rebuilt_context = restarted
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(first_context, rebuilt_context);

        // The accepted rebuild behavior: no cooldown is running after
        // the restart, so a mention well INSIDE the original window
        // (t0+2 s < t0+11 s) fires immediately.
        restarted
            .send_event(InboundEvent::Message(message("m2", 2, true)))
            .await
            .expect("send succeeds");
        let (_, text, reply_to) = expect_send_text(next_action(&mut restarted_outbound).await);
        assert_eq!(text, "after restart");
        assert_eq!(reply_to, Some("m2".to_string()));
        wait_for_counter(&fixture.store, "participations_total", 2).await;
        restarted.shutdown().await.expect("shutdown succeeds");
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

        // The reply goes out targeting the last new message. Decision
        // 70: m3 is recent (distance 0), so the send is a plain
        // standalone message with no reply target.
        let (_, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "gate reply");
        assert_eq!(reply_to, None);
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

    // --- Warmup-trigger tests (specs.md Section 9.7, decision 78) ---

    /// A scripted warmup generator (the `ScriptedReply` shape): a FIFO
    /// reply queue (the last queued reply repeats), every request
    /// recorded, and a failing mode. Arc-shareable across actor
    /// restarts — the P1 test respawns on the same doubles.
    struct ScriptedWarmup {
        replies: Mutex<std::collections::VecDeque<String>>,
        failure: Option<String>,
        calls: Mutex<usize>,
        requests: Mutex<Vec<WarmupRequest>>,
    }

    impl ScriptedWarmup {
        fn new(text: &str) -> Arc<Self> {
            Arc::new(Self {
                replies: Mutex::new(std::collections::VecDeque::from([text.to_string()])),
                failure: None,
                calls: Mutex::new(0),
                requests: Mutex::new(Vec::new()),
            })
        }

        /// Every call fails with a warmup error (the generation-failure
        /// path: the slot is consumed, the actor lives).
        fn failing(message: &str) -> Arc<Self> {
            Arc::new(Self {
                replies: Mutex::new(std::collections::VecDeque::new()),
                failure: Some(message.to_string()),
                calls: Mutex::new(0),
                requests: Mutex::new(Vec::new()),
            })
        }

        fn call_count(&self) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }

        fn requests(&self) -> Vec<WarmupRequest> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl WarmupGenerator for ScriptedWarmup {
        fn generate_warmup<'a>(
            &'a self,
            _chat_id: &'a str,
            request: &'a WarmupRequest,
        ) -> Pin<Box<dyn Future<Output = Result<String, CoreError>> + Send + 'a>> {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request.clone());
            let reply = {
                let mut replies = self
                    .replies
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if replies.len() > 1 {
                    replies.pop_front().expect("len > 1 has a front")
                } else {
                    replies.front().cloned().unwrap_or_default()
                }
            };
            let failure = self.failure.clone();
            Box::pin(async move {
                match failure {
                    Some(message) => Err(CoreError::Warmup(message)),
                    None => Ok(reply),
                }
            })
        }
    }

    /// The trigger config of the warmup tests (decision 78), TZ-PROOF:
    /// the active-hours window "00:00-23:59" schedules inside any
    /// host-local day regardless of the dev/CI machine's offset, so a
    /// scheduling tick always produces a slot. Tests that need a
    /// specific due time SEED `warmup_next_at` directly (see
    /// `seed_state`); the host offset never appears in an assertion.
    fn warmup_config() -> TriggerConfig {
        TriggerConfig {
            warmup: true,
            warmup_quota: 1,
            warmup_active_hours: ActiveHours::parse("00:00-23:59").expect("the test window parses"),
            ..TriggerConfig::default()
        }
    }

    /// Spawns an actor with the warmup services wired over a scripted
    /// generator (the `spawn_with_wake` pattern). Returns the outbound
    /// receiver.
    fn spawn_with_warmup(
        fixture: &Fixture,
        config: TriggerConfig,
        generator: Arc<ScriptedWarmup>,
    ) -> (GroupActorHandle, mpsc::Receiver<OutboundAction>) {
        let (outbound_tx, outbound_rx) = mpsc::channel(64);
        let handle = spawn_group_actor(GroupActorParams {
            pet_tag: "tamako".to_string(),
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
            warmup: Some(WarmupServices { generator }),
            summary_provider: None,
            outbound: Some(outbound_tx),
            bot_name: None,
        });
        (handle, outbound_rx)
    }

    /// Writes state-table rows directly BEFORE the actor spawns (the
    /// `set_state_directly` pattern, batched): the actor decodes the
    /// persisted state at startup, so a test SEEDS the warmup schedule
    /// (`warmup_next_at`, backoff, cooldowns, …) instead of reaching
    /// into a live actor.
    async fn seed_state(fixture: &Fixture, pairs: &[(&str, String)]) {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect();
        blocking_store_call(&fixture.store, move |store| {
            store.open_group(CHAT_ID)?;
            store.set_state_many(CHAT_ID, &pairs)
        })
        .await;
    }

    /// The RFC 3339 encoding of a seeded instant (the session encoding
    /// of `warmup_next_at` and the watch timestamps).
    fn rfc3339(at: OffsetDateTime) -> String {
        at.format(&time::format_description::well_known::Rfc3339)
            .expect("a test timestamp formats")
    }

    /// Seeds a DUE `warmup_next_at` slot.
    async fn seed_warmup_due(fixture: &Fixture, due: OffsetDateTime) {
        seed_state(fixture, &[("warmup_next_at", rfc3339(due))]).await;
    }

    /// A scripted warmup topic. `last_activity_at: None` decays as the
    /// documented 30-day default (warmup.rs `STALE_TOPIC_DAYS`), so the
    /// weight is a positive CONSTANT at every tick time these tests use
    /// — t0-based and far-future ticks alike.
    fn topic(name: &str) -> TopicCandidate {
        TopicCandidate {
            node_id: format!("id-{name}"),
            name: name.to_string(),
            edge_count: 10,
            last_activity_at: None,
        }
    }

    /// A human message with an explicit timestamp and text (the
    /// engagement-watch and tail-exclusion tests place rows relative
    /// to the REAL send time of the warmup, which
    /// `OffsetDateTime::now_utc()` owns).
    fn message_at(id: &str, at: OffsetDateTime, text: &str) -> NormalizedMessage {
        NormalizedMessage {
            platform_msg_id: id.to_string(),
            timestamp: at,
            sender_id: "u1".to_string(),
            sender_display_name: "Alice".to_string(),
            username: None,
            text: text.to_string(),
            reply_to_platform_msg_id: None,
            mentions_bot: false,
            is_reply_to_bot: false,
        }
    }

    /// A named reaction event with an explicit timestamp (the
    /// engagement-watch window is wall-clock).
    fn reaction_at(platform_msg_id: &str, at: OffsetDateTime) -> ReactionEvent {
        ReactionEvent {
            platform_msg_id: platform_msg_id.to_string(),
            timestamp: at,
            reactor_id: Some("u1".to_string()),
            anonymous: false,
            aggregated: false,
            old_emojis: vec![],
            new_emojis: vec!["👍".to_string()],
        }
    }

    /// Polls until the warmup double records `want` calls (bounded; the
    /// `wait_for_reply_calls` pattern).
    async fn wait_for_warmup_calls(generator: &ScriptedWarmup, want: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if generator.call_count() >= want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {want} warmup calls"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Fires one warmup over the scripted topic: seed a due slot, tick
    /// at it, wait for the generation and the send path. Returns the
    /// handle and the outbound receiver. The snapshot after
    /// `warmups_total` proves the completion handler ran (the counter
    /// bumps inside it).
    async fn fire_one_warmup(
        fixture: &Fixture,
        config: TriggerConfig,
        generator: Arc<ScriptedWarmup>,
        due: OffsetDateTime,
    ) -> (GroupActorHandle, mpsc::Receiver<OutboundAction>) {
        seed_warmup_due(fixture, due).await;
        let (handle, outbound) = spawn_with_warmup(fixture, config, generator);
        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        wait_for_counter(&fixture.store, "warmups_total", 1).await;
        (handle, outbound)
    }

    #[tokio::test]
    async fn p1_restart_keeps_the_persisted_schedule_and_fires_exactly_once() {
        // Rule P1 (specs.md Section 8.4): the scheduled `warmup_next_at`
        // persists; a restart never reshuffles it.
        let fixture = make_fixture();
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let config = warmup_config();
        let (handle, _outbound) =
            spawn_with_warmup(&fixture, config.clone(), Arc::clone(&generator));

        // Tick inside active hours ("00:00-23:59" is TZ-proof): the
        // first slot is scheduled and persisted.
        let t1 = t0() + time::Duration::hours(1);
        handle
            .send(ActorCommand::Tick(t1))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        let next_at = session.warmup_next_at.expect("the first slot is scheduled");
        assert!(next_at >= t1, "the slot lies at or after the tick");
        handle.shutdown().await.expect("shutdown succeeds");

        // The restart on the SAME store with the SAME scripted doubles:
        // the persisted schedule comes back byte-identically (no
        // reshuffle).
        let (restarted, mut outbound) =
            spawn_with_warmup(&fixture, config.clone(), Arc::clone(&generator));
        let session = restarted.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.warmup_next_at, Some(next_at), "no reshuffle");

        // A tick at exactly the persisted slot fires exactly once.
        restarted
            .send(ActorCommand::Tick(next_at))
            .await
            .expect("send succeeds");
        wait_for_warmup_calls(&generator, 1).await;
        let (chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(chat_id, CHAT_ID);
        assert_eq!(text, "warmup hello");
        assert_eq!(reply_to, None);
        wait_for_counter(&fixture.store, "warmups_total", 1).await;
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].direction, Direction::Outbound);
        let session = restarted.snapshot().await.expect("snapshot succeeds");
        let advanced = session
            .warmup_next_at
            .expect("the next slot is scheduled at fire time");
        assert!(
            advanced > next_at,
            "the schedule advanced past the fired slot"
        );
        restarted.shutdown().await.expect("shutdown succeeds");

        // A second restart: no double-fire. A tick at the fired slot is
        // not due (the schedule advanced past it).
        let (third, _outbound) = spawn_with_warmup(&fixture, config, generator.clone());
        third
            .send(ActorCommand::Tick(next_at))
            .await
            .expect("send succeeds");
        let session = third.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.warmup_next_at, Some(advanced));
        assert_eq!(generator.call_count(), 1);
        third.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_due_slot_is_skipped_while_muted() {
        // Section 9.7 step 1, the muted gate: the slot is consumed and
        // rescheduled, the skip logs at DEBUG, nothing is generated.
        let fixture = make_fixture();
        let due = t0() + time::Duration::hours(1);
        seed_state(
            &fixture,
            &[
                ("warmup_next_at", rfc3339(due)),
                ("muted_flag", "1".to_string()),
            ],
        )
        .await;
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let generator = ScriptedWarmup::new("never used");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let (handle, mut outbound) =
            spawn_with_warmup(&fixture, warmup_config(), generator.clone());

        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_ne!(session.warmup_next_at, Some(due), "the slot was consumed");
        assert!(
            session.warmup_next_at.is_some(),
            "the next slot is scheduled"
        );
        assert!(capture.contains("gate=\"muted\""));
        assert_eq!(generator.call_count(), 0);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert!(list_messages(&fixture.store).await.is_empty());
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_quota_floor_keeps_a_due_slot_firing_under_backoff() {
        // Decision 79 (a), replacing the decision-78 zero-floor skip
        // test: the effective quota floors at 1, so quota 1 with
        // backoff factor 3 no longer gate-skips — the slot FIRES, and
        // the backoff only lengthens the spacing: the rescheduled slot
        // honors the 2^3 = 8× window gap.
        let fixture = make_fixture();
        let due = t0() + time::Duration::hours(1);
        seed_state(
            &fixture,
            &[
                ("warmup_next_at", rfc3339(due)),
                ("warmup_backoff_factor", "3".to_string()),
            ],
        )
        .await;
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let config = warmup_config();
        let (handle, mut outbound) = spawn_with_warmup(&fixture, config.clone(), generator.clone());

        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        wait_for_warmup_calls(&generator, 1).await;
        assert_eq!(generator.call_count(), 1);
        let (_, text, _) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "warmup hello");
        wait_for_counter(&fixture.store, "warmups_total", 1).await;
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].direction, Direction::Outbound);

        // The reschedule at fire time: effective quota 1 makes the
        // whole active window one slot (the span comes from the
        // config's own "00:00-23:59"), and factor 3 pushes the next
        // slot ≥ 8 windows past the fired one.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        let next_at = session
            .warmup_next_at
            .expect("the floor keeps the next slot schedulable");
        let min_gap_secs =
            config.warmup_active_hours.span_seconds() * crate::warmup::interval_multiplier(3);
        assert!(
            next_at >= due + time::Duration::seconds(min_gap_secs as i64),
            "the next slot honors the 2^3 spacing: {next_at} ≥ {due} + {min_gap_secs} s"
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_due_slot_is_skipped_until_the_silence_window_passes() {
        // Section 8.4: the newest raw-log row of ANY direction must be
        // at least `warmup_silence` old. One message at t; a slot due
        // 60 s short of the window is skipped; a slot due exactly at
        // the window boundary fires (the gate is `>=`).
        let fixture = make_fixture();
        let config = warmup_config();
        let silence = config.warmup_silence;
        let spoke_at = t0();
        let due = spoke_at + silence - time::Duration::seconds(60);
        seed_warmup_due(&fixture, due).await;
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let (handle, mut outbound) = spawn_with_warmup(&fixture, config.clone(), generator.clone());
        handle
            .send_event(InboundEvent::Message(message("m1", 0, false)))
            .await
            .expect("send succeeds");

        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_ne!(session.warmup_next_at, Some(due), "the slot was consumed");
        assert!(capture.contains("gate=\"silence\""));
        assert_eq!(generator.call_count(), 0);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        handle.shutdown().await.expect("shutdown succeeds");

        // The second due slot at the boundary fires (the reseed keeps
        // the due time deterministic; the doubles are the same Arcs).
        let due2 = spoke_at + silence;
        seed_warmup_due(&fixture, due2).await;
        let (restarted, mut outbound) = spawn_with_warmup(&fixture, config, generator.clone());
        restarted
            .send(ActorCommand::Tick(due2))
            .await
            .expect("send succeeds");
        wait_for_warmup_calls(&generator, 1).await;
        let (_, text, _) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "warmup hello");
        wait_for_counter(&fixture.store, "warmups_total", 1).await;
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].direction, Direction::Outbound);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn overlength_warmup_is_rejected_before_persist() {
        // Decision 100 (B1): an overlength warmup text is rejected
        // before the Rule B1 write — nothing persists, nothing sends,
        // no quota consumed, no engagement watch opened.
        let fixture = make_fixture();
        let generator = ScriptedWarmup::new(&"x".repeat(4097));
        fixture.memory.set_topics(vec![topic("Tea")]);
        let due = t0() + time::Duration::hours(1);
        seed_warmup_due(&fixture, due).await;
        let (handle, mut outbound) =
            spawn_with_warmup(&fixture, warmup_config(), Arc::clone(&generator));
        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        wait_for_warmup_calls(&generator, 1).await;
        // The FIFO barrier: the snapshot command lands behind the
        // WarmupCompleted completion, so the reject has run.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(
            session.warmup_quota_used_today, 0,
            "a rejected warmup consumes no quota"
        );
        assert!(
            session.warmup_watch_row_id.is_none(),
            "a rejected warmup opens no engagement watch"
        );
        assert!(
            outbound.try_recv().is_err(),
            "a rejected warmup sends nothing"
        );
        assert!(
            list_messages(&fixture.store).await.is_empty(),
            "a rejected warmup persists no outbound row"
        );
        assert_eq!(
            counter_value(&fixture.store, "warmups_total").await,
            None,
            "a rejected warmup bumps no counter"
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn warmup_disabled_never_evaluates() {
        // Section 8.4: the `warmup` master switch disables the trigger
        // ENTIRELY — a due slot is not even consumed (no state touch).
        let fixture = make_fixture();
        let due = t0() + time::Duration::hours(1);
        seed_warmup_due(&fixture, due).await;
        let generator = ScriptedWarmup::new("never used");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let mut config = warmup_config();
        config.warmup = false;
        let (handle, mut outbound) = spawn_with_warmup(&fixture, config, generator.clone());

        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        // Untouched: the schedule, the quota-day rollover, everything.
        assert_eq!(session.warmup_next_at, Some(due));
        assert_eq!(session.warmup_quota_day, None);
        assert_eq!(session.warmup_quota_used_today, 0);
        assert_eq!(generator.call_count(), 0);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert!(list_messages(&fixture.store).await.is_empty());
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn unwired_warmup_services_are_fully_inert() {
        // `warmup: None` disables the trigger entirely (decision 78).
        // This doubles as the replay-discipline coverage: replay mode
        // wires None, and a replayed tick must not touch the warmup
        // state.
        let fixture = make_fixture();
        let due = t0() + time::Duration::hours(1);
        seed_warmup_due(&fixture, due).await;
        let handle = spawn_on(&fixture, warmup_config());

        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.warmup_next_at, Some(due));
        assert_eq!(session.warmup_quota_day, None);
        assert_eq!(session.warmup_quota_used_today, 0);
        assert_eq!(session.warmup_backoff_factor, 0);
        assert!(!session.warmup_watch_pending);
        assert!(list_messages(&fixture.store).await.is_empty());
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_topic_on_cooldown_or_in_the_tail_stays_silent() {
        // Section 9.7 step 2 (the actor wiring; the pure pick rules are
        // warmup.rs's): one candidate is named in the 50-row raw-log
        // tail (never restart the conversation that just went quiet),
        // the other is inside its per-topic cooldown. No eligible topic
        // means no warmup — the slot is consumed at DEBUG.
        let fixture = make_fixture();
        // The tail message is old enough to pass the silence gate.
        insert_messages_without_session(
            &fixture,
            &[message_at(
                "tail1",
                t0() - time::Duration::hours(100),
                "we discussed Graph Database all evening",
            )],
        )
        .await;
        // The cooldown date uses the SAME local-offset logic the actor
        // uses (TZ-proof: seeded at spawn time, the tick lands in the
        // same local day).
        let due = t0();
        let today = local_date_string(
            due,
            UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC),
        );
        seed_state(
            &fixture,
            &[
                ("warmup_next_at", rfc3339(due)),
                ("warmup_topic_cooldowns", format!("{{\"tea\":\"{today}\"}}")),
            ],
        )
        .await;
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let generator = ScriptedWarmup::new("never used");
        fixture
            .memory
            .set_topics(vec![topic("Tea"), topic("Graph Database")]);
        let (handle, mut outbound) =
            spawn_with_warmup(&fixture, warmup_config(), generator.clone());

        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_ne!(session.warmup_next_at, Some(due), "the slot was consumed");
        assert!(capture.contains("no eligible topic"));
        assert_eq!(generator.call_count(), 0);
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        // Only the seeded human row exists; nothing was sent.
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].direction, Direction::Inbound);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_warmup_request_carries_the_preamble_and_the_topic() {
        // Section 9.7 step 3: the generation input is the live context
        // as LLM-facing messages (Rule C4: item 0 is the preamble) plus
        // the sampled topic's display name.
        let fixture = make_fixture();
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let due = t0() + time::Duration::hours(1);
        let (handle, _outbound) =
            fire_one_warmup(&fixture, warmup_config(), generator.clone(), due).await;

        let requests = generator.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].topic, "Tea");
        assert_eq!(
            requests[0].messages.len(),
            1,
            "an empty group: preamble only"
        );
        assert_eq!(requests[0].messages[0].role, ContextRole::System);
        assert_eq!(requests[0].messages[0].content, TEST_PREAMBLE);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_parrot_filter_applies_to_warmup_text() {
        // Section 9.7 step 3: decisions 59/64 guard the warmup text
        // like every reply; a stripped warmup emits the decision-59
        // WARN.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let generator = ScriptedWarmup::new("I remember: Alice likes tea.\nreal text");
        fixture.memory.set_topics(vec![topic("Coffee")]);
        let due = t0() + time::Duration::hours(1);
        let (handle, mut outbound) =
            fire_one_warmup(&fixture, warmup_config(), generator.clone(), due).await;

        assert!(capture.contains("the warmup text carries imitated structure or fence debris"));
        let (_, text, _) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(text, "real text");
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text, "real text");
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_outbound_row_precedes_the_send_and_never_quotes() {
        // Section 9.7 step 4: a PLAIN standalone message (proactive
        // speech never quotes a target); Rule B1 — the raw-log row
        // exists (and carries the synthetic `bot-out:` id) by the time
        // the outbound action leaves.
        let fixture = make_fixture();
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let due = t0() + time::Duration::hours(1);
        let (handle, mut outbound) =
            fire_one_warmup(&fixture, warmup_config(), generator.clone(), due).await;

        let (chat_id, text, reply_to) = expect_send_text(next_action(&mut outbound).await);
        assert_eq!(chat_id, CHAT_ID);
        assert_eq!(text, "warmup hello");
        assert_eq!(reply_to, None);
        // The row was persisted BEFORE the send: by the time the action
        // arrived, the log already held it.
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        let bot_row = &rows[0];
        assert_eq!(bot_row.direction, Direction::Outbound);
        assert_eq!(bot_row.reply_to_platform_msg_id, None);
        assert!(bot_row.platform_msg_id.starts_with("bot-out:"));
        assert_eq!(bot_row.sender_display_name, "Tamako");

        // Rule C1: the context snapshot ends with the bot speech.
        let items = handle
            .context_snapshot()
            .await
            .expect("context snapshot succeeds");
        assert_eq!(items.len(), 2);
        let last = items.last().expect("items exist");
        assert_eq!(last.kind, ContextItemKind::BotSpeech);
        assert_eq!(
            last.content,
            render_bot_content(bot_row.id, bot_row.timestamp, "warmup hello", "tamako")
        );
        assert_eq!(last.range_tag, Some(RangeTag::single(bot_row.id)));

        // The Section 8.5 monologue lock covers the warmup speech.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.consecutive_bot_msgs, 1);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_human_reply_inside_the_window_engages_and_resets_the_backoff() {
        // Section 8.5 (decision 78 (d)): a human reply inside
        // `warmup_reaction_window` engages the warmup and resets the
        // soft backoff to zero. Quota 2 with the factor seeded at 1:
        // the effective quota 1 lets the fire happen AND the reset to 0
        // is observable.
        let fixture = make_fixture();
        let mut config = warmup_config();
        config.warmup_quota = 2;
        let due = t0() + time::Duration::hours(1);
        seed_state(
            &fixture,
            &[
                ("warmup_next_at", rfc3339(due)),
                ("warmup_backoff_factor", "1".to_string()),
            ],
        )
        .await;
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let (handle, mut outbound) = spawn_with_warmup(&fixture, config, generator.clone());
        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        wait_for_counter(&fixture.store, "warmups_total", 1).await;
        let _ = expect_send_text(next_action(&mut outbound).await);

        // The watch is open (the timestamps are REAL: the send path
        // stamps them with OffsetDateTime::now_utc()).
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert!(session.warmup_watch_pending);
        assert_eq!(session.warmup_backoff_factor, 1);
        let sent_at = session
            .warmup_watch_sent_at
            .expect("the watch has a send time");
        let expires = session
            .warmup_watch_expires_at
            .expect("the watch has an expiry");

        // The reply lands inside the window; the tick at expiry
        // resolves the watch.
        handle
            .send_event(InboundEvent::Message(message_at(
                "r1",
                sent_at + time::Duration::minutes(5),
                "nice one",
            )))
            .await
            .expect("send succeeds");
        handle
            .send(ActorCommand::Tick(expires))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(
            session.warmup_backoff_factor, 0,
            "engagement resets the backoff"
        );
        assert!(!session.warmup_watch_pending);
        // The watch fields stay persisted for forensics.
        assert_eq!(session.warmup_watch_sent_at, Some(sent_at));
        assert!(session.warmup_watch_row_id.is_some());
        assert_eq!(
            counter_value(&fixture.store, "warmup_engaged_total").await,
            Some(1)
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_reply_after_the_window_does_not_count() {
        // Section 8.5: a reply past `warmup_reaction_window` (the
        // timestamp bound keeps the window honest) does not engage; the
        // backoff increments.
        let fixture = make_fixture();
        let due = t0() + time::Duration::hours(1);
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let (handle, mut outbound) =
            fire_one_warmup(&fixture, warmup_config(), generator, due).await;
        let _ = expect_send_text(next_action(&mut outbound).await);
        let session = handle.snapshot().await.expect("snapshot succeeds");
        let sent_at = session
            .warmup_watch_sent_at
            .expect("the watch has a send time");
        let expires = session
            .warmup_watch_expires_at
            .expect("the watch has an expiry");

        // The default window is 30 min: the reply at sent+31 min is out.
        handle
            .send_event(InboundEvent::Message(message_at(
                "r1",
                sent_at + time::Duration::minutes(31),
                "too late",
            )))
            .await
            .expect("send succeeds");
        handle
            .send(ActorCommand::Tick(sent_at + time::Duration::minutes(32)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert!(expires <= sent_at + time::Duration::minutes(32));
        assert_eq!(session.warmup_backoff_factor, 1);
        assert!(!session.warmup_watch_pending);
        assert_eq!(
            counter_value(&fixture.store, "warmup_engaged_total").await,
            None
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_reaction_on_an_unlogged_message_inside_the_window_engages() {
        // Section 8.5 (decision 78 (d)), the documented approximation:
        // outbound rows carry synthetic `bot-out:{nanos}` ids (Rule
        // A3), so a reaction whose platform id is ABSENT from the log
        // is treated as a reaction to the warmup.
        let fixture = make_fixture();
        let due = t0() + time::Duration::hours(1);
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let (handle, mut outbound) =
            fire_one_warmup(&fixture, warmup_config(), generator, due).await;
        let _ = expect_send_text(next_action(&mut outbound).await);
        let session = handle.snapshot().await.expect("snapshot succeeds");
        let sent_at = session
            .warmup_watch_sent_at
            .expect("the watch has a send time");
        let expires = session
            .warmup_watch_expires_at
            .expect("the watch has an expiry");

        handle
            .send_event(InboundEvent::Reaction(reaction_at(
                "unlogged-1",
                sent_at + time::Duration::minutes(5),
            )))
            .await
            .expect("send succeeds");
        handle
            .send(ActorCommand::Tick(expires))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert!(!session.warmup_watch_pending);
        assert_eq!(session.warmup_backoff_factor, 0);
        assert_eq!(
            counter_value(&fixture.store, "warmup_engaged_total").await,
            Some(1)
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_reaction_on_a_logged_human_message_does_not_count() {
        // The absence heuristic, the other half: a reaction targeting
        // a LOGGED human message is not a reaction to the warmup.
        let fixture = make_fixture();
        // The logged human message predates the warmup (old enough to
        // pass the silence gate).
        insert_messages_without_session(
            &fixture,
            &[message_at(
                "m1",
                t0() - time::Duration::hours(100),
                "hello group",
            )],
        )
        .await;
        let due = t0();
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let (handle, mut outbound) =
            fire_one_warmup(&fixture, warmup_config(), generator, due).await;
        let _ = expect_send_text(next_action(&mut outbound).await);
        let session = handle.snapshot().await.expect("snapshot succeeds");
        let sent_at = session
            .warmup_watch_sent_at
            .expect("the watch has a send time");
        let expires = session
            .warmup_watch_expires_at
            .expect("the watch has an expiry");

        handle
            .send_event(InboundEvent::Reaction(reaction_at(
                "m1",
                sent_at + time::Duration::minutes(5),
            )))
            .await
            .expect("send succeeds");
        handle
            .send(ActorCommand::Tick(expires))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert!(!session.warmup_watch_pending);
        assert_eq!(
            session.warmup_backoff_factor, 1,
            "unengaged: the backoff increments"
        );
        assert_eq!(
            counter_value(&fixture.store, "warmup_engaged_total").await,
            None
        );
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn one_curated_info_line_per_sent_warmup() {
        // Decision 53 discipline / decision 78: exactly one INFO
        // `warmup` line per SENT warmup; a gate-skipped slot emits no
        // such line.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);

        // Phase 1: a gate-skipped slot (the muted gate — the
        // decision-79 (a) floor keeps the effective quota ≥ 1, so the
        // quota gate can no longer skip on a zeroed quota) — no line.
        let due = t0();
        seed_state(
            &fixture,
            &[
                ("warmup_next_at", rfc3339(due)),
                ("muted_flag", "1".to_string()),
            ],
        )
        .await;
        let (handle, mut outbound) =
            spawn_with_warmup(&fixture, warmup_config(), generator.clone());
        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_ne!(
            session.warmup_next_at,
            Some(due),
            "the skipped slot was consumed"
        );
        assert!(
            session.warmup_next_at.is_some(),
            "the next slot is scheduled"
        );
        assert_eq!(generator.call_count(), 0);
        // No curated line: it is the only event kind that carries
        // action="sent" together with the `warmup` message.
        assert_eq!(
            capture.count_matching(&["message=warmup ", "action=\"sent\""]),
            0
        );
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        handle.shutdown().await.expect("shutdown succeeds");

        // Phase 2: a real fire — exactly ONE curated line, naming the
        // topic, on the same capture. The reseed lifts the Phase-1
        // mute (the seeded state persists on the same store).
        let due2 = t0() + time::Duration::hours(2);
        seed_state(
            &fixture,
            &[
                ("warmup_next_at", rfc3339(due2)),
                ("muted_flag", "0".to_string()),
            ],
        )
        .await;
        let (restarted, mut outbound) =
            spawn_with_warmup(&fixture, warmup_config(), generator.clone());
        restarted
            .send(ActorCommand::Tick(due2))
            .await
            .expect("send succeeds");
        wait_for_counter(&fixture.store, "warmups_total", 1).await;
        let _ = expect_send_text(next_action(&mut outbound).await);
        // Exactly ONE curated line: exactly one event carries the
        // `warmup` message with action="sent", and it names the topic
        // (a `%`-field renders UNQUOTED in the capture: `topic=Tea`).
        assert_eq!(
            capture.count_matching(&["message=warmup ", "action=\"sent\"", "topic=Tea"]),
            1
        );
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn the_quota_counter_rolls_over_on_a_new_local_day() {
        // Section 8.4 (decision 78 (b)): the quota counter belongs to
        // one host-local day. Quota 2: the day-2 watch resolution (no
        // engagement) increments the factor to 1, and the effective
        // quota 1 still lets the day-2 slot fire. The ≥ 25 h tick gap
        // crosses a date boundary in every local TZ.
        let fixture = make_fixture();
        let mut config = warmup_config();
        config.warmup_quota = 2;
        let due = t0() + time::Duration::hours(1);
        let generator = ScriptedWarmup::new("warmup hello");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let (handle, mut outbound) =
            fire_one_warmup(&fixture, config.clone(), generator.clone(), due).await;
        let _ = expect_send_text(next_action(&mut outbound).await);
        // The quota accounting of one sent warmup (TZ-safe: the day
        // string is whatever the actor's host-local day is).
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.warmup_quota_used_today, 1);
        assert!(session.warmup_quota_day.is_some());
        handle.shutdown().await.expect("shutdown succeeds");

        // Day 2 (≥ 25 h later): the counter rolls back to a usable
        // state and the reseeded slot fires again. The cooldowns are
        // reseeded empty: fire 1 put "tea" on its 3-day cooldown, which
        // day 2 has not outlived.
        let day2 = OffsetDateTime::now_utc() + time::Duration::hours(25);
        seed_state(
            &fixture,
            &[
                ("warmup_next_at", rfc3339(day2)),
                ("warmup_topic_cooldowns", "{}".to_string()),
            ],
        )
        .await;
        let (restarted, mut outbound) = spawn_with_warmup(&fixture, config, generator.clone());
        restarted
            .send(ActorCommand::Tick(day2))
            .await
            .expect("send succeeds");
        wait_for_counter(&fixture.store, "warmups_total", 2).await;
        let _ = expect_send_text(next_action(&mut outbound).await);
        let session = restarted.snapshot().await.expect("snapshot succeeds");
        assert_eq!(session.warmup_quota_used_today, 1);
        assert!(session.warmup_quota_day.is_some());
        assert_eq!(generator.call_count(), 2);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn a_failed_warmup_generation_consumes_the_slot_and_the_actor_lives() {
        // The failure doctrine of `CoreError::Warmup` (the Wake
        // analog): one ERROR, the slot stays consumed (it was spent at
        // fire time — the next slot is the natural retry), nothing is
        // sent, and the actor keeps serving.
        let fixture = make_fixture();
        let capture = EventCapture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let due = t0() + time::Duration::hours(1);
        seed_warmup_due(&fixture, due).await;
        let generator = ScriptedWarmup::failing("the reply endpoint is down");
        fixture.memory.set_topics(vec![topic("Tea")]);
        let (handle, mut outbound) =
            spawn_with_warmup(&fixture, warmup_config(), generator.clone());

        handle
            .send(ActorCommand::Tick(due))
            .await
            .expect("send succeeds");
        wait_for_warmup_calls(&generator, 1).await;
        // The failure line proves the completion handler ran (the
        // in-flight flag resets there).
        wait_for_event(&capture, "warmup generation failed").await;
        assert_no_action(&mut outbound, Duration::from_millis(200)).await;
        assert!(list_messages(&fixture.store).await.is_empty());
        assert_eq!(counter_value(&fixture.store, "warmups_total").await, None);

        // The slot was consumed at fire time; the actor still serves.
        let session = handle.snapshot().await.expect("snapshot succeeds");
        assert_ne!(session.warmup_next_at, Some(due), "the slot was consumed");
        assert!(!session.warmup_watch_pending);
        handle.shutdown().await.expect("shutdown succeeds");
    }
}
