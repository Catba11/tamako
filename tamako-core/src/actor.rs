//! The per-group actor. Refer to specs.md Section 6.
//!
//! Message intake appends to the raw log and updates the counters. The
//! digest trigger (specs.md Section 8.2) is wired in Phase 1 (M1): when
//! the trigger fires, the actor launches the digest pipeline as a
//! spawned task and the result returns through the FIFO inbox. The wake
//! procedure stays a stub (M4).
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

use std::sync::Arc;

use rand::rngs::StdRng;
use rand::SeedableRng;
use tamako_memory::{MemoryBackend, MemoryError};
use tamako_store::{
    Direction, EventType, InsertOutcome, NewMessage, NewReaction, Store, StoreError,
};
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::config::TriggerConfig;
use crate::context::{ContextItem, LiveContext};
use crate::digest::{DigestOutcome, DigestPipeline, PostDigestHook};
use crate::event::{InboundEvent, NormalizedMessage, ReactionEvent};
use crate::session::{round_to_millis, SessionState};
use crate::trigger::{digest_should_fire, tail_stats, WakeScheduler};

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
}

/// The default inbox capacity when the caller has no preference.
pub const DEFAULT_INBOX_CAPACITY: usize = 256;

/// Commands of the per-group actor inbox. specs.md Section 6.1, rule 1:
/// all trigger events enter one FIFO inbox.
pub enum ActorCommand {
    Inbound(InboundEvent),
    /// Evaluates the wake timer at the given instant. The Phase 0 demo and
    /// tests drive time explicitly; a real timer driver is Phase 1.
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
        text: msg.text.clone(),
        reply_to_platform_msg_id: msg.reply_to_platform_msg_id.clone(),
        mentions_bot: msg.mentions_bot,
        is_reply_to_bot: msg.is_reply_to_bot,
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
    inbox_sender: &mpsc::Sender<ActorCommand>,
    now: OffsetDateTime,
) -> Result<(), CoreError> {
    let Some(pipeline) = digest else {
        return Ok(());
    };
    if *digest_in_flight {
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
        ..
    } = params;

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
    let mut context = LiveContext::rebuild(preamble, &rows, &injections);

    // One digest at a time per group (Section 6.1, rule 2).
    let mut digest_in_flight = false;

    // --- Inbox loop ---
    while let Some(command) = inbox.recv().await {
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
                // edits identically, so this append is uniform with the
                // restart rebuild.
                if let InsertOutcome::Inserted(id) = outcome {
                    context.append_human_message(
                        id,
                        &msg.sender_display_name,
                        msg.timestamp,
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
                // specs.md Section 6.2: Digest runs BEFORE Wake.
                maybe_launch_digest(
                    &store,
                    &chat_id,
                    &config,
                    &session,
                    digest.as_ref(),
                    &mut digest_in_flight,
                    &inbox_sender,
                    now,
                )
                .await?;
                if wake.should_fire(now, &config) {
                    info!(chat_id = %chat_id, "wake timer fired (stub)");
                    reset_wake(&config, &mut session, &mut wake, &mut rng, now);
                    persist_session(&store, &chat_id, &session).await?;
                }
            }
            ActorCommand::DigestCompleted(result) => {
                digest_in_flight = false;
                match result {
                    Ok(Some(outcome)) => {
                        let (batch_id, kind) = match &outcome {
                            DigestOutcome::Extracted { batch_id, .. } => {
                                (batch_id.as_str(), "extracted")
                            }
                            DigestOutcome::Skeleton { batch_id, .. } => {
                                (batch_id.as_str(), "skeleton")
                            }
                            DigestOutcome::DeadLettered { batch_id, .. } => {
                                (batch_id.as_str(), "dead_lettered")
                            }
                        };
                        info!(
                            chat_id = %chat_id,
                            batch_id,
                            outcome_kind = kind,
                            new_boundary = outcome.new_boundary(),
                            "digest completed"
                        );
                        // Rule C3 context removal with the one-chunk
                        // lag. Only items at or below the PREVIOUS
                        // boundary go; the chunk just digested,
                        // (b_old, b_new], stays as the new overlap
                        // buffer (specs.md Section 7.1). This runs for
                        // EVERY outcome variant, matching the boundary
                        // advancement: a dead-lettered batch is skipped
                        // (specs.md Section 10.3) — the skipped range
                        // stays in the raw log and its content lags out
                        // of the context mechanically at the next
                        // digest.
                        let b_old = session.last_digest_boundary_msg_id;
                        let b_new = outcome.new_boundary();
                        context.remove_at_or_below(b_old);
                        // Prune the dedup set (specs.md Section 10.2
                        // step 4) at the same cutoff.
                        let prune_chat_id = chat_id.clone();
                        let deleted = blocking_store(&store, move |store| {
                            store.delete_injected_memories_up_to(&prune_chat_id, b_old)
                        })
                        .await?;
                        debug!(chat_id = %chat_id, deleted, "injected_memories pruned");
                        // The session boundaries. Read `last_digest_at`
                        // BEFORE it is overwritten below: the FIRST
                        // completed digest has no previous chunk, so
                        // prev stays None; from the second digest on,
                        // prev is the boundary that was current before
                        // this digest.
                        session.prev_digest_boundary_msg_id = if session.last_digest_at.is_some() {
                            Some(b_old)
                        } else {
                            None
                        };
                        session.last_digest_boundary_msg_id = b_new;
                        // The wall-clock completion time: the timeout
                        // fallback of Section 8.2 measures real time
                        // since the last digest.
                        session.last_digest_at = Some(OffsetDateTime::now_utc());
                        persist_session(&store, &chat_id, &session).await?;
                        if let Some(hook) = &post_digest_hook {
                            // The hook runs AFTER the built-in Rule C3
                            // removal and the dedup prune. It stays a
                            // seam for observers that need no actor
                            // state.
                            hook.after_digest(&chat_id, &outcome).await;
                        }
                        // Re-evaluate once: the tail can still exceed the
                        // thresholds (it grew during a long extraction).
                        maybe_launch_digest(
                            &store,
                            &chat_id,
                            &config,
                            &session,
                            digest.as_ref(),
                            &mut digest_in_flight,
                            &inbox_sender,
                            OffsetDateTime::now_utc(),
                        )
                        .await?;
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
    match outcome {
        InsertOutcome::Inserted(id) => {
            // Rule C1: the log row exists first (Rule P1), then the
            // materialized view gets the same item.
            context.append_human_message(id, &msg.sender_display_name, msg.timestamp, &msg.text);
        }
        InsertOutcome::Duplicate => {
            // Idempotent intake (AGENT.md Section 6.2): a replay after a
            // crash can re-see a message. The row exists already; the
            // counters below still update, so the wake counter can count
            // one delivery twice. The log row — the source of truth — is
            // not duplicated, and the view must not duplicate either.
            debug!(chat_id = %chat_id, platform_msg_id = %msg.platform_msg_id, "duplicate delivery");
        }
    }

    // Update the session in memory.
    session.record_human_message();
    wake.record_message();
    session.wake = wake.snapshot();
    persist_session(store, chat_id, session).await?;

    // Trigger evaluation. The wake procedure is a stub (M4); only the
    // scheduling runs here. `msg.timestamp` is `now`: deterministic
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
        inbox_sender,
        now,
    )
    .await?;
    if msg.mentions_bot || msg.is_reply_to_bot {
        // specs.md Section 8.1: the bot must respond when addressed
        // directly. The muted state does not suppress a forced wake.
        info!(chat_id = %chat_id, "forced wake requested (stub)");
    } else if wake.should_fire(now, config) {
        info!(chat_id = %chat_id, "wake trigger fired (stub)");
        reset_wake(config, session, wake, rng, now);
        persist_session(store, chat_id, session).await?;
    }
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

    use crate::context::{ContextItemKind, ContextRole, RangeTag};

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
            text: format!("text of {id}"),
            reply_to_platform_msg_id: None,
            mentions_bot,
            is_reply_to_bot: false,
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
        assert_eq!(first.content, "[Alice 22:13] text of m1");
        assert_eq!(first.range_tag, Some(RangeTag::single(1)));

        let second = &items[2];
        assert_eq!(second.kind, ContextItemKind::HumanMessage);
        assert_eq!(second.role, ContextRole::User);
        assert_eq!(second.content, "[Alice 22:13] text of m2");
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
        assert_eq!(items[1].content, "[Alice 22:13] text of m1");
        assert_eq!(items[1].range_tag, Some(RangeTag::single(1)));
        assert_eq!(items[2].content, "[Alice 22:13] edited text");
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

        // The hand-computed rebuild over the same persisted rows.
        let (rows, injections) = blocking_store_call(&fixture.store, move |store| {
            let rows = store.list_messages_after(CHAT_ID, 0)?;
            let injections = store.list_injected_memories(CHAT_ID)?;
            Ok((rows, injections))
        })
        .await;
        let expected = LiveContext::rebuild(TEST_PREAMBLE.to_string(), &rows, &injections);
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
        assert_eq!(items[1].content, "[Alice 22:13] text of m3");
        assert_eq!(items[2].range_tag, Some(RangeTag::single(4)));
        assert_eq!(items[2].content, "[Alice 22:13] text of m4");
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
}
