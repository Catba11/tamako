//! The live-mode persona hot reload (decision 80, amending specs.md
//! Sections 5.3 / 6.1 rule 1 / 7.2 C4): a `notify` watcher on
//! `{data_root}/persona.toml` re-renders the preamble through the SAME
//! path as startup (`load_persona` + `PetPreambleRenderer`) and
//! broadcasts it to every live actor as `ActorCommand::ReloadPreamble`.
//! Live-only: replay never spawns the watcher (Rule P1 replay
//! determinism). The gate's disjoint preamble does NOT reload.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher as _};
use tamako_core::actor::{ActorCommand, GroupActorHandle};
use tamako_persona::{load_persona, PetPreambleRenderer, PreambleRenderer as _, SuffixMode};
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// The quiet period of the reload debounce (decision 80): editors write
/// in bursts (write-temp + rename), so one save is many events. The file
/// is evaluated ONCE after 500 ms of event silence.
const DEBOUNCE_QUIET_PERIOD: Duration = Duration::from_millis(500);

/// The payload of one applied persona reload: the new persona name (the
/// curated reload line's `persona` field) and the freshly rendered
/// preamble bytes.
pub struct ReloadNotice {
    /// The persona name of the reloaded file.
    pub persona_name: String,
    /// Whether the rendered preamble bytes changed (decision 94 (b)). A
    /// suffix-only reload rewrites the suffix slot WITHOUT the actor
    /// broadcast: the suffix never enters the preamble (decision
    /// 86 (d)), so no actor context changes and no cache anchor moves.
    pub preamble_changed: bool,
    /// The rendered preamble, bit-identical to what a startup render of
    /// the same file would produce (Rule C4).
    pub preamble: String,
    /// The rendered decision-86 reply suffix body (the `<system>` string
    /// of `tamako_persona::render_suffix`), rewritten on each reload.
    pub suffix: String,
    /// The parsed suffix rule count (decision 94 (d)): it rides the
    /// reload INFO lines, so a silently empty suffix is one log read
    /// away.
    pub suffix_rules: usize,
}

/// The outcome of reading and rendering the persona file once.
pub enum PersonaFileOutcome {
    /// The file parsed and its rendered preamble OR suffix differs from
    /// the current ones: apply it (decision 94 (b) — the suffix never
    /// enters the preamble, so a preamble-only gate silently discarded
    /// every suffix-only edit until restart).
    Applied(ReloadNotice),
    /// Both rendered artifacts are byte-identical to the current ones:
    /// no reload, no INFO (DEBUG fine — decision 53).
    Identical,
    /// The file is missing, unreadable, or malformed: keep the CURRENT
    /// preamble, one WARN, the watcher keeps running. A temporarily
    /// missing file mid-editor-save lands here too and costs one WARN.
    Invalid(String),
}

/// The strict reload evaluation (decision 80): NO fallback chain on
/// reload — the file at `path` either parses and renders or the current
/// preamble stays. Pure of I/O beyond the one file read; the testable
/// core of the watcher.
pub fn evaluate_persona_file(
    path: &Path,
    current_preamble: &str,
    current_suffix: &str,
    suffix_mode: SuffixMode,
) -> PersonaFileOutcome {
    let persona = match load_persona(path) {
        Ok(persona) => persona,
        Err(error) => return PersonaFileOutcome::Invalid(error.to_string()),
    };
    let preamble = PetPreambleRenderer.render_preamble_for_mode(&persona, suffix_mode);
    let suffix = tamako_persona::render_suffix(&persona.suffix);
    // Decision 94 (b): the identity gate compares BOTH artifacts — the
    // suffix never enters the preamble (decision 86 (d)), so comparing
    // preambles alone silently discarded every suffix-only edit.
    if preamble == current_preamble && suffix == current_suffix {
        return PersonaFileOutcome::Identical;
    }
    let suffix_rules = persona.suffix.len();
    PersonaFileOutcome::Applied(ReloadNotice {
        preamble_changed: preamble != current_preamble,
        persona_name: persona.name,
        preamble,
        suffix,
        suffix_rules,
    })
}

/// The persona-file matcher of the watcher: the watched path and, when
/// the data root canonicalizes, the canonical path. Both are needed —
/// macOS FSEvents reports REAL paths (`/private/var/...`) while the
/// configured data root may run through a symlink (`/var/...`); Linux
/// inotify reports paths as watched. Exact full-path equality (never a
/// file-name check) keeps group-directory files out of the match.
struct PersonaPathMatcher {
    watched: PathBuf,
    canonical: Option<PathBuf>,
}

impl PersonaPathMatcher {
    /// Builds the matcher for `{data_root}/persona.toml`.
    fn new(data_root: &Path) -> Self {
        let watched = data_root.join("persona.toml");
        let canonical = data_root
            .canonicalize()
            .ok()
            .map(|root| root.join("persona.toml"))
            .filter(|path| path != &watched);
        PersonaPathMatcher { watched, canonical }
    }

    /// Whether the event touches the persona file (create, write, or the
    /// rename target of an atomic editor save).
    fn touches(&self, event: &Event) -> bool {
        event
            .paths
            .iter()
            .any(|path| path == &self.watched || self.canonical.as_ref() == Some(path))
    }
}

/// Drains the channel after a first item until `quiet` passes with no
/// new item (the decision-80 debounce): a burst of N events yields ONE
/// evaluation. `first` is part of the returned burst. EVERY item —
/// persona-touching or not — restarts the quiet clock: the temp-file
/// writes of an atomic save belong to the same burst as the rename that
/// lands on persona.toml. `None` means the channel closed (the watcher
/// is gone — shutdown).
async fn drain_until_quiet<T>(
    rx: &mut mpsc::UnboundedReceiver<T>,
    first: T,
    quiet: Duration,
) -> Option<Vec<T>> {
    let mut burst = vec![first];
    loop {
        match tokio::time::timeout(quiet, rx.recv()).await {
            Ok(Some(item)) => burst.push(item),
            // Quiet: evaluate once.
            Err(_elapsed) => return Some(burst),
            // The sender is gone: the watcher was dropped (shutdown).
            Ok(None) => return None,
        }
    }
}

/// Spawns the debounced persona watcher of the live run (decision 80).
///
/// Returns the `notify` watcher, which the caller MUST bind for the
/// run's lifetime — dropping it stops the watch (run_live binds it to
/// `_persona_watcher`). `None` means creation or the watch call failed:
/// one WARN and the live run continues without hot reload (best-effort
/// feature; startup already succeeded with the current file).
///
/// The watch targets the data-root DIRECTORY, non-recursive, and events
/// are filtered to the persona.toml path: editors commonly save via
/// write-temp + rename, which REPLACES the inode — a direct file watch
/// silently dies on the first such save, while the directory watch sees
/// every save shape (create/write/rename).
pub fn spawn_persona_watcher(
    data_root: PathBuf,
    current_preamble: String,
    current_suffix: String,
    suffix_mode: SuffixMode,
    reload_tx: mpsc::Sender<ReloadNotice>,
) -> Option<RecommendedWatcher> {
    let persona_path = data_root.join("persona.toml");
    let matcher = PersonaPathMatcher::new(&data_root);
    // notify's event callback is synchronous: it pushes events into an
    // unbounded tokio channel (unbounded send never blocks) and the
    // spawned task below drains and debounces them.
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event>();
    let mut watcher = match notify::recommended_watcher(move |result: notify::Result<Event>| {
        // A notify error (event overflow) is not fatal: the next burst
        // re-triggers the evaluation. Dropping it keeps the callback
        // infallible.
        if let Ok(event) = result {
            let _ = event_tx.send(event);
        }
    }) {
        Ok(watcher) => watcher,
        Err(error) => {
            warn!(%error, "the persona watcher failed to start; persona hot reload is disabled for this run");
            return None;
        }
    };
    if let Err(error) = watcher.watch(&data_root, RecursiveMode::NonRecursive) {
        warn!(%error, "the persona watcher failed to watch the data root; persona hot reload is disabled for this run");
        return None;
    }
    tokio::spawn(async move {
        let mut current_preamble = current_preamble;
        let mut current_suffix = current_suffix;
        // The first event blocks (no timer runs while the file is
        // untouched); a persona-touching event opens a burst that the
        // debounce drains to ONE evaluation.
        while let Some(first) = event_rx.recv().await {
            if !matcher.touches(&first) {
                continue;
            }
            if drain_until_quiet(&mut event_rx, first, DEBOUNCE_QUIET_PERIOD)
                .await
                .is_none()
            {
                // The channel closed mid-burst: the watcher is gone.
                return;
            }
            match evaluate_persona_file(
                &persona_path,
                &current_preamble,
                &current_suffix,
                suffix_mode,
            ) {
                // specs.md Section 5.3 strictness on reload: a malformed
                // or unreadable file keeps the CURRENT preamble; the
                // watcher keeps running.
                PersonaFileOutcome::Invalid(error) => {
                    warn!(%error, "persona file malformed or unreadable; keeping the current preamble");
                }
                PersonaFileOutcome::Identical => {
                    debug!("persona file saved with identical rendered bytes; no reload");
                }
                PersonaFileOutcome::Applied(notice) => {
                    current_preamble = notice.preamble.clone();
                    current_suffix = notice.suffix.clone();
                    // A send failure means the live loop is gone: the
                    // watcher's job is done (shutdown).
                    if reload_tx.send(notice).await.is_err() {
                        debug!("the live loop is gone; the persona watcher stops");
                        return;
                    }
                }
            }
        }
    });
    Some(watcher)
}

/// Broadcasts one applied reload to every live actor (decision 80,
/// specs.md Section 6.1 rule 1): the command serializes through each
/// FIFO inbox like every other command. Best-effort and sync —
/// `try_send` never blocks on a backlogged actor: a dead or full-inbox
/// actor is SKIPPED with one WARN (the file is the state; its next start
/// reads it) and its chat id joins the returned list (sorted, for
/// deterministic tests).
pub fn broadcast_preamble(
    actors: &HashMap<String, GroupActorHandle>,
    preamble: &str,
) -> Vec<String> {
    let mut skipped = Vec::new();
    for (chat_id, handle) in actors {
        if handle
            .try_send(ActorCommand::ReloadPreamble(preamble.to_string()))
            .is_err()
        {
            warn!(chat_id = %chat_id, "persona reload skipped: the actor is dead or its inbox is full; its next start reads the file");
            skipped.push(chat_id.clone());
        }
    }
    skipped.sort();
    skipped
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tamako_core::actor::{spawn_group_actor, GroupActorParams, DEFAULT_INBOX_CAPACITY};
    use tamako_core::config::TriggerConfig;
    use tamako_memory::LbugBackend;
    use tamako_store::Store;
    use time::OffsetDateTime;

    use super::*;

    /// A minimal valid persona file (name and identity are the required
    /// keys) with the given display name.
    fn valid_persona(name: &str) -> String {
        format!("name = \"{name}\"\nidentity = \"a test pet\"\n")
    }

    /// A minimal valid persona file carrying ONE suffix rule.
    fn valid_persona_with_suffix(name: &str, rule: &str) -> String {
        format!("name = \"{name}\"\nidentity = \"a test pet\"\nsuffix = [\n    \"{rule}\",\n]\n")
    }

    /// Writes `contents` to `{dir}/persona.toml` and returns the path.
    fn write_persona(dir: &Path, contents: &str) -> PathBuf {
        let path = dir.join("persona.toml");
        std::fs::write(&path, contents).expect("the persona file writes");
        path
    }

    #[test]
    fn evaluate_persona_file_applies_a_valid_change() {
        let dir = tempfile::tempdir().expect("a temporary data root");
        let path = write_persona(dir.path(), &valid_persona("Alpha"));
        let expected =
            PetPreambleRenderer.render_preamble(&load_persona(&path).expect("the file parses"));
        let outcome = evaluate_persona_file(&path, "the stale preamble", "", SuffixMode::System);
        let PersonaFileOutcome::Applied(notice) = outcome else {
            panic!("a changed valid file applies");
        };
        assert_eq!(notice.persona_name, "Alpha");
        // Rule C4: the reload renders the SAME bytes a startup render of
        // the file would.
        assert_eq!(notice.preamble, expected);
        assert!(notice.preamble_changed, "a preamble change sets the flag");
    }

    #[test]
    fn evaluate_persona_file_applies_a_suffix_only_change_without_the_broadcast_flag() {
        // Decision 94 (b): an edit touching ONLY the suffix applies —
        // pre-94 the preamble-only identity gate discarded it as
        // Identical.
        let dir = tempfile::tempdir().expect("a temporary data root");
        let path = write_persona(
            dir.path(),
            &valid_persona_with_suffix("Alpha", "keep it short"),
        );
        let persona = load_persona(&path).expect("the file parses");
        let current_preamble = PetPreambleRenderer.render_preamble(&persona);
        let outcome = evaluate_persona_file(&path, &current_preamble, "", SuffixMode::System);
        let PersonaFileOutcome::Applied(notice) = outcome else {
            panic!("a suffix-only change applies");
        };
        assert!(
            !notice.preamble_changed,
            "the preamble did not change: no actor broadcast follows"
        );
        assert_eq!(
            notice.suffix,
            tamako_persona::render_suffix(&persona.suffix)
        );
        assert_eq!(notice.suffix_rules, 1);
    }

    #[test]
    fn evaluate_persona_file_reports_an_identical_suffix_only_save() {
        let dir = tempfile::tempdir().expect("a temporary data root");
        let path = write_persona(
            dir.path(),
            &valid_persona_with_suffix("Alpha", "keep it short"),
        );
        let persona = load_persona(&path).expect("the file parses");
        let current_preamble = PetPreambleRenderer.render_preamble(&persona);
        let current_suffix = tamako_persona::render_suffix(&persona.suffix);
        let outcome = evaluate_persona_file(
            &path,
            &current_preamble,
            &current_suffix,
            SuffixMode::System,
        );
        assert!(
            matches!(outcome, PersonaFileOutcome::Identical),
            "a byte-identical render pair is no reload"
        );
    }

    #[test]
    fn evaluate_persona_file_reports_identical_bytes() {
        let dir = tempfile::tempdir().expect("a temporary data root");
        let path = write_persona(dir.path(), &valid_persona("Alpha"));
        let current =
            PetPreambleRenderer.render_preamble(&load_persona(&path).expect("the file parses"));
        let outcome = evaluate_persona_file(&path, &current, "", SuffixMode::System);
        assert!(
            matches!(outcome, PersonaFileOutcome::Identical),
            "a byte-identical render is no reload"
        );
    }

    #[test]
    fn evaluate_persona_file_rejects_malformed_toml() {
        let dir = tempfile::tempdir().expect("a temporary data root");
        let path = write_persona(dir.path(), "not toml [");
        let outcome = evaluate_persona_file(&path, "the current preamble", "", SuffixMode::System);
        let PersonaFileOutcome::Invalid(error) = outcome else {
            panic!("malformed TOML is invalid");
        };
        assert!(
            error.contains("persona configuration"),
            "the message names the file problem: {error}"
        );
    }

    #[test]
    fn evaluate_persona_file_rejects_a_missing_file() {
        let dir = tempfile::tempdir().expect("a temporary data root");
        // Mid-rename save tolerance: the file can be gone at evaluation.
        let path = dir.path().join("persona.toml");
        let outcome = evaluate_persona_file(&path, "the current preamble", "", SuffixMode::System);
        assert!(
            matches!(outcome, PersonaFileOutcome::Invalid(_)),
            "a missing file is invalid, not a crash"
        );
    }

    #[tokio::test]
    async fn drain_until_quiet_collects_a_burst_into_one_call() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(1).expect("the send succeeds");
        let first = rx.recv().await.expect("the first item arrives");
        // More items keep arriving inside the quiet window. A second
        // sender moves into the pusher; the original stays alive in this
        // scope, so the channel cannot close mid-burst (in production the
        // sender lives in the notify callback for the watcher's
        // lifetime).
        let pusher_tx = tx.clone();
        let pusher = tokio::spawn(async move {
            for item in 2..=5 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                pusher_tx.send(item).expect("the send succeeds");
            }
        });
        let burst = drain_until_quiet(&mut rx, first, Duration::from_millis(100))
            .await
            .expect("the burst completes");
        pusher.await.expect("the pusher joins");
        assert_eq!(burst, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn drain_until_quiet_returns_none_when_the_channel_closes() {
        let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
        tx.send(1).expect("the send succeeds");
        drop(tx);
        let first = rx.recv().await.expect("the first item arrives");
        // A generous quiet period must not be waited out: the closed
        // channel answers immediately.
        assert!(drain_until_quiet(&mut rx, first, Duration::from_secs(60))
            .await
            .is_none());
    }

    /// Spawns a real actor over a temporary data root (the
    /// warmup_integration.rs harness pattern) for the broadcast tests.
    fn spawn_test_actor(dir: &Path, chat_id: &str) -> GroupActorHandle {
        spawn_group_actor(GroupActorParams {
            chat_id: chat_id.to_string(),
            store: Arc::new(Store::new(dir.to_path_buf())),
            memory: Arc::new(LbugBackend::new(dir.to_path_buf())),
            config: TriggerConfig::default(),
            started_at: OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("a valid unix timestamp"),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            preamble: "old preamble".to_string(),
            digest: None,
            post_digest_hook: None,
            wake: None,
            warmup: None,
            summary_provider: None,
            outbound: None,
            bot_name: None,
        })
    }

    #[tokio::test]
    async fn broadcast_preamble_reaches_a_live_actor() {
        let dir = tempfile::tempdir().expect("a temporary data root");
        let handle = spawn_test_actor(dir.path(), "chat-live");
        let mut actors = HashMap::new();
        actors.insert("chat-live".to_string(), handle);
        let skipped = broadcast_preamble(&actors, "new preamble");
        assert!(skipped.is_empty(), "a live actor is not skipped");
        let handle = actors.remove("chat-live").expect("the handle is there");
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn broadcast_preamble_skips_a_dead_actor() {
        let dir = tempfile::tempdir().expect("a temporary data root");
        let handle = spawn_test_actor(dir.path(), "chat-dead");
        handle
            .send(ActorCommand::Shutdown)
            .await
            .expect("the send succeeds");
        // The join handle is private outside tamako-core (ST-1's test
        // awaits it in-crate): poll `try_send` until the dead task has
        // dropped the inbox receiver. Bounded and deterministic — the
        // Shutdown command is already in the FIFO.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while handle
            .try_send(ActorCommand::ReloadPreamble("probe".to_string()))
            .is_ok()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the actor inbox closes within 5s of Shutdown"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut actors = HashMap::new();
        actors.insert("chat-dead".to_string(), handle);
        let skipped = broadcast_preamble(&actors, "new preamble");
        assert_eq!(skipped, vec!["chat-dead".to_string()]);
    }

    /// The end-to-end filesystem proof (decision 80): a real watcher over
    /// a real directory, a burst save, a bad write, a recovery write.
    /// Ignored by default (real filesystem events and real wall-clock
    /// debounce); run explicitly with
    /// `cargo test -p tamako persona_watch -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "filesystem watcher integration; run explicitly with --ignored"]
    async fn the_watcher_applies_one_reload_per_save_burst_and_survives_a_bad_write() {
        let dir = tempfile::tempdir().expect("a temporary data root");
        let persona_path = write_persona(dir.path(), &valid_persona("Alpha"));
        let current = PetPreambleRenderer
            .render_preamble(&load_persona(&persona_path).expect("the file parses"));
        let (reload_tx, mut reload_rx) = mpsc::channel(4);
        let _watcher = spawn_persona_watcher(
            dir.path().to_path_buf(),
            current,
            String::new(),
            SuffixMode::System,
            reload_tx,
        )
        .expect("the watcher spawns over the temporary directory");

        // A save burst: five rapid rewrites — ONE notice (the debounce).
        for _ in 0..5 {
            std::fs::write(&persona_path, valid_persona("Beta")).expect("the write succeeds");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let notice = tokio::time::timeout(Duration::from_secs(10), reload_rx.recv())
            .await
            .expect("one notice arrives within the timeout")
            .expect("the channel stays open");
        assert_eq!(notice.persona_name, "Beta");
        assert_eq!(
            notice.preamble,
            PetPreambleRenderer
                .render_preamble(&load_persona(&persona_path).expect("the file parses"))
        );
        // No second notice falls out of the same burst.
        assert!(
            tokio::time::timeout(Duration::from_secs(2), reload_rx.recv())
                .await
                .is_err(),
            "the burst produced exactly one notice"
        );

        // An invalid write: the WARN path — no notice, no death.
        std::fs::write(&persona_path, "not toml [").expect("the write succeeds");
        assert!(
            tokio::time::timeout(Duration::from_secs(2), reload_rx.recv())
                .await
                .is_err(),
            "a malformed file reloads nothing"
        );

        // The watcher survived: a final valid write still reloads.
        std::fs::write(&persona_path, valid_persona("Gamma")).expect("the write succeeds");
        let notice = tokio::time::timeout(Duration::from_secs(10), reload_rx.recv())
            .await
            .expect("one notice arrives after the bad write")
            .expect("the channel stays open");
        assert_eq!(notice.persona_name, "Gamma");
    }
}
