//! The merge-tool orchestration (current-state.md decision 74,
//! proposed-graph-database-specs.md Section 7.7): scan candidate pairs
//! from the vector index, confirm each pair once with a three-way
//! verdict, apply the confirmed plan, and roll a merge back from its
//! audit snapshot.
//!
//! The tool runs OFFLINE (the bot is stopped; decision 47: an external
//! process cannot share the per-group lbug mutex), one group per run.
//! This module is the library-level core so the whole flow is testable
//! without the CLI; the binary wires the modes (`--merge-tool` dry-run
//! by default, `--apply`, `--merge-rollback`) onto these functions.
//!
//! The confirmer seam mirrors the `EmbeddingProvider` bridge pattern of
//! [`crate::embedding`]: tamako-core cannot depend on tamako-agent
//! (AGENT.md Section 4: no dependency cycles), so the contract lives
//! here and the binary adapts `tamako_agent::EndpointMergeConfirmer`
//! onto it.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tamako_memory::{EdgeId, MemoryBackend, MemoryError, MergeOutcome, NodeMergeStats, NodeType};
use tamako_store::{MergeAuditRow, Store, StoreError};
use time::OffsetDateTime;
use tracing::{error, warn};

/// The per-node KNN width of the candidate scan (decision 74 / Section
/// 7.7 step 1). Five nearest neighbors per embedded node is generous at
/// the fragmentation scale (a fragmented entity splits into a handful
/// of nodes) and keeps the scan O(n·k).
pub const MERGE_SCAN_KNN_K: usize = 5;

/// Errors of the merge tool. Row-level failures INSIDE
/// [`plan_merges`] and [`apply_merge_plan`] are logged at WARN and
/// recorded in the plan/report instead of propagated; this type is the
/// carrier between the store/memory/confirmer seams and those handlers,
/// and the error channel of [`rollback_merge_action`].
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    /// The confirmer call failed (transport, endpoint, or a malformed
    /// verdict at the seam).
    #[error("merge confirmer failed: {0}")]
    Confirmer(String),
    /// A store call of the embedding sidecar / audit table failed.
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    /// A graph backend call failed.
    #[error("memory error: {0}")]
    Memory(#[from] MemoryError),
    /// A blocking store task failed to join.
    #[error("blocking store task failed to join: {0}")]
    Join(String),
    /// A rollback was refused (missing/non-merge/rolled-back/snapshotless
    /// audit row). Rollback refusals are LOUD by design (graph-spec
    /// Section 7.7 steps 4/5).
    #[error("merge rollback refused: {0}")]
    Rollback(String),
}

/// The three-way verdict of one candidate-pair confirmation (decision
/// 74 point 1): `same` merges, `related` links the pair with an
/// `also_known_as` edge, `different` skips. The wire strings are
/// `same`/`related`/`different` (the LLM confirmation schema and the
/// `merge_audit.verdict` CHECK constraint share them).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeVerdict {
    Same,
    Related,
    Different,
}

impl MergeVerdict {
    /// The wire string of the confirmation schema and the audit table.
    pub fn as_str(self) -> &'static str {
        match self {
            MergeVerdict::Same => "same",
            MergeVerdict::Related => "related",
            MergeVerdict::Different => "different",
        }
    }

    /// Parses a wire string back. Returns `None` for an unknown string;
    /// read paths skip, they do not fail.
    // Not std::str::FromStr: a closed-set lookup that returns Option
    // reads better at the call sites (same idiom as NodeType).
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "same" => Some(MergeVerdict::Same),
            "related" => Some(MergeVerdict::Related),
            "different" => Some(MergeVerdict::Different),
            _ => None,
        }
    }
}

/// The per-endpoint node summary handed to the confirmer: the display
/// name, the kind (kind-compatible pairs only, Section 7.7 step 1), and
/// the stored description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeNodeInfo {
    pub name: String,
    pub kind: NodeType,
    pub description: String,
}

/// The confirmer's answer for one pair: the verdict plus the LLM's or
/// operator's justification (both land in the audit row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeConfirmation {
    pub verdict: MergeVerdict,
    pub reason: String,
}

/// The merge-confirmation seam (decision 74). tamako-core cannot depend
/// on tamako-agent, so the contract lives here and the binary adapts
/// `tamako_agent::EndpointMergeConfirmer` onto it. Object-safe (the
/// `Pin<Box>` convention of `crate::embedding::EmbeddingProvider`).
pub trait MergeConfirmer: Send + Sync {
    /// Confirms one candidate pair once. `a` and `b` are the pair
    /// endpoints in the candidate's deterministic (a_id < b_id) order.
    fn confirm_merge<'a>(
        &'a self,
        a: &'a MergeNodeInfo,
        b: &'a MergeNodeInfo,
    ) -> Pin<Box<dyn Future<Output = Result<MergeConfirmation, MergeError>> + Send + 'a>>;
}

/// One candidate pair of the scan (Section 7.7 step 1): two embedded
/// nodes of the SAME kind (Person or Concept only — Alias fragmentation
/// does not exist by construction and MessageBatch skeletons are never
/// embedded) whose stored vectors sit at cosine similarity `score` at
/// or above the threshold, not already linked by
/// `known_as`/`also_known_as`. The pair is unordered; `a_id < b_id`
/// holds, so the display order is deterministic.
///
/// The descriptions ride along (beyond the minimal field set of the
/// subtask spec) because both the confirmer input and the audit row's
/// `loser_description` need them, and re-reading them at apply time
/// would race the merge itself.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeCandidate {
    pub a_id: String,
    pub b_id: String,
    pub a_name: String,
    pub b_name: String,
    pub a_description: String,
    pub b_description: String,
    /// The shared kind of the pair (Person or Concept).
    pub kind: NodeType,
    /// The cosine similarity of the stored vectors, in [threshold, 1].
    pub score: f64,
}

/// One confirmed action of the plan.
#[derive(Debug, Clone, PartialEq)]
pub struct MergePlanAction {
    pub candidate: MergeCandidate,
    pub verdict: MergeVerdict,
    /// The confirmer's justification; lands in the audit row.
    pub reason: String,
    /// The survivor of the pair per the rule documented on
    /// [`MergePlan`]. Computed for EVERY verdict: the audit row of a
    /// non-merge verdict records the candidate that WOULD have been
    /// merged away (specs.md Section 5.2).
    pub survivor_id: String,
    pub loser_id: String,
}

/// Why a scanned candidate is not an action of the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// The pair sits beyond the `max_confirmations` budget: it was NOT
    /// confirmed (no confirmer call), only reported.
    OverConfirmationBudget,
    /// The confirmer call failed; the pair is never merged.
    ConfirmationFailed(String),
}

/// The outcome of [`plan_merges`]: the confirmed actions plus the
/// skipped candidates with their reasons. Planning writes NOTHING —
/// the tool is dry-run by default (decision 74 point 3); only
/// [`apply_merge_plan`] mutates the graph and the store.
///
/// Survivor-choice rule (graph-spec Section 7.7 step 3, decision 74):
/// the node with the HIGHER edge degree survives; a tie goes to the
/// older `created_at`; a remaining tie (equal degree AND timestamp) or
/// a missing stats row (the node vanished between scan and plan — it
/// cannot be merged anyway) falls back to the lexicographically
/// smaller id, deterministic. The manual `--merge` form overrides this
/// choice at the CLI layer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MergePlan {
    pub actions: Vec<MergePlanAction>,
    pub skipped: Vec<(MergeCandidate, SkipReason)>,
}

/// One failed action of [`apply_merge_plan`]. Apply is best-effort per
/// action: a failure logs WARN, lands here, and the batch continues —
/// it never aborts mid-way silently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyFailure {
    pub loser_id: String,
    pub survivor_id: String,
    pub verdict: MergeVerdict,
    pub error: String,
}

/// The outcome of [`apply_merge_plan`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyReport {
    /// The audit row ids, one per action that got its row (insertion
    /// order).
    pub audit_ids: Vec<i64>,
    pub failures: Vec<ApplyFailure>,
}

/// Runs one synchronous store call off the async runtime (AGENT.md
/// Section 6.2). Same idiom as `crate::embedding`.
async fn store_call<T>(
    store: &Arc<Store>,
    call: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
) -> Result<T, MergeError>
where
    T: Send + 'static,
{
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || call(&store))
        .await
        .map_err(|error| MergeError::Join(error.to_string()))?
        .map_err(MergeError::from)
}

/// The candidate scan of the merge tool (Section 7.7 step 1): every
/// embedded Person/Concept node of the group is KNN-queried with its
/// OWN stored vector (k = [`MERGE_SCAN_KNN_K`]); pairs at or above
/// `threshold` cosine similarity survive, deduplicated unordered;
/// pairs already linked by `known_as`/`also_known_as` (either
/// direction) are excluded as legitimate surface-form links. The result
/// sorts by score descending, ties by (a_id, b_id) ascending, so the
/// plan's confirmation order is deterministic.
///
/// Alias and MessageBatch nodes are never candidate endpoints: Alias
/// fragmentation does not exist by construction (aliases are natural
/// keys) and MessageBatch skeletons are provenance, not entities. A
/// vec row whose graph node vanished (an orphan the next startup
/// reconciliation will tombstone, decision 66) is skipped.
pub async fn scan_merge_candidates<M: MemoryBackend>(
    store: &Arc<Store>,
    memory: &M,
    chat_id: &str,
    threshold: f64,
) -> Result<Vec<MergeCandidate>, MergeError> {
    // Vec-table-only ids: a queue-only id has no stored vector to KNN
    // with.
    let embedded = store_call(store, Store::embedded_node_ids).await?;
    if embedded.len() < 2 {
        return Ok(Vec::new());
    }
    let infos: HashMap<String, NodeType> = memory
        .node_resolution_infos(chat_id, &embedded)
        .await?
        .into_iter()
        .map(|(id, info)| (id, info.kind))
        .collect();
    let contents: HashMap<String, tamako_memory::NodeContent> = memory
        .list_node_contents(chat_id)
        .await?
        .into_iter()
        .collect();

    // The candidate endpoints: embedded Person/Concept nodes with their
    // stored vector, name, and description.
    struct Endpoint {
        kind: NodeType,
        name: String,
        description: String,
        vector: Vec<f32>,
    }
    let mut endpoints: Vec<(String, Endpoint)> = Vec::new();
    for node_id in &embedded {
        let Some(&kind) = infos.get(node_id.as_str()) else {
            continue; // Orphan vec row: the graph no longer holds it.
        };
        if !matches!(kind, NodeType::Person | NodeType::Concept) {
            continue;
        }
        let Some(content) = contents.get(node_id.as_str()) else {
            continue; // Orphan vec row (same case, listing side).
        };
        let id = node_id.clone();
        let vector = store_call(store, move |store| store.node_embedding(&id)).await?;
        let Some(vector) = vector else {
            // embedded_node_ids and node_embedding race nothing in the
            // offline tool; a missing row here means a store
            // inconsistency worth a WARN, not a failure.
            warn!(chat_id, node_id = %node_id, "merge scan: vec row vanished between listing and read; node skipped");
            continue;
        };
        endpoints.push((
            node_id.clone(),
            Endpoint {
                kind,
                name: content.name.clone(),
                description: content.description.clone(),
                vector,
            },
        ));
    }

    // Pairwise collection over the per-node KNNs. The map key is the
    // ordered (min, max) id pair; the score keeps the max of the two
    // directions (symmetric in theory, float-jittered in practice).
    let mut pairs: HashMap<(String, String), f64> = HashMap::new();
    for (node_id, endpoint) in &endpoints {
        let vector = endpoint.vector.clone();
        let hits = store_call(store, move |store| {
            store.knn_node_embeddings(&vector, MERGE_SCAN_KNN_K)
        })
        .await?;
        for (other_id, distance) in hits {
            if other_id == *node_id {
                continue;
            }
            let similarity = 1.0 - f64::from(distance);
            if similarity < threshold {
                continue;
            }
            let Some(other) = endpoints
                .iter()
                .find(|(id, _)| *id == other_id)
                .map(|(_, endpoint)| endpoint)
            else {
                continue; // Not a candidate endpoint (kind or orphan).
            };
            if other.kind != endpoint.kind {
                continue; // Kind-compatible pairs only (Section 7.7).
            }
            let key = if *node_id < other_id {
                (node_id.clone(), other_id)
            } else {
                (other_id, node_id.clone())
            };
            pairs
                .entry(key)
                .and_modify(|score| *score = score.max(similarity))
                .or_insert(similarity);
        }
    }

    // Exclude already-linked pairs and materialize the candidates.
    let mut candidates = Vec::new();
    for ((a_id, b_id), score) in pairs {
        if memory.are_linked(chat_id, &a_id, &b_id).await? {
            continue;
        }
        let endpoint_of = |id: &str| {
            endpoints
                .iter()
                .find(|(endpoint_id, _)| endpoint_id == id)
                .map(|(_, endpoint)| endpoint)
                .expect("pair endpoints come from the endpoint set")
        };
        let a = endpoint_of(&a_id);
        let b = endpoint_of(&b_id);
        candidates.push(MergeCandidate {
            a_id,
            b_id,
            a_name: a.name.clone(),
            b_name: b.name.clone(),
            a_description: a.description.clone(),
            b_description: b.description.clone(),
            kind: a.kind,
            score,
        });
    }
    candidates.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.a_id.cmp(&b.a_id))
            .then_with(|| a.b_id.cmp(&b.b_id))
    });
    Ok(candidates)
}

/// The survivor choice of graph-spec Section 7.7 step 3 (documented on
/// [`MergePlan`]): higher edge degree wins, a tie goes to the older
/// `created_at`, then the lexicographically smaller id. Returns
/// (survivor_id, loser_id).
fn choose_survivor(
    a_id: &str,
    b_id: &str,
    stats: &HashMap<String, NodeMergeStats>,
) -> (String, String) {
    let a_wins = match (stats.get(a_id), stats.get(b_id)) {
        (Some(a), Some(b)) => {
            a.edge_degree
                .cmp(&b.edge_degree)
                // Older created_at wins: invert the comparison.
                .then_with(|| b.created_at.cmp(&a.created_at))
                // Deterministic final tiebreak: the smaller id wins.
                .then_with(|| b_id.cmp(a_id))
                == Ordering::Greater
        }
        // Deviation (documented on MergePlan): a node vanished between
        // scan and plan. The pair cannot merge anyway; the fallback
        // only decides which id lands in which audit column.
        _ => a_id < b_id,
    };
    if a_wins {
        (a_id.to_string(), b_id.to_string())
    } else {
        (b_id.to_string(), a_id.to_string())
    }
}

/// Scans the group and confirms each candidate pair once, in score
/// order, capped at `max_confirmations` confirmer calls — pairs beyond
/// the budget are marked [`SkipReason::OverConfirmationBudget`] without
/// a confirmer call. A failed confirmation skips the pair
/// ([`SkipReason::ConfirmationFailed`], WARN logged); the pair is never
/// merged.
///
/// Planning is the DRY RUN (decision 74 point 3): this function writes
/// nothing — no graph mutation, no audit row, no sidecar change.
pub async fn plan_merges<M: MemoryBackend>(
    store: &Arc<Store>,
    memory: &M,
    chat_id: &str,
    threshold: f64,
    confirmer: &dyn MergeConfirmer,
    max_confirmations: usize,
) -> Result<MergePlan, MergeError> {
    let candidates = scan_merge_candidates(store, memory, chat_id, threshold).await?;
    // One stats read covers the whole candidate set (Section 5.2 rule
    // 4: the id list is one LIST parameter).
    let ids: Vec<String> = {
        let mut ids: Vec<String> = candidates
            .iter()
            .flat_map(|candidate| [candidate.a_id.clone(), candidate.b_id.clone()])
            .collect();
        ids.sort();
        ids.dedup();
        ids
    };
    let stats: HashMap<String, NodeMergeStats> = memory
        .node_merge_stats(chat_id, &ids)
        .await?
        .into_iter()
        .collect();

    let mut plan = MergePlan::default();
    for (index, candidate) in candidates.into_iter().enumerate() {
        if index >= max_confirmations {
            plan.skipped
                .push((candidate, SkipReason::OverConfirmationBudget));
            continue;
        }
        let a = MergeNodeInfo {
            name: candidate.a_name.clone(),
            kind: candidate.kind,
            description: candidate.a_description.clone(),
        };
        let b = MergeNodeInfo {
            name: candidate.b_name.clone(),
            kind: candidate.kind,
            description: candidate.b_description.clone(),
        };
        match confirmer.confirm_merge(&a, &b).await {
            Ok(confirmation) => {
                let (survivor_id, loser_id) =
                    choose_survivor(&candidate.a_id, &candidate.b_id, &stats);
                plan.actions.push(MergePlanAction {
                    candidate,
                    verdict: confirmation.verdict,
                    reason: confirmation.reason,
                    survivor_id,
                    loser_id,
                });
            }
            Err(error) => {
                warn!(chat_id, a_id = %candidate.a_id, b_id = %candidate.b_id, %error, "merge plan: confirmation failed; pair skipped");
                plan.skipped
                    .push((candidate, SkipReason::ConfirmationFailed(error.to_string())));
            }
        }
    }
    Ok(plan)
}

/// Builds the audit row of one action (specs.md Section 5.2). All
/// three verdicts are audited as the record of the confirmation; only
/// a 'same' merge carries the snapshot and the edge counters. The
/// `id`/`rolled_back`/`created_at` fields are placeholders —
/// `insert_merge_audit` ignores them.
///
/// Decision 75 audit remarks: the v9 schema stays FROZEN — the
/// invariant-pass count rides the `reason` field instead of a new
/// column. When the merge invalidated single-value duplicates, the
/// confirmer's reason gets the note
/// `"; single-value invariant: invalidated N edge(s)"` appended.
fn audit_row(
    action: &MergePlanAction,
    confirmed_by: &str,
    outcome: Option<&MergeOutcome>,
) -> MergeAuditRow {
    let candidate = &action.candidate;
    let (loser_name, loser_description) = if action.loser_id == candidate.a_id {
        (&candidate.a_name, &candidate.a_description)
    } else {
        (&candidate.b_name, &candidate.b_description)
    };
    let mut reason = action.reason.clone();
    if let Some(outcome) = outcome {
        if outcome.single_value_invalidated > 0 {
            reason.push_str(&format!(
                "; single-value invariant: invalidated {} edge(s)",
                outcome.single_value_invalidated
            ));
        }
    }
    MergeAuditRow {
        id: 0,
        loser_id: action.loser_id.clone(),
        survivor_id: action.survivor_id.clone(),
        loser_kind: candidate.kind.as_str().to_string(),
        loser_name: loser_name.clone(),
        loser_description: if loser_description.is_empty() {
            None
        } else {
            Some(loser_description.clone())
        },
        verdict: action.verdict.as_str().to_string(),
        reason,
        confirmed_by: confirmed_by.to_string(),
        edges_moved: outcome.map_or(0, |outcome| outcome.edges_moved),
        self_loops_dropped: outcome.map_or(0, |outcome| outcome.self_loops_dropped),
        edges_deduped: outcome.map_or(0, |outcome| outcome.edges_deduped),
        snapshot: outcome.map(|outcome| outcome.snapshot_json.clone()),
        rolled_back: false,
        created_at: OffsetDateTime::now_utc(),
    }
}

/// Applies one plan action and returns the new audit id.
///
/// Decision 77 (H4), audit-row-first for the 'same' verdict — the
/// honest two-phase version (the snapshot only EXISTS after
/// `merge_nodes` builds it):
///
/// 1. PLANNED ROW FIRST: insert the audit row with the
///    verdict/reason/confirmed_by/loser fields filled and snapshot NULL
///    BEFORE the graph mutation. A crash mid-merge leaves a detectable
///    snapshot-NULL 'same' row instead of an unaudited mutation, and a
///    rigged merge failure (the survivor deleted between plan and
///    apply) still leaves the row.
/// 2. The graph mutation (`merge_nodes_with_registry`) builds the
///    snapshot.
/// 3. UPDATE the row by id with the actual snapshot, the final reason
///    (the decision-75 invariant note included), and the edge counters.
///    A crash between the graph commit and this update leaves the
///    detectable snapshot-NULL row of phase 1. If the UPDATE itself
///    fails, the row and the graph state survive: log ERROR with the
///    audit id (rollback of that merge is impossible — the snapshot is
///    lost — but the failure is loud and the audit trail names the id).
///
/// The sidecar tombstones stay best-effort (WARN, never an action
/// failure; the next startup reconciliation prunes the orphans).
/// 'related'/'different' rows stay a single insert as before: no
/// snapshot exists for them.
///
/// Decision 75 (c): the 'same' merge rides the resolved single-value
/// registry, so the invariant pass closes the re-point hole on the
/// survivor (the invariant holds globally, not just at the digest
/// path). An empty registry is the decision-74 behavior.
async fn apply_one<M: MemoryBackend>(
    store: &Arc<Store>,
    memory: &M,
    chat_id: &str,
    action: &MergePlanAction,
    confirmed_by: &str,
    single_value_predicates: &[String],
) -> Result<i64, MergeError> {
    match action.verdict {
        MergeVerdict::Same => {
            // Phase 1: the planned audit row BEFORE the graph mutation.
            let planned = audit_row(action, confirmed_by, None);
            let audit_id =
                store_call(store, move |store| store.insert_merge_audit(&planned)).await?;
            // Phase 2: the graph mutation builds the snapshot.
            let outcome = memory
                .merge_nodes_with_registry(
                    chat_id,
                    &action.loser_id,
                    &action.survivor_id,
                    single_value_predicates,
                )
                .await?;
            // Phase 3: fill the row in. The final reason carries the
            // decision-75 invariant note (audit_row appends it when the
            // outcome invalidated single-value duplicates).
            let final_reason = audit_row(action, confirmed_by, Some(&outcome)).reason;
            {
                let snapshot_json = outcome.snapshot_json.clone();
                let edges_moved = outcome.edges_moved;
                let self_loops_dropped = outcome.self_loops_dropped;
                let edges_deduped = outcome.edges_deduped;
                if let Err(error) = store_call(store, move |store| {
                    store.update_merge_audit_outcome(
                        audit_id,
                        &snapshot_json,
                        &final_reason,
                        edges_moved,
                        self_loops_dropped,
                        edges_deduped,
                    )
                })
                .await
                {
                    // The merge committed and the planned row survives
                    // (snapshot NULL, detectable). Rollback of this
                    // merge is impossible; the ERROR names the audit id
                    // so the operator can investigate the row by hand.
                    error!(chat_id, audit_id, loser_id = %action.loser_id, survivor_id = %action.survivor_id, %error, "merge apply: the post-commit audit update failed; the graph mutation stands and the planned audit row survives with a NULL snapshot (rollback of this merge is impossible)");
                }
            }
            // Decision 75 (e): the invariant-pass count feeds the same
            // `facts_invalidated_total` counter as the digest write
            // path. Best effort, like every counter: a failure is a
            // WARN, never an action failure (the merge itself already
            // committed). A zero count skips the store call entirely
            // (the pipeline's `bump_counter_by` discipline).
            if outcome.single_value_invalidated > 0 {
                let count = i64::from(outcome.single_value_invalidated);
                let chat_id_owned = chat_id.to_string();
                if let Err(error) = store_call(store, move |store| {
                    store.increment_counter(&chat_id_owned, "facts_invalidated_total", count)
                })
                .await
                {
                    warn!(chat_id, loser_id = %action.loser_id, survivor_id = %action.survivor_id, %error, "merge apply: failed to increment facts_invalidated_total");
                }
            }
            // Delete the loser's vec row and queue rows (Section 7.7
            // step 3). A failure here must NOT abort the audit write —
            // without the audit row the snapshot is lost and rollback
            // impossible; the next startup reconciliation tombstones
            // the orphaned sidecar rows instead (decision 66). WARN,
            // not Err.
            let loser_id = action.loser_id.clone();
            if let Err(error) = store_call(store, move |store| {
                store.delete_node_embedding_rows(&loser_id)
            })
            .await
            {
                warn!(chat_id, loser_id = %action.loser_id, %error, "merge apply: sidecar tombstone failed; reconciliation will prune the orphan");
            }
            // Decision 76: the loser-touching edge_texts rows die with
            // the merge — the same sidecar-tombstone discipline as the
            // vec/queue rows above (Section 7.7 step 3), one best-
            // effort pass right after them. Derivation note: the
            // natural-key set is the loser's pre-merge edge set (the
            // MergeSnapshot's `edges` list), but tamako-core has no
            // JSON parser in its dependency set and the subtask's
            // three-file constraint forbids adding one — so the ids
            // come from the sidecar itself: every edge_texts row whose
            // encoded natural key (EdgeId::decode) names the loser as
            // an endpoint. That is the same set the snapshot would
            // give (the sidecar mirrors the graph pre-merge), plus any
            // harvest-missed stale rows — strictly more convergent. A
            // row whose id does not decode is left alone (the
            // reconciliation orphan pass prunes by raw set
            // difference). The re-pointed edges live on under NEW ids
            // (the survivor endpoint); their rows are the next
            // reconciliation's missing-from-sidecar upserts.
            // REGRESSION NOTE: a rollback recreates the loser and its
            // original edges, but these rows stay deleted — exactly
            // like the vec rows above, the next startup reconciliation
            // restores them (the edge diff is a journal-free set
            // difference; see crate::embedding::reconcile_group).
            // WARN, never fail: reconciliation prunes the orphans
            // either way.
            match store_call(store, Store::list_edge_text_ids).await {
                Ok(ids) => {
                    let tombstones: Vec<String> = ids
                        .into_iter()
                        .filter(|id| {
                            EdgeId::decode(id).is_ok_and(|key| {
                                key.source_id == action.loser_id || key.target_id == action.loser_id
                            })
                        })
                        .collect();
                    if !tombstones.is_empty() {
                        if let Err(error) =
                            store_call(store, move |store| store.delete_edge_texts(&tombstones))
                                .await
                        {
                            warn!(chat_id, loser_id = %action.loser_id, %error, "merge apply: edge_texts tombstone failed; reconciliation will prune the orphans");
                        }
                    }
                }
                Err(error) => {
                    warn!(chat_id, loser_id = %action.loser_id, %error, "merge apply: edge_texts scan failed; reconciliation will prune the orphans");
                }
            }
            Ok(audit_id)
        }
        MergeVerdict::Related => {
            // The edge runs survivor -> loser: the survivor is the
            // canonical entity, and the entity is the SOURCE of its
            // also_known_as edge (Section 7.4 step 5). One direction is
            // enough — the read paths match alias edges both ways.
            memory
                .link_also_known_as(chat_id, &action.survivor_id, &action.loser_id)
                .await?;
            // specs.md Section 5.2: one append-only audit row per
            // merge-tool action, ALL three verdicts — the audit is the
            // record of the confirmation itself. 'related' carries no
            // snapshot: single insert.
            let row = audit_row(action, confirmed_by, None);
            store_call(store, move |store| store.insert_merge_audit(&row)).await
        }
        // 'different' mutates nothing but still gets its row (snapshot
        // NULL): single insert.
        MergeVerdict::Different => {
            let row = audit_row(action, confirmed_by, None);
            store_call(store, move |store| store.insert_merge_audit(&row)).await
        }
    }
}

/// Executes a confirmed plan (decision 74 point 3: only `--apply`
/// calls this). Per action: `same` merges the loser into the survivor
/// and tombstones its sidecar rows; `related` links the pair with
/// `also_known_as`; `different` mutates nothing. EVERY action appends
/// its audit row (specs.md Section 5.2). `confirmed_by` is
/// `llm:<model>` or `operator` (decision 74 / migration v9).
///
/// Decision 75 (c): `single_value_predicates` is the resolved
/// per-group registry; every 'same' merge runs the invariant pass on
/// the survivor (the invariant holds globally, not just at the digest
/// path). An EMPTY slice preserves the decision-74 behavior exactly.
/// The registry rides a plain parameter (the same seam shape as
/// `MemoryBackend::merge_nodes_with_registry`) rather than an options
/// struct: one call site per CLI mode, no further apply-time options
/// in sight.
///
/// Best-effort per action: one action's failure logs WARN, lands in
/// [`ApplyReport::failures`], and the batch continues — it never
/// aborts mid-way silently.
pub async fn apply_merge_plan<M: MemoryBackend>(
    store: &Arc<Store>,
    memory: &M,
    chat_id: &str,
    plan: &MergePlan,
    confirmed_by: &str,
    single_value_predicates: &[String],
) -> ApplyReport {
    let mut report = ApplyReport::default();
    for action in &plan.actions {
        match apply_one(
            store,
            memory,
            chat_id,
            action,
            confirmed_by,
            single_value_predicates,
        )
        .await
        {
            Ok(audit_id) => report.audit_ids.push(audit_id),
            Err(error) => {
                warn!(chat_id, loser_id = %action.loser_id, survivor_id = %action.survivor_id, verdict = %action.verdict.as_str(), %error, "merge apply: action failed; continuing with the next action");
                report.failures.push(ApplyFailure {
                    loser_id: action.loser_id.clone(),
                    survivor_id: action.survivor_id.clone(),
                    verdict: action.verdict,
                    error: error.to_string(),
                });
            }
        }
    }
    report
}

/// Rolls one merge back from its audit snapshot (graph-spec Section
/// 7.7 step 4): the created edges are deleted, the loser node and its
/// original edges are restored, and the audit row flips to
/// rolled_back.
///
/// Loud refusals ([`MergeError::Rollback`]): a missing audit id, a
/// non-'same' row (nothing to roll back), an already-rolled-back row,
/// a NULL snapshot. A survivor tombstoned by a later merge fails loudly
/// in the memory backend (chained-merge rollback is out of scope).
///
/// The loser's VEC ROW IS NOT RESTORED here: the merge deleted its vec
/// row AND its done-journal rows, so the next startup reconciliation
/// (`crate::embedding::reconcile_group`, decision 66) diffs the
/// recreated node against the empty journal and re-embeds it
/// automatically (decision 74 rebuild note, Section 7.7 step 4). The
/// same holds for the loser-touching edge_texts rows (decision 76):
/// deleted at merge time, restored by the reconciliation's
/// missing-from-sidecar upserts — the edge diff is a journal-free set
/// difference, so the recreated edges reappear there unconditionally.
pub async fn rollback_merge_action<M: MemoryBackend>(
    store: &Arc<Store>,
    memory: &M,
    chat_id: &str,
    audit_id: i64,
) -> Result<(), MergeError> {
    // Decision 77 (M14): the point lookup replaces the list_merge_audit
    // full scan.
    let row = store_call(store, move |store| store.get_merge_audit(audit_id))
        .await?
        .ok_or_else(|| MergeError::Rollback(format!("no merge_audit row with id {audit_id}")))?;
    if row.verdict != MergeVerdict::Same.as_str() {
        return Err(MergeError::Rollback(format!(
            "audit row {audit_id} is a '{}' action; only a 'same' merge can roll back",
            row.verdict
        )));
    }
    if row.rolled_back {
        return Err(MergeError::Rollback(format!(
            "audit row {audit_id} is already rolled back"
        )));
    }
    let snapshot = row
        .snapshot
        .ok_or_else(|| MergeError::Rollback(format!("audit row {audit_id} carries no snapshot")))?;
    memory.rollback_merge(chat_id, &snapshot).await?;
    store_call(store, move |store| store.mark_merge_rolled_back(audit_id)).await?;
    Ok(())
}

/// Decision 77 (H6a): the staleness hash of the two-step apply. The
/// `--merge-tool` dry run writes the confirmed plan to
/// `{data_root}/{chat_id}/merge_plan.json`; `--apply` re-runs the SCAN
/// (cheap, no LLM) and executes the FILE's actions only when the
/// candidate set still matches. The hash covers the candidate set only
/// — the ordered (a_id, b_id) pair and the scan score of every
/// scanned candidate, verdict-independent — so a graph change between
/// the dry run and the apply (a new candidate, a linked or deleted
/// node, a re-embedded vector) mismatches and `--apply` refuses loudly.
///
/// Canonical input: one `a_id\nb_id\n<score bits as 16 lowercase hex>`
/// line per candidate, sorted (the pair order a_id < b_id holds by
/// construction); the score rides its f64 bit pattern so the hash is
/// jitter-free across runs over the same stored vectors. The hash
/// itself reuses the pinned FNV-1a of
/// [`crate::digest::embedding_content_hash`]: a change detector, not a
/// cryptographic hash (the file sits next to the data it describes;
/// tampering with both is out of scope).
pub fn merge_candidate_set_hash<'a>(
    candidates: impl IntoIterator<Item = &'a MergeCandidate>,
) -> String {
    let mut lines: Vec<String> = candidates
        .into_iter()
        .map(|candidate| {
            format!(
                "{}\n{}\n{:016x}",
                candidate.a_id,
                candidate.b_id,
                candidate.score.to_bits()
            )
        })
        .collect();
    lines.sort();
    crate::digest::embedding_content_hash(&lines.join("\n"), "")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use tamako_memory::identifiers;
    use tamako_memory::{LbugBackend, MemoryBatch, MemoryEdge, MemoryNode};
    use tamako_store::EMBEDDING_DIM;
    use time::macros::datetime;
    use time::{Duration, OffsetDateTime};

    use super::*;
    use crate::digest::embedding_content_hash;
    use crate::embedding::{reconcile_group, GroupEmbeddingTarget};

    const CHAT: &str = "chat_m";

    fn base() -> OffsetDateTime {
        datetime!(2026-08-17 10:00 UTC)
    }

    fn node(
        name: &str,
        kind: NodeType,
        description: &str,
        created_at: OffsetDateTime,
    ) -> MemoryNode {
        let id = match kind {
            NodeType::Concept => identifiers::concept_id(name),
            NodeType::Person => identifiers::person_id(name),
            NodeType::Alias => identifiers::alias_id(name),
            NodeType::MessageBatch => identifiers::batch_id(0, 0),
        };
        MemoryNode {
            id,
            name: name.to_string(),
            node_type: kind,
            created_at,
            updated_at: created_at,
            properties: Some(format!(r#"{{"description":"{description}"}}"#)),
        }
    }

    fn edge(
        source_id: &str,
        target_id: &str,
        relationship_name: &str,
        edge_text: &str,
        properties: Option<&str>,
        offset_secs: i64,
    ) -> MemoryEdge {
        let at = base() + Duration::seconds(offset_secs);
        MemoryEdge {
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            relationship_name: relationship_name.to_string(),
            valid_at: at,
            invalid_at: None,
            edge_text: edge_text.to_string(),
            created_at: at,
            updated_at: at,
            properties: properties.map(str::to_string),
        }
    }

    /// Sparse near-one-hot vectors give exact cosine control: a pair
    /// sharing only `dim` sits at similarity 1/sqrt(1+j^2) (~0.995 for
    /// j = 0.1), cross-pair vectors are orthogonal (similarity 0).
    fn vector(dim: usize, jitter: Option<(usize, f32)>) -> Vec<f32> {
        let mut vector = vec![0.0f32; EMBEDDING_DIM];
        vector[dim] = 1.0;
        if let Some((jitter_dim, value)) = jitter {
            vector[jitter_dim] = value;
        }
        vector
    }

    /// The scripted confirmer double: serves the scripted verdict per
    /// name pair (order-insensitive), fails where scripted, defaults to
    /// `different`.
    #[derive(Default)]
    struct ScriptedConfirmer {
        script: HashMap<(String, String), std::result::Result<MergeConfirmation, String>>,
        calls: Mutex<Vec<(String, String)>>,
    }

    impl ScriptedConfirmer {
        fn with(self, a: &str, b: &str, verdict: MergeVerdict, reason: &str) -> Self {
            let mut script = self.script;
            script.insert(
                Self::key(a, b),
                Ok(MergeConfirmation {
                    verdict,
                    reason: reason.to_string(),
                }),
            );
            ScriptedConfirmer { script, ..self }
        }

        fn failing_on(self, a: &str, b: &str) -> Self {
            let mut script = self.script;
            script.insert(Self::key(a, b), Err("endpoint down".to_string()));
            ScriptedConfirmer { script, ..self }
        }

        fn key(a: &str, b: &str) -> (String, String) {
            if a < b {
                (a.to_string(), b.to_string())
            } else {
                (b.to_string(), a.to_string())
            }
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    impl MergeConfirmer for ScriptedConfirmer {
        fn confirm_merge<'a>(
            &'a self,
            a: &'a MergeNodeInfo,
            b: &'a MergeNodeInfo,
        ) -> Pin<Box<dyn Future<Output = Result<MergeConfirmation, MergeError>> + Send + 'a>>
        {
            self.calls
                .lock()
                .expect("calls lock")
                .push((a.name.clone(), b.name.clone()));
            let result = self
                .script
                .get(&Self::key(&a.name, &b.name))
                .cloned()
                .unwrap_or_else(|| {
                    Ok(MergeConfirmation {
                        verdict: MergeVerdict::Different,
                        reason: "unscripted pair".to_string(),
                    })
                });
            Box::pin(async move { result.map_err(MergeError::Confirmer) })
        }
    }

    /// The full integration fixture: three candidate pairs (same /
    /// related / different, one near-parallel vector pair each) plus
    /// unembedded edge-endpoint fixtures.
    struct Fixture {
        rust: MemoryNode,
        rustlang: MemoryNode,
        cat: MemoryNode,
        katze: MemoryNode,
        python: MemoryNode,
        python_snake: MemoryNode,
        cargo: MemoryNode,
        programming: MemoryNode,
        ferris: MemoryNode,
    }

    impl Fixture {
        fn nodes(&self) -> Vec<MemoryNode> {
            vec![
                self.rust.clone(),
                self.rustlang.clone(),
                self.cat.clone(),
                self.katze.clone(),
                self.python.clone(),
                self.python_snake.clone(),
                self.cargo.clone(),
                self.programming.clone(),
                self.ferris.clone(),
            ]
        }

        /// The edges of the fragmented 'same' pair: a shared edge
        /// (dedup on merge), a between-pair edge (self-loop on merge),
        /// a loser-only edge with properties (moved), and enough
        /// survivor-only edges that `rust` wins the degree rule.
        fn edges(&self) -> Vec<MemoryEdge> {
            vec![
                edge(
                    &self.rust.id,
                    &self.cargo.id,
                    "mentions",
                    "Rust mentions Cargo",
                    None,
                    1,
                ),
                edge(
                    &self.rust.id,
                    &self.rustlang.id,
                    "related_to",
                    "Rust is Rust Language",
                    None,
                    2,
                ),
                edge(
                    &self.ferris.id,
                    &self.rust.id,
                    "likes",
                    "Ferris likes Rust",
                    None,
                    3,
                ),
                edge(
                    &self.rust.id,
                    &self.programming.id,
                    "is_a",
                    "Rust is a Programming language",
                    None,
                    4,
                ),
                // The shared edge: same relationship, same other
                // endpoint, same edge text as the survivor's.
                edge(
                    &self.rustlang.id,
                    &self.cargo.id,
                    "mentions",
                    "Rust mentions Cargo",
                    None,
                    5,
                ),
                // The loser-only edge, with properties to carry over.
                edge(
                    &self.rustlang.id,
                    &self.programming.id,
                    "part_of",
                    "Rust Language part of Programming",
                    Some(r#"{"note":"moved"}"#),
                    6,
                ),
            ]
        }

        fn vectors(&self) -> Vec<(&str, Vec<f32>)> {
            vec![
                (&self.rust.id, vector(0, None)),
                (&self.rustlang.id, vector(0, Some((1, 0.1)))),
                (&self.cat.id, vector(2, None)),
                (&self.katze.id, vector(2, Some((3, 0.1)))),
                (&self.python.id, vector(4, None)),
                (&self.python_snake.id, vector(4, Some((5, 0.1)))),
            ]
        }
    }

    fn fixture() -> Fixture {
        let at = base();
        Fixture {
            rust: node("Rust", NodeType::Concept, "the programming language", at),
            rustlang: node(
                "Rust Language",
                NodeType::Concept,
                "the Rust programming language",
                at + Duration::seconds(1),
            ),
            cat: node("Cat", NodeType::Concept, "a domestic animal", at),
            katze: node(
                "Katze",
                NodeType::Concept,
                "German for cat",
                at + Duration::seconds(1),
            ),
            python: node("Python", NodeType::Concept, "a programming language", at),
            python_snake: node(
                "Python Snake",
                NodeType::Concept,
                "a snake",
                at + Duration::seconds(1),
            ),
            cargo: node("Cargo", NodeType::Concept, "the Rust package manager", at),
            programming: node("Programming", NodeType::Concept, "writing programs", at),
            ferris: node("Ferris", NodeType::Person, "the crab mascot", at),
        }
    }

    /// Opens a real Store + real LbugBackend on one tempdir and seeds
    /// the graph and the vec rows.
    async fn seeded(fixture: &Fixture) -> (tempfile::TempDir, Arc<Store>, LbugBackend) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(Store::new(dir.path().to_path_buf()));
        store.open_group(CHAT).expect("open group");
        let memory = LbugBackend::new(dir.path());
        memory
            .upsert_batch(
                CHAT,
                &MemoryBatch {
                    batch_id: identifiers::batch_id(1, 10),
                    nodes: fixture.nodes(),
                    edges: fixture.edges(),
                },
            )
            .await
            .expect("seed graph");
        for (node_id, vector) in fixture.vectors() {
            store
                .upsert_node_embedding(node_id, &vector)
                .expect("seed vector");
        }
        (dir, store, memory)
    }

    fn candidate_named<'c>(
        plan_candidates: &'c [MergeCandidate],
        a: &str,
        b: &str,
    ) -> &'c MergeCandidate {
        plan_candidates
            .iter()
            .find(|candidate| {
                [candidate.a_name.as_str(), candidate.b_name.as_str()].contains(&a)
                    && [candidate.a_name.as_str(), candidate.b_name.as_str()].contains(&b)
            })
            .unwrap_or_else(|| panic!("candidate pair {a}/{b} exists"))
    }

    #[tokio::test]
    async fn scan_finds_the_three_pairs_sorted_by_score_descending() {
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;

        let candidates = scan_merge_candidates(&store, &memory, CHAT, 0.85)
            .await
            .expect("scan");
        assert_eq!(candidates.len(), 3);
        // The pair fields: deterministic a < b id order, shared kind,
        // both names, a score at or above the threshold.
        let same = candidate_named(&candidates, "Rust", "Rust Language");
        assert_eq!(same.kind, NodeType::Concept);
        assert!(same.a_id < same.b_id);
        assert!(same.score >= 0.85);
        // Sorted by score descending (ties break on ids): the scores
        // are monotone non-increasing.
        assert!(candidates
            .windows(2)
            .all(|pair| pair[0].score >= pair[1].score));
    }

    #[tokio::test]
    async fn scan_excludes_below_threshold_pairs() {
        // cos((1,0), (0.8,0.6)) = 0.8 exactly.
        let a = node("Alpha", NodeType::Concept, "a", base());
        let b = node("Beta", NodeType::Concept, "b", base());
        let fixture = Fixture {
            rust: a.clone(),
            rustlang: b.clone(),
            ..fixture()
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(Store::new(dir.path().to_path_buf()));
        store.open_group(CHAT).expect("open group");
        let memory = LbugBackend::new(dir.path());
        memory
            .upsert_batch(
                CHAT,
                &MemoryBatch {
                    batch_id: identifiers::batch_id(1, 10),
                    nodes: vec![a.clone(), b.clone()],
                    edges: vec![],
                },
            )
            .await
            .expect("seed graph");
        let mut va = vec![0.0f32; EMBEDDING_DIM];
        va[0] = 1.0;
        let mut vb = vec![0.0f32; EMBEDDING_DIM];
        vb[0] = 0.8;
        vb[1] = 0.6;
        store.upsert_node_embedding(&a.id, &va).expect("va");
        store.upsert_node_embedding(&b.id, &vb).expect("vb");

        // 0.8 < 0.85: the pair is absent at the decision-74 default.
        let candidates = scan_merge_candidates(&store, &memory, CHAT, 0.85)
            .await
            .expect("scan");
        assert!(candidates.is_empty());
        // At threshold 0.79 the same pair qualifies. (Not 0.80 on the
        // nose: 0.8/0.6 are not exact in f32, so the cosine lands an
        // epsilon below the mathematical 0.8.)
        let candidates = scan_merge_candidates(&store, &memory, CHAT, 0.79)
            .await
            .expect("scan");
        assert_eq!(candidates.len(), 1);
        drop(fixture);
    }

    #[tokio::test]
    async fn scan_never_makes_alias_or_cross_kind_endpoints() {
        let at = base();
        let person = node("Ada", NodeType::Person, "a member", at);
        let alias = node("ada", NodeType::Alias, "", at);
        let concept = node("Adaline", NodeType::Concept, "a name", at);
        // A second concept pair so the scan still finds something.
        let c1 = node("Kappa", NodeType::Concept, "k", at);
        let c2 = node("Kappa2", NodeType::Concept, "k2", at);
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(Store::new(dir.path().to_path_buf()));
        store.open_group(CHAT).expect("open group");
        let memory = LbugBackend::new(dir.path());
        memory
            .upsert_batch(
                CHAT,
                &MemoryBatch {
                    batch_id: identifiers::batch_id(1, 10),
                    nodes: vec![
                        person.clone(),
                        alias.clone(),
                        concept.clone(),
                        c1.clone(),
                        c2.clone(),
                    ],
                    edges: vec![],
                },
            )
            .await
            .expect("seed graph");
        // Person, alias, and concept share the IDENTICAL vector; only
        // the concept pair is a legal candidate.
        store
            .upsert_node_embedding(&person.id, &vector(0, None))
            .expect("p");
        store
            .upsert_node_embedding(&alias.id, &vector(0, None))
            .expect("a");
        store
            .upsert_node_embedding(&concept.id, &vector(0, None))
            .expect("c");
        store
            .upsert_node_embedding(&c1.id, &vector(1, None))
            .expect("c1");
        store
            .upsert_node_embedding(&c2.id, &vector(1, None))
            .expect("c2");

        let candidates = scan_merge_candidates(&store, &memory, CHAT, 0.85)
            .await
            .expect("scan");
        // Alias nodes never appear as endpoints (decision 74 point 6),
        // and a Person/Concept pair is kind-incompatible (Section 7.7
        // step 1) — the identical person/alias/concept vectors yield
        // nothing. The Kappa pair survives.
        assert_eq!(candidates.len(), 1);
        candidate_named(&candidates, "Kappa", "Kappa2");
    }

    #[tokio::test]
    async fn scan_excludes_already_linked_pairs() {
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        // The 'same' pair is already linked: a legitimate surface-form
        // link, not a duplicate (Section 7.7 step 1).
        memory
            .link_also_known_as(CHAT, &fixture.rust.id, &fixture.rustlang.id)
            .await
            .expect("link");

        let candidates = scan_merge_candidates(&store, &memory, CHAT, 0.85)
            .await
            .expect("scan");
        assert_eq!(candidates.len(), 2);
        assert!(candidates
            .iter()
            .all(|c| c.a_id != fixture.rust.id && c.b_id != fixture.rust.id
                || c.a_id != fixture.rustlang.id && c.b_id != fixture.rustlang.id));
    }

    #[tokio::test]
    async fn plan_caps_confirmations_and_marks_the_rest_unconfirmed() {
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        let confirmer = ScriptedConfirmer::default();

        let plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 1)
            .await
            .expect("plan");
        assert_eq!(plan.actions.len(), 1);
        assert_eq!(plan.skipped.len(), 2);
        assert!(plan
            .skipped
            .iter()
            .all(|(_, reason)| *reason == SkipReason::OverConfirmationBudget));
        // Only ONE confirmer call happened: the budget never confirms
        // the skipped pairs.
        assert_eq!(confirmer.calls().len(), 1);
    }

    #[tokio::test]
    async fn plan_skips_a_failed_confirmation_and_never_merges_the_pair() {
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        let confirmer = ScriptedConfirmer::default()
            .failing_on("Rust", "Rust Language")
            .with(
                "Cat",
                "Katze",
                MergeVerdict::Related,
                "cross-language synonym",
            );

        let plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        // The failed pair is skipped with the failure note; the
        // scripted pairs are actions.
        assert_eq!(plan.actions.len(), 2);
        let skipped: Vec<_> = plan
            .skipped
            .iter()
            .filter(|(_, reason)| matches!(reason, SkipReason::ConfirmationFailed(_)))
            .collect();
        assert_eq!(skipped.len(), 1);
        assert!(
            matches!(&skipped[0].1, SkipReason::ConfirmationFailed(note) if note.contains("endpoint down"))
        );
    }

    #[tokio::test]
    async fn planning_is_a_dry_run_and_writes_nothing() {
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        let nodes_before = memory.list_node_contents(CHAT).await.expect("nodes").len();
        let confirmer = ScriptedConfirmer::default()
            .with(
                "Rust",
                "Rust Language",
                MergeVerdict::Same,
                "identical concept",
            )
            .with(
                "Cat",
                "Katze",
                MergeVerdict::Related,
                "cross-language synonym",
            )
            .with(
                "Python",
                "Python Snake",
                MergeVerdict::Different,
                "homonyms",
            );

        let plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        assert_eq!(plan.actions.len(), 3);
        // The survivor choice of Section 7.7 step 3: `rust` has four
        // edges against `rustlang`'s three.
        let same = plan
            .actions
            .iter()
            .find(|action| action.verdict == MergeVerdict::Same)
            .expect("same action");
        assert_eq!(same.survivor_id, fixture.rust.id);
        assert_eq!(same.loser_id, fixture.rustlang.id);
        // Degree ties go to the older created_at.
        let related = plan
            .actions
            .iter()
            .find(|action| action.verdict == MergeVerdict::Related)
            .expect("related action");
        assert_eq!(related.survivor_id, fixture.cat.id);
        assert_eq!(related.loser_id, fixture.katze.id);

        // The dry run wrote NOTHING: graph unchanged, audit table
        // empty, vec rows intact.
        assert_eq!(
            memory.list_node_contents(CHAT).await.expect("nodes").len(),
            nodes_before
        );
        assert!(store.list_merge_audit().expect("audit").is_empty());
        assert!(store
            .node_embedding(&fixture.rustlang.id)
            .expect("vec row")
            .is_some());
        assert!(!memory
            .are_linked(CHAT, &fixture.cat.id, &fixture.katze.id)
            .await
            .expect("linked"));
    }

    /// Plans and applies the full three-verdict scenario; returns the
    /// fixture handle pieces the assertions need.
    async fn applied() -> (
        tempfile::TempDir,
        Arc<Store>,
        LbugBackend,
        Fixture,
        ApplyReport,
    ) {
        let fixture = fixture();
        let (dir, store, memory) = seeded(&fixture).await;
        // A pending queue row of the loser proves the merge tombstones
        // the queue rows too, not only the vec row.
        store
            .enqueue_embeddings(&[(fixture.rustlang.id.clone(), "stale".to_string())])
            .expect("enqueue");
        let confirmer = ScriptedConfirmer::default()
            .with(
                "Rust",
                "Rust Language",
                MergeVerdict::Same,
                "identical concept",
            )
            .with(
                "Cat",
                "Katze",
                MergeVerdict::Related,
                "cross-language synonym",
            )
            .with(
                "Python",
                "Python Snake",
                MergeVerdict::Different,
                "homonyms",
            );
        let plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        let report = apply_merge_plan(&store, &memory, CHAT, &plan, "llm:test-model", &[]).await;
        (dir, store, memory, fixture, report)
    }

    #[tokio::test]
    async fn apply_executes_all_three_verdicts() {
        let (_dir, store, memory, fixture, report) = applied().await;
        assert_eq!(report.audit_ids.len(), 3);
        assert!(report.failures.is_empty());

        // The 'same' merge: loser tombstoned, edges moved with
        // properties, the self-loop dropped, the shared edge deduped.
        assert_eq!(
            memory
                .node_content(CHAT, &fixture.rustlang.id)
                .await
                .expect("content"),
            None,
            "the loser is hard-deleted"
        );
        let survivor_edges = memory
            .query_rows(
                CHAT,
                "MATCH (s:Node)-[r:EDGE]->(t:Node) RETURN s.id, t.id, r.relationship_name, r.properties",
            )
            .await
            .expect("edges");
        // The re-pointed part_of edge kept its properties.
        assert!(survivor_edges.iter().any(|row| {
            row[0] == fixture.rust.id
                && row[1] == fixture.programming.id
                && row[2] == "part_of"
                && row[3].contains("moved")
        }));
        // The shared-edge re-point deduped: exactly one mentions edge
        // into cargo remains.
        assert_eq!(
            survivor_edges
                .iter()
                .filter(|row| row[1] == fixture.cargo.id && row[2] == "mentions")
                .count(),
            1
        );
        // The between-pair edge became a self-loop and was dropped:
        // no related_to edge remains at all, and no self-loop exists.
        assert!(!survivor_edges.iter().any(|row| row[2] == "related_to"));
        assert!(!survivor_edges.iter().any(|row| row[0] == row[1]));

        // The sidecar tombstone: the loser's vec row AND its queued
        // stale row are gone (all_embedding_node_ids is the UNION).
        assert_eq!(
            store.node_embedding(&fixture.rustlang.id).expect("vec"),
            None
        );
        assert!(!store
            .all_embedding_node_ids()
            .expect("ids")
            .contains(&fixture.rustlang.id));

        // The 'related' verdict linked the pair (survivor -> loser).
        assert!(memory
            .are_linked(CHAT, &fixture.cat.id, &fixture.katze.id)
            .await
            .expect("linked"));

        // The 'different' verdict mutated nothing.
        assert!(memory
            .node_content(CHAT, &fixture.python_snake.id)
            .await
            .expect("content")
            .is_some());
        assert!(!memory
            .are_linked(CHAT, &fixture.python.id, &fixture.python_snake.id)
            .await
            .expect("linked"));

        // The audit rows: all three verdicts are recorded (specs.md
        // Section 5.2), the 'same' row with the full detail.
        let rows = store.list_merge_audit().expect("audit");
        assert_eq!(rows.len(), 3);
        let same = rows
            .iter()
            .find(|row| row.verdict == "same")
            .expect("same row");
        assert_eq!(same.loser_id, fixture.rustlang.id);
        assert_eq!(same.survivor_id, fixture.rust.id);
        assert_eq!(same.loser_kind, "Concept");
        assert_eq!(same.loser_name, "Rust Language");
        assert_eq!(
            same.loser_description.as_deref(),
            Some("the Rust programming language")
        );
        assert_eq!(same.reason, "identical concept");
        assert_eq!(same.confirmed_by, "llm:test-model");
        assert_eq!(same.edges_moved, 1);
        assert_eq!(same.self_loops_dropped, 1);
        assert_eq!(same.edges_deduped, 1);
        assert!(same.snapshot.is_some(), "the rollback source");
        assert!(!same.rolled_back);
        for verdict in ["related", "different"] {
            let row = rows
                .iter()
                .find(|row| row.verdict == verdict)
                .unwrap_or_else(|| panic!("{verdict} row"));
            assert_eq!(row.snapshot, None);
            assert_eq!(row.edges_moved, 0);
            assert_eq!(row.confirmed_by, "llm:test-model");
        }
    }

    #[tokio::test]
    async fn apply_is_best_effort_per_action() {
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        let confirmer = ScriptedConfirmer::default()
            .with(
                "Rust",
                "Rust Language",
                MergeVerdict::Same,
                "identical concept",
            )
            .with(
                "Cat",
                "Katze",
                MergeVerdict::Related,
                "cross-language synonym",
            );
        let mut plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        plan.actions
            .retain(|action| action.verdict != MergeVerdict::Different);
        // Corrupt the same-action's survivor: the merge must fail
        // loudly, land in the report, and NOT stop the related action.
        for action in &mut plan.actions {
            if action.verdict == MergeVerdict::Same {
                action.survivor_id = identifiers::concept_id("gone");
            }
        }
        let report = apply_merge_plan(&store, &memory, CHAT, &plan, "operator", &[]).await;
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].loser_id, fixture.rustlang.id);
        assert_eq!(
            report.audit_ids.len(),
            1,
            "the related action still applied"
        );
        assert!(memory
            .are_linked(CHAT, &fixture.cat.id, &fixture.katze.id)
            .await
            .expect("linked"));
        // The failed merge left the graph untouched.
        assert!(memory
            .node_content(CHAT, &fixture.rustlang.id)
            .await
            .expect("content")
            .is_some());
    }

    #[tokio::test]
    async fn apply_with_registry_invalidates_the_survivors_single_value_duplicate() {
        // Decision 75 (c): two fragmented Concepts each carry ONE valid
        // `works_at` edge; the re-point leaves TWO on the survivor, and
        // the invariant pass of `merge_nodes_with_registry` invalidates
        // the older one. The counter bumps, and the audit row's reason
        // carries the invariant note (the v9 schema stays frozen).
        let at = base();
        let rust = node("Rust", NodeType::Concept, "the programming language", at);
        let rustlang = node(
            "Rust Language",
            NodeType::Concept,
            "the Rust programming language",
            at + Duration::seconds(1),
        );
        let acme = node("AcmeCorp", NodeType::Concept, "the old employer", at);
        let newcorp = node("NewCorp", NodeType::Concept, "the new employer", at);
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(Store::new(dir.path().to_path_buf()));
        store.open_group(CHAT).expect("open group");
        let memory = LbugBackend::new(dir.path());
        memory
            .upsert_batch(
                CHAT,
                &MemoryBatch {
                    batch_id: identifiers::batch_id(1, 10),
                    nodes: vec![
                        rust.clone(),
                        rustlang.clone(),
                        acme.clone(),
                        newcorp.clone(),
                    ],
                    edges: vec![
                        // The loser's OLDER works_at edge.
                        edge(
                            &rustlang.id,
                            &acme.id,
                            "works_at",
                            "Rust Language at AcmeCorp",
                            None,
                            1,
                        ),
                        // The survivor's NEWER works_at edge.
                        edge(
                            &rust.id,
                            &newcorp.id,
                            "works_at",
                            "Rust at NewCorp",
                            None,
                            2,
                        ),
                        // Enough survivor-only edges that `rust` wins the
                        // degree rule and survives.
                        edge(
                            &rust.id,
                            &acme.id,
                            "mentions",
                            "Rust mentions AcmeCorp",
                            None,
                            3,
                        ),
                        edge(
                            &rust.id,
                            &newcorp.id,
                            "mentions",
                            "Rust mentions NewCorp",
                            None,
                            4,
                        ),
                    ],
                },
            )
            .await
            .expect("seed graph");
        // Near-parallel vectors make the pair the only candidate.
        store
            .upsert_node_embedding(&rust.id, &vector(0, None))
            .expect("vec");
        store
            .upsert_node_embedding(&rustlang.id, &vector(0, Some((1, 0.1))))
            .expect("vec");

        let confirmer = ScriptedConfirmer::default().with(
            "Rust",
            "Rust Language",
            MergeVerdict::Same,
            "identical concept",
        );
        let plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        assert_eq!(plan.actions.len(), 1);
        let registry = vec!["works_at".to_string()];
        let report = apply_merge_plan(&store, &memory, CHAT, &plan, "operator", &registry).await;
        assert!(report.failures.is_empty());
        assert_eq!(report.audit_ids.len(), 1);

        // Exactly one valid works_at edge on the survivor: the NEWER
        // one (NewCorp); the re-pointed older one (AcmeCorp) is invalid.
        let rows = memory
            .query_rows(
                CHAT,
                &format!(
                    "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->(t:Node) \
                     WHERE r.relationship_name = 'works_at' \
                     RETURN t.name, r.invalid_at",
                    rust.id
                ),
            )
            .await
            .expect("edges");
        assert_eq!(rows.len(), 2, "both edges exist on the survivor");
        let valid: Vec<_> = rows
            .iter()
            .filter(|row| row[1] == "NULL")
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(
            valid,
            vec!["NewCorp".to_string()],
            "the newest valid_at wins"
        );

        // The counter bumped by the invariant count.
        assert_eq!(
            store
                .get_state(CHAT, "facts_invalidated_total")
                .expect("state"),
            Some("1".to_string())
        );

        // The audit row: the edge counters keep the v9 shape, and the
        // reason carries the invariant note verbatim.
        let row = store
            .list_merge_audit()
            .expect("audit")
            .into_iter()
            .find(|row| row.verdict == "same")
            .expect("same row");
        assert_eq!(
            row.reason,
            "identical concept; single-value invariant: invalidated 1 edge(s)"
        );
    }

    #[tokio::test]
    async fn apply_with_an_empty_registry_keeps_the_decision_74_audit_reason() {
        // The empty-registry apply path is the decision-74 behavior: no
        // invariant pass, no counter row, no note in the reason.
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        let confirmer = ScriptedConfirmer::default().with(
            "Rust",
            "Rust Language",
            MergeVerdict::Same,
            "identical concept",
        );
        let plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        let report = apply_merge_plan(&store, &memory, CHAT, &plan, "operator", &[]).await;
        assert!(report.failures.is_empty());
        let row = store
            .list_merge_audit()
            .expect("audit")
            .into_iter()
            .find(|row| row.verdict == "same")
            .expect("same row");
        assert_eq!(row.reason, "identical concept");
        assert_eq!(
            store
                .get_state(CHAT, "facts_invalidated_total")
                .expect("state"),
            None,
            "no counter row without an invariant pass"
        );
    }

    #[tokio::test]
    async fn apply_tombstones_the_losers_edge_text_rows() {
        // Decision 76: a 'same' merge deletes the loser-touching
        // edge_texts rows immediately (the vec-row tombstone
        // discipline), while the rows of edges that never touched the
        // loser survive.
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        // Seed the sidecar the way the decision-76 digest harvest does:
        // one row per graph edge, keyed by the encoded natural key.
        let graph_edges = memory.list_all_edges(CHAT).await.expect("edges");
        for (edge_id, edge_text) in &graph_edges {
            store.upsert_edge_text(edge_id, edge_text).expect("seed");
        }
        let confirmer = ScriptedConfirmer::default().with(
            "Rust",
            "Rust Language",
            MergeVerdict::Same,
            "identical concept",
        );
        let mut plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        plan.actions
            .retain(|action| action.verdict == MergeVerdict::Same);
        let report = apply_merge_plan(&store, &memory, CHAT, &plan, "operator", &[]).await;
        assert!(report.failures.is_empty());

        let remaining = store.list_edge_text_ids().expect("ids");
        // Every loser-touching row is gone (including the incoming
        // related_to edge, dropped as a self-loop, and the deduped
        // shared mentions edge — the survivor's equivalent row is keyed
        // by a DIFFERENT natural key and survives).
        assert!(remaining.iter().all(|id| {
            let key = EdgeId::decode(id).expect("decode");
            key.source_id != fixture.rustlang.id && key.target_id != fixture.rustlang.id
        }));
        // Exactly the three untouched edges keep their rows: the
        // survivor's own mentions/is_a edges and ferris's incoming
        // likes edge.
        let edge_id = |source: &str, relationship: &str, target: &str, offset: i64| {
            EdgeId {
                source_id: source.to_string(),
                relationship_name: relationship.to_string(),
                target_id: target.to_string(),
                valid_at: base() + Duration::seconds(offset),
            }
            .encode()
        };
        let mut expected = vec![
            edge_id(&fixture.rust.id, "mentions", &fixture.cargo.id, 1),
            edge_id(&fixture.ferris.id, "likes", &fixture.rust.id, 3),
            edge_id(&fixture.rust.id, "is_a", &fixture.programming.id, 4),
        ];
        expected.sort();
        assert_eq!(remaining, expected);
    }

    #[tokio::test]
    async fn reconciliation_restores_the_rolled_back_edge_text_rows() {
        // Decision 76 regression: the merge deletes the loser-touching
        // edge_texts rows; a rollback recreates the loser and its edges
        // but NOT the rows — the next reconciliation restores them (the
        // journal-free set difference), exactly like the vec rows.
        let fixture = fixture();
        let (dir, store, memory) = seeded(&fixture).await;
        let graph_edges = memory.list_all_edges(CHAT).await.expect("edges");
        for (edge_id, edge_text) in &graph_edges {
            store.upsert_edge_text(edge_id, edge_text).expect("seed");
        }
        let confirmer = ScriptedConfirmer::default().with(
            "Rust",
            "Rust Language",
            MergeVerdict::Same,
            "identical concept",
        );
        let mut plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        plan.actions
            .retain(|action| action.verdict == MergeVerdict::Same);
        let report = apply_merge_plan(&store, &memory, CHAT, &plan, "operator", &[]).await;
        assert!(report.failures.is_empty());
        let sidecar_after_merge = store.list_edge_text_ids().expect("ids").len();
        assert!(sidecar_after_merge < graph_edges.len());

        rollback_merge_action(&store, &memory, CHAT, report.audit_ids[0])
            .await
            .expect("rollback");
        // The rollback itself writes no sidecar rows.
        assert_eq!(
            store.list_edge_text_ids().expect("ids").len(),
            sidecar_after_merge
        );

        let target = GroupEmbeddingTarget::open(dir.path(), CHAT).expect("target");
        let reconcile = reconcile_group(&memory, &target).await;
        // The restored loser's edges are missing from the sidecar: the
        // reconciliation rewrites them (plus the merge-created
        // re-point, whose row the merge-time graph never had).
        assert!(reconcile.edge_texts_upserted > 0);
        // The merge-created re-pointed edges left the graph at
        // rollback; any of their harvested rows would prune. None were
        // ever written in this test, so nothing prunes.
        assert_eq!(reconcile.edge_texts_pruned, 0);
        // Convergence: the sidecar mirrors the post-rollback graph
        // exactly.
        let mut graph_ids: Vec<String> = memory
            .list_all_edges(CHAT)
            .await
            .expect("edges")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        graph_ids.sort();
        assert_eq!(store.list_edge_text_ids().expect("ids"), graph_ids);
    }

    #[tokio::test]
    async fn rollback_restores_the_pre_merge_shape() {
        let (_dir, store, memory, fixture, report) = applied().await;
        let rows = store.list_merge_audit().expect("audit");
        let same_id = rows
            .iter()
            .find(|row| row.verdict == "same")
            .expect("same row")
            .id;
        assert!(report.audit_ids.contains(&same_id));

        rollback_merge_action(&store, &memory, CHAT, same_id)
            .await
            .expect("rollback");

        // The loser node is back with its original content.
        let restored = memory
            .node_content(CHAT, &fixture.rustlang.id)
            .await
            .expect("content")
            .expect("restored");
        assert_eq!(restored.name, "Rust Language");
        assert_eq!(restored.description, "the Rust programming language");
        let edges = memory
            .query_rows(
                CHAT,
                "MATCH (s:Node)-[r:EDGE]->(t:Node) RETURN s.id, t.id, r.relationship_name",
            )
            .await
            .expect("edges");
        // The original loser edges are back (including the between-pair
        // edge and the shared mentions edge).
        assert!(edges.iter().any(|row| row[0] == fixture.rustlang.id
            && row[1] == fixture.cargo.id
            && row[2] == "mentions"));
        assert!(edges.iter().any(|row| row[0] == fixture.rust.id
            && row[1] == fixture.rustlang.id
            && row[2] == "related_to"));
        // The merge-created re-point is deleted.
        assert!(!edges.iter().any(|row| row[0] == fixture.rust.id
            && row[1] == fixture.programming.id
            && row[2] == "part_of"));
        // The audit row is marked.
        let row = store
            .list_merge_audit()
            .expect("audit")
            .into_iter()
            .find(|row| row.id == same_id)
            .expect("row");
        assert!(row.rolled_back);

        // Rolling back twice is a loud refusal.
        let error = rollback_merge_action(&store, &memory, CHAT, same_id)
            .await
            .expect_err("a second rollback is refused");
        assert!(error.to_string().contains("already rolled back"));
        // A missing audit id is a loud refusal.
        let error = rollback_merge_action(&store, &memory, CHAT, 9999)
            .await
            .expect_err("a missing audit id is refused");
        assert!(error.to_string().contains("no merge_audit row"));
        // A non-'same' row has nothing to roll back.
        let related_id = store
            .list_merge_audit()
            .expect("audit")
            .into_iter()
            .find(|row| row.verdict == "related")
            .expect("related row")
            .id;
        let error = rollback_merge_action(&store, &memory, CHAT, related_id)
            .await
            .expect_err("a 'related' row cannot roll back");
        assert!(error.to_string().contains("only a 'same' merge"));
    }

    #[tokio::test]
    async fn reconciliation_ignores_the_tombstone_and_recovers_the_rollback() {
        let fixture = fixture();
        let (dir, store, memory) = seeded(&fixture).await;
        // Journal every current node as done, so the only possible
        // enqueue is a node whose done rows vanished (the loser at
        // merge time).
        for (node_id, content) in memory.list_node_contents(CHAT).await.expect("contents") {
            store
                .record_node_embedded(
                    &node_id,
                    &embedding_content_hash(&content.name, &content.description),
                )
                .expect("journal");
        }
        let confirmer = ScriptedConfirmer::default().with(
            "Rust",
            "Rust Language",
            MergeVerdict::Same,
            "identical concept",
        );
        let mut plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        plan.actions
            .retain(|action| action.verdict == MergeVerdict::Same);
        let report = apply_merge_plan(&store, &memory, CHAT, &plan, "operator", &[]).await;
        assert!(report.failures.is_empty());

        // After the merge, reconciliation does NOTHING for the loser:
        // its graph node is gone (not re-enqueued) and its sidecar rows
        // died at merge time (no orphan to prune). Nothing else is
        // resurrected unexpectedly.
        let target = GroupEmbeddingTarget::open(dir.path(), CHAT).expect("target");
        let reconcile = reconcile_group(&memory, &target).await;
        assert_eq!(reconcile.enqueued, 0);
        assert_eq!(reconcile.pruned_orphans, 0);

        // After the rollback the restored node IS re-enqueued: its
        // done-journal rows were deleted at merge time (decision 74
        // rebuild note / Section 7.7 step 4).
        rollback_merge_action(&store, &memory, CHAT, report.audit_ids[0])
            .await
            .expect("rollback");
        let reconcile = reconcile_group(&memory, &target).await;
        assert_eq!(reconcile.enqueued, 1);
        assert_eq!(reconcile.pruned_orphans, 0);
        let claimed = target
            .store
            .claim_embedding_batch(8)
            .expect("claim")
            .into_iter()
            .map(|row| row.node_id)
            .collect::<Vec<_>>();
        assert_eq!(claimed, vec![fixture.rustlang.id.clone()]);
    }

    #[test]
    fn survivor_choice_follows_degree_then_age_then_id() {
        let stats = |degree: u64, created_at: OffsetDateTime| NodeMergeStats {
            edge_degree: degree,
            created_at,
        };
        // Higher degree wins.
        let map = HashMap::from([
            ("a".to_string(), stats(5, base())),
            ("b".to_string(), stats(3, base())),
        ]);
        assert_eq!(
            choose_survivor("a", "b", &map),
            ("a".to_string(), "b".to_string())
        );
        assert_eq!(
            choose_survivor("b", "a", &map),
            ("a".to_string(), "b".to_string())
        );
        // A degree tie goes to the older created_at.
        let map = HashMap::from([
            ("a".to_string(), stats(5, base() + Duration::seconds(1))),
            ("b".to_string(), stats(5, base())),
        ]);
        assert_eq!(
            choose_survivor("a", "b", &map),
            ("b".to_string(), "a".to_string())
        );
        // A full tie (equal degree and timestamp) is deterministic:
        // the lexicographically smaller id survives.
        let map = HashMap::from([
            ("a".to_string(), stats(5, base())),
            ("b".to_string(), stats(5, base())),
        ]);
        assert_eq!(
            choose_survivor("a", "b", &map),
            ("a".to_string(), "b".to_string())
        );
        // The documented fallback: missing stats (a node vanished
        // between scan and plan) fall back to the smaller id.
        let map = HashMap::from([("b".to_string(), stats(5, base()))]);
        assert_eq!(
            choose_survivor("a", "b", &map),
            ("a".to_string(), "b".to_string())
        );
    }

    #[test]
    fn merge_verdict_wire_strings_round_trip() {
        for (verdict, wire) in [
            (MergeVerdict::Same, "same"),
            (MergeVerdict::Related, "related"),
            (MergeVerdict::Different, "different"),
        ] {
            assert_eq!(verdict.as_str(), wire);
            assert_eq!(MergeVerdict::from_str(wire), Some(verdict));
        }
        assert_eq!(MergeVerdict::from_str("maybe"), None);
    }

    #[tokio::test]
    async fn apply_leaves_a_planned_audit_row_when_the_merge_fails() {
        // Decision 77 (H4): the audit row lands BEFORE the graph
        // mutation. A rigged merge failure (the survivor deleted
        // between plan and apply) still leaves the 'same' row —
        // verdict/reason/confirmed_by/loser filled, snapshot NULL, the
        // edge counters zero. The row is the detectable crash marker.
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        let confirmer = ScriptedConfirmer::default().with(
            "Rust",
            "Rust Language",
            MergeVerdict::Same,
            "identical concept",
        );
        let mut plan = plan_merges(&store, &memory, CHAT, 0.85, &confirmer, 10)
            .await
            .expect("plan");
        plan.actions
            .retain(|action| action.verdict == MergeVerdict::Same);
        // Rig the failure: the survivor vanishes between plan and apply.
        for action in &mut plan.actions {
            action.survivor_id = identifiers::concept_id("gone");
        }
        let report = apply_merge_plan(&store, &memory, CHAT, &plan, "llm:test-model", &[]).await;
        assert_eq!(report.failures.len(), 1);
        assert!(report.audit_ids.is_empty());

        let rows = store.list_merge_audit().expect("audit");
        assert_eq!(rows.len(), 1, "the planned row survives the failure");
        let row = &rows[0];
        assert_eq!(row.verdict, "same");
        assert_eq!(row.loser_id, fixture.rustlang.id);
        assert_eq!(row.survivor_id, identifiers::concept_id("gone"));
        assert_eq!(row.loser_name, "Rust Language");
        assert_eq!(row.reason, "identical concept");
        assert_eq!(row.confirmed_by, "llm:test-model");
        assert_eq!(row.snapshot, None, "no snapshot without the merge");
        assert_eq!(row.edges_moved, 0);
        assert!(!row.rolled_back);
        // The graph is untouched.
        assert!(memory
            .node_content(CHAT, &fixture.rustlang.id)
            .await
            .expect("content")
            .is_some());
    }

    #[tokio::test]
    async fn apply_fills_the_planned_row_with_the_snapshot_and_counts() {
        // Decision 77 (H4) happy path: the phase-3 update lands the
        // snapshot and the ACTUAL counters on the row inserted before
        // the graph mutation (one row per action, none duplicated).
        let (_dir, store, _memory, fixture, report) = applied().await;
        assert!(report.failures.is_empty());
        let rows = store.list_merge_audit().expect("audit");
        assert_eq!(rows.len(), 3, "exactly one row per action");
        let same = rows
            .iter()
            .find(|row| row.verdict == "same")
            .expect("same row");
        assert_eq!(same.loser_id, fixture.rustlang.id);
        assert!(same.snapshot.is_some(), "the rollback source landed");
        assert_eq!(same.edges_moved, 1);
        assert_eq!(same.self_loops_dropped, 1);
        assert_eq!(same.edges_deduped, 1);
        // The report's audit ids are the row ids of the planned rows.
        assert!(report.audit_ids.contains(&same.id));
        // The phase-3 update failure path logs ERROR with the audit id
        // and keeps the row + the graph state (see the error! call in
        // apply_one); log capture is unavailable here, so the path is
        // covered by code + comment.
    }

    #[tokio::test]
    async fn merge_candidate_set_hash_is_stable_and_detects_graph_changes() {
        // Decision 77 (H6a): the staleness hash of the two-step apply.
        // The same scan hashes identically (deterministic across
        // calls); a graph change between the dry run and the apply
        // (here: a pair linked, which the scan then excludes) changes
        // the candidate set and therefore the hash — the --apply
        // refusal trigger.
        let fixture = fixture();
        let (_dir, store, memory) = seeded(&fixture).await;
        let scan = || scan_merge_candidates(&store, &memory, CHAT, 0.85);

        let first = scan().await.expect("scan");
        assert_eq!(first.len(), 3);
        let hash = merge_candidate_set_hash(&first);
        // Deterministic: a second identical scan hashes the same.
        assert_eq!(merge_candidate_set_hash(&scan().await.expect("scan")), hash);

        // Link the 'same' pair: the next scan excludes it (already
        // linked), the candidate set shrinks, the hash changes.
        memory
            .link_also_known_as(CHAT, &fixture.rust.id, &fixture.rustlang.id)
            .await
            .expect("link");
        let after = scan().await.expect("scan");
        assert_eq!(after.len(), 2);
        assert_ne!(merge_candidate_set_hash(&after), hash);
    }

    #[test]
    fn merge_candidate_set_hash_ignores_verdicts_and_covers_scores() {
        // The hash is verdict-independent by construction: it covers
        // ids + scores only. A score change (a re-embedded vector pair
        // at a different similarity) changes the hash.
        let candidate = |score: f64| MergeCandidate {
            a_id: "a".to_string(),
            b_id: "b".to_string(),
            a_name: "A".to_string(),
            b_name: "B".to_string(),
            a_description: String::new(),
            b_description: String::new(),
            kind: NodeType::Concept,
            score,
        };
        let one = [candidate(0.9)];
        let other_score = [candidate(0.8)];
        assert_eq!(
            merge_candidate_set_hash(&one),
            merge_candidate_set_hash(&[candidate(0.9)]),
            "stable for the same set"
        );
        assert_ne!(
            merge_candidate_set_hash(&one),
            merge_candidate_set_hash(&other_score),
            "a score change is a candidate-set change"
        );
        assert_ne!(
            merge_candidate_set_hash(&one),
            merge_candidate_set_hash(&[]),
            "an empty set differs"
        );
    }
}
