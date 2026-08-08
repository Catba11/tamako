//! The shallow recall worker of specs.md Section 9 step 2 (Sections
//! 9.1-9.4). M5 replaces the `NoopRecall` seam with this module.
//!
//! The flow of one recall call:
//!
//! 1. Entry resolution (Section 8.1 of the database spec, steps 1 and 2
//!    ONLY — no vector search, that is Phase 2): the Person identifiers
//!    of the senders and of the reply targets, plus exact alias matches
//!    of the candidate terms. An unknown term yields no entry (step 4:
//!    no fuzzy scans, Rule R5).
//! 2. Neighbor fetch (Section 8.2): the valid direct neighbors of every
//!    entry node, one hop, deduped by `NeighborEdge::edge_id`.
//! 3. Deduplication (Section 9.3): a candidate with a row in
//!    `injected_memories` is dropped.
//! 4. Zero candidates: the relevance gate is NEVER called. The fixed
//!    cost of Section 9.1 applies to wakes with candidates only.
//! 5. The relevance gate (Section 9.2): a cheap-model call over the
//!    `gate` purpose endpoint. Conservative by default. ANY failure of
//!    the gate (LLM failure, malformed output) means "inject nothing";
//!    a wake never fails on a recall-gate error.
//! 6. Post-validation in plain Rust (never trust the model, the same
//!    principle as `validate.rs`): in-range indices only, deduped, at
//!    most `injection_cap` (the Section 9.2 hard cap, default 5 through
//!    the config key `recall_injection_cap`).
//! 7. Render (Section 9.4): exactly one `PlannedInjection` of the form
//!    "I remember: ...". An empty or "I remember nothing" injection is
//!    FORBIDDEN (Section 9.2): an empty selection yields no
//!    `PlannedInjection` at all.
//!
//! Degradation policy: a graph error of one entry (alias lookup or
//! neighbor fetch) logs a warning and skips that entry; the wake
//! continues with the remaining entries. Store failures are unexpected
//! internal failures and propagate as `CoreError`.
//!
//! ## The candidate-term tokenizer (Phase 1, documented limits)
//!
//! The tokenizer is a pure deterministic function. NO LLM term
//! extraction (Phase 1). Rules: split on whitespace and punctuation
//! (Unicode alphanumeric runs survive — CJK characters are
//! alphanumeric), normalize every token with the Section 7.1 rules
//! (`tamako_memory::identifiers::normalize`: NFKC, lowercase), drop
//! empty tokens, tokens shorter than 2 chars after normalization, and a
//! small built-in English stopword list. Dedup keeps the first
//! occurrence order. At most [`MAX_CANDIDATE_TERMS`] terms per wake.
//!
//! Documented limits (accepted recall loss of Phase 1):
//!
//! - NO multi-word terms: "San Francisco" becomes two terms.
//! - NO synonyms and NO cross-language merging (Section 7.1 CAUTION):
//!   "tama" and "たま" stay distinct terms.
//! - Single-character tokens are dropped, CJK characters included. A
//!   one-character CJK name is not a term.
//! - The stopword list is English only.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

use rig::completion::Message;

use tamako_core::actor::CoreError;
use tamako_core::wake::{GateMessage, PlannedInjection, RecallOutcome, RecallProvider};
use tamako_memory::identifiers::{alias_id, normalize, person_id};
use tamako_memory::MemoryBackend;
use tamako_store::{Store, StoreError};
use time::macros::format_description;

use crate::endpoint::{EndpointClient, EndpointConfig};
use crate::extract::AgentError;

/// The default max tokens of the relevance-gate response. The output is
/// one small JSON object (two fields); 2048 tokens is a generous bound
/// (same bound as the participation gate).
pub const RECALL_DEFAULT_MAX_TOKENS: u64 = 2048;

/// The bound of the candidate-term list of one wake. The terms feed the
/// entry resolution; 20 terms is a documented bound that keeps the
/// recall cheap (Section 9.1).
pub const MAX_CANDIDATE_TERMS: usize = 20;

/// The bound of the candidate list presented to the relevance gate
/// (Section 9.2). A larger candidate set is truncated: the FIRST
/// candidates per entry order survive. Entry order is senders first,
/// then reply targets, then alias terms — the persons of the wake rank
/// above the term matches. A documented Phase 1 choice.
pub const MAX_PRESENTED_CANDIDATES: usize = 40;

/// The built-in English stopword list of the tokenizer. Phase 1:
/// English only, a closed const set, no per-language lists.
const STOPWORDS: &[&str] = &[
    "the", "a", "an", "is", "are", "was", "were", "to", "of", "in", "on", "and", "or", "it",
    "this", "that", "i", "you", "we", "they", "he", "she", "do", "does", "did", "not", "no", "yes",
    "what", "who", "how", "when", "where", "why", "can", "could", "would", "should", "will", "for",
    "with", "at", "by", "from", "as", "be", "been", "have", "has", "had", "so", "but", "if",
    "then", "just", "me", "my", "your", "our", "their", "its", "about", "there", "here", "ok",
    "okay", "yeah", "hi", "hello", "hey", "thanks", "thank",
];

/// One candidate memory presented to the relevance gate
/// (Section 9.2): the edge text plus its validity start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecallCandidate {
    /// The dedup key (NeighborEdge::edge_id).
    pub edge_id: String,
    pub edge_text: String,
    pub valid_at: time::OffsetDateTime,
}

/// The candidate terms of one wake: normalized tokens of the new
/// message texts, deduped in first-occurrence order, at most
/// [`MAX_CANDIDATE_TERMS`] entries. Refer to the module docs for the
/// tokenizer rules and the documented Phase 1 limits.
pub fn candidate_terms(texts: &[&str]) -> Vec<String> {
    let mut terms = Vec::new();
    let mut seen = HashSet::new();
    let mut token = String::new();
    for text in texts {
        for ch in text.chars() {
            if ch.is_alphanumeric() {
                token.push(ch);
            } else {
                push_term(&mut terms, &mut seen, &token);
                token.clear();
            }
        }
        push_term(&mut terms, &mut seen, &token);
        token.clear();
        if terms.len() >= MAX_CANDIDATE_TERMS {
            return terms;
        }
    }
    terms
}

/// Normalizes one raw token and appends it when it survives the drop
/// rules (module docs): Section 7.1 normalization, the 2-char minimum,
/// the stopword list. Dedup keeps the first occurrence.
fn push_term(terms: &mut Vec<String>, seen: &mut HashSet<String>, raw: &str) {
    if raw.is_empty() || terms.len() >= MAX_CANDIDATE_TERMS {
        return;
    }
    let normalized = normalize(raw);
    // The 2-char minimum counts chars AFTER normalization. Single CJK
    // characters are dropped too (documented Phase 1 limit).
    if normalized.chars().count() < 2 {
        return;
    }
    if STOPWORDS.contains(&normalized.as_str()) {
        return;
    }
    if seen.insert(normalized.clone()) {
        terms.push(normalized);
    }
}

/// The input of the relevance gate (Section 9.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelevanceInput {
    pub new_messages: Vec<GateMessage>,
    pub candidates: Vec<RecallCandidate>,
}

/// The relevance gate of Section 9.2: selects the candidate
/// indices to inject. Conservative by default. Implementations:
/// `RigRelevanceGate` (live) and `ScriptedRelevanceGate` (tests).
///
/// The trait speaks 0-based indices into `RelevanceInput.candidates`.
/// `RigRelevanceGate` converts the 1-based wire numbers.
pub trait RelevanceGate: Send + Sync {
    fn select<'a>(
        &'a self,
        input: &'a RelevanceInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>,
    >;
}

/// The system preamble of the relevance-gate call (Section 9.2).
pub const RECALL_PREAMBLE: &str = "\
You select memories for a group pet. The pet is a member of a group chat. Before the pet speaks, it recalls memories of the group.

Rules:
1. Read the new group messages. Read the candidate memories.
2. Select a candidate memory ONLY when omitting it would materially reduce the quality of the reply or of the participation decision.
3. When in doubt, select nothing. Selecting nothing is the normal case. A memory that is loosely related is not enough.
4. Give the 1-based numbers of the selected memories in descending relevance. Select at most 5 memories.
5. Output only the JSON object of the required schema. Give one short reason. No commentary.";

/// The date format of the candidate list: YYYY-MM-DD, UTC.
const YMD_FORMAT: &[time::format_description::FormatItem<'_>] =
    format_description!("[year]-[month]-[day]");

/// Renders the user prompt of the relevance-gate call (Section 9.2):
/// one line per new message as `{row_id} {content}` (the same shape as
/// the participation gate), then a numbered candidate list
/// `1. {edge_text} (since {YYYY-MM-DD of valid_at, UTC})`.
pub fn render_recall_prompt(input: &RelevanceInput) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "New messages (id and speaker-labeled content):");
    for message in &input.new_messages {
        let _ = writeln!(out, "{} {}", message.row_id, message.content);
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "Candidate memories (number, text, validity start):");
    for (index, candidate) in input.candidates.iter().enumerate() {
        let since = candidate
            .valid_at
            .format(&YMD_FORMAT)
            .unwrap_or_else(|_| format!("{:?}", candidate.valid_at));
        let _ = writeln!(
            out,
            "{}. {} (since {})",
            index + 1,
            candidate.edge_text,
            since
        );
    }
    out
}

/// The structured relevance-gate output (Section 9.2). The LLM produces
/// exactly this shape (rig output_schema). The doc comments are part of
/// the prompt: schemars turns them into schema descriptions on the
/// Anthropic structured-output path.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct RecallSelection {
    /// The 1-based numbers of the candidate memories to inject,
    /// in descending relevance. Empty when nothing is worth
    /// injecting (the normal case).
    pub selected: Vec<u32>,
    /// One short reason.
    pub reason: String,
}

/// Converts the 1-based wire numbers of a `RecallSelection` to the
/// 0-based indices of the trait contract. A wire number of 0 is
/// dropped here; an out-of-range index survives and the caller's
/// post-validation drops it (never trust the model).
fn zero_based_indices(selection: &RecallSelection) -> Vec<usize> {
    selection
        .selected
        .iter()
        .filter_map(|&number| {
            number
                .checked_sub(1)
                .and_then(|index| usize::try_from(index).ok())
        })
        .collect()
}

/// The live relevance gate: one completion call on the cheap
/// `gate_model` endpoint (specs.md Sections 9.1, 9.2, and 13) with the
/// `RecallSelection` schema. The recall worker uses the cheap model,
/// never the main model.
pub struct RigRelevanceGate {
    client: EndpointClient,
    max_tokens: u64,
}

// The rig model handles do not implement Debug. A manual impl keeps
// RigRelevanceGate printable in test failures and logs (same pattern as
// RigGate).
impl std::fmt::Debug for RigRelevanceGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigRelevanceGate")
            .field("client", &self.client)
            .field("max_tokens", &self.max_tokens)
            .finish_non_exhaustive()
    }
}

impl RigRelevanceGate {
    /// Builds the relevance gate from an endpoint client.
    pub fn new(client: EndpointClient, max_tokens: u64) -> Self {
        RigRelevanceGate { client, max_tokens }
    }

    /// Builds the relevance gate for one resolved endpoint (the `gate`
    /// purpose, specs.md Section 13). Returns `AgentError::ProviderConfig`
    /// when the family API key is missing.
    pub fn from_endpoint(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        Ok(RigRelevanceGate::new(
            EndpointClient::build(endpoint)?,
            RECALL_DEFAULT_MAX_TOKENS,
        ))
    }
}

impl RelevanceGate for RigRelevanceGate {
    fn select<'a>(
        &'a self,
        input: &'a RelevanceInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let text = self
                .client
                .complete(
                    // The preamble becomes the system message.
                    Some(RECALL_PREAMBLE.to_string()),
                    vec![Message::user(render_recall_prompt(input))],
                    // The output schema maps to native structured output.
                    Some(schemars::schema_for!(RecallSelection)),
                    self.max_tokens,
                )
                .await?;
            let selection = serde_json::from_str::<RecallSelection>(&text)
                // A malformed output is an AgentError; the caller maps
                // ANY gate failure to "inject nothing" (Section 9.2).
                .map_err(|error| {
                    AgentError::Extraction(format!("invalid recall gate JSON: {error}"))
                })?;
            Ok(zero_based_indices(&selection))
        })
    }
}

/// The response mode of `ScriptedRelevanceGate`.
enum ScriptedGateMode {
    /// Pops the next selection per call (FIFO). An exhausted queue
    /// selects nothing.
    Selections(VecDeque<Vec<usize>>),
    /// Every call fails with `AgentError::Extraction`.
    Failing(String),
}

/// A scripted relevance gate for tests (same pattern as
/// `ScriptedGate`). Two modes:
///
/// - `ScriptedRelevanceGate::with_selections(vec_of_selections)`: pops
///   the next 0-based selection per call (FIFO; when exhausted, selects
///   nothing);
/// - `ScriptedRelevanceGate::failing(message)`: every call fails.
///
/// Every `RelevanceInput` is recorded for assertions (`inputs()`,
/// `call_count()`).
pub struct ScriptedRelevanceGate {
    mode: Mutex<ScriptedGateMode>,
    inputs: Mutex<Vec<RelevanceInput>>,
}

impl ScriptedRelevanceGate {
    /// A scripted gate that answers with the given selections in order.
    /// The selections speak 0-based indices (the trait contract).
    pub fn with_selections(selections: Vec<Vec<usize>>) -> Self {
        ScriptedRelevanceGate {
            mode: Mutex::new(ScriptedGateMode::Selections(selections.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// A scripted gate whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedRelevanceGate {
            mode: Mutex::new(ScriptedGateMode::Failing(message.into())),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// Every input the gate received, in call order.
    pub fn inputs(&self) -> Vec<RelevanceInput> {
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The number of `select` calls so far. Tests assert "the gate was
    /// never called" with this accessor.
    pub fn call_count(&self) -> usize {
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl RelevanceGate for ScriptedRelevanceGate {
    fn select<'a>(
        &'a self,
        input: &'a RelevanceInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>,
    > {
        // Lock, record, and decide synchronously; the future only
        // carries the result. A poisoned mutex is recovered; the
        // recorded inputs stay valid (same policy as ScriptedGate).
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(input.clone());
        let result = {
            let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *mode {
                ScriptedGateMode::Selections(selections) => {
                    Ok(selections.pop_front().unwrap_or_default())
                }
                ScriptedGateMode::Failing(message) => Err(AgentError::Extraction(message.clone())),
            }
        };
        Box::pin(async move { result })
    }
}

/// The shallow recall worker (specs.md Section 9 step 2). Refer to the
/// module docs for the flow and the degradation policy.
pub struct ShallowRecall<M: MemoryBackend, G: RelevanceGate> {
    store: Arc<Store>,
    memory: Arc<M>,
    gate: G,
    /// The Section 9.2 hard cap of injected memories per wake (the
    /// config key `recall_injection_cap`, default 5).
    injection_cap: u32,
}

impl<M: MemoryBackend, G: RelevanceGate> ShallowRecall<M, G> {
    pub fn new(store: Arc<Store>, memory: Arc<M>, gate: G, injection_cap: u32) -> Self {
        ShallowRecall {
            store,
            memory,
            gate,
            injection_cap,
        }
    }

    /// Runs one store call inside `tokio::task::spawn_blocking`
    /// (AGENT.md Section 6.2 — the same pattern the pipeline uses).
    async fn run_store<T>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, CoreError>
    where
        T: Send + 'static,
    {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .map_err(|error| CoreError::Join(error.to_string()))?
            .map_err(CoreError::Store)
    }

    /// Entry resolution, Section 8.1 steps 1 and 2 of the database
    /// spec. The entry order is senders first, then reply targets,
    /// then alias terms (see `MAX_PRESENTED_CANDIDATES`). Deduped,
    /// first occurrence wins.
    async fn resolve_entries(
        &self,
        chat_id: &str,
        new_messages: &[GateMessage],
    ) -> Result<Vec<String>, CoreError> {
        let mut entry_ids = Vec::new();
        let mut seen = HashSet::new();

        // Step 1a: the Person identifier of every sender.
        for message in new_messages {
            push_unique(&mut entry_ids, &mut seen, person_id(&message.sender_id));
        }

        // Step 1b: the Person identifier of every reply target. One
        // store lookup call for the distinct reply target ids of the
        // wake (batched: one spawn_blocking round trip).
        let mut reply_target_ids = Vec::new();
        let mut seen_reply_ids = HashSet::new();
        for message in new_messages {
            if let Some(reply_id) = &message.reply_to_platform_msg_id {
                if seen_reply_ids.insert(reply_id.clone()) {
                    reply_target_ids.push(reply_id.clone());
                }
            }
        }
        if !reply_target_ids.is_empty() {
            let chat_id_owned = chat_id.to_string();
            let senders = self
                .run_store(move |store| {
                    let mut senders = Vec::with_capacity(reply_target_ids.len());
                    for reply_id in &reply_target_ids {
                        senders
                            .push(store.find_sender_by_platform_msg_id(&chat_id_owned, reply_id)?);
                    }
                    Ok(senders)
                })
                .await?;
            for found in senders.into_iter().flatten() {
                push_unique(&mut entry_ids, &mut seen, person_id(&found.0));
            }
        }

        // Step 2: exact alias match per candidate term (Rule R5: enter
        // through the deterministic alias identifier). No fuzzy scans
        // (step 4).
        let texts: Vec<&str> = new_messages
            .iter()
            .map(|message| message.text.as_str())
            .collect();
        for term in candidate_terms(&texts) {
            let term_alias_id = alias_id(&term);
            match self.memory.alias_targets(chat_id, &term_alias_id).await {
                Ok(targets) => match targets.as_slice() {
                    // Exactly one target: the target node is the entry.
                    [target] => push_unique(&mut entry_ids, &mut seen, target.node_id.clone()),
                    // Two or more targets: the entry is the ALIAS node
                    // itself. Its direct neighbors are the known_as /
                    // also_known_as edges to the candidate entities, so
                    // the relevance gate still sees them. Mirrors the
                    // write-path ambiguity fallback of Section 7.4
                    // step 4.
                    [] => {}
                    _ => push_unique(&mut entry_ids, &mut seen, term_alias_id),
                },
                Err(error) => {
                    // Degrade, never fail the wake on a graph error:
                    // log and skip the term (module docs).
                    tracing::warn!(
                        term = %term,
                        error = %error,
                        "alias lookup failed; skipping the term"
                    );
                }
            }
        }
        Ok(entry_ids)
    }

    /// The recall flow (module docs, steps 1-7).
    async fn recall_inner(
        &self,
        chat_id: &str,
        new_messages: &[GateMessage],
    ) -> Result<RecallOutcome, CoreError> {
        // Steps 1-2: entry resolution + neighbor fetch. Candidates are
        // deduped by edge_id; the first occurrence wins.
        let entry_ids = self.resolve_entries(chat_id, new_messages).await?;
        let mut candidates: Vec<RecallCandidate> = Vec::new();
        let mut seen_edge_ids = HashSet::new();
        for entry_id in &entry_ids {
            match self.memory.neighbors(chat_id, entry_id).await {
                Ok(edges) => {
                    for edge in edges {
                        let edge_id = edge.edge_id();
                        if seen_edge_ids.insert(edge_id.clone()) {
                            candidates.push(RecallCandidate {
                                edge_id,
                                edge_text: edge.edge_text,
                                valid_at: edge.valid_at,
                            });
                        }
                    }
                }
                Err(error) => {
                    // Degrade, never fail the wake on a graph error:
                    // log and skip the entry (module docs).
                    tracing::warn!(
                        entry_id = %entry_id,
                        error = %error,
                        "neighbor fetch failed; skipping the entry"
                    );
                }
            }
        }

        // Step 3 (Section 9.3): drop the candidates that already have a
        // row in injected_memories. The table holds exactly the current
        // chunk — the M2 prune removes the old chunks at digest time.
        let chat_id_owned = chat_id.to_string();
        let injected = self
            .run_store(move |store| store.list_injected_memories(&chat_id_owned))
            .await?;
        let injected_edge_ids: HashSet<&str> =
            injected.iter().map(|row| row.edge_id.as_str()).collect();
        candidates.retain(|candidate| !injected_edge_ids.contains(candidate.edge_id.as_str()));

        // Step 4: zero candidates — the relevance gate is NEVER called.
        // The fixed cost of Section 9.1 applies to wakes with
        // candidates only.
        if candidates.is_empty() {
            return Ok(RecallOutcome::default());
        }

        // Step 5: the relevance gate (Section 9.2). The candidate list
        // is bounded; truncation keeps the FIRST candidates per entry
        // order (see MAX_PRESENTED_CANDIDATES). ANY gate failure means
        // "inject nothing" — never fail the wake.
        candidates.truncate(MAX_PRESENTED_CANDIDATES);
        let input = RelevanceInput {
            new_messages: new_messages.to_vec(),
            candidates,
        };
        let selection = match self.gate.select(&input).await {
            Ok(selection) => selection,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "the relevance gate failed; injecting nothing"
                );
                return Ok(RecallOutcome::default());
            }
        };

        // Step 6: post-validation in plain Rust (never trust the model,
        // same principle as validate.rs): in-range indices only,
        // deduped, at most injection_cap (the Section 9.2 hard cap).
        let mut chosen: Vec<&RecallCandidate> = Vec::new();
        let mut picked = HashSet::new();
        for index in selection {
            if index < input.candidates.len() && picked.insert(index) {
                chosen.push(&input.candidates[index]);
                if chosen.len() >= self.injection_cap as usize {
                    break;
                }
            }
        }
        // An empty or "I remember nothing" injection is FORBIDDEN
        // (Section 9.2): an empty selection yields no PlannedInjection.
        if chosen.is_empty() {
            return Ok(RecallOutcome::default());
        }

        // Step 7 (Section 9.4): exactly one PlannedInjection. The edge
        // texts are single sentences.
        let content = format!(
            "I remember: {}",
            chosen
                .iter()
                .map(|candidate| candidate.edge_text.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        );
        let edge_ids = chosen
            .iter()
            .map(|candidate| candidate.edge_id.clone())
            .collect();
        Ok(RecallOutcome {
            injections: vec![PlannedInjection { edge_ids, content }],
        })
    }
}

/// Appends an id to an ordered dedup set (first occurrence wins).
fn push_unique(ids: &mut Vec<String>, seen: &mut HashSet<String>, id: String) {
    if seen.insert(id.clone()) {
        ids.push(id);
    }
}

impl<M: MemoryBackend, G: RelevanceGate> RecallProvider for ShallowRecall<M, G> {
    fn recall<'a>(
        &'a self,
        chat_id: &'a str,
        new_messages: &'a [GateMessage],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<RecallOutcome, CoreError>> + Send + 'a>,
    > {
        Box::pin(async move { self.recall_inner(chat_id, new_messages).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tamako_memory::identifiers::concept_id;
    use tamako_memory::{LbugBackend, MemoryBatch, MemoryEdge, MemoryNode, NodeType};
    use time::macros::datetime;
    use time::OffsetDateTime;

    const CHAT: &str = "recall_test";
    const NOW: OffsetDateTime = datetime!(2026-08-07 10:00 UTC);

    fn gate_message(row_id: i64, sender_id: &str, text: &str) -> GateMessage {
        GateMessage {
            row_id,
            platform_msg_id: format!("m{row_id}"),
            content: format!("[Sender {sender_id} 13:01] {text}"),
            sender_id: sender_id.to_string(),
            reply_to_platform_msg_id: None,
            text: text.to_string(),
        }
    }

    // --- Tokenizer ---

    #[test]
    fn the_tokenizer_splits_on_whitespace_and_punctuation() {
        let terms = candidate_terms(&["hello, world! it's a test-case: foo/bar"]);
        // "it's" splits into "it" (stopword) and "s" (too short);
        // "test-case" splits into two tokens. "hello" is a stopword.
        assert_eq!(terms, vec!["world", "test", "case", "foo", "bar"]);
    }

    #[test]
    fn the_tokenizer_normalizes_nfkc_and_lowercase() {
        // Full-width characters fold to ASCII under NFKC; case folds.
        assert_eq!(candidate_terms(&["Ｔａｍａｋｏ TAMAKO"]), vec!["tamako"]);
        assert_eq!(candidate_terms(&["Espresso"]), vec!["espresso"]);
    }

    #[test]
    fn the_tokenizer_drops_stopwords_and_short_tokens() {
        assert_eq!(
            candidate_terms(&["the a an is to of in on"]),
            Vec::<String>::new()
        );
        assert_eq!(candidate_terms(&["I am ok"]), vec!["am"]);
        // Single-character tokens go even when they are no stopwords.
        let terms = candidate_terms(&["x yz"]);
        assert_eq!(terms, vec!["yz"]);
    }

    #[test]
    fn the_tokenizer_keeps_multi_character_cjk_tokens() {
        // CJK characters are alphanumeric and survive.
        assert_eq!(candidate_terms(&["玉子 は 寿司"]), vec!["玉子", "寿司"]);
        // A single CJK character is dropped (documented Phase 1 limit:
        // the 2-char minimum counts chars after normalization).
        assert_eq!(candidate_terms(&["猫"]), Vec::<String>::new());
    }

    #[test]
    fn the_tokenizer_dedups_and_keeps_first_occurrence_order() {
        let terms = candidate_terms(&["beta alpha beta gamma alpha"]);
        assert_eq!(terms, vec!["beta", "alpha", "gamma"]);
        // Across texts: the order follows the text order.
        let terms = candidate_terms(&["zulu yankee", "alpha zulu"]);
        assert_eq!(terms, vec!["zulu", "yankee", "alpha"]);
    }

    #[test]
    fn the_tokenizer_caps_at_twenty_terms() {
        let text: Vec<String> = (0..30).map(|index| format!("term{index:02}")).collect();
        let joined = text.join(" ");
        let terms = candidate_terms(&[joined.as_str()]);
        assert_eq!(terms.len(), MAX_CANDIDATE_TERMS);
        assert_eq!(terms[0], "term00");
        assert_eq!(terms[19], "term19");
    }

    // --- Prompt and preamble ---

    fn sample_input() -> RelevanceInput {
        RelevanceInput {
            new_messages: vec![
                gate_message(41, "u1", "has anyone tried the new cafe?"),
                gate_message(42, "u2", "the espresso is great"),
            ],
            candidates: vec![
                RecallCandidate {
                    edge_id: "edge-1".to_string(),
                    edge_text: "Alice likes espresso.".to_string(),
                    valid_at: NOW,
                },
                RecallCandidate {
                    edge_id: "edge-2".to_string(),
                    edge_text: "Bob plays go.".to_string(),
                    valid_at: datetime!(2025-12-31 23:00 UTC),
                },
            ],
        }
    }

    #[test]
    fn the_prompt_renders_messages_and_numbered_candidates() {
        let prompt = render_recall_prompt(&sample_input());
        // One line per new message: `{row_id} {content}`.
        assert!(prompt.contains("41 [Sender u1 13:01] has anyone tried the new cafe?"));
        assert!(prompt.contains("42 [Sender u2 13:01] the espresso is great"));
        // The numbered candidate list: 1-based, YYYY-MM-DD of valid_at.
        assert!(prompt.contains("1. Alice likes espresso. (since 2026-08-07)"));
        assert!(prompt.contains("2. Bob plays go. (since 2025-12-31)"));
    }

    #[test]
    fn the_preamble_states_the_conservative_rules() {
        // Section 9.2: conservative by default.
        assert!(RECALL_PREAMBLE.contains("materially reduce the quality"));
        assert!(RECALL_PREAMBLE.contains("select nothing"));
        assert!(RECALL_PREAMBLE.contains("the normal case"));
        assert!(RECALL_PREAMBLE.contains("the JSON object of the required schema"));
    }

    #[test]
    fn the_recall_selection_round_trips_through_json_and_schema() {
        let selection = RecallSelection {
            selected: vec![2, 1],
            reason: "the cafe topic".to_string(),
        };
        let json = serde_json::to_string(&selection).expect("to json");
        let back: RecallSelection = serde_json::from_str(&json).expect("from json");
        assert_eq!(back, selection);
        let schema = schemars::schema_for!(RecallSelection);
        let schema_json = serde_json::to_string(&schema).expect("schema to json");
        assert!(schema_json.contains("selected"));
        assert!(schema_json.contains("reason"));
    }

    #[test]
    fn zero_based_indices_convert_the_wire_numbers() {
        let selection = RecallSelection {
            selected: vec![2, 1, 0],
            reason: String::new(),
        };
        // Wire numbers are 1-based; a wire 0 is dropped. Out-of-range
        // wire numbers survive: the recall post-validation drops them.
        assert_eq!(zero_based_indices(&selection), vec![1, 0]);
        let selection = RecallSelection {
            selected: vec![99],
            reason: String::new(),
        };
        assert_eq!(zero_based_indices(&selection), vec![98]);
    }

    #[tokio::test]
    async fn the_scripted_gate_pops_fifo_then_selects_nothing() {
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0, 1]]);
        let first = gate.select(&sample_input()).await.expect("first");
        assert_eq!(first, vec![0, 1]);
        let second = gate.select(&sample_input()).await.expect("second");
        assert_eq!(second, Vec::<usize>::new());
        assert_eq!(gate.call_count(), 2);
        assert_eq!(gate.inputs()[0], sample_input());
    }

    #[tokio::test]
    async fn the_scripted_gate_failing_mode_errors_and_records_inputs() {
        let gate = ScriptedRelevanceGate::failing("boom");
        for _ in 0..2 {
            match gate.select(&sample_input()).await {
                Err(AgentError::Extraction(message)) => assert_eq!(message, "boom"),
                other => panic!("expected Extraction error, got {other:?}"),
            }
        }
        assert_eq!(gate.call_count(), 2);
    }

    #[test]
    fn from_endpoint_without_an_api_key_is_a_provider_config_error() {
        // The test environment must not carry a key for this assertion.
        use crate::endpoint::env_lock::ENV_LOCK;
        let _lock = ENV_LOCK.lock().unwrap();
        let saved_key = std::env::var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR).ok();
        let saved_family = std::env::var(crate::endpoint::LLM_API_ENV_VAR).ok();
        std::env::remove_var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR);
        std::env::remove_var(crate::endpoint::LLM_API_ENV_VAR);
        let endpoint = EndpointConfig {
            api: crate::endpoint::LlmApi::AnthropicCompatible,
            base_url: None,
            model: "claude-haiku-4-5".to_string(),
        };
        let result = RigRelevanceGate::from_endpoint(&endpoint);
        if let Some(key) = saved_key {
            std::env::set_var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR, key);
        }
        if let Some(value) = saved_family {
            std::env::set_var(crate::endpoint::LLM_API_ENV_VAR, value);
        }
        match result {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig error, got {other:?}"),
        }
    }

    // --- Integration: the full recall flow over the real backend and
    // the real store (the seeding style of resolve.rs) ---

    async fn backend() -> (tempfile::TempDir, Arc<Store>, Arc<LbugBackend>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(Store::new(dir.path().join("store")));
        let memory = Arc::new(LbugBackend::new(dir.path().join("memory")));
        memory.ensure_schema(CHAT).await.expect("schema");
        (dir, store, memory)
    }

    fn person_node(user_id: &str, name: &str) -> MemoryNode {
        MemoryNode {
            id: person_id(user_id),
            name: name.to_string(),
            node_type: NodeType::Person,
            created_at: NOW,
            updated_at: NOW,
            properties: None,
        }
    }

    fn concept_node(name: &str) -> MemoryNode {
        MemoryNode {
            id: concept_id(name),
            name: name.to_string(),
            node_type: NodeType::Concept,
            created_at: NOW,
            updated_at: NOW,
            properties: None,
        }
    }

    fn alias_node_seeded(surface_form: &str) -> MemoryNode {
        MemoryNode {
            id: alias_id(surface_form),
            name: surface_form.to_string(),
            node_type: NodeType::Alias,
            created_at: NOW,
            updated_at: NOW,
            properties: None,
        }
    }

    fn fact_edge(source_id: &str, target_id: &str, relationship: &str, text: &str) -> MemoryEdge {
        MemoryEdge {
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            relationship_name: relationship.to_string(),
            valid_at: NOW,
            invalid_at: None,
            edge_text: text.to_string(),
            created_at: NOW,
            updated_at: NOW,
            properties: None,
        }
    }

    async fn seed(memory: &LbugBackend, nodes: Vec<MemoryNode>, edges: Vec<MemoryEdge>) {
        let batch = MemoryBatch {
            batch_id: "seed".to_string(),
            nodes,
            edges,
        };
        memory.upsert_batch(CHAT, &batch).await.expect("seed");
    }

    /// Seeds one person with a fact edge to a concept.
    async fn seed_person_fact(memory: &LbugBackend, user_id: &str, name: &str, text: &str) {
        let person = person_node(user_id, name);
        let concept = concept_node(&format!("concept-of-{user_id}"));
        let edge = fact_edge(&person.id, &concept.id, "related_to", text);
        seed(memory, vec![person, concept], vec![edge]).await;
    }

    /// Seeds a person with an alias and the known_as edge (Section 7.4
    /// step 2 test data).
    async fn seed_person_alias(memory: &LbugBackend, user_id: &str, name: &str, surface: &str) {
        let person = person_node(user_id, name);
        let alias = alias_node_seeded(surface);
        let known_as = fact_edge(
            &person.id,
            &alias.id,
            "known_as",
            &format!("{surface} is a surface form of {name}."),
        );
        seed(memory, vec![person, alias], vec![known_as]).await;
    }

    fn presented_texts(gate: &ScriptedRelevanceGate) -> Vec<String> {
        gate.inputs()
            .first()
            .expect("the gate was called")
            .candidates
            .iter()
            .map(|candidate| candidate.edge_text.clone())
            .collect()
    }

    #[tokio::test]
    async fn an_alias_term_with_one_target_resolves_to_the_target() {
        // Section 8.1 step 2: an exact alias match with exactly one
        // target enters through the target node.
        let (_dir, store, memory) = backend().await;
        seed_person_alias(&memory, "u1001", "Tama", "tama").await;
        seed_person_fact(&memory, "u1001", "Tama", "Tama likes espresso.").await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u9999", "has tama been here?")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        // The fact of the alias target is a candidate. The known_as
        // edge of the target is a candidate too (one hop, Section 8.2).
        let texts = presented_texts(&recall.gate);
        assert!(texts.contains(&"Tama likes espresso.".to_string()));
        assert!(texts.contains(&"tama is a surface form of Tama.".to_string()));
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn an_alias_with_two_targets_resolves_to_the_alias_node() {
        // Section 8.1 step 2 with ambiguity: the entry is the ALIAS
        // node itself. Its known_as edges appear as candidates.
        let (_dir, store, memory) = backend().await;
        seed_person_alias(&memory, "u1001", "Tama One", "tama").await;
        seed_person_alias(&memory, "u2002", "Tama Two", "tama").await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u9999", "tama?")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        let texts = presented_texts(&recall.gate);
        assert!(texts.contains(&"tama is a surface form of Tama One.".to_string()));
        assert!(texts.contains(&"tama is a surface form of Tama Two.".to_string()));
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn an_unknown_term_yields_no_entry_and_the_gate_is_never_called() {
        // Section 8.1 step 4 / Rule R5: no fuzzy scans. An unknown term
        // yields no entry; zero candidates skip the relevance gate.
        let (_dir, store, memory) = backend().await;
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u9999", "xyzzy plugh")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome, RecallOutcome::default());
        // Section 9.1: the fixed-cost rule applies to wakes with
        // candidates only. The gate was never called.
        assert_eq!(recall.gate.call_count(), 0);
    }

    #[tokio::test]
    async fn the_sender_person_id_is_an_entry_without_an_alias() {
        // Section 8.1 step 1: the sender of a new message resolves to a
        // Person entry; no alias term is needed.
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        // Stopword-only text: no alias terms at all.
        let messages = vec![gate_message(1, "u42", "ok ok thanks")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(
            presented_texts(&recall.gate),
            vec!["Alice likes espresso.".to_string()]
        );
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn a_reply_resolves_the_targets_sender_through_the_store() {
        // Section 8.1 step 1: a reply resolves to the Person entry of
        // the reply target's sender (the store lookup of M5).
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u777", "Carol", "Carol plays go.").await;
        // The raw-log row of the reply target: sender u777, a sender
        // that differs from the replier.
        store
            .insert_message(
                CHAT,
                &tamako_store::NewMessage {
                    platform_msg_id: "m-target".to_string(),
                    direction: tamako_store::Direction::Inbound,
                    event_type: tamako_store::EventType::Message,
                    timestamp: NOW,
                    sender_id: "u777".to_string(),
                    sender_display_name: "Carol".to_string(),
                    text: "anyone up for go?".to_string(),
                    reply_to_platform_msg_id: None,
                    mentions_bot: false,
                    is_reply_to_bot: false,
                },
            )
            .expect("insert the reply target row");

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let mut message = gate_message(2, "u888", "ok");
        message.reply_to_platform_msg_id = Some("m-target".to_string());
        let outcome = recall.recall(CHAT, &[message]).await.expect("recall");

        assert_eq!(
            presented_texts(&recall.gate),
            vec!["Carol plays go.".to_string()]
        );
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn a_selection_yields_exactly_one_planned_injection() {
        // Section 9.4: one "I remember: ..." injection per wake.
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome.injections.len(), 1);
        let injection = &outcome.injections[0];
        assert_eq!(injection.content, "I remember: Alice likes espresso.");
        let expected_edge_id = recall.gate.inputs()[0].candidates[0].edge_id.clone();
        assert_eq!(injection.edge_ids, vec![expected_edge_id]);
    }

    #[tokio::test]
    async fn an_empty_selection_yields_no_injection() {
        // Section 9.2: an empty injection is forbidden; an empty
        // selection yields no PlannedInjection at all.
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome, RecallOutcome::default());
        assert_eq!(recall.gate.call_count(), 1);
    }

    #[tokio::test]
    async fn a_failing_gate_yields_no_injection_and_no_panic() {
        // Section 9.2/9.1: an LLM failure means "inject nothing"; the
        // wake never fails on a recall-gate error.
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        let gate = ScriptedRelevanceGate::failing("the model is down");
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome, RecallOutcome::default());
        assert_eq!(recall.gate.call_count(), 1);
    }

    #[tokio::test]
    async fn out_of_range_indices_are_dropped() {
        // Post-validation in plain Rust (never trust the model).
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        // Only out-of-range indices: no injection.
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![99, 7]]);
        let recall = ShallowRecall::new(store.clone(), memory.clone(), gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");
        assert_eq!(outcome, RecallOutcome::default());

        // A mix: the in-range index wins, the out-of-range index goes.
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![99, 0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");
        assert_eq!(outcome.injections.len(), 1);
        assert_eq!(
            outcome.injections[0].content,
            "I remember: Alice likes espresso."
        );
    }

    #[tokio::test]
    async fn duplicate_indices_are_deduped() {
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0, 0, 0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome.injections.len(), 1);
        assert_eq!(outcome.injections[0].edge_ids.len(), 1);
        assert_eq!(
            outcome.injections[0].content,
            "I remember: Alice likes espresso."
        );
    }

    #[tokio::test]
    async fn the_injection_cap_is_enforced() {
        // Section 9.2 hard cap: the cap arrives through the constructor
        // (the config key recall_injection_cap). Cap 2, the gate
        // selects 3, two are injected — the first two in the gate's
        // descending-relevance order.
        let (_dir, store, memory) = backend().await;
        let person = person_node("u42", "Alice");
        let mut nodes = vec![person.clone()];
        let mut edges = Vec::new();
        for (index, text) in [
            "Alice likes espresso.",
            "Alice plays go.",
            "Alice reads books.",
        ]
        .iter()
        .enumerate()
        {
            let concept = concept_node(&format!("hobby-{index}"));
            edges.push(fact_edge(&person.id, &concept.id, "related_to", text));
            nodes.push(concept);
        }
        seed(&memory, nodes, edges).await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0, 1, 2]]);
        let recall = ShallowRecall::new(store, memory, gate, 2);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome.injections.len(), 1);
        let injection = &outcome.injections[0];
        assert_eq!(injection.edge_ids.len(), 2);
        let candidates = &recall.gate.inputs()[0].candidates;
        assert_eq!(
            injection.edge_ids,
            vec![candidates[0].edge_id.clone(), candidates[1].edge_id.clone(),]
        );
        let content = injection.content.clone();
        assert!(content.starts_with("I remember: "));
        assert!(content.contains(&candidates[0].edge_text));
        assert!(content.contains(&candidates[1].edge_text));
    }

    #[tokio::test]
    async fn an_already_injected_edge_is_not_presented_and_not_injected() {
        // Section 9.3: a memory already injected in the current chunk
        // is not injected again. The candidate never reaches the gate.
        let (_dir, store, memory) = backend().await;
        let person = person_node("u42", "Alice");
        let espresso = concept_node("espresso");
        let go = concept_node("go");
        let first = fact_edge(
            &person.id,
            &espresso.id,
            "related_to",
            "Alice likes espresso.",
        );
        let second = fact_edge(&person.id, &go.id, "related_to", "Alice plays go.");
        // created_at descending (Section 8.2): the NEWER edge comes
        // first in the neighbor fetch.
        let mut second = second;
        second.created_at = NOW + time::Duration::seconds(1);
        seed(&memory, vec![person, espresso, go], vec![first, second]).await;

        // Pre-insert the dedup row of the first candidate's edge id
        // (learned through the same read path the worker uses).
        let edges = memory
            .neighbors(CHAT, &person_id("u42"))
            .await
            .expect("neighbors");
        assert_eq!(edges.len(), 2);
        let deduped_edge_id = edges[0].edge_id();
        let kept_edge_text = edges[1].edge_text.clone();
        store
            .insert_injected_memory(CHAT, &deduped_edge_id, 10, "m1-m10", "earlier injection")
            .expect("insert the dedup row");

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0, 1]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        // The deduped candidate was never presented to the gate.
        let candidates = &recall.gate.inputs()[0].candidates;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].edge_text, kept_edge_text);
        // The gate selected every presented candidate; only the
        // surviving one is injected.
        assert_eq!(outcome.injections.len(), 1);
        assert_eq!(
            outcome.injections[0].edge_ids,
            vec![candidates[0].edge_id.clone()]
        );
        assert_eq!(
            outcome.injections[0].content,
            format!("I remember: {kept_edge_text}")
        );
    }
}
