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
//! 7. Render (Section 9.4): exactly one `PlannedInjection` of the
//!    `<memory>…</memory>` shape
//!    ([`tamako_core::wake::render_injection_content`]). An empty
//!    injection is FORBIDDEN (Section 9.2): an empty selection yields
//!    no `PlannedInjection` at all.
//!
//! Degradation policy: a graph error of one entry (alias lookup or
//! neighbor fetch) logs a warning and skips that entry; the wake
//! continues with the remaining entries. Store failures are unexpected
//! internal failures and propagate as `CoreError`.
//!
//! ## The candidate-term tokenizer (Phase 1, decision 44)
//!
//! The tokenizer is a pure deterministic function. NO LLM term
//! extraction (Phase 1). Two paths feed one shared dedup namespace
//! (a term is a term):
//!
//! - Alphanumeric tokens: split on non-alphanumerics, normalize every
//!   token with the Section 7.1 rules
//!   (`tamako_memory::identifiers::normalize`: NFKC, lowercase), drop
//!   empty tokens, tokens shorter than 2 chars after normalization,
//!   and a small built-in English stopword list. Dedup keeps the
//!   first occurrence order. At most [`MAX_CANDIDATE_TERMS`] terms
//!   per wake.
//! - CJK n-grams: every maximal run of CJK characters yields ALL
//!   contiguous n-grams with n in 2..=5 (chars, not bytes). A maximal
//!   run is a maximal sequence of characters of the CJK set: CJK
//!   Unified Ideographs U+4E00..=U+9FFF, Extension A
//!   U+3400..=U+4DBF, plus Hiragana/Katakana U+3040..=U+30FF. The
//!   kana block mixes Japanese kana into the CJK runs on purpose:
//!   Japanese terms surface as n-grams too. A run breaks on any
//!   character outside the CJK set, so an ASCII letter between two
//!   CJK characters splits one run into two. There is NO whole-run
//!   token: the pre-n-gram behavior kept the whole run as one token
//!   ("你今天吃饭了吗"), which could never match an Alias. A 1-char
//!   run yields no term (n starts at 2). Every n-gram goes through
//!   `normalize` (Section 7.1 parity with the write path: a
//!   compatibility character in a message matches the alias stored
//!   under the normalized form). At most [`MAX_NGRAM_TERMS`] n-gram
//!   terms per wake; when the pool exceeds the bound, longer n-grams
//!   win (5 before 4 before 3 before 2), ties keep the
//!   first-occurrence order (message order, then position in the
//!   message).
//!
//! Output order: the alphanumeric terms first in first-occurrence
//! order, then the n-gram terms (deduped, longer first, first
//! occurrence inside one length). Both budgets are constants, not
//! configuration keys. Every lookup of a term stays an exact Alias
//! match (Section 8.1 step 2, Rule R5 — no fuzzy scans).
//!
//! Documented limits (accepted recall loss of Phase 1):
//!
//! - NO multi-word alphanumeric terms: "San Francisco" becomes two
//!   terms. CJK n-grams ARE multi-character terms by design.
//! - NO synonyms and NO cross-language merging (Section 7.1 CAUTION):
//!   "tama" and "たま" stay distinct terms.
//! - 1-char CJK runs yield no term (the n-gram path starts at 2). A
//!   one-character CJK name is not a term.
//! - The stopword list is English only. NO CJK stopword list: alias
//!   lookups only hit on exact matches against a sparse alias table,
//!   so an unmatched common-word n-gram costs one indexed miss.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

use rig::completion::Message;

use tamako_core::actor::CoreError;
// The injection rendering is defined ONCE in tamako-core (next to
// `PlannedInjection`, the type that documents the injection text
// shape): the recall renderer below uses `render_injection_content`,
// and the reply parrot filter of tamako-core matches the same
// `<memory>` shape, so the injection format and the filter can never
// drift apart (decision 59).
use tamako_core::wake::{
    render_injection_content, GateMessage, PlannedInjection, RecallOutcome, RecallProvider,
};
use tamako_memory::identifiers::{alias_id, normalize, person_id};
use tamako_memory::MemoryBackend;
use tamako_persona::CONTEXT_FORMAT_GLOSS;
use tamako_store::{Store, StoreError};
use time::macros::format_description;

use crate::endpoint::{EndpointClient, EndpointConfig};
use crate::extract::AgentError;

/// The default max tokens of the relevance-gate response. The output is
/// one small JSON object (two fields); 262144 tokens is a generous bound
/// (same bound as the participation gate).
pub const RECALL_DEFAULT_MAX_TOKENS: u64 = 262144;

/// The bound of the alphanumeric candidate-term list of one wake. The
/// terms feed the entry resolution; 20 terms is a documented bound
/// that keeps the recall cheap (Section 9.1).
pub const MAX_CANDIDATE_TERMS: usize = 20;

/// The bound of the CJK n-gram term pool of one wake (module docs:
/// the tokenizer). Separate from the [`MAX_CANDIDATE_TERMS`]
/// alphanumeric budget. Selection rule when the pool exceeds the
/// bound: longer n-grams first (5 before 4 before 3 before 2), ties
/// in first-occurrence order (message order, then position in the
/// message). A constant, NOT a configuration key.
pub const MAX_NGRAM_TERMS: usize = 40;

/// The largest n of the CJK n-grams, in chars. The smallest n is 2:
/// a 1-char CJK run yields no term (module docs).
const MAX_NGRAM_N: usize = 5;

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
    /// The fact key parts of the edge (decision 40). The edge natural
    /// key carries `valid_at`, so the same fact extracted twice with a
    /// different `valid_at` yields two distinct edge ids. The
    /// same-fact collapse of `recall_inner` keys on the triple
    /// (source_id, relationship_name, target_id) IGNORING `valid_at`
    /// and keeps the latest edge (dev-roadmap.md Section 3 item 5).
    pub source_id: String,
    pub relationship_name: String,
    pub target_id: String,
}

/// The candidate terms of one wake: the normalized alphanumeric
/// tokens of the new message texts (deduped in first-occurrence
/// order, at most [`MAX_CANDIDATE_TERMS`] entries), then the CJK
/// n-grams of every maximal CJK run (deduped against the same shared
/// namespace, longer n-grams first, at most [`MAX_NGRAM_TERMS`]
/// entries). Refer to the module docs for the tokenizer rules and the
/// documented Phase 1 limits. Pure and deterministic (decision 44):
/// no LLM term extraction, no I/O, no randomness.
pub fn candidate_terms(texts: &[&str]) -> Vec<String> {
    let mut alnum_terms = Vec::new();
    let mut ngram_terms = Vec::new();
    let mut ngram_len_counts = [0usize; MAX_NGRAM_N + 1];
    let mut seen = HashSet::new();
    let mut token = String::new();
    let mut cjk_run: Vec<char> = Vec::new();
    for text in texts {
        for ch in text.chars() {
            if is_cjk_char(ch) {
                push_term(&mut alnum_terms, &mut seen, &token);
                token.clear();
                cjk_run.push(ch);
            } else if ch.is_alphanumeric() {
                push_cjk_ngrams(&mut ngram_terms, &mut ngram_len_counts, &mut seen, &cjk_run);
                cjk_run.clear();
                token.push(ch);
            } else {
                push_term(&mut alnum_terms, &mut seen, &token);
                token.clear();
                push_cjk_ngrams(&mut ngram_terms, &mut ngram_len_counts, &mut seen, &cjk_run);
                cjk_run.clear();
            }
        }
        push_term(&mut alnum_terms, &mut seen, &token);
        token.clear();
        push_cjk_ngrams(&mut ngram_terms, &mut ngram_len_counts, &mut seen, &cjk_run);
        cjk_run.clear();
    }
    // Output order (module docs): alphanumeric terms first, then the
    // n-gram terms, longer n-grams first and first-occurrence order
    // inside one length. The stable sort keeps the collection order
    // inside one length.
    ngram_terms.sort_by_key(|term| std::cmp::Reverse(term.chars().count()));
    ngram_terms.truncate(MAX_NGRAM_TERMS);
    alnum_terms.extend(ngram_terms);
    alnum_terms
}

/// The CJK set of the tokenizer (module docs): CJK Unified Ideographs
/// U+4E00..=U+9FFF, Extension A U+3400..=U+4DBF, plus Hiragana and
/// Katakana U+3040..=U+30FF. The kana block mixes Japanese kana into
/// the CJK runs on purpose: Japanese terms surface as n-grams too.
/// Hand-rolled range check — no regex or unicode crate (Phase 1).
fn is_cjk_char(ch: char) -> bool {
    matches!(
        ch,
        '\u{3040}'..='\u{30FF}' | '\u{3400}'..='\u{4DBF}' | '\u{4E00}'..='\u{9FFF}'
    )
}

/// Normalizes one raw alphanumeric token and appends it when it
/// survives the drop rules (module docs): Section 7.1 normalization,
/// the 2-char minimum, the stopword list. Dedup keeps the first
/// occurrence. CJK characters never reach this function: the CJK runs
/// go through [`push_cjk_ngrams`].
fn push_term(terms: &mut Vec<String>, seen: &mut HashSet<String>, raw: &str) {
    if raw.is_empty() || terms.len() >= MAX_CANDIDATE_TERMS {
        return;
    }
    let normalized = normalize(raw);
    // The 2-char minimum counts chars AFTER normalization.
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

/// Emits all contiguous n-grams (n in 2..=[`MAX_NGRAM_N`], chars) of
/// one maximal CJK run, in first-occurrence order (position, then
/// growing n). A run shorter than 2 chars yields no term (the 1-char
/// drop of the module docs). There is NO whole-run token: a run
/// longer than [`MAX_NGRAM_N`] yields n-grams only.
fn push_cjk_ngrams(
    terms: &mut Vec<String>,
    len_counts: &mut [usize; MAX_NGRAM_N + 1],
    seen: &mut HashSet<String>,
    run: &[char],
) {
    if run.len() < 2 {
        return;
    }
    for start in 0..run.len() - 1 {
        let max_len = MAX_NGRAM_N.min(run.len() - start);
        for len in 2..=max_len {
            push_ngram(terms, len_counts, seen, run, start, len);
        }
    }
}

/// Normalizes one raw n-gram and appends it when it survives the
/// selection rule of [`MAX_NGRAM_TERMS`]: longer n-grams win, ties
/// keep the first occurrence. A new n-gram of length `len` loses
/// against every already-collected n-gram of length `len` or more, so
/// it can never enter the final list once the bound is full of such
/// n-grams — stop the collection then. `len_counts[len]` counts the
/// distinct collected n-grams of each length.
fn push_ngram(
    terms: &mut Vec<String>,
    len_counts: &mut [usize; MAX_NGRAM_N + 1],
    seen: &mut HashSet<String>,
    run: &[char],
    start: usize,
    len: usize,
) {
    if len_counts[len..].iter().sum::<usize>() >= MAX_NGRAM_TERMS {
        return;
    }
    let raw: String = run[start..start + len].iter().collect();
    // Section 7.1 parity with the write path: normalize every term
    // (NFKC, lowercase), so a compatibility character in a message
    // matches the alias stored under the normalized form.
    let normalized = normalize(&raw);
    if normalized.chars().count() < 2 {
        return;
    }
    if seen.insert(normalized.clone()) {
        terms.push(normalized);
        len_counts[len] += 1;
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

/// The static head of the relevance-gate system preamble (Section
/// 9.2): the role, the output shape, and rules 1-3. Rule 4 carries
/// the injection cap, which is configurable (the config key
/// `recall_injection_cap`), so the full preamble is rendered per call
/// by [`recall_preamble`] (decision 65).
pub const RECALL_PREAMBLE: &str = "\
You select memories for a group pet. The pet is a member of a group chat. Before the pet speaks, it recalls memories of the group.
Output shape (field names exactly as written): {\"selected\":[<1-based integers>],\"reason\":\"...\"}

Rules:
1. Read the new group messages. Read the candidate memories.
2. Select a candidate memory ONLY when omitting it would materially reduce the quality of the reply or of the participation decision.
3. When in doubt, select nothing. Selecting nothing is the normal case. A memory that is loosely related is not enough.";

/// Renders the full system preamble of the relevance-gate call
/// (Section 9.2) for one injection cap: [`RECALL_PREAMBLE`] plus rule
/// 4, whose "at most N" is the configured `recall_injection_cap`
/// rendered per call (decision 65), and rule 5.
pub fn recall_preamble(injection_cap: u32) -> String {
    format!(
        "{RECALL_PREAMBLE}\n\
         4. Give the 1-based numbers of the selected memories in descending relevance. Select at most {injection_cap} memories.\n\
         5. Output only the JSON object of the required schema. Give one short reason. No commentary."
    )
}

/// The full system preamble of the relevance-gate call:
/// [`recall_preamble`] plus the shared context-format gloss of
/// tamako-persona (decision 63, the deliberate preamble event). The
/// gloss is the SINGLE source in tamako-persona: the persona preamble
/// and the participation-gate preamble embed the same constant. The
/// relevance gate consumes the same XML-shaped
/// `GateMessage.content` lines as the participation gate, so it must
/// read the same explanation.
pub fn recall_system_preamble(injection_cap: u32) -> String {
    format!(
        "{}\n\n{CONTEXT_FORMAT_GLOSS}",
        recall_preamble(injection_cap)
    )
}

/// The date format of the candidate list: YYYY-MM-DD, UTC.
const YMD_FORMAT: &[time::format_description::FormatItem<'_>] =
    format_description!("[year]-[month]-[day]");

/// Renders the user prompt of the relevance-gate call (Section 9.2):
/// one line per new message as the XML-tagged `content`, then a
/// numbered candidate list `1. {edge_text} (since {YYYY-MM-DD of
/// valid_at, UTC})`.
///
/// Unlike the participation gate, the recall prompt renders NO
/// `{row_id}` prefix (decision 65): the selection contract
/// ([`RecallSelection`]) speaks 1-based candidate INDICES only — the
/// post-validation of `recall_inner` checks `index < candidates.len()`
/// and never a row id — and the message id already rides inside the
/// content as the `id="…"` attribute (with `reply_to_id="…"` for the
/// reply linkage), so a separate prefix duplicated it.
pub fn render_recall_prompt(input: &RelevanceInput) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "New messages (XML-tagged content):");
    for message in &input.new_messages {
        let _ = writeln!(out, "{}", message.content);
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
    ///
    /// The `indices`/`numbers` aliases tolerate field-name drift of
    /// endpoints that ignore the output schema and free-generate.
    /// Aliases affect deserialization only: serialization and the
    /// schemars schema keep the canonical name.
    #[serde(alias = "indices", alias = "numbers")]
    pub selected: Vec<u32>,
    /// One short reason.
    ///
    /// The `rationale` alias tolerates endpoint field-name drift.
    #[serde(alias = "rationale")]
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
    /// The Section 9.2 hard cap (the config key
    /// `recall_injection_cap`): rendered into the preamble per call
    /// (decision 65), so the model reads the same cap the
    /// post-validation of `ShallowRecall` enforces.
    injection_cap: u32,
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
    pub fn new(client: EndpointClient, max_tokens: u64, injection_cap: u32) -> Self {
        RigRelevanceGate {
            client,
            max_tokens,
            injection_cap,
        }
    }

    /// Builds the relevance gate for one resolved endpoint (the `gate`
    /// purpose, specs.md Section 13). Returns `AgentError::ProviderConfig`
    /// when the family API key is missing.
    pub fn from_endpoint(
        endpoint: &EndpointConfig,
        injection_cap: u32,
    ) -> Result<Self, AgentError> {
        Ok(RigRelevanceGate::new(
            EndpointClient::build(endpoint)?,
            RECALL_DEFAULT_MAX_TOKENS,
            injection_cap,
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
            // The shared structured flow of the endpoint layer
            // (schema per the resolved mode, one-shot repair retry).
            let selection = self
                .client
                .complete_structured::<RecallSelection>(
                    // The preamble (with the configured cap rendered
                    // in) plus the shared format gloss becomes the
                    // system message.
                    Some(recall_system_preamble(self.injection_cap)),
                    vec![Message::user(render_recall_prompt(input))],
                    schemars::schema_for!(RecallSelection),
                    self.max_tokens,
                    "invalid recall gate JSON",
                )
                // A malformed output is an AgentError; the caller maps
                // ANY gate failure to "inject nothing" (Section 9.2).
                .await?;
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

    /// The recall flow (module docs, steps 1-7, plus the same-fact
    /// collapse of `collapse_same_fact_candidates` between the
    /// neighbor fetch and the Section 9.3 dedup — decision 40).
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
                                source_id: edge.source_id,
                                relationship_name: edge.relationship_name,
                                target_id: edge.target_id,
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

        // Same-fact collapse (decision 40): the edge natural key
        // carries valid_at, so one fact extracted twice with a
        // different valid_at yields TWO candidates whose injection
        // would burn the cap twice in one wake. Collapse to the
        // latest rendering BEFORE the Section 9.3 dedup; refer to
        // `collapse_same_fact_candidates` for the ordering rationale.
        let fetched_edge_count = candidates.len();
        let mut candidates = collapse_same_fact_candidates(candidates);
        let collapsed_count = fetched_edge_count - candidates.len();

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
            // Outcome (a) of the gate observability classes (Section
            // 9.1). DEBUG only; the curated INFO wake line of
            // decision 53 is untouched.
            tracing::debug!(
                chat_id = %chat_id,
                entry_count = entry_ids.len(),
                fetched_edge_count = fetched_edge_count,
                "recall found no candidates; the relevance gate is not called"
            );
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
        tracing::debug!(
            chat_id = %chat_id,
            candidate_count = input.candidates.len(),
            collapsed_count = collapsed_count,
            "recall presents candidates to the relevance gate"
        );
        let gate_result = self.gate.select(&input).await;

        // Step 6: post-validation in plain Rust (never trust the model,
        // same principle as validate.rs): in-range indices only,
        // deduped, at most injection_cap (the Section 9.2 hard cap). A
        // failed gate has no selection to validate.
        let mut chosen: Vec<&RecallCandidate> = Vec::new();
        if let Ok(selection) = &gate_result {
            let mut picked = HashSet::new();
            for &index in selection {
                if index < input.candidates.len() && picked.insert(index) {
                    chosen.push(&input.candidates[index]);
                    if chosen.len() >= self.injection_cap as usize {
                        break;
                    }
                }
            }
        }

        // The gate outcome classes (Section 9.2 observability): the
        // fail-closed mapping makes a gate failure and a true "not
        // relevant" produce the SAME RecallOutcome; the DEBUG logs
        // distinguish them (the WARN at the failure site carries the
        // error class). Decision 53: the curated INFO wake line stays
        // untouched.
        match recall_verdict(input.candidates.len(), gate_result.is_err(), chosen.len()) {
            RecallVerdict::GateFailed => {
                // Outcome (c): fail-closed (Section 9.2).
                let error = gate_result.expect_err("the verdict says the gate failed");
                tracing::warn!(
                    error = %error,
                    "the relevance gate failed; injecting nothing"
                );
                tracing::debug!(
                    chat_id = %chat_id,
                    candidate_count = input.candidates.len(),
                    "the relevance gate failed; injecting nothing"
                );
                return Ok(RecallOutcome::default());
            }
            RecallVerdict::GateSelectedNone => {
                // Outcome (b): an empty injection is FORBIDDEN
                // (Section 9.2): an empty selection yields no
                // PlannedInjection at all.
                tracing::debug!(
                    chat_id = %chat_id,
                    candidate_count = input.candidates.len(),
                    "the relevance gate selected no candidates; injecting nothing"
                );
                return Ok(RecallOutcome::default());
            }
            // NoCandidates returned before the gate call (Section
            // 9.1); Injected falls through to the render.
            RecallVerdict::NoCandidates | RecallVerdict::Injected => {}
        }

        // Step 7 (Section 9.4): exactly one PlannedInjection in the new
        // `<memory>` shape. The edge texts are single sentences, joined
        // with a single space; the shared renderer XML-escapes the body
        // so a hostile edge text cannot break out of the tag.
        let content = {
            let body = chosen
                .iter()
                .map(|candidate| candidate.edge_text.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            render_injection_content(&body)
        };
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

/// Collapses the candidate set by fact key: one fact is the triple
/// (source_id, relationship_name, target_id), IGNORING `valid_at`
/// (decision 40: the edge natural key carries `valid_at`, so the same
/// fact extracted twice with a different `valid_at` is two distinct
/// edges). The edge with the LATEST `valid_at` survives —
/// dev-roadmap.md Section 3 item 5: read-side ordering takes the
/// latest edge per subject and predicate; the collapse makes the
/// injection path honor it. A tie on `valid_at` keeps the FIRST
/// occurrence (deterministic), and the survivor keeps the position of
/// the first occurrence of its fact.
///
/// The collapse runs BEFORE the Section 9.3 dedup against
/// `injected_memories`: an older already-injected edge must not shadow
/// the newer same-fact edge — after the collapse the survivor is the
/// latest rendering, and the dedup keys on the survivor. Corollary:
/// when the LATEST edge of a fact was already injected (a dedup row
/// exists for its edge id), the fact drops entirely and the older
/// duplicate does not come back — same fact, already told, correct.
///
/// The fetch-time `seen_edge_ids` dedup still runs first (unchanged):
/// the same edge reached through several entries (sender and
/// reply-target neighborhoods overlap) never reaches this function
/// twice.
fn collapse_same_fact_candidates(candidates: Vec<RecallCandidate>) -> Vec<RecallCandidate> {
    // Fact key -> position of the first occurrence in `collapsed`.
    let mut positions: HashMap<(String, String, String), usize> = HashMap::new();
    let mut collapsed: Vec<RecallCandidate> = Vec::new();
    for candidate in candidates {
        let fact_key = (
            candidate.source_id.clone(),
            candidate.relationship_name.clone(),
            candidate.target_id.clone(),
        );
        match positions.get(&fact_key) {
            None => {
                positions.insert(fact_key, collapsed.len());
                collapsed.push(candidate);
            }
            Some(&position) => {
                // The latest valid_at wins; a tie keeps the first
                // occurrence (deterministic).
                if candidate.valid_at > collapsed[position].valid_at {
                    collapsed[position] = candidate;
                }
            }
        }
    }
    collapsed
}

/// The outcome classes of one recall call for the DEBUG observability
/// logs (Sections 9.1 and 9.2). The fail-closed gate maps every
/// failure to "inject nothing"; without these classes a gate failure
/// and a true "not relevant" look identical in the logs. DEBUG/WARN
/// only — the curated INFO wake line of decision 53 is untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecallVerdict {
    /// Zero candidates: the relevance gate is NEVER called
    /// (Section 9.1).
    NoCandidates,
    /// The gate call failed: fail-closed, nothing is injected
    /// (Section 9.2).
    GateFailed,
    /// The gate ran, but the post-validated selection is empty.
    GateSelectedNone,
    /// At least one candidate survived post-validation: an injection
    /// is planned.
    Injected,
}

/// Classifies the outcome of one recall call from the three counts of
/// the flow. Pure and total: zero candidates short-circuits before the
/// gate (Section 9.1), and a gate failure wins over the selection
/// count (fail-closed, Section 9.2).
fn recall_verdict(candidate_count: usize, gate_failed: bool, chosen_count: usize) -> RecallVerdict {
    if candidate_count == 0 {
        RecallVerdict::NoCandidates
    } else if gate_failed {
        RecallVerdict::GateFailed
    } else if chosen_count == 0 {
        RecallVerdict::GateSelectedNone
    } else {
        RecallVerdict::Injected
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
            // The production content shape of context.rs: the message
            // id rides inside the XML as the `id` attribute.
            content: format!(r#"<msg from="{sender_id}" at="13:01" id="{row_id}">{text}</msg>"#),
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
        // A CJK run now yields all contiguous n-grams (n in 2..=5), not
        // one whole-run token. A two-char run yields exactly its
        // bigram; the one-char run "は" yields nothing.
        assert_eq!(candidate_terms(&["玉子 は 寿司"]), vec!["玉子", "寿司"]);
        // A single CJK character is dropped (documented Phase 1 limit:
        // the n-gram path starts at 2).
        assert_eq!(candidate_terms(&["猫"]), Vec::<String>::new());
    }

    #[test]
    fn the_tokenizer_extracts_all_ngrams_of_a_pure_cjk_run() {
        // A 6-char run yields every contiguous n-gram with n in 2..=5:
        // 2 five-grams, 3 four-grams, 4 trigrams, 5 bigrams, ordered
        // longer first, first occurrence inside one length. The whole
        // run is NOT a term.
        let terms = candidate_terms(&["今天天气很好"]);
        assert_eq!(
            terms,
            vec![
                "今天天气很",
                "天天气很好",
                "今天天气",
                "天天气很",
                "天气很好",
                "今天天",
                "天天气",
                "天气很",
                "气很好",
                "今天",
                "天天",
                "天气",
                "气很",
                "很好",
            ]
        );
        assert!(!terms.contains(&"今天天气很好".to_string()));
    }

    #[test]
    fn a_short_cjk_run_yields_its_ngrams_and_no_whole_run_token() {
        // A 4-char run yields 1 + 2 + 3 = 6 n-grams and nothing else:
        // the whole run survives only as its 4-gram, never as a
        // separate whole-run token.
        let terms = candidate_terms(&["春夏秋冬"]);
        assert_eq!(
            terms,
            vec!["春夏秋冬", "春夏秋", "夏秋冬", "春夏", "夏秋", "秋冬"]
        );
    }

    #[test]
    fn the_tokenizer_combines_ascii_tokens_and_cjk_ngrams() {
        // The ASCII token takes the alphanumeric path; the two CJK
        // runs take the n-gram path. Alphanumeric terms come first.
        let terms = candidate_terms(&["我今天吃了sushi，很好吃"]);
        assert_eq!(
            terms,
            vec![
                "sushi",
                "我今天吃了",
                "我今天吃",
                "今天吃了",
                "我今天",
                "今天吃",
                "天吃了",
                "很好吃",
                "我今",
                "今天",
                "天吃",
                "吃了",
                "很好",
                "好吃",
            ]
        );
    }

    #[test]
    fn an_ascii_letter_inside_a_cjk_run_splits_it_into_two_runs() {
        // "abc" and "def" go through the alphanumeric path; "明天" is
        // a maximal CJK run of its own and yields its bigram.
        let terms = candidate_terms(&["abc明天def"]);
        assert_eq!(terms, vec!["abc", "def", "明天"]);
    }

    #[test]
    fn the_tokenizer_extracts_japanese_kana_ngrams() {
        // The kana block mixes into the CJK runs on purpose: a kana
        // run yields n-grams like any CJK run.
        let terms = candidate_terms(&["たまご"]);
        assert_eq!(terms, vec!["たまご", "たま", "まご"]);
    }

    #[test]
    fn the_tokenizer_normalizes_compatibility_characters_in_cjk_runs() {
        // Section 7.1 parity: U+30FF KATAKANA DIGRAPH KOTO folds to
        // コト under NFKC, so the bigram of the run normalizes to the
        // form an alias is stored under.
        let terms = candidate_terms(&["読ヿ"]);
        assert_eq!(terms, vec![normalize("読コト")]);
        assert_eq!(terms, vec!["読コト"]);
    }

    #[test]
    fn the_tokenizer_dedups_ngrams_across_messages_and_token_paths() {
        // The same n-gram of two messages appears once (one shared
        // dedup namespace).
        let terms = candidate_terms(&["他住在北京", "北京很热闹"]);
        assert_eq!(terms.iter().filter(|term| *term == "北京").count(), 1);
        // Cross-path dedup: the halfwidth katakana token ｽｼ
        // normalizes (NFKC) to スシ on the alphanumeric path, so the
        // bigram of the later full-width run スシ is a duplicate.
        let terms = candidate_terms(&["ｽｼ、スシ"]);
        assert_eq!(terms, vec!["スシ"]);
    }

    #[test]
    fn the_tokenizer_caps_ngrams_at_forty_longer_first() {
        // A 13-char run of distinct characters yields 9 + 10 + 11 + 12
        // = 42 distinct n-grams: two more than MAX_NGRAM_TERMS. The
        // longer n-grams win; the last two bigrams (盈昃, 昃辰) drop.
        let terms = candidate_terms(&["天地玄黄宇宙洪荒日月盈昃辰"]);
        assert_eq!(terms.len(), MAX_NGRAM_TERMS);
        // All 5-grams first, in first-occurrence order ...
        assert_eq!(terms[0], "天地玄黄宇");
        assert_eq!(terms[8], "日月盈昃辰");
        // ... then the 4-grams and the 3-grams ...
        assert_eq!(
            terms.iter().position(|term| term.chars().count() == 2),
            Some(30)
        );
        // ... and every 5-gram appears before any 2-gram.
        let first_bigram = terms
            .iter()
            .position(|term| term.chars().count() == 2)
            .unwrap();
        assert!(terms[..first_bigram]
            .iter()
            .all(|term| term.chars().count() > 2));
        // The surviving bigrams keep the first-occurrence order.
        assert_eq!(terms[MAX_NGRAM_TERMS - 1], "月盈");
        assert!(!terms.contains(&"盈昃".to_string()));
        assert!(!terms.contains(&"昃辰".to_string()));
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
                    source_id: "person-alice".to_string(),
                    relationship_name: "likes".to_string(),
                    target_id: "concept-espresso".to_string(),
                },
                RecallCandidate {
                    edge_id: "edge-2".to_string(),
                    edge_text: "Bob plays go.".to_string(),
                    valid_at: datetime!(2025-12-31 23:00 UTC),
                    source_id: "person-bob".to_string(),
                    relationship_name: "plays".to_string(),
                    target_id: "concept-go".to_string(),
                },
            ],
        }
    }

    #[test]
    fn the_prompt_renders_messages_and_numbered_candidates() {
        let prompt = render_recall_prompt(&sample_input());
        // One line per new message: the XML-tagged content only. The
        // selection contract speaks candidate indices, so there is NO
        // duplicated row-id prefix (decision 65); the message id rides
        // inside the content as the `id` attribute.
        assert!(prompt
            .contains(r#"<msg from="u1" at="13:01" id="41">has anyone tried the new cafe?</msg>"#));
        assert!(prompt.contains(r#"<msg from="u2" at="13:01" id="42">the espresso is great</msg>"#));
        assert!(!prompt.contains("41 <msg"));
        assert!(!prompt.contains("42 <msg"));
        // The numbered candidate list: 1-based, YYYY-MM-DD of valid_at.
        assert!(prompt.contains("1. Alice likes espresso. (since 2026-08-07)"));
        assert!(prompt.contains("2. Bob plays go. (since 2025-12-31)"));
    }

    #[test]
    fn the_preamble_states_the_conservative_rules() {
        // Section 9.2: conservative by default. The cap sentence of
        // rule 4 is rendered per call by `recall_preamble` (decision
        // 65), so the assertions run on the rendered preamble.
        let preamble = recall_preamble(5);
        assert!(preamble.contains("materially reduce the quality"));
        assert!(preamble.contains("select nothing"));
        assert!(preamble.contains("the normal case"));
        assert!(preamble.contains("the JSON object of the required schema"));
        // The preamble states the exact output field names (a minimal
        // skeleton): field names must not rely on schema enforcement.
        assert!(preamble.contains("\"selected\""));
        assert!(preamble.contains("\"reason\""));
        // The static head carries rules 1-3; the full preamble starts
        // with it.
        assert!(preamble.starts_with(RECALL_PREAMBLE));
    }

    #[test]
    fn the_preamble_renders_the_configured_injection_cap() {
        // Decision 65: the cap is the configured `recall_injection_cap`,
        // rendered per call — not a hardcoded 5.
        let preamble = recall_preamble(3);
        assert!(preamble.contains("Select at most 3 memories."));
        assert!(!preamble.contains("at most 5"));
        // The default cap still renders its number.
        assert!(recall_preamble(5).contains("Select at most 5 memories."));
        // The static head carries no cap sentence at all.
        assert!(!RECALL_PREAMBLE.contains("at most"));
    }

    #[test]
    fn the_recall_system_preamble_appends_the_shared_format_gloss() {
        // Decision 61: the recall gate system message includes the
        // SAME XML format explanation as the persona preamble and the
        // participation-gate preamble (single source in
        // tamako-persona). The conservative rules stay first; the
        // gloss appends as a clearly separated section.
        let preamble = recall_system_preamble(5);
        assert!(preamble.starts_with(&recall_preamble(5)));
        assert!(preamble.ends_with(&format!("\n\n{CONTEXT_FORMAT_GLOSS}")));
        assert!(preamble.contains(CONTEXT_FORMAT_GLOSS));
        // The conservative rules are unchanged and still present.
        assert!(preamble.contains("materially reduce the quality"));
        assert!(preamble.contains("the normal case"));
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
    fn field_name_aliases_deserialize_into_the_canonical_struct() {
        // Endpoints that ignore the output schema free-generate; the
        // aliases tolerate the observed field-name drift
        // (deserialization only).
        let selection: RecallSelection =
            serde_json::from_str(r#"{"indices":[2,1],"rationale":"r"}"#).expect("aliases");
        assert_eq!(
            selection,
            RecallSelection {
                selected: vec![2, 1],
                reason: "r".to_string(),
            }
        );
        let selection: RecallSelection =
            serde_json::from_str(r#"{"numbers":[],"reason":"r"}"#).expect("numbers alias");
        assert!(selection.selected.is_empty());
    }

    #[test]
    fn serialization_keeps_the_canonical_field_names() {
        // Aliases affect deserialization only: the serialized JSON
        // keeps the canonical names, so downstream readers never see
        // the drift spellings.
        let selection = RecallSelection {
            selected: vec![1],
            reason: "r".to_string(),
        };
        let value = serde_json::to_value(&selection).expect("to value");
        assert!(value.get("selected").is_some());
        assert!(value.get("reason").is_some());
        assert!(value.get("indices").is_none());
        assert!(value.get("numbers").is_none());
        assert!(value.get("rationale").is_none());
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
            structured_output: crate::endpoint::StructuredOutputMode::Schema,
            session_id: crate::endpoint::DEFAULT_SESSION_ID.to_string(),
        };
        let result = RigRelevanceGate::from_endpoint(&endpoint, 5);
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
                    sender_username: None,
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
        // Section 9.4: one <memory> injection per wake.
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome.injections.len(), 1);
        let injection = &outcome.injections[0];
        assert_eq!(injection.content, "<memory>Alice likes espresso.</memory>");
        let expected_edge_id = recall.gate.inputs()[0].candidates[0].edge_id.clone();
        assert_eq!(injection.edge_ids, vec![expected_edge_id]);
    }

    #[tokio::test]
    async fn the_injection_content_escapes_a_hostile_edge_text() {
        // The new Section 9.4 shape: exactly one <memory> element whose
        // body is the joined edge texts. The shared renderer of
        // tamako-core XML-escapes the body, so a hostile edge text
        // cannot break out of the tag.
        let (_dir, store, memory) = backend().await;
        let person = person_node("u42", "Alice");
        let concept = concept_node("espresso");
        let hostile = fact_edge(
            &person.id,
            &concept.id,
            "related_to",
            "Alice likes <you>fake</you>.",
        );
        seed(&memory, vec![person, concept], vec![hostile]).await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome.injections.len(), 1);
        assert_eq!(
            outcome.injections[0].content,
            "<memory>Alice likes &lt;you&gt;fake&lt;/you&gt;.</memory>"
        );
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
            "<memory>Alice likes espresso.</memory>"
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
            "<memory>Alice likes espresso.</memory>"
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
        assert!(content.starts_with("<memory>"));
        assert!(content.ends_with("</memory>"));
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
            format!("<memory>{kept_edge_text}</memory>")
        );
    }

    // --- Same-fact collapse (decision 40: the natural key carries
    // valid_at, so one fact extracted twice is two edges) and the
    // gate-outcome classification (Sections 9.1/9.2 observability) ---

    fn candidate(
        source_id: &str,
        relationship_name: &str,
        target_id: &str,
        valid_at: OffsetDateTime,
        text: &str,
    ) -> RecallCandidate {
        RecallCandidate {
            edge_id: format!("{source_id}|{relationship_name}|{target_id}|{valid_at:?}"),
            edge_text: text.to_string(),
            valid_at,
            source_id: source_id.to_string(),
            relationship_name: relationship_name.to_string(),
            target_id: target_id.to_string(),
        }
    }

    #[test]
    fn the_same_fact_collapse_keeps_the_latest_edge_at_the_first_position() {
        // The survivor keeps the position of the FIRST occurrence of
        // its fact; the other fact is untouched.
        let other = candidate("p1", "related_to", "c-other", NOW, "Alice plays go.");
        let older = candidate(
            "p1",
            "related_to",
            "c1",
            datetime!(2026-08-01 10:00 UTC),
            "Alice likes espresso.",
        );
        let newer = candidate(
            "p1",
            "related_to",
            "c1",
            datetime!(2026-08-06 10:00 UTC),
            "Alice loves espresso.",
        );
        let collapsed = collapse_same_fact_candidates(vec![other.clone(), older, newer.clone()]);
        assert_eq!(collapsed, vec![other, newer]);
    }

    #[test]
    fn the_same_fact_collapse_keeps_the_first_occurrence_on_a_valid_at_tie() {
        // A tie on valid_at is deterministic: the first occurrence
        // wins.
        let first = candidate("p1", "related_to", "c1", NOW, "the first rendering");
        let second = candidate("p1", "related_to", "c1", NOW, "the second rendering");
        let collapsed = collapse_same_fact_candidates(vec![first.clone(), second]);
        assert_eq!(collapsed, vec![first]);
    }

    #[test]
    fn the_recall_verdict_distinguishes_the_three_gate_outcomes() {
        // (a) Zero candidates: the gate is never called (Section 9.1).
        // The candidate count short-circuits every other input.
        assert_eq!(recall_verdict(0, false, 0), RecallVerdict::NoCandidates);
        assert_eq!(recall_verdict(0, true, 0), RecallVerdict::NoCandidates);
        // (b) Candidates presented, the post-validated selection is
        // empty: the gate selected nothing relevant.
        assert_eq!(recall_verdict(3, false, 0), RecallVerdict::GateSelectedNone);
        // (c) The gate call failed: fail-closed (Section 9.2). The
        // failure class wins over the selection count.
        assert_eq!(recall_verdict(3, true, 0), RecallVerdict::GateFailed);
        // The normal injection path.
        assert_eq!(recall_verdict(3, false, 2), RecallVerdict::Injected);
    }

    /// Seeds one person whose one fact was extracted TWICE: same
    /// source, relationship, and target, different valid_at and a
    /// different text rendering (decision 40: the natural key carries
    /// valid_at, so the two renderings are two distinct edges).
    /// Returns (older, newer) as the recall read path sees them.
    async fn seed_duplicated_fact(
        memory: &LbugBackend,
    ) -> (tamako_memory::NeighborEdge, tamako_memory::NeighborEdge) {
        let person = person_node("u42", "Alice");
        let concept = concept_node("espresso");
        let older = MemoryEdge {
            valid_at: datetime!(2026-08-01 10:00 UTC),
            edge_text: "Alice likes espresso.".to_string(),
            ..fact_edge(&person.id, &concept.id, "related_to", "")
        };
        let newer = MemoryEdge {
            valid_at: datetime!(2026-08-06 10:00 UTC),
            edge_text: "Alice loves espresso.".to_string(),
            // created_at descending (Section 8.2): the newer edge
            // comes first in the neighbor fetch.
            created_at: NOW + time::Duration::seconds(1),
            ..fact_edge(&person.id, &concept.id, "related_to", "")
        };
        seed(memory, vec![person, concept], vec![older, newer]).await;

        let mut edges = memory
            .neighbors(CHAT, &person_id("u42"))
            .await
            .expect("neighbors");
        assert_eq!(edges.len(), 2);
        // Identify by valid_at, not by fetch order.
        let newer_position = edges
            .iter()
            .position(|edge| edge.valid_at == datetime!(2026-08-06 10:00 UTC))
            .expect("the newer edge");
        let newer = edges.remove(newer_position);
        let older = edges.remove(0);
        (older, newer)
    }

    #[tokio::test]
    async fn the_same_fact_collapse_presents_only_the_latest_edge() {
        // Two renderings of one fact must not burn the injection cap
        // twice in one wake: the gate sees the LATEST rendering only
        // (dev-roadmap.md Section 3 item 5).
        let (_dir, store, memory) = backend().await;
        seed_duplicated_fact(&memory).await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        let candidates = &recall.gate.inputs()[0].candidates;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].edge_text, "Alice loves espresso.");
        assert_eq!(
            outcome.injections[0].content,
            "<memory>Alice loves espresso.</memory>"
        );
    }

    #[tokio::test]
    async fn an_injected_older_edge_does_not_shadow_the_newer_same_fact_edge() {
        // The collapse runs BEFORE the Section 9.3 dedup: the dedup
        // row of the OLDER edge must not hide the newer rendering of
        // the same fact.
        let (_dir, store, memory) = backend().await;
        let (older, _newer) = seed_duplicated_fact(&memory).await;
        store
            .insert_injected_memory(CHAT, &older.edge_id(), 10, "m1-m10", "earlier injection")
            .expect("insert the dedup row");

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(
            presented_texts(&recall.gate),
            vec!["Alice loves espresso.".to_string()]
        );
        assert_eq!(
            outcome.injections[0].content,
            "<memory>Alice loves espresso.</memory>"
        );
    }

    #[tokio::test]
    async fn an_injected_latest_edge_drops_the_fact_entirely() {
        // The converse: the survivor of the collapse is the latest
        // rendering, and a dedup row for ITS edge id drops the whole
        // fact. The older duplicate does not come back — same fact,
        // already told, correct.
        let (_dir, store, memory) = backend().await;
        let (_older, newer) = seed_duplicated_fact(&memory).await;
        store
            .insert_injected_memory(CHAT, &newer.edge_id(), 10, "m1-m10", "earlier injection")
            .expect("insert the dedup row");

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(outcome, RecallOutcome::default());
        // No candidate survived: the gate was never called
        // (Section 9.1).
        assert_eq!(recall.gate.call_count(), 0);
    }
}
