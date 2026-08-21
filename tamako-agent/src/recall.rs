//! The shallow recall worker of specs.md Section 9 step 2 (Sections
//! 9.1-9.4). M5 replaces the `NoopRecall` seam with this module.
//!
//! The flow of one recall call:
//!
//! 1. Entry resolution (Section 8.1 of the database spec, steps 1 and 2
//!    ONLY): the Person identifiers
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
//! ## The deep candidate pipeline (decision 76)
//!
//! With [`DeepRecallConfig`] wired ([`ShallowRecall::with_deep_recall`]
//! — the `deep_recall` config key, default true), steps 1-2 widen
//! behind the UNTOUCHED `RecallProvider` seam. The candidate pipeline
//! of one wake is, IN ORDER:
//!
//! 1. SHALLOW: the sources above, unchanged (decision-58 tokenizer,
//!    entry resolution, one-hop neighbors, budgets).
//! 2. VECTOR ENTRY: ONE batched `embed_texts` call per wake (ONLY when
//!    candidate terms exist — zero terms never call the provider,
//!    decision 76 (f)), then KNN per term over the `node_embeddings`
//!    sidecar (k = [`VECTOR_ENTRY_KNN_K`]). A node at or above
//!    `vector_candidate_threshold` (cosine similarity; the sidecar
//!    stores cosine DISTANCE, so `similarity = 1.0 - distance`,
//!    decision 73) becomes an additional ENTRY node. NO confirmation
//!    call on the read path — confirmation is write-path only.
//! 3. TWO-HOP EXPANSION from ALL entry nodes found so far (shallow
//!    entries plus vector entries): `MemoryBackend::two_hop_edges`
//!    under the Section 8.2 rules (whitelist, validity, the 90-day
//!    window, `NEIGHBOR_EXPANSION_LIMIT` per node).
//! 4. FTS: `Store::search_edge_texts` per candidate term over the
//!    `edge_texts` sidecar (decision 76 (c)), the hits hydrated
//!    through `MemoryBackend::edges_by_ids` (valid only, stale ids
//!    dropped).
//!
//! Then: dedup EVERYTHING by edge id — source order shallow, then
//! expansion (which carries the vector entries' edges), then fts,
//! FIRST occurrence wins — then the decision-40 same-fact collapse,
//! then cap the total at `recall_candidate_cap` (default 40) BEFORE
//! the relevance gate (decision 77, S3-F8: the cap counts the
//! post-collapse candidates presented to the gate). The remaining
//! downstream steps are unchanged: the Section 9.3
//! `injected_memories` dedup, the relevance gate, the injection cap.
//! `MAX_PRESENTED_CANDIDATES` stays a hard prompt bound on top of the
//! configured cap (the defaults are equal: 40).
//!
//! Every deep source degrades INDEPENDENTLY to empty: an embed or
//! endpoint failure logs WARN, a store or memory read failure logs
//! DEBUG; recall NEVER fails a wake on a deep source. With
//! `deep_recall = false` (no [`DeepRecallConfig`] wired) the behavior
//! is byte-identical to the pre-76 shallow path.
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
use tamako_core::embedding::EmbeddingProvider;
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
use tamako_memory::{CandidateEdge, MemoryBackend, NEIGHBOR_EXPANSION_LIMIT};
use tamako_persona::CONTEXT_FORMAT_GLOSS;
use tamako_store::{Store, StoreError};
use time::macros::format_description;

use crate::endpoint::{EndpointClient, EndpointConfig};
use crate::extract::AgentError;
// Decision 72: the shared context-view section shape (header and
// empty marker) is defined ONCE next to the participation gate, so
// the two gates' view sections can never drift apart.
use crate::gate::{CONTEXT_VIEW_EMPTY, CONTEXT_VIEW_HEADER};

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
///
/// Decision 76: the configurable `recall_candidate_cap` truncates the
/// merged deep candidate pool first; this constant stays a hard prompt
/// bound on top of it (the defaults are equal: 40).
pub const MAX_PRESENTED_CANDIDATES: usize = 40;

/// Decision 76 (a): the KNN fan-out of the vector entry — per
/// candidate term, the 5 nearest node embeddings of the sidecar are
/// screened against `vector_candidate_threshold`. A modest k: the
/// vector entry widens the entry set, it does not replace the alias
/// entry; the two-hop expansion reaches the accepted nodes' edges.
pub const VECTOR_ENTRY_KNN_K: usize = 5;

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

    /// The decision-72 entry point (specs.md Section 9.2): `select`
    /// plus the shared context view. `Some(view)` renders the view
    /// AHEAD of the new messages and the candidate list; `None`
    /// renders the pre-72 delta-only prompt byte-identically (the
    /// `gate_context` kill switch). The [`RecallSelection`] index
    /// contract is unchanged: the view is read-only orientation, never
    /// a candidate.
    ///
    /// The DEFAULT ignores the view and delegates to `select`, so
    /// existing implementations stay valid unchanged; the live
    /// `RigRelevanceGate` overrides it.
    fn select_with_context<'a>(
        &'a self,
        input: &'a RelevanceInput,
        context_view: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>,
    > {
        let _ = context_view;
        self.select(input)
    }
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

/// The tail note of the relevance-gate prompt when the shared context
/// view renders (decision 72): the context is read-only orientation
/// and the selection contract stays the 1-based candidate numbers
/// (byte-stable — the view is never a candidate). Rides at the TAIL,
/// like the participation gate's target instruction.
pub const RECALL_CONTEXT_NOTE: &str =
    "The context section is read-only orientation; the selection names candidate numbers only.";

/// Renders the user prompt of the relevance-gate call (Section 9.2):
/// with `context_view` `Some(view)` (decision 72) the shared context
/// view renders AHEAD of the per-call sections (the same prefix shape
/// as the participation gate: [`CONTEXT_VIEW_HEADER`], the view bytes
/// or [`CONTEXT_VIEW_EMPTY`], then a blank line); then one line per
/// new message as the XML-tagged `content`, then a numbered candidate
/// list `1. {edge_text} (since {YYYY-MM-DD of valid_at, UTC})`, then
/// the [`RECALL_CONTEXT_NOTE`] tail. With `None` (the `gate_context`
/// kill switch) the prompt is BYTE-IDENTICAL to the pre-72 shape: no
/// context section, no tail note.
///
/// Unlike the participation gate, the recall prompt renders NO
/// `{row_id}` prefix (decision 65): the selection contract
/// ([`RecallSelection`]) speaks 1-based candidate INDICES only — the
/// post-validation of `recall_inner` checks `index < candidates.len()`
/// and never a row id — and the message id already rides inside the
/// content as the `id="…"` attribute (with `reply_to_id="…"` for the
/// reply linkage), so a separate prefix duplicated it.
pub fn render_recall_prompt(input: &RelevanceInput, context_view: Option<&str>) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    // Decision 72: the view section is a clean PREFIX — everything
    // after it is per-call content.
    if let Some(view) = context_view {
        let _ = writeln!(out, "{CONTEXT_VIEW_HEADER}");
        if view.is_empty() {
            let _ = writeln!(out, "{CONTEXT_VIEW_EMPTY}");
        } else {
            let _ = writeln!(out, "{view}");
        }
        let _ = writeln!(out);
    }
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
    if context_view.is_some() {
        // The orientation note rides at the TAIL (decision 72):
        // gate-specific text never enters the shared prefix.
        let _ = writeln!(out);
        let _ = writeln!(out, "{RECALL_CONTEXT_NOTE}");
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
        // The pre-72 delta-only shape (no shared context view).
        self.select_with_context(input, None)
    }

    fn select_with_context<'a>(
        &'a self,
        input: &'a RelevanceInput,
        context_view: Option<&'a str>,
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
                    vec![Message::user(render_recall_prompt(input, context_view))],
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
/// `call_count()`), together with the decision-72 context view of each
/// call (`context_views()`; `None` = the pre-72 delta-only shape).
pub struct ScriptedRelevanceGate {
    mode: Mutex<ScriptedGateMode>,
    inputs: Mutex<Vec<RelevanceInput>>,
    context_views: Mutex<Vec<Option<String>>>,
}

impl ScriptedRelevanceGate {
    /// A scripted gate that answers with the given selections in order.
    /// The selections speak 0-based indices (the trait contract).
    pub fn with_selections(selections: Vec<Vec<usize>>) -> Self {
        ScriptedRelevanceGate {
            mode: Mutex::new(ScriptedGateMode::Selections(selections.into())),
            inputs: Mutex::new(Vec::new()),
            context_views: Mutex::new(Vec::new()),
        }
    }

    /// A scripted gate whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedRelevanceGate {
            mode: Mutex::new(ScriptedGateMode::Failing(message.into())),
            inputs: Mutex::new(Vec::new()),
            context_views: Mutex::new(Vec::new()),
        }
    }

    /// Every input the gate received, in call order.
    pub fn inputs(&self) -> Vec<RelevanceInput> {
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Every context view the gate received, in call order (decision
    /// 72). `None` marks a pre-72 delta-only call.
    pub fn context_views(&self) -> Vec<Option<String>> {
        self.context_views
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

    /// Locks, records, and selects synchronously; the trait methods
    /// only box the result. A poisoned mutex is recovered; the
    /// recorded inputs stay valid (same policy as ScriptedGate).
    fn record_and_select(
        &self,
        input: &RelevanceInput,
        context_view: Option<&str>,
    ) -> Result<Vec<usize>, AgentError> {
        self.inputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(input.clone());
        self.context_views
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(context_view.map(str::to_string));
        let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
        match &mut *mode {
            ScriptedGateMode::Selections(selections) => {
                Ok(selections.pop_front().unwrap_or_default())
            }
            ScriptedGateMode::Failing(message) => Err(AgentError::Extraction(message.clone())),
        }
    }
}

impl RelevanceGate for ScriptedRelevanceGate {
    fn select<'a>(
        &'a self,
        input: &'a RelevanceInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>,
    > {
        let result = self.record_and_select(input, None);
        Box::pin(async move { result })
    }

    fn select_with_context<'a>(
        &'a self,
        input: &'a RelevanceInput,
        context_view: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>,
    > {
        let result = self.record_and_select(input, context_view);
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
    /// The decision-76 deep candidate pipeline (module docs). `None`
    /// is the pre-76 shallow form, byte-identical (the `deep_recall`
    /// kill switch: the wiring layer simply never calls
    /// [`ShallowRecall::with_deep_recall`]).
    deep: Option<DeepRecallConfig>,
}

/// The decision-76 deep-recall knobs and seams (module docs). The
/// wiring layer builds one per group from the resolved `TriggerConfig`
/// and calls [`ShallowRecall::with_deep_recall`] ONLY when
/// `config.deep_recall` is true.
///
/// The store half of the deep sources (KNN over `node_embeddings`,
/// LIKE over `edge_texts`) rides the SAME single-open-group `Store`
/// the recall worker already holds (the embedding-worker contract:
/// exactly one group open).
pub struct DeepRecallConfig {
    /// The embedding seam of the vector entry (the core
    /// [`EmbeddingProvider`], adapted from the rig provider by the
    /// binary): ONE batched `embed_texts` call per wake that has
    /// candidate terms (decision 76 (f)).
    pub provider: Arc<dyn EmbeddingProvider>,
    /// The cosine-similarity acceptance threshold of the vector entry:
    /// the shared `TriggerConfig::vector_candidate_threshold` of
    /// decision 73 (default 0.80), REUSED per decision 76 (a).
    pub vector_candidate_threshold: f64,
    /// The TOTAL candidate cap before the relevance gate
    /// (`TriggerConfig::recall_candidate_cap`, default 40, decision
    /// 76 (d)).
    pub candidate_cap: u32,
}

impl<M: MemoryBackend, G: RelevanceGate> ShallowRecall<M, G> {
    pub fn new(store: Arc<Store>, memory: Arc<M>, gate: G, injection_cap: u32) -> Self {
        ShallowRecall {
            store,
            memory,
            gate,
            injection_cap,
            deep: None,
        }
    }

    /// Decision 76: enables the deep candidate pipeline (module docs).
    /// Additive builder — a `ShallowRecall` without this call is the
    /// pre-76 shallow form, byte-identical.
    pub fn with_deep_recall(mut self, deep: DeepRecallConfig) -> Self {
        self.deep = Some(deep);
        self
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

    /// Decision 76 (a): the vector entry. ONE batched `embed_texts`
    /// call per wake (only when candidate terms exist — zero terms
    /// never call the provider, decision 76 (f)), then KNN per term
    /// over the `node_embeddings` sidecar (k = [`VECTOR_ENTRY_KNN_K`],
    /// ONE blocking-store round trip for every term). A node at or
    /// above `vector_candidate_threshold` (cosine similarity; the
    /// sidecar stores cosine DISTANCE, so `similarity = 1.0 -
    /// distance`, decision 73) becomes an additional entry node,
    /// deduped in first-occurrence order (term order, then KNN
    /// nearest-first). NO confirmation call on the read path —
    /// confirmation is write-path only (decision 76 (a)).
    ///
    /// Never fails the wake: an embed failure or a row-count mismatch
    /// degrades the whole source to empty with one WARN (the endpoint
    /// class); a per-term KNN store failure skips the term with a
    /// DEBUG line (the store-read class); a join failure degrades the
    /// whole source with a DEBUG line.
    async fn vector_entry_nodes(&self, terms: &[String], deep: &DeepRecallConfig) -> Vec<String> {
        if terms.is_empty() {
            return Vec::new();
        }
        // Decision 77 (M3): the provider call runs INLINE in the wake
        // task — contain a provider panic so it degrades to the
        // existing failure path (one WARN, the vector entry is
        // skipped) instead of unwinding into the wake. The CALL itself
        // sits inside the wrapped future: a panic at call time (not
        // only at poll time) is contained too.
        let vectors = match crate::contain_task_panic(async {
            deep.provider.embed_texts(terms).await
        })
        .await
        {
            Ok(Ok(vectors)) if vectors.len() == terms.len() => vectors,
            Ok(Ok(vectors)) => {
                tracing::warn!(
                    expected = terms.len(),
                    got = vectors.len(),
                    "deep recall: the batched embeddings call returned a row-count mismatch; \
                     the vector entry is skipped"
                );
                return Vec::new();
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    error = %error,
                    "deep recall: the batched embeddings call failed; the vector entry is skipped"
                );
                return Vec::new();
            }
            Err(panic) => {
                tracing::warn!(
                    error = %panic,
                    "deep recall: the batched embeddings call panicked; the vector entry is skipped"
                );
                return Vec::new();
            }
        };
        let store = self.store.clone();
        let knn_results = match tokio::task::spawn_blocking(move || {
            vectors
                .iter()
                .map(|query| store.knn_node_embeddings(query, VECTOR_ENTRY_KNN_K))
                .collect::<Vec<_>>()
        })
        .await
        {
            Ok(results) => results,
            Err(error) => {
                tracing::debug!(
                    error = %error,
                    "deep recall: the KNN store task failed to join; the vector entry is skipped"
                );
                return Vec::new();
            }
        };
        let mut accepted = Vec::new();
        let mut seen = HashSet::new();
        for (term, result) in terms.iter().zip(knn_results) {
            match result {
                Ok(hits) => {
                    for (node_id, distance) in hits {
                        let similarity = 1.0 - f64::from(distance);
                        if similarity >= deep.vector_candidate_threshold {
                            push_unique(&mut accepted, &mut seen, node_id);
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        term = %term,
                        error = %error,
                        "deep recall: the KNN lookup failed; skipping the term"
                    );
                }
            }
        }
        accepted
    }

    /// Decision 76 (a)/(c): the full-text term lookups against the
    /// `edge_texts` sidecar — ONE blocking-store round trip for every
    /// candidate term, the hit edge ids deduped in first-occurrence
    /// order (term order, then the sidecar's edge-id order). Never
    /// fails the wake: a per-term store failure skips the term with a
    /// DEBUG line; a join failure degrades the whole source to empty
    /// with a DEBUG line. Zero terms yield zero lookups.
    async fn search_edge_text_ids(&self, terms: &[String]) -> Vec<String> {
        if terms.is_empty() {
            return Vec::new();
        }
        let store = self.store.clone();
        let terms = terms.to_vec();
        let results = match tokio::task::spawn_blocking(move || {
            terms
                .iter()
                .map(|term| (term.clone(), store.search_edge_texts(term)))
                .collect::<Vec<_>>()
        })
        .await
        {
            Ok(results) => results,
            Err(error) => {
                tracing::debug!(
                    error = %error,
                    "deep recall: the edge_texts store task failed to join; the fts source is skipped"
                );
                return Vec::new();
            }
        };
        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        for (term, result) in results {
            match result {
                Ok(hits) => {
                    for id in hits {
                        push_unique(&mut ids, &mut seen, id);
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        term = %term,
                        error = %error,
                        "deep recall: the edge_texts lookup failed; skipping the term"
                    );
                }
            }
        }
        ids
    }

    /// Entry resolution, Section 8.1 steps 1 and 2 of the database
    /// spec. The entry order is senders first, then reply targets,
    /// then alias terms (see `MAX_PRESENTED_CANDIDATES`). Deduped,
    /// first occurrence wins. `terms` are the candidate terms of the
    /// wake, computed ONCE by `recall_inner` (decision 76: the vector
    /// entry and the fts source consume the same terms).
    async fn resolve_entries(
        &self,
        chat_id: &str,
        new_messages: &[GateMessage],
        terms: &[String],
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
        for term in terms {
            let term_alias_id = alias_id(term);
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
    /// `context_view` is the decision-72 shared context view forwarded
    /// to the relevance gate (`None` = the pre-72 delta-only prompt).
    async fn recall_inner(
        &self,
        chat_id: &str,
        new_messages: &[GateMessage],
        context_view: Option<&str>,
    ) -> Result<RecallOutcome, CoreError> {
        // Steps 1-2: entry resolution + neighbor fetch. Candidates are
        // deduped by edge_id; the first occurrence wins. The candidate
        // terms are computed ONCE per wake: the shallow alias entry,
        // the vector entry, and the fts source share them (decision 76
        // (a): candidate terms come from the new messages only).
        let texts: Vec<&str> = new_messages
            .iter()
            .map(|message| message.text.as_str())
            .collect();
        let terms = candidate_terms(&texts);
        let mut entry_ids = self.resolve_entries(chat_id, new_messages, &terms).await?;
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
        let shallow_count = candidates.len();

        // Decision 76: the deep sources widen the candidate pool behind
        // the untouched RecallProvider seam (module docs). Each source
        // degrades INDEPENDENTLY to empty; none fails the wake.
        if let Some(deep) = &self.deep {
            // (2) VECTOR ENTRY: additional entry nodes at or above the
            // threshold. Never fails (WARN on an embed failure, DEBUG
            // on a store read failure).
            let vector_entry_count;
            {
                let vector_entries = self.vector_entry_nodes(&terms, deep).await;
                vector_entry_count = vector_entries.len();
                let mut seen_entries: HashSet<String> = entry_ids.iter().cloned().collect();
                for node_id in vector_entries {
                    if seen_entries.insert(node_id.clone()) {
                        entry_ids.push(node_id);
                    }
                }
            }

            // (3) TWO-HOP EXPANSION from ALL entry nodes found so far
            // (shallow entries plus vector entries), under the Section
            // 8.2 rules. The hop-1 edges overlap the shallow neighbors;
            // the first-wins edge-id dedup keeps the shallow position.
            let expansion_edges = match self
                .memory
                .two_hop_edges(
                    chat_id,
                    &entry_ids,
                    time::OffsetDateTime::now_utc(),
                    NEIGHBOR_EXPANSION_LIMIT,
                )
                .await
            {
                Ok(edges) => edges,
                Err(error) => {
                    tracing::debug!(
                        chat_id = %chat_id,
                        error = %error,
                        "deep recall: the two-hop expansion failed; continuing without it"
                    );
                    Vec::new()
                }
            };
            let expansion_count = expansion_edges.len();
            for edge in expansion_edges {
                push_deep_candidate(&mut candidates, &mut seen_edge_ids, edge);
            }

            // (4) FTS: the "who discussed X" pattern over the
            // edge_texts sidecar (decision 76 (c)), hydrated valid-only
            // through edges_by_ids.
            let mut fts_count = 0;
            let hit_ids = self.search_edge_text_ids(&terms).await;
            if !hit_ids.is_empty() {
                match self.memory.edges_by_ids(chat_id, &hit_ids).await {
                    Ok(edges) => {
                        fts_count = edges.len();
                        for edge in edges {
                            push_deep_candidate(&mut candidates, &mut seen_edge_ids, edge);
                        }
                    }
                    Err(error) => {
                        tracing::debug!(
                            chat_id = %chat_id,
                            error = %error,
                            "deep recall: the edge hydration failed; continuing without the fts source"
                        );
                    }
                }
            }

            // Dedup (decision 76 (d)): first-wins by edge id in source
            // order shallow -> expansion -> fts. The cap itself lands
            // AFTER the same-fact collapse below (decision 77, S3-F8):
            // recall_candidate_cap counts the candidates PRESENTED to
            // the gate (post-dedup, post-collapse), so a collapsed pool
            // never falls below the intended cap.
            // MAX_PRESENTED_CANDIDATES stays a hard prompt bound on top
            // (step 5, unchanged).
            let deduped_count = candidates.len();
            tracing::debug!(
                chat_id = %chat_id,
                shallow_count = shallow_count,
                vector_entry_count = vector_entry_count,
                expansion_count = expansion_count,
                fts_count = fts_count,
                deduped_count = deduped_count,
                "deep recall candidate sources (source order shallow -> expansion -> fts, first-wins dedup)"
            );
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

        // The candidate cap (decision 76 (d)), AFTER the same-fact
        // collapse (decision 77, S3-F8): a capped list truncated BEFORE
        // the collapse could shrink to fewer candidates than the cap
        // intends; the cap counts the post-dedup, post-collapse
        // candidates presented to the gate.
        if let Some(deep) = &self.deep {
            candidates.truncate(deep.candidate_cap as usize);
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
        let gate_result = self.gate.select_with_context(&input, context_view).await;

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

/// Decision 76: the Section 9.3 dedup key of a deep-source candidate,
/// in the SAME pipe shape as `NeighborEdge::edge_id`
/// (`{source_id}|{relationship_name}|{target_id}|{valid_at}`, RFC 3339).
/// The `CandidateEdge.edge_id` carries the opaque `EdgeId` JSON
/// encoding (decision 75, the manual-ops namespace); the recall
/// pipeline and the `injected_memories` dedup table speak the pipe
/// shape since M5, so deep-source candidates MUST convert here — one
/// edge surfaced by two sources dedups to one candidate, and an
/// injected deep candidate dedups against later wakes.
fn deep_candidate_edge_id(edge: &CandidateEdge) -> String {
    let valid_at = edge
        .valid_at
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| format!("{:?}", edge.valid_at));
    format!(
        "{}|{}|{}|{}",
        edge.source_id, edge.relationship_name, edge.target_id, valid_at
    )
}

/// Converts one deep-source [`CandidateEdge`] into a
/// [`RecallCandidate`] and appends it under the shared first-wins
/// edge-id dedup of `recall_inner` (module docs: source order shallow,
/// then expansion, then fts).
fn push_deep_candidate(
    candidates: &mut Vec<RecallCandidate>,
    seen_edge_ids: &mut HashSet<String>,
    edge: CandidateEdge,
) {
    let edge_id = deep_candidate_edge_id(&edge);
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
        // The pre-72 delta-only shape (no shared context view).
        Box::pin(async move { self.recall_inner(chat_id, new_messages, None).await })
    }

    fn recall_with_context<'a>(
        &'a self,
        chat_id: &'a str,
        new_messages: &'a [GateMessage],
        context_view: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<RecallOutcome, CoreError>> + Send + 'a>,
    > {
        Box::pin(async move { self.recall_inner(chat_id, new_messages, context_view).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tamako_core::embedding::EmbeddingError;
    use tamako_memory::identifiers::concept_id;
    use tamako_memory::{EdgeId, LbugBackend, MemoryBatch, MemoryEdge, MemoryNode, NodeType};
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
        let prompt = render_recall_prompt(&sample_input(), None);
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
    fn without_a_context_view_the_prompt_is_byte_identical_to_pre_72() {
        // The `gate_context` kill switch (decision 72, specs.md Section
        // 9.2): `None` restores the delta-only input. The expected
        // bytes are the pre-72 render, written out literally.
        let prompt = render_recall_prompt(&sample_input(), None);
        let expected = "\
New messages (XML-tagged content):
<msg from=\"u1\" at=\"13:01\" id=\"41\">has anyone tried the new cafe?</msg>
<msg from=\"u2\" at=\"13:01\" id=\"42\">the espresso is great</msg>

Candidate memories (number, text, validity start):
1. Alice likes espresso. (since 2026-08-07)
2. Bob plays go. (since 2025-12-31)
";
        assert_eq!(prompt, expected);
    }

    #[test]
    fn the_context_view_renders_ahead_of_the_messages_and_candidates() {
        // Decision 72 (specs.md Section 9.2): the shared context view
        // is a clean PREFIX — the view section first, then the new
        // messages, then the numbered candidates, then the orientation
        // note LAST. The byte-for-byte assertion pins the section
        // order and the exact headers; the candidate list rendering
        // and the 1-based index contract are byte-stable.
        let view = "<msg from=\"u9\" at=\"12:58\" id=\"39\">lunch tomorrow?</msg>";
        let prompt = render_recall_prompt(&sample_input(), Some(view));
        let expected = "\
Context so far:
<msg from=\"u9\" at=\"12:58\" id=\"39\">lunch tomorrow?</msg>

New messages (XML-tagged content):
<msg from=\"u1\" at=\"13:01\" id=\"41\">has anyone tried the new cafe?</msg>
<msg from=\"u2\" at=\"13:01\" id=\"42\">the espresso is great</msg>

Candidate memories (number, text, validity start):
1. Alice likes espresso. (since 2026-08-07)
2. Bob plays go. (since 2025-12-31)

The context section is read-only orientation; the selection names candidate numbers only.
";
        assert_eq!(prompt, expected);
    }

    #[test]
    fn an_empty_context_view_renders_the_explicit_empty_marker() {
        // Decision 72: an empty view still renders the section with
        // the `(none yet)` marker, so the prompt SHAPE is stable
        // across wakes (the same constant-prefix choice as the
        // participation gate — the header and marker constants are
        // shared single-source).
        let prompt = render_recall_prompt(&sample_input(), Some(""));
        assert!(prompt.starts_with("Context so far:\n(none yet)\n\nNew messages"));
        assert!(prompt.ends_with(&format!("{RECALL_CONTEXT_NOTE}\n")));
    }

    #[test]
    fn the_index_contract_is_unchanged_with_a_context_view() {
        // The RecallSelection index contract is byte-stable under
        // decision 72: the candidates keep their 1-based numbers and
        // the wire-number conversion is untouched.
        let prompt = render_recall_prompt(&sample_input(), Some("the view bytes"));
        assert!(prompt.contains("1. Alice likes espresso. (since 2026-08-07)"));
        assert!(prompt.contains("2. Bob plays go. (since 2025-12-31)"));
        let selection = RecallSelection {
            selected: vec![2, 1],
            reason: "r".to_string(),
        };
        assert_eq!(zero_based_indices(&selection), vec![1, 0]);
        // The note names the contract explicitly.
        assert!(RECALL_CONTEXT_NOTE.contains("read-only orientation"));
        assert!(RECALL_CONTEXT_NOTE.contains("candidate numbers"));
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
        // Both calls went through `select`: no context view (the
        // pre-72 delta-only shape).
        assert_eq!(gate.context_views(), vec![None, None]);
    }

    #[tokio::test]
    async fn the_scripted_gate_records_the_context_view() {
        // Decision 72: the view is observable through the same capture
        // pattern as the inputs.
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![0]]);
        let first = gate
            .select_with_context(&sample_input(), Some("the view bytes"))
            .await
            .expect("first");
        assert_eq!(first, vec![0]);
        gate.select_with_context(&sample_input(), None)
            .await
            .expect("second");
        assert_eq!(
            gate.context_views(),
            vec![Some("the view bytes".to_string()), None]
        );
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
    async fn recall_with_context_forwards_the_view_to_the_relevance_gate() {
        // Decision 72 plumbing: the view the actor hands to
        // `recall_with_context` reaches the relevance gate byte-for-byte;
        // the plain `recall` entry is the pre-72 delta-only shape.
        let (_dir, store, memory) = backend().await;
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![], vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "espresso?")];
        recall
            .recall_with_context(CHAT, &messages, Some("the view bytes"))
            .await
            .expect("recall with context");
        recall.recall(CHAT, &messages).await.expect("recall");

        // Both calls produced candidates, so the gate ran twice.
        assert_eq!(recall.gate.call_count(), 2);
        assert_eq!(
            recall.gate.context_views(),
            vec![Some("the view bytes".to_string()), None]
        );
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

    // --- Decision 76: the deep candidate pipeline. Scripted embedding
    // provider + real tempdir Store (the vec0 node_embeddings sidecar
    // and the plain edge_texts sidecar, one open group) + real
    // LbugBackend (the decision-76 two_hop_edges / edges_by_ids reads)
    // — the seeding style of the resolve.rs step-3 fixtures. ---

    /// A scripted core embedding provider: pops one batch result per
    /// `embed_texts` call (FIFO), records every call's texts (the same
    /// double shape as the resolve.rs ScriptedEmbedder).
    struct ScriptedEmbedder {
        batches: Mutex<VecDeque<Result<Vec<Vec<f32>>, String>>>,
        /// A permanent failure message: every call fails with it.
        failure: Option<String>,
        calls: Mutex<Vec<Vec<String>>>,
    }

    impl ScriptedEmbedder {
        /// Every `embed_texts` call answers with the next batch (in
        /// order). An exhausted queue fails the call.
        fn with_batches(batches: Vec<Vec<Vec<f32>>>) -> Self {
            ScriptedEmbedder {
                batches: Mutex::new(batches.into_iter().map(Ok).collect::<VecDeque<_>>()),
                failure: None,
                calls: Mutex::new(Vec::new()),
            }
        }

        /// Every `embed_texts` call fails.
        fn failing(message: &str) -> Self {
            ScriptedEmbedder {
                batches: Mutex::new(VecDeque::new()),
                failure: Some(message.to_string()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len()
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    impl EmbeddingProvider for ScriptedEmbedder {
        fn embed<'a>(
            &'a self,
            text: &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<f32>, EmbeddingError>> + Send + 'a>,
        > {
            Box::pin(async move {
                let texts = [text.to_string()];
                let mut batches = self.embed_texts(&texts).await?;
                Ok(batches.remove(0))
            })
        }

        fn embed_texts<'a>(
            &'a self,
            texts: &'a [String],
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Vec<Vec<f32>>, EmbeddingError>> + Send + 'a,
            >,
        > {
            self.calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(texts.to_vec());
            let result = match &self.failure {
                Some(message) => Err(message.clone()),
                None => self
                    .batches
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .pop_front()
                    .unwrap_or_else(|| Err("scripted embedder: exhausted queue".to_string())),
            };
            Box::pin(async move { result.map_err(EmbeddingError::Provider) })
        }
    }

    /// The pinned sidecar dimension (decision 66).
    const DIMS: usize = crate::endpoint::EMBEDDING_DIMS;

    /// The unit basis vector of one dimension (DIMS wide).
    fn unit_vector(dim: usize) -> Vec<f32> {
        let mut vector = vec![0.0; DIMS];
        vector[dim] = 1.0;
        vector
    }

    /// A unit vector whose cosine similarity with `unit_vector(0)` is
    /// exactly `cosine` (up to f32 precision).
    fn tilted_vector(cosine: f32, dim: usize) -> Vec<f32> {
        let mut vector = vec![0.0; DIMS];
        vector[0] = cosine;
        vector[dim] = (1.0 - cosine * cosine).sqrt();
        vector
    }

    /// The deep knobs of one test: the decision-73 default threshold
    /// (0.80) and the given candidate cap.
    fn deep_config(provider: Arc<ScriptedEmbedder>, candidate_cap: u32) -> DeepRecallConfig {
        DeepRecallConfig {
            provider,
            vector_candidate_threshold: 0.80,
            candidate_cap,
        }
    }

    /// A fact edge with RECENT timestamps: the decision-76 two-hop
    /// expansion applies the 90-day window against the real
    /// `now_utc()`, so expansion-test edges seed at the wall clock
    /// (not the fixed `NOW` of the shallow tests).
    fn recent_fact_edge(
        source_id: &str,
        target_id: &str,
        relationship: &str,
        text: &str,
    ) -> MemoryEdge {
        let now = OffsetDateTime::now_utc();
        MemoryEdge {
            valid_at: now,
            created_at: now,
            updated_at: now,
            ..fact_edge(source_id, target_id, relationship, text)
        }
    }

    /// The opaque `EdgeId` JSON of one seeded edge — the key shape the
    /// edge_texts sidecar stores (the store never parses it).
    fn edge_json_id(edge: &MemoryEdge) -> String {
        EdgeId {
            source_id: edge.source_id.clone(),
            relationship_name: edge.relationship_name.clone(),
            target_id: edge.target_id.clone(),
            valid_at: edge.valid_at,
        }
        .encode()
    }

    /// Seeds one unconnected pair of concept nodes with one edge
    /// between them (an fts/expansion fixture: no entry reaches the
    /// pair through the shallow path).
    async fn seed_detached_fact(
        memory: &LbugBackend,
        name_a: &str,
        name_b: &str,
        text: &str,
    ) -> MemoryEdge {
        let node_a = concept_node(name_a);
        let node_b = concept_node(name_b);
        let edge = recent_fact_edge(&node_a.id, &node_b.id, "related_to", text);
        seed(memory, vec![node_a, node_b], vec![edge.clone()]).await;
        edge
    }

    #[tokio::test]
    async fn the_vector_entry_accepts_at_or_above_the_threshold_and_rejects_below() {
        // Decision 76 (a): KNN per term, similarity = 1.0 - cosine
        // distance, acceptance at or above vector_candidate_threshold
        // (0.80). The accepted node becomes an entry; its edges surface
        // through the two-hop expansion.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        // The accepted node (similarity 0.85) carries a fact; the
        // rejected node (similarity 0.50) carries another.
        let near = concept_node("near-topic");
        let near_target = concept_node("near-target");
        let near_edge = recent_fact_edge(
            &near.id,
            &near_target.id,
            "related_to",
            "Espresso pairs with cake.",
        );
        let far = concept_node("far-topic");
        let far_target = concept_node("far-target");
        let far_edge =
            recent_fact_edge(&far.id, &far_target.id, "related_to", "Go is a board game.");
        seed(
            &memory,
            vec![near.clone(), near_target, far.clone(), far_target],
            vec![near_edge, far_edge],
        )
        .await;
        store
            .upsert_node_embedding(&near.id, &tilted_vector(0.85, 1))
            .expect("seed near embedding");
        store
            .upsert_node_embedding(&far.id, &tilted_vector(0.5, 1))
            .expect("seed far embedding");

        // One candidate term ("espresso"); the batched call answers
        // with the query vector unit_vector(0).
        let embedder = Arc::new(ScriptedEmbedder::with_batches(vec![vec![unit_vector(0)]]));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5)
            .with_deep_recall(deep_config(embedder.clone(), 40));
        let messages = vec![gate_message(1, "u9999", "espresso")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        // ONE batched embeddings call for the wake, with the one term.
        assert_eq!(embedder.call_count(), 1);
        assert_eq!(embedder.calls(), vec![vec!["espresso".to_string()]]);
        let texts = presented_texts(&recall.gate);
        // The accepted node's edge surfaced through the expansion ...
        assert_eq!(
            texts
                .iter()
                .filter(|text| *text == "Espresso pairs with cake.")
                .count(),
            1
        );
        // ... the rejected node's edge did not.
        assert!(!texts.contains(&"Go is a board game.".to_string()));
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn the_two_hop_expansion_surfaces_a_hop_two_edge() {
        // Decision 76 (a): the hop-2 edge of a shallow entry's neighbor
        // is a candidate; the hop-1 edge the shallow path already
        // fetched dedups to one.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let person = person_node("u42", "Alice");
        let topic = concept_node("espresso");
        let pairing = concept_node("cake");
        let hop_one =
            recent_fact_edge(&person.id, &topic.id, "related_to", "Alice likes espresso.");
        let hop_two = recent_fact_edge(
            &topic.id,
            &pairing.id,
            "related_to",
            "Espresso pairs with cake.",
        );
        seed(
            &memory,
            vec![person, topic, pairing],
            vec![hop_one, hop_two],
        )
        .await;

        // "ok" is a stopword: no candidate terms, so the embed
        // provider is never called (decision 76 (f)); the expansion
        // still runs from the shallow entries.
        let embedder = Arc::new(ScriptedEmbedder::with_batches(Vec::new()));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5)
            .with_deep_recall(deep_config(embedder.clone(), 40));
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(embedder.call_count(), 0);
        let texts = presented_texts(&recall.gate);
        // The hop-1 edge exactly once (shallow source won the dedup) ...
        assert_eq!(
            texts
                .iter()
                .filter(|text| *text == "Alice likes espresso.")
                .count(),
            1
        );
        // ... and the hop-2 edge surfaced.
        assert_eq!(
            texts
                .iter()
                .filter(|text| *text == "Espresso pairs with cake.")
                .count(),
            1
        );
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn the_fts_source_hits_and_hydrates_a_valid_edge() {
        // Decision 76 (a)/(c): a candidate term matching an edge_texts
        // row (the "who discussed X" pattern) hydrates through
        // edges_by_ids — valid edges only; a stale sidecar row drops.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let edge = seed_detached_fact(
            &memory,
            "debate",
            "coffee",
            "The group debated 咖啡 prices.",
        )
        .await;
        store
            .upsert_edge_text(&edge_json_id(&edge), "The group debated 咖啡 prices.")
            .expect("seed the edge text");
        // A stale row: no graph edge carries this natural key, so the
        // hydration drops it silently (Section 7.6 step 6).
        let stale = EdgeId {
            source_id: concept_id("gone"),
            relationship_name: "related_to".to_string(),
            target_id: concept_id("stale"),
            valid_at: OffsetDateTime::now_utc(),
        }
        .encode();
        store
            .upsert_edge_text(&stale, "The 咖啡 stale row.")
            .expect("seed the stale edge text");

        // The two-character CJK term (the FTS5-trigram hole of
        // decision 76 (c)) hits the plain LIKE sidecar. No embeddings
        // are seeded, so the vector entry accepts nothing.
        let embedder = Arc::new(ScriptedEmbedder::with_batches(vec![vec![unit_vector(0)]]));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall =
            ShallowRecall::new(store, memory, gate, 5).with_deep_recall(deep_config(embedder, 40));
        let messages = vec![gate_message(1, "u9999", "咖啡")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        let candidates = &recall.gate.inputs()[0].candidates;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].edge_text, "The group debated 咖啡 prices.");
        // The hydrated candidate carries the Section 9.3 pipe-shaped
        // dedup key (not the opaque JSON of the sidecar).
        assert_eq!(
            candidates[0].edge_id,
            format!(
                "{}|related_to|{}|{}",
                concept_id("debate"),
                concept_id("coffee"),
                candidates[0]
                    .valid_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .expect("rfc3339")
            )
        );
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn an_edge_surfaced_by_two_sources_is_one_candidate() {
        // The cross-source dedup keys on the Section 9.3 pipe-shaped
        // edge id: one edge reached by the shallow neighbor fetch AND
        // the fts sidecar (AND the expansion hop 1) is ONE candidate.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let person = person_node("u42", "Alice");
        let topic = concept_node("espresso");
        let edge = recent_fact_edge(&person.id, &topic.id, "related_to", "Alice likes espresso.");
        seed(&memory, vec![person, topic], vec![edge.clone()]).await;
        store
            .upsert_edge_text(&edge_json_id(&edge), "Alice likes espresso.")
            .expect("seed the edge text");

        let embedder = Arc::new(ScriptedEmbedder::with_batches(vec![vec![unit_vector(0)]]));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall =
            ShallowRecall::new(store, memory, gate, 5).with_deep_recall(deep_config(embedder, 40));
        let messages = vec![gate_message(1, "u42", "espresso")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        let texts = presented_texts(&recall.gate);
        assert_eq!(texts, vec!["Alice likes espresso.".to_string()]);
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn the_candidate_cap_truncates_in_source_order_before_the_gate() {
        // Decision 76 (d): the merged pool caps at
        // recall_candidate_cap BEFORE the relevance gate; the first
        // candidates in source order (here: shallow fetch order,
        // created_at descending) survive.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let person = person_node("u42", "Alice");
        let now = OffsetDateTime::now_utc();
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
            // created_at descending: hobby-2 comes first in the
            // neighbor fetch (Section 8.2).
            let mut edge = recent_fact_edge(&person.id, &concept.id, "related_to", text);
            edge.created_at = now + time::Duration::seconds(index as i64);
            edges.push(edge);
            nodes.push(concept);
        }
        seed(&memory, nodes, edges).await;

        let embedder = Arc::new(ScriptedEmbedder::with_batches(Vec::new()));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall =
            ShallowRecall::new(store, memory, gate, 5).with_deep_recall(deep_config(embedder, 2));
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        let texts = presented_texts(&recall.gate);
        assert_eq!(
            texts,
            vec![
                "Alice reads books.".to_string(),
                "Alice plays go.".to_string(),
            ]
        );
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn deep_recall_off_is_byte_identical_to_the_shallow_path() {
        // The `deep_recall = false` form: no DeepRecallConfig wired.
        // Deep-source artifacts that WOULD match (an edge_texts hit, an
        // acceptable embedding, a hop-2 edge) add NOTHING — the
        // candidate set is the pre-76 shallow set.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let person = person_node("u42", "Alice");
        let topic = concept_node("espresso");
        let pairing = concept_node("cake");
        let hop_one =
            recent_fact_edge(&person.id, &topic.id, "related_to", "Alice likes espresso.");
        let hop_two = recent_fact_edge(
            &topic.id,
            &pairing.id,
            "related_to",
            "Espresso pairs with cake.",
        );
        seed(
            &memory,
            vec![person, topic.clone(), pairing],
            vec![hop_one.clone(), hop_two],
        )
        .await;
        store
            .upsert_edge_text(&edge_json_id(&hop_one), "Alice likes espresso.")
            .expect("seed the edge text");
        store
            .upsert_node_embedding(&topic.id, &unit_vector(0))
            .expect("seed an embedding");

        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5);
        let messages = vec![gate_message(1, "u42", "espresso")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        // Exactly the shallow candidate: no hop-2 edge, no fts
        // duplicate, no vector entry.
        assert_eq!(
            presented_texts(&recall.gate),
            vec!["Alice likes espresso.".to_string()]
        );
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn without_candidate_terms_the_embed_provider_is_never_called() {
        // Decision 76 (f): zero candidate terms never call the model —
        // not even the batched embeddings call.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        seed_person_fact(&memory, "u42", "Alice", "Alice likes espresso.").await;

        let embedder = Arc::new(ScriptedEmbedder::with_batches(Vec::new()));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5)
            .with_deep_recall(deep_config(embedder.clone(), 40));
        // Stopword-only text: no terms at all.
        let messages = vec![gate_message(1, "u42", "ok ok thanks")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        assert_eq!(embedder.call_count(), 0);
        assert_eq!(
            presented_texts(&recall.gate),
            vec!["Alice likes espresso.".to_string()]
        );
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn a_failing_embed_call_degrades_the_vector_entry_only() {
        // Failure discipline (decision 76): an embeddings-endpoint
        // failure degrades the vector entry to empty with one WARN; the
        // shallow and expansion sources run, and the wake never fails.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let person = person_node("u42", "Alice");
        let topic = concept_node("espresso");
        let pairing = concept_node("cake");
        let hop_one =
            recent_fact_edge(&person.id, &topic.id, "related_to", "Alice likes espresso.");
        let hop_two = recent_fact_edge(
            &topic.id,
            &pairing.id,
            "related_to",
            "Espresso pairs with cake.",
        );
        seed(
            &memory,
            vec![person, topic, pairing],
            vec![hop_one, hop_two],
        )
        .await;
        // An embedding that WOULD be accepted, had the call succeeded.
        let detached =
            seed_detached_fact(&memory, "vector-only", "target", "A vector-only fact.").await;
        store
            .upsert_node_embedding(&detached.source_id, &unit_vector(0))
            .expect("seed an embedding");

        let embedder = Arc::new(ScriptedEmbedder::failing("the endpoint is down"));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall = ShallowRecall::new(store, memory, gate, 5)
            .with_deep_recall(deep_config(embedder.clone(), 40));
        let messages = vec![gate_message(1, "u42", "espresso")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        // The call was attempted (a term exists) and failed; no error
        // propagated.
        assert_eq!(embedder.call_count(), 1);
        let texts = presented_texts(&recall.gate);
        assert!(texts.contains(&"Alice likes espresso.".to_string()));
        assert!(texts.contains(&"Espresso pairs with cake.".to_string()));
        // The vector-only fact stayed out: the vector entry degraded.
        assert!(!texts.contains(&"A vector-only fact.".to_string()));
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn a_failing_store_read_degrades_the_store_sources_only() {
        // Failure discipline (decision 76): the store-backed deep
        // sources (KNN and edge_texts, both single-open-group reads)
        // degrade to empty with DEBUG lines when the store contract
        // fails; the memory-backed expansion and the shallow path are
        // unaffected, and the wake never fails.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let person = person_node("u42", "Alice");
        let topic = concept_node("espresso");
        let pairing = concept_node("cake");
        let hop_one =
            recent_fact_edge(&person.id, &topic.id, "related_to", "Alice likes espresso.");
        let hop_two = recent_fact_edge(
            &topic.id,
            &pairing.id,
            "related_to",
            "Espresso pairs with cake.",
        );
        seed(
            &memory,
            vec![person, topic, pairing],
            vec![hop_one.clone(), hop_two],
        )
        .await;
        store
            .upsert_edge_text(&edge_json_id(&hop_one), "Alice likes espresso.")
            .expect("seed the edge text");
        store
            .upsert_node_embedding(&hop_one.target_id, &unit_vector(0))
            .expect("seed an embedding");
        // Break the single-open-group contract AFTER seeding: every
        // with_single_group_conn read now fails (AmbiguousGroup). The
        // chat-scoped reads (list_injected_memories) keep working.
        store.open_group("recall_test_other").expect("second group");

        let embedder = Arc::new(ScriptedEmbedder::with_batches(vec![vec![unit_vector(0)]]));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall =
            ShallowRecall::new(store, memory, gate, 5).with_deep_recall(deep_config(embedder, 40));
        let messages = vec![gate_message(1, "u42", "espresso")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        let texts = presented_texts(&recall.gate);
        // The shallow edge and the expansion hop-2 edge survived; the
        // store-backed sources contributed nothing (the fts hit would
        // have deduped to the same edge anyway — the count pins it).
        assert_eq!(
            texts
                .iter()
                .filter(|text| *text == "Alice likes espresso.")
                .count(),
            1
        );
        assert_eq!(
            texts
                .iter()
                .filter(|text| *text == "Espresso pairs with cake.")
                .count(),
            1
        );
        assert_eq!(texts.len(), 2);
        assert_eq!(outcome, RecallOutcome::default());
    }

    // ---- Decision 77: panic containment (M3) and the
    // collapse-before-cap fix (S3-F8). ----

    /// A provider double whose every `embed_texts` call PANICS (the M3
    /// fixture: an inline provider panic must not unwind into the wake
    /// task).
    struct PanickingEmbedder;

    impl EmbeddingProvider for PanickingEmbedder {
        fn embed<'a>(
            &'a self,
            _text: &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<f32>, EmbeddingError>> + Send + 'a>,
        > {
            panic!("the provider exploded")
        }

        fn embed_texts<'a>(
            &'a self,
            _texts: &'a [String],
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Vec<Vec<f32>>, EmbeddingError>> + Send + 'a,
            >,
        > {
            panic!("the provider exploded")
        }
    }

    #[tokio::test]
    async fn a_panicking_vector_entry_provider_skips_the_source_and_the_wake_succeeds() {
        // Decision 77 (M3): a PANICKING batched embeddings call
        // degrades exactly like its failure path — one WARN, the
        // vector entry is skipped — and the wake continues with the
        // shallow candidates (here: the seeded shallow fact).
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let person = person_node("u42", "Alice");
        let topic = concept_node("espresso");
        let edge = recent_fact_edge(&person.id, &topic.id, "related_to", "Alice likes espresso.");
        seed(&memory, vec![person, topic], vec![edge]).await;

        let embedder = Arc::new(PanickingEmbedder);
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall =
            ShallowRecall::new(store, memory, gate, 5).with_deep_recall(DeepRecallConfig {
                provider: embedder,
                vector_candidate_threshold: 0.80,
                candidate_cap: 40,
            });
        let messages = vec![gate_message(1, "u42", "espresso")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        // The shallow candidate still reached the gate; the wake
        // never failed on the panic.
        let texts = presented_texts(&recall.gate);
        assert_eq!(texts, vec!["Alice likes espresso.".to_string()]);
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[tokio::test]
    async fn the_candidate_cap_lands_after_the_same_fact_collapse() {
        // Decision 77 (S3-F8): pre-collapse truncation would cap the
        // fetched pool [newer dup, older dup, distinct] at 2 and the
        // collapse would then shrink it to ONE candidate, dropping the
        // distinct fact. With the cap AFTER the collapse the distinct
        // fact survives under the cap.
        let (_dir, store, memory) = backend().await;
        store.open_group(CHAT).expect("open group");
        let person = person_node("u42", "Alice");
        let topic = concept_node("espresso");
        let hobby = concept_node("go");
        let now = OffsetDateTime::now_utc();
        // Two edges of the SAME fact (same source/relationship/target,
        // different valid_at) plus one distinct fact. created_at
        // controls the neighbor-fetch order (descending, Section 8.2):
        // newer dup first, older dup second, the distinct fact LAST.
        let mut older_dup =
            recent_fact_edge(&person.id, &topic.id, "related_to", "Alice likes espresso.");
        older_dup.valid_at = now - time::Duration::days(1);
        older_dup.created_at = now + time::Duration::seconds(1);
        let mut newer_dup =
            recent_fact_edge(&person.id, &topic.id, "related_to", "Alice likes espresso.");
        newer_dup.valid_at = now;
        newer_dup.created_at = now + time::Duration::seconds(2);
        let mut distinct = recent_fact_edge(&person.id, &hobby.id, "related_to", "Alice plays go.");
        distinct.created_at = now;
        seed(
            &memory,
            vec![person, topic, hobby],
            vec![older_dup, newer_dup, distinct],
        )
        .await;

        // "ok" is a stopword: no candidate terms, the provider is
        // never called; cap 2.
        let embedder = Arc::new(ScriptedEmbedder::with_batches(Vec::new()));
        let gate = ScriptedRelevanceGate::with_selections(vec![vec![]]);
        let recall =
            ShallowRecall::new(store, memory, gate, 5).with_deep_recall(deep_config(embedder, 2));
        let messages = vec![gate_message(1, "u42", "ok")];
        let outcome = recall.recall(CHAT, &messages).await.expect("recall");

        // Post-collapse the pool is [espresso fact, go fact]; both
        // survive the cap of 2 (pre-collapse truncation would have
        // dropped "Alice plays go.").
        let texts = presented_texts(&recall.gate);
        assert_eq!(
            texts,
            vec![
                "Alice likes espresso.".to_string(),
                "Alice plays go.".to_string(),
            ]
        );
        assert_eq!(outcome, RecallOutcome::default());
    }
}
