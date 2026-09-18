//! jev-memory experiment fixture extractor (docs/jev-memory-benchmark.md).
//!
//! Reads a COPY of the production data root (never the live root) and
//! writes JSONL fixtures for the Python benchmark driver:
//!
//!   uc1_pairs.jsonl   same-kind node pairs, cosine sim >= 0.75
//!                     (>= 0.88 = production confirm band; 0.75-0.88 =
//!                     the never-confirmed missed-merge band)
//!   uc2_pairs.jsonl   merge-scan replica: sim >= 0.90, known_as pairs out
//!   edges.jsonl       all graph edges (both id shapes, endpoint names)
//!   uc3_windows.jsonl replayed wake windows with rebuilt recall candidates
//!   meta.json         counts and configuration of the extraction
//!
//! Read discipline: store.db opens ride `Store::open_group_read_only`
//! plus two dedicated SQLITE_OPEN_READ_ONLY connections (injected_memories
//! and the vec0 full scan have no pub read API); the lbug backend runs
//! its idempotent DDL on the copy (no read-only mode exists —
//! lbug_backend.rs:505-530). No file is ever written.
//!
//! Two edge-id shapes exist in production (empirically verified on the
//! snapshot): the sidecar `edge_texts` rows and `CandidateEdge.edge_id`
//! use `EdgeId::encode` (compact JSON), while `injected_memories.edge_id`
//! uses the pipe-delimited natural key. Fixture candidates carry the
//! PIPE shape (the injected join target); edges.jsonl carries both.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use tamako_agent::recall::{
    candidate_terms, render_recall_prompt, RecallCandidate, RelevanceInput,
};
use tamako_core::wake::GateMessage;
use tamako_memory::{CandidateEdge, EdgeId, LbugBackend, MemoryBackend};
use tamako_store::{Direction, MessageRow, Store};

/// Pair-scan floor: the [0.75, 0.88) band below the production confirm
/// threshold is the missed-merge zone this experiment measures.
const PAIR_SIM_FLOOR: f64 = 0.75;
/// config.rs:376 vector_candidate_threshold (production confirm band).
const CONFIRM_BAND: f64 = 0.88;
/// config.rs:378 merge_candidate_threshold.
const MERGE_BAND: f64 = 0.90;
/// Per-node KNN fan-out of the pair scan.
const PAIR_SCAN_K: usize = 32;
/// recall.rs:197 VECTOR_ENTRY_KNN_K.
const VECTOR_ENTRY_KNN_K: usize = 5;
/// recall.rs:190 MAX_PRESENTED_CANDIDATES.
const MAX_PRESENTED_CANDIDATES: usize = 40;
/// backend.rs:114 NEIGHBOR_EXPANSION_LIMIT.
const NEIGHBOR_EXPANSION_LIMIT: usize = 500;
/// Production wake_msg_count default (config.rs:330).
const WINDOW_MESSAGES: usize = 5;
/// Evenly spaced replay windows per active group.
const RANDOM_WINDOWS_PER_GROUP: usize = 8;

const EMBEDDING_MODEL: &str = "google/gemini-embedding-2";
const OPENROUTER_EMBEDDINGS_URL: &str = "https://openrouter.ai/api/v1/embeddings";

struct NodeRec {
    id: String,
    name: String,
    kind: String,
    description: String,
}

#[derive(Serialize, Clone)]
struct PairOut {
    chat_id: String,
    a_id: String,
    b_id: String,
    a_name: String,
    a_desc: String,
    b_name: String,
    b_desc: String,
    sim: f64,
    kind: String,
}

#[derive(Serialize)]
struct EdgeOut {
    chat_id: String,
    edge_id: String,
    json_id: String,
    source_name: String,
    target_name: String,
    relationship: String,
    edge_text: String,
    valid_at: String,
    invalid: bool,
}

#[derive(Serialize)]
struct CandidateOut {
    edge_id: String,
    edge_text: String,
    valid_at: String,
}

#[derive(Serialize)]
struct WindowOut {
    chat_id: String,
    window_id: String,
    origin: String,
    prompt_text: String,
    candidates: Vec<CandidateOut>,
    injected_edge_ids: Vec<String>,
}

/// Parses one lbug display-string TIMESTAMP (RFC 3339, possibly with a
/// space instead of `T`). Unparseable is loud — the edge-id join needs
/// µs fidelity (decision 117).
fn parse_ts(raw: &str) -> Result<OffsetDateTime> {
    let trimmed = raw.trim();
    if let Ok(ts) = OffsetDateTime::parse(trimmed, &Rfc3339) {
        return Ok(ts);
    }
    let normalized = trimmed.replacen(' ', "T", 1);
    OffsetDateTime::parse(&normalized, &Rfc3339)
        .with_context(|| format!("unparseable lbug timestamp display: {raw:?}"))
}

/// The pipe-delimited natural-key id (injected_memories shape) of one
/// hydrated candidate: decode the JSON id, re-render pipe.
fn pipe_id_of(candidate: &CandidateEdge) -> Result<String> {
    let key = EdgeId::decode(&candidate.edge_id)
        .with_context(|| format!("candidate edge id does not decode: {}", candidate.edge_id))?;
    let valid_rfc = key
        .valid_at
        .format(&Rfc3339)
        .unwrap_or_else(|_| format!("{:?}", key.valid_at));
    Ok(format!(
        "{}|{}|{}|{}",
        key.source_id, key.relationship_name, key.target_id, valid_rfc
    ))
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Minimal GateMessage content renderer (context.rs:730-763 shape:
/// `<msg from=".." at="HH:MM" id="N">text</msg>`). The rare optional
/// attributes (user=, reply=, mention, fwd=) are omitted — documented
/// simplification of the replay.
fn render_msg_content(row: &MessageRow) -> String {
    let hhmm = row
        .timestamp
        .format(&time::macros::format_description!("[hour]:[minute]"))
        .unwrap_or_else(|_| "00:00".to_string());
    format!(
        "<msg from=\"{}\" at=\"{}\" id=\"{}\">{}</msg>",
        xml_escape(&row.sender_display_name),
        hhmm,
        row.id,
        xml_escape(&row.text)
    )
}

/// One embedding call, single-text shape (decision 81: OpenRouter routes
/// single-text to the ZDR-compliant provider; never batch). None on any
/// failure — the caller degrades the vector entry to FTS-only and the
/// degradation is visible in meta (windows carry their candidate count).
fn embed_single(api_key: &str, text: &str) -> Option<Vec<f32>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .ok()?;
    let body = serde_json::json!({"model": EMBEDDING_MODEL, "input": text});
    let resp = client
        .post(OPENROUTER_EMBEDDINGS_URL)
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let value: serde_json::Value = resp.json().ok()?;
    let vector: Vec<f32> = value
        .get("data")?
        .get(0)?
        .get("embedding")?
        .as_array()?
        .iter()
        .filter_map(|v| v.as_f64().map(|f| f as f32))
        .collect();
    if vector.is_empty() {
        None
    } else {
        Some(vector)
    }
}

struct InjectedRow {
    edge_id: String,
    created_at: OffsetDateTime,
}

/// injected_memories has no pub read API in tamako-store (write-side
/// dedup only); read it through a dedicated read-only connection.
fn read_injected(data_root: &Path, chat_id: &str) -> Result<Vec<InjectedRow>> {
    let path = data_root.join(chat_id).join("store.db");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let conn =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut stmt =
        conn.prepare("SELECT edge_id, created_at FROM injected_memories ORDER BY created_at, id")?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut out = Vec::with_capacity(rows.len());
    for (edge_id, created_raw) in rows {
        let created_at = OffsetDateTime::parse(&created_raw, &Rfc3339).with_context(|| {
            format!("injected_memories created_at not RFC3339: {created_raw:?}")
        })?;
        out.push(InjectedRow {
            edge_id,
            created_at,
        });
    }
    Ok(out)
}

/// vec0 full scan: node_embeddings answers plain SELECTs with the
/// float32 little-endian blob. Requires vec0 on the connection
/// (register_sqlite_vec is pub, store.rs:420).
fn read_all_vectors(data_root: &Path, chat_id: &str) -> Result<Vec<(String, Vec<f32>)>> {
    let path = data_root.join(chat_id).join("store.db");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let conn =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    tamako_store::register_sqlite_vec(&conn)?;
    let mut stmt = conn.prepare("SELECT node_id, embedding FROM node_embeddings")?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut out = Vec::with_capacity(rows.len());
    for (id, blob) in rows {
        let (chunks, []) = blob.as_chunks::<4>() else {
            anyhow::bail!("embedding blob length {} not divisible by 4", blob.len());
        };
        out.push((id, chunks.iter().map(|c| f32::from_le_bytes(*c)).collect()));
    }
    Ok(out)
}

struct GroupOut {
    uc1_pairs: Vec<PairOut>,
    uc2_pairs: Vec<PairOut>,
    edges: Vec<EdgeOut>,
    windows: Vec<WindowOut>,
    node_count: usize,
    embedded_count: usize,
    embedded_journal_count: usize,
    edge_text_ids: usize,
    edge_id_join_ok: usize,
    edge_id_join_miss: usize,
}

async fn extract_group(data_root: &Path, chat_id: &str, api_key: Option<&str>) -> Result<GroupOut> {
    let store = Store::new(data_root);
    store.open_group_read_only(chat_id)?;
    let memory = LbugBackend::new(data_root);

    // --- Nodes ---
    let node_rows = memory
        .query_rows(
            chat_id,
            "MATCH (n:Node) RETURN n.id, n.name, n.type, n.properties ORDER BY n.id",
        )
        .await?;
    let mut nodes: Vec<NodeRec> = Vec::with_capacity(node_rows.len());
    for row in &node_rows {
        let description = row
            .get(3)
            .filter(|p| !p.is_empty() && p.as_str() != "NULL")
            .and_then(|p| serde_json::from_str::<serde_json::Value>(p).ok())
            .and_then(|v| {
                v.get("description")
                    .and_then(|d| d.as_str().map(str::to_string))
            })
            .unwrap_or_default();
        nodes.push(NodeRec {
            id: row[0].clone(),
            name: row[1].clone(),
            kind: row[2].clone(),
            description,
        });
    }
    let node_by_id: HashMap<&str, &NodeRec> = nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    // --- Edges (full scan; both id shapes) ---
    let edge_rows = memory
        .query_rows(
            chat_id,
            "MATCH (s:Node)-[r:EDGE]->(t:Node) RETURN s.id, t.id, \
             r.relationship_name, r.valid_at, r.invalid_at, r.edge_text",
        )
        .await?;
    struct EdgeRec {
        json_id: String,
        pipe_id: String,
        source_id: String,
        target_id: String,
        relationship: String,
        valid_at: String,
        invalid: bool,
        edge_text: String,
        source_name: String,
        target_name: String,
    }
    let mut edges: Vec<EdgeRec> = Vec::with_capacity(edge_rows.len());
    for row in &edge_rows {
        let valid_at = parse_ts(&row[3])?;
        let invalid = row.get(4).is_some_and(|s| s != "NULL" && !s.is_empty());
        let json_id = EdgeId {
            source_id: row[0].clone(),
            relationship_name: row[2].clone(),
            target_id: row[1].clone(),
            valid_at,
        }
        .encode();
        let valid_rfc = valid_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| format!("{valid_at:?}"));
        let pipe_id = format!("{}|{}|{}|{}", row[0], row[2], row[1], valid_rfc);
        edges.push(EdgeRec {
            json_id,
            pipe_id,
            source_name: node_by_id
                .get(row[0].as_str())
                .map(|n| n.name.clone())
                .unwrap_or_default(),
            target_name: node_by_id
                .get(row[1].as_str())
                .map(|n| n.name.clone())
                .unwrap_or_default(),
            source_id: row[0].clone(),
            target_id: row[1].clone(),
            relationship: row[2].clone(),
            valid_at: valid_rfc,
            invalid,
            edge_text: row.get(5).cloned().unwrap_or_default(),
        });
    }

    // Join sanity: recomputed JSON ids vs the sidecar's stored ids.
    let sidecar_ids: HashSet<String> = store
        .list_edge_text_ids()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let json_set: HashSet<&str> = edges.iter().map(|e| e.json_id.as_str()).collect();
    let join_ok = sidecar_ids
        .iter()
        .filter(|id| json_set.contains(id.as_str()))
        .count();
    let join_miss = sidecar_ids.len() - join_ok;

    // --- UC-1 / UC-2 pair scan (raw vectors out, per-node KNN) ---
    let raw_vectors = read_all_vectors(data_root, chat_id)?;
    let embedded_journal = store.all_embedding_node_ids().unwrap_or_default().len();
    let mut best_sim: HashMap<(String, String), f64> = HashMap::new();
    for (id, vector) in &raw_vectors {
        for (other_id, distance) in store.knn_node_embeddings(vector, PAIR_SCAN_K)? {
            if &other_id == id {
                continue;
            }
            let sim = 1.0 - f64::from(distance);
            if sim < PAIR_SIM_FLOOR {
                continue;
            }
            let (a, b) = if *id < other_id {
                (id, &other_id)
            } else {
                (&other_id, id)
            };
            let entry = best_sim.entry((a.clone(), b.clone())).or_insert(sim);
            if sim > *entry {
                *entry = sim;
            }
        }
    }
    // known_as/also_known_as exclusion of the merge scan (merge.rs:242-245).
    let mut linked: HashSet<(String, String)> = HashSet::new();
    for e in &edges {
        if e.relationship == "known_as" || e.relationship == "also_known_as" {
            linked.insert((e.source_id.clone(), e.target_id.clone()));
            linked.insert((e.target_id.clone(), e.source_id.clone()));
        }
    }
    let mut uc1_pairs: Vec<PairOut> = Vec::new();
    let mut uc2_pairs: Vec<PairOut> = Vec::new();
    for ((a, b), sim) in &best_sim {
        let (Some(na), Some(nb)) = (node_by_id.get(a.as_str()), node_by_id.get(b.as_str())) else {
            continue;
        };
        if na.kind != nb.kind {
            continue;
        }
        let kind = na.kind.to_lowercase();
        if kind != "person" && kind != "concept" {
            continue;
        }
        let pair = PairOut {
            chat_id: chat_id.to_string(),
            a_id: a.clone(),
            b_id: b.clone(),
            a_name: na.name.clone(),
            a_desc: na.description.clone(),
            b_name: nb.name.clone(),
            b_desc: nb.description.clone(),
            sim: *sim,
            kind,
        };
        if pair.sim >= MERGE_BAND && !linked.contains(&(pair.a_id.clone(), pair.b_id.clone())) {
            uc2_pairs.push(pair.clone());
        }
        uc1_pairs.push(pair);
    }
    let by_sim = |x: &PairOut, y: &PairOut| {
        y.sim
            .partial_cmp(&x.sim)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.a_id.cmp(&y.a_id))
            .then_with(|| x.b_id.cmp(&y.b_id))
    };
    uc1_pairs.sort_by(by_sim);
    uc2_pairs.sort_by(by_sim);

    // --- Windows ---
    let messages = store.list_messages(chat_id)?;
    let injected = read_injected(data_root, chat_id)?;
    let mut windows: Vec<WindowOut> = Vec::new();

    // Injected-aligned: the WINDOW_MESSAGES inbound rows just before each
    // distinct injection instant (approximates the wake the injection
    // rode; documented simplification).
    let mut seen_stamps: HashSet<i64> = HashSet::new();
    for inj in &injected {
        if !seen_stamps.insert(inj.created_at.unix_timestamp()) {
            continue;
        }
        let window_msgs: Vec<&MessageRow> = messages
            .iter()
            .filter(|m| m.direction == Direction::Inbound && m.timestamp <= inj.created_at)
            .rev()
            .take(WINDOW_MESSAGES)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        if window_msgs.len() < 2 {
            continue;
        }
        let injected_ids: Vec<String> = injected
            .iter()
            .filter(|i| i.created_at.unix_timestamp() == inj.created_at.unix_timestamp())
            .map(|i| i.edge_id.clone())
            .collect();
        let window_id = format!("inj-{}", inj.created_at.unix_timestamp());
        if let Some(w) = build_window(
            &store,
            &memory,
            chat_id,
            &window_id,
            "injected",
            &window_msgs,
            injected_ids,
            api_key,
        )
        .await?
        {
            windows.push(w);
        }
    }

    // Evenly spaced windows: deterministic stride over inbound runs.
    let inbound: Vec<&MessageRow> = messages
        .iter()
        .filter(|m| m.direction == Direction::Inbound)
        .collect();
    if inbound.len() >= WINDOW_MESSAGES * 4 {
        let stride = (inbound.len() / (RANDOM_WINDOWS_PER_GROUP + 1)).max(1);
        for i in 0..RANDOM_WINDOWS_PER_GROUP {
            let end = (i + 1) * stride;
            if end < WINDOW_MESSAGES || end > inbound.len() {
                continue;
            }
            let window_msgs = &inbound[end - WINDOW_MESSAGES..end];
            let window_id = format!("rand-{chat_id}-{}", window_msgs[0].id);
            if let Some(w) = build_window(
                &store,
                &memory,
                chat_id,
                &window_id,
                "random",
                window_msgs,
                Vec::new(),
                api_key,
            )
            .await?
            {
                windows.push(w);
            }
        }
    }

    Ok(GroupOut {
        uc1_pairs,
        uc2_pairs,
        edges: edges
            .iter()
            .map(|e| EdgeOut {
                chat_id: chat_id.to_string(),
                edge_id: e.pipe_id.clone(),
                json_id: e.json_id.clone(),
                source_name: e.source_name.clone(),
                target_name: e.target_name.clone(),
                relationship: e.relationship.clone(),
                edge_text: e.edge_text.clone(),
                valid_at: e.valid_at.clone(),
                invalid: e.invalid,
            })
            .collect(),
        windows,
        node_count: nodes.len(),
        embedded_count: raw_vectors.len(),
        embedded_journal_count: embedded_journal,
        edge_text_ids: sidecar_ids.len(),
        edge_id_join_ok: join_ok,
        edge_id_join_miss: join_miss,
    })
}

/// Builds one replayed wake window: candidate assembly mirrors recall.rs
/// (production tokenizer + sidecar FTS + vector-entry KNN + two-hop
/// expansion, same-fact collapse, cap 40), then the byte-faithful prompt
/// render via the production pub fns.
#[allow(clippy::too_many_arguments)]
async fn build_window(
    store: &Store,
    memory: &LbugBackend,
    chat_id: &str,
    window_id: &str,
    origin: &str,
    window_msgs: &[&MessageRow],
    injected_ids: Vec<String>,
    api_key: Option<&str>,
) -> Result<Option<WindowOut>> {
    let texts: Vec<&str> = window_msgs.iter().map(|m| m.text.as_str()).collect();

    // FTS entry: production tokenizer (recall.rs:237) + sidecar search.
    let mut fts_ids: Vec<String> = Vec::new();
    let mut seen_fts: HashSet<String> = HashSet::new();
    for term in candidate_terms(&texts) {
        for id in store.search_edge_texts(&term).unwrap_or_default() {
            if seen_fts.insert(id.clone()) {
                fts_ids.push(id);
            }
        }
    }

    // Vector entry: embed the window text (single-text), KNN k=5.
    let entry_node_ids: Vec<String> = match api_key {
        Some(key) => {
            let key = key.to_string();
            let text = texts.join("\n");
            match tokio::task::spawn_blocking(move || embed_single(&key, &text)).await {
                Ok(Some(vector)) => store
                    .knn_node_embeddings(&vector, VECTOR_ENTRY_KNN_K)?
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect(),
                _ => Vec::new(),
            }
        }
        None => Vec::new(),
    };

    // Two-hop expansion over the entry nodes (production path).
    let mut candidates: Vec<CandidateEdge> = if entry_node_ids.is_empty() {
        Vec::new()
    } else {
        memory
            .two_hop_edges(
                chat_id,
                &entry_node_ids,
                OffsetDateTime::now_utc(),
                NEIGHBOR_EXPANSION_LIMIT,
            )
            .await
            .unwrap_or_default()
    };
    candidates.extend(
        memory
            .edges_by_ids(chat_id, &fts_ids)
            .await
            .unwrap_or_default(),
    );

    // Same-fact collapse (recall.rs keys on the natural-key triple,
    // ignoring valid_at, keeping the latest), then the presented cap.
    let mut by_fact: HashMap<(String, String, String), &CandidateEdge> = HashMap::new();
    for c in &candidates {
        let key = (
            c.source_id.clone(),
            c.relationship_name.clone(),
            c.target_id.clone(),
        );
        match by_fact.get(&key) {
            Some(existing) if existing.valid_at >= c.valid_at => {}
            _ => {
                by_fact.insert(key, c);
            }
        }
    }
    let mut collapsed: Vec<&CandidateEdge> = by_fact.into_values().collect();
    collapsed.sort_by_key(|edge| std::cmp::Reverse(edge.valid_at));
    collapsed.truncate(MAX_PRESENTED_CANDIDATES);
    if collapsed.is_empty() {
        return Ok(None);
    }

    let mut recall_candidates: Vec<RecallCandidate> = Vec::with_capacity(collapsed.len());
    for c in &collapsed {
        recall_candidates.push(RecallCandidate {
            edge_id: pipe_id_of(c)?,
            edge_text: c.edge_text.clone(),
            valid_at: c.valid_at,
            source_id: c.source_id.clone(),
            relationship_name: c.relationship_name.clone(),
            target_id: c.target_id.clone(),
        });
    }

    let gate_messages: Vec<GateMessage> = window_msgs
        .iter()
        .map(|m| GateMessage {
            row_id: m.id,
            platform_msg_id: m.platform_msg_id.clone(),
            content: render_msg_content(m),
            sender_id: m.sender_id.clone(),
            reply_to_platform_msg_id: m.reply_to_platform_msg_id.clone(),
            text: m.text.clone(),
        })
        .collect();
    let input = RelevanceInput {
        new_messages: gate_messages,
        candidates: recall_candidates.clone(),
    };
    let prompt_text = render_recall_prompt(&input, None);

    Ok(Some(WindowOut {
        chat_id: chat_id.to_string(),
        window_id: window_id.to_string(),
        origin: origin.to_string(),
        prompt_text,
        candidates: recall_candidates
            .iter()
            .map(|c| CandidateOut {
                edge_id: c.edge_id.clone(),
                edge_text: c.edge_text.clone(),
                valid_at: c
                    .valid_at
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| format!("{:?}", c.valid_at)),
            })
            .collect(),
        injected_edge_ids: injected_ids,
    }))
}

fn write_jsonl<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let mut out = String::new();
    for row in rows {
        out.push_str(&serde_json::to_string(row)?);
        out.push('\n');
    }
    std::fs::write(path, out)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut data_root = PathBuf::from("data-copy");
    let mut out_dir = PathBuf::from("fixtures");
    let mut skip_embed = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-root" => {
                data_root = PathBuf::from(args.next().context("--data-root needs a value")?)
            }
            "--out" => out_dir = PathBuf::from(args.next().context("--out needs a value")?),
            "--skip-embed" => skip_embed = true,
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }
    let api_key = if skip_embed {
        None
    } else {
        std::env::var("OPENROUTER_API_KEY").ok()
    };
    if api_key.is_none() {
        eprintln!("note: vector entry degraded to FTS-only (no OPENROUTER_API_KEY / --skip-embed)");
    }
    std::fs::create_dir_all(&out_dir)?;

    let mut groups: Vec<String> = std::fs::read_dir(&data_root)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("-100"))
        .collect();
    groups.sort();

    let mut all_uc1 = Vec::new();
    let mut all_uc2 = Vec::new();
    let mut all_edges = Vec::new();
    let mut all_windows = Vec::new();
    let mut meta = Vec::new();
    for chat_id in &groups {
        let out = extract_group(&data_root, chat_id, api_key.as_deref()).await?;
        println!(
            "{chat_id}: nodes={} embedded={} (journal {}) uc1={} uc2={} edges={} windows={} join ok={} miss={}",
            out.node_count,
            out.embedded_count,
            out.embedded_journal_count,
            out.uc1_pairs.len(),
            out.uc2_pairs.len(),
            out.edges.len(),
            out.windows.len(),
            out.edge_id_join_ok,
            out.edge_id_join_miss,
        );
        meta.push(serde_json::json!({
            "chat_id": chat_id,
            "nodes": out.node_count,
            "embedded": out.embedded_count,
            "embedded_journal": out.embedded_journal_count,
            "uc1_pairs": out.uc1_pairs.len(),
            "uc2_pairs": out.uc2_pairs.len(),
            "edges": out.edges.len(),
            "windows": out.windows.len(),
            "edge_text_ids": out.edge_text_ids,
            "edge_id_join_ok": out.edge_id_join_ok,
            "edge_id_join_miss": out.edge_id_join_miss,
        }));
        all_uc1.extend(out.uc1_pairs);
        all_uc2.extend(out.uc2_pairs);
        all_edges.extend(out.edges);
        all_windows.extend(out.windows);
    }

    write_jsonl(&out_dir.join("uc1_pairs.jsonl"), &all_uc1)?;
    write_jsonl(&out_dir.join("uc2_pairs.jsonl"), &all_uc2)?;
    write_jsonl(&out_dir.join("edges.jsonl"), &all_edges)?;
    write_jsonl(&out_dir.join("uc3_windows.jsonl"), &all_windows)?;
    std::fs::write(
        out_dir.join("meta.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "generated_at": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default(),
            "groups": meta,
            "pair_sim_floor": PAIR_SIM_FLOOR,
            "confirm_band": CONFIRM_BAND,
            "merge_band": MERGE_BAND,
        }))?,
    )?;
    println!(
        "fixtures: {} uc1 pairs, {} uc2 pairs, {} edges, {} windows -> {}",
        all_uc1.len(),
        all_uc2.len(),
        all_edges.len(),
        all_windows.len(),
        out_dir.display()
    );
    Ok(())
}
