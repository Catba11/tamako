//! One-off alias-edge edge_text backfill (jev-memory data-quality goal).
//!
//! Root cause (verified against the fixtures): resolve.rs:527-530 fills
//! BOTH slots of "{} is a surface form of {}." with `extracted.name`, so
//! 100% of known_as/also_known_as edge texts are tautologies. On top of
//! that MERGE_NODE's ON MATCH SET n.name=$name renames an entity to its
//! LATEST surface form on every re-bind, so the text of an OLDER alias
//! edge ("Y is a surface form of Y") goes stale once the entity is
//! renamed to X: the statement is false against the current graph.
//!
//! This tool re-renders every alias edge's text from CURRENT graph
//! state: "{alias surface form} is a surface form of {entity name}.".
//! Tautological-but-true texts (entity currently named by this surface
//! form) are left alone; stale ones become informative. Edge topology
//! and row counts are never modified; the run is idempotent.
//!
//! Dry-run by default; --apply writes. TEST COPY ONLY — never point
//! --data-root at /var/lib/tamako.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::json;
use std::hash::{Hash, Hasher};
use tamako_memory::{EdgeId, LbugBackend};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

const LIST_ALIAS: &str = "MATCH (s:Node)-[r:EDGE]->(a:Node) \
    WHERE r.relationship_name IN ['known_as', 'also_known_as'] AND r.invalid_at IS NULL \
    RETURN s.id, s.name, a.id, a.name, r.relationship_name, r.valid_at, r.edge_text, \
    CAST(r.valid_at AS STRING)";

const COUNT_INVALID: &str = "MATCH (s:Node)-[r:EDGE]->(a:Node) \
    WHERE r.relationship_name IN ['known_as', 'also_known_as'] AND r.invalid_at IS NOT NULL \
    RETURN count(r)";

/// Data-quality D2: the batch-independent sentinel valid_at of a
/// structural alias binding (OffsetDateTime::UNIX_EPOCH), as the lbug
/// display string renders it.
const SENTINEL_RFC: &str = "1970-01-01T00:00:00Z";

const ALIGN_VALID_AT: &str = "MATCH (s:Node)-[r:EDGE]->(a:Node) \
    WHERE r.relationship_name IN ['known_as', 'also_known_as'] AND r.invalid_at IS NULL \
    SET r.valid_at = CAST('1970-01-01 00:00:00' AS TIMESTAMP)";

/// openCypher string-literal escaping: backslash first, then quote.
fn esc(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\'', "\\'")
}

#[derive(Default)]
struct GroupStats {
    total_rows: usize,
    invalid_rows: usize,
    distinct_keys: usize,
    duplicate_rows: usize,
    tautological_rows: usize,
    stale_rows: usize,
    pending_keys: usize,
    dedup_deleted: usize,
    max_rows_per_key: usize,
    valid_at_misaligned: usize,
    sidecar_alias_rows: usize,
    sidecar_rekeyed: usize,
    sidecar_text_updated: usize,
    sidecar_dangling_removed: usize,
    topo_sha256: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut data_root: Option<PathBuf> = None;
    let mut apply = false;
    let mut dedupe = false;
    let mut align_valid_at = false;
    let mut sidecar = false;
    let mut allow_production = false;
    let mut report_path: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-root" => {
                data_root = Some(PathBuf::from(
                    args.next().context("--data-root needs a value")?,
                ))
            }
            "--apply" => apply = true,
            "--dedupe" => dedupe = true,
            "--align-valid-at" => align_valid_at = true,
            "--sidecar" => sidecar = true,
            "--allow-production" => allow_production = true,
            "--report" => {
                report_path = Some(PathBuf::from(
                    args.next().context("--report needs a value")?,
                ))
            }
            other => anyhow::bail!("unknown flag: {other}"),
        }
    }
    let data_root = data_root.context("--data-root is required")?;
    anyhow::ensure!(
        allow_production || !data_root.starts_with("/var/lib/tamako"),
        "refusing to run against the production data root without --allow-production"
    );

    // A group directory is one holding a store.db (the canonical group
    // marker, main.rs). The production data root also holds non-group
    // dirs (Frameworks/, bugscope/) that must NOT be probed:
    // LbugBackend's database() CREATES a memory.lbug where none exists,
    // so probing a non-group dir writes a junk database into the data
    // root.
    let mut chat_ids: Vec<String> = std::fs::read_dir(&data_root)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().join("store.db").is_file())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();
    chat_ids.sort();

    let backend = LbugBackend::new(&data_root);
    let mut groups = BTreeMap::new();
    let mut totals = GroupStats::default();

    for chat_id in &chat_ids {
        let rows = backend.query_rows(chat_id, LIST_ALIAS).await?;
        let invalid = backend.query_rows(chat_id, COUNT_INVALID).await?;
        let mut stats = GroupStats {
            invalid_rows: invalid
                .first()
                .and_then(|row| row.first())
                .and_then(|cell| cell.parse().ok())
                .unwrap_or(0),
            ..GroupStats::default()
        };
        let mut keys: BTreeMap<(String, String, String), (String, String, usize)> = BTreeMap::new();
        let mut topo = BTreeSet::new();

        for row in &rows {
            anyhow::ensure!(row.len() == 8, "unexpected row shape: {row:?}");
            let (s_id, s_name, a_id, a_name, rel, valid_at) = (
                row[0].as_str(),
                row[1].as_str(),
                row[2].as_str(),
                row[3].as_str(),
                row[4].as_str(),
                row[5].as_str(),
            );
            stats.total_rows += 1;
            topo.insert(format!("{s_id}|{a_id}|{rel}|{valid_at}"));
            let entry = keys
                .entry((s_id.to_string(), a_id.to_string(), rel.to_string()))
                .or_insert_with(|| (s_name.to_string(), a_name.to_string(), 0));
            entry.2 += 1;
            if s_name == a_name {
                stats.tautological_rows += 1;
            }
        }

        stats.distinct_keys = keys.len();
        stats.duplicate_rows = stats.total_rows - stats.distinct_keys;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for line in &topo {
            line.hash(&mut hasher);
        }
        stats.topo_sha256 = format!("{:016x}", hasher.finish());

        for ((s_id, a_id, rel), (s_name, a_name, _count)) in &keys {
            let new_text = format!("{a_name} is a surface form of {s_name}.");
            let key_pending = rows.iter().any(|row| {
                row[0] == *s_id && row[2] == *a_id && row[4] == *rel && row[6] != new_text
            });
            if !key_pending {
                continue;
            }
            stats.pending_keys += 1;
            stats.stale_rows += rows
                .iter()
                .filter(|row| {
                    row[0] == *s_id && row[2] == *a_id && row[4] == *rel && row[6] != new_text
                })
                .count();
            if apply {
                let update = format!(
                    "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->(a:Node {{id: '{}'}}) \
                     WHERE r.relationship_name = '{}' \
                     SET r.edge_text = '{}'",
                    esc(s_id),
                    esc(a_id),
                    esc(rel),
                    esc(&new_text)
                );
                backend.query_rows(chat_id, &update).await?;
            }
        }

        if dedupe {
            // Data-quality D2: keep the EARLIEST row of each binding
            // key, delete the later batch-duplicates. Rows sharing the
            // keeper's display timestamp are indistinguishable and are
            // left in place (reported via max_rows_per_key).
            let mut per_key: BTreeMap<(String, String, String), Vec<(String, String)>> =
                BTreeMap::new();
            for row in &rows {
                per_key
                    .entry((row[0].clone(), row[2].clone(), row[4].clone()))
                    .or_default()
                    .push((row[5].clone(), row[7].clone()));
            }
            for ((s_id, a_id, rel), vals) in &per_key {
                stats.max_rows_per_key = stats.max_rows_per_key.max(vals.len());
                if vals.len() < 2 {
                    continue;
                }
                let keeper = vals.iter().min().expect("nonempty");
                let doomed: Vec<&String> = vals
                    .iter()
                    .map(|(_rfc, native)| native)
                    .filter(|native| *native != &keeper.1)
                    .collect();
                if doomed.is_empty() {
                    continue;
                }
                if apply {
                    let list = doomed
                        .iter()
                        .map(|cell| format!("'{}'", esc(cell)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let delete = format!(
                        "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->(a:Node {{id: '{}'}}) \
                         WHERE r.relationship_name = '{}' AND r.invalid_at IS NULL \
                         AND CAST(r.valid_at AS STRING) IN [{}] DELETE r",
                        esc(s_id),
                        esc(a_id),
                        esc(rel),
                        list
                    );
                    backend.query_rows(chat_id, &delete).await?;
                }
                stats.dedup_deleted += doomed.len();
            }
        }

        if align_valid_at {
            let misaligned = rows.iter().filter(|row| row[5] != SENTINEL_RFC).count();
            stats.valid_at_misaligned = misaligned;
            if apply && misaligned > 0 {
                backend.query_rows(chat_id, ALIGN_VALID_AT).await?;
            }
        }

        if sidecar {
            let sc = sync_sidecar(&data_root, chat_id, &rows, align_valid_at, apply)?;
            stats.sidecar_alias_rows = sc.alias_rows;
            stats.sidecar_rekeyed = sc.rekeyed;
            stats.sidecar_text_updated = sc.text_updated;
            stats.sidecar_dangling_removed = sc.dangling_removed;
        }

        totals.total_rows += stats.total_rows;
        totals.invalid_rows += stats.invalid_rows;
        totals.distinct_keys += stats.distinct_keys;
        totals.duplicate_rows += stats.duplicate_rows;
        totals.tautological_rows += stats.tautological_rows;
        totals.stale_rows += stats.stale_rows;
        totals.pending_keys += stats.pending_keys;
        totals.dedup_deleted += stats.dedup_deleted;
        totals.max_rows_per_key = totals.max_rows_per_key.max(stats.max_rows_per_key);
        totals.valid_at_misaligned += stats.valid_at_misaligned;
        totals.sidecar_alias_rows += stats.sidecar_alias_rows;
        totals.sidecar_rekeyed += stats.sidecar_rekeyed;
        totals.sidecar_text_updated += stats.sidecar_text_updated;
        totals.sidecar_dangling_removed += stats.sidecar_dangling_removed;
        groups.insert(
            chat_id.clone(),
            json!({
                "total_rows": stats.total_rows,
                "invalid_rows": stats.invalid_rows,
                "distinct_keys": stats.distinct_keys,
                "duplicate_rows": stats.duplicate_rows,
                "tautological_rows": stats.tautological_rows,
                "stale_rows": stats.stale_rows,
                "pending_keys": stats.pending_keys,
                "dedup_deleted": stats.dedup_deleted,
                "max_rows_per_key": stats.max_rows_per_key,
                "valid_at_misaligned": stats.valid_at_misaligned,
                "sidecar_alias_rows": stats.sidecar_alias_rows,
                "sidecar_rekeyed": stats.sidecar_rekeyed,
                "sidecar_text_updated": stats.sidecar_text_updated,
                "sidecar_dangling_removed": stats.sidecar_dangling_removed,
                "topo_sha256": stats.topo_sha256,
            }),
        );
    }

    let report = json!({
        "applied": apply,
        "dedupe": dedupe,
        "groups": groups,
        "totals": {
            "total_rows": totals.total_rows,
            "invalid_rows": totals.invalid_rows,
            "distinct_keys": totals.distinct_keys,
            "duplicate_rows": totals.duplicate_rows,
            "tautological_rows": totals.tautological_rows,
            "stale_rows": totals.stale_rows,
            "pending_keys": totals.pending_keys,
            "dedup_deleted": totals.dedup_deleted,
            "max_rows_per_key": totals.max_rows_per_key,
            "valid_at_misaligned": totals.valid_at_misaligned,
            "sidecar_alias_rows": totals.sidecar_alias_rows,
            "sidecar_rekeyed": totals.sidecar_rekeyed,
            "sidecar_text_updated": totals.sidecar_text_updated,
            "sidecar_dangling_removed": totals.sidecar_dangling_removed,
        },
    });
    let text = serde_json::to_string_pretty(&report)?;
    match report_path {
        Some(path) => std::fs::write(&path, &text)?,
        None => println!("{text}"),
    }
    eprintln!(
        "alias_backfill: applied={apply} dedupe={dedupe} groups={} stale_rows={} pending_keys={} dedup_deleted={}",
        groups.len(),
        totals.stale_rows,
        totals.pending_keys,
        totals.dedup_deleted
    );
    Ok(())
}

#[derive(Default)]
struct SidecarStats {
    alias_rows: usize,
    rekeyed: usize,
    text_updated: usize,
    dangling_removed: usize,
}

/// Syncs the store.db `edge_texts` sidecar with the graph's alias
/// edges (decision 76 mirror). Rows keyed by a natural key that no
/// longer exists (dedupe removals) are deleted; rows whose valid_at
/// changed (sentinel alignment) are re-keyed; drifted texts are
/// rewritten. Non-alias rows are never touched.
fn sync_sidecar(
    data_root: &Path,
    chat_id: &str,
    graph_rows: &[Vec<String>],
    aligned: bool,
    apply: bool,
) -> Result<SidecarStats> {
    let mut stats = SidecarStats::default();
    let path = data_root.join(chat_id).join("store.db");
    if !path.exists() {
        return Ok(stats);
    }
    let mut live: HashMap<(String, String, String), (String, String)> = HashMap::new();
    for row in graph_rows {
        let valid_rfc = if aligned {
            SENTINEL_RFC
        } else {
            row[5].as_str()
        };
        let valid_at = OffsetDateTime::parse(valid_rfc, &Rfc3339)
            .with_context(|| format!("unparseable valid_at display: {valid_rfc:?}"))?;
        let expected_id = EdgeId {
            source_id: row[0].clone(),
            relationship_name: row[4].clone(),
            target_id: row[2].clone(),
            valid_at,
        }
        .encode();
        let expected_text = format!("{} is a surface form of {}.", row[3], row[1]);
        live.insert(
            (row[0].clone(), row[4].clone(), row[2].clone()),
            (expected_id, expected_text),
        );
    }

    let conn = rusqlite::Connection::open(&path)?;
    let sidecar_rows: Vec<(String, String)> = conn
        .prepare("SELECT edge_id, edge_text FROM edge_texts")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    for (edge_id, text) in sidecar_rows {
        let natural = match EdgeId::decode(&edge_id) {
            Ok(natural) => natural,
            Err(_) => continue,
        };
        if natural.relationship_name != "known_as" && natural.relationship_name != "also_known_as" {
            continue;
        }
        stats.alias_rows += 1;
        let key = (
            natural.source_id.clone(),
            natural.relationship_name.clone(),
            natural.target_id.clone(),
        );
        match live.get(&key) {
            None => {
                stats.dangling_removed += 1;
                if apply {
                    conn.execute("DELETE FROM edge_texts WHERE edge_id = ?1", [&edge_id])?;
                }
            }
            Some((expected_id, expected_text)) => {
                if *expected_id != edge_id {
                    stats.rekeyed += 1;
                    if apply {
                        conn.execute("DELETE FROM edge_texts WHERE edge_id = ?1", [&edge_id])?;
                        conn.execute(
                            "INSERT OR REPLACE INTO edge_texts (edge_id, edge_text) \
                             VALUES (?1, ?2)",
                            [expected_id, expected_text],
                        )?;
                    }
                } else if *expected_text != text {
                    stats.text_updated += 1;
                    if apply {
                        conn.execute(
                            "UPDATE edge_texts SET edge_text = ?2 WHERE edge_id = ?1",
                            [&edge_id, expected_text],
                        )?;
                    }
                }
            }
        }
    }
    Ok(stats)
}
