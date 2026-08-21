//! Decision 77 (H5) runtime experiment, kept as a committable ignored
//! test: what does lbug do when a SECOND process opens a group's
//! memory.lbug while the first process holds it open? The outcome is
//! documented on the lock comment in tamako/src/main.rs
//! (`acquire_group_lock`).
//!
//! Run explicitly:
//!
//! ```sh
//! cargo test -p tamako --test lbug_cross_process -- --ignored --nocapture
//! ```
//!
//! Shape: the parent test opens the group's memory.lbug through
//! `LbugBackend` and holds it, then re-executes THIS test binary
//! (std::process::Command) filtered to the child test with an env var
//! pointing at the same data root. The child opens the same group and
//! reports the outcome on stdout. A timeout kills a blocked child so a
//! hypothetical blocking lock cannot hang the suite.

use std::process::Command;

use tamako_memory::{LbugBackend, MemoryBackend, MemoryBatch, MemoryNode, NodeType};

const CHILD_ENV: &str = "TAMAKO_LBUG_EXPERIMENT_CHILD";
const CHAT: &str = "chat_x";
const CHILD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// One trivial batch: a single Concept node, no edges.
fn tiny_batch(tag: &str) -> MemoryBatch {
    let at = time::OffsetDateTime::from_unix_timestamp(1_785_500_000).expect("valid timestamp");
    let name = format!("Experiment {tag}");
    MemoryBatch {
        batch_id: tamako_memory::identifiers::batch_id(1, 10),
        nodes: vec![MemoryNode {
            id: tamako_memory::identifiers::concept_id(&name),
            name,
            node_type: NodeType::Concept,
            created_at: at,
            updated_at: at,
            properties: None,
        }],
        edges: vec![],
    }
}

#[tokio::test]
#[ignore = "runtime experiment (decision 77, H5); run explicitly with --ignored"]
async fn cross_process_lbug_experiment_parent() {
    // Re-entrancy guard: when the harness runs the whole file (e.g. a
    // bare `--ignored`), the child test runs as a top-level test too
    // and returns immediately without the env var.
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    // The first process opens the group database and holds it.
    let backend = LbugBackend::new(dir.path());
    backend
        .upsert_batch(CHAT, &tiny_batch("parent"))
        .await
        .expect("parent write");
    assert!(dir.path().join(CHAT).join("memory.lbug").exists());

    // The second process: this same test binary, filtered to the child.
    let exe = std::env::current_exe().expect("current exe");
    let mut child = Command::new(exe)
        .args([
            "cross_process_lbug_experiment_child",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_ENV, dir.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn the child process");

    // Wait with a timeout: a hypothetical blocking lock in lbug must
    // not hang the suite — kill the child and report the block.
    let started = std::time::Instant::now();
    let output = loop {
        match child.try_wait().expect("poll the child") {
            Some(_) => break child.wait_with_output().expect("collect the child output"),
            None if started.elapsed() > CHILD_TIMEOUT => {
                child.kill().expect("kill the blocked child");
                println!("CHILD-OUTCOME: the second process BLOCKED on the lbug open (killed after {CHILD_TIMEOUT:?})");
                break child.wait_with_output().expect("collect the killed child");
            }
            None => std::thread::sleep(std::time::Duration::from_millis(100)),
        }
    };
    println!("child exit status: {}", output.status);
    println!("child stdout:\n{}", String::from_utf8_lossy(&output.stdout));
    println!("child stderr:\n{}", String::from_utf8_lossy(&output.stderr));

    // Whatever the child saw, the first process's handle must still
    // work (no silent corruption of the parent side).
    backend
        .upsert_batch(CHAT, &tiny_batch("parent-again"))
        .await
        .expect("the parent still writes after the child ran");
}

#[tokio::test]
#[ignore = "runtime experiment child (decision 77, H5); driven by the parent test"]
async fn cross_process_lbug_experiment_child() {
    // Without the env var this is a no-op (a bare `--ignored` run of
    // the file runs both tests; only the parent-spawned process gets
    // the data root).
    let Ok(data_root) = std::env::var(CHILD_ENV) else {
        return;
    };
    let backend = LbugBackend::new(data_root);
    match backend.upsert_batch(CHAT, &tiny_batch("child")).await {
        Ok(()) => println!(
            "CHILD-OUTCOME: the second process opened memory.lbug and WROTE without an error"
        ),
        Err(error) => println!("CHILD-OUTCOME: the second process FAILED: {error}"),
    }
}
