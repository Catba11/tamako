//! Spike S1: prove sqlite-vec (vec0) works with Tamako's rusqlite stack.
//!
//! This is a TECHNICAL SPIKE, not the production feature. It verifies, in a
//! scratch test binary:
//!
//! 1. vec0 registers on a rusqlite `Connection` that uses our exact
//!    `Store::open_group` pragmas (WAL + synchronous=NORMAL, bundled SQLite).
//! 2. A `float[3072]` vec0 virtual table supports insert + KNN (`MATCH` +
//!    `k = ?`) with the rowid as the node id, and ranks an identical vector
//!    first at distance ~0.
//! 3. The vec0 table coexists with normal schema tables in the SAME .db
//!    file under WAL, across close/reopen.
//! 4. Reopening the file REQUIRES re-registering the extension on the new
//!    connection (registration is per-connection, not persisted).
//! 5. Rough storage cost of N=100 random 3072-dim f32 vectors (bytes/node
//!    vs the naive 3072*4 = 12288 baseline).
//!
//! Registration idiom: `sqlite-vec` 0.1.9 exports only the raw C entry
//! point `sqlite3_vec_init`, declared (incorrectly, with no parameters)
//! as a Rust extern. The canonical helper `tamako_store::register_sqlite_vec`
//! (used by both Store open paths) contains the documented transmute;
//! this spike switches to it so there is exactly one copy.

#![cfg(test)]

use rusqlite::Connection;
use std::path::Path;
// The spike measures the pinned sidecar dimension, so DIM is the store
// crate's pin (decision 81: 3072, google/gemini-embedding-2's native
// dimension) instead of a local copy.
use tamako_store::{register_sqlite_vec, EMBEDDING_DIM as DIM};

/// Opens a file DB exactly like `Store::open_group` does: WAL +
/// synchronous=NORMAL, then registers sqlite-vec via the canonical
/// helper.
fn open_spike_db(path: &Path) -> tamako_store::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    register_sqlite_vec(&conn)?;
    Ok(conn)
}

/// Encodes an f32 vector as the little-endian byte blob vec0 accepts for
/// `float[N]` columns (the production idiom; avoids slow JSON text).
fn vec_to_blob(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Deterministic xorshift PRNG — keeps the spike dependency-free (no rand).
struct XorShift(u64);

impl XorShift {
    fn next_f32(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        // Map to [-1, 1).
        ((self.0 >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0) as f32
    }

    fn vector(&mut self) -> Vec<f32> {
        (0..DIM).map(|_| self.next_f32()).collect()
    }
}

/// KNN query per the vec0 0.1.x idiom: `embedding MATCH ? AND k = ?`.
/// Returns (rowid, distance) pairs, nearest first.
fn knn(conn: &Connection, query: &[f32], k: u32) -> rusqlite::Result<Vec<(i64, f64)>> {
    let mut stmt = conn.prepare(
        "SELECT rowid, distance FROM vec_items
         WHERE embedding MATCH ?1 AND k = ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![vec_to_blob(query), k], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    rows.collect()
}

#[test]
fn vec0_knn_roundtrip_coexists_with_normal_tables_under_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("store.db");

    let mut rng = XorShift(0x1234_5678_9abc_def0);
    let vectors: Vec<Vec<f32>> = (0..5).map(|_| rng.vector()).collect();

    {
        let conn = open_spike_db(&db_path).expect("open spike db");
        assert_eq!(
            conn.pragma_query_value(None, "journal_mode", |r| r.get::<_, String>(0))
                .expect("journal_mode"),
            "wal"
        );

        // The vec0 table and a normal schema-style table in the SAME file.
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[{DIM}]);
             CREATE TABLE normal_table (id INTEGER PRIMARY KEY, note TEXT NOT NULL);
             INSERT INTO normal_table (note) VALUES ('hello');",
        ))
        .expect("create tables");

        // vec0 idiom: the virtual table's rowid IS the node id.
        let mut insert = conn
            .prepare("INSERT INTO vec_items(rowid, embedding) VALUES (?1, ?2)")
            .expect("prepare insert");
        for (i, v) in vectors.iter().enumerate() {
            insert
                .execute(rusqlite::params![(i + 1) as i64, vec_to_blob(v)])
                .expect("insert vector");
        }
        drop(insert);

        // KNN with one of the inserted vectors: itself first, distance ~0.
        let hits = knn(&conn, &vectors[2], 3).expect("knn");
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].0, 3, "identical vector must rank first");
        assert!(hits[0].1.abs() < 1e-6, "distance ~0, got {}", hits[0].1);
        assert_ne!(hits[1].0, 3);

        // A distinct query vector does NOT rank node 3 first. It ranks
        // ITSELF first (it is in the table as rowid 5) at distance ~0.
        let hits = knn(&conn, &vectors[4], 3).expect("knn distinct");
        assert_ne!(hits[0].0, 3, "distinct vector must not rank node 3 first");
        assert_eq!(hits[0].0, 5, "distinct vector ranks itself first");
        assert!(hits[0].1.abs() < 1e-6);
        assert!(hits[1].1 > 0.0, "the next-nearest node is at distance > 0");
    }

    // Reopen: same file, fresh connection. Registration is per-connection,
    // so the new connection must be registered again before vec0 resolves.
    {
        let conn = open_spike_db(&db_path).expect("reopen spike db");
        let note: String = conn
            .query_row("SELECT note FROM normal_table WHERE id = 1", [], |r| {
                r.get(0)
            })
            .expect("normal table survives reopen in the same file");
        assert_eq!(note, "hello");
        let hits = knn(&conn, &vectors[0], 1).expect("knn after reopen");
        assert_eq!(hits[0].0, 1, "vec0 data survives close/reopen");
        assert!(hits[0].1.abs() < 1e-6);
    }
}

#[test]
fn vec0_requires_registration_on_every_new_connection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("store.db");

    // First connection creates the vec0 table (registered).
    {
        let conn = open_spike_db(&db_path).expect("open");
        conn.execute_batch("CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[8]);")
            .expect("create with registered connection");
    }

    // Second connection WITHOUT registration: the vec0 module is unknown.
    // Finding for migration v7: opening an existing store.db is not enough;
    // Store must register sqlite-vec on every connection it opens.
    let conn = Connection::open(&db_path).expect("open unregistered");
    let err = conn
        .execute_batch("CREATE VIRTUAL TABLE other_vec USING vec0(embedding float[8]);")
        .expect_err("vec0 must be unavailable without registration");
    assert!(
        err.to_string().contains("no such module: vec0"),
        "unexpected error: {err}"
    );
    // Even reading an EXISTING vec0 table fails on an unregistered
    // connection, because the module implementation is not loaded.
    let err = match conn.prepare("SELECT rowid FROM vec_items") {
        Ok(_) => panic!("prepare on unregistered connection must fail"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("no such module: vec0"),
        "unexpected error: {err}"
    );
}

#[test]
fn vec0_storage_cost_3072_dim() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("store.db");

    let conn = open_spike_db(&db_path).expect("open");
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[{DIM}]);"
    ))
    .expect("create vec table");
    // Fold the WAL back so the .db file size reflects the table content.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");
    let size_before = std::fs::metadata(&db_path).expect("stat").len();

    let checkpoint_size = |conn: &Connection| {
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint");
        std::fs::metadata(&db_path).expect("stat").len()
    };

    const N: usize = 100;
    let mut rng = XorShift(0xdead_beef_cafe_f00d);
    let insert_n = |conn: &Connection, rng: &mut XorShift, from: usize, to: usize| {
        let mut insert = conn
            .prepare("INSERT INTO vec_items(rowid, embedding) VALUES (?1, ?2)")
            .expect("prepare");
        for i in from..to {
            insert
                .execute(rusqlite::params![
                    (i + 1) as i64,
                    vec_to_blob(&rng.vector())
                ])
                .expect("insert");
        }
    };

    insert_n(&conn, &mut rng, 0, N);
    let size_at_100 = checkpoint_size(&conn);

    // vec0 allocates storage in chunk granules: default chunk_size = 1024
    // vectors, each chunk preallocated as a zeroblob of
    // chunk_size * DIM * 4 bytes (12 MiB for float[3072]). The first 100
    // vectors therefore cost one whole granule. Filling the rest of the
    // chunk (vectors 101..1024) must cost ~nothing.
    let delta_first_chunk = size_at_100 - size_before;
    insert_n(&conn, &mut rng, N, 1024);
    let size_at_1024 = checkpoint_size(&conn);
    let delta_fill = size_at_1024 - size_at_100;

    let naive = (DIM * 4) as u64;
    println!(
        "vec0 storage cost: first 100 vectors of dim {DIM}: +{delta_first_chunk} bytes \
         ({:.0} bytes/node, naive {naive}) — one preallocated 1024-vector chunk granule; \
         filling vectors 101..1024: +{delta_fill} bytes ({:.1} bytes/node marginal)",
        delta_first_chunk as f64 / N as f64,
        delta_fill as f64 / (1024 - N) as f64
    );

    // The granule is exactly one 1024-slot chunk (1024 * 12288 = 12 MiB)
    // plus a handful of shadow-table/btree pages. The `64 * 4096` slack
    // below is 64 SQLite 4 KiB pages, not dimension math.
    let granule = 1024 * naive;
    assert!(
        delta_first_chunk >= granule && delta_first_chunk < granule + 64 * 4096,
        "first-chunk delta {delta_first_chunk} outside [{granule}, {})",
        granule + 64 * 4096
    );
    // Marginal cost within the preallocated chunk is a few pages at most.
    assert!(
        delta_fill < 64 * 4096,
        "filling the preallocated chunk cost {delta_fill} bytes"
    );
}
