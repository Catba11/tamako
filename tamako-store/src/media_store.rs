//! MediaStore: the global (cross-group) media cache at
//! `{data_root}/media.db`. Refer to current-state.md decision 82(f).
//!
//! Decision 82 captions media AT INTAKE: a photo/sticker message is
//! downloaded, normalized, and captioned by a vision model; the caption
//! is embedded into the message text as a
//! `<media type="sticker">caption</media>` element. Telegram stickers are
//! PUBLIC platform objects, so their captions carry no per-group privacy
//! concern: one global `sticker_captions` table keyed by the platform's
//! `file_unique_id` caches them across every group. A cache hit costs
//! zero model calls.
//!
//! This is the FIRST global store. It lives OUTSIDE every group
//! directory (the per-group store is `{data_root}/{chat_id}/store.db`,
//! Rule P5), so group teardown/rebuild never touches it. The database
//! has NO vec0 tables, so this module never calls
//! `register_sqlite_vec` — registration of the sqlite-vec extension is
//! per-connection and explicit (see store.rs), so no factoring of the
//! existing registration was needed.
//!
//! Schema versioning: media.db does NOT ride the group MIGRATIONS chain
//! of schema.rs. It carries its own tiny `user_version` pragma, starting
//! at 1 (MEDIA_SCHEMA_VERSION).
//!
//! All APIs are synchronous, like the whole crate. Callers run them
//! inside `tokio::task::spawn_blocking`. Refer to AGENT.md Section 6.2.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension};

use crate::error::{Result, StoreError};
use crate::schema;

/// Schema version of media.db, stored in the `user_version` pragma.
/// Bump on the next schema change and extend `run_media_migrations`.
const MEDIA_SCHEMA_VERSION: u32 = 1;

/// The global media cache: one SQLite database at
/// `{data_root}/media.db`, shared by every group. Refer to
/// current-state.md decision 82(f).
///
/// Synchronous API. `rusqlite::Connection` is not `Sync`, so the single
/// connection sits behind a Mutex; callers wrap calls in
/// `tokio::task::spawn_blocking` (AGENT.md Section 6.2).
pub struct MediaStore {
    conn: Mutex<Connection>,
}

impl MediaStore {
    /// Opens/creates `{data_root}/media.db` with the SAME pragmas as the
    /// per-group store (specs.md Section 5.1): busy_timeout(2s),
    /// journal_mode=WAL, synchronous=NORMAL. Creates `data_root` if
    /// needed, matching the `create_dir_all` of `Store::open_group`.
    ///
    /// Unlike `Store::open_group` this does NOT call
    /// `register_sqlite_vec`: media.db has no vec0 tables. Sets up the
    /// schema at `user_version` MEDIA_SCHEMA_VERSION.
    pub fn open(data_root: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_root)?;
        let conn = Connection::open(data_root.join("media.db"))?;
        // Decision 77 (M5): wait out brief SQLITE_BUSY windows instead of
        // failing the open immediately — same choice as the per-group
        // open.
        conn.busy_timeout(Duration::from_secs(2))?;
        // Refer to specs.md Section 5.1: WAL mode, synchronous=NORMAL.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        run_media_migrations(&conn)?;
        Ok(MediaStore {
            conn: Mutex::new(conn),
        })
    }

    /// The cached caption of one sticker, or None on a cache miss.
    pub fn get_sticker_caption(&self, file_unique_id: &str) -> Result<Option<String>> {
        let conn = self.lock();
        let caption = conn
            .query_row(
                "SELECT caption FROM sticker_captions WHERE file_unique_id = ?1",
                rusqlite::params![file_unique_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(caption)
    }

    /// Caches the caption of one sticker. INSERT OR IGNORE: first write
    /// wins. A concurrent or repeated caption of the same sticker is
    /// harmless — the existing row is kept (decision 82(f)).
    ///
    /// `created_at` is stamped from the Rust side as RFC 3339 TEXT, the
    /// house timestamp idiom (specs.md Section 5.2).
    pub fn put_sticker_caption(&self, file_unique_id: &str, caption: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT OR IGNORE INTO sticker_captions
                (file_unique_id, caption, created_at)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![file_unique_id, caption, schema::now_rfc3339()?],
        )?;
        Ok(())
    }

    /// Locks the connection. A poisoned mutex is recovered; the
    /// connection inside stays valid — the same recovery as
    /// `Store::lock`.
    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Applies the media.db schema. Unlike the per-group store.db there is
/// no `schema_migrations` table and no MIGRATIONS chain here: the
/// `user_version` pragma carries the version. The seam for a later v2 is
/// the version check below — append a step, bump MEDIA_SCHEMA_VERSION.
fn run_media_migrations(conn: &Connection) -> Result<()> {
    let version: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > MEDIA_SCHEMA_VERSION {
        return Err(StoreError::SchemaFromTheFuture {
            found: version,
            known: MEDIA_SCHEMA_VERSION,
        });
    }
    if version < 1 {
        // v1: the global sticker-caption cache (decision 82(f)).
        // Stickers are public platform objects keyed by file_unique_id.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sticker_captions (
                file_unique_id TEXT PRIMARY KEY,
                caption        TEXT NOT NULL,
                created_at     TEXT NOT NULL
            );",
        )?;
        conn.pragma_update(None, "user_version", 1)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_media_store() -> (tempfile::TempDir, MediaStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = MediaStore::open(dir.path()).expect("open media store");
        (dir, store)
    }

    #[test]
    fn put_then_get_round_trips() {
        let (_dir, store) = temp_media_store();
        store
            .put_sticker_caption("uid-1", "a smiling cat sticker")
            .expect("put");
        let caption = store.get_sticker_caption("uid-1").expect("get");
        assert_eq!(caption.as_deref(), Some("a smiling cat sticker"));
    }

    #[test]
    fn get_of_unknown_id_returns_none() {
        let (_dir, store) = temp_media_store();
        let caption = store.get_sticker_caption("uid-missing").expect("get");
        assert_eq!(caption, None);
    }

    #[test]
    fn double_put_keeps_the_first_caption() {
        let (_dir, store) = temp_media_store();
        store.put_sticker_caption("uid-1", "first").expect("put 1");
        store.put_sticker_caption("uid-1", "second").expect("put 2");
        let caption = store.get_sticker_caption("uid-1").expect("get");
        assert_eq!(caption.as_deref(), Some("first"));
    }

    #[test]
    fn reopen_persists_captions() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let store = MediaStore::open(dir.path()).expect("open 1");
            store
                .put_sticker_caption("uid-1", "a waving dog sticker")
                .expect("put");
        }
        let store = MediaStore::open(dir.path()).expect("open 2");
        let caption = store.get_sticker_caption("uid-1").expect("get");
        assert_eq!(caption.as_deref(), Some("a waving dog sticker"));
    }

    #[test]
    fn opened_db_has_user_version_1() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = MediaStore::open(dir.path()).expect("open");
        drop(store);
        let conn = Connection::open(dir.path().join("media.db")).expect("reopen raw");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read user_version");
        assert_eq!(version, MEDIA_SCHEMA_VERSION);
    }
}
