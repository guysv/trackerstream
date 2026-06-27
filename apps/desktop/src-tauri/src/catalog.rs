//! Catalog-over-IPNS lazy query (R1). The catalog SQLite DB is published on IPFS
//! under the master's signed IPNS record; this module lets the thin client QUERY it
//! without downloading the whole file, by mounting it on a read-only SQLite VFS whose
//! page reads are served over Bitswap. A query (indexed lookup / covering-index browse)
//! touches O(log n) + result pages, so the client fetches a handful of 16 KB blocks,
//! not the ~3 MB DB. (phiresky's sql.js-httpvfs idea, over Bitswap instead of HTTP range.)
//!
//! The crux is the sync→async bridge: `sqlite-vfs`'s `read_exact_at` is synchronous,
//! but the only way to a block is async (`Ipfs::cat_unixfs(..).range(..)`). We run the
//! whole `rusqlite` open+query on a `spawn_blocking` thread and `block_on` a captured
//! runtime handle for each ranged read — never blocking an async worker.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use cid::Cid;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use rust_ipfs::Ipfs;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlite_vfs::{DatabaseHandle, LockKind, OpenOptions, Vfs, WalDisabled};
use tokio::runtime::Handle;

/// Cache + fetch granularity. Matches the publish chunker (`size-16384` raw-leaves), so
/// each cached chunk aligns to exactly one UnixFS leaf block — a ranged cat over a
/// 16 KB-aligned span fetches whole leaves, and cached chunks survive across queries.
const CHUNK: u64 = 16 * 1024;
/// Read-ahead window on sequential access (phiresky's "read heads"): a covering-index
/// browse scans pages in ascending order, so on a sequential read we prefetch this many
/// chunks ahead — fetched concurrently (≈ one FETCH_CONCURRENCY batch), so it costs ~one
/// round-trip's wall-clock, not N. Kept to a single batch to bound over-fetch on short
/// scans (a wider window pulls pages the query never reads).
const PREFETCH_CHUNKS: u64 = 15;
/// Max concurrent leaf fetches per prefetch batch (Bitswap wants to the master).
const FETCH_CONCURRENCY: usize = 16;
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const VFS_NAME: &str = "ipfs-catalog";
/// Absolute ceiling on a term's document frequency for bm25 ranking; on small catalogs
/// the relative (fraction-of-total) guard in `is_selective` dominates. See that fn.
const RANK_DOC_LIMIT: i64 = 4000;

// ---------------------------------------------------------------------------------
// The VFS: serves SQLite page reads from a CID over Bitswap.
// ---------------------------------------------------------------------------------

/// The VFS itself is stateless and registered once, app-wide. The node handle,
/// runtime handle, and target CID for a given query are passed in per-open via a
/// thread-local (set on the blocking thread that drives `Connection::open`), so the
/// VFS isn't permanently bound to whichever node first used it.
struct IpfsVfs;

#[derive(Clone)]
struct OpenCtx {
    ipfs: Ipfs,
    rt: Handle,
    cid: Cid,
}

thread_local! {
    static OPEN_CTX: std::cell::RefCell<Option<OpenCtx>> = const { std::cell::RefCell::new(None) };
}

/// Total bytes pulled over the VFS this process — lets tests assert a query fetched
/// ≪ the whole DB (the "lazy" claim). Relaxed; monotonic until a test resets it.
pub(crate) static FETCHED_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Default)]
struct Cache {
    chunks: HashMap<u64, Arc<Vec<u8>>>,
    /// Last chunk index read, for sequential-access detection (prefetch trigger).
    last_end: Option<u64>,
}

/// One open catalog DB, bound to a resolved root CID.
struct CatalogFile {
    ipfs: Ipfs,
    rt: Handle,
    cid: Cid,
    size: u64,
    lock: LockKind,
    cache: Mutex<Cache>,
}

impl CatalogFile {
    fn last_chunk(&self) -> u64 {
        if self.size == 0 { 0 } else { (self.size - 1) / CHUNK }
    }

    /// Fetch `[start, end)` of the file via a ranged UnixFS cat (walks only the leaves
    /// overlapping the range). Blocks the current (spawn_blocking) thread on the runtime.
    fn fetch_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        let ipfs = self.ipfs.clone();
        let cid = self.cid;
        let bytes = self
            .rt
            .block_on(async move {
                ipfs.cat_unixfs(cid)
                    .range(start..end)
                    .set_local(false)
                    .timeout(READ_TIMEOUT)
                    .await
            })
            .map_err(|e| io::Error::other(format!("cat {cid} [{start}..{end}): {e}")))?;
        FETCHED_BYTES.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(bytes.to_vec())
    }

    /// Ensure every chunk in `[first, last]` is cached. Missing chunks (plus a prefetch
    /// tail on sequential access) are fetched CONCURRENTLY — each as its own ranged cat,
    /// so the leaves download in parallel instead of one serial round-trip per page. The
    /// internal DAG nodes are shared via the local blockstore after the first call. (This
    /// only helps sequential scans — covering-index browse, schema load — since a B-tree
    /// point/FTS lookup chains one dependent page read at a time.)
    fn ensure_chunks(&self, first: u64, last: u64) -> io::Result<()> {
        let missing: Vec<u64> = {
            let cache = self.cache.lock().unwrap();
            if !(first..=last).any(|c| !cache.chunks.contains_key(&c)) {
                return Ok(()); // all present
            }
            let sequential = cache.last_end.is_some_and(|le| first <= le + 1);
            let hi = if sequential {
                (last + PREFETCH_CHUNKS).min(self.last_chunk())
            } else {
                last
            };
            (first..=hi).filter(|c| !cache.chunks.contains_key(c)).collect()
        };

        let ipfs = self.ipfs.clone();
        let cid = self.cid;
        let size = self.size;
        let fetched: Vec<io::Result<(u64, Vec<u8>)>> = self.rt.block_on(async move {
            use futures::StreamExt;
            futures::stream::iter(missing.into_iter().map(|c| {
                let ipfs = ipfs.clone();
                async move {
                    let start = c * CHUNK;
                    let end = ((c + 1) * CHUNK).min(size);
                    let bytes = ipfs
                        .cat_unixfs(cid)
                        .range(start..end)
                        .set_local(false)
                        .timeout(READ_TIMEOUT)
                        .await
                        .map_err(|e| io::Error::other(format!("cat chunk {c} of {cid}: {e}")))?;
                    FETCHED_BYTES.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    Ok((c, bytes.to_vec()))
                }
            }))
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect()
            .await
        });

        let mut cache = self.cache.lock().unwrap();
        for r in fetched {
            let (c, bytes) = r?;
            cache.chunks.entry(c).or_insert_with(|| Arc::new(bytes));
        }
        Ok(())
    }
}

impl DatabaseHandle for CatalogFile {
    type WalIndex = WalDisabled;

    fn size(&self) -> io::Result<u64> {
        Ok(self.size)
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let end = offset + buf.len() as u64;
        if end > self.size {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read past EOF"));
        }
        let first = offset / CHUNK;
        let last = (end - 1) / CHUNK;
        self.ensure_chunks(first, last)?;

        let cache = self.cache.lock().unwrap();
        for c in first..=last {
            let chunk = cache
                .chunks
                .get(&c)
                .ok_or_else(|| io::Error::other("chunk missing after fetch"))?;
            let chunk_start = c * CHUNK;
            let avail_end = chunk_start + chunk.len() as u64;
            let seg_start = offset.max(chunk_start);
            let seg_end = end.min(avail_end);
            if seg_end <= seg_start {
                continue;
            }
            let from = (seg_start - chunk_start) as usize;
            let to = (seg_end - chunk_start) as usize;
            let dst = (seg_start - offset) as usize;
            buf[dst..dst + (to - from)].copy_from_slice(&chunk[from..to]);
        }
        drop(cache);
        self.cache.lock().unwrap().last_end = Some(last);
        Ok(())
    }

    fn write_all_at(&mut self, _buf: &[u8], _offset: u64) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::PermissionDenied, "read-only catalog"))
    }

    fn sync(&mut self, _data_only: bool) -> io::Result<()> {
        Ok(())
    }

    fn set_len(&mut self, _size: u64) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::PermissionDenied, "read-only catalog"))
    }

    fn lock(&mut self, lock: LockKind) -> io::Result<bool> {
        self.lock = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> io::Result<bool> {
        Ok(false)
    }

    fn current_lock(&self) -> io::Result<LockKind> {
        Ok(self.lock)
    }

    fn wal_index(&self, _readonly: bool) -> io::Result<Self::WalIndex> {
        Ok(WalDisabled::default())
    }
}

impl Vfs for IpfsVfs {
    type Handle = CatalogFile;

    fn open(&self, _db: &str, _opts: OpenOptions) -> io::Result<Self::Handle> {
        // The node handle + target CID arrive via the per-query thread-local (the "path"
        // SQLite passes is just the CID string, but the ctx is authoritative).
        let ctx = OPEN_CTX
            .with(|c| c.borrow().clone())
            .ok_or_else(|| io::Error::other("catalog VFS opened with no query context"))?;
        // Probe the SQLite header for the exact file size (page_size * page_count) so
        // xFileSize doesn't have to fetch the whole DAG.
        let probe = CatalogFile {
            ipfs: ctx.ipfs.clone(),
            rt: ctx.rt.clone(),
            cid: ctx.cid,
            size: u64::MAX,
            lock: LockKind::None,
            cache: Mutex::new(Cache::default()),
        };
        let hdr = probe.fetch_range(0, 100)?;
        if hdr.len() < 100 || &hdr[0..16] != b"SQLite format 3\0" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not a sqlite db"));
        }
        let page_size = match u16::from_be_bytes([hdr[16], hdr[17]]) {
            1 => 65536u64,
            v => v as u64,
        };
        let page_count = u32::from_be_bytes([hdr[28], hdr[29], hdr[30], hdr[31]]) as u64;
        Ok(CatalogFile {
            ipfs: ctx.ipfs,
            rt: ctx.rt,
            cid: ctx.cid,
            size: page_size * page_count,
            lock: LockKind::None,
            cache: Mutex::new(Cache::default()),
        })
    }

    fn delete(&self, _db: &str) -> io::Result<()> {
        Ok(())
    }

    fn exists(&self, db: &str) -> io::Result<bool> {
        // The main DB ("<cid>") exists; sidecar probes ("<cid>-journal"/"-wal") don't
        // parse as a CID -> false, so SQLite sees no hot journal and reads directly.
        let name = db.rsplit('/').next().unwrap_or(db);
        Ok(name.parse::<Cid>().is_ok())
    }

    fn temporary_name(&self) -> String {
        "ipfs-catalog-temp".into()
    }

    fn random(&self, buf: &mut [i8]) {
        for b in buf.iter_mut() {
            *b = 0;
        }
    }

    fn sleep(&self, duration: Duration) -> Duration {
        duration
    }
}

static REGISTER: Once = Once::new();

fn ensure_registered() {
    REGISTER.call_once(|| {
        sqlite_vfs::register(VFS_NAME, IpfsVfs, false).expect("register ipfs-catalog vfs");
    });
}

// ---------------------------------------------------------------------------------
// Query API: the 4 handlers ported from apps/server/src/catalog.ts (same SQL).
// ---------------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CatalogReq {
    Search { q: String, #[serde(default)] limit: Option<i64> },
    List {
        #[serde(default)] format: Option<String>,
        #[serde(default)] sort: Option<String>,
        #[serde(default)] limit: Option<i64>,
        #[serde(default)] offset: Option<i64>,
    },
    Get { id: i64 },
    Formats {},
}

/// Resolve + open the catalog over the VFS and answer a query. Runs on a blocking
/// thread (rusqlite is sync; the VFS block_on's per page read). Returns JSON matching
/// the frontend response shapes so the Svelte call sites are unchanged.
pub async fn run_query(ipfs: Ipfs, cid: Cid, req: CatalogReq) -> Result<Value, String> {
    let rt = Handle::current();
    tokio::task::spawn_blocking(move || {
        ensure_registered();
        OPEN_CTX.with(|c| *c.borrow_mut() = Some(OpenCtx { ipfs, rt, cid }));
        let result = (|| {
            let conn = Connection::open_with_flags_and_vfs(
                cid.to_string(),
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                VFS_NAME,
            )
            .map_err(|e| format!("open catalog {cid}: {e}"))?;
            conn.pragma_update(None, "query_only", true).ok();
            dispatch(&conn, &req).map_err(|e| e.to_string())
        })();
        OPEN_CTX.with(|c| *c.borrow_mut() = None);
        result
    })
    .await
    .map_err(|e| format!("catalog query task: {e}"))?
}

fn dispatch(conn: &Connection, req: &CatalogReq) -> rusqlite::Result<Value> {
    match req {
        CatalogReq::Search { q, limit } => search(conn, q, limit.unwrap_or(50)),
        CatalogReq::List { format, sort, limit, offset } => {
            list(conn, format.as_deref(), sort.as_deref(), limit.unwrap_or(100), offset.unwrap_or(0))
        }
        CatalogReq::Get { id } => get(conn, *id),
        CatalogReq::Formats {} => formats(conn),
    }
}

const HIT_COLS: &str =
    "m.id, m.filename, m.format, m.title, m.duration, m.channels, m.root_cid";

/// Map a 7-column hit row (id, filename, format, title, duration, channels, root_cid).
fn hit_row(r: &rusqlite::Row) -> rusqlite::Result<Value> {
    Ok(json!({
        "id": r.get::<_, i64>(0)?,
        "filename": r.get::<_, String>(1)?,
        "format": r.get::<_, String>(2)?,
        "title": r.get::<_, Option<String>>(3)?.unwrap_or_default(),
        "duration": r.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
        "channels": r.get::<_, Option<i64>>(5)?.unwrap_or(0),
        "rootCid": r.get::<_, String>(6)?,
    }))
}

fn collect(stmt: &mut rusqlite::Statement, params: &[&dyn rusqlite::ToSql]) -> rusqlite::Result<Vec<Value>> {
    let rows = stmt.query_map(params, hit_row)?;
    rows.collect()
}

fn search(conn: &Connection, query: &str, limit: i64) -> rusqlite::Result<Value> {
    let q = query.trim();
    if q.is_empty() {
        return Ok(json!({ "results": [] }));
    }
    let explicit = q.contains('"') || q.contains('*') || q.contains(':') || q.contains('^');
    let matchstr = if explicit {
        q.to_string()
    } else {
        q.split_whitespace().map(|t| format!("\"{t}\"*")).collect::<Vec<_>>().join(" ")
    };
    // bm25 guard: rank only when the match set is small; a broad query takes the first
    // LIMIT matches in rowid order (avoids scoring the whole index — see is_selective).
    let order = if explicit || is_selective(conn, &matchstr) { "ORDER BY bm25(modules_fts)" } else { "" };
    let sql = format!(
        "SELECT {HIT_COLS} FROM modules_fts f JOIN modules m ON m.id = f.rowid \
         WHERE modules_fts MATCH ?1 {order} LIMIT ?2"
    );
    let mut stmt = conn.prepare(&sql)?;
    let results = collect(&mut stmt, &[&matchstr as &dyn rusqlite::ToSql, &limit])?;
    Ok(json!({ "results": results }))
}

/// Decide whether to apply global bm25 ranking. `ORDER BY bm25` forces SQLite to score
/// EVERY matching row before LIMIT — fine when the match set is small, ruinous for a
/// broad query (lab: a near-stopword = 19k pages; measured on prod: a broad search = 130
/// cold pages / ~28 s). We probe the REAL match count of the full MATCH expression
/// (prefix-expanded, multi-term ANDed) but stop at the threshold — so the probe reads at
/// most ~`limit` postings in rowid order, never the whole index. Over the threshold ⇒
/// drop ranking and take the first LIMIT matches in rowid order. Threshold is relative to
/// catalog size (with an absolute ceiling) so it holds as the corpus grows ~100×.
/// (Earlier tried `fts5vocab` for per-term doc counts — it has no range seek and scans
/// the entire vocab, i.e. the whole FTS index over Bitswap. This bounded COUNT avoids that.)
fn is_selective(conn: &Connection, matchstr: &str) -> bool {
    let total: i64 = conn
        .query_row("SELECT value FROM meta WHERE key='total'", [], |r| r.get::<_, String>(0))
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let limit = if total > 0 { RANK_DOC_LIMIT.min((total / 10).max(200)) } else { RANK_DOC_LIMIT };
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM (SELECT 1 FROM modules_fts WHERE modules_fts MATCH ?1 LIMIT ?2)",
            rusqlite::params![matchstr, limit + 1],
            |r| r.get(0),
        )
        .unwrap_or(0);
    n <= limit
}

fn list(
    conn: &Connection,
    format: Option<&str>,
    sort: Option<&str>,
    limit: i64,
    offset: i64,
) -> rusqlite::Result<Value> {
    let limit = limit.clamp(1, 500);
    let offset = offset.max(0);
    let order = match sort {
        Some("random") => "RANDOM()",
        Some("title") => "title COLLATE NOCASE",
        _ => "ingested_at DESC, id DESC",
    };
    let where_clause = if format.is_some() { "WHERE format = ?" } else { "" };
    let sql = format!(
        "SELECT {} FROM modules {where_clause} ORDER BY {order} LIMIT ? OFFSET ?",
        HIT_COLS.replace("m.", "")
    );
    let mut stmt = conn.prepare(&sql)?;
    let results = if let Some(fmt) = format {
        collect(&mut stmt, &[&fmt as &dyn rusqlite::ToSql, &limit, &offset])?
    } else {
        collect(&mut stmt, &[&limit as &dyn rusqlite::ToSql, &offset])?
    };
    Ok(json!({ "results": results }))
}

fn get(conn: &Connection, id: i64) -> rusqlite::Result<Value> {
    let row = conn
        .query_row(
            "SELECT id, filename, format, title, duration, channels, root_cid, \
                    num_samples, num_instruments, num_subsongs, size_bytes, instruments, comment \
             FROM modules WHERE id = ?1",
            [id],
            |r| {
                Ok(json!({
                    "id": r.get::<_, i64>(0)?,
                    "filename": r.get::<_, String>(1)?,
                    "format": r.get::<_, String>(2)?,
                    "title": r.get::<_, Option<String>>(3)?.unwrap_or_default(),
                    "duration": r.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
                    "channels": r.get::<_, Option<i64>>(5)?.unwrap_or(0),
                    "rootCid": r.get::<_, String>(6)?,
                    "numSamples": r.get::<_, Option<i64>>(7)?.unwrap_or(0),
                    "numInstruments": r.get::<_, Option<i64>>(8)?.unwrap_or(0),
                    "numSubsongs": r.get::<_, Option<i64>>(9)?.unwrap_or(0),
                    "sizeBytes": r.get::<_, Option<i64>>(10)?.unwrap_or(0),
                    "instruments": r.get::<_, Option<String>>(11)?.unwrap_or_default(),
                    "comment": r.get::<_, Option<String>>(12)?.unwrap_or_default(),
                }))
            },
        )
        .optional()?;
    Ok(row.unwrap_or(Value::Null))
}

fn formats(conn: &Connection) -> rusqlite::Result<Value> {
    // Fast path: precomputed aggregates in the meta table (refreshed each ingest).
    let counts: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = 'format_counts'", [], |r| r.get(0))
        .optional()?;
    let total: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = 'total'", [], |r| r.get(0))
        .optional()?;
    if let (Some(c), Some(t)) = (counts, total) {
        let arr: Value = serde_json::from_str(&c).unwrap_or_else(|_| json!([]));
        return Ok(json!({ "formats": arr, "total": t.parse::<i64>().unwrap_or(0) }));
    }
    // Fallback scan (DB without a meta table).
    let mut stmt =
        conn.prepare("SELECT format, COUNT(*) AS count FROM modules GROUP BY format ORDER BY count DESC")?;
    let formats: Vec<Value> = stmt
        .query_map([], |r| {
            Ok(json!({ "format": r.get::<_, String>(0)?, "count": r.get::<_, i64>(1)? }))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM modules", [], |r| r.get(0))?;
    Ok(json!({ "formats": formats, "total": total }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use rust_ipfs::unixfs::UnixfsStatus;
    use rust_unixfs::file::adder::Chunker;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    /// Build a synthetic catalog DB (the columns/indexes/fts/meta the ported queries
    /// hit) at `path`, rollback-journal so it opens read-only over the VFS. Row 7's
    /// title carries a unique token so search is exactly-1; meta carries the aggregates.
    fn build_catalog(path: &str, rows: i64) {
        let db = rusqlite::Connection::open(path).unwrap();
        db.execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=DELETE;
             CREATE TABLE modules(id INTEGER PRIMARY KEY, source TEXT UNIQUE, filename TEXT,
               format TEXT, title TEXT, duration REAL, channels INTEGER, num_samples INTEGER,
               num_instruments INTEGER, num_subsongs INTEGER, root_cid TEXT, num_blocks INTEGER,
               size_bytes INTEGER, instruments TEXT, comment TEXT, ingested_at INTEGER);
             CREATE INDEX idx_browse_latest ON modules(
               ingested_at DESC, id DESC, filename, format, title, duration, channels, root_cid);
             CREATE VIRTUAL TABLE modules_fts USING fts5(
               title, filename, instruments, comment, content='', tokenize='unicode61');
             CREATE VIRTUAL TABLE modules_vocab USING fts5vocab('modules_fts','row');
             CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);",
        )
        .unwrap();
        let formats = ["mod", "it", "xm", "s3m"];
        {
            let tx = db.unchecked_transaction().unwrap();
            for i in 1..=rows {
                let fmt = formats[(i as usize) % formats.len()];
                let title = if i == 7 {
                    "track neonunicorn rare".to_string()
                } else {
                    format!("common track {i}")
                };
                let filename = format!("f{i}.{fmt}");
                let instruments = format!("bass kick snare sample{i:05}");
                db.execute(
                    "INSERT INTO modules(source,filename,format,title,duration,channels,num_samples,
                       num_instruments,num_subsongs,root_cid,num_blocks,size_bytes,instruments,comment,ingested_at)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
                    rusqlite::params![
                        format!("s{i}"), filename, fmt, title, 100.0 + i as f64, 4i64, 8i64, 4i64,
                        1i64, format!("bafkreiroot{i}"), 3i64, 1000i64, instruments, "", 1_700_000_000i64 + i
                    ],
                )
                .unwrap();
                let id = db.last_insert_rowid();
                db.execute(
                    "INSERT INTO modules_fts(rowid,title,filename,instruments,comment) VALUES(?1,?2,?3,?4,?5)",
                    rusqlite::params![id, title, filename, format!("bass kick snare sample{i:05}"), ""],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        // Precomputed aggregates (what the client's formats()/count() read).
        let mut stmt = db
            .prepare("SELECT format, COUNT(*) c FROM modules GROUP BY format ORDER BY c DESC")
            .unwrap();
        let counts: Vec<Value> = stmt
            .query_map([], |r| Ok(json!({ "format": r.get::<_, String>(0)?, "count": r.get::<_, i64>(1)? })))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        drop(stmt);
        db.execute("INSERT INTO meta VALUES('format_counts', ?1)", [serde_json::to_string(&counts).unwrap()])
            .unwrap();
        db.execute("INSERT INTO meta VALUES('total', ?1)", [rows.to_string()]).unwrap();
        drop(db); // flush + close; a clean rollback-journal file remains
    }

    /// Add a file to a node as a page-aligned (16 KB) UnixFS DAG; return its root CID.
    async fn add_catalog(node: &crate::ipfs::Node, path: &str) -> Cid {
        let mut add = node
            .ipfs
            .add_unixfs(std::path::PathBuf::from(path))
            .chunk(Chunker::Size(16 * 1024))
            .pin(true);
        let mut root = None;
        while let Some(status) = add.next().await {
            match status {
                UnixfsStatus::CompletedStatus { path, .. } => root = path.root().cid().copied(),
                UnixfsStatus::FailedStatus { error, .. } => panic!("add_unixfs failed: {error}"),
                _ => {}
            }
        }
        root.expect("add_unixfs yielded a root CID")
    }

    /// R1.1 ship-gate (folds the old R1.2): node A publishes the catalog DAG; node B —
    /// holding none of it — resolves it over Bitswap from A and answers search/browse/
    /// detail/formats correctly, fetching ≪ the whole DB. Two real in-process nodes, no
    /// tracker, mirroring `peer_to_peer_queries_survive_a_dead_box`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "slow (~5s, spins up two real libp2p nodes + builds a catalog DB)"]
    async fn catalog_queries_lazily_over_bitswap_from_a_peer() {
        const ROWS: i64 = 5000;
        let path = std::env::temp_dir().join(format!("ts-catalog-test-{}.db", std::process::id()));
        let path = path.to_str().unwrap().to_string();
        build_catalog(&path, ROWS);
        let db_size = std::fs::metadata(&path).unwrap().len();
        assert!(db_size > 512 * 1024, "test DB should span many blocks, got {db_size} bytes");

        // Node A holds the catalog DAG; node B connects to A and holds none of it.
        let a = crate::ipfs::start(None).await.unwrap();
        let cid = add_catalog(&a, &path).await;
        let b = crate::ipfs::start(None).await.unwrap();
        let port = a
            .ipfs
            .listening_addresses()
            .await
            .unwrap()
            .iter()
            .find_map(|m| m.iter().find_map(|p| match p {
                rust_ipfs::Protocol::Tcp(port) => Some(port),
                _ => None,
            }))
            .expect("A TCP listener");
        crate::ipfs::connect(&b.ipfs, &format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", a.peer_id))
            .await
            .unwrap();

        // All queries run on B, sourcing every page from A over Bitswap.
        let q = |req| run_query(b.ipfs.clone(), cid, req);

        // search: the unique token resolves to exactly row 7.
        let hits = tokio::time::timeout(
            Duration::from_secs(30),
            q(CatalogReq::Search { q: "neonunicorn".into(), limit: Some(20) }),
        )
        .await
        .expect("search within 30s")
        .expect("search ok");
        let results = hits["results"].as_array().unwrap();
        assert_eq!(results.len(), 1, "one match for the unique token");
        assert_eq!(results[0]["id"].as_i64(), Some(7));

        // browse latest: 5 rows, newest (highest ingested_at == highest id) first.
        let list = q(CatalogReq::List { format: None, sort: Some("latest".into()), limit: Some(5), offset: None })
            .await
            .expect("list ok");
        let rows = list["results"].as_array().unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0]["id"].as_i64(), Some(ROWS), "latest first");

        // detail by id, measuring partiality: a point lookup fetches ≪ the whole DB.
        FETCHED_BYTES.store(0, Ordering::SeqCst);
        let detail = q(CatalogReq::Get { id: 7 }).await.expect("get ok");
        assert_eq!(detail["format"].as_str(), Some("s3m")); // 7 % 4 == 3 -> formats[3]
        assert!(detail["title"].as_str().unwrap().contains("neonunicorn"));
        let fetched = FETCHED_BYTES.load(Ordering::SeqCst);
        assert!(fetched > 0, "the query actually fetched pages over Bitswap");
        assert!(
            fetched < db_size,
            "detail lookup must fetch only touched pages ({fetched} B), not the whole DB ({db_size} B)",
        );

        // formats: aggregates served from the meta table.
        let fmts = q(CatalogReq::Formats {}).await.expect("formats ok");
        assert_eq!(fmts["total"].as_i64(), Some(ROWS));
        assert!(fmts["formats"].as_array().unwrap().iter().any(|f| f["format"] == "s3m"));

        println!(
            "OK: B answered search/browse/detail/formats over Bitswap from A; \
             detail fetched {fetched} B of a {db_size} B catalog ({}%)",
            fetched * 100 / db_size
        );
        let _ = std::fs::remove_file(&path);
    }

}
