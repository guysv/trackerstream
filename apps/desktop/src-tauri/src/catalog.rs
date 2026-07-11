//! Catalog-over-IPNS lazy query (R1). The catalog SQLite DB is published on IPFS
//! under the master's signed IPNS record; this module lets the thin client QUERY it
//! without downloading the whole file, by mounting it on a read-only SQLite VFS whose
//! page reads are served over Bitswap. A query (indexed lookup / covering-index browse)
//! touches O(log n) + result pages, so the client fetches a handful of 16 KB blocks,
//! not the ~3 MB DB. (phiresky's sql.js-httpvfs idea, over Bitswap instead of HTTP range.)
//!
//! The crux is the sync→async bridge: `sqlite-vfs`'s `read_exact_at` is synchronous,
//! but the only way to a block is async (the sidecar RPC `cat?offset&length`). We run the
//! whole `rusqlite` open+query on a `spawn_blocking` thread and `block_on` a captured
//! runtime handle for each ranged read — never blocking an async worker.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::Duration;

use cid::Cid;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use crate::rpc::NodeRpc;
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
/// Minimum forward-contiguous run before read-ahead engages. A covering-index browse scans
/// pages in order and quickly builds a long run; a keyword search interleaves FTS posting reads
/// with scattered rowid lookups, so its run never gets here — which stops prefetch from pulling
/// ~3× the pages a search actually reads (measured: jungle 11.3 MB → 3.5 MB at limit 200).
const PREFETCH_MIN_RUN: u64 = 4;
/// Max concurrent leaf fetches per prefetch batch (Bitswap wants to the master).
const FETCH_CONCURRENCY: usize = 16;
const VFS_NAME: &str = "ipfs-catalog";

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
    rpc: NodeRpc,
    rt: Handle,
    cid: Cid,
    /// `CANCEL_EPOCH` at query start; the VFS aborts once the global epoch moves past it.
    epoch: u64,
}

fn cancelled_err() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "catalog query superseded")
}

thread_local! {
    static OPEN_CTX: std::cell::RefCell<Option<OpenCtx>> = const { std::cell::RefCell::new(None) };
}

/// Total bytes pulled over the VFS this process — lets tests assert a query fetched
/// ≪ the whole DB (the "lazy" claim). Relaxed; monotonic until a test resets it.
pub(crate) static FETCHED_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Number of *serialized* network fetch rounds (the latency driver over Bitswap). Each
/// blocking `ensure_chunks`/`fetch_range` that actually hits the network bumps this once —
/// a wave's chunks fetch concurrently, but the query can't proceed until the wave returns,
/// so wave-count ≈ dependent round-trips ≈ cold-latency / RTT. (A B-tree descent = one wave
/// per level; the scattered FTS→row gather = ~one wave per hit — the metric that exposes it.)
pub(crate) static FETCH_WAVES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Total individual ranged-cat calls (fan-out). `waves` counts blocking rounds; this counts
/// the leaves requested across all rounds — together they separate round-trips from bytes.
pub(crate) static CAT_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Bytes pulled over the catalog VFS so far this process (see `FETCHED_BYTES`). Exposed
/// so the search-fetch benchmark (`tests/catalog_search_bench.rs`) can measure how much a
/// fresh client pulls per query: reset, run one `run_query`, read this.
pub fn fetched_bytes() -> u64 {
    FETCHED_BYTES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Zero the VFS fetched-bytes counter (benchmark/test hook — call before each measured query).
pub fn reset_fetched_bytes() {
    FETCHED_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Serialized fetch rounds over the VFS this process (see `FETCH_WAVES`) — the latency proxy.
pub fn fetch_waves() -> u64 {
    FETCH_WAVES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Total ranged-cat calls (leaves requested) this process (see `CAT_CALLS`).
pub fn cat_calls() -> u64 {
    CAT_CALLS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Zero the wave + cat-call counters (benchmark/test hook — call before each measured op).
pub fn reset_fetch_counters() {
    FETCH_WAVES.store(0, std::sync::atomic::Ordering::Relaxed);
    CAT_CALLS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Monotonic cancel epoch. An open `CatalogFile` captures the value it started at and aborts
/// its next VFS page fetch once this has advanced — so a superseded search STOPS pulling pages
/// over Bitswap instead of running to completion. (The frontend's stale-result guard hides the
/// result; only this stops the bytes.) Bumped by `cancel_inflight`, checked in the VFS reads.
static CANCEL_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Cancel every catalog query currently in flight — each aborts at its next page fetch (within
/// one Bitswap round-trip). Driven by the `catalog_cancel` command when a new search (or a
/// cleared box) supersedes the last. A query that starts AFTER this call is unaffected.
pub fn cancel_inflight() {
    CANCEL_EPOCH.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

/// Capacity of the shared page cache, in 16 KB chunks. 4096 × 16 KB = 64 MB — room for the
/// schema, the FTS dictionary's upper B-tree, and a large working set of postings, so an
/// interactive typing burst (and pagination) descends from warm pages instead of re-fetching
/// the top-of-tree per Connection.
const PAGE_CACHE_CHUNKS: usize = 4096;

/// Process-global, CID-keyed page cache. Content addressing makes every `(CID, chunk)` value
/// immutable, so entries never need invalidation — a new catalog CID just populates fresh keys
/// and stale ones age out. Every query opens its own `Connection` with only query-LOCAL
/// sequential state ([`Cache`]); the fetched *pages* live here, shared, so the debounce storm
/// while typing and every `loadMore` reuse the schema + FTS top-of-tree instead of pulling them
/// again. Tick-LRU: `get`/`put` stamp a monotonic tick; eviction is an O(n) min-scan run only
/// when inserting past capacity (bounded by a query's fetch count — negligible next to a page RTT).
struct PageCache {
    map: HashMap<(Cid, u64), (Arc<Vec<u8>>, u64)>,
    tick: u64,
    cap: usize,
}

impl PageCache {
    fn contains(&self, k: &(Cid, u64)) -> bool {
        self.map.contains_key(k)
    }
    /// Fetch a page and mark it most-recently-used. Only real reads call this (not the
    /// membership probes in `ensure_chunks`), so recency tracks what queries actually touch.
    fn get(&mut self, k: &(Cid, u64)) -> Option<Arc<Vec<u8>>> {
        self.tick += 1;
        let tick = self.tick;
        self.map.get_mut(k).map(|e| {
            e.1 = tick;
            e.0.clone()
        })
    }
    fn put(&mut self, k: (Cid, u64), v: Arc<Vec<u8>>) {
        self.tick += 1;
        self.map.insert(k, (v, self.tick));
        while self.map.len() > self.cap {
            let Some(lru) = self.map.iter().min_by_key(|(_, (_, t))| *t).map(|(k, _)| *k) else {
                break;
            };
            self.map.remove(&lru);
        }
    }
}

fn page_cache() -> &'static Mutex<PageCache> {
    static CACHE: OnceLock<Mutex<PageCache>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(PageCache { map: HashMap::new(), tick: 0, cap: PAGE_CACHE_CHUNKS })
    })
}

/// Process-global (CID → file size) cache. The DB size is derived from the SQLite header
/// (`page_size * page_count`), immutable per CID — so the FIRST `open` for a CID probes the
/// header over Bitswap and every later `open` reuses it. Without this, the concurrent gather
/// (A1) would pay one header round-trip PER connection it opens, adding a wave per hit and
/// erasing the parallelism win. Also trims a round-trip off every ordinary per-query open.
fn size_cache() -> &'static Mutex<HashMap<Cid, u64>> {
    static SIZES: OnceLock<Mutex<HashMap<Cid, u64>>> = OnceLock::new();
    SIZES.get_or_init(|| Mutex::new(HashMap::new()))
}

// ---------------------------------------------------------------------------------
// TSZCAT — per-page-zstd catalog manifest. The root CID is NOT a raw SQLite file but a
// small manifest listing one block CID per (zstd-compressed) 16 KB SQLite page. The client
// fetches the manifest once, then `block/get`s + decompresses each page it needs — so the
// wire carries ~2.2x fewer bytes while lazy paging is fully preserved (each page is still an
// independent content-addressed fetch). Layout: [magic 8][page_size u32 LE][page_count u32 LE]
// [page_count × 36-byte CIDv1(raw, sha2-256)]. A raw-SQLite root (legacy) is detected by its
// "SQLite format 3\0" magic and served the original way — so one client reads both formats.
const TSZCAT_MAGIC: &[u8; 8] = b"TSZCAT1\n";
const CID_LEN: usize = 36; // CIDv1 raw sha2-256: 0x01 0x55 0x12 0x20 + 32-byte digest

/// Parsed manifest: the logical page size and the per-page block CIDs, keyed by root CID so a
/// re-open of the same catalog reuses it (immutable — content addressed).
fn manifest_cache() -> &'static Mutex<HashMap<Cid, Arc<(u64, Vec<Cid>)>>> {
    static M: OnceLock<Mutex<HashMap<Cid, Arc<(u64, Vec<Cid>)>>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Drop every cached page (test hook). The fetch-byte benchmark calls this before each measured
/// term so it reports true fresh-client cost — otherwise the shared cache would (correctly) show
/// a re-queried client pulling far less than a cold one.
pub fn clear_page_cache() {
    page_cache().lock().unwrap().map.clear();
}

/// Drop the cached (CID → size) entries (test hook) so the next open re-probes the header —
/// lets the A1 bench measure a truly cold first query rather than reusing an earlier probe.
pub fn clear_size_cache() {
    size_cache().lock().unwrap().clear();
    manifest_cache().lock().unwrap().clear();
}

#[derive(Default)]
struct Cache {
    /// Last chunk index read, for sequential-access detection (prefetch trigger).
    last_end: Option<u64>,
    /// Length of the current forward-contiguous run of reads. Advanced in `read_exact_at`
    /// (which runs on every read, cache hit or miss), reset on any jump/backward seek. Gates
    /// prefetch so only an established scan triggers read-ahead — see `PREFETCH_MIN_RUN`.
    seq_run: u64,
}

/// One open catalog DB, bound to a resolved root CID.
struct CatalogFile {
    rpc: NodeRpc,
    rt: Handle,
    cid: Cid,
    size: u64,
    epoch: u64,
    lock: LockKind,
    cache: Mutex<Cache>,
    /// `Some` iff this is a TSZCAT per-page-zstd catalog: `(page_size, page_block_cids)`.
    /// Then reads go via `read_zstd` (block/get + decompress per page) instead of ranged cat.
    manifest: Option<Arc<(u64, Vec<Cid>)>>,
}

impl CatalogFile {
    fn last_chunk(&self) -> u64 {
        if self.size == 0 { 0 } else { (self.size - 1) / CHUNK }
    }

    /// This query has been superseded (a newer search / a clear bumped `CANCEL_EPOCH`).
    fn cancelled(&self) -> bool {
        CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst) != self.epoch
    }

    /// Fetch `[start, end)` of the file via a ranged UnixFS cat (walks only the leaves
    /// overlapping the range). Blocks the current (spawn_blocking) thread on the runtime.
    fn fetch_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if self.cancelled() {
            return Err(cancelled_err());
        }
        let rpc = self.rpc.clone();
        let cid = self.cid;
        let bytes = self
            .rt
            .block_on(async move { rpc.cat(&cid.to_string(), start, end - start).await })
            .map_err(|e| io::Error::other(format!("cat {cid} [{start}..{end}): {e}")))?;
        FETCHED_BYTES.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        FETCH_WAVES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        CAT_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(bytes)
    }

    /// Ensure every chunk in `[first, last]` is cached. Missing chunks (plus a prefetch
    /// tail on sequential access) are fetched CONCURRENTLY — each as its own ranged cat,
    /// so the leaves download in parallel instead of one serial round-trip per page. The
    /// internal DAG nodes are shared via the local blockstore after the first call. (This
    /// only helps sequential scans — covering-index browse, schema load — since a B-tree
    /// point/FTS lookup chains one dependent page read at a time.)
    fn ensure_chunks(&self, first: u64, last: u64) -> io::Result<()> {
        if self.cancelled() {
            return Err(cancelled_err());
        }
        let seq_run = self.cache.lock().unwrap().seq_run;
        let missing: Vec<u64> = {
            let pc = page_cache().lock().unwrap();
            if (first..=last).all(|c| pc.contains(&(self.cid, c))) {
                return Ok(()); // all present in the shared cache
            }
            // Prefetch only once inside an established forward scan (browse). Search's
            // FTS-read → scattered-rowid-lookup alternation never builds the run up, so it
            // no longer triggers the read-ahead that pulled pages it never read.
            let hi = if seq_run >= PREFETCH_MIN_RUN {
                (last + PREFETCH_CHUNKS).min(self.last_chunk())
            } else {
                last
            };
            (first..=hi).filter(|c| !pc.contains(&(self.cid, *c))).collect()
        };

        // One dependent round-trip: the query can't advance until this batch returns, so it
        // counts as a single wave regardless of how many leaves it fetches concurrently.
        if !missing.is_empty() {
            FETCH_WAVES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            CAT_CALLS.fetch_add(missing.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }

        let rpc = self.rpc.clone();
        let cid = self.cid;
        let size = self.size;
        let epoch = self.epoch;
        // Race the fetch batch against the cancel epoch. If a newer search supersedes this one
        // mid-flight, the `cancel` arm wins and `fetch` is DROPPED — which drops the in-flight
        // `rpc.cat` reqwest futures, closing their sidecar connections; tsnode's handleCat sees
        // `r.Context()` cancel and aborts the Bitswap fetch for pages we no longer want. The
        // per-chunk epoch check bounds waste for chunks not yet dialed. (Poll, not a Notify, to
        // sidestep the notify-before-await race — 20 ms latency is nothing next to a page RTT.)
        let fetched: Option<Vec<io::Result<(u64, Vec<u8>)>>> = self.rt.block_on(async move {
            use futures::StreamExt;
            let fetch = futures::stream::iter(missing.into_iter().map(|c| {
                let rpc = rpc.clone();
                async move {
                    if CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst) != epoch {
                        return Err(cancelled_err());
                    }
                    let start = c * CHUNK;
                    let end = ((c + 1) * CHUNK).min(size);
                    let bytes = rpc
                        .cat(&cid.to_string(), start, end - start)
                        .await
                        .map_err(|e| io::Error::other(format!("cat chunk {c} of {cid}: {e}")))?;
                    FETCHED_BYTES.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    Ok((c, bytes))
                }
            }))
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect::<Vec<_>>();
            tokio::pin!(fetch);
            let cancel = async move {
                while CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst) == epoch {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            };
            tokio::pin!(cancel);
            tokio::select! {
                biased;
                _ = &mut cancel => None,
                v = &mut fetch => Some(v),
            }
        });
        let fetched = fetched.ok_or_else(cancelled_err)?;

        let mut pc = page_cache().lock().unwrap();
        for r in fetched {
            let (c, bytes) = r?;
            let k = (self.cid, c);
            if !pc.contains(&k) {
                pc.put(k, Arc::new(bytes));
            }
        }
        Ok(())
    }

    // ----- TSZCAT (per-page-zstd) read path ---------------------------------------
    /// Ensure logical pages `[first, last]` are decompressed into the shared page cache (keyed by
    /// each page's own block CID). Missing pages' COMPRESSED blocks are `block/get`'d concurrently
    /// and zstd-decompressed; a forward scan prefetches ahead like `ensure_chunks`. `FETCHED_BYTES`
    /// counts the compressed wire bytes — the win. Racing the cancel epoch, same as `ensure_chunks`.
    fn ensure_pages(&self, first: u64, last: u64) -> io::Result<()> {
        if self.cancelled() {
            return Err(cancelled_err());
        }
        let (page_size, pages) = {
            let m = self.manifest.as_ref().expect("ensure_pages on a raw catalog");
            (m.0, m.1.clone())
        };
        let last_page = (pages.len() as u64).saturating_sub(1);
        let seq_run = self.cache.lock().unwrap().seq_run;
        let missing: Vec<u64> = {
            let pc = page_cache().lock().unwrap();
            if (first..=last).all(|p| pc.contains(&(pages[p as usize], 0))) {
                return Ok(());
            }
            let hi = if seq_run >= PREFETCH_MIN_RUN {
                (last + PREFETCH_CHUNKS).min(last_page)
            } else {
                last
            };
            (first..=hi).filter(|p| !pc.contains(&(pages[*p as usize], 0))).collect()
        };
        let rpc = self.rpc.clone();
        let epoch = self.epoch;
        let want: Vec<(u64, Cid)> = missing.iter().map(|&p| (p, pages[p as usize])).collect();
        let fetched: Option<Vec<io::Result<(u64, Vec<u8>)>>> = self.rt.block_on(async move {
            use futures::StreamExt;
            let fetch = futures::stream::iter(want.into_iter().map(|(p, pcid)| {
                let rpc = rpc.clone();
                async move {
                    if CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst) != epoch {
                        return Err(cancelled_err());
                    }
                    let comp = rpc.block_get(&pcid.to_string()).await
                        .map_err(|e| io::Error::other(format!("block/get page {p} of {pcid}: {e}")))?;
                    FETCHED_BYTES.fetch_add(comp.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    let page = zstd::bulk::decompress(&comp, page_size as usize)
                        .map_err(|e| io::Error::other(format!("zstd decompress page {p}: {e}")))?;
                    Ok((p, page))
                }
            }))
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect::<Vec<_>>();
            tokio::pin!(fetch);
            let cancel = async move {
                while CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst) == epoch {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            };
            tokio::pin!(cancel);
            tokio::select! {
                biased;
                _ = &mut cancel => None,
                v = &mut fetch => Some(v),
            }
        });
        let fetched = fetched.ok_or_else(cancelled_err)?;
        if !missing.is_empty() {
            FETCH_WAVES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            CAT_CALLS.fetch_add(missing.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        let mut pc = page_cache().lock().unwrap();
        for r in fetched {
            let (p, page) = r?;
            let k = (pages[p as usize], 0u64);
            if !pc.contains(&k) {
                pc.put(k, Arc::new(page));
            }
        }
        Ok(())
    }

    /// Serve a read from a TSZCAT catalog: the logical byte range maps to whole 16 KB pages, each
    /// decompressed and cached by its block CID. Mirrors the raw `read_exact_at` assembly.
    fn read_zstd(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let end = offset + buf.len() as u64;
        if end > self.size {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read past EOF"));
        }
        let page_size = self.manifest.as_ref().unwrap().0;
        let first = offset / page_size;
        let last = (end - 1) / page_size;
        {
            let mut cache = self.cache.lock().unwrap();
            let advancing = cache.last_end.is_some_and(|le| first == le || first == le + 1);
            cache.seq_run = if advancing { cache.seq_run.saturating_add(1) } else { 0 };
        }
        self.ensure_pages(first, last)?;
        let pages = self.manifest.as_ref().unwrap().1.clone();
        let served: Vec<(u64, Arc<Vec<u8>>)> = {
            let mut pc = page_cache().lock().unwrap();
            (first..=last)
                .map(|p| {
                    pc.get(&(pages[p as usize], 0))
                        .map(|a| (p, a))
                        .ok_or_else(|| io::Error::other("page missing after fetch"))
                })
                .collect::<io::Result<_>>()?
        };
        for (p, page) in &served {
            let pstart = p * page_size;
            let avail_end = pstart + page.len() as u64;
            let seg_start = offset.max(pstart);
            let seg_end = end.min(avail_end);
            if seg_end <= seg_start {
                continue;
            }
            let from = (seg_start - pstart) as usize;
            let to = (seg_end - pstart) as usize;
            let dst = (seg_start - offset) as usize;
            buf[dst..dst + (to - from)].copy_from_slice(&page[from..to]);
        }
        self.cache.lock().unwrap().last_end = Some(last);
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
        if self.manifest.is_some() {
            return self.read_zstd(buf, offset);
        }
        let end = offset + buf.len() as u64;
        if end > self.size {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read past EOF"));
        }
        let first = offset / CHUNK;
        let last = (end - 1) / CHUNK;
        {
            // Advance the forward-run counter on a contiguous step (or re-read of the last
            // chunk), reset on any jump/backward seek — before ensure_chunks reads it to gate
            // prefetch. Done here (not in ensure_chunks) because prefetched reads hit the cache
            // and short-circuit ensure_chunks, but must still count toward the run.
            let mut cache = self.cache.lock().unwrap();
            let advancing = cache.last_end.is_some_and(|le| first == le || first == le + 1);
            cache.seq_run = if advancing { cache.seq_run.saturating_add(1) } else { 0 };
        }
        self.ensure_chunks(first, last)?;

        // Pull the served pages out of the shared cache (marking them MRU), then release the
        // global lock before the memcpy so a concurrent query isn't blocked on our copy.
        let chunks: Vec<(u64, Arc<Vec<u8>>)> = {
            let mut pc = page_cache().lock().unwrap();
            (first..=last)
                .map(|c| {
                    pc.get(&(self.cid, c))
                        .map(|a| (c, a))
                        .ok_or_else(|| io::Error::other("chunk missing after fetch"))
                })
                .collect::<io::Result<_>>()?
        };
        for (c, chunk) in &chunks {
            let c = *c;
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
        let mk = |size: u64, manifest: Option<Arc<(u64, Vec<Cid>)>>| CatalogFile {
            rpc: ctx.rpc.clone(),
            rt: ctx.rt.clone(),
            cid: ctx.cid,
            size,
            epoch: ctx.epoch,
            lock: LockKind::None,
            cache: Mutex::new(Cache::default()),
            manifest,
        };
        // Fast paths: a prior open already resolved this CID's format (manifest or raw size).
        if let Some(m) = manifest_cache().lock().unwrap().get(&ctx.cid).cloned() {
            return Ok(mk(m.0 * m.1.len() as u64, Some(m)));
        }
        if let Some(sz) = size_cache().lock().unwrap().get(&ctx.cid).copied() {
            return Ok(mk(sz, None));
        }
        // Cold: probe the first bytes to tell a TSZCAT per-page-zstd manifest from a raw SQLite
        // image, so xFileSize et al. don't fetch the whole DAG.
        let probe = mk(u64::MAX, None);
        let head = probe.fetch_range(0, 100)?;
        if head.len() >= 16 && &head[0..8] == TSZCAT_MAGIC {
            let page_size = u32::from_le_bytes([head[8], head[9], head[10], head[11]]) as u64;
            let page_count = u32::from_le_bytes([head[12], head[13], head[14], head[15]]) as u64;
            let man_len = 16 + CID_LEN as u64 * page_count;
            let man = probe.fetch_range(0, man_len)?;
            if (man.len() as u64) < man_len {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated TSZCAT manifest"));
            }
            let mut pages = Vec::with_capacity(page_count as usize);
            for i in 0..page_count as usize {
                let off = 16 + i * CID_LEN;
                let cid = Cid::try_from(&man[off..off + CID_LEN]).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("bad page cid {i}: {e}"))
                })?;
                pages.push(cid);
            }
            let m = Arc::new((page_size, pages));
            manifest_cache().lock().unwrap().insert(ctx.cid, m.clone());
            return Ok(mk(page_size * page_count, Some(m)));
        }
        if head.len() < 100 || &head[0..16] != b"SQLite format 3\0" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not a sqlite db or TSZCAT manifest"));
        }
        let page_size = match u16::from_be_bytes([head[16], head[17]]) {
            1 => 65536u64,
            v => v as u64,
        };
        let page_count = u32::from_be_bytes([head[28], head[29], head[30], head[31]]) as u64;
        let sz = page_size * page_count;
        size_cache().lock().unwrap().insert(ctx.cid, sz);
        Ok(mk(sz, None))
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
    Search {
        q: String,
        #[serde(default)] limit: Option<i64>,
        /// Keyset cursor for pagination: return only matches with rowid > `after` (the id of
        /// the last row the client already has). Omit for the first page. Cheaper than OFFSET
        /// over the Bitswap VFS — it skips straight to the next span instead of re-scanning.
        #[serde(default)] after: Option<i64>,
        /// Restrict the match to title/filename only (the `names_fts` index) instead of the
        /// full modules_fts (which also matches instruments + comment). The search box's
        /// "names only" toggle sets this — discovery search off, known-module lookup on.
        #[serde(default)] names: bool,
    },
    List {
        #[serde(default)] format: Option<String>,
        /// Browse a single genre (TMA genreid). Served index-only by the partial
        /// idx_browse_genre; takes precedence over `format` (one facet at a time).
        #[serde(default)] genre: Option<i64>,
        /// Browse the *un-genred* tail (`genreid IS NULL`) — the home "n/a" facet. The majority
        /// of the corpus is un-genred, so idx_browse_genre (partial, IS NOT NULL) can't serve
        /// this; the latest-sort path walks idx_latest and matches fast (most rows qualify).
        /// Takes precedence over both `genre` and `format`.
        #[serde(default)] no_genre: bool,
        #[serde(default)] sort: Option<String>,
        #[serde(default)] limit: Option<i64>,
        #[serde(default)] offset: Option<i64>,
    },
    Get { id: i64 },
    /// Resolve by content md5 — the stable key playlists store (survives re-ingest,
    /// unlike the rowid `id`). Answers with the same `ModuleDetail` shape as `Get`.
    GetByMd5 { md5: String },
    Formats {},
    /// Per-genre counts for the home genre directory — reads the precomputed
    /// meta.genre_counts (one page), mirror of `Formats`.
    Genres {},
}

/// Resolve an IPNS name (e.g. `CATALOG_IPNS_KEY`) to its current CID via the node's
/// `routing/get`, verifying the signed record locally (the node is an untrusted cache).
/// Thin helper for the search benchmark to hit the prod-published catalog; the app's own
/// path (`resolve_ipns_name` in lib.rs) additionally layers an on-disk verified cache.
pub async fn resolve_ipns_cid(rpc: &NodeRpc, name: &str) -> Result<Cid, String> {
    let record = rpc.routing_get(name).await.map_err(|e| e.to_string())?;
    crate::ipns::verify_b64(name, &record).map_err(|e| e.to_string())
}

/// Prewarm the shared page cache for a catalog: run one tiny probe search so the schema, the FTS
/// dictionary's upper B-tree, and a first posting/row land in `PAGE_CACHE` before the user types.
/// The first real keystroke then descends from warm nodes instead of paying the cold schema +
/// FTS-root round-trips. Cheap (`LIMIT 1`, flat rowid). Best-effort — the caller ignores errors.
pub async fn warm(rpc: NodeRpc, cid: Cid) -> Result<(), String> {
    run_search_stream(rpc, cid, "the".to_string(), 1, None, false, |_| {}).await?;
    Ok(())
}

/// Client's expected catalog schema version — mirrors `CATALOG_SCHEMA_VERSION` in
/// apps/server/src/catalog.ts. The client hand-ports the server SQL with POSITIONAL row
/// mapping (`HIT_COLS` / `DETAIL_COLS` + `r.get(N)`), so a column reorder/drop it can't see
/// would silently mis-read. Bump in lockstep with the server ONLY on a non-additive layout
/// change; additive columns (as md5 + genreid already were) keep this version and stay readable.
const CATALOG_SCHEMA_VERSION: i64 = 1;

/// Catalog CIDs whose schema_version we've already checked — warn at most once per catalog per
/// process (mirrors the size_cache/manifest_cache accessor pattern).
fn schema_checked() -> &'static Mutex<HashSet<Cid>> {
    static S: OnceLock<Mutex<HashSet<Cid>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Read `meta.schema_version` once per catalog CID and WARN if it's newer than this client
/// understands. The positional row-mapping would otherwise mis-read a reordered/dropped column
/// with no signal — this is the soft-degrade guard (wire-version hardening). Best-effort: a
/// catalog predating the key (no row) or a read error is treated as compatible and stays silent,
/// since additive schema growth remains readable.
fn check_schema_version(conn: &Connection, cid: &Cid) {
    if schema_checked().lock().unwrap().contains(cid) {
        return;
    }
    let ver: Option<i64> = conn
        .query_row("SELECT value FROM meta WHERE key = 'schema_version'", [], |r| r.get::<_, String>(0))
        .optional()
        .ok()
        .flatten()
        .and_then(|s| s.parse::<i64>().ok());
    if let Some(v) = ver {
        if v > CATALOG_SCHEMA_VERSION {
            log::warn!(
                target: "catalog",
                "catalog {cid} schema_version {v} is newer than this client understands \
                 ({CATALOG_SCHEMA_VERSION}); some columns may read incompletely — please update trackerstream"
            );
        }
    }
    schema_checked().lock().unwrap().insert(*cid);
}

/// Resolve + open the catalog over the VFS and answer a query. Runs on a blocking
/// thread (rusqlite is sync; the VFS block_on's per page read). Returns JSON matching
/// the frontend response shapes so the Svelte call sites are unchanged.
pub async fn run_query(rpc: NodeRpc, cid: Cid, req: CatalogReq) -> Result<Value, String> {
    let rt = Handle::current();
    tokio::task::spawn_blocking(move || {
        ensure_registered();
        // Snapshot the cancel epoch NOW; a later search bumps it and this query's VFS reads abort.
        let epoch = CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst);
        OPEN_CTX.with(|c| *c.borrow_mut() = Some(OpenCtx { rpc, rt, cid, epoch }));
        let result = (|| {
            let conn = Connection::open_with_flags_and_vfs(
                cid.to_string(),
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                VFS_NAME,
            )
            .map_err(|e| format!("open catalog {cid}: {e}"))?;
            conn.pragma_update(None, "query_only", true).ok();
            // Larger page cache than SQLite's 2 MB default (~128 of these 16 KB pages): a
            // bm25 scan's working set is several MB, and each connection is opened fresh per
            // query, so a small cache thrashes and re-reads pages within a single scan. 64 MB
            // holds any bounded query's working set. Negative = KiB (not page count).
            conn.pragma_update(None, "cache_size", -65536i64).ok();
            // Soft-degrade guard: warn once if this catalog was baked with a newer schema than
            // our positional row-mapping understands (wire-version hardening). Never fails the query.
            check_schema_version(&conn, &cid);
            dispatch(&conn, &req).map_err(|e| e.to_string())
        })();
        OPEN_CTX.with(|c| *c.borrow_mut() = None);
        result
    })
    .await
    .map_err(|e| format!("catalog query task: {e}"))?
}

/// Bench/analysis hook: run the FTS MATCH ONLY (no JOIN to `modules`) and return how many
/// rowids it produced. Its VFS waves isolate the FTS-walk cost; `full_search_waves − this`
/// is the scattered per-hit gather — exactly the part a concurrent gather (A1) would collapse
/// from N serial page-fetches to ~ceil(N/concurrency) parallel ones. Not a product path.
pub async fn run_fts_only(rpc: NodeRpc, cid: Cid, q: String, limit: i64) -> Result<usize, String> {
    let rt = Handle::current();
    tokio::task::spawn_blocking(move || -> Result<usize, String> {
        ensure_registered();
        let epoch = CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst);
        OPEN_CTX.with(|c| *c.borrow_mut() = Some(OpenCtx { rpc, rt, cid, epoch }));
        let out = (|| -> Result<usize, String> {
            let conn = Connection::open_with_flags_and_vfs(
                cid.to_string(), OpenFlags::SQLITE_OPEN_READ_ONLY, VFS_NAME,
            ).map_err(|e| format!("open catalog {cid}: {e}"))?;
            conn.pragma_update(None, "query_only", true).ok();
            conn.pragma_update(None, "cache_size", -65536i64).ok();
            let q = q.trim();
            let matchstr = match build_matchstr(q) {
                Some(m) => m,
                None => return Ok(0),
            };
            let mut stmt = conn
                .prepare("SELECT rowid FROM modules_fts WHERE modules_fts MATCH ?1 LIMIT ?2")
                .map_err(|e| e.to_string())?;
            let n = stmt
                .query_map(rusqlite::params![matchstr, limit], |r| r.get::<_, i64>(0))
                .map_err(|e| e.to_string())?
                .count();
            Ok(n)
        })();
        OPEN_CTX.with(|c| *c.borrow_mut() = None);
        out
    })
    .await
    .map_err(|e| format!("fts-only task: {e}"))?
}

/// A1 phase 1: matched rowids via FTS ONLY (no JOIN), ascending FTS order. Same match
/// expansion as `search`. The FTS walk is inherently serial (dependent posting reads); this
/// isolates it so phase 2 can parallelize the part that scatters — the per-hit gather.
async fn fts_rowids(
    rpc: NodeRpc, cid: Cid, q: String, limit: i64, after: Option<i64>,
) -> Result<Vec<i64>, String> {
    let rt = Handle::current();
    tokio::task::spawn_blocking(move || -> Result<Vec<i64>, String> {
        ensure_registered();
        let epoch = CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst);
        OPEN_CTX.with(|c| *c.borrow_mut() = Some(OpenCtx { rpc, rt, cid, epoch }));
        let out = (|| -> Result<Vec<i64>, String> {
            let conn = Connection::open_with_flags_and_vfs(
                cid.to_string(), OpenFlags::SQLITE_OPEN_READ_ONLY, VFS_NAME,
            ).map_err(|e| format!("open {cid}: {e}"))?;
            conn.pragma_update(None, "query_only", true).ok();
            conn.pragma_update(None, "cache_size", -65536i64).ok();
            let q = q.trim();
            let matchstr = match build_matchstr(q) {
                Some(m) => m,
                None => return Ok(vec![]),
            };
            let mut ids = Vec::new();
            if let Some(a) = after {
                let mut stmt = conn.prepare(
                    "SELECT rowid FROM modules_fts WHERE modules_fts MATCH ?1 AND rowid > ?2 LIMIT ?3",
                ).map_err(|e| e.to_string())?;
                let rows = stmt.query_map(rusqlite::params![matchstr, a, limit], |r| r.get::<_, i64>(0))
                    .map_err(|e| e.to_string())?;
                for r in rows { ids.push(r.map_err(|e| e.to_string())?); }
            } else {
                let mut stmt = conn.prepare(
                    "SELECT rowid FROM modules_fts WHERE modules_fts MATCH ?1 LIMIT ?2",
                ).map_err(|e| e.to_string())?;
                let rows = stmt.query_map(rusqlite::params![matchstr, limit], |r| r.get::<_, i64>(0))
                    .map_err(|e| e.to_string())?;
                for r in rows { ids.push(r.map_err(|e| e.to_string())?); }
            }
            Ok(ids)
        })();
        OPEN_CTX.with(|c| *c.borrow_mut() = None);
        out
    }).await.map_err(|e| format!("fts_rowids task: {e}"))?
}

/// A1 phase 2 worker: one hit-column row by id on its own connection. Many of these run
/// concurrently on spawn_blocking threads; the SIZE_CACHE keeps each open probe-free, so the
/// only Bitswap cost is the row's own B-tree descent — and those descents overlap.
async fn get_hit_row(rpc: NodeRpc, cid: Cid, id: i64) -> Option<Value> {
    let rt = Handle::current();
    tokio::task::spawn_blocking(move || -> Option<Value> {
        ensure_registered();
        let epoch = CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst);
        OPEN_CTX.with(|c| *c.borrow_mut() = Some(OpenCtx { rpc, rt, cid, epoch }));
        let out = (|| {
            let conn = Connection::open_with_flags_and_vfs(
                cid.to_string(), OpenFlags::SQLITE_OPEN_READ_ONLY, VFS_NAME,
            )
            .map_err(|e| log::warn!(target: "catalog", "get_hit_row open {cid}: {e}"))
            .ok()?;
            conn.pragma_update(None, "query_only", true).ok();
            let cols = HIT_COLS.replace("m.", "");
            let sql = format!("SELECT {cols} FROM modules WHERE id = ?1");
            // A DB/VFS/Bitswap read error here would otherwise silently drop the hit from the
            // results (indistinguishable from "no such row"); log so a network/DB fault is visible.
            conn.query_row(&sql, [id], hit_row)
                .optional()
                .map_err(|e| log::warn!(target: "catalog", "get_hit_row id={id}: {e}"))
                .ok()
                .flatten()
        })();
        OPEN_CTX.with(|c| *c.borrow_mut() = None);
        out
    }).await.ok().flatten()
}

/// A1: two-phase concurrent search. Phase 1 gets the matching rowids via FTS only; phase 2
/// fetches each hit row CONCURRENTLY (`buffer_unordered`), collapsing the scattered gather —
/// which dominates cold search latency (one serialized page-fetch chain per hit) — into
/// ~ceil(N/FETCH_CONCURRENCY) overlapping waves. Returns `{results:[...]}` in ascending FTS
/// rowid order (the concurrent gather completes out of order, so we re-sort). Same result set
/// as `search`; only the fetch scheduling differs.
pub async fn run_search_parallel(
    rpc: NodeRpc, cid: Cid, q: String, limit: i64, after: Option<i64>,
) -> Result<Value, String> {
    let ids = fts_rowids(rpc.clone(), cid, q, limit, after).await?;
    use futures::StreamExt;
    let mut rows: Vec<(i64, Value)> = Vec::with_capacity(ids.len());
    let mut it = ids.into_iter();
    // Prewarm: fetch the FIRST hit alone so the `modules` B-tree root + upper interior pages
    // land in the shared page cache before the fan-out. Otherwise the concurrent gets all
    // descend from cold and thundering-herd re-fetch those shared pages (measured: +40 waves,
    // +0.6 MB, and it erased the parallelism win). With the tree warm, the concurrent gets only
    // pull their own distinct leaves.
    if let Some(first) = it.next() {
        if let Some(v) = get_hit_row(rpc.clone(), cid, first).await {
            rows.push((first, v));
        }
    }
    let rest: Vec<(i64, Value)> = futures::stream::iter(it.map(|id| {
        let rpc = rpc.clone();
        async move { get_hit_row(rpc, cid, id).await.map(|v| (id, v)) }
    }))
    .buffer_unordered(FETCH_CONCURRENCY)
    .filter_map(|x| async move { x })
    .collect()
    .await;
    rows.extend(rest);
    rows.sort_by_key(|(id, _)| *id);
    Ok(json!({ "results": rows.into_iter().map(|(_, v)| v).collect::<Vec<_>>() }))
}

fn dispatch(conn: &Connection, req: &CatalogReq) -> rusqlite::Result<Value> {
    match req {
        CatalogReq::Search { q, limit, after, names } => search(conn, q, limit.unwrap_or(50), *after, *names),
        CatalogReq::List { format, genre, no_genre, sort, limit, offset } => {
            list(conn, format.as_deref(), *genre, *no_genre, sort.as_deref(), limit.unwrap_or(100), offset.unwrap_or(0))
        }
        CatalogReq::Get { id } => get(conn, *id),
        CatalogReq::GetByMd5 { md5 } => get_by_md5(conn, md5),
        CatalogReq::Formats {} => formats(conn),
        CatalogReq::Genres {} => genres(conn),
    }
}

const HIT_COLS: &str =
    "m.id, m.md5, m.filename, m.format, m.title, m.duration, m.channels, m.root_cid";

/// Map an 8-column hit row (id, md5, filename, format, title, duration, channels, root_cid).
/// `md5` is the content hash — the stable, cross-rebake key playlists store (the rowid `id`
/// is reassigned on every full re-ingest, so it must never be persisted).
fn hit_row(r: &rusqlite::Row) -> rusqlite::Result<Value> {
    Ok(json!({
        "id": r.get::<_, i64>(0)?,
        "md5": r.get::<_, Option<String>>(1)?.unwrap_or_default(),
        "filename": r.get::<_, String>(2)?,
        "format": r.get::<_, String>(3)?,
        "title": r.get::<_, Option<String>>(4)?.unwrap_or_default(),
        "duration": r.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
        "channels": r.get::<_, Option<i64>>(6)?.unwrap_or(0),
        "rootCid": r.get::<_, String>(7)?,
    }))
}

fn collect(stmt: &mut rusqlite::Statement, params: &[&dyn rusqlite::ToSql]) -> rusqlite::Result<Vec<Value>> {
    let rows = stmt.query_map(params, hit_row)?;
    rows.collect()
}

/// The FTS table a search matches against: the full index (title/filename/instruments/comment)
/// or the names-only index (title/filename), per the "names only" toggle. Both carry the same
/// rowid (`modules.id`), so the JOIN to `modules` and keyset pagination are identical either way.
fn fts_table(names_only: bool) -> &'static str {
    if names_only { "names_fts" } else { "modules_fts" }
}

/// Build the FTS5 MATCH string for a user query against the `detail='none'` catalog indexes
/// (`modules_fts` / `names_fts`). Those indexes DON'T support phrase queries, so we must never
/// emit one. A single whitespace term like `c20g_j` is TWO `unicode61` tokens (`_`, `.`, `-`, `'`
/// … are separators), so quoting it whole as `"c20g_j"` is a two-token phrase and FTS5 errors
/// with "phrase queries are not supported (detail!=full)" — which the search UI surfaces as
/// "search offline". Instead we split on the SAME boundaries `unicode61` tokenizes on (non
/// alphanumerics) and emit each sub-token as its own prefix term (`"tok"*`), space-joined =
/// implicit AND. For normal single-word/multi-word queries this is byte-identical to the old
/// behavior; it only changes terms that used to fail. A query already using FTS operators
/// (`"`, `*`, `:`, `^`) is passed through verbatim so power users keep full control. Returns
/// `None` when nothing tokenizable remains (e.g. the query is all punctuation) — callers should
/// treat that as an empty result set rather than running `MATCH ''` (which itself errors).
fn build_matchstr(q: &str) -> Option<String> {
    if q.contains('"') || q.contains('*') || q.contains(':') || q.contains('^') {
        return Some(q.to_string()); // explicit FTS syntax — the user drives the query
    }
    let terms: Vec<String> = q
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| format!("\"{s}\"*"))
        .collect();
    if terms.is_empty() { None } else { Some(terms.join(" ")) }
}

fn search(conn: &Connection, query: &str, limit: i64, after: Option<i64>, names_only: bool) -> rusqlite::Result<Value> {
    let q = query.trim();
    if q.is_empty() {
        return Ok(json!({ "results": [] }));
    }
    let fts = fts_table(names_only);
    let matchstr = match build_matchstr(q) {
        Some(m) => m,
        None => return Ok(json!({ "results": [] })),
    };
    // No relevance ranking: take the first LIMIT matches in FTS rowid (≈ ingest) order, like
    // ModArchive's own search. `ORDER BY bm25` had to score EVERY matching row before LIMIT, and
    // each scored posting is a page fetched over the Bitswap VFS — so a mid-frequency term
    // ("mario": ~34 MB / 2000+ pages / 90 s cold) cost far more than a broad one. Flat rowid
    // order touches ~280 pages (~4.5 MB) regardless of term frequency. (If top-result relevance
    // matters later, rank a bounded rowid-order window — never the whole match set.)
    // Keyset pagination: results are ascending FTS rowid, so the next page is simply the
    // matches with rowid > the last id the client holds. Skips straight past what's already
    // shown instead of OFFSET re-scanning it over the VFS.
    let results = if let Some(a) = after {
        let sql = format!(
            "SELECT {HIT_COLS} FROM {fts} f JOIN modules m ON m.id = f.rowid \
             WHERE {fts} MATCH ?1 AND f.rowid > ?2 LIMIT ?3"
        );
        let mut stmt = conn.prepare(&sql)?;
        collect(&mut stmt, &[&matchstr as &dyn rusqlite::ToSql, &a, &limit])?
    } else {
        let sql = format!(
            "SELECT {HIT_COLS} FROM {fts} f JOIN modules m ON m.id = f.rowid \
             WHERE {fts} MATCH ?1 LIMIT ?2"
        );
        let mut stmt = conn.prepare(&sql)?;
        collect(&mut stmt, &[&matchstr as &dyn rusqlite::ToSql, &limit])?
    };
    Ok(json!({ "results": results }))
}

/// Streaming search: same result set as `search`, but calls `on_row` for each hit the instant
/// SQLite steps to it — each step pulls only that row's pages over the VFS — so the UI can render
/// results as they arrive instead of after the whole page lands. Opens its own connection (same
/// setup as `run_query`) and returns the number of rows emitted. Aborts early (returning what it
/// sent) if a newer query bumps the cancel epoch mid-stream.
pub async fn run_search_stream(
    rpc: NodeRpc,
    cid: Cid,
    q: String,
    limit: i64,
    after: Option<i64>,
    names_only: bool,
    on_row: impl Fn(Value) + Send + 'static,
) -> Result<usize, String> {
    let rt = Handle::current();
    tokio::task::spawn_blocking(move || {
        ensure_registered();
        let epoch = CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst);
        OPEN_CTX.with(|c| *c.borrow_mut() = Some(OpenCtx { rpc, rt, cid, epoch }));
        let result = (|| -> Result<usize, String> {
            let conn = Connection::open_with_flags_and_vfs(
                cid.to_string(),
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                VFS_NAME,
            )
            .map_err(|e| format!("open catalog {cid}: {e}"))?;
            conn.pragma_update(None, "query_only", true).ok();
            conn.pragma_update(None, "cache_size", -65536i64).ok();
            search_stream(&conn, &q, limit, after, names_only, epoch, &on_row).map_err(|e| e.to_string())
        })();
        OPEN_CTX.with(|c| *c.borrow_mut() = None);
        result
    })
    .await
    .map_err(|e| format!("catalog stream task: {e}"))?
}

/// Row-at-a-time variant of `search`'s query. Steps the statement, handing each hit to `on_row`;
/// checks the cancel epoch between rows so a superseded stream stops fetching pages promptly.
fn search_stream(
    conn: &Connection,
    query: &str,
    limit: i64,
    after: Option<i64>,
    names_only: bool,
    epoch: u64,
    on_row: &(dyn Fn(Value) + Send),
) -> rusqlite::Result<usize> {
    let q = query.trim();
    if q.is_empty() {
        return Ok(0);
    }
    let fts = fts_table(names_only);
    let matchstr = match build_matchstr(q) {
        Some(m) => m,
        None => return Ok(0),
    };
    let cursor = after.unwrap_or(0);
    let sql = if after.is_some() {
        format!(
            "SELECT {HIT_COLS} FROM {fts} f JOIN modules m ON m.id = f.rowid \
             WHERE {fts} MATCH ?1 AND f.rowid > ?2 LIMIT ?3"
        )
    } else {
        format!(
            "SELECT {HIT_COLS} FROM {fts} f JOIN modules m ON m.id = f.rowid \
             WHERE {fts} MATCH ?1 LIMIT ?2"
        )
    };
    let params: Vec<&dyn rusqlite::ToSql> = if after.is_some() {
        vec![&matchstr, &cursor, &limit]
    } else {
        vec![&matchstr, &limit]
    };
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params.as_slice())?;
    let mut n = 0usize;
    while let Some(r) = rows.next()? {
        // A newer search bumped the epoch → stop; no point pulling pages for a stale query.
        if CANCEL_EPOCH.load(std::sync::atomic::Ordering::SeqCst) != epoch {
            break;
        }
        on_row(hit_row(r)?);
        n += 1;
    }
    Ok(n)
}

fn list(
    conn: &Connection,
    format: Option<&str>,
    genre: Option<i64>,
    no_genre: bool,
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
    // One facet at a time keeps every browse index-only (idx_browse_genre / idx_browse_format).
    // no_genre ("n/a") wins first, then genre, then format. `genreid = ?` implies IS NOT NULL, so
    // the *partial* idx_browse_genre still covers it (verified via EXPLAIN QUERY PLAN). The IS NULL
    // tail has no partial index (it's most of the corpus), so it filters over the sort index.
    let where_clause = if no_genre {
        "WHERE genreid IS NULL"
    } else if genre.is_some() {
        "WHERE genreid = ?"
    } else if format.is_some() {
        "WHERE format = ?"
    } else {
        ""
    };
    let sql = format!(
        "SELECT {} FROM modules {where_clause} ORDER BY {order} LIMIT ? OFFSET ?",
        HIT_COLS.replace("m.", "")
    );
    let mut stmt = conn.prepare(&sql)?;
    let results = if no_genre {
        collect(&mut stmt, &[&limit as &dyn rusqlite::ToSql, &offset])?
    } else if let Some(g) = genre {
        collect(&mut stmt, &[&g as &dyn rusqlite::ToSql, &limit, &offset])?
    } else if let Some(fmt) = format {
        collect(&mut stmt, &[&fmt as &dyn rusqlite::ToSql, &limit, &offset])?
    } else {
        collect(&mut stmt, &[&limit as &dyn rusqlite::ToSql, &offset])?
    };
    Ok(json!({ "results": results }))
}

// Genre-less column list — the fallback query for a catalog predating the genre
// column/table (see `detail_get`). Unprefixed since it selects from `modules` alone.
const DETAIL_COLS: &str =
    "id, md5, filename, format, title, duration, channels, root_cid, \
     num_samples, num_instruments, num_subsongs, size_bytes, instruments, comment";

// Genre-enabled column list: same 14 columns (aliased `m.`) plus the joined genre label
// at index 14. LEFT JOIN so un-genred rows (the majority) still return, with genre NULL.
const DETAIL_COLS_GENRE: &str =
    "m.id, m.md5, m.filename, m.format, m.title, m.duration, m.channels, m.root_cid, \
     m.num_samples, m.num_instruments, m.num_subsongs, m.size_bytes, m.instruments, m.comment, \
     g.genre";

/// Map a full detail row to the `ModuleDetail` JSON shape. Handles both column lists:
/// `genre` reads column 14, which is absent in the genre-less fallback — an out-of-range
/// `get` there yields None (→ null → "n/a" in the UI), so one mapper serves both queries.
fn detail_row(r: &rusqlite::Row) -> rusqlite::Result<Value> {
    Ok(json!({
        "id": r.get::<_, i64>(0)?,
        "md5": r.get::<_, Option<String>>(1)?.unwrap_or_default(),
        "filename": r.get::<_, String>(2)?,
        "format": r.get::<_, String>(3)?,
        "title": r.get::<_, Option<String>>(4)?.unwrap_or_default(),
        "duration": r.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
        "channels": r.get::<_, Option<i64>>(6)?.unwrap_or(0),
        "rootCid": r.get::<_, String>(7)?,
        "numSamples": r.get::<_, Option<i64>>(8)?.unwrap_or(0),
        "numInstruments": r.get::<_, Option<i64>>(9)?.unwrap_or(0),
        "numSubsongs": r.get::<_, Option<i64>>(10)?.unwrap_or(0),
        "sizeBytes": r.get::<_, Option<i64>>(11)?.unwrap_or(0),
        "instruments": r.get::<_, Option<String>>(12)?.unwrap_or_default(),
        "comment": r.get::<_, Option<String>>(13)?.unwrap_or_default(),
        "genre": r.get::<_, Option<String>>(14).ok().flatten(),
    }))
}

/// Run the genre-joined detail query, falling back to the genre-less one on a catalog that
/// predates the genre column/table (the JOIN would fail to prepare there). Either way the
/// detail pane loads; on the fallback path `genre` is simply null. `where_frag` is the
/// predicate after `modules`/`modules m` (e.g. `id = ?1`), using the `m.` alias.
fn detail_get(conn: &Connection, where_frag: &str, params: &[&dyn rusqlite::ToSql]) -> rusqlite::Result<Value> {
    let sql = format!(
        "SELECT {DETAIL_COLS_GENRE} FROM modules m \
         LEFT JOIN genres g ON g.genreid = m.genreid WHERE m.{where_frag}"
    );
    if let Ok(row) = conn.query_row(&sql, params, detail_row).optional() {
        return Ok(row.unwrap_or(Value::Null));
    }
    let sql = format!("SELECT {DETAIL_COLS} FROM modules WHERE {where_frag}");
    let row = conn.query_row(&sql, params, detail_row).optional()?;
    Ok(row.unwrap_or(Value::Null))
}

fn get(conn: &Connection, id: i64) -> rusqlite::Result<Value> {
    detail_get(conn, "id = ?1", &[&id])
}

/// Resolve a module by its content md5 — the stable key playlists persist. Seeks the
/// `idx_modules_md5` index (a handful of pages over the Bitswap VFS), not a full scan.
/// md5 is not unique (the same file can be cataloged under multiple sources), but such
/// rows are byte-identical, hence share a `root_cid` — `LIMIT 1` is well-defined.
fn get_by_md5(conn: &Connection, md5: &str) -> rusqlite::Result<Value> {
    detail_get(conn, "md5 = ?1 LIMIT 1", &[&md5])
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

/// Per-genre counts for the home genre directory. Reads the precomputed meta.genre_counts
/// (`[{genreid,genre,count}]`, most-populous first) — a single page over the VFS, mirror of
/// `formats`. Empty list on a DB predating genre (no meta key) rather than a modules⋈genres scan.
fn genres(conn: &Connection) -> rusqlite::Result<Value> {
    let counts: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = 'genre_counts'", [], |r| r.get(0))
        .optional()?;
    let arr: Value = counts
        .and_then(|c| serde_json::from_str(&c).ok())
        .unwrap_or_else(|| json!([]));
    Ok(json!({ "genres": arr }))
}
