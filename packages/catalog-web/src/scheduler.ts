// The catalog page scheduler — a port of apps/desktop/src-tauri/src/catalog.rs's ensure_chunks.
//
// THIS FILE IS LOAD-BEARING. A naive VFS (fetch each page as SQLite asks for it) is functionally
// correct and passes every test, and is unusable on a WAN: a measured browser spike ran an FTS
// query as 204 pages = 204 SERIALIZED round-trips. At a 50ms RTT that is a ten-second search.
// Everything here exists to collapse that wave count, and the constants are not arbitrary — they
// were tuned against the real corpus on the Rust side. Keep them in lockstep.
import type { CID } from "multiformats/cid";

/** Cache + fetch granularity. Matches the publish chunker (`size-16384`), so each cached page
 *  aligns to exactly one TSZCAT block / UnixFS leaf, and one SQLite page. */
export const PAGE_SIZE = 16 * 1024;
/** Read-ahead window, in pages — one concurrent batch. */
export const PREFETCH_PAGES = 15;
/** Minimum forward-contiguous run before read-ahead engages.
 *
 *  The subtle one. Browse is a forward scan and loves read-ahead; SEARCH is not — FTS posting
 *  reads alternate with scattered rowid lookups and never build a run. Prefetching there pulled
 *  ~3x the pages it actually read ("jungle" 11.3MB -> 3.5MB once this gate landed). Do not remove
 *  it to "make search faster": it makes search slower. */
export const PREFETCH_MIN_RUN = 4;
/** In-flight block fetches per batch. */
export const FETCH_CONCURRENCY = 16;
/** Shared page cache bound, in pages (4096 * 16KB = 64MB). Content-addressed, so entries never
 *  need invalidating. */
export const PAGE_CACHE_PAGES = 4096;

export type BlockFetch = (cid: CID, signal?: AbortSignal) => Promise<Uint8Array>;
export type PageDecode = (compressed: Uint8Array) => Uint8Array;

/** Mirrors the Rust FETCHED_BYTES / FETCH_WAVES / CAT_CALLS counters. `waves` is the metric that
 *  matters: it counts DEPENDENT round-trips (a batch is one wave however many blocks it fetches
 *  concurrently), so it is cold latency / RTT. Correctness tests won't catch a regression here;
 *  only this will. */
export const stats = { wireBytes: 0, plainBytes: 0, blocks: 0, waves: 0, cacheHits: 0 };
export function resetStats(): void {
  Object.assign(stats, { wireBytes: 0, plainBytes: 0, blocks: 0, waves: 0, cacheHits: 0 });
}

/** Empty the shared page cache. NOT used by the app (content-addressed entries never go stale); it
 *  exists so the warm-bundle generator can run each canonical query truly COLD and thus capture that
 *  query's FULL page set, not just the pages a prior query in the workload hadn't already cached. */
export function clearPageCache(): void {
  pageCache.clear();
}

/** Tick-LRU over (catalog root, page index). Global, because every query opens its own connection
 *  and they should all share the pages someone already paid for. */
class PageCache {
  private map = new Map<string, Uint8Array>();

  get(key: string): Uint8Array | undefined {
    const v = this.map.get(key);
    if (v !== undefined) {
      this.map.delete(key); // re-insert = mark MRU (Map preserves insertion order)
      this.map.set(key, v);
    }
    return v;
  }

  put(key: string, val: Uint8Array): void {
    if (this.map.has(key)) return; // content-addressed: an existing entry is already correct
    this.map.set(key, val);
    while (this.map.size > PAGE_CACHE_PAGES) {
      const oldest = this.map.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.map.delete(oldest);
    }
  }

  clear(): void {
    this.map.clear();
  }
}

const pageCache = new PageCache();

/** Run `tasks` with at most `limit` in flight. The JS analogue of Rust's
 *  `buffer_unordered(FETCH_CONCURRENCY)` — the whole point is that ONE dependent round-trip
 *  fetches up to 16 blocks at once. */
async function pooled<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const out: T[] = new Array(tasks.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, tasks.length) }, async () => {
    for (;;) {
      const i = next++;
      if (i >= tasks.length) return;
      out[i] = await tasks[i]();
    }
  });
  await Promise.all(workers);
  return out;
}

/**
 * Fetches + decodes catalog pages on demand, with the read-ahead and cancellation behaviour the
 * Rust VFS has. One instance per open catalog (keyed by its root CID).
 */
export class PageScheduler {
  private seqRun = 0;
  private lastEnd: number | null = null;
  /** Bumped by cancel(): a superseded search must stop fetching pages nobody will read. */
  private epoch = 0;

  // Explicit fields, not TS parameter properties: Node's type-stripping (which the repo's tests
  // use to run .ts directly) rejects those, and this package should stay testable outside a bundler.
  private readonly rootKey: string;
  private readonly pages: CID[];
  private readonly fetchBlock: BlockFetch;
  private readonly decode: PageDecode;

  constructor(rootKey: string, pages: CID[], fetchBlock: BlockFetch, decode: PageDecode) {
    this.rootKey = rootKey;
    this.pages = pages;
    this.fetchBlock = fetchBlock;
    this.decode = decode;
  }

  get pageCount(): number {
    return this.pages.length;
  }

  /** Supersede the in-flight query. Its remaining fetches are abandoned, so the node stops pulling
   *  pages the user has already typed past. */
  cancel(): void {
    this.epoch++;
  }

  private key(i: number): string {
    return `${this.rootKey}:${i}`;
  }

  /**
   * Advance the forward-run counter, then ensure `[first, last]` (plus any read-ahead) are resident.
   *
   * The run counter is advanced HERE and not inside the fetch path, and that is deliberate: a
   * prefetched page hits the cache and never reaches the fetch path, but it must still count toward
   * the run — otherwise a successful read-ahead resets the very signal that earned it.
   */
  async ensure(first: number, last: number): Promise<void> {
    const advancing = this.lastEnd !== null && (first === this.lastEnd || first === this.lastEnd + 1);
    this.seqRun = advancing ? this.seqRun + 1 : 0;
    this.lastEnd = last;

    const epoch = this.epoch;
    const missing: number[] = [];
    // Read-ahead only inside an established forward scan — see PREFETCH_MIN_RUN.
    const hi =
      this.seqRun >= PREFETCH_MIN_RUN ? Math.min(last + PREFETCH_PAGES, this.pages.length - 1) : last;
    for (let i = first; i <= hi; i++) {
      if (pageCache.get(this.key(i)) === undefined) missing.push(i);
      else if (i >= first && i <= last) stats.cacheHits++;
    }
    if (missing.length === 0) return;

    // One dependent round-trip: the query cannot advance until this batch returns, so it is a
    // single wave however many blocks it pulls concurrently.
    stats.waves++;

    const results = await pooled(
      missing.map((i) => async () => {
        if (this.epoch !== epoch) return null; // superseded mid-flight — don't pay for it
        const comp = await this.fetchBlock(this.pages[i]);
        const plain = this.decode(comp);
        stats.blocks++;
        stats.wireBytes += comp.length;
        stats.plainBytes += plain.length;
        return [i, plain] as const;
      }),
      FETCH_CONCURRENCY,
    );

    if (this.epoch !== epoch) throw new CancelledError();
    for (const r of results) if (r) pageCache.put(this.key(r[0]), r[1]);
  }

  /** The decoded page. Only valid after `ensure` covered it. */
  page(i: number): Uint8Array {
    const p = pageCache.get(this.key(i));
    if (p === undefined) throw new Error(`catalog: page ${i} not resident (ensure() missed it)`);
    return p;
  }
}

export class CancelledError extends Error {
  constructor() {
    super("catalog query cancelled");
    this.name = "CancelledError";
  }
}
