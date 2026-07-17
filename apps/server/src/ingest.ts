// Ingest pipeline: corpus module -> CID-DAG (sample-level dedup, or flat for
// formats the parsers don't cover) -> block-put + recursive pin on the master
// kubo node (shared chunks stored once) -> libopenmpt metadata -> SQLite/FTS5
// catalog row carrying the root CID. Incremental + re-runnable (skips by source).
import { createHash } from "node:crypto";
import { readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { constants as zlibConstants, zstdCompressSync } from "node:zlib";
import { DatabaseSync } from "node:sqlite";
import { buildDag, buildDagV3, buildDagV4, buildFlatDag, detectFormat, KuboRpc, loadDagToKubo, flacStats } from "@trackerstream/repack";
import { CID } from "multiformats/cid";
import { corpusStats } from "./corpus.ts";
import { CATALOG_IPNS_KEY } from "@trackerstream/config";
import { Catalog } from "./catalog.ts";
import { initMeta, extractModule, type ModuleMeta } from "./meta.ts";
import { forEachModule } from "./corpus.ts";
import { BakePool, Semaphore, Mutex, type BakeResult } from "./bake-pool.ts";

// Catalog publish layout (R1): page-aligned 16 KB chunks so each SQLite page maps to
// one stable UnixFS block (deterministic chunking -> high cross-rebake block reuse).
// NOT raw-leaves: the client's vendored rust-unixfs visitor can't walk raw (codec
// 0x55) leaves ("walk failed"), so leaves stay dag-pb. kubo enables raw-leaves by
// default at cid-version=1, hence the explicit rawLeaves:false in the addFile call.
const CATALOG_CHUNKER = "size-16384";
const CATALOG_KEY_NAME = "catalog"; // kubo keystore name for the catalog signing key
const CATALOG_LIFETIME = "48h"; // IPNS record validity window the client enforces as EOL
const DEFAULT_GENRE_MAP = "api-dumps/tma-genres.json"; // tma-genre-extract.py output (md5 -> genre)

export interface IngestOpts {
  root: string;
  dbPath: string;
  kuboApi: string;
  formats?: string[];
  limit?: number;
  /** Re-bake already-cataloged modules: rebuild the DAG and, when the root CID
   *  changed (e.g. seek tables added since first ingest), re-pin + repoint the
   *  catalog and unpin the superseded root. Without this, existing sources are
   *  skipped (the default incremental behavior). */
  rebuild?: boolean;
  /** Backfill-only pass: for each corpus module already cataloged without an md5,
   *  compute the md5 of the raw file and UPDATE the row (the TMA/ModArchive join
   *  key). Skips DAG build / metadata / kubo entirely — I/O-bound, no re-bake. Used
   *  once after the md5 column is added; then re-publishes the catalog. */
  backfillMd5?: boolean;
  /** Reindex-only pass: rebuild `modules_fts` from the `modules` table (to apply an FTS
   *  schema change like `prefix='2 3'`) and drop the unused fts5vocab table, then
   *  republish. No corpus walk, no DAG build — the module blocks/pins/root_cids are
   *  untouched, only the catalog's search index. Used after an FTS schema edit. */
  reindexFts?: boolean;
  /** Backfill-only pass: stamp genreid onto already-cataloged rows by md5-joining the
   *  api-dumps/tma-genres map (`genreMap`). Pure SQL — no corpus walk, no DAG build, no
   *  re-bake (md5 is already on every row) — then refreshes meta.genre_counts and
   *  republishes. Used once after the genreid column is added (needs md5 backfilled first). */
  backfillGenre?: boolean;
  /** Path to the md5->genre JSON (tma-genre-extract.py output). Used by BACKFILL_GENRE and,
   *  when present, to stamp genreid inline during a normal/rebuild ingest. Defaults to
   *  api-dumps/tma-genres.json; absent file => genre is simply skipped. */
  genreMap?: string;
  /** Publish the catalog DB to IPFS under the master-signed IPNS key at the end of
   *  ingest (R1). Default true; set false for dev slices. */
  publish?: boolean;
  /** Subset re-bake allowlist: when set, only modules whose `source` is in this set are
   *  walked/re-baked (everything else is skipped). Used with rebuild=true to re-bake a
   *  targeted slice (e.g. a genre + a liked list) to v4 without touching the rest. */
  sources?: Set<string>;
  onProgress?: (s: IngestStats) => void;
}

export interface IngestStats {
  processed: number;
  skipped: number;
  failed: number;
  flat: number;
  /** Re-baked: root CID changed and was repointed (rebuild mode only). */
  rebuilt: number;
  /** Already up to date: rebuilt DAG, root CID unchanged (rebuild mode only). */
  unchanged: number;
  total: number;
  ms: number;
}

export async function runIngest(opts: IngestOpts): Promise<IngestStats> {
  const cat = new Catalog(opts.dbPath);
  await initMeta();
  const rpc = new KuboRpc(opts.kuboApi);
  await rpc.id(); // fail fast if the master node is unreachable

  if (opts.backfillMd5) return backfillMd5(cat, rpc, opts);
  if (opts.reindexFts) return reindexFtsMode(cat, rpc, opts);
  if (opts.backfillGenre) return backfillGenre(cat, rpc, opts);

  // Best-effort genre enrichment for the normal/rebuild path: if the md5->genre map is
  // present, populate the label table and stamp genreid on each newly-inserted row inline
  // (existing rows are covered by a separate BACKFILL_GENRE pass). Absent map => no genre.
  const genres = loadGenreMap(opts.genreMap);
  if (genres) cat.upsertGenres(genres.labels);

  const t0 = Date.now();
  let processed = 0,
    skipped = 0,
    failed = 0,
    flat = 0,
    rebuilt = 0,
    unchanged = 0;
  // Count a module as failed AND say which one + why. Previously every failure path was a bare
  // `failed++`, so a run reporting "N failed" gave no clue which module or what broke. `reason`
  // labels the phase; `err` (when present) carries the actual error.
  const fail = (source: string, reason: string, err?: unknown): void => {
    failed++;
    console.error(`[ingest] fail ${source}: ${reason}${err !== undefined ? ` — ${String(err)}` : ""}`);
  };
  // Bake profiler (PROFILE=1): cumulative wall-time per phase, to spot the bottleneck.
  const PROFILE = process.env.PROFILE === "1";
  const prof = { decode: 0, dag: 0, kubo: 0, cat: 0, pin: 0 };

  // Parallel bake (BAKE_WORKERS=N): a pool of worker threads does the CPU half (decode + FLAC
  // encode + CDC) while THIS thread keeps the shared-resource I/O (block/put + catalog + pin)
  // serialized. REBUILD-ONLY: every module must already be cataloged (the full v4 re-bake is
  // pure rebuild); a not-yet-cataloged module is skipped in this mode.
  const nWorkers = +(process.env.BAKE_WORKERS ?? 0);
  if (nWorkers > 0) {
    const pool = new BakePool(nWorkers, new URL("./bake-worker.ts", import.meta.url));
    const sem = new Semaphore(nWorkers + 2); // bound in-flight modules (backpressure vs the serial I/O)
    const io = new Mutex();
    const inflight = new Set<Promise<void>>();
    const applyRebuild = async (m: { source: string }, existingRoot: string, r: BakeResult) => {
      if (!r.ok || !r.root) {
        fail(m.source, "worker bake failed", r.error);
        return;
      }
      if (r.root === existingRoot) {
        unchanged++;
        return;
      }
      const blocks = r.blocks!.map((b) => ({ cid: CID.parse(b.cid), bytes: b.bytes }));
      let _t = Date.now();
      try {
        const { mismatched } = await loadDagToKubo(rpc, blocks, CID.parse(r.root));
        if (mismatched.length) {
          fail(m.source, `loadDagToKubo: ${mismatched.length} block(s) mismatched`);
          return;
        }
      } catch (e) {
        fail(m.source, "loadDagToKubo threw (rebuild)", e);
        return;
      }
      prof.kubo += Date.now() - _t;
      _t = Date.now();
      cat.updateRoot(m.source, r.root, blocks.length);
      prof.cat += Date.now() - _t;
      _t = Date.now();
      try {
        await rpc.pinRm(existingRoot, true);
      } catch {
        /* orphan pin — harmless */
      }
      prof.pin += Date.now() - _t;
      rebuilt++;
      if (opts.onProgress && (rebuilt + unchanged) % 200 === 0) {
        opts.onProgress({ processed, skipped, failed, flat, rebuilt, unchanged, total: cat.count(), ms: Date.now() - t0 });
      }
    };
    await forEachModule(
      opts.root,
      async (m) => {
        const existing = cat.getSourceMeta(m.source);
        if (!existing || !opts.rebuild) {
          skipped++;
          return;
        }
        await sem.acquire(); // backpressure: at most nWorkers+2 modules in flight
        const p = pool
          .run(m.bytes, m.name) // structured-cloned into the worker (m.bytes is a shared pool Buffer)
          .then((r) => io.run(() => applyRebuild(m, existing.rootCid, r)))
          .catch((e) => fail(m.source, "bake/apply pipeline threw", e))
          .finally(() => sem.release());
        inflight.add(p);
        void p.finally(() => inflight.delete(p));
      },
      { formats: opts.formats, limit: opts.limit, sources: opts.sources },
    );
    await Promise.all(inflight);
    await pool.close();
  } else
    await forEachModule(
      opts.root,
      async (m) => {
        const existing = cat.getSourceMeta(m.source);
        // Default (incremental) mode: skip anything already cataloged.
        if (existing && !opts.rebuild) {
          skipped++;
        return;
      }
      const bytes = new Uint8Array(m.bytes);
      const ext = m.name.split(".").pop()?.toLowerCase() ?? "";

      // Single libopenmpt load -> metadata + decoded per-slot PCM (v2 input).
      let _t = Date.now();
      const mod = extractModule(bytes);
      const meta: ModuleMeta | null = mod ? mod.meta : null;
      const fmt = detectFormat(bytes);
      prof.decode += Date.now() - _t;

      let dag;
      let isFlat = false;
      _t = Date.now();
      // v3/v4 bake (v2 streaming + byte-exact reassembly from the SAME blocks) for parsed+decodable
      // formats. buildDagV3/V4 throw for modules with compressed samples (IT 0x08) or unparseable
      // slots -> fall to v1 buildDag (sample-separated, byte-exact, still reassemble-able; this is
      // where "compressed IT stays on v1" lands). mo3/unparseable -> v1 flat DAG. Every root is
      // reassemble-able: v3/v4 via reassembleV3/V4, v1/flat via reassemble. See REBUILD.md.
      //
      // v4 = v3 + FLAC-compressed sample leaves (~2x smaller, ~52% smaller first-playable). It is
      // NOT backward-readable (a v2/v3 client plays FLAC bytes as noise), so it is GATED behind
      // BAKE_V4=1: flip it ONLY once the v4-aware desktop client has shipped, then do the full
      // v4 REBUILD. Default stays v3 so an accidental ingest can't strand the deployed fleet.
      const bakeV4 = process.env.BAKE_V4 === "1";
      if (mod && fmt && fmt !== "mo3") {
        try {
          dag = bakeV4 ? await buildDagV4(bytes, mod.decoded) : await buildDagV3(bytes, mod.decoded);
        } catch {
          /* compressed sample(s) / unparseable slots -> v1 byte-exact below */
        }
      }
      if (!dag) {
        try {
          dag = await buildDag(bytes); // v1 sample-separated: byte-exact + cross-module sample dedup
        } catch {
          /* not parseable at all -> whole-file flat DAG */
        }
      }
      if (!dag) {
        try {
          dag = await buildFlatDag(bytes, ext);
          isFlat = true;
        } catch (e) {
          fail(m.source, "buildFlatDag threw (unbakeable)", e);
          return;
        }
      }
      prof.dag += Date.now() - _t;

      // Re-bake path: the module is already cataloged. Only do work when the DAG
      // root actually changed (e.g. seek tables added since the first ingest).
      if (existing) {
        if (dag.root.toString() === existing.rootCid) {
          unchanged++;
          return;
        }
        _t = Date.now();
        try {
          const { mismatched } = await loadDagToKubo(rpc, dag.blocks, dag.root);
          if (mismatched.length) {
            fail(m.source, `loadDagToKubo: ${mismatched.length} block(s) mismatched (rebuild)`);
            return;
          }
        } catch (e) {
          fail(m.source, "loadDagToKubo threw (rebuild)", e);
          return;
        }
        prof.kubo += Date.now() - _t;
        _t = Date.now();
        cat.updateRoot(m.source, dag.root.toString(), dag.blocks.length);
        prof.cat += Date.now() - _t;
        // Drop the superseded root's pin (shared leaves remain pinned under the
        // new root). Best-effort: an orphan pin is harmless, just disk.
        _t = Date.now();
        try {
          await rpc.pinRm(existing.rootCid, true);
        } catch {
          /* leave the old root pinned; verify-pinset will flag it as an orphan */
        }
        prof.pin += Date.now() - _t;
        rebuilt++;
        if (opts.onProgress && (rebuilt + unchanged + processed) % 200 === 0) {
          opts.onProgress({ processed, skipped, failed, flat, rebuilt, unchanged, total: cat.count(), ms: Date.now() - t0 });
        }
        return;
      }

      // New module: full ingest (metadata + catalog row).
      try {
        const { mismatched } = await loadDagToKubo(rpc, dag.blocks, dag.root);
        if (mismatched.length) {
          fail(m.source, `loadDagToKubo: ${mismatched.length} block(s) mismatched (new)`);
          return;
        }
      } catch (e) {
        fail(m.source, "loadDagToKubo threw (new)", e);
        return;
      }

      if (!meta) {
        fail(m.source, "no metadata (libopenmpt could not parse)");
        return;
      }

      const md5 = createHash("md5").update(bytes).digest("hex");
      cat.insert({
        source: m.source,
        filename: m.name,
        format: meta.type || ext,
        title: meta.title || m.name,
        duration: meta.duration,
        channels: meta.channels,
        numSamples: meta.numSamples,
        numInstruments: meta.numInstruments,
        numSubsongs: meta.numSubsongs,
        rootCid: dag.root.toString(),
        numBlocks: dag.blocks.length,
        sizeBytes: bytes.length,
        instruments: meta.instruments,
        comment: meta.comment,
        md5,
        genreId: genres?.byMd5.get(md5) ?? null,
      });
      processed++;
      if (isFlat) flat++;

      if (opts.onProgress && (processed + skipped) % 200 === 0) {
        opts.onProgress({ processed, skipped, failed, flat, rebuilt, unchanged, total: cat.count(), ms: Date.now() - t0 });
      }
    },
    { formats: opts.formats, limit: opts.limit, sources: opts.sources },
  );

  if (PROFILE) {
    const wall = Date.now() - t0;
    const n = rebuilt + processed || 1; // modules that did real work (unchanged skip write phases)
    const unzip = corpusStats.openMs + corpusStats.readMs;
    const other = wall - (prof.decode + prof.dag + prof.kubo + prof.cat + prof.pin + corpusStats.listMs + unzip);
    const s = (ms: number) => (ms / 1000).toFixed(1) + "s";
    const pc = (ms: number) => ((100 * ms) / wall).toFixed(0) + "%";
    const per = (ms: number) => (ms / n).toFixed(1) + "ms";
    // Extrapolate to the full corpus: one-time tree scan is FIXED; per-module phases scale by 170049/n.
    const full = (ms: number) => ((ms / n) * 170049) / 3.6e6; // hours
    console.log(
      `\n=== PROFILE (wall ${s(wall)} · ${n} did-work · ${unchanged} unchanged-skipped) ===\n` +
        `  phase                cum       %     per-module\n` +
        `  decode(libopenmpt)  ${s(prof.decode).padStart(6)}  ${pc(prof.decode).padStart(4)}  ${per(prof.decode)}\n` +
        `  FLAC encode         ${s(flacStats.ms).padStart(6)}  ${pc(flacStats.ms).padStart(4)}  ${per(flacStats.ms)}  (${flacStats.calls} calls)\n` +
        `  dag-other(cdc/cbor) ${s(prof.dag - flacStats.ms).padStart(6)}  ${pc(prof.dag - flacStats.ms).padStart(4)}  ${per(prof.dag - flacStats.ms)}\n` +
        `  block/put (node)    ${s(prof.kubo).padStart(6)}  ${pc(prof.kubo).padStart(4)}  ${per(prof.kubo)}\n` +
        `  pinRm (node)        ${s(prof.pin).padStart(6)}  ${pc(prof.pin).padStart(4)}  ${per(prof.pin)}\n` +
        `  catalog updateRoot  ${s(prof.cat).padStart(6)}  ${pc(prof.cat).padStart(4)}  ${per(prof.cat)}\n` +
        `  unzip (open+read)   ${s(unzip).padStart(6)}  ${pc(unzip).padStart(4)}  ${per(unzip)}\n` +
        `  tree-scan ONE-TIME  ${s(corpusStats.listMs).padStart(6)}  ${pc(corpusStats.listMs).padStart(4)}  (fixed, not per-module)\n` +
        `  other/overhead      ${s(other).padStart(6)}  ${pc(other).padStart(4)}\n` +
        `  --> full-170k extrapolation: FLAC ${full(flacStats.ms).toFixed(1)}h · unzip ${full(unzip).toFixed(1)}h · block/put ${full(prof.kubo).toFixed(1)}h · decode ${full(prof.decode).toFixed(1)}h · pin ${full(prof.pin).toFixed(1)}h · TOTAL/module ${per(wall - corpusStats.listMs)} => ~${(((wall - corpusStats.listMs) / n * 170049) / 3.6e6).toFixed(1)}h + scan\n`,
    );
  }

  // Refresh precomputed aggregates and fold the WAL into the main file so the
  // on-disk DB is a consistent, page-aligned snapshot before we publish it.
  cat.refreshMeta();
  cat.checkpoint();
  const stats: IngestStats = { processed, skipped, failed, flat, rebuilt, unchanged, total: cat.count(), ms: Date.now() - t0 };
  cat.close();

  if (opts.publish !== false) {
    try {
      await publishCatalog(rpc, opts);
    } catch (e) {
      // The module DAGs are already pinned; only the IPNS announce failed. Clients
      // keep resolving the previous record until the next rebake re-announces.
      console.error(`catalog publish failed (DAG pinned; IPNS not re-announced): ${e}`);
    }
  }
  return stats;
}

/** One-shot md5 backfill (see IngestOpts.backfillMd5). Walks the corpus, hashes
 *  each module file, and fills the md5 of its already-cataloged row. No DAG build,
 *  no libopenmpt, no kubo block-put — just unzip + md5 — so it's I/O-bound and
 *  orders of magnitude cheaper than a full re-bake. Re-runnable: rows already
 *  carrying an md5 are skipped, so an interrupted pass resumes cleanly. */
async function backfillMd5(cat: Catalog, rpc: KuboRpc, opts: IngestOpts): Promise<IngestStats> {
  const t0 = Date.now();
  let processed = 0,
    skipped = 0;
  await forEachModule(
    opts.root,
    async (m) => {
      // Only hash sources that are cataloged *and* still missing an md5.
      if (!cat.md5Missing(m.source)) {
        skipped++;
        return;
      }
      cat.setMd5(m.source, createHash("md5").update(m.bytes).digest("hex"));
      processed++;
      if (opts.onProgress && (processed + skipped) % 1000 === 0) {
        opts.onProgress({ processed, skipped, failed: 0, flat: 0, rebuilt: 0, unchanged: 0, total: cat.count(), ms: Date.now() - t0 });
      }
    },
    { formats: opts.formats, limit: opts.limit },
  );
  cat.checkpoint(); // fold the WAL in before the snapshot
  const stats: IngestStats = { processed, skipped, failed: 0, flat: 0, rebuilt: 0, unchanged: 0, total: cat.count(), ms: Date.now() - t0 };
  cat.close();
  if (opts.publish !== false) {
    try {
      await publishCatalog(rpc, opts);
    } catch (e) {
      console.error(`catalog publish failed (md5 backfilled locally; IPNS not re-announced): ${e}`);
    }
  }
  return stats;
}

/** One-shot FTS reindex (see IngestOpts.reindexFts). Rebuilds `modules_fts` in place from
 *  the `modules` table — applying an FTS schema change such as `prefix='2 3'` that
 *  `CREATE ... IF NOT EXISTS` can't — drops the unused fts5vocab table, then republishes.
 *  No corpus walk, no DAG build, no libopenmpt: the module blocks, pins and root_cids are
 *  untouched; only the catalog's search index changes. Seconds, not the hours a re-bake takes. */
async function reindexFtsMode(cat: Catalog, rpc: KuboRpc, opts: IngestOpts): Promise<IngestStats> {
  const t0 = Date.now();
  const total = cat.reindexFts();
  cat.checkpoint(); // fold the WAL in before the snapshot
  const stats: IngestStats = { processed: total, skipped: 0, failed: 0, flat: 0, rebuilt: 0, unchanged: 0, total, ms: Date.now() - t0 };
  cat.close();
  if (opts.publish !== false) {
    try {
      await publishCatalog(rpc, opts);
    } catch (e) {
      console.error(`catalog publish failed (FTS reindexed locally; IPNS not re-announced): ${e}`);
    }
  }
  return stats;
}

/** Decode the handful of XML/HTML entities the TMA genre labels carry un-decoded
 *  ("Drum &amp; Bass" -> "Drum & Bass"). Applied once at load, so meta.genre_counts and the
 *  genres table hold clean display text. */
function unescapeHtml(s: string): string {
  return s
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#0*39;/g, "'");
}

/** Load the md5->genre JSON (tma-genre-extract.py output) into {byMd5, labels}. Returns
 *  null if the file is absent, so a normal ingest run without the dumps just skips genre
 *  enrichment instead of failing. genreid 0 is the "ungenred" sentinel and is dropped. */
function loadGenreMap(path?: string): { byMd5: Map<string, number>; labels: Map<number, string> } | null {
  const p = path ?? DEFAULT_GENRE_MAP;
  let raw: string;
  try {
    raw = readFileSync(p, "utf8");
  } catch {
    return null;
  }
  const obj = JSON.parse(raw) as Record<string, { genreid: number; genre: string }>;
  const byMd5 = new Map<string, number>();
  const labels = new Map<number, string>();
  for (const [md5, rec] of Object.entries(obj)) {
    if (!rec.genreid) continue;
    byMd5.set(md5.toLowerCase(), rec.genreid);
    if (!labels.has(rec.genreid)) labels.set(rec.genreid, unescapeHtml(rec.genre));
  }
  return { byMd5, labels };
}

/** One-shot genre backfill (see IngestOpts.backfillGenre). Stamps genreid onto existing rows
 *  by md5-joining the genre map — pure SQL, no corpus walk / DAG build / kubo put (md5 is
 *  already on every row), so it's even cheaper than the md5 backfill. Refreshes meta.genre_counts
 *  (the client's home directory) and republishes. Re-runnable; needs md5 already backfilled. */
async function backfillGenre(cat: Catalog, rpc: KuboRpc, opts: IngestOpts): Promise<IngestStats> {
  const t0 = Date.now();
  const genres = loadGenreMap(opts.genreMap);
  if (!genres) {
    cat.close();
    throw new Error(
      `BACKFILL_GENRE: genre map not found at ${opts.genreMap ?? DEFAULT_GENRE_MAP} ` +
        `(run scripts/tma-genre-extract.py, or set GENRE_MAP)`,
    );
  }
  const matched = cat.applyGenres(genres.byMd5, genres.labels);
  cat.refreshMeta(); // fold genre_counts (+ total/format) into meta for the client directory
  cat.checkpoint(); // fold the WAL in before the snapshot
  const total = cat.count();
  console.log(`  genre backfill: ${matched} rows stamped across ${genres.labels.size} genres`);
  const stats: IngestStats = { processed: matched, skipped: 0, failed: 0, flat: 0, rebuilt: 0, unchanged: 0, total, ms: Date.now() - t0 };
  cat.close();
  if (opts.publish !== false) {
    try {
      await publishCatalog(rpc, opts);
    } catch (e) {
      console.error(`catalog publish failed (genre backfilled locally; IPNS not re-announced): ${e}`);
    }
  }
  return stats;
}

/** Publish the freshly-ingested catalog DB to IPFS under the master-signed IPNS
 *  key (R1). The node's name/publish signs the record and distributes it itself —
 *  DHT PutValue + gossipsub push — so clients resolve it over libp2p with no HTTP
 *  hop. Snapshots the DB to a sibling file first — never adds the live path. */
async function publishCatalog(rpc: KuboRpc, opts: IngestOpts): Promise<void> {
  const snapshot = `${opts.dbPath}.snapshot`;
  // Atomic, race-free snapshot: VACUUM INTO reads the live DB in a single read
  // transaction (consistent even if another writer is mid-checkpoint) and writes a fresh
  // file in the default rollback-journal (DELETE) mode — so a client can open it READ-ONLY
  // over the Bitswap VFS without a sidecar -wal file (a WAL header would make SQLite demand
  // the -wal the single published file doesn't carry). Replaces a plain copyFileSync (which
  // could capture a torn page mid-checkpoint) plus a separate WAL->DELETE conversion.
  // VACUUM INTO requires the target not to pre-exist.
  try {
    unlinkSync(snapshot);
  } catch {
    /* no stale snapshot -> fine */
  }
  const src = new DatabaseSync(opts.dbPath);
  src.exec(`VACUUM INTO '${snapshot.replace(/'/g, "''")}'`);
  src.close();
  try {
    if (/^(1|true|yes)$/i.test(process.env.ZSTD_CATALOG ?? "")) {
      // HARD CUT: the per-page-zstd (TSZCAT) manifest IS the published catalog, under the main
      // `catalog` key — ~2.2x fewer page bytes over the Bitswap VFS. The client VFS auto-detects
      // raw-SQLite vs TSZCAT by the root magic, so no key/config change is needed to read it.
      await publishZstdCatalog(rpc, snapshot, CATALOG_KEY_NAME, opts.kuboApi);
    } else {
      const cid = await rpc.addFile(snapshot, {
        chunker: CATALOG_CHUNKER,
        rawLeaves: false, // dag-pb leaves — rust-unixfs can't walk raw leaves (see above)
        cidVersion: 1,
        pin: true,
      });
      const peerId = await rpc.keyGen(CATALOG_KEY_NAME); // idempotent; base58 PeerId
      await rpc.namePublish(cid, { key: CATALOG_KEY_NAME, lifetime: CATALOG_LIFETIME });
      // namePublish already signed + stored + distributed the record (DHT PutValue +
      // gossipsub, allow-offline) — the publish is done HERE. The routingGet below is only a
      // read-back verify, so a transient RPC flake there must NOT be reported as a publish
      // failure (it previously threw `fetch failed` after the record had already landed).
      console.log(`catalog published: cid=${cid} ipns=${peerId}`);
      try {
        const record = await rpc.routingGet(peerId);
        if (!record) console.error(`  !! publish verify: routingGet(${peerId}) returned no record (may still be propagating)`);
      } catch (e) {
        console.error(`  publish verify skipped (transient, record already signed+stored): ${e}`);
      }
      if (!CATALOG_IPNS_KEY) {
        console.log(`  -> set CATALOG_IPNS_KEY="${peerId}" in packages/config and ship a client build`);
      } else if (CATALOG_IPNS_KEY !== peerId) {
        console.error(`  !! config CATALOG_IPNS_KEY (${CATALOG_IPNS_KEY}) != master key (${peerId}); clients will resolve the wrong name`);
      }
    }
  } finally {
    try {
      unlinkSync(snapshot);
    } catch {
      /* snapshot already gone -> fine */
    }
  }
}

const TSZCAT_PAGE = 16384; // must equal the SQLite page_size (each page = one zstd block)
const TSZCAT_LEVEL = 19; // one-time offline compression; client decode is fast at any level

/** Build + publish the TSZCAT per-page-zstd catalog from a consistent snapshot, under `keyName`.
 *  Each 16 KB SQLite page is zstd-compressed into its OWN raw block (batched `block/put-many`); a
 *  binary manifest ([magic][page_size][page_count][page_count × 36-byte CIDv1]) lists them and is
 *  the published root. The client fetches the manifest once then `block/get`s + decompresses each
 *  page it reads — so lazy paging is preserved while the wire carries ~2.2x fewer page bytes. Page
 *  blocks are pinned (in parallel batches) so repo GC can't drop them. */
async function publishZstdCatalog(rpc: KuboRpc, snapshot: string, keyName: string, kuboApi?: string): Promise<void> {
  const raw = readFileSync(snapshot);
  if (raw.length % TSZCAT_PAGE !== 0) throw new Error(`snapshot not page-aligned: ${raw.length}`);
  const n = raw.length / TSZCAT_PAGE;
  const cids = [];
  const BATCH = 512;
  for (let i = 0; i < n; i += BATCH) {
    const entries = [];
    for (let p = i; p < Math.min(i + BATCH, n); p++) {
      const comp = zstdCompressSync(raw.subarray(p * TSZCAT_PAGE, (p + 1) * TSZCAT_PAGE), {
        params: { [zlibConstants.ZSTD_c_compressionLevel]: TSZCAT_LEVEL },
      });
      entries.push({ bytes: comp, codec: 0x55 }); // raw block
    }
    cids.push(...(await rpc.blockPutMany(entries)));
  }
  if (cids.length !== n) throw new Error(`page cid count ${cids.length} != ${n}`);
  const man = Buffer.alloc(16 + 36 * n);
  Buffer.from("TSZCAT1\n", "latin1").copy(man, 0);
  man.writeUInt32LE(TSZCAT_PAGE, 8);
  man.writeUInt32LE(n, 12);
  cids.forEach((cid, i) => {
    const b = cid.bytes;
    if (b.length !== 36) throw new Error(`page cid ${i} is ${b.length}B, expected 36 (CIDv1 raw sha2-256)`);
    Buffer.from(b).copy(man, 16 + i * 36);
  });
  const manPath = `${snapshot}.tszcat`;
  writeFileSync(manPath, man);
  try {
    const manCid = await rpc.addFile(manPath, {
      chunker: CATALOG_CHUNKER, rawLeaves: false, cidVersion: 1, pin: true,
    });
    // Pin the page blocks (they're referenced by the manifest BYTES, not as DAG links, so the
    // manifest's recursive pin doesn't cover them) — in parallel batches to bound RPC round-trips.
    for (let i = 0; i < cids.length; i += 64) {
      await Promise.all(cids.slice(i, i + 64).map((c) => rpc.pinAdd(c, false)));
    }
    const peerId = await rpc.keyGen(keyName);
    await rpc.namePublish(manCid, { key: keyName, lifetime: CATALOG_LIFETIME });
    console.log(
      `catalog(zstd) published: cid=${manCid} ipns=${peerId} ` +
        `(${n} pages, manifest ${(man.length / 1e6).toFixed(2)} MB)`,
    );
    if (keyName === CATALOG_KEY_NAME && CATALOG_IPNS_KEY && CATALOG_IPNS_KEY !== peerId) {
      console.error(`  !! config CATALOG_IPNS_KEY (${CATALOG_IPNS_KEY}) != master key (${peerId})`);
    }
    // Regenerate the web client's warm bundle for THIS root and drop it in the Caddy web root, so a
    // fresh browser's first search pre-loads the hot FTS pages over HTTP (apps/web warmbundle.ts).
    // Best-effort: a failure (no web root, wa-sqlite hiccup) must never fail a publish, and a stale
    // bundle is a safe client-side no-op (the loader checks the root). Only for the live catalog key.
    if (keyName === CATALOG_KEY_NAME && kuboApi) {
      const dir = process.env.WARM_BUNDLE_DIR ?? "/var/www/trackerstream";
      try {
        const { generateWarmBundle } = await import("./gen-warm-bundle.ts");
        const pages = await generateWarmBundle(kuboApi, manCid.toString(), dir);
        console.log(`catalog warm bundle: ${pages} pages -> ${dir}/catalog-warm.{bin,json}`);
      } catch (e) {
        console.error(`  !! warm bundle generation skipped: ${e instanceof Error ? e.message : e}`);
      }
    }
  } finally {
    try {
      unlinkSync(manPath);
    } catch {
      /* already gone -> fine */
    }
  }
}
