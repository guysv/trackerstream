// Ingest pipeline: corpus module -> CID-DAG (sample-level dedup, or flat for
// formats the parsers don't cover) -> block-put + recursive pin on the master
// kubo node (shared chunks stored once) -> libopenmpt metadata -> SQLite/FTS5
// catalog row carrying the root CID. Incremental + re-runnable (skips by source).
import { createHash } from "node:crypto";
import { unlinkSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { buildDagV2, buildFlatDag, detectFormat, KuboRpc, loadDagToKubo } from "@trackerstream/repack";
import { CATALOG_IPNS_KEY } from "@trackerstream/config";
import { Catalog } from "./catalog.ts";
import { initMeta, extractModule, type ModuleMeta } from "./meta.ts";
import { forEachModule } from "./corpus.ts";

// Catalog publish layout (R1): page-aligned 16 KB chunks so each SQLite page maps to
// one stable UnixFS block (deterministic chunking -> high cross-rebake block reuse).
// NOT raw-leaves: the client's vendored rust-unixfs visitor can't walk raw (codec
// 0x55) leaves ("walk failed"), so leaves stay dag-pb. kubo enables raw-leaves by
// default at cid-version=1, hence the explicit rawLeaves:false in the addFile call.
const CATALOG_CHUNKER = "size-16384";
const CATALOG_KEY_NAME = "catalog"; // kubo keystore name for the catalog signing key
const CATALOG_LIFETIME = "48h"; // IPNS record validity window the client enforces as EOL

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
  /** Publish the catalog DB to IPFS under the master-signed IPNS key at the end of
   *  ingest (R1). Default true; set false for dev slices. */
  publish?: boolean;
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

  const t0 = Date.now();
  let processed = 0,
    skipped = 0,
    failed = 0,
    flat = 0,
    rebuilt = 0,
    unchanged = 0;

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
      const mod = extractModule(bytes);
      const meta: ModuleMeta | null = mod ? mod.meta : null;
      const fmt = detectFormat(bytes);

      let dag;
      let isFlat = false;
      // v2 bake for parsed+decodable formats; mo3/unparseable -> v1 flat DAG.
      if (mod && fmt && fmt !== "mo3") {
        try {
          dag = await buildDagV2(bytes, mod.decoded);
        } catch {
          /* fall through to flat */
        }
      }
      if (!dag) {
        try {
          dag = await buildFlatDag(bytes, ext);
          isFlat = true;
        } catch {
          failed++;
          return;
        }
      }

      // Re-bake path: the module is already cataloged. Only do work when the DAG
      // root actually changed (e.g. seek tables added since the first ingest).
      if (existing) {
        if (dag.root.toString() === existing.rootCid) {
          unchanged++;
          return;
        }
        try {
          const { mismatched } = await loadDagToKubo(rpc, dag.blocks, dag.root);
          if (mismatched.length) {
            failed++;
            return;
          }
        } catch {
          failed++;
          return;
        }
        cat.updateRoot(m.source, dag.root.toString(), dag.blocks.length);
        // Drop the superseded root's pin (shared leaves remain pinned under the
        // new root). Best-effort: an orphan pin is harmless, just disk.
        try {
          await rpc.pinRm(existing.rootCid, true);
        } catch {
          /* leave the old root pinned; verify-pinset will flag it as an orphan */
        }
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
          failed++;
          return;
        }
      } catch {
        failed++;
        return;
      }

      if (!meta) {
        failed++;
        return;
      }

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
        md5: createHash("md5").update(bytes).digest("hex"),
      });
      processed++;
      if (isFlat) flat++;

      if (opts.onProgress && (processed + skipped) % 200 === 0) {
        opts.onProgress({ processed, skipped, failed, flat, rebuilt, unchanged, total: cat.count(), ms: Date.now() - t0 });
      }
    },
    { formats: opts.formats, limit: opts.limit },
  );

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
  } finally {
    try {
      unlinkSync(snapshot);
    } catch {
      /* snapshot already gone -> fine */
    }
  }
}
