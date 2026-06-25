// Ingest pipeline: corpus module -> CID-DAG (sample-level dedup, or flat for
// formats the parsers don't cover) -> block-put + recursive pin on the master
// kubo node (shared chunks stored once) -> libopenmpt metadata -> SQLite/FTS5
// catalog row carrying the root CID. Incremental + re-runnable (skips by source).
import { buildDagV2, buildFlatDag, detectFormat, KuboRpc, loadDagToKubo } from "@trackerstream/repack";
import { Catalog } from "./catalog.ts";
import { initMeta, extractModule, type ModuleMeta } from "./meta.ts";
import { forEachModule } from "./corpus.ts";

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
      });
      processed++;
      if (isFlat) flat++;

      if (opts.onProgress && (processed + skipped) % 200 === 0) {
        opts.onProgress({ processed, skipped, failed, flat, rebuilt, unchanged, total: cat.count(), ms: Date.now() - t0 });
      }
    },
    { formats: opts.formats, limit: opts.limit },
  );

  const stats: IngestStats = { processed, skipped, failed, flat, rebuilt, unchanged, total: cat.count(), ms: Date.now() - t0 };
  cat.close();
  return stats;
}
