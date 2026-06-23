// Ingest pipeline: corpus module -> CID-DAG (sample-level dedup, or flat for
// formats the parsers don't cover) -> block-put + recursive pin on the master
// kubo node (shared chunks stored once) -> libopenmpt metadata -> SQLite/FTS5
// catalog row carrying the root CID. Incremental + re-runnable (skips by source).
import { buildDag, buildFlatDag, KuboRpc, loadDagToKubo } from "@trackerstream/repack";
import { Catalog } from "./catalog.ts";
import { initMeta, extractMeta } from "./meta.ts";
import { forEachModule } from "./corpus.ts";

export interface IngestOpts {
  root: string;
  dbPath: string;
  kuboApi: string;
  formats?: string[];
  limit?: number;
  onProgress?: (s: IngestStats) => void;
}

export interface IngestStats {
  processed: number;
  skipped: number;
  failed: number;
  flat: number;
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
    flat = 0;

  await forEachModule(
    opts.root,
    async (m) => {
      if (cat.has(m.source)) {
        skipped++;
        return;
      }
      const bytes = new Uint8Array(m.bytes);
      const ext = m.name.split(".").pop()?.toLowerCase() ?? "";

      let dag;
      let isFlat = false;
      try {
        dag = await buildDag(bytes);
      } catch {
        try {
          dag = await buildFlatDag(bytes, ext);
          isFlat = true;
        } catch {
          failed++;
          return;
        }
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

      const meta = extractMeta(bytes);
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
        opts.onProgress({ processed, skipped, failed, flat, total: cat.count(), ms: Date.now() - t0 });
      }
    },
    { formats: opts.formats, limit: opts.limit },
  );

  const stats: IngestStats = { processed, skipped, failed, flat, total: cat.count(), ms: Date.now() - t0 };
  cat.close();
  return stats;
}
