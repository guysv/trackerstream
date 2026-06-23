// Ingest the corpus into the catalog + master pin store. Re-runnable.
//   CORPUS=~/tmp/modarchive KUBO_API=http://127.0.0.1:5001 \
//   CATALOG_DB=/srv/trackerstream/catalog/catalog.db  node --experimental-sqlite bin/ingest.ts
//   LIMIT=500 FORMATS=mod,it,s3m,xm  ... (for a quick slice)
import { homedir } from "node:os";
import { join } from "node:path";
import { runIngest } from "../src/ingest.ts";

const root = process.env.CORPUS ?? join(homedir(), "tmp/modarchive");
const dbPath = process.env.CATALOG_DB ?? "./catalog.db";
const kuboApi = process.env.KUBO_API ?? "http://127.0.0.1:5001";
const limit = +(process.env.LIMIT ?? 0);
const formats = process.env.FORMATS?.split(",");

console.log(`ingest: corpus=${root} db=${dbPath} kubo=${kuboApi} limit=${limit || "∞"}`);
const stats = await runIngest({
  root,
  dbPath,
  kuboApi,
  limit,
  formats,
  onProgress: (s) =>
    console.log(
      `  ${s.processed} ingested (${s.flat} flat) · ${s.skipped} skipped · ${s.failed} failed · ${(s.ms / 1000).toFixed(0)}s · ${s.total} in catalog`,
    ),
});
console.log(`DONE: ${JSON.stringify(stats)}`);
