// Ingest the corpus into the catalog + master pin store. Re-runnable.
//   CORPUS=~/tmp/modarchive KUBO_API=http://127.0.0.1:5001 \
//   CATALOG_DB=/srv/trackerstream/catalog/catalog.db  node --experimental-sqlite bin/ingest.ts
//   LIMIT=500 FORMATS=mod,it,s3m,xm  ... (for a quick slice)
//   REBUILD=1 ... (re-bake cataloged modules whose DAG root changed, e.g. to add
//                  seek tables; repoints the catalog + re-pins, unpins old roots)
import { homedir } from "node:os";
import { join } from "node:path";
import { runIngest } from "../src/ingest.ts";

const root = process.env.CORPUS ?? join(homedir(), "tmp/modarchive");
const dbPath = process.env.CATALOG_DB ?? "./catalog.db";
const kuboApi = process.env.KUBO_API ?? "http://127.0.0.1:5001";
const limit = +(process.env.LIMIT ?? 0);
const formats = process.env.FORMATS?.split(",");
const rebuild = /^(1|true|yes)$/i.test(process.env.REBUILD ?? "");

console.log(
  `ingest: corpus=${root} db=${dbPath} kubo=${kuboApi} limit=${limit || "∞"}${rebuild ? " REBUILD" : ""}`,
);
const stats = await runIngest({
  root,
  dbPath,
  kuboApi,
  limit,
  formats,
  rebuild,
  onProgress: (s) =>
    console.log(
      `  ${s.processed} ingested (${s.flat} flat) · ${s.rebuilt} rebuilt · ${s.unchanged} unchanged · ${s.skipped} skipped · ${s.failed} failed · ${(s.ms / 1000).toFixed(0)}s · ${s.total} in catalog`,
    ),
});
console.log(`DONE: ${JSON.stringify(stats)}`);
