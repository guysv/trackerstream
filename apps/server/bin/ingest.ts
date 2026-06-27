// Ingest the corpus into the catalog + master pin store. Re-runnable.
//   CORPUS=~/tmp/modarchive KUBO_API=http://127.0.0.1:5001 \
//   CATALOG_DB=/srv/trackerstream/catalog/catalog.db  node --experimental-sqlite bin/ingest.ts
//   LIMIT=500 FORMATS=mod,it,s3m,xm  ... (for a quick slice)
//   REBUILD=1 ... (re-bake cataloged modules whose DAG root changed, e.g. to add
//                  seek tables; repoints the catalog + re-pins, unpins old roots)
import { homedir } from "node:os";
import { join } from "node:path";
import { API_PORT } from "@trackerstream/config";
import { runIngest } from "../src/ingest.ts";

const root = process.env.CORPUS ?? join(homedir(), "tmp/modarchive");
const dbPath = process.env.CATALOG_DB ?? "./catalog.db";
const kuboApi = process.env.KUBO_API ?? "http://127.0.0.1:5001";
const limit = +(process.env.LIMIT ?? 0);
const formats = process.env.FORMATS?.split(",");
const rebuild = /^(1|true|yes)$/i.test(process.env.REBUILD ?? "");
// Publish the catalog to IPNS at the end (R1). On by default; PUBLISH=0 for dev slices.
const publish = !/^(0|false|no)$/i.test(process.env.PUBLISH ?? "");
const apiPublishUrl = process.env.API_PUBLISH_URL ?? `http://127.0.0.1:${API_PORT}`;
const ipnsToken = process.env.TS_IPNS_TOKEN;

console.log(
  `ingest: corpus=${root} db=${dbPath} kubo=${kuboApi} limit=${limit || "∞"}${rebuild ? " REBUILD" : ""}${publish ? "" : " NO-PUBLISH"}`,
);
const stats = await runIngest({
  root,
  dbPath,
  kuboApi,
  limit,
  formats,
  rebuild,
  publish,
  apiPublishUrl,
  ipnsToken,
  onProgress: (s) =>
    console.log(
      `  ${s.processed} ingested (${s.flat} flat) · ${s.rebuilt} rebuilt · ${s.unchanged} unchanged · ${s.skipped} skipped · ${s.failed} failed · ${(s.ms / 1000).toFixed(0)}s · ${s.total} in catalog`,
    ),
});
console.log(`DONE: ${JSON.stringify(stats)}`);
