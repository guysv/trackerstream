// Start the catalog/search HTTP API.
//   CATALOG_DB=/srv/trackerstream/catalog/catalog.db  node --experimental-sqlite bin/serve.ts
//
// Binds 127.0.0.1 by default: in production the API sits BEHIND Caddy (TLS
// termination on 443 -> reverse_proxy 127.0.0.1:8080, see deploy/setup-tls.sh),
// and ufw denies :8080 externally. Set API_HOST=0.0.0.0 for direct/local access
// (e.g. dev, or a pre-TLS deployment reached on http://<ip>:8080).
import { dirname, join } from "node:path";
import { API_PORT } from "@trackerstream/config";
import { Catalog } from "../src/catalog.ts";
import { Tracker } from "../src/tracker.ts";
import { IpnsStore } from "../src/ipns.ts";
import { createApi } from "../src/api.ts";

const dbPath = process.env.CATALOG_DB ?? "./catalog.db";
// Persist published IPNS records beside the catalog DB so they survive a restart.
const ipnsPath = process.env.IPNS_STORE ?? join(dirname(dbPath), "ipns.json");
const port = +(process.env.API_PORT ?? API_PORT);
const host = process.env.API_HOST ?? "127.0.0.1";

// Peer-assist presence TTL: a client re-announces every ~30s, so 90s tolerates a
// couple missed heartbeats before we drop it from the roster.
const PRESENCE_TTL_MS = 90_000;

const catalog = new Catalog(dbPath);
const tracker = new Tracker();
const ipns = new IpnsStore(ipnsPath);
const server = createApi(catalog, tracker, ipns);
const sweep = setInterval(() => tracker.sweep(PRESENCE_TTL_MS), 30_000);
server.listen(port, host, () =>
  console.log(`trackerstream API on ${host}:${port}  (${catalog.count()} modules)`),
);

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, () => {
    clearInterval(sweep);
    server.close();
    catalog.close();
    process.exit(0);
  });
}
