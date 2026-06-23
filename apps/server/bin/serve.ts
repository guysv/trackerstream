// Start the catalog/search HTTP API.
//   CATALOG_DB=/srv/trackerstream/catalog/catalog.db  node --experimental-sqlite bin/serve.ts
import { API_PORT } from "@trackerstream/config";
import { Catalog } from "../src/catalog.ts";
import { Social } from "../src/social.ts";
import { createApi } from "../src/api.ts";

const dbPath = process.env.CATALOG_DB ?? "./catalog.db";
const port = +(process.env.API_PORT ?? API_PORT);

const catalog = new Catalog(dbPath);
const social = new Social(dbPath);
const server = createApi(catalog, social);
server.listen(port, () => console.log(`trackerstream API on :${port}  (${catalog.count()} modules)`));

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, () => {
    server.close();
    catalog.close();
    social.close();
    process.exit(0);
  });
}
