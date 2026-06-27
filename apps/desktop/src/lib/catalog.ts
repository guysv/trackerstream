// Catalog/search client. The catalog SQLite DB is published on IPFS under the
// master's signed IPNS record (R1); the embedded node lazily queries it over a
// Bitswap-backed SQLite VFS (only the pages a query touches are fetched), so the
// catalog needs no HTTP control plane. Module *bytes* never come from here either;
// results carry a root CID the data plane resolves P2P.
import { invoke } from "@tauri-apps/api/core";
import { CATALOG_IPNS_KEY } from "@trackerstream/config";

export interface ModuleHit {
  id: number;
  filename: string;
  format: string;
  title: string;
  duration: number;
  channels: number;
  rootCid: string;
}

export interface ModuleDetail extends ModuleHit {
  numSamples: number;
  numInstruments: number;
  numSubsongs: number;
  sizeBytes: number;
  instruments: string;
  comment: string;
}

export interface FormatCount {
  format: string;
  count: number;
}

// One Tauri command answers every catalog query: it resolves the catalog IPNS name
// to the current DB CID (local cache -> tracker -> peer-pull) and runs the query over
// the Bitswap VFS, returning the same JSON shapes the old HTTP /catalog API did.
function query<T>(req: Record<string, unknown>): Promise<T> {
  return invoke<T>("catalog_query", { name: CATALOG_IPNS_KEY, req });
}

export const search = (q: string, limit = 60): Promise<ModuleHit[]> =>
  query<{ results: ModuleHit[] }>({ op: "search", q, limit }).then((r) => r.results);

export const listModules = (opts: {
  format?: string;
  sort?: "latest" | "random" | "title";
  limit?: number;
  offset?: number;
}): Promise<ModuleHit[]> =>
  query<{ results: ModuleHit[] }>({
    op: "list",
    format: opts.format,
    sort: opts.sort ?? "latest",
    limit: opts.limit ?? 100,
    offset: opts.offset ?? 0,
  }).then((r) => r.results);

export const getModule = (id: number): Promise<ModuleDetail> =>
  query<ModuleDetail>({ op: "get", id });

export const getFormats = (): Promise<{ formats: FormatCount[]; total: number }> =>
  query({ op: "formats" });
