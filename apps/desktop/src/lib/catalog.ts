// Catalog/search client. The catalog SQLite DB is published on IPFS under the
// master's signed IPNS record (R1); the embedded node lazily queries it over a
// Bitswap-backed SQLite VFS (only the pages a query touches are fetched), so the
// catalog needs no HTTP control plane. Module *bytes* never come from here either;
// results carry a root CID the data plane resolves P2P.
import { invoke, Channel } from "@tauri-apps/api/core";
import { CATALOG_IPNS_KEY, CATALOG_Z_IPNS_KEY } from "@trackerstream/config";

// Prefer the per-page-zstd catalog (TSZCAT) when the master publishes one, else the raw SQLite.
// This build's VFS auto-detects the format from the resolved root, so a single `name` works for
// both — the choice is purely which IPNS record we resolve. Empty CATALOG_Z_IPNS_KEY -> raw.
const CATALOG_NAME = CATALOG_Z_IPNS_KEY || CATALOG_IPNS_KEY;

export interface ModuleHit {
  id: number;
  /** Content md5 (lowercase hex) — the stable, cross-rebake key. The rowid `id` is
   *  reassigned on every full re-ingest, so persist md5 (playlists, likes), never id. */
  md5: string;
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
  return invoke<T>("catalog_query", { name: CATALOG_NAME, req });
}

// `after` is a keyset cursor: the id of the last hit already shown. Pass it to fetch the
// next page (matches with a higher rowid); omit for the first page.
export const search = (q: string, limit = 60, after?: number): Promise<ModuleHit[]> =>
  query<{ results: ModuleHit[] }>({ op: "search", q, limit, after }).then((r) => r.results);

// Streaming search: `onRow` fires for each hit the instant its pages arrive over the VFS, so the
// UI paints results progressively instead of after the whole page. Resolves to the total row
// count when the stream completes. Same keyset `after` cursor as `search`.
export function searchStream(
  q: string,
  limit: number,
  after: number | undefined,
  onRow: (h: ModuleHit) => void,
): Promise<number> {
  const ch = new Channel<ModuleHit>();
  ch.onmessage = onRow;
  return invoke<number>("catalog_search_stream", { name: CATALOG_NAME, q, limit, after, onRow: ch });
}

// Abort any in-flight catalog query (the last search): its VFS page reads stop and the
// in-flight Bitswap cats are dropped so tsnode stops fetching pages we no longer want.
export const cancelSearch = (): Promise<void> => invoke("catalog_cancel");

// Prewarm the catalog page cache (schema + FTS upper tree) so the first keystroke's search
// descends from warm pages instead of paying the cold schema/FTS-root fetches. Fire-and-forget
// on search-page mount; best-effort (a failure just means the first search pays the cold cost).
export const warmCatalog = (): Promise<void> => invoke("catalog_warm", { name: CATALOG_NAME });

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

// Resolve a module by its stable content md5 — how playlists (which persist md5, not the
// rebake-unstable rowid) turn a stored track into a playable CID at play time.
export const getModuleByMd5 = (md5: string): Promise<ModuleDetail> =>
  query<ModuleDetail>({ op: "get_by_md5", md5 });

export const getFormats = (): Promise<{ formats: FormatCount[]; total: number }> =>
  query({ op: "formats" });
