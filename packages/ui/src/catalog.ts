// Catalog/search facade.
//
// The catalog SQLite DB is published on IPFS under the master's signed IPNS record; the node lazily
// queries it over a Bitswap-backed SQLite VFS (only the pages a query touches are fetched), so the
// catalog needs no HTTP control plane. Module *bytes* never come from here either; results carry a
// root CID the data plane resolves P2P.
//
// This file owns the request shapes and the md5->CID cache warming. The NodeClient underneath owns
// only *how* the query is executed (Tauri invoke into a Rust VFS, or wa-sqlite over Bitswap in a
// worker) — so this logic is identical on desktop and web, by construction.
import { client } from "./client/index.ts";
import { rememberHit, rememberHits } from "./cidCache.ts";
import type { FormatCount, GenreCount, ModuleDetail, ModuleHit } from "./client/types.ts";

export type { FormatCount, GenreCount, ModuleDetail, ModuleHit };

const query = <T>(req: Record<string, unknown>): Promise<T> => client().catalog.query<T>(req);

// `after` is a keyset cursor: the id of the last hit already shown. Pass it to fetch the
// next page (matches with a higher rowid); omit for the first page.
// `namesOnly` restricts the match to title/filename (the names_fts index) instead of the full
// index that also matches instrument + comment text — the search box's "names only" toggle.
export const search = (q: string, limit = 60, after?: number, namesOnly = false): Promise<ModuleHit[]> =>
  query<{ results: ModuleHit[] }>({ op: "search", q, limit, after, names: namesOnly }).then((r) => {
    rememberHits(r.results); // warm the offline md5->CID cache from anything the user browses
    return r.results;
  });

// Streaming search: `onRow` fires for each hit the instant its pages arrive over the VFS, so the
// UI paints results progressively instead of after the whole page. Resolves to the total row
// count when the stream completes. Same keyset `after` cursor as `search`. `namesOnly` matches the
// title/filename-only index (the "names only" toggle) rather than instruments/comments too.
export function searchStream(
  q: string,
  limit: number,
  after: number | undefined,
  onRow: (h: ModuleHit) => void,
  namesOnly = false,
): Promise<number> {
  return client().catalog.searchStream({ q, limit, after, namesOnly }, (h) => {
    rememberHit(h); // warm the offline cache with each streamed hit's CID + metadata
    onRow(h);
  });
}

// Abort any in-flight catalog query (the last search): its VFS page reads stop and the
// in-flight block fetches are dropped so the node stops fetching pages we no longer want.
export const cancelSearch = (): Promise<void> => client().catalog.cancel();

// Prewarm the catalog page cache (schema + FTS upper tree) so the first keystroke's search
// descends from warm pages instead of paying the cold schema/FTS-root fetches. Fire-and-forget
// on search-page mount; best-effort (a failure just means the first search pays the cold cost).
export const warmCatalog = (): Promise<void> => client().catalog.warm();

// `genre` (a genreid from getGenres) browses a single genre — index-only via the partial
// idx_browse_genre. `noGenre` browses the un-genred tail (genreid IS NULL) — the home "n/a"
// facet, which is most of the corpus. One facet at a time; the server's precedence is
// noGenre > genre > format. Omit all to browse the whole corpus.
export const listModules = (opts: {
  format?: string;
  genre?: number;
  noGenre?: boolean;
  sort?: "latest" | "random" | "title";
  limit?: number;
  offset?: number;
}): Promise<ModuleHit[]> =>
  query<{ results: ModuleHit[] }>({
    op: "list",
    format: opts.format,
    genre: opts.genre,
    no_genre: opts.noGenre ?? false,
    sort: opts.sort ?? "latest",
    limit: opts.limit ?? 100,
    offset: opts.offset ?? 0,
  }).then((r) => {
    rememberHits(r.results);
    return r.results;
  });

export const getModule = (id: number): Promise<ModuleDetail> =>
  query<ModuleDetail>({ op: "get", id }).then((d) => {
    rememberHit(d);
    return d;
  });

// Resolve a module by its stable content md5 — how playlists (which persist md5, not the
// rebake-unstable rowid) turn a stored track into a playable CID at play time. Every
// success also refreshes the local last-known-good CID cache so the same playlist stays
// playable if the catalog later goes offline (see cidCache.ts).
export const getModuleByMd5 = (md5: string): Promise<ModuleDetail> =>
  query<ModuleDetail>({ op: "get_by_md5", md5 }).then((d) => {
    rememberHit(d);
    return d;
  });

export const getFormats = (): Promise<{ formats: FormatCount[]; total: number }> =>
  query({ op: "formats" });

// The home genre directory: precomputed per-genre counts (meta.genre_counts), most-populous
// first — one page over the VFS, mirror of getFormats. Empty on a catalog predating genre.
export const getGenres = (): Promise<{ genres: GenreCount[] }> => query({ op: "genres" });
