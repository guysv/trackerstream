// The catalog's FTS5 query layer — a port of the query half of catalog.rs.
//
// The two rules encoded here are both load-bearing and both counter-intuitive; see the comments.

/**
 * Build the FTS5 MATCH string for a user query.
 *
 * The FTS tables are built with `detail='none'`, which FORBIDS phrase queries. A term like
 * `c20g_j` is TWO `unicode61` tokens (`_`, `.`, `-`, `'` … are separators), so quoting it whole as
 * `"c20g_j"` is a two-token phrase and FTS5 errors with "phrase queries are not supported
 * (detail!=full)" — which the UI surfaces to the user as "search offline". So: split on the SAME
 * boundaries unicode61 tokenizes on (non-alphanumerics) and emit each sub-token as its own prefix
 * term, space-joined = implicit AND.
 *
 * A query already using FTS operators (" * : ^) passes through verbatim, so power users keep full
 * control. Returns null when nothing tokenizable remains (e.g. all punctuation) — the caller must
 * treat that as an empty result set rather than running `MATCH ''`, which itself errors.
 */
export function buildMatchstr(q: string): string | null {
  if (q.includes('"') || q.includes("*") || q.includes(":") || q.includes("^")) {
    return q; // explicit FTS syntax — the user drives the query
  }
  const terms = q
    .split(/[^\p{L}\p{N}]+/u) // unicode61: non-alphanumerics are separators
    .filter((s) => s.length > 0)
    .map((s) => `"${s}"*`);
  return terms.length ? terms.join(" ") : null;
}

/** names_fts matches title/filename only (the "names only" toggle); modules_fts also matches
 *  instrument names + comment text. */
export const ftsTable = (namesOnly: boolean): string => (namesOnly ? "names_fts" : "modules_fts");

/** The 8 columns every hit carries. The browse indexes are COVERING over exactly these, which is
 *  what keeps a browse index-only (3 pages, not 12MB). Do not add a column here without adding it
 *  to the indexes in apps/server/src/catalog.ts — you would silently turn every browse into a
 *  table scan over the VFS. */
export const HIT_COLS = "m.id, m.md5, m.filename, m.format, m.title, m.duration, m.channels, m.root_cid";

/**
 * Search SQL.
 *
 * NO RELEVANCE RANKING, deliberately. `ORDER BY bm25` has to score EVERY matching row before
 * LIMIT, and each scored posting is a page fetched over the network — so a mid-frequency term
 * ("mario": ~34MB / 2000+ pages / 90s cold) cost far more than a broad one. Flat rowid order
 * touches ~280 pages regardless of term frequency.
 *
 * Keyset pagination, not OFFSET: results are ascending FTS rowid, so the next page is simply the
 * matches with rowid > the last id the client holds — skipping straight past what's already shown
 * instead of re-scanning it over the VFS.
 */
export function searchSql(namesOnly: boolean, after?: number): string {
  const fts = ftsTable(namesOnly);
  const cursor = after !== undefined ? `AND f.rowid > ${Number(after)}` : "";
  return `SELECT ${HIT_COLS}
            FROM ${fts} f JOIN modules m ON m.id = f.rowid
           WHERE ${fts} MATCH ? ${cursor}
           ORDER BY f.rowid
           LIMIT ?`;
}

/** Browse. One facet at a time — that is what keeps each browse index-only against its covering
 *  index (idx_browse_latest / _format / _title / _genre). Precedence mirrors the Rust: noGenre >
 *  genre > format. */
export function listSql(opts: {
  format?: string;
  genre?: number;
  noGenre?: boolean;
  sort?: "latest" | "random" | "title";
}): { sql: string; params: unknown[] } {
  const params: unknown[] = [];
  let where = "";
  if (opts.noGenre) {
    where = "WHERE m.genreid IS NULL";
  } else if (opts.genre !== undefined) {
    where = "WHERE m.genreid = ?";
    params.push(opts.genre);
  } else if (opts.format) {
    where = "WHERE m.format = ?";
    params.push(opts.format);
  }
  const order =
    opts.sort === "title" ? "ORDER BY m.title" : opts.sort === "random" ? "ORDER BY RANDOM()" : "ORDER BY m.ingested_at DESC, m.id DESC";
  return { sql: `SELECT ${HIT_COLS} FROM modules m ${where} ${order} LIMIT ? OFFSET ?`, params };
}

/** Map a raw SQLite row (HIT_COLS order) to the ModuleHit the UI speaks. */
export function rowToHit(r: unknown[]): {
  id: number;
  md5: string;
  filename: string;
  format: string;
  title: string;
  duration: number;
  channels: number;
  rootCid: string;
} {
  return {
    id: Number(r[0]),
    md5: String(r[1] ?? ""),
    filename: String(r[2] ?? ""),
    format: String(r[3] ?? ""),
    title: String(r[4] ?? ""),
    duration: Number(r[5] ?? 0),
    channels: Number(r[6] ?? 0),
    rootCid: String(r[7] ?? ""),
  };
}
