/// <reference path="./wa-sqlite.d.ts" />
// The catalog query engine: opens the Bitswap-backed SQLite DB and answers the same six ops the
// Rust `dispatch` does (search | list | get | get_by_md5 | formats | genres), so the UI's request
// shapes are unchanged between shells.
import * as SQLite from "@journeyapps/wa-sqlite";
import SQLiteAsyncFactory from "@journeyapps/wa-sqlite/dist/wa-sqlite-async.mjs";
import { decompress } from "fzstd";
import { CID } from "multiformats/cid";
import { PageScheduler, resetStats, stats } from "./scheduler.ts";
import { buildMatchstr, listSql, rowToHit, searchSql } from "./search.ts";
import { parseTszcat, type TszcatManifest } from "./tszcat.ts";
import { CatalogVFS } from "./vfs.ts";
import type { BlockFetch } from "./scheduler.ts";

export interface CatalogStats {
  wireBytes: number;
  plainBytes: number;
  blocks: number;
  /** Dependent round-trips. THE metric — cold latency is waves x RTT, and a correctness test will
   *  never catch a regression in it. */
  waves: number;
  cacheHits: number;
}

export class CatalogEngine {
  private sqlite3: any;
  private db!: number;
  private sched!: PageScheduler;
  private man!: TszcatManifest;

  /**
   * @param rootCid   the catalog root (a TSZCAT manifest), resolved from IPNS by the caller
   * @param readRoot  fetch the manifest file's bytes (a UnixFS whole-file read — the ONLY one)
   * @param fetchBlock fetch one raw block by CID (Bitswap)
   */
  static async open(
    rootCid: string,
    readRoot: (cid: CID) => Promise<Uint8Array>,
    fetchBlock: BlockFetch,
    // Browsers fetch the .wasm over http and need nothing here. Node cannot fetch file:// URLs, so
    // a Node harness/test hands the bytes in directly.
    opts: { wasmBinary?: BufferSource } = {},
  ): Promise<CatalogEngine> {
    const e = new CatalogEngine();
    const root = CID.parse(rootCid);
    e.man = parseTszcat(await readRoot(root));
    e.sched = new PageScheduler(rootCid, e.man.pages, fetchBlock, decompress);

    const module = await SQLiteAsyncFactory(opts.wasmBinary ? { wasmBinary: opts.wasmBinary } : undefined);
    e.sqlite3 = SQLite.Factory(module);
    // Register and open under the SAME name. (Don't reach for vfs.name — FacadeVFS does not
    // surface it, and open_v2 with an undefined VFS fails as an opaque `sqlite3_open_v2`.)
    const vfsName = `catalog-${rootCid.slice(-8)}`;
    const vfs = await CatalogVFS.create(vfsName, module, e.man, e.sched);
    e.sqlite3.vfs_register(vfs, false);
    // The FILENAME is arbitrary — our VFS serves one file and ignores the name — but it must be a
    // plain name. FacadeVFS resolves it as a URL, and a bare CID is not one, which surfaces only as
    // an opaque SQLITE_CANTOPEN. The root is identified by the VFS instance, not the path.
    e.db = await e.sqlite3.open_v2("catalog.db", SQLite.SQLITE_OPEN_READONLY, vfsName);
    return e;
  }

  /** Serializes every statement against the single db handle.
   *
   *  SQLite is not reentrant, and over an ASYNC VFS a query suspends mid-statement while a page is
   *  fetched — so a second query starting in that window re-enters the engine and blows up inside
   *  FTS5 with "vtable constructor called recursively: modules_fts". The app triggers this trivially:
   *  warm(), genres() and the user's search all fire at once on mount. The Rust sidesteps it by
   *  opening a fresh Connection per query; here one handle plus a queue is simpler and equivalent
   *  (the page cache, which is the expensive part, is shared either way). */
  private queue: Promise<unknown> = Promise.resolve();

  private serialize<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.queue.then(fn, fn);
    this.queue = run.catch(() => {}); // a failed query must not poison the queue
    return run;
  }

  /** Supersede the in-flight query — the user typed another character. */
  cancel(): void {
    this.sched.cancel();
  }

  stats(): CatalogStats {
    return { ...stats };
  }

  private async rows(sql: string, params: unknown[] = []): Promise<unknown[][]> {
    const out: unknown[][] = [];
    for await (const stmt of this.sqlite3.statements(this.db, sql)) {
      if (params.length) this.sqlite3.bind_collection(stmt, params as any);
      while ((await this.sqlite3.step(stmt)) === SQLite.SQLITE_ROW) out.push(this.sqlite3.row(stmt));
    }
    return out;
  }

  /** Streaming search: `onRow` fires per hit as its pages land, so the UI paints progressively
   *  instead of after the whole page. Returns the row count. */
  searchStream(
    opts: { q: string; limit: number; after?: number; namesOnly?: boolean },
    onRow: (hit: ReturnType<typeof rowToHit>) => void,
  ): Promise<number> {
    return this.serialize(() => this.searchStreamLocked(opts, onRow));
  }

  private async searchStreamLocked(
    opts: { q: string; limit: number; after?: number; namesOnly?: boolean },
    onRow: (hit: ReturnType<typeof rowToHit>) => void,
  ): Promise<number> {
    resetStats();
    const q = opts.q.trim();
    if (!q) return 0;
    const matchstr = buildMatchstr(q);
    if (matchstr === null) return 0; // all punctuation — an empty result set, NOT `MATCH ''` (errors)

    const sql = searchSql(opts.namesOnly ?? false, opts.after);
    let n = 0;
    for await (const stmt of this.sqlite3.statements(this.db, sql)) {
      this.sqlite3.bind_text(stmt, 1, matchstr);
      this.sqlite3.bind_int(stmt, 2, opts.limit);
      while ((await this.sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
        onRow(rowToHit(this.sqlite3.row(stmt)));
        n++;
      }
    }
    return n;
  }

  /** The six ops, mirroring catalog.rs's dispatch. */
  query(req: Record<string, unknown>): Promise<unknown> {
    return this.serialize(() => this.queryLocked(req));
  }

  private async queryLocked(req: Record<string, unknown>): Promise<unknown> {
    switch (req.op) {
      case "search": {
        const results: ReturnType<typeof rowToHit>[] = [];
        await this.searchStreamLocked( // already holding the queue — don't re-enter it
          {
            q: String(req.q ?? ""),
            limit: Number(req.limit ?? 60),
            after: req.after as number | undefined,
            namesOnly: Boolean(req.names),
          },
          (h) => results.push(h),
        );
        return { results };
      }
      case "list": {
        resetStats();
        const { sql, params } = listSql({
          format: req.format as string | undefined,
          genre: req.genre as number | undefined,
          noGenre: Boolean(req.no_genre),
          sort: (req.sort as "latest" | "random" | "title") ?? "latest",
        });
        const rows = await this.rows(sql, [...params, Number(req.limit ?? 100), Number(req.offset ?? 0)]);
        return { results: rows.map(rowToHit) };
      }
      case "get":
        return this.detail("m.id = ?", [Number(req.id)]);
      case "get_by_md5":
        return this.detail("m.md5 = ?", [String(req.md5)]);
      case "formats": {
        const rows = await this.rows(
          "SELECT format, count(*) AS n FROM modules GROUP BY format ORDER BY n DESC",
        );
        return {
          formats: rows.map((r) => ({ format: String(r[0]), count: Number(r[1]) })),
          total: rows.reduce((a, r) => a + Number(r[1]), 0),
        };
      }
      case "genres": {
        // Precomputed counts (meta.genre_counts) — one page, not a GROUP BY over the corpus.
        const rows = await this.rows("SELECT value FROM meta WHERE key = 'genre_counts'");
        const raw = rows[0]?.[0];
        return { genres: raw ? JSON.parse(String(raw)) : [] };
      }
      default:
        throw new Error(`catalog: unknown op ${String(req.op)}`);
    }
  }

  private async detail(where: string, params: unknown[]): Promise<unknown> {
    const rows = await this.rows(
      `SELECT m.id, m.md5, m.filename, m.format, m.title, m.duration, m.channels, m.root_cid,
              m.num_samples, m.num_instruments, m.num_subsongs, m.size_bytes, m.instruments,
              m.comment, g.genre
         FROM modules m LEFT JOIN genres g ON g.genreid = m.genreid
        WHERE ${where} LIMIT 1`,
      params,
    );
    const r = rows[0];
    if (!r) throw new Error("catalog: module not found");
    return {
      ...rowToHit(r),
      numSamples: Number(r[8] ?? 0),
      numInstruments: Number(r[9] ?? 0),
      numSubsongs: Number(r[10] ?? 0),
      sizeBytes: Number(r[11] ?? 0),
      instruments: String(r[12] ?? ""),
      comment: String(r[13] ?? ""),
      genre: r[14] == null ? null : String(r[14]),
    };
  }
}
