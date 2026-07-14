/// <reference path="./wa-sqlite.d.ts" />
// The durable playlist store — a port of playlists.rs's SQLite layer.
//
// The SQL *is* the spec: schema, migrations and queries are copied from the Rust so the two clients
// agree on what a playlist is. Backed by wa-sqlite over IndexedDB (IDBBatchAtomicVFS), which works
// on the main thread — unlike OPFS sync access handles, which exist ONLY in a worker. The playlist
// DB is small and written rarely (nothing like the catalog's per-keystroke paging), so it does not
// earn a worker of its own.
import * as SQLite from "@journeyapps/wa-sqlite";
import SQLiteAsyncFactory from "@journeyapps/wa-sqlite/dist/wa-sqlite-async.mjs";
import { IDBBatchAtomicVFS } from "@journeyapps/wa-sqlite/src/examples/IDBBatchAtomicVFS.js";
import type { PlaylistDoc } from "./doc.ts";

/** Verbatim from playlists.rs:235-252. */
const SCHEMA = `
CREATE TABLE IF NOT EXISTS playlists (
  name TEXT PRIMARY KEY,
  key_name TEXT,
  title TEXT NOT NULL DEFAULT '',
  doc_json TEXT NOT NULL,
  record_b64 TEXT,
  seq INTEGER NOT NULL DEFAULT 0,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  is_mine INTEGER NOT NULL DEFAULT 0,
  held INTEGER NOT NULL DEFAULT 0,
  published INTEGER NOT NULL DEFAULT 0,
  last_update_at INTEGER NOT NULL DEFAULT 0,
  last_played_at INTEGER
);
CREATE VIRTUAL TABLE IF NOT EXISTS playlists_fts
  USING fts5(name UNINDEXED, title, tracks);
CREATE TABLE IF NOT EXISTS rejected (
  name TEXT NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY (name, seq)
);`;

/** Applied unconditionally; a duplicate-column error means "already applied", exactly as the Rust's
 *  `let _ = conn.execute(...)` intends. */
const MIGRATIONS = [
  "ALTER TABLE playlists ADD COLUMN held INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE playlists ADD COLUMN eol INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE playlists ADD COLUMN tombstoned INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE playlists ADD COLUMN liked INTEGER NOT NULL DEFAULT 0",
];

export interface Row {
  name: string;
  title: string;
  doc_json: string;
  record_b64: string | null;
  seq: number;
  size_bytes: number;
  is_mine: number;
  held: number;
  published: number;
  tombstoned: number;
  liked: number;
  eol: number;
  last_update_at: number;
  last_played_at: number | null;
  key_name: string | null;
}

export const nowSecs = (): number => Math.floor(Date.now() / 1000);

export class PlaylistStore {
  private sqlite3: any;
  private db!: number;
  private queue: Promise<unknown> = Promise.resolve();

  static async open(): Promise<PlaylistStore> {
    const s = new PlaylistStore();
    const module = await SQLiteAsyncFactory();
    s.sqlite3 = SQLite.Factory(module);
    const vfs = await IDBBatchAtomicVFS.create("ts-playlists", module);
    s.sqlite3.vfs_register(vfs, false);
    s.db = await s.sqlite3.open_v2(
      "playlists.db",
      SQLite.SQLITE_OPEN_CREATE | SQLite.SQLITE_OPEN_READWRITE,
      "ts-playlists",
    );
    await s.exec(SCHEMA);
    for (const m of MIGRATIONS) await s.exec(m).catch(() => {});
    return s;
  }

  /** SQLite is not reentrant, and over an async VFS a statement suspends mid-flight while a page is
   *  read — so two overlapping calls re-enter and die inside FTS5. Serialize. */
  private serialize<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.queue.then(fn, fn);
    this.queue = run.catch(() => {});
    return run;
  }

  private async exec(sql: string, params: unknown[] = []): Promise<unknown[][]> {
    const out: unknown[][] = [];
    for await (const stmt of this.sqlite3.statements(this.db, sql)) {
      if (params.length) this.sqlite3.bind_collection(stmt, params as any);
      while ((await this.sqlite3.step(stmt)) === SQLite.SQLITE_ROW) out.push(this.sqlite3.row(stmt));
    }
    return out;
  }

  run(sql: string, params: unknown[] = []): Promise<unknown[][]> {
    return this.serialize(() => this.exec(sql, params));
  }

  private static COLS =
    "name,title,doc_json,record_b64,seq,size_bytes,is_mine,held,published,tombstoned,liked,eol,last_update_at,last_played_at,key_name";

  private static toRow(r: unknown[]): Row {
    return {
      name: String(r[0]),
      title: String(r[1] ?? ""),
      doc_json: String(r[2] ?? ""),
      record_b64: r[3] == null ? null : String(r[3]),
      seq: Number(r[4] ?? 0),
      size_bytes: Number(r[5] ?? 0),
      is_mine: Number(r[6] ?? 0),
      held: Number(r[7] ?? 0),
      published: Number(r[8] ?? 0),
      tombstoned: Number(r[9] ?? 0),
      liked: Number(r[10] ?? 0),
      eol: Number(r[11] ?? 0),
      last_update_at: Number(r[12] ?? 0),
      last_played_at: r[13] == null ? null : Number(r[13]),
      key_name: r[14] == null ? null : String(r[14]),
    };
  }

  async get(name: string): Promise<Row | null> {
    const rows = await this.run(`SELECT ${PlaylistStore.COLS} FROM playlists WHERE name=?`, [name]);
    return rows[0] ? PlaylistStore.toRow(rows[0]) : null;
  }

  async list(): Promise<Row[]> {
    const rows = await this.run(
      // Library first (mine, then held), then the seen tier; most recently played/updated on top —
      // which is why `played()` bumps last_played_at before the view re-queries.
      `SELECT ${PlaylistStore.COLS} FROM playlists
        ORDER BY is_mine DESC, held DESC,
                 COALESCE(last_played_at, 0) DESC, last_update_at DESC`,
    );
    return rows.map(PlaylistStore.toRow);
  }

  /** FTS over titles + track text. Note this uses a DIFFERENT matchstr shape than the catalog:
   *  playlists_fts is a normal (detail=full) FTS5 table, so quoted-token + trailing `*` is fine. */
  async search(q: string): Promise<Row[]> {
    const terms = q
      .split(/[^\p{L}\p{N}]+/u)
      .filter(Boolean)
      .map((t) => `"${t}"*`);
    if (!terms.length) return [];
    const rows = await this.run(
      `SELECT ${PlaylistStore.COLS.split(",")
        .map((c) => `p.${c}`)
        .join(",")}
         FROM playlists_fts f JOIN playlists p ON p.name = f.name
        WHERE playlists_fts MATCH ? LIMIT 200`,
      [terms.join(" ")],
    );
    return rows.map(PlaylistStore.toRow);
  }

  /** Insert-or-replace a playlist row + its FTS entry.
   *
   *  `docBytes` MUST be the bytes as received/emitted — never a re-serialization. A future v:3 field
   *  we don't understand has to survive storage and re-announce byte-for-byte, or the doc stops
   *  hashing to its record's CID and every peer rejects it. */
  async upsert(args: {
    name: string;
    doc: PlaylistDoc;
    docBytes: Uint8Array;
    recordB64: string | null;
    seq: number;
    eol: number;
    isMine?: boolean;
    keyName?: string | null;
    held?: boolean;
    published?: boolean;
  }): Promise<void> {
    const json = new TextDecoder().decode(args.docBytes);
    const tracks = (args.doc.ts ?? []).map((t) => `${t[1]} ${t[2]}`).join(" ");
    await this.run(
      `INSERT INTO playlists (name,title,doc_json,record_b64,seq,size_bytes,tombstoned,eol,last_update_at,
                              is_mine,key_name,held,published)
       VALUES (?,?,?,?,?,?,?,?,?,
               COALESCE((SELECT is_mine FROM playlists WHERE name=?1), 0),
               COALESCE((SELECT key_name FROM playlists WHERE name=?1), NULL),
               COALESCE((SELECT held    FROM playlists WHERE name=?1), 0),
               COALESCE((SELECT published FROM playlists WHERE name=?1), 0))
       ON CONFLICT(name) DO UPDATE SET
         title=excluded.title, doc_json=excluded.doc_json, record_b64=excluded.record_b64,
         seq=excluded.seq, size_bytes=excluded.size_bytes, tombstoned=excluded.tombstoned,
         eol=excluded.eol, last_update_at=excluded.last_update_at`,
      [
        args.name,
        args.doc.t ?? "",
        json,
        args.recordB64,
        args.seq,
        args.docBytes.length,
        args.doc.del ? 1 : 0,
        args.eol,
        nowSecs(),
      ],
    );
    if (args.isMine !== undefined || args.keyName !== undefined) {
      await this.run("UPDATE playlists SET is_mine=COALESCE(?2,is_mine), key_name=COALESCE(?3,key_name) WHERE name=?1", [
        args.name,
        args.isMine === undefined ? null : args.isMine ? 1 : 0,
        args.keyName ?? null,
      ]);
    }
    if (args.held !== undefined) await this.run("UPDATE playlists SET held=? WHERE name=?", [args.held ? 1 : 0, args.name]);
    if (args.published !== undefined) {
      await this.run("UPDATE playlists SET published=? WHERE name=?", [args.published ? 1 : 0, args.name]);
    }
    await this.run("DELETE FROM playlists_fts WHERE name=?", [args.name]);
    await this.run("INSERT INTO playlists_fts (name,title,tracks) VALUES (?,?,?)", [
      args.name,
      args.doc.t ?? "",
      tracks,
    ]);
  }

  async remove(name: string): Promise<void> {
    await this.run("DELETE FROM playlists WHERE name=?", [name]);
    await this.run("DELETE FROM playlists_fts WHERE name=?", [name]);
  }

  /** The seen tier (neither mine nor held) is what the budget governs — library rows are exempt.
   *  Evict oldest-first until we're under it. */
  async enforceBudget(budgetBytes: number): Promise<void> {
    const rows = await this.run(
      `SELECT name, size_bytes FROM playlists
        WHERE is_mine=0 AND held=0
        ORDER BY COALESCE(last_played_at,0) ASC, last_update_at ASC`,
    );
    let total = rows.reduce((n, r) => n + Number(r[1] ?? 0), 0);
    for (const r of rows) {
      if (total <= budgetBytes) break;
      await this.remove(String(r[0]));
      total -= Number(r[1] ?? 0);
    }
  }
}
