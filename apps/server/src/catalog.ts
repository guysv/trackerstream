// Catalog store: SQLite + FTS5 (Node's built-in node:sqlite, no native dep).
// One row per ingested module, carrying its root CID; an FTS5 index over the
// Mod Archive search axes (title / filename / instrument+sample text / comment).
// Ingest is incremental: `source` (outerZip::innerName) is the idempotency key.
import { DatabaseSync } from "node:sqlite";

export interface ModuleRow {
  source: string;
  filename: string;
  format: string;
  title: string;
  duration: number;
  channels: number;
  numSamples: number;
  numInstruments: number;
  numSubsongs: number;
  rootCid: string;
  numBlocks: number;
  sizeBytes: number;
  instruments: string; // instrument + sample names, space-joined
  comment: string;
}

export interface SearchHit {
  id: number;
  filename: string;
  format: string;
  title: string;
  duration: number;
  channels: number;
  rootCid: string;
}

export class Catalog {
  private db: DatabaseSync;
  private insertStmt;
  private hasSourceStmt;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS modules (
        id INTEGER PRIMARY KEY,
        source TEXT UNIQUE NOT NULL,
        filename TEXT NOT NULL,
        format TEXT NOT NULL,
        title TEXT,
        duration REAL,
        channels INTEGER,
        num_samples INTEGER,
        num_instruments INTEGER,
        num_subsongs INTEGER,
        root_cid TEXT NOT NULL,
        num_blocks INTEGER,
        size_bytes INTEGER,
        instruments TEXT,
        comment TEXT,
        ingested_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_modules_format ON modules(format);
      CREATE INDEX IF NOT EXISTS idx_modules_root ON modules(root_cid);
      CREATE VIRTUAL TABLE IF NOT EXISTS modules_fts USING fts5(
        title, filename, instruments, comment,
        content='', tokenize='unicode61'
      );
      CREATE TABLE IF NOT EXISTS playlists (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        is_public INTEGER NOT NULL DEFAULT 0,
        owner TEXT,
        created_at INTEGER,
        updated_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS playlist_items (
        playlist_id INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
        position INTEGER NOT NULL,
        module_id INTEGER NOT NULL,
        root_cid TEXT NOT NULL,
        PRIMARY KEY (playlist_id, position)
      );
    `);
    this.insertStmt = this.db.prepare(`
      INSERT INTO modules
        (source, filename, format, title, duration, channels, num_samples,
         num_instruments, num_subsongs, root_cid, num_blocks, size_bytes,
         instruments, comment, ingested_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(source) DO NOTHING
    `);
    this.hasSourceStmt = this.db.prepare("SELECT 1 FROM modules WHERE source = ? LIMIT 1");
  }

  has(source: string): boolean {
    return this.hasSourceStmt.get(source) !== undefined;
  }

  insert(m: ModuleRow): void {
    const now = Date.now();
    const res = this.insertStmt.run(
      m.source, m.filename, m.format, m.title, m.duration, m.channels,
      m.numSamples, m.numInstruments, m.numSubsongs, m.rootCid, m.numBlocks,
      m.sizeBytes, m.instruments, m.comment, now,
    );
    if (res.changes > 0) {
      const id = res.lastInsertRowid as number;
      this.db
        .prepare("INSERT INTO modules_fts(rowid, title, filename, instruments, comment) VALUES (?,?,?,?,?)")
        .run(id, m.title, m.filename, m.instruments, m.comment);
    }
  }

  search(query: string, limit = 50): SearchHit[] {
    // FTS5 MATCH over all columns; fall back to a prefix query for bare terms.
    const q = query.trim();
    if (!q) return [];
    const match = q.includes('"') || /[*:^]/.test(q) ? q : q.split(/\s+/).map((t) => `"${t}"*`).join(" ");
    const rows = this.db
      .prepare(`
        SELECT m.id, m.filename, m.format, m.title, m.duration, m.channels, m.root_cid AS rootCid
        FROM modules_fts f JOIN modules m ON m.id = f.rowid
        WHERE modules_fts MATCH ?
        ORDER BY bm25(modules_fts) LIMIT ?
      `)
      .all(match, limit) as unknown as SearchHit[];
    return rows;
  }

  get(id: number): (SearchHit & { numSamples: number; numInstruments: number; numSubsongs: number; comment: string; instruments: string; sizeBytes: number }) | undefined {
    return this.db
      .prepare(`
        SELECT id, filename, format, title, duration, channels, root_cid AS rootCid,
               num_samples AS numSamples, num_instruments AS numInstruments,
               num_subsongs AS numSubsongs, size_bytes AS sizeBytes,
               instruments, comment
        FROM modules WHERE id = ?
      `)
      .get(id) as never;
  }

  /** Browse listings: latest / random / by title, optionally filtered by format. */
  list(opts: { format?: string; sort?: "latest" | "random" | "title"; limit?: number; offset?: number }): SearchHit[] {
    const limit = Math.min(500, Math.max(1, opts.limit ?? 100));
    const offset = Math.max(0, opts.offset ?? 0);
    const order =
      opts.sort === "random" ? "RANDOM()" : opts.sort === "title" ? "title COLLATE NOCASE" : "ingested_at DESC, id DESC";
    const where = opts.format ? "WHERE format = ?" : "";
    const args: (string | number)[] = opts.format ? [opts.format] : [];
    args.push(limit, offset);
    return this.db
      .prepare(`
        SELECT id, filename, format, title, duration, channels, root_cid AS rootCid
        FROM modules ${where} ORDER BY ${order} LIMIT ? OFFSET ?
      `)
      .all(...args) as unknown as SearchHit[];
  }

  formatCounts(): { format: string; count: number }[] {
    return this.db
      .prepare("SELECT format, COUNT(*) AS count FROM modules GROUP BY format ORDER BY count DESC")
      .all() as unknown as { format: string; count: number }[];
  }

  count(): number {
    return (this.db.prepare("SELECT COUNT(*) AS n FROM modules").get() as { n: number }).n;
  }

  playlistCount(): number {
    return (this.db.prepare("SELECT COUNT(*) AS n FROM playlists").get() as { n: number }).n;
  }

  // --- playlists (control plane; never on the P2P plane) ---

  createPlaylist(name: string, isPublic: boolean, owner: string | null): number {
    const now = Date.now();
    const res = this.db
      .prepare("INSERT INTO playlists (name, is_public, owner, created_at, updated_at) VALUES (?,?,?,?,?)")
      .run(name, isPublic ? 1 : 0, owner, now, now);
    return res.lastInsertRowid as number;
  }

  listPlaylists(owner?: string): Array<{ id: number; name: string; isPublic: boolean; count: number }> {
    const where = owner ? "WHERE p.owner = ? OR p.is_public = 1" : "";
    const rows = this.db
      .prepare(`
        SELECT p.id, p.name, p.is_public AS isPublic,
               (SELECT COUNT(*) FROM playlist_items pi WHERE pi.playlist_id = p.id) AS count
        FROM playlists p ${where} ORDER BY p.updated_at DESC
      `)
      .all(...(owner ? [owner] : [])) as Array<{ id: number; name: string; isPublic: number; count: number }>;
    return rows.map((r) => ({ ...r, isPublic: !!r.isPublic }));
  }

  getPlaylist(id: number): { id: number; name: string; isPublic: boolean; owner: string | null; items: SearchHit[] } | undefined {
    const p = this.db
      .prepare("SELECT id, name, is_public AS isPublic, owner FROM playlists WHERE id = ?")
      .get(id) as { id: number; name: string; isPublic: number; owner: string | null } | undefined;
    if (!p) return undefined;
    const items = this.db
      .prepare(`
        SELECT m.id, m.filename, m.format, m.title, m.duration, m.channels, m.root_cid AS rootCid
        FROM playlist_items pi JOIN modules m ON m.id = pi.module_id
        WHERE pi.playlist_id = ? ORDER BY pi.position
      `)
      .all(id) as unknown as SearchHit[];
    return { id: p.id, name: p.name, isPublic: !!p.isPublic, owner: p.owner, items };
  }

  setPlaylistItems(id: number, moduleIds: number[]): void {
    this.db.prepare("DELETE FROM playlist_items WHERE playlist_id = ?").run(id);
    const ins = this.db.prepare(
      "INSERT INTO playlist_items (playlist_id, position, module_id, root_cid) " +
        "SELECT ?, ?, id, root_cid FROM modules WHERE id = ?",
    );
    moduleIds.forEach((mid, pos) => ins.run(id, pos, mid));
    this.db.prepare("UPDATE playlists SET updated_at = ? WHERE id = ?").run(Date.now(), id);
  }

  updatePlaylistMeta(id: number, name?: string, isPublic?: boolean): void {
    if (name !== undefined) this.db.prepare("UPDATE playlists SET name = ? WHERE id = ?").run(name, id);
    if (isPublic !== undefined)
      this.db.prepare("UPDATE playlists SET is_public = ? WHERE id = ?").run(isPublic ? 1 : 0, id);
    this.db.prepare("UPDATE playlists SET updated_at = ? WHERE id = ?").run(Date.now(), id);
  }

  deletePlaylist(id: number): void {
    this.db.prepare("DELETE FROM playlist_items WHERE playlist_id = ?").run(id);
    this.db.prepare("DELETE FROM playlists WHERE id = ?").run(id);
  }

  close(): void {
    this.db.close();
  }
}
