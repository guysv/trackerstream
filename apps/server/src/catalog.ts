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

  /** Existing row's id + root CID for a source (rebuild path). */
  getSourceMeta(source: string): { id: number; rootCid: string } | undefined {
    return this.db
      .prepare("SELECT id, root_cid AS rootCid FROM modules WHERE source = ?")
      .get(source) as { id: number; rootCid: string } | undefined;
  }

  /** Point a module at a new root CID — used after a re-bake produces a new
   *  manifest. Returns the module id. */
  updateRoot(source: string, rootCid: string, numBlocks: number): number | undefined {
    const row = this.getSourceMeta(source);
    if (!row) return undefined;
    this.db
      .prepare("UPDATE modules SET root_cid = ?, num_blocks = ? WHERE id = ?")
      .run(rootCid, numBlocks, row.id);
    return row.id;
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

  close(): void {
    this.db.close();
  }
}
