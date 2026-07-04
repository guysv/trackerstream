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
  md5: string; // lowercase-hex md5 of the raw module file (TMA/ModArchive join key)
}

export class Catalog {
  private db: DatabaseSync;
  private insertStmt;
  private hasSourceStmt;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
    // Wait (don't error) for a lock — lets multiple sharded ingest workers open and
    // write the catalog concurrently (WAL: one writer at a time). MUST be first: even
    // `journal_mode = WAL` takes a brief exclusive lock, so without this a second
    // worker starting at the same time dies with "database is locked". Harmless for
    // the single-writer path.
    this.db.exec("PRAGMA busy_timeout = 60000;");
    // page_size must be set before any table/WAL exists; it's a silent no-op on an
    // already-created DB (apply via a one-time REBUILD ingest into a fresh file).
    // 16 KB = the IPFS page-aligned chunk unit the catalog is published under, so
    // each SQLite page maps to exactly one stable UnixFS block (R1, see plan).
    this.db.exec("PRAGMA page_size = 16384;");
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
        md5 TEXT,
        ingested_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_modules_format ON modules(format);
      CREATE INDEX IF NOT EXISTS idx_modules_root ON modules(root_cid);
      -- Covering indexes for the three browse orders (list()): every browse column
      -- lives in the index so a listing is index-only — no table scan. Lab: collapses
      -- browse from ~18k pages (73 MB) to ~16 pages (64 KB) over Bitswap. id is the
      -- rowid (implicitly present) but is listed to satisfy the ORDER BY tiebreaker.
      CREATE INDEX IF NOT EXISTS idx_browse_latest ON modules(
        ingested_at DESC, id DESC, filename, format, title, duration, channels, root_cid);
      CREATE INDEX IF NOT EXISTS idx_browse_format ON modules(
        format, ingested_at DESC, id DESC, filename, title, duration, channels, root_cid);
      CREATE INDEX IF NOT EXISTS idx_browse_title ON modules(
        title COLLATE NOCASE, id, filename, format, duration, channels, root_cid);
      CREATE VIRTUAL TABLE IF NOT EXISTS modules_fts USING fts5(
        title, filename, instruments, comment,
        content='', tokenize='unicode61'
      );
      -- Per-term document frequencies (view over the FTS index, no storage): lets
      -- search() cheaply detect a low-selectivity term and skip global bm25 ranking.
      CREATE VIRTUAL TABLE IF NOT EXISTS modules_vocab USING fts5vocab('modules_fts', 'row');
      -- Precomputed aggregates (refreshed at end of ingest) so count()/formatCounts()
      -- are O(1) lookups, not full-table scans, when queried over the Bitswap VFS.
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    `);
    // Migration for catalogs created before the md5 column existed: CREATE TABLE
    // IF NOT EXISTS never alters an existing table, so add the column explicitly.
    // No-op on a fresh DB (the CREATE above already carries md5). Populate old rows
    // with a BACKFILL_MD5 ingest pass. The md5 index is created after, since it
    // references a column that may only just now exist.
    const cols = this.db.prepare("PRAGMA table_info(modules)").all() as { name: string }[];
    if (!cols.some((c) => c.name === "md5")) {
      this.db.exec("ALTER TABLE modules ADD COLUMN md5 TEXT");
    }
    // Index the join key: md5 lookups (e.g. genre enrichment) hit the index, not a
    // full scan — critical when the DB is queried page-by-page over the Bitswap VFS.
    // Not UNIQUE: the corpus can hold the same file under multiple sources.
    this.db.exec("CREATE INDEX IF NOT EXISTS idx_modules_md5 ON modules(md5);");
    this.insertStmt = this.db.prepare(`
      INSERT INTO modules
        (source, filename, format, title, duration, channels, num_samples,
         num_instruments, num_subsongs, root_cid, num_blocks, size_bytes,
         instruments, comment, md5, ingested_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
      m.sizeBytes, m.instruments, m.comment, m.md5, now,
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

  /** A cataloged source that still has no md5 (a BACKFILL_MD5 target). Lets the
   *  backfill pass skip hashing bytes for rows already filled — cheap, indexed. */
  md5Missing(source: string): boolean {
    return (
      this.db
        .prepare("SELECT 1 FROM modules WHERE source = ? AND md5 IS NULL LIMIT 1")
        .get(source) !== undefined
    );
  }

  /** Backfill the md5 for one source. Guarded by `md5 IS NULL` so it's idempotent
   *  and never overwrites an existing hash. Returns whether a row was updated. */
  setMd5(source: string, md5: string): boolean {
    const res = this.db
      .prepare("UPDATE modules SET md5 = ? WHERE source = ? AND md5 IS NULL")
      .run(md5, source);
    return res.changes > 0;
  }

  // Catalog search/browse/detail are no longer served here (R1): the published DB is
  // queried by clients over the Bitswap SQLite VFS. The server only writes the catalog
  // (insert/updateRoot), reports count(), and bakes the aggregates the client reads.

  private scanFormatCounts(): { format: string; count: number }[] {
    return this.db
      .prepare("SELECT format, COUNT(*) AS count FROM modules GROUP BY format ORDER BY count DESC")
      .all() as unknown as { format: string; count: number }[];
  }

  count(): number {
    const row = this.db.prepare("SELECT value FROM meta WHERE key = 'total'").get() as
      | { value: string }
      | undefined;
    if (row) return +row.value;
    return this.scanCount();
  }

  private scanCount(): number {
    return (this.db.prepare("SELECT COUNT(*) AS n FROM modules").get() as { n: number }).n;
  }

  /** Recompute the precomputed aggregates (total + per-format counts) from the
   *  live tables. Called at the end of ingest, just before the DB is snapshotted
   *  and published, so the meta table the client reads is always current. */
  refreshMeta(): void {
    const total = this.scanCount();
    const counts = this.scanFormatCounts();
    const up = this.db.prepare("INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value");
    up.run("total", String(total));
    up.run("format_counts", JSON.stringify(counts));
  }

  /** Fold the WAL back into the main DB file so an on-disk copy of it is a
   *  self-contained, consistent snapshot (used before the publish-to-IPFS copy). */
  checkpoint(): void {
    this.db.exec("PRAGMA wal_checkpoint(TRUNCATE);");
  }

  close(): void {
    this.db.close();
  }
}
