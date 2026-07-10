// Catalog store: SQLite + FTS5 (Node's built-in node:sqlite, no native dep).
// One row per ingested module, carrying its root CID; an FTS5 index over the
// Mod Archive search axes (title / filename / instrument+sample text / comment).
// Ingest is incremental: `source` (outerZip::innerName) is the idempotency key.
import { DatabaseSync } from "node:sqlite";

// Catalog schema version, written into meta.schema_version on every bake (wire-version
// hardening). The client (catalog.rs) hand-ports this SQL with POSITIONAL row-mapping, so a
// column reorder/drop would silently mis-read with no signal. Bump this ONLY on a
// non-backward-compatible layout change (column reorder/rename/drop); purely ADDITIVE changes
// (new columns old clients don't query — as md5 and genreId already were) keep the version, so
// a newer catalog stays readable by older clients. An incompatible bump needs the signed
// min_client_version gate before it can ship.
export const CATALOG_SCHEMA_VERSION = 1;

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
  genreId?: number | null; // TMA genre id (via md5 join); null when the corpus file isn't genred
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
        genreid INTEGER,
        ingested_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_modules_format ON modules(format);
      CREATE INDEX IF NOT EXISTS idx_modules_root ON modules(root_cid);
      -- Covering index for the default (latest) browse: every HIT_COLS column lives in the
      -- index so a listing is index-only — no table scan. id is the rowid (implicitly present)
      -- but is listed to satisfy the ORDER BY tiebreaker. NOTE: idx_browse_latest deliberately
      -- OMITS md5 (unlike format/title/genre below): the latest browse returns the newest rows,
      -- whose rowids are contiguous, so even the non-covering md5 fetch hits a few adjacent table
      -- pages (good locality) — not worth the md5 weight on the largest, most-used index. The
      -- format/title/genre browses return rows scattered across the whole rowid space, where a
      -- per-row table fetch is ~1 page each (~12 MB for 300 rows), so those DO carry md5 and are
      -- built via ensureCoveringIndex() below (self-heals older md5-less indexes into covering ones).
      CREATE INDEX IF NOT EXISTS idx_browse_latest ON modules(
        ingested_at DESC, id DESC, filename, format, title, duration, channels, root_cid);
      -- prefix='2 3': dedicated 2- and 3-char prefix indexes so the client's 2-3 char
      -- prefix queries ("ab"* / "abc"*) seek instead of scanning a wide dictionary range
      -- over the Bitswap VFS. The search box gates on a 2-char minimum, so these cover it.
      -- detail='none': search takes flat-rowid matches (no bm25, no phrase/NEAR, no column
      -- filters — none of which the client uses), so FTS5's per-token POSITION lists are dead
      -- weight. Dropping them shrinks modules_fts_data (measured: served DB 381 -> 263 MB) and
      -- ~halves the FTS-walk round-trips over the Bitswap VFS, with IDENTICAL result sets (lab-
      -- verified parity incl. instrument/comment terms). columnsize=0 drops the bm25 docsize
      -- table we no longer read. Trade-off: forecloses column-scoped MATCH (instruments:foo)
      -- if a future feature wants it — switch to detail='column' (keeps filters, +48 MB) then.
      CREATE VIRTUAL TABLE IF NOT EXISTS modules_fts USING fts5(
        title, filename, instruments, comment,
        content='', tokenize='unicode61', prefix='2 3', detail='none', columnsize=0
      );
      -- Names-only index for the "restrict to title/filename" search toggle. modules_fts
      -- matches instruments+comment too (great for discovery, noisy when you know the exact
      -- module you want); this one is just the two name columns, so the client MATCHes it
      -- instead when the toggle is on. Cheapest way to get name-scoped search: modules_fts is
      -- detail='none'/columnsize=0, which forecloses column-scoped MATCH (title:foo) — a
      -- second small table over the two SHORT columns is far less weight than switching the
      -- main index to detail='column' (+48 MB) would be. Same rowid (modules.id), so hits
      -- JOIN to modules identically. Populated by insert()/reindexFts() alongside modules_fts.
      CREATE VIRTUAL TABLE IF NOT EXISTS names_fts USING fts5(
        title, filename,
        content='', tokenize='unicode61', prefix='2 3', detail='none', columnsize=0
      );
      -- Precomputed aggregates (refreshed at end of ingest) so count()/formatCounts()
      -- are O(1) lookups, not full-table scans, when queried over the Bitswap VFS.
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      -- Genre label lookup (TMA genre id -> text). 77 rows, populated during a genre
      -- backfill/ingest from the api-dumps/tma-genres map. Small enough for the client to
      -- read whole; refreshMeta() also folds it into meta.genre_counts (id+label+count) so
      -- the home genre directory is a single meta read, not a modules⋈genres scan over Bitswap.
      -- (Does NOT reference modules.genreid, so it's safe here even on a pre-genre DB.)
      CREATE TABLE IF NOT EXISTS genres (genreid INTEGER PRIMARY KEY, genre TEXT NOT NULL);
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
    // Same story for genreid on catalogs created before the genre column existed. Add it
    // before creating idx_browse_genre below (that index references the column, so it can't
    // live in the CREATE block above — that runs before this ALTER on an old DB). Populate
    // old rows with a BACKFILL_GENRE ingest pass (pure md5-join UPDATE, no re-bake).
    if (!cols.some((c) => c.name === "genreid")) {
      this.db.exec("ALTER TABLE modules ADD COLUMN genreid INTEGER");
    }
    // Index the join key: md5 lookups (e.g. genre enrichment) hit the index, not a
    // full scan — critical when the DB is queried page-by-page over the Bitswap VFS.
    // Not UNIQUE: the corpus can hold the same file under multiple sources.
    this.db.exec("CREATE INDEX IF NOT EXISTS idx_modules_md5 ON modules(md5);");
    // Covering browse indexes for the SCATTERED orders (format / title / genre). Each carries
    // every HIT_COLS column — crucially md5 — so a filtered/sorted listing is index-only over
    // Bitswap. Without md5 the index is non-covering (HIT_COLS SELECTs md5), forcing a table-page
    // fetch per row; these orders return rows scattered across the whole rowid space, so that's
    // ~1 page per row (~12 MB for a 300-row browse). ensureCoveringIndex() self-heals any earlier
    // md5-less version of the index (drops + rebuilds). genre is built here (after the ALTER) so
    // its genreid column exists; it's PARTIAL (WHERE genreid IS NOT NULL) since only ~17% of the
    // corpus is genred. `list` filters WHERE genreid=? (implies IS NOT NULL → SQLite still picks
    // it as a COVERING INDEX, verified via EXPLAIN QUERY PLAN).
    this.ensureCoveringIndex(
      "idx_browse_format",
      `CREATE INDEX IF NOT EXISTS idx_browse_format ON modules(
        format, ingested_at DESC, id DESC, md5, filename, title, duration, channels, root_cid);`,
    );
    this.ensureCoveringIndex(
      "idx_browse_title",
      `CREATE INDEX IF NOT EXISTS idx_browse_title ON modules(
        title COLLATE NOCASE, id, md5, filename, format, duration, channels, root_cid);`,
    );
    this.ensureCoveringIndex(
      "idx_browse_genre",
      `CREATE INDEX IF NOT EXISTS idx_browse_genre ON modules(
        genreid, ingested_at DESC, id DESC, md5, filename, format, title, duration, channels, root_cid)
        WHERE genreid IS NOT NULL;`,
    );
    this.insertStmt = this.db.prepare(`
      INSERT INTO modules
        (source, filename, format, title, duration, channels, num_samples,
         num_instruments, num_subsongs, root_cid, num_blocks, size_bytes,
         instruments, comment, md5, genreid, ingested_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(source) DO NOTHING
    `);
    this.hasSourceStmt = this.db.prepare("SELECT 1 FROM modules WHERE source = ? LIMIT 1");
  }

  /** Create a covering browse index, self-healing an earlier md5-less version. `CREATE INDEX
   *  IF NOT EXISTS` can't widen an existing index, so if the stored index SQL lacks `md5` (an
   *  older build predating the covering fix — which made browse non-covering, a table fetch per
   *  row) we DROP it first, then run `createSql` to rebuild it covering. Idempotent: once the
   *  live index already carries md5, the drop is skipped and the CREATE is a no-op. `name` is a
   *  fixed internal literal (not user input). Runs only during ingest opens, not client reads. */
  private ensureCoveringIndex(name: string, createSql: string): void {
    const row = this.db
      .prepare("SELECT sql FROM sqlite_master WHERE type='index' AND name = ?")
      .get(name) as { sql: string } | undefined;
    if (row && !/\bmd5\b/.test(row.sql)) this.db.exec(`DROP INDEX ${name}`);
    this.db.exec(createSql);
  }

  has(source: string): boolean {
    return this.hasSourceStmt.get(source) !== undefined;
  }

  insert(m: ModuleRow): void {
    const now = Date.now();
    const res = this.insertStmt.run(
      m.source, m.filename, m.format, m.title, m.duration, m.channels,
      m.numSamples, m.numInstruments, m.numSubsongs, m.rootCid, m.numBlocks,
      m.sizeBytes, m.instruments, m.comment, m.md5, m.genreId ?? null, now,
    );
    if (res.changes > 0) {
      const id = res.lastInsertRowid as number;
      this.db
        .prepare("INSERT INTO modules_fts(rowid, title, filename, instruments, comment) VALUES (?,?,?,?,?)")
        .run(id, m.title, m.filename, m.instruments, m.comment);
      this.db
        .prepare("INSERT INTO names_fts(rowid, title, filename) VALUES (?,?,?)")
        .run(id, m.title, m.filename);
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

  /** Upsert the genre label table (TMA genre id -> text). Idempotent; a re-sweep that
   *  renames a genre updates the text in place. Wrapped in one transaction (77 rows). */
  upsertGenres(labels: Map<number, string>): void {
    const up = this.db.prepare(
      "INSERT INTO genres(genreid, genre) VALUES (?, ?) ON CONFLICT(genreid) DO UPDATE SET genre = excluded.genre",
    );
    this.db.exec("BEGIN");
    try {
      for (const [id, text] of labels) up.run(id, text);
      this.db.exec("COMMIT");
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }

  /** Stamp genreid onto modules by md5 join — the BACKFILL_GENRE pass. Upserts the label
   *  table first, then UPDATEs every row whose md5 is in the map. Pure SQL, no corpus walk
   *  (md5 is already on every row), so it's cheaper than the md5 backfill. All corpus copies
   *  of a file (same md5 under multiple sources) get stamped. `clearFirst` resets genreid to
   *  NULL before applying, for exact parity with a fresh TMA sweep (drops genres no longer in
   *  the map); off by default so an interrupted/partial map only adds. Returns rows stamped. */
  applyGenres(byMd5: Map<string, number>, labels: Map<number, string>, clearFirst = false): number {
    this.upsertGenres(labels);
    const upd = this.db.prepare("UPDATE modules SET genreid = ? WHERE md5 = ?");
    this.db.exec("BEGIN");
    try {
      if (clearFirst) this.db.exec("UPDATE modules SET genreid = NULL");
      let matched = 0;
      for (const [md5, id] of byMd5) matched += upd.run(id, md5).changes as number;
      this.db.exec("COMMIT");
      return matched;
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }

  // Catalog search/browse/detail are no longer served here (R1): the published DB is
  // queried by clients over the Bitswap SQLite VFS. The server only writes the catalog
  // (insert/updateRoot), reports count(), and bakes the aggregates the client reads.

  private scanFormatCounts(): { format: string; count: number }[] {
    return this.db
      .prepare("SELECT format, COUNT(*) AS count FROM modules GROUP BY format ORDER BY count DESC")
      .all() as unknown as { format: string; count: number }[];
  }

  /** Per-genre counts over the ACTUAL corpus (not TMA's totals) joined to labels, most
   *  populous first — folded into meta.genre_counts for the client's home genre directory. */
  private scanGenreCounts(): { genreid: number; genre: string; count: number }[] {
    return this.db
      .prepare(
        `SELECT m.genreid AS genreid, g.genre AS genre, COUNT(*) AS count
           FROM modules m JOIN genres g ON g.genreid = m.genreid
          WHERE m.genreid IS NOT NULL
          GROUP BY m.genreid
          ORDER BY count DESC`,
      )
      .all() as unknown as { genreid: number; genre: string; count: number }[];
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
    up.run("genre_counts", JSON.stringify(this.scanGenreCounts()));
    // Schema-version signal so an older client can detect a layout it can't positionally
    // read and soft-degrade instead of mis-mapping columns (wire-version hardening).
    up.run("schema_version", String(CATALOG_SCHEMA_VERSION));
  }

  /** Rebuild the FTS index in place from the `modules` table, and drop the now-unused
   *  fts5vocab table. Re-creates `modules_fts` with the CURRENT schema (the constructor's
   *  `CREATE ... IF NOT EXISTS` can't alter an existing virtual table, so a schema change
   *  like adding `prefix='2 3'` needs this), then repopulates every row. Touches only the
   *  catalog's search index — the module DAGs, pins and root_cids are untouched, so it's a
   *  cheap catalog-only republish, not a corpus re-bake. Returns the row count. */
  reindexFts(): number {
    this.db.exec("DROP TABLE IF EXISTS modules_vocab");
    this.db.exec("DROP TABLE IF EXISTS modules_fts");
    this.db.exec(`
      CREATE VIRTUAL TABLE modules_fts USING fts5(
        title, filename, instruments, comment,
        content='', tokenize='unicode61', prefix='2 3', detail='none', columnsize=0
      );
    `);
    this.db.exec(`
      INSERT INTO modules_fts(rowid, title, filename, instruments, comment)
      SELECT id, title, filename, instruments, comment FROM modules;
    `);
    // Rebuild the names-only index in the same pass, so a plain FTS reindex/republish
    // (not a full corpus re-bake) is enough to light up the title/filename search toggle
    // on an existing catalog that predates names_fts.
    this.db.exec("DROP TABLE IF EXISTS names_fts");
    this.db.exec(`
      CREATE VIRTUAL TABLE names_fts USING fts5(
        title, filename,
        content='', tokenize='unicode61', prefix='2 3', detail='none', columnsize=0
      );
    `);
    this.db.exec(`
      INSERT INTO names_fts(rowid, title, filename)
      SELECT id, title, filename FROM modules;
    `);
    return this.scanCount();
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
