// Social (lite): custom email accounts, follows, presence ("now playing"), and
// share links. Lives entirely in the control-plane DB over HTTP — NEVER on the
// P2P plane. Shares a single SQLite file with the catalog (WAL allows multiple
// connections); auth uses node:crypto scrypt (no external deps).
import { DatabaseSync } from "node:sqlite";
import { randomBytes, scryptSync, timingSafeEqual } from "node:crypto";

export interface User {
  id: number;
  email: string;
}

export interface PresenceRow {
  userId: number;
  email: string;
  moduleId: number | null;
  rootCid: string | null;
  title: string | null;
  updatedAt: number;
}

export class Social {
  private db: DatabaseSync;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL,
        salt TEXT NOT NULL, hash TEXT NOT NULL, created_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, created_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS follows (
        follower_id INTEGER NOT NULL, followee_id INTEGER NOT NULL,
        PRIMARY KEY (follower_id, followee_id)
      );
      CREATE TABLE IF NOT EXISTS presence (
        user_id INTEGER PRIMARY KEY, module_id INTEGER, root_cid TEXT,
        title TEXT, updated_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS shares (
        code TEXT PRIMARY KEY, kind TEXT NOT NULL, ref_id INTEGER NOT NULL,
        root_cid TEXT, created_at INTEGER
      );
    `);
  }

  // --- auth ---

  register(email: string, password: string): { token: string; user: User } {
    const salt = randomBytes(16).toString("hex");
    const hash = scryptSync(password, salt, 32).toString("hex");
    const res = this.db
      .prepare("INSERT INTO users (email, salt, hash, created_at) VALUES (?,?,?,?)")
      .run(email.toLowerCase(), salt, hash, Date.now());
    const id = res.lastInsertRowid as number;
    return { token: this.mkSession(id), user: { id, email } };
  }

  login(email: string, password: string): { token: string; user: User } | null {
    const u = this.db
      .prepare("SELECT id, email, salt, hash FROM users WHERE email = ?")
      .get(email.toLowerCase()) as { id: number; email: string; salt: string; hash: string } | undefined;
    if (!u) return null;
    const h = scryptSync(password, u.salt, 32);
    if (!timingSafeEqual(h, Buffer.from(u.hash, "hex"))) return null;
    return { token: this.mkSession(u.id), user: { id: u.id, email: u.email } };
  }

  private mkSession(userId: number): string {
    const token = randomBytes(24).toString("hex");
    this.db.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?,?,?)").run(token, userId, Date.now());
    return token;
  }

  userForToken(token: string | null): User | null {
    if (!token) return null;
    const r = this.db
      .prepare("SELECT u.id, u.email FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token = ?")
      .get(token) as User | undefined;
    return r ?? null;
  }

  // --- follows + presence ---

  follow(follower: number, followee: number): void {
    this.db
      .prepare("INSERT OR IGNORE INTO follows (follower_id, followee_id) VALUES (?,?)")
      .run(follower, followee);
  }
  unfollow(follower: number, followee: number): void {
    this.db.prepare("DELETE FROM follows WHERE follower_id = ? AND followee_id = ?").run(follower, followee);
  }

  setPresence(userId: number, moduleId: number | null, rootCid: string | null, title: string | null): void {
    this.db
      .prepare(`
        INSERT INTO presence (user_id, module_id, root_cid, title, updated_at) VALUES (?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET module_id=?, root_cid=?, title=?, updated_at=?
      `)
      .run(userId, moduleId, rootCid, title, Date.now(), moduleId, rootCid, title, Date.now());
  }

  /** Who the user follows + their current/recent listening. */
  following(userId: number): PresenceRow[] {
    return this.db
      .prepare(`
        SELECT u.id AS userId, u.email, p.module_id AS moduleId, p.root_cid AS rootCid,
               p.title, COALESCE(p.updated_at, 0) AS updatedAt
        FROM follows f JOIN users u ON u.id = f.followee_id
        LEFT JOIN presence p ON p.user_id = u.id
        WHERE f.follower_id = ? ORDER BY updatedAt DESC
      `)
      .all(userId) as unknown as PresenceRow[];
  }

  findUser(email: string): User | null {
    const r = this.db.prepare("SELECT id, email FROM users WHERE email = ?").get(email.toLowerCase()) as
      | User
      | undefined;
    return r ?? null;
  }

  // --- share links ---

  createShare(kind: "module" | "playlist", refId: number, rootCid: string | null): string {
    const code = randomBytes(6).toString("base64url");
    this.db
      .prepare("INSERT INTO shares (code, kind, ref_id, root_cid, created_at) VALUES (?,?,?,?,?)")
      .run(code, kind, refId, rootCid, Date.now());
    return code;
  }

  resolveShare(code: string): { kind: string; refId: number; rootCid: string | null } | null {
    const r = this.db
      .prepare("SELECT kind, ref_id AS refId, root_cid AS rootCid FROM shares WHERE code = ?")
      .get(code) as { kind: string; refId: number; rootCid: string | null } | undefined;
    return r ?? null;
  }

  userCount(): number {
    return (this.db.prepare("SELECT COUNT(*) AS n FROM users").get() as { n: number }).n;
  }

  close(): void {
    this.db.close();
  }
}
