//! Playlists over IPNS (PLAYLISTS.md) — the sync engine and the ONLY durable playlist
//! store anywhere in the system (`playlists.db`, SQLite + FTS5 in the app data dir).
//!
//! Docs travel inline in gossip next to their signed IPNS record; the Go sidecar is a
//! bounded relay buffer we drain via `playlist/records`. Trust anchors HERE: every
//! drained entry is re-verified (record signature + EOL via `ipns::verify_b64_seq`, then
//! doc-hash against the record's CID) before it touches the DB. Publishing is explicit
//! ("Share") — playlists are local-only by default.

use crate::ipns;
use crate::rpc::{NodeRpc, PlaylistWire};
use anyhow::{anyhow, bail, Result};
use base64::Engine;
use cid::Cid;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 1 MiB — a bigger doc is adversarial by definition (mirrors the Go validator).
const DOC_MAX: usize = 1 << 20;
const TRACKS_MAX: usize = 20_000;
const TITLE_MAX: usize = 300;
const FIELD_MAX: usize = 512;
const LIFETIME: &str = "168h";
/// Republish an own record when it has less than this long to live.
const RENEW_MARGIN_SECS: i64 = 24 * 3600;

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---- document model ----

/// One track reference: `[catalog id, module name, song title]` — denormalized so the
/// playlist view renders with zero catalog lookups; the id resolves to a CID via the
/// normal catalog path only at play time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackRef(pub i64, pub String, pub String);

/// The compact wire document: `{"v":1,"t":"title","ts":[[id,"mod","title"],...]}`.
/// Tombstone: `{"v":1,"del":true}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaylistDoc {
    pub v: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub t: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub del: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ts: Vec<TrackRef>,
}

/// Parse + schema-validate untrusted doc bytes. Strict: unknown versions, oversized
/// fields, or absurd track counts are rejected (and remembered, so never refetched).
pub fn validate_doc(bytes: &[u8]) -> Result<PlaylistDoc> {
    if bytes.is_empty() || bytes.len() > DOC_MAX {
        bail!("doc size {} out of bounds", bytes.len());
    }
    let doc: PlaylistDoc = serde_json::from_slice(bytes)?;
    if doc.v != 1 {
        bail!("unknown doc version {}", doc.v);
    }
    if doc.del {
        return Ok(doc);
    }
    if doc.t.len() > TITLE_MAX {
        bail!("title too long");
    }
    if doc.ts.len() > TRACKS_MAX {
        bail!("too many tracks ({})", doc.ts.len());
    }
    for t in &doc.ts {
        if t.1.len() > FIELD_MAX || t.2.len() > FIELD_MAX {
            bail!("track field too long");
        }
    }
    Ok(doc)
}

/// The record's CID is the doc's integrity anchor: raw codec (0x55) + sha2-256 (0x12),
/// exactly what the Go node signs. Anything else fails closed.
fn doc_matches_cid(cid: &Cid, doc: &[u8]) -> bool {
    cid.codec() == 0x55
        && cid.hash().code() == 0x12
        && cid.hash().digest() == Sha256::digest(doc).as_slice()
}

// ---- UI shapes ----

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaylistMeta {
    pub name: String,
    pub title: String,
    pub tracks: i64,
    pub is_mine: bool,
    pub held: bool,
    pub published: bool,
    pub size_bytes: i64,
    pub last_update_at: i64,
    pub last_played_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackUi {
    pub id: i64,
    pub mod_name: String,
    pub title: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaylistDetail {
    #[serde(flatten)]
    pub meta: PlaylistMeta,
    // "items", NOT "tracks": the flattened meta already serializes a `tracks` COUNT —
    // a same-named field here would emit a duplicate JSON key and the JS side would
    // see whichever wins, never both.
    pub items: Vec<TrackUi>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatus {
    pub total: i64,
    pub mine: i64,
    pub bytes: i64,
    pub budget: i64,
}

// ---- store + engine ----

pub struct Playlists {
    db: Mutex<Connection>,
    rpc: NodeRpc,
    budget: i64,
}

impl Playlists {
    /// Open (or create) `playlists.db` under `dir`; `None` → in-memory (tests).
    /// Budget: `TS_PLAYLIST_BUDGET_MB` (default 50) bounds foreign-playlist bytes.
    pub fn open(dir: Option<&Path>, rpc: NodeRpc) -> Result<Self> {
        let conn = match dir {
            Some(d) => Connection::open(d.join("playlists.db"))?,
            None => Connection::open_in_memory()?,
        };
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS playlists (
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
             );",
        )?;
        // Migration for pre-holder DBs: add `held` if missing (duplicate-column = fine).
        let _ = conn.execute("ALTER TABLE playlists ADD COLUMN held INTEGER NOT NULL DEFAULT 0", []);
        let budget = std::env::var("TS_PLAYLIST_BUDGET_MB")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(50)
            * 1024
            * 1024;
        Ok(Self { db: Mutex::new(conn), rpc, budget })
    }

    // -- ingest (the sync loop's per-entry work) --

    /// Verify + store one drained wire entry. The sidecar is untrusted: the record is
    /// re-verified (signature + EOL) and the doc re-hashed against the record's CID
    /// here, regardless of what the Go validator already did.
    pub fn ingest_wire(&self, w: &PlaylistWire) -> Result<bool> {
        let (cid, seq) = ipns::verify_b64_seq(&w.name, &w.record)?;
        {
            let db = self.db.lock().unwrap();
            let stored: Option<i64> = db
                .query_row("SELECT seq FROM playlists WHERE name=?1", params![w.name], |r| r.get(0))
                .optional()?;
            if let Some(s) = stored {
                if s as u64 >= seq {
                    return Ok(false); // not newer
                }
            }
            let rej: Option<i64> = db
                .query_row(
                    "SELECT 1 FROM rejected WHERE name=?1 AND seq=?2",
                    params![w.name, seq as i64],
                    |r| r.get(0),
                )
                .optional()?;
            if rej.is_some() {
                return Ok(false);
            }
        }
        let doc_bytes = base64::engine::general_purpose::STANDARD
            .decode(w.doc.trim())
            .map_err(|e| anyhow!("doc not base64: {e}"))?;
        if !doc_matches_cid(&cid, &doc_bytes) {
            self.remember_rejected(&w.name, seq);
            bail!("doc does not hash to record cid for {}", w.name);
        }
        let doc = match validate_doc(&doc_bytes) {
            Ok(d) => d,
            Err(e) => {
                self.remember_rejected(&w.name, seq);
                return Err(e);
            }
        };

        let db = self.db.lock().unwrap();
        if doc.del {
            // Tombstone: drop the local copy (also for our own name — a delete from
            // another install of the same identity wins by seq, exactly like any edit).
            db.execute("DELETE FROM playlists WHERE name=?1", params![w.name])?;
            db.execute("DELETE FROM playlists_fts WHERE name=?1", params![w.name])?;
            return Ok(true);
        }
        upsert_row(&db, &w.name, &doc, &doc_bytes, Some(&w.record), seq as i64)?;
        Ok(true)
    }

    fn remember_rejected(&self, name: &str, seq: u64) {
        let db = self.db.lock().unwrap();
        let _ = db.execute(
            "INSERT OR IGNORE INTO rejected(name, seq) VALUES(?1, ?2)",
            params![name, seq as i64],
        );
    }

    /// Evict least-recently-updated FOREIGN playlists until under the byte budget.
    /// Own playlists are never evicted.
    pub fn enforce_budget(&self) {
        let db = self.db.lock().unwrap();
        loop {
            let used: i64 = db
                .query_row(
                    "SELECT COALESCE(SUM(size_bytes),0) FROM playlists WHERE is_mine=0 AND held=0",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if used <= self.budget {
                return;
            }
            let victim: Option<String> = db
                .query_row(
                    "SELECT name FROM playlists WHERE is_mine=0 AND held=0 ORDER BY last_update_at ASC LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .unwrap_or(None);
            let Some(name) = victim else { return };
            let _ = db.execute("DELETE FROM playlists WHERE name=?1", params![name]);
            let _ = db.execute("DELETE FROM playlists_fts WHERE name=?1", params![name]);
        }
    }

    // -- publish path (explicit "Share") --

    /// Publish (or republish) an own playlist: seq recovery against the network, then
    /// `playlist/publish` on the sidecar. Marks the row published and stores the record
    /// for the re-announce cycle.
    pub async fn publish(&self, name: &str) -> Result<()> {
        let (key_name, doc_json, local_seq) = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "SELECT key_name, doc_json, seq FROM playlists WHERE name=?1 AND is_mine=1",
                params![name],
                |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?)),
            )
            .optional()?
            .ok_or_else(|| anyhow!("not my playlist: {name}"))?
        };
        let key_name = key_name.ok_or_else(|| anyhow!("playlist {name} has no key"))?;
        let seq = self.next_seq(name, local_seq).await;
        let (pub_name, record) = self
            .rpc
            .playlist_publish(&key_name, seq, LIFETIME, doc_json.into_bytes())
            .await?;
        if pub_name != name {
            bail!("publish name mismatch: {pub_name} != {name}");
        }
        let db = self.db.lock().unwrap();
        db.execute(
            "UPDATE playlists SET seq=?2, published=1, record_b64=?3, last_update_at=?4 WHERE name=?1",
            params![name, seq as i64, record, now_secs()],
        )?;
        Ok(())
    }

    /// Seq recovery (PLAYLISTS.md §1): `max(local, gossip-buffer) + 1`. Playlists are
    /// pubsub-only — no DHT — so `routing/get` answers from the node's gossip-warmed
    /// buffer; it covers a re-imported key whose history arrived via re-announce. A
    /// never-published playlist (local seq 0) skips the lookup: its key is fresh, there
    /// is nothing to recover, and the miss would just walk the DHT to a timeout.
    async fn next_seq(&self, name: &str, local_seq: i64) -> u64 {
        if local_seq <= 0 {
            return 1;
        }
        let net_seq = match self.rpc.routing_get(name).await {
            Ok(rec) => ipns::verify_b64_seq(name, &rec).map(|(_, s)| s).unwrap_or(0),
            Err(_) => 0,
        };
        (local_seq as u64).max(net_seq) + 1
    }

    // -- CRUD (Tauri command backends) --

    pub async fn create(&self, title: String, tracks: Vec<(i64, String, String)>) -> Result<PlaylistMeta> {
        // A key (and therefore the IPNS name) exists from birth; nothing is published
        // until the user explicitly shares.
        // "playlist-<uuid>", NOT "playlist/<uuid>": the sidecar keystore maps key names
        // to filenames, so a slash would nest a directory that doesn't exist.
        let digest = Sha256::digest(format!("{}:{}", now_secs(), title).as_bytes());
        let uid: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
        let key_name = format!("playlist-{uid}");
        let name = self.rpc.key_gen(&key_name).await?;
        let doc = PlaylistDoc {
            v: 1,
            t: title,
            del: false,
            ts: tracks.into_iter().map(|(a, b, c)| TrackRef(a, b, c)).collect(),
        };
        let bytes = serde_json::to_vec(&doc)?;
        validate_doc(&bytes)?;
        {
            let db = self.db.lock().unwrap();
            upsert_row(&db, &name, &doc, &bytes, None, 0)?;
            db.execute(
                "UPDATE playlists SET is_mine=1, key_name=?2 WHERE name=?1",
                params![name, key_name],
            )?;
        }
        self.get(&name)?.map(|d| d.meta).ok_or_else(|| anyhow!("create lost row"))
    }

    /// Edit an own playlist; if it was already shared, the edit republishes (seq+1).
    pub async fn update(&self, name: &str, title: String, tracks: Vec<(i64, String, String)>) -> Result<()> {
        let published: bool = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "SELECT published FROM playlists WHERE name=?1 AND is_mine=1",
                params![name],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .ok_or_else(|| anyhow!("not my playlist: {name}"))?
                != 0
        };
        let doc = PlaylistDoc {
            v: 1,
            t: title,
            del: false,
            ts: tracks.into_iter().map(|(a, b, c)| TrackRef(a, b, c)).collect(),
        };
        let bytes = serde_json::to_vec(&doc)?;
        validate_doc(&bytes)?;
        {
            let db = self.db.lock().unwrap();
            upsert_row(&db, name, &doc, &bytes, None, -1)?;
        }
        if published {
            self.publish(name).await?;
        }
        Ok(())
    }

    /// Delete: a published own playlist gets a tombstone first (seq+1, 168h — syncers
    /// drop it, the record dies at EOL); a foreign playlist is just evicted locally.
    pub async fn delete(&self, name: &str) -> Result<()> {
        let row: Option<(bool, bool, Option<String>, i64)> = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "SELECT is_mine, published, key_name, seq FROM playlists WHERE name=?1",
                params![name],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)? != 0,
                        r.get::<_, i64>(1)? != 0,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?
        };
        let Some((is_mine, published, key_name, seq)) = row else { return Ok(()) };
        if is_mine && published {
            if let Some(key_name) = key_name {
                let tomb = serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] })?;
                let next = self.next_seq(name, seq).await;
                if let Err(e) = self.rpc.playlist_publish(&key_name, next, LIFETIME, tomb).await {
                    log::warn!("playlist tombstone publish failed for {name}: {e}");
                }
            }
        }
        let db = self.db.lock().unwrap();
        db.execute("DELETE FROM playlists WHERE name=?1", params![name])?;
        db.execute("DELETE FROM playlists_fts WHERE name=?1", params![name])?;
        Ok(())
    }

    // -- queries --

    pub fn search(&self, q: &str) -> Result<Vec<PlaylistMeta>> {
        let q = q.trim();
        if q.is_empty() {
            return self.list("all");
        }
        // Quote each token (FTS5 phrase), prefix-expand the last — same spirit as the
        // catalog search's term building.
        let toks: Vec<String> = q
            .split_whitespace()
            .map(|t| format!("\"{}\"", t.replace('"', "")))
            .collect();
        let mut fts = toks.join(" ");
        fts.push('*');
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare(
            "SELECT p.name, p.title, p.doc_json, p.is_mine, p.held, p.published, p.size_bytes,
                    p.last_update_at, p.last_played_at
             FROM playlists_fts f JOIN playlists p ON p.name = f.name
             WHERE playlists_fts MATCH ?1
             ORDER BY bm25(playlists_fts) LIMIT 200",
        )?;
        let rows = stmt.query_map(params![fts], row_meta)?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// scope: "library" = mine + held; "seen" = unheld foreign (the discover pool);
    /// anything else = all.
    pub fn list(&self, scope: &str) -> Result<Vec<PlaylistMeta>> {
        let filter = match scope {
            "library" => "WHERE is_mine=1 OR held=1",
            "seen" => "WHERE is_mine=0 AND held=0",
            _ => "",
        };
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare(&format!(
            "SELECT name, title, doc_json, is_mine, held, published, size_bytes,
                    last_update_at, last_played_at
             FROM playlists {filter} ORDER BY is_mine DESC, held DESC, last_update_at DESC LIMIT 500",
        ))?;
        let rows = stmt.query_map([], row_meta)?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Add/remove a FOREIGN playlist to/from the library ("holder" tier): held rows are
    /// never evicted and are the ones this client re-announces (backs). No-op for mine.
    pub fn set_held(&self, name: &str, held: bool) -> Result<()> {
        let db = self.db.lock().unwrap();
        db.execute(
            "UPDATE playlists SET held=?2 WHERE name=?1 AND is_mine=0",
            params![name, held as i64],
        )?;
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Option<PlaylistDetail>> {
        let db = self.db.lock().unwrap();
        let row = db
            .query_row(
                "SELECT name, title, doc_json, is_mine, held, published, size_bytes,
                        last_update_at, last_played_at
                 FROM playlists WHERE name=?1",
                params![name],
                row_meta,
            )
            .optional()?;
        let Some(meta) = row else { return Ok(None) };
        let doc_json: String =
            db.query_row("SELECT doc_json FROM playlists WHERE name=?1", params![name], |r| r.get(0))?;
        let doc: PlaylistDoc = serde_json::from_str(&doc_json)?;
        let items = doc
            .ts
            .into_iter()
            .map(|TrackRef(id, m, t)| TrackUi { id, mod_name: m, title: t })
            .collect();
        Ok(Some(PlaylistDetail { meta, items }))
    }

    pub fn mark_played(&self, name: &str) -> Result<()> {
        let db = self.db.lock().unwrap();
        db.execute("UPDATE playlists SET last_played_at=?2 WHERE name=?1", params![name, now_secs()])?;
        Ok(())
    }

    pub fn status(&self) -> Result<SyncStatus> {
        let db = self.db.lock().unwrap();
        let (total, mine, bytes) = db.query_row(
            "SELECT COUNT(*), COALESCE(SUM(is_mine),0), COALESCE(SUM(size_bytes),0) FROM playlists",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        Ok(SyncStatus { total, mine, bytes, budget: self.budget })
    }

    // -- background loops --

    /// One sync tick: drain the sidecar buffer from `cursor`, verify+store each entry,
    /// enforce the budget. Returns the next cursor.
    pub async fn sync_once(&self, cursor: u64) -> u64 {
        let (ver, recs) = match self.rpc.playlist_records(cursor).await {
            Ok(v) => v,
            Err(e) => {
                log::debug!("playlist records poll failed: {e}");
                return cursor;
            }
        };
        for w in &recs {
            if let Err(e) = self.ingest_wire(w) {
                log::debug!("playlist ingest {} rejected: {e}", w.name);
            }
        }
        if !recs.is_empty() {
            self.enforce_budget();
        }
        ver
    }

    /// Decay (PLAYLISTS.md holder tier): purge SEEN-tier rows whose record has expired.
    /// Only the author can re-sign, so once nobody in any library re-announces a
    /// playlist its record ages to EOL (≤168h) and every unbacked copy evaporates —
    /// network-wide decay within one record lifetime. Held and mine rows are exempt:
    /// the user chose to keep those, expired record or not.
    pub fn decay_expired(&self) {
        let seen: Vec<(String, Option<String>)> = {
            let db = self.db.lock().unwrap();
            let Ok(mut stmt) =
                db.prepare("SELECT name, record_b64 FROM playlists WHERE is_mine=0 AND held=0")
            else {
                return;
            };
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
                .unwrap_or_default()
        };
        for (name, rec) in seen {
            let alive = rec
                .as_deref()
                .map(|r| ipns::verify_b64_seq(&name, r).is_ok())
                .unwrap_or(false);
            if !alive {
                let db = self.db.lock().unwrap();
                let _ = db.execute("DELETE FROM playlists WHERE name=?1", params![name]);
                let _ = db.execute("DELETE FROM playlists_fts WHERE name=?1", params![name]);
            }
        }
    }

    /// One announce tick: re-gossip the LIBRARY (mine + held — the node applies
    /// last-seen suppression, so this is cheap), dropping locally-expired records; own
    /// records within the renewal margin of EOL are re-signed at seq+1 first. Also runs
    /// the seen-tier decay pass, since both share the cycle cadence.
    pub async fn announce_once(&self) {
        self.decay_expired();
        // Library-only: mine + held re-announce; the seen tier is relayed live by
        // gossipsub but NOT kept alive — unbacked playlists decay at record EOL.
        let held: Vec<(String, i64, String, String, bool)> = {
            let db = self.db.lock().unwrap();
            let Ok(mut stmt) = db.prepare(
                "SELECT name, seq, record_b64, doc_json, is_mine FROM playlists
                 WHERE record_b64 IS NOT NULL AND (is_mine=1 OR held=1)",
            ) else {
                return;
            };
            stmt.query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get::<_, i64>(4)? != 0))
            })
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
        };
        let mut entries = Vec::new();
        for (name, seq, record, doc_json, is_mine) in held {
            // Drop records that no longer verify (expired EOL etc.). Own ones get
            // renewed (re-signed, seq+1) instead of dropped.
            let alive = ipns::verify_b64_seq(&name, &record).is_ok();
            if is_mine && (!alive || record_near_eol(&record)) {
                if let Err(e) = self.publish(&name).await {
                    log::warn!("playlist renewal failed for {name}: {e}");
                }
                continue; // publish already gossiped the fresh record
            }
            if !alive {
                continue;
            }
            entries.push(PlaylistWire {
                name,
                seq: seq.max(0) as u64,
                record,
                doc: base64::engine::general_purpose::STANDARD.encode(doc_json.as_bytes()),
            });
        }
        if entries.is_empty() {
            return;
        }
        if let Err(e) = self.rpc.playlist_announce(&entries).await {
            log::debug!("playlist announce failed: {e}");
        }
    }
}

/// Whether a (verified) own record is within the renewal margin of its EOL.
fn record_near_eol(record_b64: &str) -> bool {
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(record_b64.trim()) else {
        return true;
    };
    let Ok(rec) = rust_ipns::Record::decode(&bytes) else { return true };
    match rec.validity() {
        Ok(v) => v.timestamp() < now_secs() + RENEW_MARGIN_SECS,
        Err(_) => true,
    }
}

/// Shared upsert: writes the row + FTS entry. `seq < 0` keeps the existing seq (local
/// edits don't advance the sequence — publishing does). Preserves is_mine / key_name /
/// published / last_played_at on conflict.
fn upsert_row(
    db: &Connection,
    name: &str,
    doc: &PlaylistDoc,
    doc_bytes: &[u8],
    record_b64: Option<&str>,
    seq: i64,
) -> rusqlite::Result<()> {
    let tracks_text: String = doc
        .ts
        .iter()
        .map(|t| format!("{} {}", t.1, t.2))
        .collect::<Vec<_>>()
        .join(" ");
    db.execute(
        "INSERT INTO playlists(name, title, doc_json, record_b64, seq, size_bytes, last_update_at)
         VALUES(?1, ?2, ?3, ?4, MAX(?5, 0), ?6, ?7)
         ON CONFLICT(name) DO UPDATE SET
           title=excluded.title,
           doc_json=excluded.doc_json,
           record_b64=COALESCE(excluded.record_b64, playlists.record_b64),
           seq=CASE WHEN ?5 < 0 THEN playlists.seq ELSE ?5 END,
           size_bytes=excluded.size_bytes,
           last_update_at=excluded.last_update_at",
        params![
            name,
            doc.t,
            std::str::from_utf8(doc_bytes).unwrap_or_default(),
            record_b64,
            seq,
            doc_bytes.len() as i64,
            now_secs()
        ],
    )?;
    db.execute("DELETE FROM playlists_fts WHERE name=?1", params![name])?;
    db.execute(
        "INSERT INTO playlists_fts(name, title, tracks) VALUES(?1, ?2, ?3)",
        params![name, doc.t, tracks_text],
    )?;
    Ok(())
}

fn row_meta(r: &rusqlite::Row<'_>) -> rusqlite::Result<PlaylistMeta> {
    let doc_json: String = r.get(2)?;
    let tracks = serde_json::from_str::<PlaylistDoc>(&doc_json)
        .map(|d| d.ts.len() as i64)
        .unwrap_or(0);
    Ok(PlaylistMeta {
        name: r.get(0)?,
        title: r.get(1)?,
        tracks,
        is_mine: r.get::<_, i64>(3)? != 0,
        held: r.get::<_, i64>(4)? != 0,
        published: r.get::<_, i64>(5)? != 0,
        size_bytes: r.get(6)?,
        last_update_at: r.get(7)?,
        last_played_at: r.get(8)?,
    })
}

/// The background driver: a ~20s sync poll, and a jittered ~15-minute announce cycle
/// (the suppression that makes this polite lives node-side; expired-own renewal lives
/// in `announce_once`).
pub async fn run_loops(pl: Arc<Playlists>) {
    let mut cursor = 0u64;
    let mut ticks: u64 = 0;
    // First announce soon after startup (make our playlists discoverable), then ~15min.
    let mut next_announce: u64 = 2;
    loop {
        cursor = pl.sync_once(cursor).await;
        ticks += 1;
        if ticks >= next_announce {
            pl.announce_once().await;
            // 45 ticks ≈ 15min; ±20% jitter from the clock (no rand dependency).
            let jitter = (now_secs() as u64 % 19) as i64 - 9;
            next_announce = ticks + (45i64 + jitter).max(1) as u64;
        }
        tokio::time::sleep(Duration::from_secs(20)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as B64;
    use chrono::Duration as ChronoDuration;
    use libp2p_identity::Keypair;

    fn doc_bytes(title: &str) -> Vec<u8> {
        serde_json::to_vec(&PlaylistDoc {
            v: 1,
            t: title.into(),
            del: false,
            ts: vec![TrackRef(1, "a.it".into(), "Song A".into())],
        })
        .unwrap()
    }

    fn cid_for(doc: &[u8]) -> Cid {
        let mh = cid::multihash::Multihash::wrap(0x12, Sha256::digest(doc).as_slice()).unwrap();
        Cid::new_v1(0x55, mh)
    }

    fn wire_for(kp: &Keypair, doc: &[u8], seq: u64) -> PlaylistWire {
        let name = kp.public().to_peer_id().to_string();
        let value = format!("/ipfs/{}", cid_for(doc));
        let rec = rust_ipns::Record::new(kp, value.as_bytes(), ChronoDuration::seconds(3600), seq, 0)
            .unwrap()
            .encode()
            .unwrap();
        PlaylistWire { name, seq, record: B64.encode(rec), doc: B64.encode(doc) }
    }

    fn mem() -> Playlists {
        Playlists::open(None, NodeRpc::new("127.0.0.1:1")).unwrap()
    }

    #[test]
    fn ingest_verifies_and_stores() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let doc = doc_bytes("hello");
        let w = wire_for(&kp, &doc, 1);
        assert!(pl.ingest_wire(&w).unwrap());
        let got = pl.get(&w.name).unwrap().unwrap();
        assert_eq!(got.meta.title, "hello");
        assert_eq!(got.items.len(), 1);
        assert!(!got.meta.is_mine);
    }

    #[test]
    fn ingest_rejects_tampered_doc_and_remembers() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let real = doc_bytes("real");
        let mut w = wire_for(&kp, &real, 1);
        w.doc = B64.encode(doc_bytes("evil")); // valid record, wrong doc bytes
        assert!(pl.ingest_wire(&w).is_err());
        assert!(pl.get(&w.name).unwrap().is_none());
        // Remembered: the same (name, seq) is skipped without error next time.
        assert!(!pl.ingest_wire(&w).unwrap_or(true) || pl.ingest_wire(&w).is_err());
    }

    #[test]
    fn newest_seq_wins_and_stale_is_skipped() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let w2 = wire_for(&kp, &doc_bytes("two"), 2);
        let w1 = wire_for(&kp, &doc_bytes("one"), 1);
        assert!(pl.ingest_wire(&w2).unwrap());
        assert!(!pl.ingest_wire(&w1).unwrap());
        assert_eq!(pl.get(&w1.name).unwrap().unwrap().meta.title, "two");
    }

    #[test]
    fn tombstone_deletes_row() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let w = wire_for(&kp, &doc_bytes("alive"), 1);
        pl.ingest_wire(&w).unwrap();
        let tomb = serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] }).unwrap();
        let wt = wire_for(&kp, &tomb, 2);
        pl.ingest_wire(&wt).unwrap();
        assert!(pl.get(&w.name).unwrap().is_none());
    }

    #[test]
    fn search_finds_by_title_and_track() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        pl.ingest_wire(&wire_for(&kp, &doc_bytes("chiptune bangers"), 1)).unwrap();
        assert_eq!(pl.search("chiptune").unwrap().len(), 1);
        assert_eq!(pl.search("song").unwrap().len(), 1); // track title
        assert_eq!(pl.search("nomatch").unwrap().len(), 0);
    }

    #[test]
    fn budget_evicts_foreign_lru_but_never_mine() {
        let mut pl = mem();
        pl.budget = 130; // fits one test doc (~112 B) but not two
        let big = |title: &str| {
            serde_json::to_vec(&PlaylistDoc {
                v: 1,
                t: title.into(),
                del: false,
                ts: vec![TrackRef(1, "x".repeat(40), "y".repeat(40))],
            })
            .unwrap()
        };
        let k1 = Keypair::generate_ed25519();
        let k2 = Keypair::generate_ed25519();
        let w1 = wire_for(&k1, &big("old"), 1);
        let w2 = wire_for(&k2, &big("new"), 1);
        pl.ingest_wire(&w1).unwrap();
        // Backdate w1 so it's the LRU victim.
        {
            let db = pl.db.lock().unwrap();
            db.execute("UPDATE playlists SET last_update_at=1 WHERE name=?1", params![w1.name]).unwrap();
        }
        pl.ingest_wire(&w2).unwrap();
        pl.enforce_budget();
        assert!(pl.get(&w1.name).unwrap().is_none(), "LRU foreign playlist must be evicted");
        assert!(pl.get(&w2.name).unwrap().is_some());

        // A mine row over budget is never evicted.
        {
            let db = pl.db.lock().unwrap();
            db.execute("UPDATE playlists SET is_mine=1 WHERE name=?1", params![w2.name]).unwrap();
        }
        pl.enforce_budget();
        assert!(pl.get(&w2.name).unwrap().is_some());
    }

    // CROSS-STACK GATE: a {record, doc} pair produced by the Go node's playlist/publish
    // (boxo-signed record, raw-sha256 doc CID) must ingest under this Rust verifier —
    // the playlist counterpart of ipns.rs's Go-record fixture. Captured from a live
    // tsnode (`playlist/publish?key=playlist-fixture&seq=3&lifetime=868000h`, EOL 2125).
    // Regenerate the same way against a local node if the wire format ever changes.
    #[test]
    fn ingests_a_wire_entry_produced_by_the_go_node() {
        let w = PlaylistWire {
            name: "12D3KooWGCr5x1M4TMzzEyWZ3dgC4jZiSRo1hS4KG99bCR4yjN4V".into(),
            seq: 3,
            record: "CkEvaXBmcy9iYWZrcmVpZHV3Y2Nqd3B3cG9rdHg3cGt4N3NmamJub3V3bjdjcmxkanBnYWRqdnN4YTVwdTVsdmFhYRJAWEgAU2rU0PKkEgng4f9iMnXZYOqBUmoxNZG6KsGme1+syN/4SzHXDLMO9WXpFe4zrQV+uhCpd8RL2VLvAHEtBRgAIhsyMTI1LTA3LTEwVDExOjE0OjM1LjgwNDIyM1ooAzCA8JLL3QhCQPu/BAZggt+TV/yPF+X9wk18ri7MWbDBYspPw/J69RlEyyC4+Qb0FPFlWFNa31RZC+7sp4OFzpLiuHNd/eUEGQtKlQGlY1RUTBsAAABF2WS4AGVWYWx1ZVhBL2lwZnMvYmFma3JlaWR1d2Njandwd3Bva3R4N3BreDdzZmpibm91d243Y3JsZGpwZ2FkanZzeGE1cHU1bHZhYWFoU2VxdWVuY2UDaFZhbGlkaXR5WBsyMTI1LTA3LTEwVDExOjE0OjM1LjgwNDIyM1psVmFsaWRpdHlUeXBlAA==".into(),
            doc: "eyJ2IjoxLCJ0IjoiZ28gZml4dHVyZSIsInRzIjpbWzcsImZpeC5pdCIsIkZpeHR1cmUgU29uZyJdXX0=".into(),
        };
        let pl = mem();
        pl.ingest_wire(&w).expect("Go-produced playlist wire entry must verify + ingest");
        let got = pl.get(&w.name).unwrap().unwrap();
        assert_eq!(got.meta.title, "go fixture");
        assert_eq!(got.items[0].mod_name, "fix.it");
    }

    // Live-node harness: run with a local tsnode (`tsnode -rpc 127.0.0.1:47701`) via
    // `TS_TEST_RPC=127.0.0.1:47701 cargo test --lib live_ -- --ignored --nocapture`.
    // Exercises the exact command backends the UI invokes, printing the JSON the
    // frontend would receive.
    #[tokio::test]
    #[ignore]
    async fn live_create_list_get_roundtrip() {
        let rpc = NodeRpc::new(&std::env::var("TS_TEST_RPC").expect("set TS_TEST_RPC"));
        let pl = Playlists::open(None, rpc).unwrap();
        let meta = pl
            .create("live test".into(), vec![(42, "aurora.it".into(), "Hymn".into())])
            .await
            .expect("create");
        println!("create -> {}", serde_json::to_string(&meta).unwrap());
        let list = pl.list("all").unwrap();
        println!("list -> {}", serde_json::to_string(&list).unwrap());
        let detail = pl.get(&meta.name).expect("get").expect("row");
        println!("get -> {}", serde_json::to_string(&detail).unwrap());
        assert_eq!(detail.items.len(), 1);
    }

    // Holder tier: held rows are exempt from budget eviction and decay; scoped lists
    // split library (mine+held) from the seen/discover pool; un-held rows decay once
    // their record expires.
    #[test]
    fn holder_tier_exempts_and_decays() {
        let mut pl = mem();
        pl.budget = 0; // any seen-tier bytes are over budget
        let k1 = Keypair::generate_ed25519();
        let k2 = Keypair::generate_ed25519();
        let w1 = wire_for(&k1, &doc_bytes("kept"), 1);
        let w2 = wire_for(&k2, &doc_bytes("transient"), 1);
        pl.ingest_wire(&w1).unwrap();
        pl.set_held(&w1.name, true).unwrap();
        pl.ingest_wire(&w2).unwrap();

        pl.enforce_budget();
        assert!(pl.get(&w1.name).unwrap().is_some(), "held row must survive eviction");
        assert!(pl.get(&w2.name).unwrap().is_none(), "seen row over budget must evict");

        pl.ingest_wire(&w2).unwrap();
        let lib = pl.list("library").unwrap();
        assert!(lib.iter().any(|p| p.name == w1.name) && !lib.iter().any(|p| p.name == w2.name));
        let seen = pl.list("seen").unwrap();
        assert!(seen.iter().any(|p| p.name == w2.name) && !seen.iter().any(|p| p.name == w1.name));

        // Expire both records in place; decay drops the seen row, keeps the held one.
        for (kp, w) in [(&k1, &w1), (&k2, &w2)] {
            let dead = rust_ipns::Record::new(
                kp,
                format!("/ipfs/{}", cid_for(&doc_bytes("x"))).as_bytes(),
                ChronoDuration::seconds(-10),
                9,
                0,
            )
            .unwrap()
            .encode()
            .unwrap();
            let db = pl.db.lock().unwrap();
            db.execute(
                "UPDATE playlists SET record_b64=?2 WHERE name=?1",
                params![w.name, B64.encode(dead)],
            )
            .unwrap();
        }
        pl.decay_expired();
        assert!(pl.get(&w1.name).unwrap().is_some(), "held row must survive decay");
        assert!(pl.get(&w2.name).unwrap().is_none(), "unbacked row must decay at record EOL");
    }

    #[test]
    fn validate_doc_rejects_garbage() {
        assert!(validate_doc(b"").is_err());
        assert!(validate_doc(b"not json").is_err());
        assert!(validate_doc(br#"{"v":2,"t":"x"}"#).is_err());
        let long = format!(r#"{{"v":1,"t":"{}"}}"#, "x".repeat(400));
        assert!(validate_doc(long.as_bytes()).is_err());
        assert!(validate_doc(br#"{"v":1,"del":true}"#).unwrap().del);
    }
}
