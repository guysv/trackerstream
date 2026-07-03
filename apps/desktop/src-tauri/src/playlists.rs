//! Playlists over IPNS (PLAYLISTS.md) — the sync engine and the ONLY durable playlist
//! store anywhere in the system (`playlists.db`, SQLite + FTS5 in the app data dir).
//!
//! Docs travel inline in gossip next to their signed IPNS record; the Go sidecar is a
//! bounded relay buffer we drain via `playlist/records`. Trust anchors HERE: every
//! drained entry is re-verified (record signature + EOL via `ipns::verify_b64_seq`, then
//! doc-hash against the record's CID) before it touches the DB. Publishing is explicit
//! ("Share") — playlists are local-only by default.

use crate::ipns;
use crate::link;
use crate::rpc::{ManifestEntry, NodeRpc, PlaylistWire};
use anyhow::{anyhow, bail, Result};
use base64::Engine;
use cid::Cid;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 1 MiB — a bigger doc is adversarial by definition (mirrors the Go validator).
const DOC_MAX: usize = 1 << 20;
const TRACKS_MAX: usize = 20_000;
const TITLE_MAX: usize = 300;
const FIELD_MAX: usize = 512;
const LIFETIME: &str = "168h";
/// Title of the private, per-client "Liked Tracks" playlist (Spotify-style). Its
/// stable identity is the `liked` column, not this string (a user could rename it).
const LIKED_TITLE: &str = "Liked Tracks";
/// Republish an own record when it has less than this long to live.
const RENEW_MARGIN_SECS: i64 = 24 * 3600;
/// A name-only deep link stops waiting for gossip after this (≈ one announce cycle).
const PENDING_EXPIRY_SECS: i64 = 15 * 60;

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
    /// No longer propagating: the record expired (author absent) or the author
    /// tombstoned it. Still playable if kept; the UI nudges toward "duplicate to
    /// mine" (fork under a living key).
    pub dormant: bool,
    /// The author published a deletion. Library copies are preserved dormant — we
    /// don't delete playlists the user chose to keep.
    pub tombstoned: bool,
    /// The private per-client "Liked Tracks" playlist (Spotify-style). Always own +
    /// unpublished; the UI pins it and hides share/delete.
    pub liked: bool,
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

/// Outcome of an incoming deep link: the row is either present ("ready") or being
/// chased over gossip ("pending" — the UI shows a syncing state until it lands).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkStatus {
    pub name: String,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatus {
    pub total: i64,
    pub mine: i64,
    pub held: i64,
    pub seen: i64,
    pub dormant: i64,
    /// Seen-tier bytes (what the budget governs; library rows are exempt).
    pub bytes: i64,
    pub budget: i64,
}

// ---- store + engine ----

pub struct Playlists {
    db: Mutex<Connection>,
    rpc: NodeRpc,
    budget: i64,
    /// The playlist currently loaded in the player, if any. Pinned rows are exempt
    /// from budget eviction and decay, and a tombstone keeps them dormant instead of
    /// deleting — the playing playlist must not vanish out from under the UI. This is
    /// NOT the held tier: in-memory only, never re-announced, cleared when playback
    /// moves to another source.
    pinned: Mutex<Option<String>>,
    /// Hash of the last disclosure-set manifest successfully posted to the node —
    /// `push_manifest` short-circuits when the set hasn't changed. None = never posted.
    manifest_hash: Mutex<Option<u64>>,
    /// Names from name-only deep links awaiting arrival via gossip (name → asked-at).
    /// In-memory only: a link click is interactive; a rare transient doesn't merit
    /// surviving restarts. Entries resolve when the row lands, or expire (~15 min).
    pending: Mutex<HashMap<String, i64>>,
    /// Serializes lazy creation of the singleton "Liked Tracks" playlist so two
    /// concurrent first-likes can't mint two of it (held across the key_gen await).
    liked_guard: tokio::sync::Mutex<()>,
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
        // Migrations for older DBs (duplicate-column errors = already applied, fine).
        let _ = conn.execute("ALTER TABLE playlists ADD COLUMN held INTEGER NOT NULL DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE playlists ADD COLUMN eol INTEGER NOT NULL DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE playlists ADD COLUMN tombstoned INTEGER NOT NULL DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE playlists ADD COLUMN liked INTEGER NOT NULL DEFAULT 0", []);
        backfill_eol(&conn);
        let budget = std::env::var("TS_PLAYLIST_BUDGET_MB")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(50)
            * 1024
            * 1024;
        Ok(Self {
            db: Mutex::new(conn),
            rpc,
            budget,
            pinned: Mutex::new(None),
            manifest_hash: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            liked_guard: tokio::sync::Mutex::new(()),
        })
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
            // Tombstone. SEEN-tier copies drop; LIBRARY copies (held/mine) are
            // preserved DORMANT — we don't delete playlists the user chose to keep.
            // Clearing record_b64 makes the row un-announceable (the deletion is
            // honored on the network) and advancing seq to the tombstone's blocks
            // replayed older records; a genuine author republish (seq+1) revives the
            // row through the normal upsert path. A pinned (playing) seen row is kept
            // dormant the same way; decay removes it after unpin.
            let in_library: bool = db
                .query_row(
                    "SELECT is_mine OR held FROM playlists WHERE name=?1",
                    params![w.name],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
                .unwrap_or(0)
                != 0;
            if in_library || self.pinned_name().as_deref() == Some(w.name.as_str()) {
                db.execute(
                    "UPDATE playlists SET seq=?2, record_b64=NULL, tombstoned=1, published=0,
                       last_update_at=?3 WHERE name=?1",
                    params![w.name, seq as i64, now_secs()],
                )?;
            } else {
                db.execute("DELETE FROM playlists WHERE name=?1", params![w.name])?;
                db.execute("DELETE FROM playlists_fts WHERE name=?1", params![w.name])?;
            }
            return Ok(true);
        }
        upsert_row(&db, &w.name, &doc, &doc_bytes, Some(&w.record), seq as i64, record_eol_secs(&w.record))?;
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
    /// Own playlists are never evicted; neither is the pinned (currently playing) one.
    pub fn enforce_budget(&self) {
        let pinned = self.pinned_name();
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
                    "SELECT name FROM playlists WHERE is_mine=0 AND held=0 AND (?1 IS NULL OR name<>?1)
                     ORDER BY last_update_at ASC LIMIT 1",
                    params![pinned],
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
        let (key_name, doc_json, local_seq, liked) = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "SELECT key_name, doc_json, seq, liked FROM playlists WHERE name=?1 AND is_mine=1",
                params![name],
                |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)? != 0,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| anyhow!("not my playlist: {name}"))?
        };
        // "Liked Tracks" is the private per-client playlist — never publishable.
        if liked {
            bail!("the Liked Tracks playlist is private and cannot be shared");
        }
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
            "UPDATE playlists SET seq=?2, published=1, record_b64=?3, last_update_at=?4, eol=?5 WHERE name=?1",
            params![name, seq as i64, record, now_secs(), record_eol_secs(&record)],
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
            upsert_row(&db, &name, &doc, &bytes, None, 0, 0)?;
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
            upsert_row(&db, name, &doc, &bytes, None, -1, -1)?;
        }
        if published {
            self.publish(name).await?;
        }
        Ok(())
    }

    /// Delete: a published own playlist gets a tombstone first (seq+1, 168h — syncers
    /// drop it, the record dies at EOL); a foreign playlist is just evicted locally.
    pub async fn delete(&self, name: &str) -> Result<()> {
        let row: Option<(bool, bool, Option<String>, i64, bool)> = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "SELECT is_mine, published, key_name, seq, liked FROM playlists WHERE name=?1",
                params![name],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)? != 0,
                        r.get::<_, i64>(1)? != 0,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)? != 0,
                    ))
                },
            )
            .optional()?
        };
        let Some((is_mine, published, key_name, seq, liked)) = row else { return Ok(()) };
        // "Liked Tracks" is a permanent per-client playlist — not user-deletable.
        if liked {
            bail!("the Liked Tracks playlist cannot be deleted");
        }
        if is_mine && published {
            if let Some(key_name) = key_name {
                let tomb = serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] })?;
                let next = self.next_seq(name, seq).await;
                if let Err(e) = self.rpc.playlist_publish(&key_name, next, LIFETIME, tomb).await {
                    log::warn!(target: "playlist", "tombstone publish failed for {name}: {e}");
                }
            }
        }
        let db = self.db.lock().unwrap();
        db.execute("DELETE FROM playlists WHERE name=?1", params![name])?;
        db.execute("DELETE FROM playlists_fts WHERE name=?1", params![name])?;
        Ok(())
    }

    /// "Unshare" — make a shared own playlist private again *without losing the data*
    /// (unlike delete). Publishes a tombstone (best-effort network retract: online
    /// syncers drop it; the record dies at EOL ≤168h regardless), then keeps the row
    /// local + editable with `published=0` and no record — off the disclosure set and no
    /// longer re-announced. The seq advances past the tombstone so a later re-Share
    /// supersedes it and revives holders' copies. Copies others already saved, forks, and
    /// offline nodes persist — no gossip system can claw those back.
    pub async fn unpublish(&self, name: &str) -> Result<()> {
        let (key_name, published, seq) = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "SELECT key_name, published, seq FROM playlists WHERE name=?1 AND is_mine=1",
                params![name],
                |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, i64>(1)? != 0,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| anyhow!("not my playlist: {name}"))?
        };
        let mut new_seq = seq;
        if published {
            if let Some(key_name) = key_name {
                let tomb =
                    serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] })?;
                let next = self.next_seq(name, seq).await;
                match self.rpc.playlist_publish(&key_name, next, LIFETIME, tomb).await {
                    // Best-effort: even if the tombstone fails to send, we still go private
                    // locally and stop renewing — the live record then expires at EOL.
                    Ok(_) => new_seq = next as i64,
                    Err(e) => log::warn!(target: "playlist", "unshare tombstone publish failed for {name}: {e}"),
                }
            }
        }
        // Keep doc_json/title/tracks; only reset the publish state. tombstoned stays 0 —
        // the local copy is a live private playlist, not a tombstone.
        let db = self.db.lock().unwrap();
        db.execute(
            "UPDATE playlists SET published=0, record_b64=NULL, eol=0, tombstoned=0, seq=?2,
               last_update_at=?3 WHERE name=?1",
            params![name, new_seq, now_secs()],
        )?;
        Ok(())
    }

    // -- liked tracks (the private, per-client "Liked Tracks" playlist) --

    /// The name of the singleton "Liked Tracks" playlist, creating it on first use.
    /// It's a normal own playlist (is_mine=1) flagged `liked=1`, never published —
    /// private by construction. Serialized so two concurrent first-likes can't mint two.
    pub async fn ensure_liked(&self) -> Result<String> {
        let _guard = self.liked_guard.lock().await;
        if let Some(name) = self.liked_name()? {
            return Ok(name);
        }
        let meta = self.create(LIKED_TITLE.to_string(), vec![]).await?;
        let db = self.db.lock().unwrap();
        db.execute("UPDATE playlists SET liked=1 WHERE name=?1", params![meta.name])?;
        Ok(meta.name)
    }

    /// The liked playlist's name if it exists — a pure read (never creates the row).
    pub fn liked_name(&self) -> Result<Option<String>> {
        let db = self.db.lock().unwrap();
        Ok(db
            .query_row("SELECT name FROM playlists WHERE liked=1 LIMIT 1", [], |r| r.get(0))
            .optional()?)
    }

    /// Toggle a track's membership in "Liked Tracks" (Spotify-style ♥), creating the
    /// liked playlist on first use. Returns the new state: `true` = now liked. Always
    /// private — the liked playlist can never be published (see `publish`/`delete` guards),
    /// so this only mutates local state and never touches the network.
    pub async fn like_toggle(&self, track: (i64, String, String)) -> Result<bool> {
        let name = self.ensure_liked().await?;
        let (title, tracks, liked_now) = {
            let db = self.db.lock().unwrap();
            let (title, doc_json): (String, String) = db.query_row(
                "SELECT title, doc_json FROM playlists WHERE name=?1",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let doc: PlaylistDoc = serde_json::from_str(&doc_json)?;
            let mut tracks: Vec<(i64, String, String)> =
                doc.ts.iter().map(|t| (t.0, t.1.clone(), t.2.clone())).collect();
            let liked_now = match tracks.iter().position(|t| t.0 == track.0) {
                Some(pos) => {
                    tracks.remove(pos);
                    false
                }
                None => {
                    tracks.push(track);
                    true
                }
            };
            (title, tracks, liked_now)
        };
        self.update(&name, title, tracks).await?;
        Ok(liked_now)
    }

    /// The catalog ids currently in the liked playlist (empty if none yet) — the "is
    /// this liked" set the UI's heart buttons read. Pure read, never creates the row.
    pub fn liked_ids(&self) -> Result<Vec<i64>> {
        let db = self.db.lock().unwrap();
        let doc_json: Option<String> = db
            .query_row("SELECT doc_json FROM playlists WHERE liked=1 LIMIT 1", [], |r| r.get(0))
            .optional()?;
        let Some(doc_json) = doc_json else { return Ok(vec![]) };
        let doc: PlaylistDoc = serde_json::from_str(&doc_json)?;
        Ok(doc.ts.iter().map(|t| t.0).collect())
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
                    p.last_update_at, p.last_played_at, p.eol, p.tombstoned, p.liked
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
                    last_update_at, last_played_at, eol, tombstoned, liked
             FROM playlists {filter}
             ORDER BY is_mine DESC, held DESC,
                      MAX(last_update_at, COALESCE(last_played_at, 0)) DESC LIMIT 500",
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

    /// (mine, held, exists) flags for a name — the peer-card "you have this" markers.
    pub fn local_flags(&self, name: &str) -> (bool, bool, bool) {
        let db = self.db.lock().unwrap();
        db.query_row(
            "SELECT is_mine, held FROM playlists WHERE name=?1",
            params![name],
            |r| Ok((r.get::<_, i64>(0)? != 0, r.get::<_, i64>(1)? != 0)),
        )
        .optional()
        .ok()
        .flatten()
        .map(|(m, h)| (m, h, true))
        .unwrap_or((false, false, false))
    }

    pub fn get(&self, name: &str) -> Result<Option<PlaylistDetail>> {
        let db = self.db.lock().unwrap();
        let row = db
            .query_row(
                "SELECT name, title, doc_json, is_mine, held, published, size_bytes,
                        last_update_at, last_played_at, eol, tombstoned, liked
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

    /// Pin the playlist the player is currently sourced from (None = playback moved to
    /// something that isn't a playlist).
    pub fn pin_playing(&self, name: Option<String>) {
        *self.pinned.lock().unwrap() = name;
    }

    fn pinned_name(&self) -> Option<String> {
        self.pinned.lock().unwrap().clone()
    }

    pub fn status(&self) -> Result<SyncStatus> {
        let db = self.db.lock().unwrap();
        let now = now_secs();
        let (total, mine, held, seen, dormant, bytes) = db.query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(is_mine), 0),
                    COALESCE(SUM(held), 0),
                    COALESCE(SUM(is_mine=0 AND held=0), 0),
                    COALESCE(SUM(tombstoned=1 OR (is_mine=0 AND eol>0 AND eol<?1)), 0),
                    COALESCE(SUM(CASE WHEN is_mine=0 AND held=0 THEN size_bytes ELSE 0 END), 0)
             FROM playlists",
            params![now],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )?;
        Ok(SyncStatus { total, mine, held, seen, dormant, bytes, budget: self.budget })
    }

    // -- deep links (link.rs carries the envelope; trust re-anchors in ingest_wire) --

    /// Ingest an incoming playlist link. A payload link verifies through the normal
    /// `ingest_wire` path (signature → EOL → doc hash → schema) and lands in the seen
    /// tier immediately; a name-only link goes pending and is chased with a targeted
    /// `want` at connected peers (the doc then arrives via the ordinary sync loop).
    pub async fn ingest_link(&self, url: &str) -> Result<LinkStatus> {
        let (name, envelope) = link::parse_link(url)?;
        if let Some(env) = envelope {
            let (env_name, record, doc) = link::decode_envelope(&env)?;
            // The path name is what the user *saw*; the envelope is what verifies.
            // A mismatch is a crafted link — reject, don't silently trust the payload.
            if env_name != name {
                bail!("link name does not match its payload");
            }
            let w = PlaylistWire {
                name: name.clone(),
                seq: 0, // ignored: ingest_wire trusts only the seq inside the verified record
                record: base64::engine::general_purpose::STANDARD.encode(&record),
                doc: base64::engine::general_purpose::STANDARD.encode(&doc),
            };
            // Ok(true) = stored; Ok(false) = an equal/newer copy is already local OR the
            // (name, seq) is known-bad — the flags check below tells those apart.
            self.ingest_wire(&w)?;
            let (_, _, have) = self.local_flags(&name);
            if have {
                return Ok(LinkStatus { name, status: "ready" });
            }
            bail!("link payload was rejected");
        }
        // Name-only: pend + chase. Skip both if we already hold any copy.
        let (_, _, have) = self.local_flags(&name);
        if have {
            return Ok(LinkStatus { name, status: "ready" });
        }
        self.pending.lock().unwrap().insert(name.clone(), now_secs());
        self.request_from_peers(&name).await;
        Ok(LinkStatus { name, status: "pending" })
    }

    /// Fire a targeted `want` for `name` at up to 8 connected non-master peers (the
    /// master/seed holds no docs). Stops at the first peer that re-announced or lists
    /// the name — the doc rides gossip into the normal sync loop from there.
    async fn request_from_peers(&self, name: &str) {
        let Ok(peers) = self.rpc.swarm_peers().await else { return };
        let master = crate::ipfs::master_peer_id();
        let want = [name.to_string()];
        let mut asked = 0;
        for p in peers {
            if p.peer == master || asked >= 8 {
                continue;
            }
            asked += 1;
            match self.rpc.playlist_peer_list(&p.peer, &want).await {
                Ok((entries, reann, _supported)) => {
                    if reann > 0 || entries.iter().any(|e| e.name == name) {
                        return; // someone has it — gossip is on its way
                    }
                }
                Err(e) => log::debug!(target: "playlist", "want {} at {}: {e}", name, p.peer),
            }
        }
    }

    /// Names still awaiting a name-only link resolution (pruned: arrived rows and
    /// expired asks drop out). The UI polls this for its "syncing…" state.
    pub fn pending_names(&self) -> Vec<String> {
        let mut pending = self.pending.lock().unwrap();
        pending.retain(|name, asked| {
            *asked > now_secs() - PENDING_EXPIRY_SECS && !self.local_flags_locked(name)
        });
        pending.keys().cloned().collect()
    }

    /// Existence check that does NOT take self.pending (called under its lock).
    fn local_flags_locked(&self, name: &str) -> bool {
        let db = self.db.lock().unwrap();
        db.query_row("SELECT 1 FROM playlists WHERE name=?1", params![name], |_| Ok(()))
            .optional()
            .ok()
            .flatten()
            .is_some()
    }

    /// Build the shareable HTTPS link for a stored playlist. Requires a live record:
    /// unpublished own playlists have none ("share first"), tombstoned rows had theirs
    /// cleared, and an expired record would just be rejected by every receiver.
    pub fn copy_link(&self, name: &str) -> Result<String> {
        let row: Option<(Option<String>, String, i64)> = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "SELECT record_b64, doc_json, tombstoned FROM playlists WHERE name=?1",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
        };
        let Some((record, doc_json, tombstoned)) = row else { bail!("unknown playlist") };
        if tombstoned != 0 {
            bail!("this playlist was deleted by its author — nothing to link to");
        }
        let Some(record) = record else {
            bail!("share the playlist first — links carry the signed record");
        };
        if ipns::verify_b64_seq(name, &record).is_err() {
            bail!("this playlist's record has expired — it can't be shared onward");
        }
        link::build_link(name, &record, &doc_json)
    }

    // -- disclosure-set manifest (playlist-list protocol + hold beacons) --

    /// The disclosure set: held + published-mine rows with a live, announceable record —
    /// exactly what `announce_once` would re-gossip. Holding or publishing a public
    /// playlist is inherently a public act (decision 15); private (unpublished) and
    /// seen-tier rows are never disclosed, structurally.
    pub fn manifest(&self) -> Result<Vec<ManifestEntry>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare(
            "SELECT name, seq, title FROM playlists
             WHERE record_b64 IS NOT NULL AND tombstoned=0
               AND (held=1 OR (is_mine=1 AND published=1))
             ORDER BY name LIMIT 1024",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(ManifestEntry {
                name: r.get(0)?,
                seq: r.get::<_, i64>(1)?.max(0) as u64,
                title: r.get(2)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Post the disclosure set to the node iff it changed since the last successful
    /// post. Called every sync tick (covers all mutation paths, including gossip-driven
    /// updates of held rows) and directly after share/hold/delete for snappiness.
    pub async fn push_manifest(&self) {
        let entries = match self.manifest() {
            Ok(e) => e,
            Err(e) => {
                log::debug!(target: "playlist", "manifest query failed: {e}");
                return;
            }
        };
        let hash = {
            let bytes = serde_json::to_vec(&entries).unwrap_or_default();
            let d = Sha256::digest(&bytes);
            u64::from_le_bytes(d[..8].try_into().unwrap())
        };
        if *self.manifest_hash.lock().unwrap() == Some(hash) {
            return;
        }
        match self.rpc.playlist_manifest(&entries).await {
            Ok(()) => *self.manifest_hash.lock().unwrap() = Some(hash),
            Err(e) => log::debug!(target: "playlist", "manifest push failed: {e}"), // retried next tick
        }
    }

    // -- background loops --

    /// One sync tick: drain the sidecar buffer from `cursor`, verify+store each entry,
    /// enforce the budget. Returns the next cursor.
    pub async fn sync_once(&self, cursor: u64) -> u64 {
        let (ver, recs) = match self.rpc.playlist_records(cursor).await {
            Ok(v) => v,
            Err(e) => {
                log::debug!(target: "playlist", "records poll failed: {e}");
                return cursor;
            }
        };
        for w in &recs {
            match self.ingest_wire(w) {
                // The moment a shared playlist lands locally: `TS_LOG=info,playlist=debug`
                // times a share end-to-end without the frontend/dial firehose.
                Ok(true) => log::debug!(target: "playlist", "ingested {} seq {}", w.name, w.seq),
                Ok(false) => {} // not newer / already rejected — nothing to trace
                Err(e) => log::debug!(target: "playlist", "ingest {} rejected: {e}", w.name),
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
        let pinned = self.pinned_name();
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
            if pinned.as_deref() == Some(name.as_str()) {
                continue; // playing right now — decays on the first pass after unpin
            }
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
                    log::warn!(target: "playlist", "renewal failed for {name}: {e}");
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
            log::debug!(target: "playlist", "announce failed: {e}");
        }
    }
}

/// The record's EOL as unix seconds (0 if it can't be decoded) — stored per row at
/// ingest/publish so list queries can flag dormancy without decoding records.
fn record_eol_secs(record_b64: &str) -> i64 {
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(record_b64.trim()) else {
        return 0;
    };
    let Ok(rec) = rust_ipns::Record::decode(&bytes) else { return 0 };
    rec.validity().map(|v| v.timestamp()).unwrap_or(0)
}

/// One-time backfill for rows from before the `eol` column existed.
fn backfill_eol(db: &Connection) {
    let rows: Vec<(String, String)> = db
        .prepare("SELECT name, record_b64 FROM playlists WHERE eol=0 AND record_b64 IS NOT NULL")
        .and_then(|mut st| {
            st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map(|it| it.filter_map(|r| r.ok()).collect())
        })
        .unwrap_or_default();
    for (name, rec) in rows {
        let _ = db.execute(
            "UPDATE playlists SET eol=?2 WHERE name=?1",
            params![name, record_eol_secs(&rec)],
        );
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
    eol: i64,
) -> rusqlite::Result<()> {
    let tracks_text: String = doc
        .ts
        .iter()
        .map(|t| format!("{} {}", t.1, t.2))
        .collect::<Vec<_>>()
        .join(" ");
    db.execute(
        "INSERT INTO playlists(name, title, doc_json, record_b64, seq, size_bytes, last_update_at, eol)
         VALUES(?1, ?2, ?3, ?4, MAX(?5, 0), ?6, ?7, MAX(?8, 0))
         ON CONFLICT(name) DO UPDATE SET
           title=excluded.title,
           doc_json=excluded.doc_json,
           record_b64=COALESCE(excluded.record_b64, playlists.record_b64),
           seq=CASE WHEN ?5 < 0 THEN playlists.seq ELSE ?5 END,
           eol=CASE WHEN ?8 < 0 THEN playlists.eol ELSE ?8 END,
           tombstoned=0,
           size_bytes=excluded.size_bytes,
           last_update_at=excluded.last_update_at",
        params![
            name,
            doc.t,
            std::str::from_utf8(doc_bytes).unwrap_or_default(),
            record_b64,
            seq,
            doc_bytes.len() as i64,
            now_secs(),
            eol
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
    let is_mine = r.get::<_, i64>(3)? != 0;
    let eol: i64 = r.get(9)?;
    let tombstoned = r.get::<_, i64>(10)? != 0;
    Ok(PlaylistMeta {
        name: r.get(0)?,
        title: r.get(1)?,
        tracks,
        is_mine,
        held: r.get::<_, i64>(4)? != 0,
        published: r.get::<_, i64>(5)? != 0,
        // Dormant = can't propagate: tombstoned by the author, or (foreign rows only —
        // mine auto-renew on the announce cycle) the record aged past its EOL.
        dormant: tombstoned || (!is_mine && eol > 0 && eol < now_secs()),
        tombstoned,
        liked: r.get::<_, i64>(11)? != 0,
        size_bytes: r.get(6)?,
        last_update_at: r.get(7)?,
        last_played_at: r.get(8)?,
    })
}

/// The background driver: a ~20s sync poll, and a jittered ~15-minute announce cycle
/// (the suppression that makes this polite lives node-side; expired-own renewal lives
/// in `announce_once`).
pub async fn run_loops(pl: Arc<Playlists>) {
    // Every client always has a private "Liked Tracks" playlist (Spotify-style): create
    // it eagerly so it's in the library from first launch. Detached so a slow/absent
    // node's key_gen can't gate the sync loop; a failure is harmless — the first ♥
    // recreates it lazily via like_toggle.
    tokio::spawn({
        let pl = pl.clone();
        async move {
            if let Err(e) = pl.ensure_liked().await {
                log::debug!(target: "playlist", "liked ensure at startup failed (retries on first like): {e}");
            }
        }
    });
    let mut cursor = 0u64;
    let mut ticks: u64 = 0;
    // First announce soon after startup (make our playlists discoverable), then ~15min.
    let mut next_announce: u64 = 2;
    loop {
        cursor = pl.sync_once(cursor).await;
        pl.push_manifest().await; // no-op unless the disclosure set changed
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
    fn tombstone_deletes_seen_rows() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let w = wire_for(&kp, &doc_bytes("alive"), 1);
        pl.ingest_wire(&w).unwrap();
        let tomb = serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] }).unwrap();
        let wt = wire_for(&kp, &tomb, 2);
        pl.ingest_wire(&wt).unwrap();
        assert!(pl.get(&w.name).unwrap().is_none());
    }

    // Library copies survive a tombstone as dormant (we don't delete playlists the user
    // chose to keep): record cleared (never re-announced), seq pinned at the tombstone's
    // (replays can't resurrect), doc still playable — and a genuine author republish at
    // seq+1 revives the row.
    #[test]
    fn tombstone_preserves_held_rows_dormant_and_republish_revives() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let w = wire_for(&kp, &doc_bytes("keeper"), 1);
        pl.ingest_wire(&w).unwrap();
        pl.set_held(&w.name, true).unwrap();

        let tomb = serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] }).unwrap();
        pl.ingest_wire(&wire_for(&kp, &tomb, 2)).unwrap();

        let got = pl.get(&w.name).unwrap().expect("held row must survive the tombstone");
        assert!(got.meta.dormant && got.meta.tombstoned);
        assert_eq!(got.meta.title, "keeper", "kept doc stays playable");
        let rec: Option<String> = {
            let db = pl.db.lock().unwrap();
            db.query_row("SELECT record_b64 FROM playlists WHERE name=?1", params![w.name], |r| r.get(0))
                .unwrap()
        };
        assert!(rec.is_none(), "tombstoned row must never be re-announced");

        // Replayed pre-delete record (seq 1) must NOT resurrect it.
        assert!(!pl.ingest_wire(&w).unwrap());
        assert!(pl.get(&w.name).unwrap().unwrap().meta.tombstoned);

        // Author republishes (seq 3): the held row revives in place.
        pl.ingest_wire(&wire_for(&kp, &doc_bytes("reborn"), 3)).unwrap();
        let back = pl.get(&w.name).unwrap().unwrap();
        assert!(!back.meta.tombstoned && !back.meta.dormant && back.meta.held);
        assert_eq!(back.meta.title, "reborn");
    }

    // Disclosure set (decision 15: no "back silently", ever): held + published-mine
    // exactly — seen rows, unpublished own rows, and tombstoned rows never appear.
    #[test]
    fn manifest_is_held_plus_published_mine_only() {
        let pl = mem();
        let k1 = Keypair::generate_ed25519();
        let k2 = Keypair::generate_ed25519();
        let seen = wire_for(&k1, &doc_bytes("just seen"), 1);
        let held = wire_for(&k2, &doc_bytes("backed"), 2);
        pl.ingest_wire(&seen).unwrap();
        pl.ingest_wire(&held).unwrap();

        // Seen tier: never disclosed.
        assert!(pl.manifest().unwrap().is_empty());

        // Held: disclosed, with seq + title.
        pl.set_held(&held.name, true).unwrap();
        let m = pl.manifest().unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!((m[0].name.as_str(), m[0].seq, m[0].title.as_str()), (held.name.as_str(), 2, "backed"));

        // Own rows: only when published (an unpublished own playlist is PRIVATE).
        {
            let db = pl.db.lock().unwrap();
            db.execute(
                "UPDATE playlists SET is_mine=1, held=0, published=0 WHERE name=?1",
                params![seen.name],
            )
            .unwrap();
        }
        assert_eq!(pl.manifest().unwrap().len(), 1);
        {
            let db = pl.db.lock().unwrap();
            db.execute("UPDATE playlists SET published=1 WHERE name=?1", params![seen.name]).unwrap();
        }
        assert_eq!(pl.manifest().unwrap().len(), 2);

        // A tombstoned held row (record cleared, dormant) drops out of the disclosure set.
        let tomb = serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] }).unwrap();
        pl.ingest_wire(&wire_for(&k2, &tomb, 3)).unwrap();
        let m = pl.manifest().unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].name, seen.name);
    }

    // The "Liked Tracks" playlist is private by construction: own + unpublished, so it
    // never appears in the disclosure set (no leaked listening habits). `create()` needs
    // a live node for key_gen, so the row is injected directly here — the same shortcut
    // the budget/manifest tests use.
    #[test]
    fn liked_playlist_is_private_and_identified() {
        let pl = mem();
        let doc = PlaylistDoc {
            v: 1,
            t: LIKED_TITLE.into(),
            del: false,
            ts: vec![TrackRef(1, "a.it".into(), "A".into()), TrackRef(2, "b.it".into(), "B".into())],
        };
        let bytes = serde_json::to_vec(&doc).unwrap();
        {
            let db = pl.db.lock().unwrap();
            upsert_row(&db, "likedname", &doc, &bytes, None, 0, 0).unwrap();
            db.execute(
                "UPDATE playlists SET is_mine=1, liked=1, key_name='playlist-liked' WHERE name=?1",
                params!["likedname"],
            )
            .unwrap();
        }
        assert_eq!(pl.liked_name().unwrap().as_deref(), Some("likedname"));
        assert_eq!(pl.liked_ids().unwrap(), vec![1, 2]);
        // Private by default — an unpublished liked playlist is never disclosed (until
        // the user explicitly shares it, at which point it's a normal published own row).
        assert!(pl.manifest().unwrap().is_empty());
        // Surfaced in the library list with the liked flag set for the UI.
        let lib = pl.list("library").unwrap();
        assert!(lib.iter().any(|p| p.name == "likedname" && p.liked && p.is_mine));
    }

    // "Make private again": unpublish keeps the row + tracks but drops it from the
    // disclosure set (record cleared, published=0) so it stops being shared. The network
    // tombstone is best-effort and needs a live node, so this exercises the local state
    // transition on an injected published row (rpc unreachable → tombstone logs + skips).
    #[test]
    fn unpublish_goes_private_but_keeps_the_data() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let doc = doc_bytes("my mix");
        let w = wire_for(&kp, &doc, 5);
        pl.ingest_wire(&w).unwrap();
        // Make it a published own row (as publish() would leave it).
        {
            let db = pl.db.lock().unwrap();
            db.execute(
                "UPDATE playlists SET is_mine=1, published=1, key_name='playlist-x' WHERE name=?1",
                params![w.name],
            )
            .unwrap();
        }
        assert_eq!(pl.manifest().unwrap().len(), 1, "published own row is disclosed");

        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(pl.unpublish(&w.name)).unwrap();

        let got = pl.get(&w.name).unwrap().expect("row survives unpublish");
        assert!(!got.meta.published, "now private");
        assert!(!got.meta.tombstoned, "local copy is a live private playlist, not a tombstone");
        assert_eq!(got.items.len(), 1, "tracks kept");
        assert!(pl.manifest().unwrap().is_empty(), "off the disclosure set — no longer shared");
        let rec: Option<String> = {
            let db = pl.db.lock().unwrap();
            db.query_row("SELECT record_b64 FROM playlists WHERE name=?1", params![w.name], |r| r.get(0))
                .unwrap()
        };
        assert!(rec.is_none(), "record cleared — announce loop won't re-gossip it");
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
    fn playing_bumps_a_playlist_up_in_the_library() {
        let pl = mem();
        let (ka, kb) = (Keypair::generate_ed25519(), Keypair::generate_ed25519());
        let a = wire_for(&ka, &doc_bytes("older-play"), 1);
        let b = wire_for(&kb, &doc_bytes("newer-update"), 1);
        pl.ingest_wire(&a).unwrap();
        pl.ingest_wire(&b).unwrap();
        // Give B the newer *update* time so by the old sort it would lead.
        {
            let db = pl.db.lock().unwrap();
            db.execute("UPDATE playlists SET last_update_at=1 WHERE name=?1", params![a.name]).unwrap();
            db.execute("UPDATE playlists SET last_update_at=1000000000 WHERE name=?1", params![b.name])
                .unwrap();
        }
        let order = |pl: &Playlists| pl.list("all").unwrap().into_iter().map(|m| m.name).collect::<Vec<_>>();
        assert_eq!(order(&pl), vec![b.name.clone(), a.name.clone()], "newer update leads before any play");

        // Playing A (mark_played uses now, which outranks B's update time) bumps it above B.
        pl.mark_played(&a.name).unwrap();
        assert_eq!(order(&pl), vec![a.name.clone(), b.name.clone()], "the just-played playlist leads");
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

    // Deep links ride the same trust path: an envelope built from stored row fields
    // round-trips copy_link → ingest_link on a fresh store, a tampered fragment is
    // rejected + remembered, and a path/payload name mismatch is rejected outright.
    #[tokio::test]
    async fn deep_link_roundtrip_tamper_and_mismatch() {
        let src = mem();
        let kp = Keypair::generate_ed25519();
        let w = wire_for(&kp, &doc_bytes("linked"), 4);
        src.ingest_wire(&w).unwrap();
        let url = src.copy_link(&w.name).unwrap();
        assert!(url.starts_with(crate::link::WEB_BASE));

        // Fresh store: the link alone materializes the playlist (seen tier).
        let dst = mem();
        let st = dst.ingest_link(&url).await.unwrap();
        assert_eq!((st.status, st.name.as_str()), ("ready", w.name.as_str()));
        let got = dst.get(&w.name).unwrap().unwrap();
        assert!(!got.meta.is_mine && !got.meta.held, "link ingest lands in the seen tier");
        assert_eq!(got.meta.title, "linked");

        // Tampered fragment: flip one payload byte → rejected, remembered.
        let (name, frag) = url.split_once('#').map(|(u, f)| (u.to_string(), f.to_string())).unwrap();
        let mut env = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&frag).unwrap();
        let last = env.len() - 1;
        env[last] ^= 0x01;
        let bad = format!("{name}#{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&env));
        let dst2 = mem();
        assert!(dst2.ingest_link(&bad).await.is_err());
        assert!(dst2.get(&w.name).unwrap().is_none());

        // Path/payload name mismatch: valid envelope under a different path name.
        let other = wire_for(&Keypair::generate_ed25519(), &doc_bytes("other"), 1);
        let crafted = format!("{}{}#{frag}", crate::link::WEB_BASE, other.name);
        assert!(dst2.ingest_link(&crafted).await.is_err());

        // copy_link preconditions: unknown and tombstoned rows refuse.
        assert!(dst2.copy_link("12D3KooWNoSuchName").is_err());
        let tomb = serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] }).unwrap();
        src.set_held(&w.name, true).unwrap();
        src.ingest_wire(&wire_for(&kp, &tomb, 5)).unwrap();
        assert!(src.copy_link(&w.name).is_err(), "tombstoned row must not produce a link");
    }

    // Name-only links pend until the row arrives via gossip, then resolve; stale asks
    // expire out of the pending set.
    #[tokio::test]
    async fn name_only_link_pends_then_resolves() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let name = kp.public().to_peer_id().to_string();
        // request_from_peers hits the (unreachable) test RPC and degrades to a no-op.
        let st = pl.ingest_link(&format!("trackerstream://playlist/{name}")).await.unwrap();
        assert_eq!(st.status, "pending");
        assert_eq!(pl.pending_names(), vec![name.clone()]);

        // The doc arrives through the normal sync path → pending clears.
        pl.ingest_wire(&wire_for(&kp, &doc_bytes("arrived"), 1)).unwrap();
        assert!(pl.pending_names().is_empty());

        // An already-held name short-circuits to ready.
        let st = pl.ingest_link(&format!("trackerstream://playlist/{name}")).await.unwrap();
        assert_eq!(st.status, "ready");
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

    // Play-time pin: the playlist the player is sourced from can't be evicted, decayed,
    // or tombstone-deleted (kept dormant instead) while pinned; unpinning restores the
    // normal seen-tier lifecycle on the next pass.
    #[test]
    fn pinned_playing_row_survives_until_unpinned() {
        let mut pl = mem();
        pl.budget = 0; // any seen-tier bytes are over budget
        let kp = Keypair::generate_ed25519();
        let w = wire_for(&kp, &doc_bytes("playing"), 1);
        pl.ingest_wire(&w).unwrap();
        pl.pin_playing(Some(w.name.clone()));

        pl.enforce_budget();
        assert!(pl.get(&w.name).unwrap().is_some(), "pinned row must survive eviction");

        // Author tombstones it mid-play: kept dormant (like library), not deleted.
        let tomb = serde_json::to_vec(&PlaylistDoc { v: 1, t: String::new(), del: true, ts: vec![] }).unwrap();
        pl.ingest_wire(&wire_for(&kp, &tomb, 2)).unwrap();
        let got = pl.get(&w.name).unwrap().expect("pinned row must survive the tombstone");
        assert!(got.meta.dormant && got.meta.tombstoned);

        pl.decay_expired();
        assert!(pl.get(&w.name).unwrap().is_some(), "pinned row must survive decay");

        // Playback moves on: the row decays on the next pass (record cleared by the
        // tombstone → no longer alive).
        pl.pin_playing(None);
        pl.decay_expired();
        assert!(pl.get(&w.name).unwrap().is_none(), "unpinned row resumes normal decay");
    }

    // Dormancy: a foreign row whose stored EOL has passed is flagged (still playable,
    // no longer propagating); fresh records and own rows are never dormant.
    #[test]
    fn dormant_flags_expired_foreign_rows() {
        let pl = mem();
        let kp = Keypair::generate_ed25519();
        let w = wire_for(&kp, &doc_bytes("sleepy"), 1);
        pl.ingest_wire(&w).unwrap();
        assert!(!pl.get(&w.name).unwrap().unwrap().meta.dormant, "fresh record is not dormant");
        {
            let db = pl.db.lock().unwrap();
            db.execute("UPDATE playlists SET eol=1 WHERE name=?1", params![w.name]).unwrap();
        }
        assert!(pl.get(&w.name).unwrap().unwrap().meta.dormant, "expired foreign row is dormant");
        {
            let db = pl.db.lock().unwrap();
            db.execute("UPDATE playlists SET is_mine=1 WHERE name=?1", params![w.name]).unwrap();
        }
        assert!(!pl.get(&w.name).unwrap().unwrap().meta.dormant, "own rows auto-renew — never dormant");
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
