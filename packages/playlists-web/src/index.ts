// Browser playlist authoring: the store, the keys, the gossip loop.
//
// A port of apps/desktop/src-tauri/src/playlists.rs plus the client half of node/playlist.go and
// node/beacon.go. It needs NO Go-side change and NO inbound dialability, because a playlist doc
// travels INLINE next to its signed IPNS record — publishing is one gossipsub message, and nobody
// ever dials the author. That is what makes a browser (which cannot be dialled) a first-class
// publisher rather than a second-class reader.
import { peerIdFromString } from "@libp2p/peer-id";
import type { Libp2p } from "libp2p";
import { createIPNSRecord, marshalIPNSRecord, unmarshalIPNSRecord } from "./ipns-compat.ts";
import { BEACON_TOPIC, PLAYLIST_TOPIC, decodePlaylistMsg, encodePlaylistMsg, validatePlaylistMsg, UnknownWireVersion } from "./wire.ts";
import { DOC_VERSION, LIFETIME_MS, RENEW_MARGIN_SECS, docCid, encodeDoc, validateDoc, type PlaylistDoc, type TrackRef } from "./doc.ts";
import { PlaylistStore, nowSecs, type Row } from "./store.ts";
import { createKey, deleteKey, exportKeys, importKeys, loadKey, requestPersistence, type PlaylistKey } from "./keys.ts";
import { encodeBeacon, nameHash8 } from "./beacon.ts";
import { buildLink, decodeEnvelope, parseLink } from "./link.ts";

export { exportKeys, importKeys, requestPersistence } from "./keys.ts";
export { buildLink, parseLink } from "./link.ts";

const BUDGET_BYTES = 50 * 1024 * 1024; // seen-tier only; library rows are exempt
const ANNOUNCE_MS = 15 * 60 * 1000;
const BEACON_MS = 60 * 60 * 1000;
const LIKED_TITLE = "Liked Tracks";

const b64 = (b: Uint8Array): string => btoa(String.fromCharCode(...b));
const unb64 = (s: string): Uint8Array => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

/** The UI's PlaylistMeta shape (see @trackerstream/ui). Duplicated structurally rather than
 *  imported so this package stays UI-agnostic. */
export interface Meta {
  name: string;
  title: string;
  tracks: number;
  isMine: boolean;
  held: boolean;
  published: boolean;
  dormant: boolean;
  tombstoned: boolean;
  liked: boolean;
  sizeBytes: number;
  lastUpdateAt: number;
  lastPlayedAt: number | null;
}

export interface Detail extends Meta {
  items: { md5: string; modName: string; title: string }[];
}

function metaOf(r: Row): Meta {
  const doc = JSON.parse(r.doc_json || "{}") as PlaylistDoc;
  return {
    name: r.name,
    title: r.title,
    tracks: (doc.ts ?? []).length,
    isMine: !!r.is_mine,
    held: !!r.held,
    published: !!r.published,
    // No longer propagating: the record expired, or the author tombstoned it. Still playable — the
    // UI nudges toward "duplicate to mine".
    dormant: !!r.tombstoned || (r.eol > 0 && r.eol < nowSecs()),
    tombstoned: !!r.tombstoned,
    liked: !!r.liked,
    sizeBytes: r.size_bytes,
    lastUpdateAt: r.last_update_at,
    lastPlayedAt: r.last_played_at,
  };
}

function detailOf(r: Row): Detail {
  const doc = JSON.parse(r.doc_json || "{}") as PlaylistDoc;
  return {
    ...metaOf(r),
    items: (doc.ts ?? []).map((t) => ({ md5: t[0], modName: t[1], title: t[2] })),
  };
}

export class Playlists {
  private store!: PlaylistStore;
  private libp2p!: Libp2p;
  private onChange: () => void = () => {};
  private timers: ReturnType<typeof setInterval>[] = [];
  /** Names we've seen announced recently — suppression state, so we don't re-gossip something the
   *  mesh just carried (node/playlist.go's playlistAnnounceWindow). */
  private lastSeen = new Map<string, number>();
  private pending = new Set<string>();

  static async create(libp2p: Libp2p, onChange: () => void): Promise<Playlists> {
    const p = new Playlists();
    p.libp2p = libp2p;
    p.store = await PlaylistStore.open();
    p.onChange = onChange;
    await p.subscribe();
    // The announce cycle re-gossips mine + held, and re-signs my own records before they expire.
    p.timers.push(setInterval(() => void p.announceOnce(), ANNOUNCE_MS));
    p.timers.push(setInterval(() => void p.beaconOnce(), BEACON_MS));
    void p.announceOnce();
    void p.beaconOnce();
    return p;
  }

  stop(): void {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
  }

  private get pubsub(): any {
    return (this.libp2p.services as { pubsub?: any }).pubsub;
  }

  private async subscribe(): Promise<void> {
    const ps = this.pubsub;
    if (!ps) throw new Error("playlists: no pubsub service");
    // The topic validator runs on EVERY message this peer relays, not just the ones it stores. A
    // browser leaf still forwards to its mesh, so it must not pass garbage on.
    ps.topicValidators?.set?.(PLAYLIST_TOPIC, async (_peer: unknown, msg: { data: Uint8Array }) => {
      try {
        const { name, rec, doc } = decodePlaylistMsg(msg.data);
        await validatePlaylistMsg(name, rec, doc);
        return "accept";
      } catch (e) {
        // An unknown-newer wire version is IGNORED, never REJECTED: rejecting would penalise honest
        // peers that merely upgraded, and a newer network could score the older half into isolation.
        return e instanceof UnknownWireVersion ? "ignore" : "reject";
      }
    });
    ps.addEventListener("message", (ev: { detail: { topic: string; data: Uint8Array } }) => {
      if (ev.detail.topic !== PLAYLIST_TOPIC) return;
      void this.ingestWire(ev.detail.data).catch(() => {});
    });
    ps.subscribe(PLAYLIST_TOPIC);
    ps.subscribe(BEACON_TOPIC);
  }

  /** Ingest a gossiped envelope. Re-verifies from scratch: the topic validator may have run on a
   *  different peer's behalf, and the Rust re-verifies at ingest too — the store trusts nothing. */
  private async ingestWire(bytes: Uint8Array): Promise<void> {
    const { name, rec, doc } = decodePlaylistMsg(bytes);
    const seq = await validatePlaylistMsg(name, rec, doc);
    await this.ingestVerified(name, rec, doc, seq);
  }

  private async ingestVerified(name: string, rec: Uint8Array, doc: Uint8Array, seq: bigint): Promise<void> {
    const existing = await this.store.get(name);
    if (existing && existing.seq >= Number(seq)) return; // not newer — keep what we have
    const parsed = validateDoc(doc);
    const eol = recordEolSecs(rec);
    await this.store.upsert({
      name,
      doc: parsed,
      docBytes: doc, // the ORIGINAL bytes — never a re-serialization
      recordB64: b64(rec),
      seq: Number(seq),
      eol,
    });
    this.pending.delete(name);
    this.lastSeen.set(name, nowSecs());
    await this.store.enforceBudget(BUDGET_BYTES);
    this.onChange();
  }

  /** Seq recovery: max(local, whatever the mesh has shown us) + 1. Playlists are pubsub-only, so
   *  "the network's seq" is simply the highest we've seen announced. A never-published playlist
   *  (seq 0) skips it: its key is fresh and there is nothing to recover. */
  private nextSeq(local: number): bigint {
    return BigInt(Math.max(local, 0) + 1);
  }

  private async sign(key: PlaylistKey, doc: Uint8Array, seq: bigint): Promise<Uint8Array> {
    const cid = await docCid(doc);
    const record = await createIPNSRecord(key as never, `/ipfs/${cid.toString()}`, seq, LIFETIME_MS);
    return marshalIPNSRecord(record);
  }

  private async gossip(name: string, rec: Uint8Array, doc: Uint8Array): Promise<void> {
    await this.pubsub.publish(PLAYLIST_TOPIC, encodePlaylistMsg(name, rec, doc));
    this.lastSeen.set(name, nowSecs());
  }

  // ---- the NodeClient.playlists surface ----

  async list(): Promise<Meta[]> {
    return (await this.store.list()).map(metaOf);
  }

  async search(q: string): Promise<Meta[]> {
    return (await this.store.search(q)).map(metaOf);
  }

  async get(name: string): Promise<Detail | null> {
    const r = await this.store.get(name);
    return r ? detailOf(r) : null;
  }

  async create(title: string, tracks: TrackRef[]): Promise<Meta> {
    // Ask for durable storage BEFORE the first key exists. If the browser evicts our IndexedDB, the
    // key is gone and this playlist can never be updated, republished, or tombstoned again.
    await requestPersistence();
    const { name } = await createKey();
    const doc: PlaylistDoc = { v: DOC_VERSION, t: title, ts: tracks };
    const bytes = encodeDoc(doc);
    validateDoc(bytes);
    // A key (and therefore the name) exists from birth; NOTHING is published until the user
    // explicitly shares.
    await this.store.upsert({ name, doc, docBytes: bytes, recordB64: null, seq: 0, eol: 0, isMine: true, keyName: name });
    this.onChange();
    return metaOf((await this.store.get(name))!);
  }

  async update(name: string, title: string, tracks: TrackRef[]): Promise<void> {
    const row = await this.store.get(name);
    if (!row?.is_mine) throw new Error("not my playlist");
    const doc: PlaylistDoc = { v: DOC_VERSION, t: title, ts: tracks };
    const bytes = encodeDoc(doc);
    validateDoc(bytes);
    await this.store.upsert({ name, doc, docBytes: bytes, recordB64: row.record_b64, seq: row.seq, eol: row.eol });
    // An edit to an already-shared playlist republishes at seq+1.
    if (row.published) await this.publish(name);
    else this.onChange();
  }

  async publish(name: string): Promise<void> {
    const row = await this.store.get(name);
    if (!row?.is_mine) throw new Error("not my playlist");
    const key = await loadKey(name);
    if (!key) throw new Error(`playlist key for ${name} is missing — was this browser's storage cleared?`);
    const doc = new TextEncoder().encode(row.doc_json);
    const seq = this.nextSeq(row.seq);
    const rec = await this.sign(key, doc, seq);
    await this.store.upsert({
      name,
      doc: JSON.parse(row.doc_json),
      docBytes: doc,
      recordB64: b64(rec),
      seq: Number(seq),
      eol: recordEolSecs(rec),
      published: true,
    });
    await this.gossip(name, rec, doc);
    this.onChange();
  }

  /** "Make private again": publish a tombstone so the mesh stops propagating it, but keep the data
   *  locally. Library copies elsewhere are preserved dormant rather than deleted. */
  async unpublish(name: string): Promise<void> {
    await this.tombstone(name);
    await this.store.run("UPDATE playlists SET published=0 WHERE name=?", [name]);
    this.onChange();
  }

  async remove(name: string): Promise<void> {
    const row = await this.store.get(name);
    if (row?.is_mine && row.published) await this.tombstone(name).catch(() => {});
    await this.store.remove(name);
    await deleteKey(name);
    this.onChange();
  }

  private async tombstone(name: string): Promise<void> {
    const row = await this.store.get(name);
    if (!row?.is_mine || !row.published) return;
    const key = await loadKey(name);
    if (!key) return; // no key, no tombstone — nothing we can do, and it is not worth failing over
    const doc = encodeDoc({ v: DOC_VERSION, del: true });
    const seq = this.nextSeq(row.seq);
    const rec = await this.sign(key, doc, seq);
    await this.gossip(name, rec, doc);
    await this.store.run("UPDATE playlists SET seq=?2, tombstoned=1, record_b64=?3 WHERE name=?1", [
      name,
      Number(seq),
      b64(rec),
    ]);
  }

  async hold(name: string, held: boolean): Promise<void> {
    await this.store.run("UPDATE playlists SET held=? WHERE name=?", [held ? 1 : 0, name]);
    this.onChange();
  }

  async played(name: string): Promise<void> {
    await this.store.run("UPDATE playlists SET last_played_at=? WHERE name=?", [nowSecs(), name]);
    this.onChange();
  }

  async syncStatus(): Promise<{ total: number; mine: number; held: number; seen: number; dormant: number; bytes: number; budget: number }> {
    const rows = await this.store.list();
    const now = nowSecs();
    return {
      total: rows.length,
      mine: rows.filter((r) => r.is_mine).length,
      held: rows.filter((r) => r.held).length,
      seen: rows.filter((r) => !r.is_mine && !r.held).length,
      dormant: rows.filter((r) => r.tombstoned || (r.eol > 0 && r.eol < now)).length,
      bytes: rows.filter((r) => !r.is_mine && !r.held).reduce((n, r) => n + r.size_bytes, 0),
      budget: BUDGET_BYTES,
    };
  }

  // ---- liked tracks (the private playlist — own, never published) ----

  /** Single-flight: the UI calls likedIds() and likedName() concurrently on mount, and without this
   *  each would see "no liked playlist yet" and create one — leaving the user with two. */
  private likedInFlight: Promise<Row> | null = null;

  private likedRow(): Promise<Row> {
    if (!this.likedInFlight) {
      this.likedInFlight = this.resolveLikedRow().finally(() => {
        this.likedInFlight = null;
      });
    }
    return this.likedInFlight;
  }

  private async resolveLikedRow(): Promise<Row> {
    const rows = await this.store.list();
    const found = rows.find((r) => r.liked);
    if (found) return found;
    const meta = await this.create(LIKED_TITLE, []);
    await this.store.run("UPDATE playlists SET liked=1 WHERE name=?", [meta.name]);
    return (await this.store.get(meta.name))!;
  }

  async likedName(): Promise<string> {
    return (await this.likedRow()).name;
  }

  async likedIds(): Promise<string[]> {
    const doc = JSON.parse((await this.likedRow()).doc_json) as PlaylistDoc;
    return (doc.ts ?? []).map((t) => t[0]);
  }

  async likeToggle(track: TrackRef): Promise<boolean> {
    const row = await this.likedRow();
    const doc = JSON.parse(row.doc_json) as PlaylistDoc;
    const ts = doc.ts ?? [];
    const at = ts.findIndex((t) => t[0] === track[0]);
    const nowLiked = at === -1;
    if (nowLiked) ts.push(track);
    else ts.splice(at, 1);
    await this.update(row.name, doc.t ?? LIKED_TITLE, ts);
    return nowLiked;
  }

  // ---- links ----

  async copyLink(name: string): Promise<string> {
    const row = await this.store.get(name);
    if (!row?.record_b64) throw new Error("playlist has no live record — share it first");
    return buildLink(name, row.record_b64, row.doc_json);
  }

  /** A link is a THIRD transport with identical trust: the payload rides the fragment and verifies
   *  through the same path a gossiped playlist does. A name-only link pends until gossip delivers. */
  async ingestLink(url: string): Promise<{ name: string; status: "ready" | "pending" }> {
    const { name, envelope } = parseLink(url);
    if (!envelope) {
      this.pending.add(name);
      return { name, status: "pending" };
    }
    const { name: n, record, doc } = decodeEnvelope(envelope);
    if (n !== name) throw new Error("link name does not match its payload");
    const seq = await validatePlaylistMsg(n, record, doc);
    await this.ingestVerified(n, record, doc, seq);
    return { name, status: "ready" };
  }

  async pendingNames(): Promise<string[]> {
    return [...this.pending];
  }

  // ---- the loops ----

  /** Re-gossip mine + held, and re-sign my own records before they expire.
   *
   *  Suppression: skip anything the mesh has carried within the announce window — otherwise every
   *  holder of a popular playlist re-broadcasts it on the same cadence. */
  private async announceOnce(): Promise<void> {
    const now = nowSecs();
    for (const row of await this.store.list()) {
      if (!row.is_mine && !row.held) continue;
      if (row.tombstoned) continue;
      const seen = this.lastSeen.get(row.name) ?? 0;
      if (now - seen < 10 * 60) continue; // playlistAnnounceWindow

      if (row.is_mine && row.published) {
        const key = await loadKey(row.name);
        if (!key) continue;
        const doc = new TextEncoder().encode(row.doc_json);
        // Within the renewal margin of EOL: re-sign at seq+1 so the record keeps propagating.
        if (row.eol > 0 && row.eol < now + RENEW_MARGIN_SECS) {
          const seq = this.nextSeq(row.seq);
          const rec = await this.sign(key, doc, seq);
          await this.store.run("UPDATE playlists SET seq=?2, record_b64=?3, eol=?4 WHERE name=?1", [
            row.name,
            Number(seq),
            b64(rec),
            recordEolSecs(rec),
          ]);
          await this.gossip(row.name, rec, doc);
        } else if (row.record_b64) {
          await this.gossip(row.name, unb64(row.record_b64), doc);
        }
      } else if (row.held && row.record_b64) {
        // Re-announce someone else's playlist verbatim. NEVER re-serialize the doc: a future field
        // we don't understand would be dropped, and the doc would stop hashing to its record's CID.
        await this.gossip(row.name, unb64(row.record_b64), new TextEncoder().encode(row.doc_json));
      }
    }
  }

  /** "I hold these" — truncated name hashes, hourly. Cheap, and it makes web peers contribute to
   *  the backer counts desktop users see. */
  private async beaconOnce(): Promise<void> {
    const rows = await this.store.list();
    const mine = rows.filter((r) => (r.is_mine && r.published) || r.held);
    if (!mine.length) return;
    const hashes = await Promise.all(mine.map((r) => nameHash8(r.name)));
    await this.pubsub.publish(BEACON_TOPIC, encodeBeacon(hashes)).catch(() => {});
  }
}

/** The record's EOL, in unix seconds — what drives `dormant` and the renewal margin. */
function recordEolSecs(rec: Uint8Array): number {
  try {
    const r = unmarshalIPNSRecord(rec);
    return Math.floor(new Date(r.validity).getTime() / 1000);
  } catch {
    return 0;
  }
}

/** Shape-check a name before it reaches the DB or a request. */
export const validPlaylistName = (n: string): boolean => {
  try {
    peerIdFromString(n);
    return true;
  } catch {
    return false;
  }
};
