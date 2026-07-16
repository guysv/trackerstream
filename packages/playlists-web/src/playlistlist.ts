// Playlist-list wire — a port of node/playlistlist.go's request/response codec + serving guards.
//
// A peer asks "what playlists do you hold?" over the "/trackerstream/playlist-list/1.0.0" stream and
// gets back this node's DISCLOSURE SET: held ∪ published-mine, exactly what it re-announces on gossip.
// One length-prefixed (uvarint) JSON frame each way, byte-compatible with the Go side's go-msgio
// framing. Optional `want` names trigger a targeted, suppression-bypassing re-announce, per-name
// cooldown-bounded. This file is the pure codec + guards; the stream I/O and the gossip re-announce
// live in index.ts (which owns libp2p + the store), mirroring how beacon.ts pairs with the loop there.

/** Client-only stream protocol (the seed holds no library and never serves it), matches node/config.go. */
export const PLAYLIST_LIST_PROTOCOL = "/trackerstream/playlist-list/1.0.0";

export const PL_MAX_ENTRIES = 256; // response entry cap (mirrors plListMaxEntries)
export const PL_WANT_MAX = 64; // want names per request (plListWantMax)
export const PL_TITLE_MAX = 300; // title byte clamp (plListTitleMax)
export const PL_REQ_MAX = 8 << 10; // request frame reader bound (plListReqMax)
export const PL_RESP_MAX = 256 << 10; // response frame reader bound (plListMaxFrame)

const WANT_COOLDOWN_SECS = 30; // per-name forced-reannounce window (plWantCooldown)
const PEER_RATE_PER_SEC = 0.1; // per-peer request refill (plListPeerRate)
const PEER_BURST = 4; // per-peer request burst (plListPeerBurst)
const PEER_TABLE_MAX = 4096; // bounded per-peer limiter table (plListPeerLims)
const COOLDOWN_TABLE_MAX = 1024; // bounded per-name cooldown table (plWantCoolEnts)

/** One disclosed playlist — the JSON element shape is capitalised to match the Go struct tags exactly
 *  ({"Name","Seq","Title"}), so a browser and a desktop interoperate on the wire without translation. */
export interface PlaylistListEntry {
  Name: string;
  Seq: number;
  Title: string;
}

interface Req {
  want?: string[];
}
interface Resp {
  playlists: PlaylistListEntry[];
  reannounced: number;
}

const enc = new TextEncoder();
const dec = new TextDecoder();

export const encodeReq = (want: string[]): Uint8Array =>
  enc.encode(JSON.stringify({ want: want.length ? want.slice(0, PL_WANT_MAX) : undefined } satisfies Req));

export function decodeReq(bytes: Uint8Array): { want: string[] } {
  const req = JSON.parse(dec.decode(bytes)) as Req;
  const want = Array.isArray(req.want) ? req.want.filter((n) => typeof n === "string").slice(0, PL_WANT_MAX) : [];
  return { want };
}

export const encodeResp = (playlists: PlaylistListEntry[], reannounced: number): Uint8Array =>
  enc.encode(JSON.stringify({ playlists: playlists.slice(0, PL_MAX_ENTRIES), reannounced } satisfies Resp));

export function decodeResp(bytes: Uint8Array): Resp {
  const r = JSON.parse(dec.decode(bytes)) as Resp;
  const playlists = (Array.isArray(r.playlists) ? r.playlists : []).slice(0, PL_MAX_ENTRIES).map((e) => ({
    Name: String(e.Name ?? ""),
    Seq: Number(e.Seq ?? 0),
    Title: String(e.Title ?? "").slice(0, PL_TITLE_MAX),
  }));
  return { playlists, reannounced: Number(r.reannounced ?? 0) };
}

/** Per-peer token bucket (0.1/s, burst 4), in a bounded insertion-ordered map: a request storm from a
 *  minted-ID sybil is bounded by the table cap, exactly as the Go LRU is (the real backstop is the
 *  per-name cooldown; per-peer buckets just make the human-scale case cheap). `nowSecs` is passed in so
 *  the caller controls the clock (and tests can shrink it). */
export class PeerRateLimiter {
  private buckets = new Map<string, { tokens: number; at: number }>();

  allow(peer: string, nowSecs: number): boolean {
    let b = this.buckets.get(peer);
    if (!b) {
      b = { tokens: PEER_BURST, at: nowSecs };
      if (this.buckets.size >= PEER_TABLE_MAX) this.buckets.delete(this.buckets.keys().next().value as string);
    } else {
      this.buckets.delete(peer); // re-insert to keep it recent in the eviction order
      b.tokens = Math.min(PEER_BURST, b.tokens + (nowSecs - b.at) * PEER_RATE_PER_SEC);
      b.at = nowSecs;
    }
    this.buckets.set(peer, b);
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  }
}

/** Per-name forced-reannounce cooldown (30s), bounded table. `allow` records the attempt, so a `want`
 *  storm re-gossips a fat doc at most once per window per name — mirrors plListState.wantAllowed. */
export class ReannounceCooldown {
  private at = new Map<string, number>();

  allow(name: string, nowSecs: number): boolean {
    const last = this.at.get(name);
    if (last !== undefined && nowSecs - last < WANT_COOLDOWN_SECS) return false;
    this.at.delete(name);
    if (this.at.size >= COOLDOWN_TABLE_MAX) this.at.delete(this.at.keys().next().value as string);
    this.at.set(name, nowSecs);
    return true;
  }
}
