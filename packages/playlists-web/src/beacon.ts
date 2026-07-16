// Hold beacons — a port of node/beacon.go's wire format.
//
// A beacon says "I hold these playlists" without saying which peer holds what to anyone who wasn't
// listening: names are truncated to sha256(name)[:8]. Publishing it from the browser is ~30 lines
// and it makes web peers contribute to the backer counts DESKTOP users see.
import { sha256 } from "multiformats/hashes/sha2";

const BEACON_VERSION = 0x01;

/** sha256(name)[:8] — the beacon identifier for a playlist name. */
export async function nameHash8(name: string): Promise<Uint8Array> {
  const d = await sha256.digest(new TextEncoder().encode(name));
  return d.digest.subarray(0, 8);
}

/** `[version][uvarint N][N x 8-byte hash]` */
export function encodeBeacon(hashes: Uint8Array[]): Uint8Array {
  const head: number[] = [BEACON_VERSION];
  let n = hashes.length;
  while (n >= 0x80) {
    head.push((n & 0x7f) | 0x80);
    n >>>= 7;
  }
  head.push(n);
  const out = new Uint8Array(head.length + hashes.length * 8);
  out.set(head, 0);
  hashes.forEach((h, i) => out.set(h, head.length + i * 8));
  return out;
}

/** Signalled by decodeBeacon when the frame is a newer, unknown beacon version — IGNORE it (never
 *  penalise a peer that merely upgraded), the same forward-compat stance the playlist wire takes. */
export class UnknownBeaconVersion extends Error {}

/** Inverse of encodeBeacon — the counting half (node/beacon.go's decodeBeacon). Returns the 8-byte
 *  name hashes so the caller can tally distinct origins per playlist. Throws UnknownBeaconVersion on a
 *  future version and a plain Error on a structurally-malformed frame. Caps N so a hostile length can't
 *  make us allocate unboundedly. */
export function decodeBeacon(bytes: Uint8Array): Uint8Array[] {
  if (bytes.length < 1) throw new Error("beacon: empty");
  if (bytes[0] !== BEACON_VERSION) throw new UnknownBeaconVersion(`beacon version ${bytes[0]}`);
  let i = 1;
  let n = 0;
  let shift = 0;
  for (;;) {
    if (i >= bytes.length) throw new Error("beacon: truncated length");
    const b = bytes[i++];
    n |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7;
    if (shift > 28) throw new Error("beacon: length overflow");
  }
  n >>>= 0;
  if (n > 4096) throw new Error("beacon: too many hashes");
  if (bytes.length - i !== n * 8) throw new Error("beacon: body length mismatch");
  const out: Uint8Array[] = [];
  for (let k = 0; k < n; k++) out.push(bytes.subarray(i + k * 8, i + k * 8 + 8));
  return out;
}

const BEACON_WINDOW_SECS = 24 * 60 * 60; // popularity window (matches beacon.go's 24h originSet prune)
const BEACON_MIN_GAP_SECS = 2 * 60; // per-origin min gap (beaconValidator's rate floor)
const BEACON_MAX_ORIGINS = 512; // per-name origin cap (originSet bound)

const hex = (b: Uint8Array): string => Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");

/** The 8-byte name hash as a hex key — the ledger keys and the backer lookup must agree on it. */
export const beaconHashHex = (h: Uint8Array): string => hex(h);

/** Distinct-origin backer counts over a 24h window — the counting half of the beacon protocol
 *  (node/beacon.go's originSet + backers). Keyed by the 8-byte name hash, so a playlist's popularity
 *  is countable without anyone disclosing which peer holds what. A per-origin min-gap (2 min) mirrors
 *  the Go beaconValidator, and the per-name origin table is capped so a flood can't grow it unbounded.
 *  `nowSecs` is passed in so the caller owns the clock. */
export class BeaconLedger {
  private byHash = new Map<string, Map<string, number>>(); // nameHashHex -> (origin -> lastSeenSecs)

  record(origin: string, hashes: Uint8Array[], nowSecs: number): void {
    for (const h of hashes) {
      const key = hex(h);
      let origins = this.byHash.get(key);
      if (!origins) {
        origins = new Map();
        this.byHash.set(key, origins);
      }
      const last = origins.get(origin);
      if (last !== undefined && nowSecs - last < BEACON_MIN_GAP_SECS) continue; // too soon from this origin
      origins.delete(origin); // re-insert to keep recent in the bounded eviction order
      if (origins.size >= BEACON_MAX_ORIGINS) origins.delete(origins.keys().next().value as string);
      origins.set(origin, nowSecs);
    }
  }

  count(nameHashHex: string, nowSecs: number): number {
    const origins = this.byHash.get(nameHashHex);
    if (!origins) return 0;
    let n = 0;
    for (const [o, t] of origins) {
      if (nowSecs - t < BEACON_WINDOW_SECS) n++;
      else origins.delete(o); // prune stale origins on read
    }
    return n;
  }
}
