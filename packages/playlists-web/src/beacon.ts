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
