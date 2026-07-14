// The playlist gossip envelope — a port of node/playlist.go.
//
// The document travels INLINE next to its signed IPNS record. That is the single fact that makes
// browser publishing possible at all: nobody ever dials the author to fetch the doc, so a browser
// (which cannot be dialled) is a first-class publisher. From playlist.go's own header: "no bitswap,
// no blockstore, no pinning", and "pubsub+seq ONLY — deliberately no DHT writes".
import { peerIdFromString } from "@libp2p/peer-id";
import { CID } from "multiformats/cid";
import { unmarshalIPNSRecord } from "ipns";
import { ipnsValidator, multihashToIPNSRoutingKey } from "./ipns-compat.ts";
import { DOC_MAX, docCid } from "./doc.ts";

export const PLAYLIST_TOPIC = "/trackerstream/playlist/1.0.0";
export const BEACON_TOPIC = "/trackerstream/playlist-beacon/1.0.0";

const MSG_VERSION = 0x01;
const NAME_MAX = 128;
const RECORD_MAX = 10 << 10;

/** An unknown leading version byte means IGNORE, never REJECT. The whole peer-scoring doctrine
 *  rests on it: rejecting a message we simply don't understand yet would penalise honest peers that
 *  have merely upgraded, and a newer network could score the older half of the mesh into isolation. */
export class UnknownWireVersion extends Error {
  constructor() {
    super("unknown wire message version");
    this.name = "UnknownWireVersion";
  }
}

function uvarint(n: number): Uint8Array {
  const out: number[] = [];
  while (n >= 0x80) {
    out.push((n & 0x7f) | 0x80);
    n >>>= 7;
  }
  out.push(n);
  return new Uint8Array(out);
}

function readUvarint(b: Uint8Array, at: number): [value: number, next: number] {
  let x = 0;
  let s = 0;
  for (let i = at; i < b.length; i++) {
    const c = b[i];
    if (c < 0x80) return [x | (c << s), i + 1];
    x |= (c & 0x7f) << s;
    s += 7;
    if (s > 35) break;
  }
  throw new Error("malformed playlist envelope frame");
}

/** `[version][uvarint-len name][name][uvarint-len rec][rec][uvarint-len doc][doc]` */
export function encodePlaylistMsg(name: string, rec: Uint8Array, doc: Uint8Array): Uint8Array {
  const nb = new TextEncoder().encode(name);
  const parts: Uint8Array[] = [new Uint8Array([MSG_VERSION])];
  for (const p of [nb, rec, doc]) {
    parts.push(uvarint(p.length), p);
  }
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let w = 0;
  for (const p of parts) {
    out.set(p, w);
    w += p.length;
  }
  return out;
}

export function decodePlaylistMsg(b: Uint8Array): { name: string; rec: Uint8Array; doc: Uint8Array } {
  // Version dispatch FIRST: an unknown-newer version must never reach the frame logic below, which
  // is only valid within THIS version's layout.
  if (b.length === 0 || b[0] !== MSG_VERSION) throw new UnknownWireVersion();
  let at = 1;
  const frame = (max: number): Uint8Array => {
    const [len, next] = readUvarint(b, at);
    if (len > max || next + len > b.length) throw new Error("malformed playlist envelope frame");
    at = next + len;
    return b.subarray(next, at);
  };
  const nb = frame(NAME_MAX);
  const rec = frame(RECORD_MAX);
  const doc = frame(DOC_MAX);
  if (at !== b.length) throw new Error("trailing bytes in playlist envelope");
  return { name: new TextDecoder().decode(nb), rec, doc };
}

/**
 * Fully verify a playlist message. The result is self-certifying: a forged, tampered or expired one
 * never rides the mesh.
 *
 * This runs in TWO places, and both matter. As the gossipsub topic validator — a browser leaf still
 * forwards to its mesh, so it must not relay garbage. And again at ingest, mirroring the Rust, which
 * re-verifies regardless of what the node already checked.
 *
 * Returns the record's sequence number.
 */
export async function validatePlaylistMsg(name: string, rec: Uint8Array, doc: Uint8Array): Promise<bigint> {
  if (doc.length === 0 || doc.length > DOC_MAX) {
    throw new Error(`playlist doc size ${doc.length} out of bounds`);
  }
  const peerId = peerIdFromString(name); // throws on a bad name
  // Signature AND EOL — the JS `ipns` validator checks both, as boxo's Validate does.
  await ipnsValidator(multihashToIPNSRoutingKey(peerId.toMultihash()), rec);
  const r = unmarshalIPNSRecord(rec);

  const value = r.value;
  if (!value.startsWith("/ipfs/")) throw new Error(`record value ${value} is not /ipfs/`);
  const c = CID.parse(value.slice("/ipfs/".length));
  // The doc must hash to the CID the record signed over. This is what binds the (unsigned) document
  // to the (signed) record — without it, anyone could staple any doc to a valid record.
  const chk = await docCid(doc);
  if (!chk.equals(c)) throw new Error(`doc does not hash to record cid ${c}`);
  return r.sequence;
}
