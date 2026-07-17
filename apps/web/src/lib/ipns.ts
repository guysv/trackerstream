// Resolving the catalog's IPNS name, browser-side.
//
// tsnode publishes the catalog root two ways (node/ipns.go PublishIPNS): a DHT PutValue under the
// standard IPNS routing key, and a push on the `/trackerstream/catalog/1.0.0` gossipsub topic. The
// gossip path is the fast one for a running peer — but the master only publishes on ingest (daily),
// so a browser that just opened cannot wait for the next push. It must ASK.
//
// It asks the peers it is ALREADY connected to, DIRECTLY, before falling back to a full DHT walk.
// This is the whole reason boot is fast: measured, the master answers a raw GET_VALUE in ~150ms, but
// kad-dht's get() query takes 10-13s for the identical lookup — it burns that time on routing-table
// warmup, disjoint-path exploration and verify-pings even though the authoritative publisher (the
// master) is one hop away and holds the record. So we send the DHT GET_VALUE ourselves, over the
// open connection, and only if no connected peer answers do we pay for the walk.
//
// The records themselves are stock IPNS V2 (boxo `ipns`), and the JS `ipns` package validates them
// byte-for-byte — asserted by node/ipns_jsinterop_test.go against a frozen JS-signed vector, so
// this cannot silently drift from what the Go and Rust verifiers accept. Verifying the signature
// against the NAME's public key is what makes the direct-ask safe: a peer (even the seed) cannot
// forge a newer record, so asking it directly is no less trustworthy than a full DHT query.
import { peerIdFromString } from "@libp2p/peer-id";
import { CATALOG_IPNS_KEY } from "@trackerstream/config";
import { multihashToIPNSRoutingKey, unmarshalIPNSRecord } from "ipns";
import { ipnsValidator } from "ipns/validator";
import type { PeerId } from "@libp2p/interface";
import type { Libp2p } from "libp2p";

/** The gossip topic tsnode pushes catalog records on. The envelope is homegrown — NOT the libp2p
 *  ipns-pubsub spec: plain JSON `{name: <base58 PeerId>, record: <base64 IPNS V2 record>}`. */
export const CATALOG_TOPIC = "/trackerstream/catalog/1.0.0";

/** The custom-namespaced Kademlia protocol (node/config.go DHTPrefix). Must match node.ts. */
const DHT_PROTOCOL = "/trackerstream/kad/1.0.0";

/** How long to wait on the DIRECT ask of already-connected peers before giving up on it and falling
 *  back to the DHT walk. The master answers in ~150ms; this is generous slack for a slow link. */
const DIRECT_TIMEOUT_MS = 4_000;

/** Bound the DHT-walk FALLBACK. Only reached when no connected peer answered directly — a genuinely
 *  cold overlay — so a silent hang here reads to the user as "the app is broken". */
const RESOLVE_TIMEOUT_MS = 20_000;

const dbg = (): boolean => Boolean(import.meta.env?.DEV || import.meta.env?.VITE_EXPOSE_NODE);

// ---- minimal DHT wire codec (Kademlia Message, frozen protocol; see @libp2p/kad-dht message/dht) ----
// We only need to WRITE a GET_VALUE request (type + key) and READ the `record` field out of the
// reply, so we hand-roll the two protobuf fragments rather than import kad-dht internals (its Message
// codec is not part of the package's public exports, and a deep import breaks the static build).
//   Message.type   = field 1, varint     (GET_VALUE = 1)
//   Message.key    = field 2, bytes
//   Message.record = field 3, bytes       <- the IPNS record we want, in the reply

const MSG_TYPE_GET_VALUE = 1;

/** Encode an unsigned varint (protobuf base-128). */
function uvarint(n: number): number[] {
  const out: number[] = [];
  while (n >= 0x80) {
    out.push((n & 0x7f) | 0x80);
    n >>>= 7;
  }
  out.push(n);
  return out;
}

/** Read an unsigned varint at `off`, returning [value, nextOffset]. */
function readUvarint(buf: Uint8Array, off: number): [number, number] {
  let shift = 0;
  let val = 0;
  let i = off;
  for (;;) {
    const b = buf[i++];
    val += (b & 0x7f) * 2 ** shift; // avoids the 32-bit ceiling of <<
    if ((b & 0x80) === 0) break;
    shift += 7;
  }
  return [val, i];
}

/** A framed GET_VALUE request: the message body, prefixed with its own uvarint length (the framing
 *  it-protobuf-stream / go-msgio use on the DHT stream). */
function encodeGetValue(key: Uint8Array): Uint8Array {
  const body = [0x08, MSG_TYPE_GET_VALUE, 0x12, ...uvarint(key.length), ...key]; // field1 varint, field2 bytes
  return Uint8Array.from([...uvarint(body.length), ...body]);
}

/** Scan a protobuf message for a length-delimited field, returning its bytes or null. Handles the
 *  wire types our two messages use (varint, length-delimited) and skips anything else defensively. */
function extractField(body: Uint8Array, wantField: number): Uint8Array | null {
  let i = 0;
  while (i < body.length) {
    const [tag, next] = readUvarint(body, i);
    i = next;
    const field = tag >>> 3;
    const wire = tag & 0x7;
    if (wire === 0) {
      [, i] = readUvarint(body, i); // varint value
    } else if (wire === 2) {
      const [len, afterLen] = readUvarint(body, i);
      i = afterLen;
      if (field === wantField) return body.subarray(i, i + len);
      i += len;
    } else if (wire === 5) {
      i += 4;
    } else if (wire === 1) {
      i += 8;
    } else {
      break; // unknown wire type — bail rather than misparse
    }
  }
  return null;
}

/** The DHT reply's `record` (Message field 3) is a serialized Libp2pRecord `{key, value, ...}`, NOT
 *  the raw IPNS record — the IPNS record is its `value` (field 2). kad-dht's get() unwraps this for
 *  you; doing it by hand we must peel both layers. Returns the raw IPNS record bytes or null. */
function unwrapIpnsRecord(messageBody: Uint8Array): Uint8Array | null {
  const libp2pRecord = extractField(messageBody, 3); // Message.record = Libp2pRecord bytes
  if (!libp2pRecord) return null;
  const value = extractField(libp2pRecord, 2); // Libp2pRecord.value = IPNS record bytes
  return value && value.length > 0 ? value : null;
}

/** Pull one uvarint-length-prefixed frame off a libp2p stream's source, returning the message body
 *  (without the length prefix). Responses are small (~1.4 KB), usually one chunk. */
async function readFrame(source: AsyncIterable<{ subarray(): Uint8Array } | Uint8Array>): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let total = 0;
  for await (const chunk of source) {
    const u = chunk instanceof Uint8Array ? chunk : chunk.subarray();
    parts.push(u);
    total += u.length;
    // Try to parse the frame length once we have a few bytes, then wait for the full body.
    const buf = parts.length === 1 ? parts[0] : concat(parts, total);
    try {
      const [len, headerEnd] = readUvarint(buf, 0);
      if (buf.length - headerEnd >= len) return buf.subarray(headerEnd, headerEnd + len);
    } catch {
      /* not enough bytes for the length varint yet — keep reading */
    }
  }
  throw new Error("ipns: stream ended before a full frame");
}

function concat(parts: Uint8Array[], total: number): Uint8Array {
  const out = new Uint8Array(total);
  let w = 0;
  for (const p of parts) {
    out.set(p, w);
    w += p.length;
  }
  return out;
}

type Resolved = { seq: bigint; value: string };

/** Ask ONE connected peer for the record directly, over the DHT protocol. Returns the validated
 *  record or null (peer doesn't speak DHT / has no record / sent junk). Never throws for the caller. */
async function askPeer(libp2p: Libp2p, peer: PeerId, key: Uint8Array, signal: AbortSignal): Promise<Resolved | null> {
  let stream;
  try {
    stream = await libp2p.dialProtocol(peer, DHT_PROTOCOL, { signal });
  } catch {
    return null; // peer doesn't support the DHT protocol, or the dial failed
  }
  try {
    await stream.sink(
      (async function* () {
        yield encodeGetValue(key);
      })(),
    );
    const body = await readFrame(stream.source);
    const record = unwrapIpnsRecord(body);
    if (!record) return null; // peer connected but holds no value for the key
    await ipnsValidator(key, record); // signature + EOL — throws on forged/expired/malformed
    const rec = unmarshalIPNSRecord(record);
    return { seq: rec.sequence, value: rec.value };
  } catch {
    return null;
  } finally {
    await stream.close().catch(() => {});
  }
}

/** Resolve `/ipns/<name>` to a root CID, verifying the record locally.
 *
 *  Fast path: ask every already-connected peer directly, in parallel, and take the newest valid
 *  record any of them return. Fallback: a full kad-dht walk, only if the direct ask finds nothing. */
export async function resolveIpns(libp2p: Libp2p, name = CATALOG_IPNS_KEY): Promise<string> {
  const key = multihashToIPNSRoutingKey(peerIdFromString(name).toMultihash());

  // --- fast path: the peers we already hold connections to (the master is always one of them) ---
  const peers = libp2p.getConnections().map((c) => c.remotePeer);
  if (peers.length > 0) {
    const t0 = performance.now();
    const signal = AbortSignal.timeout(DIRECT_TIMEOUT_MS);
    const results = await Promise.all(peers.map((p) => askPeer(libp2p, p, key, signal)));
    let best: Resolved | null = null;
    for (const r of results) if (r && (!best || r.seq > best.seq)) best = r; // newest-sequence-wins
    if (dbg()) {
      const hits = results.filter(Boolean).length;
      console.info(`[ipns] direct ask: ${hits}/${peers.length} peers answered in ${Math.round(performance.now() - t0)}ms`);
    }
    if (best) return toCid(best.value, name);
  }

  // --- fallback: full DHT walk (cold overlay — no connected peer had the record) ---
  const dht = (libp2p.services as { dht?: any }).dht;
  if (!dht) throw new Error("ipns: no DHT service");
  const signal = AbortSignal.timeout(RESOLVE_TIMEOUT_MS);
  const t0 = performance.now();
  let best: Resolved | null = null;
  for await (const ev of dht.get(key, { signal })) {
    if (ev.name !== "VALUE") continue;
    try {
      await ipnsValidator(key, ev.value);
      const rec = unmarshalIPNSRecord(ev.value);
      if (!best || rec.sequence > best.seq) best = { seq: rec.sequence, value: rec.value };
    } catch {
      /* an invalid record from one peer is not fatal — keep asking the others */
    }
  }
  if (dbg()) console.info(`[ipns] DHT-walk fallback: ${Math.round(performance.now() - t0)}ms`);
  if (!best) throw new Error(`ipns: no valid record for ${name}`);
  return toCid(best.value, name);
}

function toCid(value: string, name: string): string {
  const cid = value.replace(/^\/ipfs\//, "");
  if (!cid) throw new Error(`ipns: record for ${name} has no /ipfs/<cid> value`);
  return cid;
}
