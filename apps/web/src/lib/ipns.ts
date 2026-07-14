// Resolving the catalog's IPNS name, browser-side.
//
// tsnode publishes the catalog root two ways (node/ipns.go PublishIPNS): a DHT PutValue under the
// standard IPNS routing key, and a push on the `/trackerstream/catalog/1.0.0` gossipsub topic. The
// gossip path is the fast one for a running peer — but the master only publishes on ingest (daily),
// so a browser that just opened cannot wait for the next push. It must ASK, which means the DHT.
//
// The records themselves are stock IPNS V2 (boxo `ipns`), and the JS `ipns` package validates them
// byte-for-byte — asserted by node/ipns_jsinterop_test.go against a frozen JS-signed vector, so
// this cannot silently drift from what the Go and Rust verifiers accept.
import { peerIdFromString } from "@libp2p/peer-id";
import { CATALOG_IPNS_KEY } from "@trackerstream/config";
import { multihashToIPNSRoutingKey, unmarshalIPNSRecord } from "ipns";
import { ipnsValidator } from "ipns/validator";
import type { Libp2p } from "libp2p";

/** The gossip topic tsnode pushes catalog records on. The envelope is homegrown — NOT the libp2p
 *  ipns-pubsub spec: plain JSON `{name: <base58 PeerId>, record: <base64 IPNS V2 record>}`. */
export const CATALOG_TOPIC = "/trackerstream/catalog/1.0.0";

/** Resolve `/ipns/<name>` to a root CID over the custom DHT, verifying the record locally.
 *
 *  The node is an untrusted cache: we verify the signature against the NAME's public key, so no
 *  peer (and no seed) can forge a newer record. Same trust anchor the desktop uses. */
export async function resolveIpns(libp2p: Libp2p, name = CATALOG_IPNS_KEY): Promise<string> {
  const peerId = peerIdFromString(name);
  const key = multihashToIPNSRoutingKey(peerId.toMultihash());

  const dht = (libp2p.services as { dht?: any }).dht;
  if (!dht) throw new Error("ipns: no DHT service");

  let best: { seq: bigint; value: string } | null = null;
  for await (const ev of dht.get(key)) {
    if (ev.name !== "VALUE") continue;
    try {
      // Signature + EOL. Throws on a forged, expired or malformed record.
      await ipnsValidator(key, ev.value);
      const rec = unmarshalIPNSRecord(ev.value);
      const value = rec.value; // "/ipfs/<cid>"
      // Newest-sequence-wins: the IPNS sequence is the total order, and several peers may answer.
      if (!best || rec.sequence > best.seq) best = { seq: rec.sequence, value };
    } catch {
      /* an invalid record from one peer is not fatal — keep asking the others */
    }
  }
  if (!best) throw new Error(`ipns: no valid record for ${name}`);
  const cid = best.value.replace(/^\/ipfs\//, "");
  if (!cid) throw new Error(`ipns: record for ${name} has no /ipfs/<cid> value`);
  return cid;
}
