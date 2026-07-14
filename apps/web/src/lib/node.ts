// The browser's libp2p node.
//
// This IS trackerstream's node — not a client of one. Same Bitswap (`/ipfs/bitswap/1.2.0`, stock,
// no prefix), same gossipsub topics, same custom-prefixed DHT, same IPNS records. The desktop runs
// a Go tsnode beside it; here the tab is the peer.
//
// Verified by spike: a browser dials tsnode over webrtc-direct in ~150ms and Bitswap-fetches a real
// module byte-exactly, with Helia unmodified.
import { gossipsub } from "@chainsafe/libp2p-gossipsub";
import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { bitswap } from "@helia/block-brokers";
import { circuitRelayTransport } from "@libp2p/circuit-relay-v2";
import { generateKeyPair, privateKeyFromProtobuf, privateKeyToProtobuf } from "@libp2p/crypto/keys";
import { identify } from "@libp2p/identify";
import { kadDHT, passthroughMapper } from "@libp2p/kad-dht";
import { ping } from "@libp2p/ping";
import { webRTC, webRTCDirect } from "@libp2p/webrtc";
import { multiaddr } from "@multiformats/multiaddr";
import { ipnsValidator } from "ipns/validator";
import { ipnsSelector } from "ipns/selector";
import { BOOTSTRAP_URL } from "@trackerstream/config";
import { IDBBlockstore } from "blockstore-idb";
import { IDBDatastore } from "datastore-idb";
import { createHelia, type Helia } from "helia";
import { get as idbGet, set as idbSet } from "idb-keyval";
import { createLibp2p, type Libp2p } from "libp2p";

/** tsnode namespaces its DHT (node/config.go: DHTPrefix = "/trackerstream"), so a stock kad-dht
 *  would talk to nobody. The prefix is declared IMMORTAL on the Go side — never change it. */
const DHT_PROTOCOL = "/trackerstream/kad/1.0.0";

/** Advertised in the libp2p UserAgent. Bump with the client, not the wire. */
const VERSION = "0.1.0";

/** tsnode raises gossipsub's max message size to 2 MiB (node/pubsub.go). js-libp2p defaults to
 *  1 MiB, and a max-size playlist doc would be SILENTLY DROPPED — not an error, just a message that
 *  never arrives. Match it. */
const MAX_PUBSUB_MSG = 4 << 20;

/** What /bootstrap.json serves (node/cmd/tsedge). */
export interface Bootstrap {
  peerId: string;
  /** webrtc-direct multiaddrs, certhash included. */
  addrs: string[];
  iceServers: RTCIceServer[];
  ttl: number;
}

/** Our own key, persisted so the PeerId survives a reload.
 *
 *  Stability is not cosmetic: relay reservations, DHT provider records and playlist beacons are all
 *  keyed by PeerId, and a fresh identity on every page load would make this peer unfindable and its
 *  reservations unreusable. Stored in the SAME protobuf encoding node/keystore.go writes, which is
 *  what makes browser<->desktop key export/import possible at all. */
async function loadOrCreateKey() {
  const stored = await idbGet<Uint8Array>("ts:hostkey");
  if (stored) return privateKeyFromProtobuf(stored);
  const key = await generateKeyPair("Ed25519");
  await idbSet("ts:hostkey", privateKeyToProtobuf(key));
  return key;
}

/** Fetch the seed's live address.
 *
 *  A browser CANNOT bake in a multiaddr: go-libp2p regenerates its WebRTC certificate on every
 *  process start, so the master's /certhash changes on EVERY restart. This is also why callers must
 *  re-fetch and retry on a dial failure rather than treating the first answer as durable. */
export async function fetchBootstrap(): Promise<Bootstrap> {
  // Dev/CI can point at a local tsedge (see node/cmd/tsedge). Prod always uses the baked URL.
  const url = (import.meta.env?.VITE_BOOTSTRAP_URL as string | undefined) ?? BOOTSTRAP_URL;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`bootstrap: ${res.status} ${res.statusText}`);
  return (await res.json()) as Bootstrap;
}

export interface TsNode {
  libp2p: Libp2p;
  helia: Helia;
  /** Re-fetch /bootstrap.json and redial. Call on any dial failure — the certhash has probably
   *  rotated under us. */
  redial(): Promise<void>;
}

export async function startNode(): Promise<TsNode> {
  const [key, boot] = await Promise.all([loadOrCreateKey(), fetchBootstrap()]);

  const libp2p = await createLibp2p({
    // Identify ourselves on the wire the way tsnode does (trackerstream/<ver>/<role>). Without it a
    // web peer is anonymous, and node/control.go — which classifies peers by their agent string —
    // cannot tell a trackerstream browser from a stranger.
    nodeInfo: { name: "trackerstream", version: `${VERSION}/web` },
    // The libp2p ecosystem is mid-migration: @libp2p/peer-id pulls @libp2p/crypto@5.1.x, which
    // depends on @libp2p/interface@3, while libp2p@2.x's own types are built against interface@2.
    // Both are in the tree and the shapes are identical — it is a nominal clash, not a runtime one.
    // Cast once, here, rather than smearing `any` through the key handling.
    privateKey: key as unknown as NonNullable<Parameters<typeof createLibp2p>[0]>["privateKey"],
    // A browser cannot listen on a socket. The two entries below are for the browser<->browser mesh
    // (a relay reservation makes us dialable; /webrtc is the transport the SDP upgrade lands on) —
    // both are inert until a relay accepts a reservation.
    addresses: { listen: ["/p2p-circuit", "/webrtc"] },
    transports: [
      webRTCDirect(), // -> the master and any public seed. No STUN/TURN: the certhash IS the handshake.
      // -> other browsers. SDP is signalled over the circuit relay; ICE uses the seed's coturn,
      //    which is what keeps browser<->browser DIRECT instead of collapsing back onto the relay.
      //    Note bitswap REFUSES relayed (Limited) connections by design, so there is no fallback:
      //    if ICE fails, that pair simply exchanges nothing.
      webRTC({ rtcConfiguration: () => ({ iceServers: boot.iceServers }) }),
      circuitRelayTransport(),
    ],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    connectionGater: {
      // The seed may be reachable on a private/LAN address during development.
      denyDialMultiaddr: async () => false,
    },
    services: {
      identify: identify(),
      ping: ping(), // kad-dht depends on it (peer liveness); also what the peers pane reads for RTT
      dht: kadDHT({
        protocol: DHT_PROTOCOL,
        clientMode: true,
        // WITHOUT THESE THE CATALOG NEVER RESOLVES. js-kad-dht ships exactly one default record
        // validator — `pk` — so an /ipns/... record coming back from a GET_VALUE is rejected INSIDE
        // kad-dht before the caller ever sees it. The selector is what picks the newest record when
        // several peers answer (IPNS sequence order). Go's DHT registers the same pair
        // (dht.NamespacedValidator("ipns", ...) in node/node.go).
        validators: { ipns: ipnsValidator },
        selectors: { ipns: ipnsSelector },
        // js-kad-dht defaults to removePrivateAddressesMapper — sensible hygiene on the PUBLIC
        // Amino DHT, wrong here. This is a PRIVATE overlay (its own DHT prefix), where peers on
        // LANs and loopback are legitimate members; stripping their addresses leaves the routing
        // table with nothing to query and every lookup times out having asked nobody.
        peerInfoMapper: passthroughMapper,
      }),
      pubsub: gossipsub({ maxInboundDataLength: MAX_PUBSUB_MSG }),
    },
  });

  // The IDB stores must be opened before Helia touches them — createHelia does not do it for us.
  const blockstore = new IDBBlockstore("ts-blocks");
  const datastore = new IDBDatastore("ts-data");
  await Promise.all([blockstore.open(), datastore.open()]);

  const helia = await createHelia({
    libp2p,
    blockstore,
    datastore,
    // Bitswap ONLY. No trustless-gateway fallback on purpose: if this works, it worked over libp2p,
    // and a silent HTTP fallback would hide a broken data plane behind a working-looking UI.
    blockBrokers: [bitswap()],
  });

  const dial = async (b: Bootstrap): Promise<void> => {
    let last: unknown;
    for (const a of b.addrs) {
      try {
        await libp2p.dial(multiaddr(a));
        return;
      } catch (e) {
        last = e;
      }
    }
    throw new Error(`bootstrap: no addr dialable (${String(last)})`);
  };

  await dial(boot);

  return {
    libp2p,
    helia,
    redial: async () => dial(await fetchBootstrap()),
  };
}
