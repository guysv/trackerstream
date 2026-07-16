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
import { peerIdFromString } from "@libp2p/peer-id";
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

  // Reserve a circuit slot on the master so this browser is DIALABLE (…/p2p-circuit/webrtc) and can
  // SERVE blocks to other peers instead of only leeching — parity with a NATed desktop. Two triggers,
  // deliberately overlapping:
  //   - each of the master's live addrs + "/p2p-circuit" makes the circuit-relay listener reserve on
  //     it DETERMINISTICALLY at startup (the CircuitListen path opens/reuses the master connection and
  //     sends RESERVE — boot.addrs already carry /p2p/<masterId>, which the matcher requires). No
  //     reliance on relay discovery finding the master on its own.
  //   - bare "/p2p-circuit" keeps discovery running so the reservation RE-ESTABLISHES on reconnect
  //     after the master's per-restart certhash rotation: the certhash-bearing configured addrs above
  //     are stale by then, but the discovery topology re-fires on the supervisor's fresh connection.
  // "/webrtc" is where the private-to-private SDP upgrade lands once a reservation exists; @libp2p/webrtc's
  // listener turns each reserved "…/p2p-circuit" into the dialable "…/p2p-circuit/webrtc" we advertise.
  const listenAddrs = [...boot.addrs.map((a) => `${a}/p2p-circuit`), "/p2p-circuit", "/webrtc"];

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
    // A browser cannot listen on a socket; it becomes reachable ONLY by reserving on a relay. See
    // listenAddrs above — the master's addrs make the reservation deterministic, and a dialer then
    // reaches us over "…/p2p-circuit/webrtc" (relayed SDP + hole-punch to a direct datachannel).
    addresses: { listen: listenAddrs },
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

  const masterId = boot.peerId;

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

  const masterPeer = peerIdFromString(masterId);

  // Liveness by PROBE, not by inspection. A webrtc-direct connection to a seed that has restarted
  // stays status:'open' and listed in getConnections() until webrtc's own DTLS/ICE timeout — tens
  // of seconds during which every "are we connected?" check lies, a dial-to-peer dedupes onto the
  // corpse, and the app looks fine while talking to nobody. The only trustworthy signal is trying
  // to USE the connection: a ping that can't complete quickly means the master is gone, whatever
  // the connection object claims.
  const pingSvc = (libp2p.services as { ping?: { ping(p: typeof masterPeer, opts?: { signal?: AbortSignal }): Promise<number> } }).ping;
  const masterAlive = async (): Promise<boolean> => {
    if (!pingSvc) return libp2p.getConnections(masterPeer).some((c) => c.status === "open");
    try {
      // A HARD race, not just the AbortSignal: against a dead webrtc-direct connection js-libp2p's
      // ping can hang PAST its own abort (the DTLS/SCTP layer doesn't unwind promptly), and a probe
      // that never resolves would freeze the whole supervisor. The race guarantees a verdict in
      // bounded time regardless of what the transport does; a slow ping counts as "not alive", which
      // for our purpose (should we redial?) is the safe reading.
      const probe = pingSvc.ping(masterPeer, { signal: AbortSignal.timeout(3_000) });
      const timeout = new Promise<never>((_, rej) => setTimeout(() => rej(new Error("ping timeout")), 3_500));
      await Promise.race([probe, timeout]);
      return true;
    } catch {
      return false;
    }
  };

  // Re-fetch /bootstrap.json, then redial. The re-fetch is the whole point: a seed RESTART is the
  // common reason we lose the master, and a restart ROTATES the certhash — so the addr we hold is
  // already dead and dialing it again just fails. Only the endpoint knows the live one.
  let attempt = 0;
  const redialOnce = async (): Promise<void> => {
    try {
      const fresh = await fetchBootstrap();
      // Tear down the corpse BEFORE dialing. hangUp closes the zombie connection and, with the
      // peerStore purge, stops libp2p from deduping the new dial onto the dead certhash — the exact
      // reason a plain redial "succeeds" and still talks to nobody.
      await libp2p.hangUp(masterPeer).catch(() => {});
      await libp2p.peerStore.delete(masterPeer).catch(() => {});
      await dial(fresh);
      attempt = 0;
    } catch (e) {
      if (import.meta.env?.DEV || import.meta.env?.VITE_EXPOSE_NODE) {
        console.warn(`[redial] attempt ${attempt} failed:`, e instanceof Error ? e.message : e);
      }
      attempt++;
    }
  };

  // One supervisor, not an event listener: peer:disconnect fires late (and sometimes on a transient
  // blip) precisely because of the zombie problem above, so we don't trust it as the trigger. Poll
  // liveness instead, and redial when the probe says the master is unreachable — backing off only
  // while it stays down so a genuinely-offline seed isn't a hot loop.
  //
  // BEST-EFFORT, KNOWN-LIMITED. This recovers the common case (seed restart -> new certhash) in
  // local testing, but webrtc-direct connection-death is genuinely hard to observe in js-libp2p: a
  // zombie connection reads healthy for tens of seconds and even the liveness ping can lag, so
  // recovery latency is measured in seconds-to-tens-of-seconds and the tail is flaky. A real fix
  // needs the js-libp2p webrtc transport to surface connection death promptly — which lands in the
  // planned js-libp2p fork (also needed for go<->js private-webrtc interop). Until then this is a
  // strict improvement over "a seed restart requires a manual reload", not a guarantee.
  let supervising = false;
  const supervisor = setInterval(async () => {
    if (supervising) return;
    supervising = true;
    try {
      if (await masterAlive()) {
        attempt = 0;
        return;
      }
      await redialOnce();
      const backoff = Math.min(1000 * 2 ** attempt, 15_000);
      if (attempt > 0) await new Promise((r) => setTimeout(r, backoff));
    } finally {
      supervising = false;
    }
  }, 4_000);
  // (No unref: setInterval returns a number in the browser, not a Node Timeout — nothing to unref,
  // and `"unref" in <number>` would throw.)

  // Kept for the TsNode.redial() contract (a caller can force one), but the supervisor is what
  // actually keeps us connected.
  const redial = redialOnce;

  return { libp2p, helia, redial };
}
