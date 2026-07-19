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
import type { BitswapLike } from "./blockfetch.ts";
import { circuitRelayTransport } from "@libp2p/circuit-relay-v2";
import { FaultTolerance } from "@libp2p/interface";
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
import { libp2pRouting } from "@helia/routers";
import { CATALOG_TOPIC } from "./ipns.ts";
import { startDonorDiscovery } from "./offload.ts";
import { BandwidthTracker, bandwidthMetrics } from "./bandwidth.ts";
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
  /** The live @helia/bitswap instance (reached through the block broker). Exposed so the media block
   *  path can drive bitswap's SINGLE-PEER want primitives directly (seed-offloading fetch, see
   *  blockfetch.ts) instead of the default broadcast-to-everyone `blockstore.get`. */
  bitswap: BitswapLike;
  /** The seed's PeerId (from /bootstrap.json). The one peer the offload fetch never asks for bytes
   *  while a donor holds the block. */
  masterId: string;
  /** Per-peer up/down byte counter (peers-pane attribution), parity with the desktop's go-libp2p
   *  BandwidthCounter. Read by web.ts peers()/peerDetail(). */
  bandwidth: BandwidthTracker;
  /** Re-fetch /bootstrap.json and redial. Call on any dial failure — the certhash has probably
   *  rotated under us. */
  redial(): Promise<void>;
}

export async function startNode(): Promise<TsNode> {
  const [key, boot] = await Promise.all([loadOrCreateKey(), fetchBootstrap()]);

  // Per-peer bandwidth: supply a metrics component so libp2p's byte hooks (which it already calls but
  // that no-op without one) tally up/down per peer. All-protocol, matching the desktop counter.
  const bandwidth = new BandwidthTracker();

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
    // Per-peer byte accounting (see bandwidth.ts). A `(components) => Metrics` factory; ours needs no
    // components. Without this the trackProtocolStream/trackMultiaddrConnection hooks are no-ops.
    metrics: bandwidthMetrics(bandwidth),
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
    // Do NOT let a failed listen kill the whole node. js-libp2p defaults to FATAL_ALL: if ANY listen
    // address fails to bind, createLibp2p throws and the tab shows "could not join the swarm" — nothing
    // works. But EVERY one of our listen addrs is a circuit-relay reservation on the master
    // (…/p2p-circuit and /webrtc, which need a reservation to listen at all). When the master can't
    // grant one — it just restarted, is momentarily non-public, or is briefly unreachable — the whole
    // boot FAILS, even though our DIRECT webrtc-direct connection to it (Bitswap + catalog) would work
    // fine. Reservations only make us dialable BY OTHERS; they are not required to consume. NO_FATAL
    // lets the node boot on whatever bound, so a browser always comes up and serves/plays, and the
    // reservation re-establishes on its own when the master is ready (the discovery topology re-fires).
    // Observed live 2026-07-19: a master restart left every browser unable to boot until this.
    transportManager: { faultTolerance: FaultTolerance.NO_FATAL },
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
      // doPX (R6 star→mesh): when we prune a topic-mesh peer, hand it signed peer records of other
      // members so it re-meshes without a DHT walk — parity with node/pubsub.go's WithPeerExchange.
      // Low-impact on a browser (it rarely overflows a mesh and prunes), but symmetric and harmless.
      pubsub: gossipsub({ maxInboundDataLength: MAX_PUBSUB_MSG, doPX: true }),
    },
  });

  // The IDB stores must be opened before Helia touches them — createHelia does not do it for us.
  const blockstore = new IDBBlockstore("ts-blocks");
  const datastore = new IDBDatastore("ts-data");
  await Promise.all([blockstore.open(), datastore.open()]);

  // Wrap the bitswap block-broker factory so we keep a reference to the Bitswap instance it builds.
  // The broker's public `.bitswap` field is the only handle to the single-peer want primitives
  // (wantSessionPresence/wantSessionBlock) the seed-offloading media fetch needs (see blockfetch.ts);
  // Helia never surfaces it otherwise. Pure factory composition — no reach into Helia internals.
  let bitswapRef: BitswapLike | undefined;
  const captureBitswap = () => {
    const make = bitswap();
    return (components: Parameters<ReturnType<typeof bitswap>>[0]) => {
      const broker = make(components);
      bitswapRef = (broker as unknown as { bitswap: BitswapLike }).bitswap;
      return broker;
    };
  };

  const helia = await createHelia({
    libp2p,
    blockstore,
    datastore,
    // Bitswap ONLY. No trustless-gateway fallback on purpose: if this works, it worked over libp2p,
    // and a silent HTTP fallback would hide a broken data plane behind a working-looking UI.
    blockBrokers: [captureBitswap()],
    // Route ONLY over our custom libp2p DHT. Helia otherwise defaults `routers` to
    // [libp2pRouting, httpGatewayRouting()], and httpGatewayRouting() points at public gateways
    // (https://4everland.io by default) that answer findProviders for EVERY cid with a bogus
    // HTTP-gateway "provider" and don't honour the abort signal — which both poisoned discovery on
    // this private overlay with junk records and made findProviders hang. We have no gateway block
    // broker to consume those providers anyway, so the HTTP router is pure noise. Drop it.
    routers: [libp2pRouting(libp2p)],
  });

  const masterId = boot.peerId;

  // Every master dial MUST be bounded. A webrtc-direct dial has no inherent deadline, and against a
  // master that still holds a half-open connection to our PeerId — the exact state right after a
  // multi-tab failover promotes a new node under the SAME persisted identity — the dial neither
  // succeeds nor fails: it HANGS. An unbounded hang inside redialOnce() below would leave the
  // supervisor's `supervising` flag stuck true forever, so every later tick early-returns and the
  // node never reconnects (observed: promoted tab sits at 0 peers indefinitely). The timeout turns a
  // hang into a fast failure the supervisor can back off and retry — so once the master finally drops
  // the departed tab's connection, a redial lands.
  const DIAL_TIMEOUT_MS = 12_000;
  const dial = async (b: Bootstrap): Promise<void> => {
    let last: unknown;
    for (const a of b.addrs) {
      try {
        await libp2p.dial(multiaddr(a), { signal: AbortSignal.timeout(DIAL_TIMEOUT_MS) });
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

  // Track the seed's certhash so recovery can tell a RESTART from a same-certhash DROP. A restart
  // ROTATES the certhash, so the fresh bootstrap advertises a genuinely-new, dialable endpoint and a
  // redial works. A drop that ISN'T a restart (idle death, network blip) keeps the SAME certhash — and
  // js-libp2p cannot re-dial a webrtc-direct connection to a certhash it has already closed: the seed
  // still holds our dead connection (webrtc death is slow to register there) and rejects the re-dial as
  // a duplicate, indefinitely (js-libp2p#1835). All of a bootstrap's addrs carry the one master cert.
  const certhashOf = (b: Bootstrap): string => {
    for (const a of b.addrs) {
      const h = a.split("/certhash/")[1]?.split("/")[0];
      if (h) return h;
    }
    return "";
  };
  let currentCerthash = certhashOf(boot);

  let attempt = 0;
  // Dial a fresh bootstrap. Tear down the corpse FIRST — hangUp closes the zombie and the peerStore
  // purge stops libp2p deduping the new dial onto the dead certhash — then dial and record the certhash
  // we are now on.
  const dialFresh = async (fresh: Bootstrap): Promise<void> => {
    await libp2p.hangUp(masterPeer).catch(() => {});
    await libp2p.peerStore.delete(masterPeer).catch(() => {});
    await dial(fresh);
    currentCerthash = certhashOf(fresh) || currentCerthash;
  };

  // TsNode.redial(): force a re-fetch + dial. Kept for the contract (a caller can force one); the
  // supervisor below is what actually keeps us connected.
  const redial = async (): Promise<void> => {
    await dialFresh(await fetchBootstrap());
  };

  // Last-resort recovery: RELOAD to get a fresh node — the ONLY thing that reconnects a same-certhash
  // webrtc-direct drop (#1835). The PeerId is persisted (loadOrCreateKey), so a reload keeps our SAME
  // identity: reservations, provider records and beacons survive. Guarded against reload loops two
  // ways: (1) everConnected — reload only to recover a connection we actually HAD, so a reloaded
  // session that can't reach the seed leaves everConnected false and stops reloading; (2) a
  // sessionStorage cooldown, so even a flapping seed can't reload faster than RELOAD_COOLDOWN_MS.
  const RELOAD_COOLDOWN_MS = 90_000;
  let everConnected = true; // the initial `await dial(boot)` above succeeded
  const RELOAD_KEY = "ts:reconnect-reload-at";
  const canReload = (): boolean => {
    try {
      return Date.now() - Number(sessionStorage.getItem(RELOAD_KEY) ?? 0) > RELOAD_COOLDOWN_MS;
    } catch {
      return true;
    }
  };
  const reloadToRecover = (): void => {
    try {
      sessionStorage.setItem(RELOAD_KEY, String(Date.now()));
    } catch {
      /* private mode / storage disabled — reload anyway; everConnected still guards the loop */
    }
    location.reload();
  };

  const backoff = async (): Promise<void> => {
    attempt++;
    await new Promise((r) => setTimeout(r, Math.min(1000 * 2 ** attempt, 15_000)));
  };

  // One supervisor, not an event listener: peer:disconnect fires late (and sometimes on a transient
  // blip) precisely because of the zombie problem above, so we don't trust it as the trigger. Poll
  // liveness; on a dead seed, decide RESTART vs DROP by the live certhash:
  //   - bootstrap fetch FAILS  -> the endpoint (hence seed) is likely down: back off, NEVER reload.
  //   - certhash CHANGED       -> master restarted: redial the fresh, dialable endpoint.
  //   - certhash SAME          -> our conn is a zombie the seed still holds (#1835): reload under our
  //                               persisted PeerId. Recovers in ~one probe+fetch, not a grind of doomed
  //                               12s dials.
  let supervising = false;
  const supervisor = setInterval(async () => {
    if (supervising) return;
    supervising = true;
    try {
      if (await masterAlive()) {
        attempt = 0;
        everConnected = true;
        return;
      }
      const dev = import.meta.env?.DEV || import.meta.env?.VITE_EXPOSE_NODE;
      let fresh: Bootstrap;
      try {
        fresh = await fetchBootstrap();
      } catch (e) {
        if (dev) console.warn(`[redial] bootstrap fetch failed:`, e instanceof Error ? e.message : e);
        await backoff();
        return;
      }
      if (certhashOf(fresh) && certhashOf(fresh) !== currentCerthash) {
        try {
          await dialFresh(fresh);
          attempt = 0;
        } catch (e) {
          if (dev) console.warn(`[redial] attempt ${attempt} failed:`, e instanceof Error ? e.message : e);
          await backoff();
        }
        return;
      }
      if (everConnected && canReload()) {
        reloadToRecover();
        return;
      }
      await backoff(); // can't reload (cooldown, or never connected this session) — keep probing
    } finally {
      supervising = false;
    }
  }, 4_000);
  // (No unref: setInterval returns a number in the browser, not a Node Timeout — nothing to unref,
  // and `"unref" in <number>` would throw.)

  // Mesh membership — parity with a NATed desktop (minus DCUtR, which needs a UDP/TCP socket the
  // browser doesn't have). Two pieces:
  //  1. Join the catalog gossipsub topic. It is the ONE topic every node is in, so subscribing puts us
  //     in the universal mesh and relays catalog records the way a desktop does — where before the
  //     browser touched the catalog only via a one-shot DHT resolve at boot and stayed out of the mesh.
  //     (We don't re-point the already-resolved catalog on updates yet; that is a separate change.)
  //  2. Eagerly connect to the network's public donors (see startDonorDiscovery / node/fwd.go). Without
  //     it the browser is a lone leaf on the master; with it it pre-connects to the stable public peers
  //     that are also the best offload sources — the same findProviders(donorRendezvous)→dial loop a
  //     desktop's AutoRelay peer source runs.
  try {
    (libp2p.services as { pubsub?: { subscribe(t: string): void } }).pubsub?.subscribe(CATALOG_TOPIC);
  } catch {
    /* pubsub unexpectedly absent — non-fatal, the DHT resolve path still works */
  }
  startDonorDiscovery(libp2p);

  if (bitswapRef == null) throw new Error("bitswap broker was never constructed"); // createHelia always builds it
  return { libp2p, helia, bitswap: bitswapRef, masterId, bandwidth, redial };
}
