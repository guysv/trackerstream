// Offload: get bytes from someone other than the seed.
//
// TIER 1 — public desktops. Every tsnode now listens on webrtc-direct (node/config.go), so any
// publicly-reachable desktop is browser-dialable. The connection is DIRECT, so it is not flagged
// Limited and bitswap flows over it: no relay, no STUN, no reservation, no coturn. This is the whole
// mechanism, and it costs one listen address on their side and a findProviders call on ours.
//
// TIER 2 — other browsers, over WebRTC with SDP signalled across the seed's circuit relay and ICE
// hardened by STUN. That falls out of the libp2p config in node.ts; what this file adds is the other
// half: a browser that never `provide`s what it holds is invisible as a source, so the mesh has
// nothing to offload FROM.
//
// TIER 3 — NATed desktops, over private-to-private WebRTC (/p2p-circuit/webrtc): the tsnode now runs
// the webrtcprivate transport (node.go), so a browser signals SDP across the seed's relay and
// hole-punches to a DIRECT datachannel with a desktop that has no browser-dialable address of its
// own. This is what lifts the old "a browser can never reach a NATed desktop" limit.
import type { PeerId } from "@libp2p/interface";
import type { Multiaddr } from "@multiformats/multiaddr";
import type { Helia } from "helia";
import type { Libp2p } from "libp2p";
import { CID } from "multiformats/cid";
import { sha256 } from "multiformats/hashes/sha2";

/** libp2p refuses to dial an encryption-skipping transport (webrtc-direct) address that lacks a peer
 *  id — "outbound connection that skipped encryption must have a peer id". DHT provider records carry
 *  bare `…/webrtc-direct/certhash/…` with NO `/p2p`, so we must encapsulate the provider's id before
 *  dialing. A relayed `…/p2p-circuit/webrtc/p2p/<target>` addr already ends in the peer id, so those
 *  pass through untouched (this is exactly why browser↔browser offload worked but browser→desktop did
 *  not — desktops advertise the bare webrtc-direct form). */
const withPeerId = (m: Multiaddr, id: string): Multiaddr => (m.getPeerId() != null ? m : m.encapsulate(`/p2p/${id}`));

/** Per-address dial budget. A reachable webrtc-direct addr connects in ~60ms; this bounds how long we
 *  waste on an unreachable one before moving to the next. */
const PER_ADDR_DIAL_MS = 5_000;

/** Dial a peer's addresses ONE AT A TIME, first success wins. This works around three js-libp2p
 *  webrtc-direct dialer behaviours that together make a plain `dial(addrs)` fail whenever the address
 *  set mixes reachable and unreachable entries — which it almost always does (a desktop advertises
 *  loopback + LAN + public + tailscale, and only one category is reachable from any given browser):
 *    1. no happy-eyeballs — an unreachable webrtc-direct addr HANGS the whole peer-dial for the full
 *       timeout instead of failing fast, so one bad addr sinks the good ones;
 *    2. concurrent dials to one peer COALESCE, so Promise.any can't race them;
 *    3. a failed webrtc dial leaves per-peer state that blocks the next attempt — hence the hangUp.
 *  A reachable addr then connects in ~60ms; each unreachable one costs only PER_ADDR_DIAL_MS. */
async function dialPeerAddrs(libp2p: Libp2p, peer: PeerId, addrs: Multiaddr[]): Promise<boolean> {
  for (const addr of addrs) {
    if (libp2p.getConnections(peer).length > 0) return true; // already connected (this or a concurrent path)
    try {
      await libp2p.dial(addr, { signal: AbortSignal.timeout(PER_ADDR_DIAL_MS) });
      return true;
    } catch {
      // A failed webrtc-direct dial leaves a half-open PeerConnection / dial-backoff that makes the
      // NEXT addr time out too; tear it down so the next attempt starts clean.
      await libp2p.hangUp(peer).catch(() => {});
    }
  }
  return false;
}

/** A browser cannot dial TCP or QUIC. Anything else in a provider's address list is noise to us.
 *  The `webrtc` branch matches BOTH a direct `/webrtc-direct` (public desktop, Tier 1) and the
 *  `/webrtc` inside a `/p2p-circuit/webrtc/…` relayed address (NATed desktop, Tier 3) — libp2p picks
 *  the direct one when both are on offer, and falls to the circuit path otherwise.
 *
 *  Exported because it is also the "am I dialable?" test the node applies to its OWN addresses: once a
 *  relay reservation lands, `getMultiaddrs()` grows a `…/p2p-circuit/webrtc` that matches here, which
 *  is what flips this browser from leech to server (see node.ts, web.ts `reachable`). */
export const dialable = (addr: string): boolean =>
  /\/(webrtc-direct|p2p-circuit\/webrtc|webrtc|wss|tls\/ws|ws)(\/|$)/.test(addr);

/** How long to hunt for providers before giving up and letting bitswap ask the seed anyway. This is
 *  a best-effort warm, not a dependency: playback must never wait on the DHT. */
const FIND_TIMEOUT_MS = 4_000;
const MAX_DIALS = 4;

/**
 * Warm-connect the peers that hold `root`, BEFORE playback reaches it.
 *
 * Fire-and-forget by contract (the UI calls it when a track enters the queue). Bitswap then finds
 * the blocks on an already-connected peer instead of asking the seed. Failing is fine — the seed is
 * always there.
 */
export async function warmRoot(libp2p: Libp2p, root: string): Promise<void> {
  let cid: CID;
  try {
    cid = CID.parse(root);
  } catch {
    return;
  }
  const signal = AbortSignal.timeout(FIND_TIMEOUT_MS);
  const dialed = new Set<string>();
  try {
    // libp2p.contentRouting, NOT helia.routing: Helia's Routing.findProviders merges the router
    // output with a PERPETUAL FIND_PEER address-refresh queue generator that never ends and ignores
    // our AbortSignal, so the loop hangs forever after the DHT query completes (verified). We run a
    // single router (the custom DHT) anyway, so that merge layer is pure overhead — go straight to
    // the libp2p layer, which terminates on the signal and returns providers with their addresses.
    for await (const prov of libp2p.contentRouting.findProviders(cid, { signal })) {
      if (dialed.size >= MAX_DIALS) break;
      const id = prov.id.toString();
      if (id === libp2p.peerId.toString() || dialed.has(id)) continue;
      const addrs = prov.multiaddrs.filter((m) => dialable(m.toString())).map((m) => withPeerId(m, id));
      if (!addrs.length) continue; // no browser-reachable address at all (only a bare TCP/QUIC desktop
      // with neither a public webrtc-direct nor a /p2p-circuit/webrtc relay address)
      dialed.add(id);
      // Don't await: one slow provider must not hold up the others, and nothing downstream needs the
      // connection to exist — bitswap will use it if it lands in time. Per-addr dialing (not the raw
      // addr array) because a plain dial(addrs) hangs on the provider's unreachable addresses.
      void dialPeerAddrs(libp2p, prov.id, addrs).catch(() => {});
    }
  } catch {
    /* no providers, or the DHT timed out — bitswap falls back to the seed */
  }
}

/**
 * Announce that we hold `root`, so other browsers can pull it from us.
 *
 * Throttled hard on purpose: browsers churn (a tab close is a peer leaving), DHT writes are not
 * free, and a provider record for a peer that vanished is worse than no record — it makes someone
 * else waste a dial. So we only ever announce what the user actually kept or played.
 */
export class Provider {
  private announced = new Set<string>();
  private libp2p: Libp2p;
  private helia: Helia;

  constructor(libp2p: Libp2p, helia: Helia) {
    this.libp2p = libp2p;
    this.helia = helia;
  }

  /** Call once a module has actually been fetched. Idempotent per session. */
  async provide(root: string): Promise<void> {
    if (this.announced.has(root)) return;
    // Only worth announcing if anyone could dial us back: a browser with no relay reservation is
    // undialable, and a provider record pointing at it is a trap for whoever finds it.
    const reachable = this.libp2p.getMultiaddrs().some((m) => dialable(m.toString()));
    if (!reachable) return;
    this.announced.add(root);
    try {
      await this.helia.routing.provide(CID.parse(root));
    } catch {
      this.announced.delete(root); // transient — let a later play retry
    }
  }
}

// ---- mesh membership: eagerly connect to the network's public donors ----
//
// Without this the browser is a LEAF: it holds one connection (the master) and only ever dials
// content providers on demand (warmRoot). A NATed desktop, by contrast, joins the mesh by running
// AutoRelay's peer source — findProviders(donorRendezvous) then connect — against the publicly-
// reachable nodes that advertise themselves there (node/fwd.go donorPeerSource + node/pins.go
// reprovide + node/control.go on reachability→public). This is the browser port of exactly that loop:
// it turns the browser into a mesh member that pre-connects to the stable public peers (which are also
// the best offload sources). We can only DIAL the webrtc-reachable subset — a browser has no UDP/TCP
// socket, so it can never DCUtR-hole-punch to a QUIC/TCP-only peer — but that is a browser reality,
// not a wiring gap, and the public donors are webrtc-direct dialable.

/** Must hash-match node/config.go's `mustDonorRendezvous`: sha256("trackerstream/donors/v1") as a
 *  raw-codec CIDv1. Deterministic — every node computes the same key; nothing is ever bitswapped. */
const DONOR_RENDEZVOUS_INPUT = "trackerstream/donors/v1";
const DONOR_DISCOVERY_INTERVAL_MS = 5 * 60 * 1000; // re-sweep cadence (AutoRelay-like; churn-tolerant)
const DONOR_FIND_TIMEOUT_MS = 45_000; // the DHT provider walk is slow on a sparse overlay (~30s seen)
const DONOR_MAX_DIALS = 4;

let donorCidPromise: Promise<CID> | null = null;
const donorRendezvousCid = (): Promise<CID> =>
  (donorCidPromise ??= (async () => {
    const h = await sha256.digest(new TextEncoder().encode(DONOR_RENDEZVOUS_INPUT));
    return CID.createV1(0x55, h);
  })());

/** One discovery sweep: find the public donors and dial the browser-dialable ones we're not already
 *  connected to. Best-effort and fire-and-forget — a failed dial or an empty DHT is fine. */
async function connectDonorsOnce(libp2p: Libp2p): Promise<void> {
  const cid = await donorRendezvousCid();
  const signal = AbortSignal.timeout(DONOR_FIND_TIMEOUT_MS);
  const self = libp2p.peerId.toString();
  let dialed = 0;
  try {
    for await (const prov of libp2p.contentRouting.findProviders(cid, { signal })) {
      if (dialed >= DONOR_MAX_DIALS) break;
      const id = prov.id.toString();
      if (id === self) continue;
      if (libp2p.getConnections(prov.id).length > 0) continue; // already meshed with this donor
      const addrs = prov.multiaddrs.filter((m) => dialable(m.toString())).map((m) => withPeerId(m, id));
      if (!addrs.length) continue; // a donor with no webrtc surface — unreachable from a browser
      dialed++;
      // Per-addr dialing with its OWN budget — never the find `signal` (near-expired by the time a
      // donor is yielded) and never a raw addr array (hangs on the donor's unreachable addresses).
      // Don't await: one slow donor mustn't block dialing the rest.
      void dialPeerAddrs(libp2p, prov.id, addrs).catch(() => {});
    }
  } catch {
    /* no donors advertised yet, or the DHT walk timed out — we sweep again on the interval */
  }
}

/** Start the eager mesh-membership loop: one sweep now, then every DONOR_DISCOVERY_INTERVAL_MS.
 *  Returns a stop function. */
export function startDonorDiscovery(libp2p: Libp2p): () => void {
  void connectDonorsOnce(libp2p);
  const timer = setInterval(() => void connectDonorsOnce(libp2p), DONOR_DISCOVERY_INTERVAL_MS);
  return () => clearInterval(timer);
}
