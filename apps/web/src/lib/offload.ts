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
// Note the hard limit behind all of this: go-libp2p has no private-to-private WebRTC transport, so a
// browser can never reach a NATed desktop — only a publicly-reachable one. See the plan.
import type { Helia } from "helia";
import type { Libp2p } from "libp2p";
import { CID } from "multiformats/cid";

/** A browser cannot dial TCP or QUIC. Anything else in a provider's address list is noise to us. */
const dialable = (addr: string): boolean => /\/(webrtc-direct|webrtc|wss|tls\/ws|ws)(\/|$)/.test(addr);

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
export async function warmRoot(libp2p: Libp2p, helia: Helia, root: string): Promise<void> {
  let cid: CID;
  try {
    cid = CID.parse(root);
  } catch {
    return;
  }
  const signal = AbortSignal.timeout(FIND_TIMEOUT_MS);
  const dialed = new Set<string>();
  try {
    for await (const prov of helia.routing.findProviders(cid, { signal })) {
      if (dialed.size >= MAX_DIALS) break;
      const id = prov.id.toString();
      if (id === libp2p.peerId.toString() || dialed.has(id)) continue;
      const addrs = prov.multiaddrs.filter((m) => dialable(m.toString()));
      if (!addrs.length) continue; // a NATed desktop — unreachable from a browser, by construction
      dialed.add(id);
      // Don't await: one slow provider must not hold up the others, and nothing downstream needs
      // the connection to exist — bitswap will use it if it lands in time.
      void libp2p.dial(addrs, { signal }).catch(() => {});
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
