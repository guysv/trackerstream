// Seed-offloading block fetch — the browser port of boxo's bitswap fetch discipline.
//
// THE PROBLEM this fixes. The default Helia fetch path (`helia.blockstore.get` → bitswap `want()` →
// `wantList.wantBlock()`) broadcasts a WANT-BLOCK to EVERY connected bitswap peer (@helia/bitswap
// want-list.js). The seed is a persistent, 1-hop, lowest-RTT connection, so for every block the
// browser asks BOTH the seed and any desktop/other-browser that holds it — and both send the full
// block back (the debounced CANCEL loses the race). Result: the seed serves a full duplicate copy of
// everything, i.e. NO fetch-side offload, however many donors are connected. Measured directly:
// downloading a module the desktop also holds raised the seed's bandwidth by ~the whole module.
//
// WHY THE DESKTOP DOESN'T DO THIS. go-libp2p + boxo never broadcasts WANT-BLOCK. Even a plain
// GetBlock runs through a session (boxo client.go: "creates a temporary internal session"): it
// broadcasts cheap WANT-HAVE to discover havers, then sends a TARGETED WANT-BLOCK to ONE chosen haver
// (sessionwantsender.go: "Send a want-block to the chosen peer / Send a want-have to each other
// peer"), with a `sentTo` guard that forbids a second WANT-BLOCK while one is outstanding. So exactly
// one peer ever sends the bytes, and the seed just gets a HAVE it can ignore.
//
// THIS FILE ports that discipline to the browser, using the single-peer primitives @helia/bitswap
// already exposes but its default `get` path doesn't use — `wantSessionPresence` (WANT-HAVE to one
// peer) and `wantSessionBlock` (WANT-BLOCK to one peer). Per block:
//   1. local? return it (offline read, never networks).
//   2. WANT-HAVE the connected NON-SEED peers; collect the havers within a short window.
//   3. if any non-seed peer HAS it: targeted WANT-BLOCK to one (boxo's weighted peer choice), one
//      haver at a time — which respects the browser's single-webrtc-conn serial constraint. Verify +
//      store, done. The seed sends nothing.
//   4. otherwise fall back to the seed via the normal broadcast path — reached ONLY when no non-seed
//      peer holds the block, so no duplicate full-block send happens. "The seed is always there."
//
// This is STRONGER than boxo on purpose: boxo may still pick the seed as bestPeer even when a donor
// has the block (it ranks by past latency); we ask the seed for bytes only as a last resort, because
// the whole point of the web client is to shed seed egress (offload thesis, §9).
import type { PeerId } from "@libp2p/interface";
import type { Helia } from "helia";
import { setMaxListeners } from "@libp2p/interface";
import { peerIdFromString } from "@libp2p/peer-id";
import type { CID } from "multiformats/cid";
import { sha256 } from "multiformats/hashes/sha2";

/** One inbound presence/block answer from a peer (shape returned by the wantList session primitives). */
interface SessionResult {
  has: boolean;
  block?: Uint8Array;
  sender: PeerId;
  cid: CID;
}

/** The slice of the @helia/bitswap instance we reach into. `wantList.peers` is the set of connected
 *  peers that speak bitswap; the two `wantSession*` methods send to ONE peer (not a broadcast). */
export interface BitswapLike {
  wantList: {
    peers: { keys(): IterableIterator<PeerId> };
    wantSessionPresence(cid: CID, peer: PeerId, opts?: { signal?: AbortSignal }): Promise<SessionResult>;
    wantSessionBlock(cid: CID, peer: PeerId, opts?: { signal?: AbortSignal }): Promise<SessionResult>;
  };
}

/** How long to wait for HAVE / DONT_HAVE answers before deciding who (if anyone) to WANT-BLOCK. A
 *  reachable donor answers a WANT-HAVE in one RTT (~60–300ms over webrtc); this bounds how long a
 *  silent peer delays the block. Kept short so the seed fallback isn't noticeably slower than today. */
const HAVE_WINDOW_MS = 700;
/** Budget for a single targeted WANT-BLOCK to a haver before giving up on it and trying the next. */
const BLOCK_TIMEOUT_MS = 6_000;
/** A peer that delivered a block is presumed to hold the rest of that module's blocks, so subsequent
 *  blocks go straight to it with a WANT-BLOCK — no re-discovery. This is the browser stand-in for
 *  boxo's session-scoped optimized-peer set (which amortises WANT-HAVE discovery across a DAG); a
 *  short TTL keeps it from pinning to a peer that has since dropped the content or churned away. */
const HAVER_TTL_MS = 30_000;
/** After a discovery round finds NO non-seed donor holds our content, skip discovery (go straight to
 *  the seed) for this long before probing again. Without it, a serial reassemble would pay a wasted
 *  WANT-HAVE round-trip on EVERY block whenever content-less public donors are connected (the common
 *  case — donor discovery meshes with random public peers, not holders of this exact module). */
const NO_DONOR_COOLDOWN_MS = 4_000;
/** Targeted WANT-BLOCK attempts to the seed before giving up on it and dropping to the broadcast/DHT
 *  last resort. The seed is the module's origin so it always holds the block; a failure here is a
 *  transient timeout under load, not a real absence, so a couple of retries clears it. */
const SEED_ATTEMPTS = 3;

function digestsEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * Build the media block fetcher. `masterId` is the seed's PeerId string — the ONE peer we never ask
 * for bytes while a donor holds the block. Route the module/media path through this instead of
 * `helia.blockstore.get`; leave the catalog path alone (its pages live on the seed and non-seed peers
 * never hold them, so a WANT-HAVE round-trip there would be pure added latency).
 */
export function makeOffloadFetch(deps: { helia: Helia; bitswap: BitswapLike; masterId: string }): (cid: CID) => Promise<Uint8Array> {
  const { helia, bitswap, masterId } = deps;
  const wl = bitswap.wantList;

  // Boxo peerResponseTracker: how often each peer was FIRST to give us a block. Used to weight the
  // WANT-BLOCK choice so load spreads across donors instead of hammering whichever answered first,
  // while a brand-new donor still keeps a nonzero chance (+1 smoothing).
  const firstResponder = new Map<string, number>();
  // Positive cache: peerId -> when it last delivered a block. A recent deliverer is tried directly on
  // the next block (skip the WANT-HAVE round-trip), mirroring boxo's session optimized-peer set.
  const recentHavers = new Map<string, number>();
  // Negative cache: skip discovery until this time — set when a round finds no donor holds our content.
  let noDonorUntil = 0;

  // Many per-block fetches can be in flight at once (reassemble walks the DAG); each wantSession* adds
  // a short-lived listener to the wantList emitter. Lift the default cap so we don't get spurious
  // MaxListenersExceeded warnings.
  try {
    setMaxListeners(Infinity, wl as unknown as EventTarget);
  } catch {
    /* not an EventTarget in this build — harmless */
  }

  const localGet = async (cid: CID): Promise<Uint8Array | null> => {
    try {
      // offline:true => never touches the network; throws if not resident locally.
      return await helia.blockstore.get(cid, { offline: true });
    } catch {
      return null;
    }
  };

  /** Connected bitswap peers minus the seed — the donors we prefer to pull from. */
  const donors = (): PeerId[] => [...wl.peers.keys()].filter((p) => p.toString() !== masterId);

  /** Boxo's weighted-random pick over the havers, by past first-responder count (+1 smoothing). */
  const choose = (peers: PeerId[]): PeerId => {
    let total = 0;
    for (const p of peers) total += (firstResponder.get(p.toString()) ?? 0) + 1;
    let r = Math.random() * total;
    for (const p of peers) {
      r -= (firstResponder.get(p.toString()) ?? 0) + 1;
      if (r <= 0) return p;
    }
    return peers[peers.length - 1];
  };

  /** Belt-and-suspenders content check before storing. Bitswap already verifies (it derives the CID
   *  from the block's own bytes and only matches our want on digest equality), so this only ever
   *  catches a bug, and only for sha256 CIDs — anything else we trust bitswap's own check on. */
  const verifyPut = async (cid: CID, bytes: Uint8Array): Promise<void> => {
    if (cid.multihash.code === sha256.code) {
      const mh = await sha256.digest(bytes);
      if (!digestsEqual(mh.digest, cid.multihash.digest)) throw new Error("block hash mismatch");
    }
    await helia.blockstore.put(cid, bytes);
  };

  /** One targeted WANT-BLOCK to a single peer. On delivery: verify, store, bump the weighting +
   *  positive cache, and return the bytes. Returns null if the peer doesn't have it or times out. */
  const wantBlockFrom = async (cid: CID, peer: PeerId, now: number): Promise<Uint8Array | null> => {
    try {
      const r = await wl.wantSessionBlock(cid, peer, { signal: AbortSignal.timeout(BLOCK_TIMEOUT_MS) });
      if (r.has && r.block != null) {
        await verifyPut(cid, r.block);
        const key = peer.toString();
        firstResponder.set(key, (firstResponder.get(key) ?? 0) + 1);
        recentHavers.set(key, now);
        return r.block;
      }
    } catch {
      /* peer failed to deliver */
    }
    return null;
  };

  /** Try to get the block from a non-seed donor. Returns bytes (already stored) or null if no donor
   *  holds it / all attempts fail — the caller then falls back to the seed. */
  const tryOffload = async (cid: CID): Promise<Uint8Array | null> => {
    const cands = donors();
    if (cands.length === 0) return null; // lone browser on the seed — no offload possible, skip the round-trip
    const now = performance.now();

    // 1. Known-haver fast path. A donor that recently delivered a block is presumed to hold the rest
    //    of this module — WANT-BLOCK it directly, skipping the WANT-HAVE discovery round-trip. This is
    //    what makes blocks 2..N of a module cost ~1 RTT (parity with the seed) instead of 2.
    const known = cands.filter((p) => {
      const t = recentHavers.get(p.toString());
      return t != null && now - t < HAVER_TTL_MS;
    });
    if (known.length > 0) {
      const got = await wantBlockFrom(cid, choose(known), now);
      if (got != null) return got; // fell through: this deliverer lacks THIS block (rare) -> discover
    }

    // 2. Discovery — unless a recent round already found no donor holds our content (cooldown), in
    //    which case go straight to the seed rather than paying a WANT-HAVE round-trip per block.
    if (now < noDonorUntil) return null;

    // WANT-HAVE every donor in parallel; a donor that has it answers has:true, one that doesn't (or
    // stays silent past the window) drops out. sendDontHave is set by the primitive, so a compliant
    // peer answers either way within one RTT.
    const signal = AbortSignal.timeout(HAVE_WINDOW_MS);
    const answers = await Promise.all(
      cands.map(async (p) => {
        try {
          const r = await wl.wantSessionPresence(cid, p, { signal });
          return r.has ? p : null;
        } catch {
          return null; // timed out / stream failed — treat as "doesn't have it"
        }
      }),
    );
    const havers = answers.filter((p): p is PeerId => p != null);
    if (havers.length === 0) {
      noDonorUntil = now + NO_DONOR_COOLDOWN_MS; // back off discovery; the seed will serve
      return null;
    }

    // Targeted WANT-BLOCK to ONE haver at a time (never a fan-out: the browser's single webrtc-direct
    // conn head-of-line-blocks under concurrency, so serial is the fast path). On failure, drop that
    // haver and try the next; the seed is never in this pool.
    const pool = [...havers];
    while (pool.length > 0) {
      const pick = choose(pool);
      pool.splice(pool.indexOf(pick), 1);
      const got = await wantBlockFrom(cid, pick, now);
      if (got != null) return got;
    }
    return null;
  };

  // The seed's PeerId as an object, for the targeted seed fetch below. Memoised — parsing is pure.
  let seedPeerId: PeerId | null = null;
  const seed = (): PeerId => (seedPeerId ??= peerIdFromString(masterId));

  /** Pull a block FROM THE SEED with a targeted WANT-BLOCK — never the broadcast path.
   *
   *  WHY NOT helia.blockstore.get. That broadcasts the ENTIRE wantlist as one batched bitswap message
   *  to every connected peer. A reassemble asks for a module's leaves all at once (stream.ts fetches
   *  every sample/chunk concurrently), so the batch is large — and a go-libp2p seed over the browser's
   *  single webrtc-direct connection leaves that batched message UNANSWERED: measured on prod, a
   *  ~136-want batch is sent (taking ~6s) and comes back with zero blocks and zero presences. Worse,
   *  @helia/bitswap then never re-sends — want-list.js marks each want "sent" per peer and has no
   *  retry-on-no-response — so the fetch hangs until the caller's own timeout, i.e. the module never
   *  loads. A SINGLE-want wantSessionBlock to the same seed is answered in ~one RTT (and 40 concurrent
   *  land in ~2.5s), and the bitswap network layer already caps concurrent sends, so one targeted want
   *  per leaf is both correct and safe. This is the path cached/donor-served modules always took; only
   *  an uncached module with no dialable donor ever reached the broken broadcast — which is exactly why
   *  some modules "never load" while others play fine. */
  const wantSeed = async (cid: CID): Promise<Uint8Array | null> => {
    for (let i = 0; i < SEED_ATTEMPTS; i++) {
      const got = await wantBlockFrom(cid, seed(), performance.now());
      if (got != null) return got;
    }
    return null;
  };

  return async (cid: CID): Promise<Uint8Array> => {
    const local = await localGet(cid);
    if (local != null) return local;

    const offloaded = await tryOffload(cid).catch(() => null);
    if (offloaded != null) return offloaded;

    // Seed fallback — targeted, not broadcast (see wantSeed). Reached when no non-seed donor HAVEs the
    // block, which is the common case; the seed is the origin, so this virtually always returns bytes.
    const fromSeed = await wantSeed(cid);
    if (fromSeed != null) return fromSeed;

    // Absolute last resort: the seed itself returned DONT_HAVE / timed out every attempt — a genuine
    // "not on the seed" case where only the DHT / another provider can help. Helia's broadcast path
    // reaches them; it is off the hot path and no longer where a normal module fetch ends up.
    return helia.blockstore.get(cid);
  };
}
