// The web shell's NodeClient: a js-libp2p peer doing Bitswap in the tab.
//
// Mirror image of apps/desktop/src/lib/client/tauri.ts. Same interface, same UI above it — the only
// difference is that this one IS the node rather than talking to one.
import { CatalogClient } from "@trackerstream/catalog-web";
import { Playlists } from "@trackerstream/playlists-web";
import type {
  Capabilities,
  CatalogSearchOpts,
  LinkStatus,
  ModuleHit,
  NodeClient,
  NodeInfo,
  PeerDetail,
  PeerPlaylists,
  PeerStats,
  PlaylistDetail,
  PlaylistMeta,
  PlaylistScope,
  PlaylistSyncStatus,
  StreamEvent,
  TrackTuple,
  Unsub,
} from "@trackerstream/ui/client";
import { WEB_CAPS, downloadBlob, makePlatform } from "../webplatform.ts";
import { unixfs } from "@helia/unixfs";
import { peerIdFromString } from "@libp2p/peer-id";
import { MASTER_PEER_ID } from "@trackerstream/config";
import { CID } from "multiformats/cid";
import { resolveIpns } from "../ipns.ts";
import { startNode, type TsNode } from "../node.ts";
import { startWarmBundle } from "../warmbundle.ts";
import { get as idbGet, set as idbSet } from "idb-keyval";
import { getSample, getSkeleton, startStream } from "../stream.ts";
import { Provider, dialable, warmRoot } from "../offload.ts";
import { makeOffloadFetch } from "../blockfetch.ts";

export class WebClient implements NodeClient {
  readonly caps: Capabilities = WEB_CAPS;

  private readonly ts: TsNode;
  private readonly cat: CatalogClient;
  private readonly pl: Playlists;
  /** Fan-out for `playlists:changed`. The desktop gets this from Tauri's event bus; here the gossip
   *  ingest loop emits it directly — same signal, no round-trip. */
  private readonly listeners: Set<() => void>;
  private readonly provider: Provider;

  private constructor(ts: TsNode, cat: CatalogClient, pl: Playlists, listeners: Set<() => void>) {
    this.ts = ts;
    this.cat = cat;
    this.pl = pl;
    this.listeners = listeners;
    this.provider = new Provider(ts.libp2p, ts.helia);
    // Media blocks go through the seed-offloading fetch (WANT-HAVE donors -> targeted WANT-BLOCK ->
    // seed fallback, see blockfetch.ts), NOT the default broadcast-to-everyone blockstore.get that
    // makes the seed serve a duplicate of every block. The catalog path stays on blockstore.get: its
    // pages live on the seed and donors never hold them, so a WANT-HAVE round-trip there is pure cost.
    this.block = makeOffloadFetch({ helia: ts.helia, bitswap: ts.bitswap, masterId: ts.masterId });
  }

  static async create(): Promise<WebClient> {
    const mark = (import.meta.env?.DEV || import.meta.env?.VITE_EXPOSE_NODE) ? (() => {
      const t0 = performance.now();
      let last = t0;
      return (label: string) => {
        const now = performance.now();
        console.info(`[boot] ${label}: +${Math.round(now - last)}ms (total ${Math.round(now - t0)}ms)`);
        last = now;
      };
    })() : (_: string) => {};

    const ts = await startNode();
    mark("startNode (fetchBootstrap + dial seed)");
    // Dev-only handle for inspecting the live node from the console (connections, peerStore, redial
    // state) — indispensable for debugging the promoted-node reconnect. Gated, never in prod.
    if (import.meta.env?.DEV || import.meta.env?.VITE_EXPOSE_NODE) (globalThis as { __tsnode?: TsNode }).__tsnode = ts;

    // Politely close the node when this leader tab goes away (close OR reload). Our PeerId is
    // persistent (loadOrCreateKey), so without a clean close the seed keeps our webrtc-direct
    // connection as a half-open corpse keyed to that identity and only reaps it on its own slow
    // timeout — during which a reloaded tab's redial under the SAME PeerId hangs (see node.ts) and
    // the stale conn burns a seed connection slot. Stopping libp2p sends the close so the seed frees
    // us at once. Best-effort: pagehide's budget is tight and stop() is async, so the seed-side
    // eviction (tsnode control.go evictStale) is the reliable backstop; this just makes the common
    // reload path fast. pagehide (not unload) so it still fires on mobile/bfcache paths.
    addEventListener("pagehide", () => void Promise.resolve(ts.libp2p.stop()).catch(() => {}), { once: true });

    const fs = unixfs(ts.helia);

    // Resolve the catalog's IPNS name over the custom DHT, verifying the record locally — the node
    // is an untrusted cache, exactly as on desktop.
    //
    // The dev override exists because resolving requires reaching a DHT *server*, and in a local
    // rig the only peer a browser can dial is a NATed client-mode node that answers nothing. It is
    // never used in prod, where the master is a DHT server the browser dials directly.
    // VITE_CATALOG_IPNS points the resolve at a DIFFERENT name (used to exercise the real DHT path
    // against a local seed); VITE_CATALOG_CID skips resolution entirely. Neither is used in prod.
    const ipnsName = (import.meta.env?.VITE_CATALOG_IPNS as string | undefined) ?? undefined;
    const resolve = () => resolveIpns(ts.libp2p, ipnsName);

    // BOOT-LATENCY: the DHT resolve is SLOW on a fresh browser — the master isn't in kad-dht's
    // routing table yet (~5s warmup) and the single GET_VALUE round-trip to it can take ~8s, so a
    // cold resolve blocks "connecting to the swarm" for 10s+. But the catalog root is
    // content-addressed and only changes on ingest (~daily), so a returning tab should never wait on
    // it. Persist the last resolved root and reuse it INSTANTLY on refresh; refresh it in the
    // background for next boot. A day-stale root is harmless (old roots stay fetchable, and Bitswap
    // serves the same pages); only the very first visit — or a dev CID override — pays the DHT cost.
    const CATALOG_ROOT_KEY = "ts:catalog-root";
    const override = import.meta.env?.VITE_CATALOG_CID as string | undefined;
    let rootCid: string;
    if (override) {
      rootCid = override;
    } else {
      const cached = await idbGet<string>(CATALOG_ROOT_KEY);
      if (cached) {
        rootCid = cached;
        // Background refresh: update the cache (and thus next boot) if the published root moved.
        // Non-blocking; we don't re-point this session (day-stale is fine). Best-effort.
        void resolve()
          .then((fresh) => (fresh !== cached ? idbSet(CATALOG_ROOT_KEY, fresh) : undefined))
          .catch(() => {});
      } else {
        rootCid = await resolve(); // first visit only — the one time we eat the DHT latency
        await idbSet(CATALOG_ROOT_KEY, rootCid).catch(() => {});
      }
    }
    mark("catalog root (cached=instant; cold=direct-ask ~150ms)");

    // Pre-load the catalog's hot FTS pages over HTTP (see warmbundle.ts) so a fresh profile's first
    // search skips the ~26 serialized cold Bitswap reads. Awaits only the tiny manifest; the blob
    // loads in the background. A page the bundle carries then waits for that load instead of a slow
    // cold Bitswap fetch — pages it doesn't carry never wait, so the home page (meta rows) is
    // unaffected. Best-effort: no bundle / wrong root / any failure => today's cold cost.
    const warmBundleUrl = (import.meta.env?.VITE_WARM_BUNDLE_URL as string | undefined) ?? "/catalog-warm.json";
    const warm = await startWarmBundle(ts.helia, rootCid, warmBundleUrl).catch(() => null);

    const cat = new CatalogClient(rootCid, {
      // The TSZCAT manifest is the only UnixFS read in the whole catalog path, and it's whole-file.
      readRoot: async (cid) => {
        const chunks: Uint8Array[] = [];
        for await (const c of fs.cat(cid)) chunks.push(c);
        const out = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
        let w = 0;
        for (const c of chunks) {
          out.set(c, w);
          w += c.length;
        }
        return out;
      },
      // Every catalog PAGE is its own raw block — a plain Bitswap fetch, no UnixFS, no ranged reads.
      // If the warm bundle carries this page, wait for its (HTTP, no-contention) load rather than
      // racing it with a cold Bitswap fetch; once loaded the block is resident, so this is a cache hit.
      getBlock: async (cid) => {
        if (warm?.has(cid)) await warm.ready;
        return ts.helia.blockstore.get(cid);
      },
    });

    // The listener set exists BEFORE Playlists so its onChange can close over it — the store needs a
    // way to signal the UI, and the UI subscribes through the client that owns the store.
    const listeners = new Set<() => void>();
    const pl = await Playlists.create(ts.libp2p, () => {
      for (const cb of listeners) cb();
    });
    mark("Playlists.create (store + subscribe)");
    return new WebClient(ts, cat, pl, listeners);
  }

  /** Media block fetch — set in the constructor to the seed-offloading fetcher (blockfetch.ts).
   *  Used by fetchModule / startStream / saveModule; the catalog uses its own getBlock. */
  private readonly block: (cid: CID) => Promise<Uint8Array>;

  node = {
    info: async (): Promise<NodeInfo> => ({
      peer_id: this.ts.libp2p.peerId.toString(),
      listening: this.ts.libp2p.getMultiaddrs().map((a) => a.toString()),
    }),
    peers: async (): Promise<PeerStats> => {
      // getConnections() returns one entry PER CONNECTION, and the browser can hold several to a
      // single peer — a transient relay/signaling conn beside the hole-punched direct one, or a
      // zombie master conn mid-redial. Emitting a row per connection then yields DUPLICATE peer ids,
      // which collides the peers list's keyed {#each … (p.id)} (Svelte each_key_duplicate) and breaks
      // row-click reconciliation — the "peer pane sometimes won't open" bug, browser-only because the
      // desktop reports per-peer from Rust. Dedupe to ONE row per peer; per-peer up/down comes from
      // the BandwidthTracker (bandwidth.ts) keyed by peer id, not per connection, so nothing to sum.
      const byPeer = new Map<string, { id: string; down: number; up: number; connected: true; role: "master" | "other" }>();
      for (const c of this.ts.libp2p.getConnections()) {
        const id = c.remotePeer.toString();
        if (byPeer.has(id)) continue;
        const { down, up } = this.ts.bandwidth.get(id);
        // Label the seed the same way the desktop does (id === master); otherwise the master — which
        // every browser holds a persistent webrtc-direct connection to — shows as a generic "other".
        byPeer.set(id, { id, down, up, connected: true, role: id === MASTER_PEER_ID ? "master" : "other" });
      }
      return {
        connected: byPeer.size,
        peers: [...byPeer.values()],
        // `reachable` is the AutoNAT PUBLIC/private verdict (true=public, false=private, null=undecided)
        // — the desktop reports the raw AutoNAT status here. A browser is NEVER public: it has no
        // socket, so it's dialable only via a relay reservation (…/p2p-circuit/webrtc) + WebRTC
        // hole-punch — which is exactly the "private" case (relay/DCUtR only), the same verdict a NATed
        // desktop reports even while it serves. So never emit `true` (the old `.some(dialable)` did, and
        // the badge rendered an impossible "public"). Reserved -> false (private, serving via relay);
        // not-yet-reserved -> null (still establishing a reservation), which shows "checking…".
        reachable: this.ts.libp2p.getMultiaddrs().some((m) => dialable(m.toString())) ? false : null,
      };
    },
    peerDetail: async (id: string): Promise<PeerDetail> => {
      const conns = this.ts.libp2p.getConnections().filter((c) => c.remotePeer.toString() === id);
      // "relayed" must come from libp2p's own `limits` flag, NOT the remoteAddr string. A private-webrtc
      // conn that hole-punched to a DIRECT datachannel keeps its `…/p2p-circuit/webrtc` DIAL multiaddr as
      // remoteAddr (the browser can't observe the negotiated ICE candidate the way go can), so matching
      // "/p2p-circuit" flags a genuine direct offload as "relayed via master" — false. `limits` is set
      // ONLY on Limited (actually-relayed) conns and is undefined once direct (it's also the exact flag
      // bitswap gates on, so a conn carrying blocks is provably not Limited). A peer is relayed only if we
      // hold NO direct conn to it — prefer a direct conn for the displayed transport/addr too.
      const direct = conns.find((x) => x.limits == null);
      const c = direct ?? conns[0];
      const { down, up } = this.ts.bandwidth.get(id);
      return {
        id,
        connected: conns.length > 0,
        role: id === MASTER_PEER_ID ? "master" : "other",
        warm_reason: [],
        down,
        up,
        addrs: conns.map((x) => x.remoteAddr.toString()),
        relayed: conns.length > 0 && direct == null,
        transport: c?.remoteAddr.toString().includes("webrtc-direct") ? "webrtc-direct" : "webrtc",
        agent: null,
        protocols: [],
        observed_addr: null,
        rtt_ms: null,
      };
    },
    // The UI calls this with the baked-in BOOTSTRAP_MULTIADDRS before playback ("ensure we can
    // reach the seed"). Every one of them is TCP or QUIC — a browser can dial NEITHER, so a naive
    // dial throws NoValidAddressesError on every play. We already hold a webrtc-direct connection
    // to the seed from boot, so: dial only what a browser CAN dial, and treat "already connected"
    // as success rather than failing on addresses that were never meant for us.
    connect: async (addr: string): Promise<void> => {
      const dialable = /\/(webrtc|webrtc-direct|wss|tls\/ws|ws|p2p-circuit)(\/|$)/.test(addr);
      if (!dialable) {
        if (this.ts.libp2p.getConnections().length > 0) return; // the seed is already reachable
        throw new Error(`web client cannot dial ${addr} (browsers have no TCP/QUIC)`);
      }
      const { multiaddr } = await import("@multiformats/multiaddr");
      await this.ts.libp2p.dial(multiaddr(addr));
    },
    // The browser holds exactly one bootstrap connection and libp2p keeps it; nothing to pin.
    keepaliveMaster: async (): Promise<void> => {},
    // Tier-1 offload: ask the DHT who holds this root and pre-dial the ones a browser CAN reach
    // (publicly-reachable desktops now listen on webrtc-direct, and other browsers reachable via
    // the relay). Bitswap then finds the blocks on an already-connected peer instead of the seed.
    // Fire-and-forget: playback must never wait on the DHT.
    warmRoot: async (root: string): Promise<void> => {
      void warmRoot(this.ts.libp2p, root);
    },
  };

  catalog = {
    query: <T>(req: Record<string, unknown>): Promise<T> => this.cat.query<T>(req),
    searchStream: (opts: CatalogSearchOpts, onRow: (h: ModuleHit) => void): Promise<number> =>
      this.cat.searchStream(opts, onRow),
    cancel: async (): Promise<void> => this.cat.cancel(),
    warm: (): Promise<void> => this.cat.warm(),
  };

  media = {
    fetchModule: async (root: string): Promise<ArrayBuffer> => {
      const { reassemble } = await import("@trackerstream/repack/dag");
      const { bytes } = await reassemble(CID.parse(root), this.block, { verify: true });
      return bytes.buffer as ArrayBuffer;
    },
    startStream: async (root: string, onEvent: (e: StreamEvent) => void): Promise<void> => {
      await startStream(root, this.block, onEvent);
      // We now hold this module's blocks. Announce it so other browsers can pull it from US — a
      // peer that never provides is a pure leech, and the mesh has nothing to offload from.
      void this.provider.provide(root);
    },
    getSkeleton: async (root: string): Promise<ArrayBuffer> => {
      const b = getSkeleton(root);
      return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer;
    },
    getSample: async (root: string, index: number): Promise<ArrayBuffer> => {
      const b = getSample(root, index);
      return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer;
    },
    // The prefetch scheduler is local (stream.ts), and it already fetches in fence order, so the
    // playhead hint has nothing to reprioritise yet. Kept as a no-op so the closed loop in
    // player.svelte.ts is unchanged.
    setPlayhead: async (): Promise<void> => {},
    saveModule: async (args: { root: string; md5: string; filename: string }): Promise<void> => {
      const bytes = await this.reassembleForDownload(args.root);
      downloadBlob(bytes, args.filename);
    },
  };

  /** Reassemble a module's byte-exact original from its root CID. Split out of saveModule so the
   *  leader can hand the bytes to a follower (which does the Blob download in its OWN tab — see
   *  multitab/server.ts); the leader downloading here would drop the file into a background tab. */
  async reassembleForDownload(root: string): Promise<Uint8Array> {
    const { reassembleAny } = await import("../rebuild.ts");
    return reassembleAny(CID.parse(root), this.block);
  }

  playlists = {
    search: (q: string): Promise<PlaylistMeta[]> => this.pl.search(q) as Promise<PlaylistMeta[]>,
    list: (scope: PlaylistScope): Promise<PlaylistMeta[]> => this.pl.list(scope) as Promise<PlaylistMeta[]>,
    hold: (name: string, held: boolean): Promise<void> => this.pl.hold(name, held),
    get: (name: string): Promise<PlaylistDetail | null> => this.pl.get(name) as Promise<PlaylistDetail | null>,
    create: (title: string, tracks: TrackTuple[]): Promise<PlaylistMeta> =>
      this.pl.create(title, tracks) as Promise<PlaylistMeta>,
    update: (name: string, title: string, tracks: TrackTuple[]): Promise<void> =>
      this.pl.update(name, title, tracks),
    remove: (name: string): Promise<void> => this.pl.remove(name),
    publish: (name: string): Promise<void> => this.pl.publish(name),
    unpublish: (name: string): Promise<void> => this.pl.unpublish(name),
    syncStatus: (): Promise<PlaylistSyncStatus> => this.pl.syncStatus(),
    likeToggle: (track: TrackTuple): Promise<boolean> => this.pl.likeToggle(track),
    likedIds: (): Promise<string[]> => this.pl.likedIds(),
    likedName: (): Promise<string> => this.pl.likedName(),
    ingestLink: (url: string): Promise<LinkStatus> => this.pl.ingestLink(url),
    copyLink: (name: string): Promise<string> => this.pl.copyLink(name),
    pending: (): Promise<string[]> => this.pl.pendingNames(),
    // Distinct-origin backer counts over a 24h window, tallied from inbound beacons (the browser now
    // counts, not just emits). A web leaf still sees fewer origins than a desktop; 0 means "no beacon
    // heard", normal for the first hour after startup.
    backers: (names: string[]): Promise<Record<string, number>> => this.pl.backers(names),
    // Play-time pin: the browser store has no decay/eviction racing playback (enforceBudget only
    // touches the seen tier, and a playing playlist is in the library), so there is nothing to pin.
    pin: async (): Promise<void> => {},
    played: (name: string): Promise<void> => this.pl.played(name),
    ofPeer: async (id: string): Promise<PeerPlaylists> => {
      const r = await this.pl.peerPlaylists(peerIdFromString(id), []);
      return { supported: r.supported, playlists: r.playlists };
    },
    request: async (id: string, name: string): Promise<number> =>
      (await this.pl.peerPlaylists(peerIdFromString(id), [name])).reannounced,
  };

  platform = makePlatform();

  events = {
    // The browser's equivalent of the desktop's sync loop: gossip ingest fires this the instant the
    // network moves a playlist under us.
    on: (_event: "playlists:changed", cb: () => void): Unsub => {
      this.listeners.add(cb);
      return () => this.listeners.delete(cb);
    },
  };
}
