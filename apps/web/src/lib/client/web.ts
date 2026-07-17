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
  MediaKeyAction,
  ModuleHit,
  NodeClient,
  NodeInfo,
  NowPlayingMeta,
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
import { unixfs } from "@helia/unixfs";
import { peerIdFromString } from "@libp2p/peer-id";
import { MASTER_PEER_ID } from "@trackerstream/config";
import { CID } from "multiformats/cid";
import { resolveIpns } from "../ipns.ts";
import { startNode, type TsNode } from "../node.ts";
import { get as idbGet, set as idbSet } from "idb-keyval";
import { getSample, getSkeleton, startStream } from "../stream.ts";
import { Provider, dialable, warmRoot } from "../offload.ts";

export class WebClient implements NodeClient {
  readonly caps: Capabilities = {
    // A browser cannot execute a program. Saving still works — reassembly is byte-exact, so we hand
    // the user the real module file as a Blob.
    openInTracker: false,
    logsDir: false,
    // navigator.mediaSession routes hardware media keys to the audio-playing tab. What we cannot do
    // is grab them system-wide while another app is focused — a weaker capability, not a missing one.
    globalMediaKeys: false,
    // The browser now reserves on the master (see node.ts), so it is dialable AND serves
    // "/trackerstream/playlist-list/1.0.0" like a desktop — it both answers and asks. (The seed still
    // doesn't serve it by design; that peer just returns supported:false, which is a normal answer.)
    peerPlaylists: true,
  };

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
      getBlock: async (cid) => ts.helia.blockstore.get(cid),
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

  private block = async (cid: CID): Promise<Uint8Array> => this.ts.helia.blockstore.get(cid);

  node = {
    info: async (): Promise<NodeInfo> => ({
      peer_id: this.ts.libp2p.peerId.toString(),
      listening: this.ts.libp2p.getMultiaddrs().map((a) => a.toString()),
    }),
    peers: async (): Promise<PeerStats> => {
      const conns = this.ts.libp2p.getConnections();
      return {
        connected: conns.length,
        // Per-peer up/down from the BandwidthTracker metrics component (bandwidth.ts) — all-protocol
        // byte attribution, parity with the desktop's go-libp2p BandwidthCounter.
        peers: conns.map((c) => {
          const id = c.remotePeer.toString();
          const { down, up } = this.ts.bandwidth.get(id);
          return {
            id,
            down,
            up,
            connected: true,
            // Label the seed the same way the desktop does (id === master). Without this the master —
            // which every browser holds a persistent webrtc-direct connection to — shows as a generic
            // "other" peer, i.e. unrecognised.
            role: id === MASTER_PEER_ID ? ("master" as const) : ("other" as const),
          };
        }),
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
      const c = conns[0];
      const { down, up } = this.ts.bandwidth.get(id);
      return {
        id,
        connected: conns.length > 0,
        role: id === MASTER_PEER_ID ? "master" : "other",
        warm_reason: [],
        down,
        up,
        addrs: conns.map((x) => x.remoteAddr.toString()),
        relayed: c ? c.remoteAddr.toString().includes("/p2p-circuit") : false,
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
      const { reassembleAny } = await import("../rebuild.ts");
      const bytes = await reassembleAny(CID.parse(args.root), this.block);
      const url = URL.createObjectURL(new Blob([bytes as BlobPart], { type: "application/octet-stream" }));
      const a = document.createElement("a");
      a.href = url;
      a.download = args.filename;
      a.click();
      URL.revokeObjectURL(url);
    },
  };

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

  platform = {
    log: (level: "debug" | "warn" | "error", ...args: unknown[]): void => {
      // No log FILE in a browser — the console is the log.
      (level === "error" ? console.error : level === "warn" ? console.warn : console.debug)(...args);
    },
    // The web "deep link" is simply the /p/<name> route the tab was opened on.
    onDeepLink: (cb: (url: string) => void): Unsub => {
      if (location.pathname.startsWith("/p/")) cb(location.href);
      return () => {};
    },
    nowPlaying: (meta: NowPlayingMeta | null): void => {
      if (!("mediaSession" in navigator)) return;
      const ms = navigator.mediaSession;
      if (!meta) {
        ms.metadata = null;
        ms.playbackState = "none";
        return;
      }
      ms.metadata = new MediaMetadata({ title: meta.title, artist: meta.artist, album: "trackerstream" });
      ms.playbackState = meta.playing ? "playing" : "paused";
    },
    mediaKeys: (handler: (a: MediaKeyAction) => void): Unsub => {
      if (!("mediaSession" in navigator)) return () => {};
      const ms = navigator.mediaSession;
      const wire: [MediaSessionAction, MediaKeyAction][] = [
        ["play", "play"],
        ["pause", "pause"],
        ["nexttrack", "next"],
        ["previoustrack", "prev"],
      ];
      for (const [a, action] of wire) {
        try {
          ms.setActionHandler(a, () => handler(action));
        } catch {
          /* the browser doesn't support this action */
        }
      }
      return () => {
        for (const [a] of wire) {
          try {
            ms.setActionHandler(a, null);
          } catch {
            /* ignore */
          }
        }
      };
    },
  };

  events = {
    // The browser's equivalent of the desktop's sync loop: gossip ingest fires this the instant the
    // network moves a playlist under us.
    on: (_event: "playlists:changed", cb: () => void): Unsub => {
      this.listeners.add(cb);
      return () => this.listeners.delete(cb);
    },
  };
}
