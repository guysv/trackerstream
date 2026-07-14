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
import { CID } from "multiformats/cid";
import { resolveIpns } from "../ipns.ts";
import { startNode, type TsNode } from "../node.ts";
import { getSample, getSkeleton, startStream } from "../stream.ts";

export class WebClient implements NodeClient {
  readonly caps: Capabilities = {
    // A browser cannot execute a program. Saving still works — reassembly is byte-exact, so we hand
    // the user the real module file as a Blob.
    openInTracker: false,
    logsDir: false,
    // navigator.mediaSession routes hardware media keys to the audio-playing tab. What we cannot do
    // is grab them system-wide while another app is focused — a weaker capability, not a missing one.
    globalMediaKeys: false,
    // Needs a relay reservation to be dialable (Phase 5), and the master doesn't serve playlist-list
    // anyway (client-only by design), so an empty answer here is CORRECT, not broken.
    peerPlaylists: false,
  };

  private readonly ts: TsNode;
  private readonly cat: CatalogClient;
  private readonly pl: Playlists;
  /** Fan-out for `playlists:changed`. The desktop gets this from Tauri's event bus; here the gossip
   *  ingest loop emits it directly — same signal, no round-trip. */
  private readonly listeners: Set<() => void>;

  private constructor(ts: TsNode, cat: CatalogClient, pl: Playlists, listeners: Set<() => void>) {
    this.ts = ts;
    this.cat = cat;
    this.pl = pl;
    this.listeners = listeners;
  }

  static async create(): Promise<WebClient> {
    const ts = await startNode();
    const fs = unixfs(ts.helia);

    // Resolve the catalog's IPNS name over the custom DHT, verifying the record locally — the node
    // is an untrusted cache, exactly as on desktop.
    //
    // The dev override exists because resolving requires reaching a DHT *server*, and in a local
    // rig the only peer a browser can dial is a NATed client-mode node that answers nothing. It is
    // never used in prod, where the master is a DHT server the browser dials directly.
    const rootCid =
      (import.meta.env?.VITE_CATALOG_CID as string | undefined) ?? (await resolveIpns(ts.libp2p));

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
        // Helia's bitswap does not expose a per-peer byte ledger the way boxo does, so we cannot
        // reproduce the desktop's up/down attribution yet. Report the connections honestly with
        // zeroed counters rather than inventing numbers.
        peers: conns.map((c) => ({
          id: c.remotePeer.toString(),
          down: 0,
          up: 0,
          connected: true,
          role: "other" as const,
        })),
        // A browser is never publicly dialable.
        reachable: false,
      };
    },
    peerDetail: async (id: string): Promise<PeerDetail> => {
      const conns = this.ts.libp2p.getConnections().filter((c) => c.remotePeer.toString() === id);
      const c = conns[0];
      return {
        id,
        connected: conns.length > 0,
        role: "other",
        warm_reason: [],
        down: 0,
        up: 0,
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
    // The desktop's warm_root asks the tracker who holds a root and pre-dials them. Wiring that to
    // dht.findProviders is Phase 5 (Tier 1 offload); until then Bitswap simply asks the seed.
    warmRoot: async (): Promise<void> => {},
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
    startStream: (root: string, onEvent: (e: StreamEvent) => void): Promise<void> =>
      startStream(root, this.block, onEvent),
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
    list: (_scope: PlaylistScope): Promise<PlaylistMeta[]> => this.pl.list() as Promise<PlaylistMeta[]>,
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
    // Beacon COUNTING (24h window, distinct origins) is not implemented yet; a browser leaf also
    // sees fewer origins than a desktop. Report 0 rather than a wrong number — 0 already means
    // "no beacon heard", which is normal for the first hour after startup.
    backers: async (): Promise<Record<string, number>> => ({}),
    // Play-time pin: the browser store has no decay/eviction racing playback (enforceBudget only
    // touches the seen tier, and a playing playlist is in the library), so there is nothing to pin.
    pin: async (): Promise<void> => {},
    played: (name: string): Promise<void> => this.pl.played(name),
    ofPeer: async (): Promise<PeerPlaylists> => ({ supported: false, playlists: [] }),
    request: async (): Promise<number> => 0,
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
