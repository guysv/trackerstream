// The follower side of the tab RPC: a full NodeClient that runs no node of its own.
//
// Every method call is serialized to the leader over the BroadcastChannel and awaited; streaming
// methods relay their callbacks back the same way. `platform.*` and `caps` stay local (see
// webplatform.ts) — they belong to this tab's own browser surface, not the leader's.
//
// On failover this same object PROMOTES in place: it boots a real WebClient, starts serving the
// other followers, and thereafter routes its own calls straight to that local node. Promoting the
// installed client in place (rather than swapping the setClient() singleton) keeps the UI's
// events.on subscriptions — which this object owns — alive across the handoff.
import type {
  CatalogSearchOpts,
  Capabilities,
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
import { WebClient } from "../client/web.ts";
import { WEB_CAPS, downloadBlob, makePlatform } from "../webplatform.ts";
import { startLeaderServer } from "./server.ts";
import {
  RPC_CHANNEL,
  SAVE_MODULE_BYTES,
  STREAMING_ARG,
  splitMethod,
  type Msg,
  type Probe,
  type Req,
} from "./protocol.ts";

interface Pending {
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
  onCb?: (arg: unknown) => void;
}

export class FollowerClient implements NodeClient {
  readonly caps: Capabilities = WEB_CAPS;
  readonly platform = makePlatform();

  private readonly ch = new BroadcastChannel(RPC_CHANNEL);
  private readonly selfId = crypto.randomUUID();
  private seq = 0;
  private readonly inflight = new Map<number, Pending>();
  private readonly listeners = new Set<() => void>();

  /** Set once this tab promotes to leader — thereafter calls go straight to the local node. */
  private local: WebClient | null = null;
  private leaderId: string | null = null;

  private readyResolve!: () => void;
  private readonly ready = new Promise<void>((r) => (this.readyResolve = r));

  constructor() {
    this.ch.onmessage = (ev) => this.onMsg(ev.data as Msg);
  }

  /** Resolve once a ready leader has announced itself. Probes immediately and keeps probing until
   *  answered, so a tab that started while the leader was still booting its node gets unblocked. */
  async waitReady(): Promise<void> {
    const probe = () => this.ch.postMessage({ t: "probe", from: this.selfId } satisfies Probe);
    probe();
    const iv = setInterval(() => {
      if (!this.leaderId) probe();
    }, 1000);
    try {
      await this.ready;
    } finally {
      clearInterval(iv);
    }
  }

  private onMsg(m: Msg): void {
    if (m.t === "hello") {
      const prev = this.leaderId;
      this.leaderId = m.leader;
      // Leadership moved to a new tab: anything we had outstanding to the old leader is lost.
      if (prev && prev !== m.leader) this.failInflight("leader changed");
      this.readyResolve();
      return;
    }
    if (m.t === "changed") {
      for (const cb of this.listeners) cb();
      return;
    }
    // Only replies (cb/ok/err) carry `to`; req/probe are other tabs' traffic we ignore.
    if (m.t !== "cb" && m.t !== "ok" && m.t !== "err") return;
    if (m.to !== this.selfId) return; // addressed to another follower
    const p = this.inflight.get(m.id);
    if (!p) return;
    if (m.t === "cb") {
      p.onCb?.(m.arg);
    } else if (m.t === "ok") {
      this.inflight.delete(m.id);
      p.resolve(m.value);
    } else {
      this.inflight.delete(m.id);
      p.reject(new Error(m.message));
    }
  }

  private failInflight(why: string): void {
    for (const p of this.inflight.values()) p.reject(new Error(why));
    this.inflight.clear();
  }

  private rpc<T>(method: string, args: unknown[], onCb?: (arg: unknown) => void): Promise<T> {
    const id = ++this.seq;
    return new Promise<T>((resolve, reject) => {
      this.inflight.set(id, { resolve: resolve as (v: unknown) => void, reject, onCb });
      this.ch.postMessage({ t: "req", from: this.selfId, id, method, args } satisfies Req);
    });
  }

  /** Route a plain call: to the local node once promoted, else over RPC. */
  private call<T>(method: string, args: unknown[]): Promise<T> {
    if (this.local) {
      const [g, f] = splitMethod(method);
      return (this.local as unknown as Record<string, Record<string, (...a: unknown[]) => Promise<T>>>)[g][f](...args);
    }
    return this.rpc<T>(method, args);
  }

  /** Route a streaming call (a callback fires per chunk, then the promise resolves). */
  private callStream<T>(method: string, args: unknown[], cb: (arg: unknown) => void): Promise<T> {
    if (this.local) {
      const [g, f] = splitMethod(method);
      const withCb = args.slice();
      withCb.splice(STREAMING_ARG[method], 0, cb);
      return (this.local as unknown as Record<string, Record<string, (...a: unknown[]) => Promise<T>>>)[g][f](...withCb);
    }
    return this.rpc<T>(method, args, cb);
  }

  /** Failover: the leader tab died and we won the lock. Boot a real node in place, take over serving
   *  the other followers, and reroute our own calls to it. */
  async promote(): Promise<void> {
    const wc = await WebClient.create();
    this.local = wc;
    // The node's gossip ingest now drives our listeners directly.
    wc.events.on("playlists:changed", () => {
      for (const cb of this.listeners) cb();
    });
    startLeaderServer(wc);
    // Requests we had outstanding to the now-dead leader can't be answered; the UI retries on the
    // next interaction. Then drop the follower channel — we serve via the leader server's own now.
    this.failInflight("promoted to leader");
    this.ch.close();
    this.readyResolve();
  }

  node = {
    info: (): Promise<NodeInfo> => this.call<NodeInfo>("node.info", []),
    peers: (): Promise<PeerStats> => this.call<PeerStats>("node.peers", []),
    peerDetail: (id: string): Promise<PeerDetail> => this.call<PeerDetail>("node.peerDetail", [id]),
    connect: (addr: string): Promise<void> => this.call<void>("node.connect", [addr]),
    keepaliveMaster: (addrs: string[]): Promise<void> => this.call<void>("node.keepaliveMaster", [addrs]),
    warmRoot: (root: string): Promise<void> => this.call<void>("node.warmRoot", [root]),
  };

  catalog = {
    query: <T>(req: Record<string, unknown>): Promise<T> => this.call<T>("catalog.query", [req]),
    searchStream: (opts: CatalogSearchOpts, onRow: (h: ModuleHit) => void): Promise<number> =>
      this.callStream<number>("catalog.searchStream", [opts], (arg) => onRow(arg as ModuleHit)),
    cancel: (): Promise<void> => this.call<void>("catalog.cancel", []),
    warm: (): Promise<void> => this.call<void>("catalog.warm", []),
  };

  media = {
    fetchModule: (root: string): Promise<ArrayBuffer> => this.call<ArrayBuffer>("media.fetchModule", [root]),
    startStream: (root: string, onEvent: (e: StreamEvent) => void): Promise<void> =>
      this.callStream<void>("media.startStream", [root], (arg) => onEvent(arg as StreamEvent)),
    getSkeleton: (root: string): Promise<ArrayBuffer> => this.call<ArrayBuffer>("media.getSkeleton", [root]),
    getSample: (root: string, index: number): Promise<ArrayBuffer> =>
      this.call<ArrayBuffer>("media.getSample", [root, index]),
    setPlayhead: (root: string, order: number): Promise<void> => this.call<void>("media.setPlayhead", [root, order]),
    // The reassembly needs the node (leader), but the download must land in THIS tab — so we fetch
    // bytes over RPC and do the Blob locally. Once promoted, the local node does both.
    saveModule: async (args: { root: string; md5: string; filename: string }): Promise<void> => {
      if (this.local) return this.local.media.saveModule(args);
      const bytes = await this.rpc<Uint8Array>(SAVE_MODULE_BYTES, [args]);
      downloadBlob(bytes, args.filename);
    },
  };

  playlists = {
    search: (q: string): Promise<PlaylistMeta[]> => this.call<PlaylistMeta[]>("playlists.search", [q]),
    list: (scope: PlaylistScope): Promise<PlaylistMeta[]> => this.call<PlaylistMeta[]>("playlists.list", [scope]),
    hold: (name: string, held: boolean): Promise<void> => this.call<void>("playlists.hold", [name, held]),
    get: (name: string): Promise<PlaylistDetail | null> => this.call<PlaylistDetail | null>("playlists.get", [name]),
    create: (title: string, tracks: TrackTuple[]): Promise<PlaylistMeta> =>
      this.call<PlaylistMeta>("playlists.create", [title, tracks]),
    update: (name: string, title: string, tracks: TrackTuple[]): Promise<void> =>
      this.call<void>("playlists.update", [name, title, tracks]),
    remove: (name: string): Promise<void> => this.call<void>("playlists.remove", [name]),
    publish: (name: string): Promise<void> => this.call<void>("playlists.publish", [name]),
    unpublish: (name: string): Promise<void> => this.call<void>("playlists.unpublish", [name]),
    syncStatus: (): Promise<PlaylistSyncStatus> => this.call<PlaylistSyncStatus>("playlists.syncStatus", []),
    likeToggle: (track: TrackTuple): Promise<boolean> => this.call<boolean>("playlists.likeToggle", [track]),
    likedIds: (): Promise<string[]> => this.call<string[]>("playlists.likedIds", []),
    likedName: (): Promise<string> => this.call<string>("playlists.likedName", []),
    ingestLink: (url: string): Promise<LinkStatus> => this.call<LinkStatus>("playlists.ingestLink", [url]),
    copyLink: (name: string): Promise<string> => this.call<string>("playlists.copyLink", [name]),
    pending: (): Promise<string[]> => this.call<string[]>("playlists.pending", []),
    backers: (names: string[]): Promise<Record<string, number>> =>
      this.call<Record<string, number>>("playlists.backers", [names]),
    pin: (name: string | null): Promise<void> => this.call<void>("playlists.pin", [name]),
    played: (name: string): Promise<void> => this.call<void>("playlists.played", [name]),
    ofPeer: (id: string): Promise<PeerPlaylists> => this.call<PeerPlaylists>("playlists.ofPeer", [id]),
    request: (id: string, name: string): Promise<number> => this.call<number>("playlists.request", [id, name]),
  };

  events = {
    on: (_event: "playlists:changed", cb: () => void): Unsub => {
      this.listeners.add(cb);
      return () => this.listeners.delete(cb);
    },
  };
}
