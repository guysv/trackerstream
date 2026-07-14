// The seam between the shared UI and whatever node is underneath it.
//
// The desktop reaches an in-process Go tsnode sidecar through ~40 Tauri `invoke` commands. The web
// client IS the node — a js-libp2p peer doing Bitswap in the tab. Same UI, same data model, two
// wildly different transports. NodeClient is the contract; each shell ships one implementation.
//
// Grouped by domain (node / catalog / media / playlists / platform) rather than mirroring the flat
// command list, because that is how the frontend already splits: p2p.ts, catalog.ts,
// playlists.svelte.ts. Every Tauri `Channel<T>` becomes a plain callback — the consumers already
// took callbacks, so that is a drop-in, and it is the one primitive a browser cannot borrow.
import type {
  DownloadResult,
  LinkStatus,
  FormatCount,
  GenreCount,
  MediaKeyAction,
  ModuleDetail,
  ModuleHit,
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
  TrackerInfo,
  Unsub,
} from "./types.ts";

/** What this shell can actually do. The UI reads these to degrade, never to error.
 *
 *  Note what is NOT here: media keys. The web shell does not "lack" them — it implements them
 *  differently (navigator.mediaSession routes hardware keys to the audio-playing tab). Only the
 *  global-while-another-app-is-focused case is lost, so it is a weaker capability, not a missing
 *  one; `globalMediaKeys` records the difference for labelling, not for hiding the feature. */
export interface Capabilities {
  /** Launch a module in an installed tracker (SchismTracker/MilkyTracker/OpenMPT). Desktop only:
   *  a browser cannot execute a program. NOTE this is NOT the same as saving the file — see
   *  `media.saveModule`, which every shell supports. */
  openInTracker: boolean;
  /** Reveal the rotating log file's directory. Desktop only. */
  logsDir: boolean;
  /** OS-global media keys (grabbed even when another app is focused) vs tab-scoped MediaSession. */
  globalMediaKeys: boolean;
  /** Can ask a peer what playlists it holds. Requires dialing that peer: on web this needs a relay
   *  reservation, and the master does not implement playlist-list at all (client-only by design),
   *  so an empty answer is CORRECT, not an error. */
  peerPlaylists: boolean;
}

export interface CatalogSearchOpts {
  q: string;
  limit: number;
  /** Keyset cursor: the id of the last hit already shown. Omit for the first page. */
  after?: number;
  /** Restrict the match to title/filename (the names_fts index) instead of the full index. */
  namesOnly?: boolean;
}

export interface NodeClient {
  readonly caps: Capabilities;

  node: {
    info(): Promise<NodeInfo>;
    peers(): Promise<PeerStats>;
    peerDetail(id: string): Promise<PeerDetail>;
    connect(addr: string): Promise<void>;
    /** Pin a persistent, auto-reconnecting connection to the master so it is not dialed lazily
     *  and pruned when idle. */
    keepaliveMaster(addrs: string[]): Promise<void>;
    /** Ask who holds `root` and warm-connect them BEFORE playback reaches it, so Bitswap finds the
     *  blocks on already-connected peers and the master is bypassed. Fire-and-forget. */
    warmRoot(root: string): Promise<void>;
  };

  catalog: {
    /** One call answers every catalog query, dispatched on `req.op`:
     *  search | list | get | get_by_md5 | formats | genres.
     *
     *  Deliberately opaque: the request shapes are the catalog's own (they mirror catalog.rs's
     *  `dispatch`), and keeping them intact means the UI's request-building and md5->CID cache
     *  warming stay exactly as they are — the adapter is a pure router, not a translator. */
    query<T>(req: Record<string, unknown>): Promise<T>;
    /** Streaming search: `onRow` fires per hit the instant its pages land, so results paint
     *  progressively. Resolves with the total row count. Supersedes any in-flight search. */
    searchStream(opts: CatalogSearchOpts, onRow: (hit: ModuleHit) => void): Promise<number>;
    /** Abort the in-flight query: VFS page reads stop and in-flight block fetches are dropped. */
    cancel(): Promise<void>;
    /** Prime the page cache (schema + FTS upper tree) so the first keystroke descends from warm
     *  pages instead of paying the cold fetches. Fire-and-forget; best-effort. */
    warm(): Promise<void>;
  };

  media: {
    /** v1/flat root -> exact module bytes. v2+ roots stream instead. */
    fetchModule(root: string): Promise<ArrayBuffer>;
    /** Begin a v2+ stream; `onEvent` ticks skeleton -> sample... -> complete. Metadata only. */
    startStream(root: string, onEvent: (e: StreamEvent) => void): Promise<void>;
    /** Assembled skeleton bytes (init the immortal openmpt instance). Ready after `skeleton`. */
    getSkeleton(root: string): Promise<ArrayBuffer>;
    /** One streamed sample's decoded PCM. Ready after its `sample` event. */
    getSample(root: string, index: number): Promise<ArrayBuffer>;
    /** Push the live playhead order so the prefetch scheduler reprioritizes (closed loop; also
     *  reseeds the queue on seek). Fire-and-forget. */
    setPlayhead(root: string, order: number): Promise<void>;
    /** Reassemble the byte-exact original and hand it to the user. Universal: the desktop writes it
     *  to the Downloads dir, the web shell offers it as a Blob download. Reassembly is byte-exact
     *  either way, so this is NOT a desktop-only capability — only *launching a tracker* is. */
    saveModule(args: { root: string; md5: string; filename: string }): Promise<void>;
    /** Desktop only (caps.openInTracker). Save + launch an installed tracker. */
    openInTracker?(args: { root: string; md5: string; filename: string; openWith?: string }): Promise<DownloadResult>;
    /** Desktop only (caps.openInTracker). Filesystem probe for installed trackers. */
    installedTrackers?(): Promise<TrackerInfo[]>;
  };

  /** 1:1 with the playlist_* command surface. Deliberately not "improved": the durable store, the
   *  gossip envelope, and the seq/tombstone semantics are the same on both shells (the web client
   *  reimplements playlists.rs over wa-sqlite + gossipsub), so a divergent shape here would only
   *  invite the two from drifting apart. */
  playlists: {
    search(q: string): Promise<PlaylistMeta[]>;
    list(scope: PlaylistScope): Promise<PlaylistMeta[]>;
    /** Add/remove a foreign playlist to/from the library — the "holder" tier: backed
     *  (re-announced), never evicted. Distinct from duplicate (fork) and delete. */
    hold(name: string, held: boolean): Promise<void>;
    get(name: string): Promise<PlaylistDetail | null>;
    create(title: string, tracks: TrackTuple[]): Promise<PlaylistMeta>;
    update(name: string, title: string, tracks: TrackTuple[]): Promise<void>;
    remove(name: string): Promise<void>;
    /** The explicit "Share" action — the only thing that makes a playlist public. */
    publish(name: string): Promise<void>;
    /** "Make private again": best-effort tombstone retract, keep the data local. */
    unpublish(name: string): Promise<void>;
    syncStatus(): Promise<PlaylistSyncStatus>;
    /** Toggle a track in "Liked Tracks" (creates the private playlist on first use). */
    likeToggle(track: TrackTuple): Promise<boolean>;
    likedIds(): Promise<string[]>;
    likedName(): Promise<string>;
    ingestLink(url: string): Promise<LinkStatus>;
    copyLink(name: string): Promise<string>;
    /** Names from name-only links still waiting on gossip (the "syncing…" placeholder). */
    pending(): Promise<string[]>;
    /** Hold-beacon backer counts (windowed distinct holders). 0 = no beacon heard, which is
     *  normal for the first hour after startup. */
    backers(names: string[]): Promise<Record<string, number>>;
    /** Play-time pin: while the queue is sourced from a playlist, that row is protected from
     *  decay / budget eviction / tombstone deletion so it can't vanish under playback. Transient
     *  — NOT the held tier; nothing is backed or re-announced. `null` releases. */
    pin(name: string | null): Promise<void>;
    played(name: string): Promise<void>;
    /** Ask one connected peer what it holds (deliberate 1:1 pull, never polled). */
    ofPeer(peerId: string): Promise<PeerPlaylists>;
    /** Ask a peer holding `name` to re-announce it; the doc arrives via the normal gossip path. */
    request(peerId: string, name: string): Promise<number>;
  };

  platform: {
    log(level: "debug" | "warn" | "error", ...args: unknown[]): void;
    /** Desktop: the `trackerstream://` OS scheme. Web: the in-app /p/<name> route. */
    onDeepLink(cb: (url: string) => void): Unsub;
    /** Desktop: the OS Now Playing slot. Web: navigator.mediaSession.metadata. */
    nowPlaying(meta: NowPlayingMeta | null): void;
    /** Desktop: OS-global shortcuts. Web: navigator.mediaSession action handlers. */
    mediaKeys(handler: (action: MediaKeyAction) => void): Unsub;
    /** Desktop only (caps.logsDir). */
    openLogsDir?(): Promise<void>;
  };

  /** Backend-pushed invalidations. Desktop: Tauri's event bus. Web: emitted by the local
   *  gossipsub ingest loop — same signal, no round-trip. */
  events: {
    on(event: "playlists:changed", cb: () => void): Unsub;
  };
}
