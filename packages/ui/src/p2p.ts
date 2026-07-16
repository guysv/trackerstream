// Facade over the node's peer + media surface.
//
// Module bytes come from CID blocks over libp2p — the frontend never fetches a module file over
// HTTP. On desktop the node is an in-process Go sidecar reached via Tauri; on web the node IS the
// tab (js-libp2p + Bitswap). Everything above this line is identical either way.
import { can, client } from "./client/index.ts";

export type {
  DownloadResult,
  NodeInfo,
  PeerDetail,
  PeerEntry,
  PeerPlaylistEntry,
  PeerPlaylists,
  PeerRole,
  PeerStats,
  StreamEvent,
  TrackerInfo,
} from "./client/types.ts";
import type { DownloadResult, NodeInfo, PeerDetail, PeerPlaylists, PeerStats, StreamEvent, TrackerInfo } from "./client/types.ts";

export const nodeInfo = (): Promise<NodeInfo> => client().node.info();

/** Snapshot of per-peer cumulative up/down bytes + connected state (peers pane). */
export const peerStats = (): Promise<PeerStats> => client().node.peers();

/** Rich, on-demand info for one peer (the peers-pane detail view). Heavier than peerStats —
 *  fetched only while a peer is selected. All local-node sourced. */
export const peerDetail = (id: string): Promise<PeerDetail> => client().node.peerDetail(id);

/** Ask one connected peer what playlists it holds (deliberate 1:1 pull — fetched on demand from
 *  the peer card, never polled). `supported: false` is a normal answer, not an error: the seed
 *  does not implement playlist-list by design, and a web peer needs a relay reservation to be
 *  dialable at all. */
export const peerPlaylists = (id: string): Promise<PeerPlaylists> =>
  can("peerPlaylists")
    ? client().playlists.ofPeer(id)
    : Promise.resolve({ supported: false, playlists: [] });

/** Request a targeted re-announce of one playlist from a peer that holds it; the doc arrives
 *  through the normal gossip -> sync path (check discover shortly after). */
export const requestPlaylist = (peerId: string, name: string): Promise<number> =>
  client().playlists.request(peerId, name);

export const connectPeer = (addr: string): Promise<void> => client().node.connect(addr);

/** Queue-driven pre-connection: ask who holds `root` and warm-connect them BEFORE playback reaches
 *  it, so Bitswap finds the blocks on already-connected peers and the master is bypassed.
 *  Fire-and-forget; degrades to the master on any failure. Call when a root ENTERS the queue. */
export const warmRoot = (root: string): Promise<void> => client().node.warmRoot(root);

/** Pin a persistent, auto-reconnecting connection to the master (call once at startup with the
 *  bootstrap addrs) so it isn't dialed lazily + pruned when idle. */
export const keepaliveMaster = (addrs: string[]): Promise<void> => client().node.keepaliveMaster(addrs);

/** Resolve a v1 root -> exact module bytes (ArrayBuffer). v2+ roots stream instead. */
export const fetchModule = (root: string): Promise<ArrayBuffer> => client().media.fetchModule(root);

/** Detect which external trackers are installed. Desktop-only: a browser cannot execute a program,
 *  so it returns [] — and menus.ts already renders a disabled "No tracker found" placeholder for
 *  the empty case, so the web build degrades with no extra branching. */
export const listInstalledTrackers = (): Promise<TrackerInfo[]> =>
  can("openInTracker") ? client().media.installedTrackers!() : Promise.resolve([]);

/** Reassemble a module's byte-exact original from its root CID, verify it against the catalog
 *  `md5`, and hand it to the user. Saving works EVERYWHERE (desktop writes to Downloads, web offers
 *  a Blob) — only *launching a tracker* is desktop-only, so the two are split. `openWith` is a
 *  TrackerInfo.id the backend re-resolves; an id it doesn't know is refused, not launched. */
export const downloadAndOpen = (args: {
  root: string;
  md5: string;
  filename: string;
  openWith?: string;
}): Promise<DownloadResult> => {
  if (can("openInTracker")) return client().media.openInTracker!(args);
  return client()
    .media.saveModule(args)
    .then(() => ({ path: args.filename, launched: false, launch_error: null }));
};

/** Reassemble a module's byte-exact original and hand it to the user WITHOUT launching anything —
 *  the web "Download" action (desktop uses "Open with" instead). Saves everywhere: a Blob download
 *  in the browser, ~/Downloads on desktop. */
export const downloadModule = (args: { root: string; md5: string; filename: string }): Promise<void> =>
  client().media.saveModule(args);

/** Begin a v2+ stream; `onEvent` ticks skeleton -> sample… -> complete. */
export const startStream = (root: string, onEvent: (e: StreamEvent) => void): Promise<void> =>
  client().media.startStream(root, onEvent);

/** Assembled skeleton bytes (init the immortal instance). Ready after `skeleton`. */
export const getSkeleton = (root: string): Promise<ArrayBuffer> => client().media.getSkeleton(root);

/** One streamed sample's decoded PCM. Ready after its `sample` event. */
export const getSample = (root: string, index: number): Promise<ArrayBuffer> =>
  client().media.getSample(root, index);

/** Push the live playhead order so the prefetch scheduler reprioritizes (closed loop; also reseeds
 *  the queue on seek). Fire-and-forget. */
export const setPlayhead = (root: string, order: number): Promise<void> =>
  client().media.setPlayhead(root, order);
