// The desktop shell's NodeClient: every method is a Tauri `invoke` into the Rust backend, which
// talks to the in-process Go tsnode sidecar over a loopback RPC.
//
// This file is the ONLY place @tauri-apps is imported in the desktop frontend. Everything above it
// (@trackerstream/ui) is shared with the web client verbatim. Nothing here has logic of its own —
// it is a router. If you find yourself adding behaviour here, it probably belongs in the UI facade
// so the web shell gets it too.
import { Channel, invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { debug, error, warn } from "@tauri-apps/plugin-log";
import { getCurrent, onOpenUrl } from "@tauri-apps/plugin-deep-link";
import { isRegistered, register, unregister } from "@tauri-apps/plugin-global-shortcut";
import { CATALOG_IPNS_KEY, CATALOG_Z_IPNS_KEY } from "@trackerstream/config";
import type {
  CatalogSearchOpts,
  DownloadResult,
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
  TrackerInfo,
  Unsub,
} from "@trackerstream/ui/client";

// Prefer the per-page-zstd catalog (TSZCAT) when the master publishes one, else the raw SQLite.
// The Rust VFS auto-detects the format from the resolved root, so a single `name` works for both —
// the choice is purely which IPNS record we resolve.
const CATALOG_NAME = CATALOG_Z_IPNS_KEY || CATALOG_IPNS_KEY;

// ---- media keys: the OS-level grab, which only a native shell can do ----

// The plugin's accelerator names for the three hardware media keys. Same identifiers on Windows and
// Linux. There's only one physical play/pause key, so it carries toggle semantics.
const SHORTCUTS: [accelerator: string, action: MediaKeyAction][] = [
  ["MediaPlayPause", "playpause"],
  ["MediaTrackNext", "next"],
  ["MediaTrackPrevious", "prev"],
];

// macOS delivers media keys through the native MediaPlayer bridge, not global shortcuts — Carbon
// hotkeys can't grab the NSSystemDefined media keys, so registering them there would only produce
// "Failed to watch media key event" noise (and the plugin isn't compiled in on macOS at all).
const isMacOS = typeof navigator !== "undefined" && /Mac/i.test(navigator.userAgent);

let registered: string[] = [];
let shortcutsClaimed = false;
let dispatchKey: ((a: MediaKeyAction) => void) | null = null;

/** Grab the media keys — once, and only when we have something to play.
 *
 *  A global shortcut is an exclusive, system-wide grab with no arbitration: whoever holds
 *  MediaPlayPause receives it even while idle in the background. Grabbing it at launch therefore
 *  means a never-used trackerstream silently swallows the key the user meant for Spotify. Deferring
 *  the grab to first playback (i.e. the first non-null nowPlaying) keeps an idle app out of the way,
 *  and matches how macOS behaves anyway. */
async function claimShortcuts(): Promise<void> {
  if (isMacOS || shortcutsClaimed) return;
  shortcutsClaimed = true;
  for (const [accel, action] of SHORTCUTS) {
    try {
      // A registration left behind by a previous (crashed) run makes register() throw, so clear any
      // stale grab of this key first.
      if (await isRegistered(accel)) await unregister(accel);
      await register(accel, (event) => {
        // The plugin fires on both key-down and key-up; act on the press only.
        if (event.state === "Pressed") dispatchKey?.(action);
      });
      registered.push(accel);
    } catch {
      // A key already claimed by another app, or one this OS/keyboard doesn't expose, shouldn't
      // block the rest — the app still works, just without that one key.
    }
  }
}

async function releaseShortcuts(): Promise<void> {
  const held = registered;
  registered = [];
  shortcutsClaimed = false;
  await Promise.all(held.map((accel) => unregister(accel).catch(() => {})));
}

export class TauriClient implements NodeClient {
  readonly caps = {
    openInTracker: true,
    logsDir: true,
    globalMediaKeys: true,
    peerPlaylists: true,
  };

  node = {
    info: (): Promise<NodeInfo> => invoke<NodeInfo>("node_info"),
    peers: (): Promise<PeerStats> => invoke<PeerStats>("peer_stats"),
    peerDetail: (id: string): Promise<PeerDetail> => invoke<PeerDetail>("peer_detail", { peerId: id }),
    connect: (addr: string): Promise<void> => invoke("connect_peer", { addr }),
    keepaliveMaster: (addrs: string[]): Promise<void> => invoke("keepalive_master", { addrs }),
    warmRoot: (root: string): Promise<void> => invoke("warm_root", { root }),
  };

  catalog = {
    query: <T>(req: Record<string, unknown>): Promise<T> =>
      invoke<T>("catalog_query", { name: CATALOG_NAME, req }),
    searchStream: (opts: CatalogSearchOpts, onRow: (h: ModuleHit) => void): Promise<number> => {
      const ch = new Channel<ModuleHit>();
      ch.onmessage = onRow;
      return invoke<number>("catalog_search_stream", {
        name: CATALOG_NAME,
        q: opts.q,
        limit: opts.limit,
        after: opts.after,
        names: opts.namesOnly ?? false,
        onRow: ch,
      });
    },
    cancel: (): Promise<void> => invoke("catalog_cancel"),
    warm: (): Promise<void> => invoke("catalog_warm", { name: CATALOG_NAME }),
  };

  media = {
    fetchModule: (root: string): Promise<ArrayBuffer> => invoke<ArrayBuffer>("fetch_module", { root }),
    startStream: (root: string, onEvent: (e: StreamEvent) => void): Promise<void> => {
      const ch = new Channel<StreamEvent>();
      ch.onmessage = onEvent;
      return invoke("start_stream", { root, onEvent: ch });
    },
    getSkeleton: (root: string): Promise<ArrayBuffer> => invoke<ArrayBuffer>("get_skeleton", { root }),
    getSample: (root: string, index: number): Promise<ArrayBuffer> =>
      invoke<ArrayBuffer>("get_sample", { root, index }),
    setPlayhead: (root: string, order: number): Promise<void> => invoke("set_playhead", { root, order }),
    // Save-without-launch is download_and_open with no tracker: the backend writes the byte-exact
    // rebuild to the Downloads dir and simply doesn't exec anything.
    saveModule: (args: { root: string; md5: string; filename: string }): Promise<void> =>
      invoke<DownloadResult>("download_and_open", { ...args, openWith: null }).then(() => {}),
    openInTracker: (args: {
      root: string;
      md5: string;
      filename: string;
      openWith?: string;
    }): Promise<DownloadResult> =>
      invoke<DownloadResult>("download_and_open", {
        root: args.root,
        md5: args.md5,
        filename: args.filename,
        openWith: args.openWith ?? null,
      }),
    installedTrackers: (): Promise<TrackerInfo[]> => invoke<TrackerInfo[]>("list_installed_trackers"),
  };

  playlists = {
    search: (q: string): Promise<PlaylistMeta[]> => invoke<PlaylistMeta[]>("playlist_search", { q }),
    list: (scope: PlaylistScope): Promise<PlaylistMeta[]> =>
      invoke<PlaylistMeta[]>("playlist_list", { scope }),
    hold: (name: string, held: boolean): Promise<void> => invoke("playlist_hold", { name, held }),
    get: (name: string): Promise<PlaylistDetail | null> =>
      invoke<PlaylistDetail | null>("playlist_get", { name }),
    create: (title: string, tracks: TrackTuple[]): Promise<PlaylistMeta> =>
      invoke<PlaylistMeta>("playlist_create", { title, tracks }),
    update: (name: string, title: string, tracks: TrackTuple[]): Promise<void> =>
      invoke("playlist_update", { name, title, tracks }),
    remove: (name: string): Promise<void> => invoke("playlist_delete", { name }),
    publish: (name: string): Promise<void> => invoke("playlist_publish", { name }),
    unpublish: (name: string): Promise<void> => invoke("playlist_unpublish", { name }),
    syncStatus: (): Promise<PlaylistSyncStatus> => invoke<PlaylistSyncStatus>("playlist_sync_status"),
    likeToggle: (track: TrackTuple): Promise<boolean> => invoke<boolean>("playlist_like_toggle", { track }),
    likedIds: (): Promise<string[]> => invoke<string[]>("playlist_liked_ids"),
    likedName: (): Promise<string> => invoke<string>("playlist_liked_name"),
    ingestLink: (url: string): Promise<LinkStatus> => invoke<LinkStatus>("playlist_ingest_link", { url }),
    copyLink: (name: string): Promise<string> => invoke<string>("playlist_copy_link", { name }),
    pending: (): Promise<string[]> => invoke<string[]>("playlist_pending"),
    backers: (names: string[]): Promise<Record<string, number>> =>
      invoke<Record<string, number>>("playlist_backers", { names }),
    pin: (name: string | null): Promise<void> => invoke("playlist_pin", { name }),
    played: (name: string): Promise<void> => invoke("playlist_played", { name }),
    ofPeer: (peerId: string): Promise<PeerPlaylists> => invoke<PeerPlaylists>("peer_playlists", { peerId }),
    request: (peerId: string, name: string): Promise<number> =>
      invoke<number>("playlist_request", { peerId, name }),
  };

  platform = {
    // Into the shared rotating log file (tauri-plugin-log), alongside the Rust/Go logs — so a
    // swallowed catch in the UI is diagnosable from the same place as a backend failure.
    log: (level: "debug" | "warn" | "error", ...args: unknown[]): void => {
      const line = args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" ");
      const sink = level === "error" ? error : level === "warn" ? warn : debug;
      void sink(line).catch(() => {});
    },

    onDeepLink: (cb: (url: string) => void): Unsub => {
      let stop: (() => void) | null = null;
      let dead = false;
      void (async () => {
        try {
          // The cold-start URL: the link that LAUNCHED the app, which arrives before any listener
          // could have been registered.
          const launched = await getCurrent();
          if (launched) for (const u of launched) cb(u);
          const un = await onOpenUrl((urls) => {
            for (const u of urls) cb(u);
          });
          if (dead) un();
          else stop = un;
        } catch {
          /* deep-link plugin unavailable in this runtime */
        }
      })();
      return () => {
        dead = true;
        stop?.();
      };
    },

    // The native OS Now Playing slot (macOS; a no-op elsewhere). Publishing it is also what makes
    // the system route media keys + headphone buttons to us, so it must fire on every play/pause.
    nowPlaying: (meta: NowPlayingMeta | null): void => {
      if (!meta) {
        void invoke("clear_now_playing").catch(() => {});
        return;
      }
      void claimShortcuts(); // first actual playback — now, and only now, is it fair to take the keys
      void invoke("update_now_playing", { ...meta }).catch(() => {});
    },

    mediaKeys: (handler: (action: MediaKeyAction) => void): Unsub => {
      dispatchKey = handler;
      // macOS delivers media keys + headphone commands through the native MediaPlayer bridge
      // (src-tauri/src/mediakeys_macos.rs), which emits this event.
      const un = listen<MediaKeyAction>("media-remote-command", (e) => handler(e.payload));
      return () => {
        dispatchKey = null;
        void releaseShortcuts();
        void un.then((fn) => fn()).catch(() => {});
      };
    },

    openLogsDir: (): Promise<void> => invoke("open_logs_dir"),
  };

  events = {
    on: (event: "playlists:changed", cb: () => void): Unsub => {
      const un = listen(event, () => cb());
      return () => void un.then((fn) => fn()).catch(() => {});
    },
  };
}
