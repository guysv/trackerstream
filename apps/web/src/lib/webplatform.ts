// Browser-local pieces of the NodeClient, shared by the leader (WebClient) and the follower
// (FollowerClient).
//
// These deliberately DO NOT touch the libp2p node: `caps` is a constant, and every `platform`
// method operates on the tab's own browser surface — navigator.mediaSession, location, the console.
// A follower must run them locally (the media keys and Now Playing slot belong to whichever tab is
// actually decoding audio, not the leader tab that happens to own the node), so they are factored
// out here rather than proxied across the tab boundary.
import type { Capabilities, MediaKeyAction, NowPlayingMeta, Unsub } from "@trackerstream/ui/client";

/** What a browser shell can do. Constant for the web build — same in leader and follower tabs. */
export const WEB_CAPS: Capabilities = {
  // A browser cannot execute a program. Saving still works — reassembly is byte-exact, so we hand
  // the user the real module file as a Blob.
  openInTracker: false,
  logsDir: false,
  // navigator.mediaSession routes hardware media keys to the audio-playing tab. What we cannot do
  // is grab them system-wide while another app is focused — a weaker capability, not a missing one.
  globalMediaKeys: false,
  // The browser reserves on the master (see node.ts), so it is dialable AND serves
  // "/trackerstream/playlist-list/1.0.0" like a desktop — it both answers and asks.
  peerPlaylists: true,
};

/** The `platform` group — pure browser, no node. Identical in leader and follower tabs. */
export function makePlatform(): {
  log(level: "debug" | "warn" | "error", ...args: unknown[]): void;
  onDeepLink(cb: (url: string) => void): Unsub;
  nowPlaying(meta: NowPlayingMeta | null): void;
  mediaKeys(handler: (action: MediaKeyAction) => void): Unsub;
} {
  return {
    log: (level, ...args) => {
      // No log FILE in a browser — the console is the log.
      (level === "error" ? console.error : level === "warn" ? console.warn : console.debug)(...args);
    },
    // The web "deep link" is simply the /p/<name> route the tab was opened on.
    onDeepLink: (cb) => {
      if (location.pathname.startsWith("/p/")) cb(location.href);
      return () => {};
    },
    nowPlaying: (meta) => {
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
    mediaKeys: (handler) => {
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
}

/** Hand `bytes` to the user as a file download. Runs in whichever tab calls it — which is why a
 *  follower reassembles remotely but downloads LOCALLY (a background leader tab firing this would
 *  drop the file into the wrong window). */
export function downloadBlob(bytes: Uint8Array, filename: string): void {
  const url = URL.createObjectURL(new Blob([bytes as BlobPart], { type: "application/octet-stream" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
