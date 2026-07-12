// Hardware media-key + OS "Now Playing" integration. Two layers feed one debounced
// dispatcher so a single physical press (which can surface via BOTH paths at once on some
// systems) only acts once:
//
//   1. Global shortcuts (tauri-plugin-global-shortcut): captures the keyboard media keys
//      (Play/Pause, Next, Previous) plus most wired/Bluetooth headset transport buttons
//      system-wide — they work even when the window isn't focused, the same behavior a
//      native media player has.
//   2. navigator.mediaSession: publishes the current track to the OS Now Playing widget and
//      registers action handlers, so a remote command that routes through the media session
//      (rather than as a raw media-key HID event) is honored too.
//
// Playback state itself lives in player.svelte.ts; this module only translates presses into
// the existing toggle()/playNext()/playPrev() controls.
import { register, unregister, isRegistered } from "@tauri-apps/plugin-global-shortcut";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { invoke } from "@tauri-apps/api/core";
import { player, playNext, playPrev, nowPlaying } from "./player.svelte";
import { dbg, logWarn } from "./debug";

type Action = "playpause" | "next" | "prev";

// A single press can reach us twice — once via the global-shortcut HID path and once via a
// mediaSession remote command. Coalesce repeats of the SAME action within this window so we
// act once regardless of how many layers delivered it.
const COALESCE_MS = 250;
const lastFired: Record<Action, number> = { playpause: 0, next: 0, prev: 0 };

function dispatch(action: Action): void {
  const now = performance.now();
  if (now - lastFired[action] < COALESCE_MS) return;
  lastFired[action] = now;
  dbg("mediaKey", { action });
  switch (action) {
    case "playpause":
      // Mirror the spacebar handler: only toggle once a module is actually loaded, so a
      // stray press with nothing playing is a no-op rather than an error.
      if (player.info) player.toggle();
      break;
    case "next":
      playNext();
      break;
    case "prev":
      playPrev();
      break;
  }
}

// The plugin's accelerator names for the three hardware media keys. Same identifiers on
// macOS, Windows and Linux.
const SHORTCUTS: [accelerator: string, action: Action][] = [
  ["MediaPlayPause", "playpause"],
  ["MediaTrackNext", "next"],
  ["MediaTrackPrevious", "prev"],
];

let registered: string[] = [];

// macOS delivers media keys through the native MediaPlayer bridge, not global shortcuts —
// Carbon hotkeys can't grab the NSSystemDefined media keys, so registering them there only
// produces "Failed to watch media key event" noise. Let the native path own macOS; use global
// shortcuts on Windows/Linux, where they do capture the media keys.
const isMacOS = typeof navigator !== "undefined" && /Mac/i.test(navigator.userAgent);

async function registerShortcuts(): Promise<void> {
  if (isMacOS) return;
  for (const [accel, action] of SHORTCUTS) {
    try {
      // A registration left behind by a previous (crashed) run makes register() throw, so
      // clear any stale grab of this key first.
      if (await isRegistered(accel)) await unregister(accel);
      await register(accel, (event) => {
        // The plugin fires on both key-down and key-up; act on the press only so a single
        // tap isn't handled twice.
        if (event.state === "Pressed") dispatch(action);
      });
      registered.push(accel);
    } catch (e) {
      // A key already claimed by another app, or one this OS/keyboard doesn't expose,
      // shouldn't block the rest — the app still works, just without that one key.
      logWarn("mediaKeys:register", e, { accel });
    }
  }
}

async function unregisterShortcuts(): Promise<void> {
  await Promise.all(
    registered.map((accel) => unregister(accel).catch((e) => logWarn("mediaKeys:unregister", e, { accel }))),
  );
  registered = [];
}

// Whether the mediaSession action handlers have been installed yet (once is enough — they
// stay attached for the app's lifetime).
let mediaSessionWired = false;

function wireMediaSession(): void {
  if (mediaSessionWired || !("mediaSession" in navigator)) return;
  const ms = navigator.mediaSession;
  try {
    // Route every play/pause command through the same debounced toggle. With playbackState
    // reported below, the OS sends "play" only when paused and "pause" only when playing, so
    // a toggle always lands the right way.
    ms.setActionHandler("play", () => dispatch("playpause"));
    ms.setActionHandler("pause", () => dispatch("playpause"));
    ms.setActionHandler("nexttrack", () => dispatch("next"));
    ms.setActionHandler("previoustrack", () => dispatch("prev"));
    mediaSessionWired = true;
  } catch (e) {
    logWarn("mediaKeys:mediaSession", e);
  }
}

/**
 * Push the current track + play/pause state to the OS Now Playing widget. Call this from a
 * reactive context (a Svelte `$effect`) that reads `nowPlaying.hit` and `player.playing` so it
 * re-runs on every change — the reads below are what establish that dependency.
 *
 * Two sinks: `navigator.mediaSession` (where it activates) and, on macOS, the native
 * MPNowPlayingInfoCenter via the `update_now_playing` command — the latter is also what makes
 * the OS route media keys + headphone commands back to us, so it must fire on every play/pause.
 */
export function syncMediaSession(): void {
  const hit = nowPlaying.hit;
  const playing = player.playing;
  const title = hit ? hit.title || hit.filename : "";
  // Tracker modules have no artist field; surface the format as the subtitle instead.
  const artist = hit ? (hit.format ? hit.format.toUpperCase() : "tracker module") : "";

  if ("mediaSession" in navigator) {
    const ms = navigator.mediaSession;
    if (hit) {
      const MediaMetadataCtor = (globalThis as { MediaMetadata?: typeof MediaMetadata }).MediaMetadata;
      if (MediaMetadataCtor) {
        ms.metadata = new MediaMetadataCtor({ title, artist, album: "trackerstream" });
      }
      ms.playbackState = playing ? "playing" : "paused";
    } else {
      ms.metadata = null;
      ms.playbackState = "none";
    }
  }

  // Native OS Now Playing (macOS): the Rust command no-ops on other platforms. Fire only when a
  // track is loaded so we don't claim the Now Playing slot with nothing playing.
  if (hit) {
    void invoke("update_now_playing", { title, artist, playing }).catch((e) =>
      logWarn("mediaKeys:update_now_playing", e),
    );
  }
}

// macOS delivers media keys + headphone commands through the native MediaPlayer bridge
// (src-tauri/src/mediakeys_macos.rs), which emits this event. Route it into the same debounced
// dispatcher so it shares the coalesce guard with the global-shortcut path.
async function listenNativeCommands(): Promise<UnlistenFn> {
  return listen<Action>("media-remote-command", (e) => dispatch(e.payload));
}

/**
 * Start capturing media keys and populating Now Playing. Returns a cleanup function that
 * releases the global shortcuts and the native command listener. Safe to call once at startup.
 */
export function initMediaKeys(): () => void {
  wireMediaSession();
  void registerShortcuts();
  const unlisten = listenNativeCommands();
  return () => {
    void unregisterShortcuts();
    void unlisten.then((fn) => fn()).catch((e) => logWarn("mediaKeys:unlisten", e));
  };
}
