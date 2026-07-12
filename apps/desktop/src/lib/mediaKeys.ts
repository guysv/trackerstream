// Hardware media-key + OS "Now Playing" integration.
//
// Three layers feed one dispatcher:
//
//   1. The native macOS MediaPlayer bridge (src-tauri/src/mediakeys_macos.rs), which emits
//      `media-remote-command`. This is the only path on macOS that sees the keyboard media keys
//      AND the AirPods/headset transport buttons.
//   2. Global shortcuts (tauri-plugin-global-shortcut) on Windows/Linux, where they do capture the
//      media keys system-wide — even unfocused, like a native player. Not compiled on macOS.
//   3. navigator.mediaSession, for any remote command that routes through the media session rather
//      than as a raw key event.
//
// Playing nicely with other media apps
// ------------------------------------
// The media keys and the OS Now Playing slot are shared, single-occupant resources: whoever grabs
// them takes them away from Spotify/Music/YouTube. So we take them LAZILY — nothing is grabbed at
// launch. We register the global shortcuts and publish a Now Playing entry only once the user has
// actually started playing a module (see `syncMediaSession`), and we hand the slot back when
// nothing is loaded. A trackerstream sitting idle in the background stays out of the way, and on
// macOS the OS arbitrates the rest for us (whoever last reported "playing" owns the keys).
//
// Playback state itself lives in player.svelte.ts; this module only translates presses into the
// existing play()/pause()/toggle()/playNext()/playPrev() controls.
import { register, unregister, isRegistered } from "@tauri-apps/plugin-global-shortcut";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { invoke } from "@tauri-apps/api/core";
import { untrack } from "svelte";
import { player, playNext, playPrev, nowPlaying, queue } from "./player.svelte";
import { dbg, logWarn } from "./debug";

// `play` and `pause` are deliberately NOT collapsed into `playpause`. The OS tells us which one it
// wants, and a toggle is only correct while the OS's view of our play state matches ours — the
// moment the two drift, a `play` arriving while we already think we're playing would *pause* us,
// i.e. exactly inverted. Applied separately, each command is idempotent and any drift is
// self-correcting.
type Action = "play" | "pause" | "playpause" | "next" | "prev";

// A single press can reach us twice (macOS can deliver both `togglePlayPause` and `play` for one
// key), so a repeat of the same action inside this window is dropped. The windows differ by action
// because the cost of a double-fire does:
//   - playpause is a TOGGLE: a duplicate undoes the press, so it needs a real guard.
//   - play/pause are idempotent (we check the current state before acting) — no guard needed.
//   - next/prev are not idempotent, but deliberately double-tapping to skip two tracks is a thing
//     people do, so the guard is only wide enough to swallow a duplicate delivery of one press —
//     well under the ~150ms+ of a human double-tap.
const COALESCE_MS: Record<Action, number> = { playpause: 250, play: 0, pause: 0, next: 60, prev: 60 };
const lastFired: Record<Action, number> = { playpause: 0, play: 0, pause: 0, next: 0, prev: 0 };

function dispatch(action: Action): void {
  const now = performance.now();
  if (now - lastFired[action] < COALESCE_MS[action]) return;
  lastFired[action] = now;
  dbg("mediaKey", { action });
  // Mirror the spacebar handler: with nothing loaded, a transport command is a no-op rather than an
  // error. The OS shouldn't send us any (we keep the remote commands disabled until a track is
  // loaded), but a stray global-shortcut press on Win/Linux can still land here.
  if (!player.info) return;
  switch (action) {
    case "play":
      if (!player.playing) void player.play();
      break;
    case "pause":
      if (player.playing) player.pause();
      break;
    case "playpause":
      player.toggle();
      break;
    case "next":
      playNext();
      break;
    case "prev":
      playPrev();
      break;
  }
}

// The plugin's accelerator names for the three hardware media keys. Same identifiers on Windows and
// Linux. There's only one physical play/pause key, so it carries toggle semantics.
const SHORTCUTS: [accelerator: string, action: Action][] = [
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

/**
 * Grab the media keys — once, and only when we have something to play.
 *
 * A global shortcut is an exclusive, system-wide grab with no arbitration: whoever holds
 * MediaPlayPause receives it even while idle in the background. Grabbing it at launch therefore
 * means a never-used trackerstream silently swallows the key the user meant for Spotify. Deferring
 * the grab to first playback keeps an idle app out of the way, and matches how macOS behaves anyway
 * (the system only routes keys to us once we're the active Now Playing app).
 */
async function claimShortcuts(): Promise<void> {
  if (isMacOS || shortcutsClaimed) return;
  shortcutsClaimed = true;
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

async function releaseShortcuts(): Promise<void> {
  const held = registered;
  registered = [];
  shortcutsClaimed = false;
  await Promise.all(
    held.map((accel) => unregister(accel).catch((e) => logWarn("mediaKeys:unregister", e, { accel }))),
  );
}

// Whether the mediaSession action handlers are currently installed.
let mediaSessionWired = false;

function wireMediaSession(): void {
  if (mediaSessionWired || !("mediaSession" in navigator)) return;
  const ms = navigator.mediaSession;
  try {
    ms.setActionHandler("play", () => dispatch("play"));
    ms.setActionHandler("pause", () => dispatch("pause"));
    ms.setActionHandler("nexttrack", () => dispatch("next"));
    ms.setActionHandler("previoustrack", () => dispatch("prev"));
    mediaSessionWired = true;
  } catch (e) {
    logWarn("mediaKeys:mediaSession", e);
  }
}

function unwireMediaSession(): void {
  if (!mediaSessionWired) return;
  const ms = navigator.mediaSession;
  try {
    for (const a of ["play", "pause", "nexttrack", "previoustrack"] as const) ms.setActionHandler(a, null);
    ms.metadata = null;
    ms.playbackState = "none";
  } catch (e) {
    logWarn("mediaKeys:unwireMediaSession", e);
  }
  mediaSessionWired = false;
}

/** What the OS Now Playing widget shows. Tracker modules have no artist field, so the format
 *  stands in as the subtitle. */
function nowPlayingMeta(): { title: string; artist: string } | null {
  const hit = nowPlaying.hit;
  if (!hit) return null;
  return {
    title: hit.title || hit.filename,
    artist: hit.format ? hit.format.toUpperCase() : "tracker module",
  };
}

/**
 * Push the current track + play/pause state to the OS Now Playing widget, or hand the slot back
 * when nothing is loaded. Call from a reactive context (a Svelte `$effect`): the reads below —
 * `nowPlaying.hit`, `player.playing`, `player.info`, `queue.index`, `queue.items.length` — are what
 * make it re-run on every track change, play/pause and queue edit.
 *
 * Deliberately NOT reactive on `player.pos`: that ticks continuously, and depending on it would
 * fire an IPC call every frame. The OS only needs an elapsed time plus a playback rate — it
 * extrapolates the scrubber from those itself — so we publish on discrete changes and resync after
 * a seek (`player.onSeek`, wired in `initMediaKeys`).
 */
export function syncMediaSession(): void {
  const meta = nowPlayingMeta();
  const playing = player.playing;
  const duration = player.info?.durationSeconds ?? nowPlaying.hit?.duration ?? 0;
  // Read the playhead untracked — see the note above about per-frame IPC.
  const elapsed = untrack(() => player.pos?.seconds ?? 0);

  const hasNext = queue.index >= 0 && queue.index < queue.items.length - 1;
  // playPrev() restarts the current track when >5s in, so "previous" does something whenever a
  // track is loaded — not only when a previous queue entry exists. Same as the in-app back button.
  const hasPrev = meta != null;

  if ("mediaSession" in navigator) {
    const ms = navigator.mediaSession;
    const MediaMetadataCtor = (globalThis as { MediaMetadata?: typeof MediaMetadata }).MediaMetadata;
    if (meta) {
      if (MediaMetadataCtor) ms.metadata = new MediaMetadataCtor({ ...meta, album: "trackerstream" });
      ms.playbackState = playing ? "playing" : "paused";
    } else {
      ms.metadata = null;
      ms.playbackState = "none";
    }
  }

  // The native OS Now Playing slot (macOS; a no-op elsewhere). Publishing it is also what makes the
  // system route media keys + headphone buttons to us, so it must fire on every play/pause. And
  // releasing it when nothing is loaded is what keeps an idle trackerstream out of Control Center,
  // rather than holding the keys hostage from whatever the user is really listening to.
  if (!meta) {
    void invoke("clear_now_playing").catch((e) => logWarn("mediaKeys:clear_now_playing", e));
    return;
  }
  // First actual playback: now — and only now — is it fair to take the media keys.
  void claimShortcuts();
  void invoke("update_now_playing", { ...meta, playing, duration, elapsed, hasNext, hasPrev }).catch((e) =>
    logWarn("mediaKeys:update_now_playing", e),
  );
}

// A seek is a discrete jump the OS can't extrapolate, so republish the elapsed time. The worklet
// reports the new playhead asynchronously — player.pos is still pre-seek at the moment the seek is
// sent — so let the next tick land before reading it.
let seekResync: ReturnType<typeof setTimeout> | null = null;
function scheduleSeekResync(): void {
  if (seekResync) clearTimeout(seekResync);
  seekResync = setTimeout(() => {
    seekResync = null;
    syncMediaSession();
  }, 250);
}

// macOS delivers media keys + headphone commands through the native MediaPlayer bridge
// (src-tauri/src/mediakeys_macos.rs), which emits this event. Route it into the same dispatcher so
// it shares the coalesce guard with the global-shortcut path.
async function listenNativeCommands(): Promise<UnlistenFn> {
  return listen<Action>("media-remote-command", (e) => dispatch(e.payload));
}

/**
 * Start listening for media keys and keeping Now Playing in step. Grabs nothing yet — the global
 * shortcuts and the Now Playing slot are claimed on first playback (see `syncMediaSession`).
 * Returns a cleanup function that releases everything we hold.
 */
export function initMediaKeys(): () => void {
  wireMediaSession();
  player.onSeek = scheduleSeekResync;
  const unlisten = listenNativeCommands();
  return () => {
    if (seekResync) clearTimeout(seekResync);
    seekResync = null;
    player.onSeek = null;
    unwireMediaSession();
    void releaseShortcuts();
    void invoke("clear_now_playing").catch((e) => logWarn("mediaKeys:clear_now_playing", e));
    void unlisten.then((fn) => fn()).catch((e) => logWarn("mediaKeys:unlisten", e));
  };
}
