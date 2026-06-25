// App-wide playback: a singleton ModPlayer + the progressive-streaming flow,
// shared by the browse list (to start playback) and the now-playing bar (to show
// + control it). Module bytes stream from CID blocks over libp2p (the embedded
// rust-ipfs node); the control plane only hands us a root CID.
import { BOOTSTRAP_MULTIADDRS } from "@trackerstream/config";
import { ModPlayer } from "./audio/ModPlayer.svelte";
import { connectPeer, startStream, getStreamBuffer } from "./p2p";
import { pushPresence } from "./social.svelte";
import type { ModuleHit } from "./catalog";

export const player = new ModPlayer();

export const nowPlaying = $state<{
  hit: ModuleHit | null;
  pct: number;
  streaming: boolean;
  /** Waiting for the first pattern's samples to arrive before playback starts
   *  (B2) — true from the moment we begin streaming until the backend reports
   *  the opening sample set is resident (`playable`). Drives the UI indicator. */
  buffering: boolean;
  error: string;
}>({ hit: null, pct: 0, streaming: false, buffering: false, error: "" });

/** The play queue (Phase 6) — also drives next/prev + gapless auto-advance.
 *  Persisted locally so it survives restarts. */
export const queue = $state<{ items: ModuleHit[]; index: number }>(loadQueue());

function loadQueue(): { items: ModuleHit[]; index: number } {
  try {
    const s = JSON.parse(localStorage.getItem("ts.queue") ?? "");
    if (Array.isArray(s?.items)) return { items: s.items, index: -1 };
  } catch {
    /* none */
  }
  return { items: [], index: -1 };
}
function saveQueue(): void {
  try {
    localStorage.setItem("ts.queue", JSON.stringify({ items: queue.items }));
  } catch {
    /* headless */
  }
}

// Gapless auto-advance: when a track ends, play the next queued one.
player.onEnded = () => playNext();

export function playList(items: ModuleHit[], index: number): void {
  queue.items = items;
  queue.index = index;
  saveQueue();
  const hit = items[index];
  if (hit) void playModule(hit);
}

export function clearQueue(): void {
  queue.items = [];
  queue.index = -1;
  saveQueue();
}

export function removeFromQueue(i: number): void {
  queue.items.splice(i, 1);
  if (i < queue.index) queue.index--;
  else if (i === queue.index) queue.index = Math.min(queue.index, queue.items.length - 1);
  saveQueue();
}

export function moveInQueue(i: number, dir: -1 | 1): void {
  const j = i + dir;
  if (j < 0 || j >= queue.items.length) return;
  [queue.items[i], queue.items[j]] = [queue.items[j], queue.items[i]];
  if (queue.index === i) queue.index = j;
  else if (queue.index === j) queue.index = i;
  saveQueue();
}

export function playNext(): void {
  if (queue.index >= 0 && queue.index < queue.items.length - 1) playList(queue.items, queue.index + 1);
}

export function playPrev(): void {
  if (queue.index > 0) playList(queue.items, queue.index - 1);
}

export function enqueue(hit: ModuleHit, next = false): void {
  if (next && queue.index >= 0) queue.items.splice(queue.index + 1, 0, hit);
  else queue.items.push(hit);
  saveQueue();
  if (queue.index < 0) playList(queue.items, 0);
}

let connected = false;
async function ensureConnected() {
  if (connected) return;
  // Try every bootstrap addr (literal IPs first, then /dns*) until one connects —
  // a failed dial (e.g. an unsupported /dns* transport) just falls through.
  for (const addr of BOOTSTRAP_MULTIADDRS) {
    try {
      await connectPeer(addr);
      connected = true;
      return;
    } catch {
      /* try the next addr */
    }
  }
  /* none connected; will retry on next play (master may be momentarily unreachable) */
}

/** Stream + play a module, starting on the first (skeleton) blocks. */
export async function playModule(hit: ModuleHit): Promise<void> {
  nowPlaying.hit = hit;
  nowPlaying.pct = 0;
  nowPlaying.streaming = true;
  nowPlaying.buffering = true; // waiting for the first pattern's samples (B2)
  nowPlaying.error = "";
  pushPresence(hit); // presence: "now playing" (no-op when logged out)
  try {
    await player.init();
    await ensureConnected();
    let started = false;
    let lastReload = 0;
    await startStream(hit.rootCid, async (p) => {
      nowPlaying.pct = Math.round(p.pct);
      try {
        // Hold playback until the backend reports the opening sample set is
        // resident (`playable`). Starting earlier renders the first pattern with
        // not-yet-arrived samples as silence — the chopped-opening bug.
        if (!started && p.playable) {
          started = true;
          nowPlaying.buffering = false;
          await player.load(await getStreamBuffer(hit.rootCid));
          await player.play();
        } else if (started && (p.complete || Date.now() - lastReload > 700)) {
          lastReload = Date.now();
          player.reload(await getStreamBuffer(hit.rootCid));
        }
      } catch (e) {
        nowPlaying.error = String(e);
      }
      if (p.complete) nowPlaying.streaming = false;
    });
  } catch (e) {
    nowPlaying.error = String(e);
    nowPlaying.streaming = false;
    nowPlaying.buffering = false;
  }
}
