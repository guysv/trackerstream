// App-wide playback: a singleton ModPlayer + the progressive-streaming flow,
// shared by the browse list (to start playback) and the now-playing bar (to show
// + control it). Module bytes stream from CID blocks over libp2p (the embedded
// rust-ipfs node); the control plane only hands us a root CID.
import { BOOTSTRAP_MULTIADDRS } from "@trackerstream/config";
import { ModPlayer } from "./audio/ModPlayer.svelte";
import { Fence } from "./audio/fence";
import { connectPeer, startStream, getSkeleton, getSample, setPlayhead } from "./p2p";
import { pushPresence } from "./social.svelte";
import { dbg } from "./debug";
import type { ModuleHit } from "./catalog";

export const player = new ModPlayer();

// Buffering UI is driven by the worklet fence (can appear mid-track now).
player.onBuffering = (active) => {
  dbg("fence.buffering", { active, pct: nowPlaying.pct, streaming: nowPlaying.streaming });
  nowPlaying.buffering = active;
};
// Monotonic play token. Bumped on every playModule() so a superseded track's
// still-running stream callback can detect it's stale and stop touching the
// shared worklet / nowPlaying state (the worklet is a singleton; a stale provide
// would patch samples into the WRONG instance — the cross-track contamination
// that left tracks stuck buffering).
let playEpoch = 0;

// Main-thread mirror of the worklet's fence, fed the same provideSample indices,
// so the UI can compute how much of the SONG TIMELINE is actually playable
// (fence-resident) — the real buffered region the seek bar fills, as opposed to
// raw % of samples downloaded. Same Fence logic that gates audio => the bar
// matches what you hear.
let bufferFence: Fence | null = null;
let planOrderSeconds: { order: number; seconds: number }[] = [];
function recomputeBuffered(): void {
  if (!bufferFence) return;
  const dur =
    player.info?.durationSeconds ||
    (planOrderSeconds.length ? planOrderSeconds[planOrderSeconds.length - 1].seconds : 0);
  nowPlaying.bufferedSeconds = bufferFence.playableSeconds(planOrderSeconds, dur);
}

// Closed-loop prefetch: push the live playhead order to the backend on change so
// the fetch scheduler prioritizes the samples the playhead is approaching.
let lastSentOrder = -1;
player.onPos = (pos) => {
  // Refresh the buffered edge against the now-known duration (info loads after the
  // first samples may already have arrived).
  recomputeBuffered();
  if (nowPlaying.hit && pos.order !== lastSentOrder) {
    dbg("pos.order", { order: pos.order, row: pos.row, seconds: +pos.seconds.toFixed(2) });
    lastSentOrder = pos.order;
    void setPlayhead(nowPlaying.hit.rootCid, pos.order);
  }
};

export const nowPlaying = $state<{
  hit: ModuleHit | null;
  pct: number;
  streaming: boolean;
  /** Waiting for the first pattern's samples to arrive before playback starts
   *  (B2) — true from the moment we begin streaming until the backend reports
   *  the opening sample set is resident (`playable`). Drives the UI indicator. */
  buffering: boolean;
  /** How many seconds of the SONG TIMELINE are contiguously playable from the
   *  start given the samples resident so far (fence-accurate). Drives the seek-bar
   *  buffered fill — the real buffered region, not raw % of samples downloaded. */
  bufferedSeconds: number;
  error: string;
}>({ hit: null, pct: 0, streaming: false, buffering: false, bufferedSeconds: 0, error: "" });

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

/**
 * Stream + play a module on the immortal-instance path: init the worklet from the
 * skeleton, start playback immediately (the fence holds at the opening until the
 * first checkpoint's samples are resident), then feed each streamed sample via
 * provideSample as it arrives. No recreate-on-grow, no opening-gate polling — the
 * worklet fence owns buffering, which is now sample-accurate and can appear
 * mid-track as an honest underrun.
 */
export async function playModule(hit: ModuleHit): Promise<void> {
  dbg("playModule.start", { cid: hit.rootCid, title: hit.title ?? hit.filename });
  nowPlaying.hit = hit;
  nowPlaying.pct = 0;
  nowPlaying.streaming = true;
  nowPlaying.buffering = true; // fence will clear this once the opening is resident
  nowPlaying.bufferedSeconds = 0;
  nowPlaying.error = "";
  lastSentOrder = -1;
  bufferFence = null;
  planOrderSeconds = [];
  const epoch = ++playEpoch; // this track's token; stale callbacks bail on mismatch
  pushPresence(hit); // presence: "now playing" (no-op when logged out)
  try {
    await player.init();
    await ensureConnected();
    let total = 0;
    let got = 0;
    // The instance must be init'd before any sample is patched in: provideSample
    // and init are separate postMessages, and the skeleton handler has more awaits
    // than the sample handler, so without this barrier a sample can reach the
    // worklet BEFORE init and be applied to the wrong/old instance (lost) — the
    // race that left tracks silently stuck at the opening. Sample handlers await
    // this; it resolves once init has been posted for this track.
    let markInited!: () => void;
    const inited = new Promise<void>((r) => (markInited = r));
    await startStream(hit.rootCid, async (e) => {
      if (epoch !== playEpoch) return; // superseded by a newer track
      try {
        switch (e.type) {
          case "skeleton": {
            total = e.samples;
            dbg("ev.skeleton", { samples: total, checkpoints: e.plan.checkpoints?.length ?? 0 });
            // Mirror the worklet fence on the main thread for the buffered-timeline bar.
            bufferFence = new Fence(e.plan);
            planOrderSeconds = e.plan.orderSeconds ?? [];
            const skel = await getSkeleton(hit.rootCid);
            if (epoch !== playEpoch) return;
            await player.loadStream(skel, e.plan);
            await player.play(); // fence holds at the opening until samples land
            markInited();
            break;
          }
          case "sample": {
            const pcm = await getSample(hit.rootCid, e.index);
            await inited; // never provide a sample before the instance exists
            if (epoch !== playEpoch) return; // a newer track owns the worklet now
            player.provideSample(e.index, e.frames, pcm);
            bufferFence?.provide(e.index);
            recomputeBuffered(); // grow the buffered-timeline bar as samples land
            got++;
            // Monotonic: the synchronous `complete` handler can run (and set 100)
            // before these async sample handlers resume, so guard against pct
            // regressing 100 -> low -> 100 (a visible flicker on slower paths).
            if (total > 0)
              nowPlaying.pct = Math.max(nowPlaying.pct, Math.round((100 * got) / total));
            // Log first/last + every 25% so we can see pct vs. buffering coherence.
            if (got === 1 || got === total || nowPlaying.pct % 25 === 0)
              dbg("ev.sample", { index: e.index, got, total, pct: nowPlaying.pct });
            break;
          }
          case "complete":
            dbg("ev.complete", {
              got,
              total,
              pctWas: nowPlaying.pct,
              bufferingStuck: nowPlaying.buffering,
            });
            nowPlaying.streaming = false;
            nowPlaying.pct = 100;
            break;
          case "error":
            dbg("ev.error", { message: e.message });
            nowPlaying.error = e.message;
            break;
        }
      } catch (err) {
        dbg("ev.exception", { err: String(err) });
        if (epoch === playEpoch) nowPlaying.error = String(err);
      }
    });
  } catch (e) {
    dbg("playModule.catch", { err: String(e) });
    nowPlaying.error = String(e);
    nowPlaying.streaming = false;
    nowPlaying.buffering = false;
  }
}
