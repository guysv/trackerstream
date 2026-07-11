// App-wide playback: a singleton ModPlayer + the progressive-streaming flow,
// shared by the browse list (to start playback) and the now-playing bar (to show
// + control it). Module bytes stream from CID blocks over libp2p (the embedded
// rust-ipfs node); the control plane only hands us a root CID.
import { BOOTSTRAP_MULTIADDRS } from "@trackerstream/config";
import { ModPlayer } from "./audio/ModPlayer.svelte";
import { Fence } from "./audio/fence";
import { connectPeer, warmRoot, startStream, getSkeleton, getSample, setPlayhead } from "./p2p";
import { dbg, logWarn } from "./debug";
import { getModuleByMd5, type ModuleHit, type ModuleDetail } from "./catalog";

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

// Lean projection: the queue stores ModuleHits, so a re-resolved ModuleDetail is trimmed
// back to the hit shape (keeps localStorage small — no instruments/comment blobs).
const toHit = (d: ModuleDetail): ModuleHit => ({
  id: d.id,
  md5: d.md5,
  filename: d.filename,
  format: d.format,
  title: d.title,
  duration: d.duration,
  channels: d.channels,
  rootCid: d.rootCid,
});

// A restored queue holds each track's rootCid from whenever it was queued; a corpus
// rebake changes those CIDs, so replaying by the stored CID would fetch orphaned blocks
// (exactly the bug playlists had). Re-resolve each item by its stable md5 to the current
// catalog rootCid. Fresh items added this session already carry a current CID, so this
// only rewrites the restored set — and only slots whose CID actually moved.
//
// Safety: matches by md5 against the LIVE queue at apply time (survives concurrent edits),
// updates in place (never changes length/order, so queue.index stays valid), and NEVER
// drops on a failed lookup — a lookup that rejects (node/catalog still warming) keeps the
// stored item and schedules a retry; a genuinely-removed module (resolves to null) is left
// as-is rather than silently vanishing from the user's queue.
export async function refreshQueueRoots(attempt = 0): Promise<void> {
  const targets = queue.items.filter((it) => it.md5);
  if (!targets.length) return;
  let transient = false;
  let lastErr: unknown;
  const fresh = new Map<string, ModuleHit>();
  await Promise.all(
    targets.map(async (it) => {
      try {
        const d = (await getModuleByMd5(it.md5)) as ModuleDetail | null;
        if (d?.rootCid) fresh.set(it.md5, toHit(d));
      } catch (e) {
        transient = true; // node/catalog not ready yet — keep the stored item, retry below
        lastErr = e;
      }
    }),
  );
  let changed = false;
  for (let i = 0; i < queue.items.length; i++) {
    const cur = queue.items[i];
    const d = cur.md5 ? fresh.get(cur.md5) : undefined;
    if (d && d.rootCid !== cur.rootCid) {
      queue.items[i] = d;
      changed = true;
    }
  }
  if (changed) saveQueue();
  // Converge as the node warms without needing a manual replay (bounded so we don't spin).
  if (transient && attempt < 5) setTimeout(() => void refreshQueueRoots(attempt + 1), 3000);
  else if (transient) logWarn("queue:refreshRoots", lastErr); // gave up after the retry budget
}

// Gapless auto-advance: when a track ends, play the next queued one.
player.onEnded = () => playNext();

// Fires when the queue's backing list is replaced or cleared — a change of play
// source. next/prev pass queue.items back in, so they don't fire. playlists.svelte.ts
// uses this to release its play-time pin.
let onQueueSourceChange: (() => void) | undefined;
export function setOnQueueSourceChange(cb: () => void): void {
  onQueueSourceChange = cb;
}

export function playList(items: ModuleHit[], index: number): void {
  if (items !== queue.items) onQueueSourceChange?.();
  queue.items = items;
  queue.index = index;
  saveQueue();
  const hit = items[index];
  if (hit) {
    void warmRoot(hit.rootCid); // pre-connect holders for the track we're about to play
    void playModule(hit);
  }
}

export function clearQueue(): void {
  onQueueSourceChange?.();
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
  // 5s+ into the track: rewind to the start rather than skipping to the previous
  // queue entry — matches the standard media-player back-button convention.
  if ((player.pos?.seconds ?? 0) >= 5) {
    player.seekSeconds(0);
    return;
  }
  if (queue.index > 0) playList(queue.items, queue.index - 1);
}

export function enqueue(hit: ModuleHit, next = false): void {
  if (next && queue.index >= 0) queue.items.splice(queue.index + 1, 0, hit);
  else queue.items.push(hit);
  saveQueue();
  // Queue-driven pre-connection (PEER-ASSIST.md §2.4): warm holders for this root
  // the moment it enters the queue, before playback reaches it. playList (the
  // empty-queue auto-play below) warms the playing item itself.
  if (queue.index < 0) playList(queue.items, 0);
  else void warmRoot(hit.rootCid);
}

let connected = false;
async function ensureConnected() {
  if (connected) return;
  // Try every bootstrap addr (literal IPs first, then /dns*) until one connects —
  // a failed dial (e.g. an unsupported /dns* transport) just falls through.
  let lastErr: unknown;
  for (const addr of BOOTSTRAP_MULTIADDRS) {
    try {
      await connectPeer(addr);
      connected = true;
      return;
    } catch (e) {
      lastErr = e; // try the next addr
    }
  }
  // None connected; will retry on next play (master may be momentarily unreachable). Log the
  // last dial error so a total bootstrap failure is visible rather than a silent no-op.
  logWarn("ensureConnected", lastErr, { addrs: BOOTSTRAP_MULTIADDRS.length });
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
