// Module streaming, browser-side — the TS counterpart of ipfs.rs::stream_v2.
//
// The split the whole player depends on: `startStream` emits METADATA only (a skeleton event, then
// one event per sample as it lands); the actual bytes are pulled by getSkeleton/getSample. That is
// exactly the Tauri Channel's contract, which is why the worklet, ModPlayer and the fence work here
// unchanged — they never knew where the bytes came from.
import {
  applyEnc,
  assembleSkeletonV2,
  decodeV4Sample,
  reassemble,
  type PlanV2,
  type SampleV4,
} from "@trackerstream/repack/dag";
import { Fence } from "@trackerstream/ui/audio/fence.ts";
import type { StreamEvent } from "@trackerstream/ui/client";
import * as dagCbor from "@ipld/dag-cbor";
import { CID } from "multiformats/cid";
import { sha256 } from "multiformats/hashes/sha2";

export type BlockGetter = (cid: CID) => Promise<Uint8Array>;

/** Re-hash every block against its CID. The in-browser trust boundary on reassembly: a peer that
 *  serves us corrupt bytes must not be able to make us play them. */
async function fetchVerified(cid: CID, get: BlockGetter): Promise<Uint8Array> {
  const bytes = await get(cid);
  const digest = await sha256.digest(bytes);
  const expect = CID.createV1(cid.code, digest);
  if (!expect.equals(cid)) throw new Error(`block ${cid} failed CID verification`);
  return bytes;
}

const concat = (parts: Uint8Array[]): Uint8Array => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let w = 0;
  for (const p of parts) {
    out.set(p, w);
    w += p.length;
  }
  return out;
};

interface Streamed {
  skeleton: Uint8Array;
  samples: Map<number, Uint8Array>; // slot -> decoded PCM
}

/** One in-flight module. Kept per root so getSkeleton/getSample can answer after the events fire. */
const active = new Map<string, Streamed>();

export function getSkeleton(root: string): Uint8Array {
  const s = active.get(root);
  if (!s) throw new Error(`stream: no skeleton for ${root}`);
  return s.skeleton;
}

export function getSample(root: string, index: number): Uint8Array {
  const pcm = active.get(root)?.samples.get(index);
  if (!pcm) throw new Error(`stream: sample ${index} of ${root} not ready`);
  return pcm;
}

/** Decode one sample's leaves into playable PCM. v4 leaves may be FLAC; v2/v3 carry an enc bitmask
 *  (sign/delta/deinterleave) that must be undone in lockstep with the bake. */
async function decodeSample(s: SampleV4, get: BlockGetter): Promise<Uint8Array> {
  const leaves = await Promise.all(s.chunks.map((c) => fetchVerified(c, get)));
  const raw = concat(leaves);
  if (s.encCodec !== undefined && s.encCodec !== 0) return decodeV4Sample(raw, s);
  const enc = (s as unknown as { enc?: number }).enc ?? 0;
  return enc ? applyEnc(enc, raw, s.bitDepth, s.channels) : raw;
}

/**
 * Begin streaming `root`. Mirrors stream_v2's ordering, which is not incidental:
 *
 *   1. skeleton first (the immortal openmpt instance can't init without it),
 *   2. then every sample the fence needs at order 0 — in ONE batch (the "H1" prefetch). This is the
 *      lever that lets streaming beat full-load: a single want-list instead of N+1 round-trips.
 *   3. only then the rest, so the fence opens as early as physically possible.
 */
export async function startStream(
  root: string,
  get: BlockGetter,
  onEvent: (e: StreamEvent) => void,
): Promise<void> {
  const rootCid = CID.parse(root);
  const manifest = dagCbor.decode<Record<string, any>>(await fetchVerified(rootCid, get));
  const v = Number(manifest.v ?? 1);

  // v0/v1 have no streaming plan. The Rust does the same thing: reassemble the whole file and hand
  // it over as a "full-load skeleton" with an empty plan, so the player path stays identical.
  if (v < 2) {
    const { bytes } = await reassemble(rootCid, get, { verify: true });
    active.set(root, { skeleton: bytes, samples: new Map() });
    onEvent({ type: "skeleton", plan: { orderSeconds: [], checkpoints: [] }, samples: 0 });
    onEvent({ type: "complete" });
    return;
  }
  if (v > 4) throw new Error(`stream: manifest v${v} is newer than this client understands`);

  const index = manifest.index ?? dagCbor.decode<any>(await fetchVerified(manifest.indexRoot, get));
  const samples: SampleV4[] = index.samples;
  const plan: PlanV2 = index.plan;

  // Skeleton: fetch the content chunks and re-inflate the zero runs from the layout recipe. The
  // zeros — the bulk of a compressed IT's skeleton — were never transferred.
  const parts = await Promise.all(manifest.skeletonChunks.map((c: CID) => fetchVerified(c, get)));
  const skeleton = assembleSkeletonV2(parts, manifest.skeletonLayout);
  const state: Streamed = { skeleton, samples: new Map() };
  active.set(root, state);

  onEvent({ type: "skeleton", plan, samples: samples.length });

  // Which slots must be resident before the fence will let order 0 play.
  const fence = new Fence(plan);
  const requiredAt0 = new Set(fence.requiredAt(0));
  const bySlot = new Map(samples.map((s) => [s.index, s]));

  const emit = async (s: SampleV4): Promise<void> => {
    const pcm = await decodeSample(s, get);
    state.samples.set(s.index, pcm);
    onEvent({ type: "sample", index: s.index, frames: s.frames });
  };

  try {
    // H1: everything the fence needs at order 0, concurrently, before anything else.
    await Promise.all([...requiredAt0].map((slot) => bySlot.get(slot)).filter(Boolean).map((s) => emit(s!)));
    // Then the remainder. Playback has already started by now.
    await Promise.all(samples.filter((s) => !requiredAt0.has(s.index)).map(emit));
    onEvent({ type: "complete" });
  } catch (e) {
    onEvent({ type: "error", message: e instanceof Error ? e.message : String(e) });
  }
}
