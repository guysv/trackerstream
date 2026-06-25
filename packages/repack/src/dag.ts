// Module <-> content-addressed CID-DAG (the lab/CID.md design).
//
//   manifest (dag-cbor, root, tiny)
//     format, originalLength, cdc params
//     skeletonChunks: [CID]     — CDC chunks of everything that is NOT sample PCM
//                                 (headers/orders/patterns); module-unique
//     samples: [{ offset, length, pcmRoot: CID }]
//   pcm-root (dag-cbor, per sample): { chunks: [CID], length }
//   chunk (raw leaf): sample-PCM or skeleton bytes  — SHARED across modules
//
// Identical sample chunks share a CID and are stored/served once (measured
// ~41% sub-sample dedup corpus-wide). Reassembly rebuilds the EXACT original
// bytes, so playback is bit-identical (verified in test/roundtrip.ts).

import { CID } from "multiformats/cid";
import { sha256 } from "multiformats/hashes/sha2";
import * as dagCbor from "@ipld/dag-cbor";
import { cdcChunks, DEFAULT_CDC, type CdcConfig } from "./cdc.ts";
import { sampleRegions, sampleSlots, type Format } from "./parse.ts";
import { computeSeekTables } from "./seek.ts";

const RAW_CODE = 0x55;
const DAG_CBOR_CODE = 0x71;

export interface Block {
  cid: CID;
  bytes: Uint8Array;
}

export interface SampleEntry {
  offset: number;
  length: number;
  pcmRoot: CID;
}

/** Seek-support tables (MVP-FOLLOWUP B1/B2/B3); sample refs are indices into
 * `samples[]`. Optional — absent for flat DAGs and non-IT-family modules. */
export interface SeekTable {
  /** Cumulative seconds at the start of each order entry (the T<->order map). */
  orderSeconds: { order: number; seconds: number }[];
  /** Per-order resident sample sets for cold seeks (indices into samples[]). */
  checkpoints: { order: number; samples: number[] }[];
}

export interface Manifest {
  v: number;
  format: string; // parser Format, or a bare extension for flat DAGs
  originalLength: number;
  cdc: CdcConfig;
  skeletonChunks: CID[];
  samples: SampleEntry[];
  /** Sample indices the first played pattern triggers — fetch first (B2). */
  segment0?: number[];
  /** Seek/timing tables for cold-seek resident-set fetch (B1/B3). */
  seek?: SeekTable;
}

export interface BuiltDag {
  root: CID;
  manifest: Manifest;
  /** Unique blocks (deduped by CID within this module). */
  blocks: Block[];
  stats: {
    originalLength: number;
    skeletonBytes: number;
    sampleBytes: number;
    numSamples: number;
    numChunks: number;
    uniqueChunks: number;
    manifestBytes: number;
  };
}

async function rawBlock(bytes: Uint8Array): Promise<Block> {
  const digest = await sha256.digest(bytes);
  return { cid: CID.createV1(RAW_CODE, digest), bytes };
}

async function cborBlock(value: unknown): Promise<Block> {
  const bytes = dagCbor.encode(value);
  const digest = await sha256.digest(bytes);
  return { cid: CID.createV1(DAG_CBOR_CODE, digest), bytes };
}

/** Concatenate the non-sample byte runs (the "skeleton") in file order. */
function buildSkeleton(
  data: Uint8Array,
  regions: { offset: number; length: number }[],
): Uint8Array {
  let skelLen = data.length;
  for (const r of regions) skelLen -= r.length;
  const skel = new Uint8Array(skelLen);
  let w = 0;
  let prevEnd = 0;
  for (const r of regions) {
    const gap = data.subarray(prevEnd, r.offset);
    skel.set(gap, w);
    w += gap.length;
    prevEnd = r.offset + r.length;
  }
  const tail = data.subarray(prevEnd, data.length);
  skel.set(tail, w);
  return skel;
}

/** Build the content-addressed DAG for a module buffer (sample-level dedup). */
export async function buildDag(data: Uint8Array, cdc: CdcConfig = DEFAULT_CDC): Promise<BuiltDag> {
  const parsed = sampleRegions(data);
  if (!parsed) throw new Error("unparseable module (cannot locate sample regions)");
  return buildFromRegions(data, parsed.format, parsed.regions, cdc);
}

/**
 * Whole-file CDC DAG (no sample separation) — the fallback for formats the
 * parsers don't cover yet (e.g. mptm/mo3). Still fetchable + reassembled
 * byte-exact, just without cross-module sample dedup.
 */
export async function buildFlatDag(
  data: Uint8Array,
  format: string,
  cdc: CdcConfig = DEFAULT_CDC,
): Promise<BuiltDag> {
  return buildFromRegions(data, format, [], cdc);
}

async function buildFromRegions(
  data: Uint8Array,
  format: string,
  regions: { offset: number; length: number }[],
  cdc: CdcConfig,
): Promise<BuiltDag> {
  const byCid = new Map<string, Block>();
  const add = (b: Block) => {
    const k = b.cid.toString();
    if (!byCid.has(k)) byCid.set(k, b);
    return b.cid;
  };

  let numChunks = 0;

  // CDC-chunk a buffer into raw leaves, returning the ordered chunk CIDs.
  const chunkToCids = async (buf: Uint8Array): Promise<CID[]> => {
    const cids: CID[] = [];
    for (const c of cdcChunks(buf, cdc)) {
      numChunks++;
      // c is a subarray view; copy so the stored block owns its bytes.
      const b = await rawBlock(c.slice());
      cids.push(add(b));
    }
    return cids;
  };

  // Per-sample pcm-roots.
  const samples: SampleEntry[] = [];
  let sampleBytes = 0;
  for (const r of regions) {
    const pcm = data.subarray(r.offset, r.offset + r.length);
    sampleBytes += r.length;
    const chunks = await chunkToCids(pcm);
    const pcmRoot = add(await cborBlock({ chunks, length: r.length }));
    samples.push({ offset: r.offset, length: r.length, pcmRoot });
  }

  // Skeleton (everything that is not sample PCM).
  const skeleton = buildSkeleton(data, regions);
  const skeletonChunks = await chunkToCids(skeleton);

  const manifest: Manifest = {
    v: 1,
    format,
    originalLength: data.length,
    cdc,
    skeletonChunks,
    samples,
  };

  // Bake seek-support tables (B1/B2/B3) for IT-family modules. The tables key
  // samples by PCM byte offset; map those to indices into samples[] (an
  // offset-sorted subset — compressed/absent samples don't appear and are
  // dropped here). Purely additive: a consumer that ignores these still streams
  // bit-exactly, so this never affects reassembly.
  if (regions.length) {
    const tables = computeSeekTables(data, format as Format);
    if (tables) {
      const idxByOffset = new Map<number, number>();
      samples.forEach((s, i) => idxByOffset.set(s.offset, i));
      const toIdx = (offs: number[]): number[] => {
        const out: number[] = [];
        for (const o of offs) {
          const i = idxByOffset.get(o);
          if (i !== undefined) out.push(i);
        }
        return out;
      };
      const segment0 = toIdx(tables.segment0Offsets);
      if (segment0.length) manifest.segment0 = segment0;
      const checkpoints = tables.checkpoints
        .map((c) => ({ order: c.order, samples: toIdx(c.residentOffsets) }))
        .filter((c) => c.samples.length);
      if (checkpoints.length) {
        manifest.seek = { orderSeconds: tables.orderSeconds, checkpoints };
      }
    }
  }

  const manifestBlock = await cborBlock(manifest);
  add(manifestBlock);

  return {
    root: manifestBlock.cid,
    manifest,
    blocks: [...byCid.values()],
    stats: {
      originalLength: data.length,
      skeletonBytes: skeleton.length,
      sampleBytes,
      numSamples: regions.length,
      numChunks,
      uniqueChunks: byCid.size - samples.length - 1, // minus pcm-roots + manifest
      manifestBytes: manifestBlock.bytes.length,
    },
  };
}

// ===========================================================================
// Repack v2 — the immortal-instance object tree (STREAMING-PARITY-V2-SCHEMA.md)
//
// v2 stops reconstructing the original file. It feeds DECODED PCM by libopenmpt
// sample slot into one immortal instance via provide_sample, so the tree is
// organized for planning + seeking, not byte-exact reassembly. The single key
// everywhere is the 1-based libopenmpt sample slot.
// ===========================================================================

export const MANIFEST_V2 = 2;
/** Above this encoded-manifest size the index spills to its own block. */
export const INDEX_SPILL_BYTES = 256 * 1024;

/** One streamed sample: decoded native-layout PCM, keyed by libopenmpt slot. */
export interface SampleV2 {
  index: number; // 1-based libopenmpt slot (== provide_sample arg)
  frames: number; // decoded nLength (== provide_sample `frames` arg)
  channels: number; // native interleave: 1 | 2
  bitDepth: number; // native: 8 | 16  (byteLength = frames*channels*bitDepth/8)
  chunks: CID[]; // ordered raw leaves of this sample's decoded PCM
}

/** Planning index — drives both the prefetch scheduler and the client fence. */
export interface PlanV2 {
  /** Cumulative seconds at the start of each valid order (T<->order seek map). */
  orderSeconds: { order: number; seconds: number }[];
  /** Full resident set (slot indices) per strided order — NOT delta-encoded. */
  checkpoints: { order: number; samples: number[] }[];
}

export interface IndexV2 {
  samples: SampleV2[];
  plan: PlanV2;
}

export interface ManifestV2 {
  v: number; // 2
  format: Format;
  cdc: CdcConfig;
  skeletonChunks: CID[]; // normalized skeleton: streamed slots' PCM zeroed
  index?: IndexV2; // inline (default)
  indexRoot?: CID; // OR spilled to its own block (large modules)
}

/** Decoded native-layout PCM for one sample slot (the bake's libopenmpt dump). */
export interface DecodedSample {
  index: number; // 1-based slot (from the get_num_samples loop)
  frames: number; // debug_sample_frames
  data: Uint8Array; // debug_sample_data bytes (length == debug_sample_bytes)
}

export interface BuiltDagV2 {
  root: CID;
  manifest: ManifestV2;
  blocks: Block[];
  stats: {
    format: Format;
    skeletonBytes: number;
    streamedSamples: number;
    residentSamples: number; // rode in the skeleton (compressed/unlocatable)
    sampleBytes: number;
    numChunks: number;
    uniqueChunks: number;
    manifestBytes: number;
    spilled: boolean;
  };
}

/** Copy of `data` with each streamed slot's on-disk PCM zeroed (still a valid,
 * loadable module — create_from_memory yields silent, all-pending samples). */
function buildSkeletonV2(data: Uint8Array, streamed: { offset: number; length: number }[]): Uint8Array {
  const sk = new Uint8Array(data); // full-size copy
  for (const s of streamed) sk.fill(0, s.offset, s.offset + s.length);
  return sk;
}

/**
 * Build the v2 DAG for a parsed module. `decoded` is the per-slot native-layout
 * PCM dumped from libopenmpt by the caller (debug_sample_* accessors). A slot is
 * STREAMED only if we can locate its uncompressed on-disk region AND its length
 * matches the decode; otherwise its real PCM rides in the skeleton (always
 * resident, dropped from the plan) — never wrong audio, just less dedup.
 * Throws if the format can't be parsed (caller falls back to v1 flat DAG).
 */
export async function buildDagV2(
  data: Uint8Array,
  decoded: DecodedSample[],
  cdc: CdcConfig = DEFAULT_CDC,
): Promise<BuiltDagV2> {
  const parsed = sampleSlots(data);
  if (!parsed) throw new Error("unparseable module (cannot locate sample slots)");
  const { format, slots } = parsed;

  const dumpByIndex = new Map<number, DecodedSample>();
  for (const d of decoded) dumpByIndex.set(d.index, d);

  // Decide streamed vs resident-in-skeleton, slot by slot.
  const streamed: { slot: (typeof slots)[number]; dump: DecodedSample }[] = [];
  for (const slot of slots) {
    const dump = dumpByIndex.get(slot.index);
    if (!dump || dump.data.length === 0) continue; // no PCM dumped -> rides in skeleton
    if (dump.data.length !== slot.length) continue; // length disagree -> be safe, don't stream
    streamed.push({ slot, dump });
  }

  const byCid = new Map<string, Block>();
  const add = (b: Block) => {
    const k = b.cid.toString();
    if (!byCid.has(k)) byCid.set(k, b);
    return b.cid;
  };
  let numChunks = 0;
  const chunkToCids = async (buf: Uint8Array): Promise<CID[]> => {
    const cids: CID[] = [];
    for (const c of cdcChunks(buf, cdc)) {
      numChunks++;
      cids.push(add(await rawBlock(c.slice())));
    }
    return cids;
  };

  // Sample table (inline leaf CIDs — the pre-resolved index).
  const samples: SampleV2[] = [];
  let sampleBytes = 0;
  for (const { slot, dump } of streamed) {
    sampleBytes += dump.data.length;
    const chunks = await chunkToCids(dump.data);
    samples.push({
      index: slot.index,
      frames: dump.frames,
      channels: slot.channels,
      bitDepth: slot.bitDepth,
      chunks,
    });
  }

  // Normalized skeleton (streamed regions zeroed) -> chunks.
  const skeleton = buildSkeletonV2(
    data,
    streamed.map((s) => s.slot),
  );
  const skeletonChunks = await chunkToCids(skeleton);

  // Planning index. computeSeekTables works in PCM byte offsets; map those to
  // the streamed slot indices (offsets that don't resolve to a streamed slot are
  // dropped — they ride in the skeleton, always resident).
  const idxByOffset = new Map<number, number>();
  for (const { slot } of streamed) idxByOffset.set(slot.offset, slot.index);
  const toSlots = (offs: number[]): number[] => {
    const out: number[] = [];
    for (const o of offs) {
      const i = idxByOffset.get(o);
      if (i !== undefined) out.push(i);
    }
    return out;
  };

  const plan: PlanV2 = { orderSeconds: [], checkpoints: [] };
  const tables = computeSeekTables(data, format);
  if (tables) {
    plan.orderSeconds = tables.orderSeconds;
    plan.checkpoints = tables.checkpoints
      .map((c) => ({ order: c.order, samples: toSlots(c.residentOffsets) }))
      .filter((c) => c.samples.length);
    // Synthesize an opening checkpoint from segment0 when the format only gives
    // segment0 (MOD/S3M/XM today; full checkpoints land with the seek extension).
    if (!plan.checkpoints.length) {
      const seg0 = toSlots(tables.segment0Offsets);
      if (seg0.length) plan.checkpoints = [{ order: 0, samples: seg0 }];
    }
  }

  // Inline the index; spill to its own block if the manifest gets too big.
  const index: IndexV2 = { samples, plan };
  let manifest: ManifestV2 = { v: MANIFEST_V2, format, cdc, skeletonChunks, index };
  let manifestBlock = await cborBlock(manifest);
  let spilled = false;
  if (manifestBlock.bytes.length > INDEX_SPILL_BYTES) {
    const indexBlock = await cborBlock(index);
    add(indexBlock);
    manifest = { v: MANIFEST_V2, format, cdc, skeletonChunks, indexRoot: indexBlock.cid };
    manifestBlock = await cborBlock(manifest);
    spilled = true;
  }
  add(manifestBlock);

  return {
    root: manifestBlock.cid,
    manifest,
    blocks: [...byCid.values()],
    stats: {
      format,
      skeletonBytes: skeleton.length,
      streamedSamples: streamed.length,
      residentSamples: slots.length - streamed.length,
      sampleBytes,
      numChunks,
      uniqueChunks: byCid.size,
      manifestBytes: manifestBlock.bytes.length,
      spilled,
    },
  };
}

export type BlockGetter = (cid: CID) => Promise<Uint8Array>;

/**
 * Enumerate every CID in a module's DAG (manifest + skeleton chunks + pcm-roots
 * + sample chunks), fetching only the two index levels (manifest, pcm-roots).
 * Used to drive concurrent prefetch (the Phase 3 fetch plan starts from here).
 */
export async function allCids(root: CID, get: BlockGetter): Promise<CID[]> {
  const manifest = dagCbor.decode<Manifest>(await get(root));
  const cids: CID[] = [root, ...manifest.skeletonChunks];
  for (const s of manifest.samples) {
    cids.push(s.pcmRoot);
    const pcmRoot = dagCbor.decode<{ chunks: CID[]; length: number }>(await get(s.pcmRoot));
    cids.push(...pcmRoot.chunks);
  }
  return cids;
}

/** Fetch many blocks concurrently (bounded pool) into a CID->bytes map. */
export async function prefetch(
  cids: CID[],
  get: BlockGetter,
  concurrency = 32,
): Promise<Map<string, Uint8Array>> {
  const out = new Map<string, Uint8Array>();
  let i = 0;
  const worker = async () => {
    while (i < cids.length) {
      const cid = cids[i++];
      const key = cid.toString();
      if (!out.has(key)) out.set(key, await get(cid));
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, cids.length) }, worker));
  return out;
}

/** A streamed sample with its decoded PCM assembled from leaf chunks. */
export interface FetchedSampleV2 {
  index: number;
  frames: number;
  channels: number;
  bitDepth: number;
  pcm: Uint8Array;
}

/**
 * Fetch a v2 root into the pieces a client needs: the skeleton bytes (for
 * create_from_memory), each streamed sample's assembled decoded PCM (for
 * provide_sample), and the plan. The reference path for the parity harness and
 * headless validation; the shipped client streams these incrementally instead.
 */
export async function fetchV2(
  root: CID,
  get: BlockGetter,
  opts: { verify?: boolean } = {},
): Promise<{ manifest: ManifestV2; skeleton: Uint8Array; samples: FetchedSampleV2[]; plan: PlanV2 }> {
  const verify = opts.verify ?? true;
  const manifest = dagCbor.decode<ManifestV2>(await fetchVerified(root, get, verify));
  if (manifest.v !== MANIFEST_V2) throw new Error(`not a v2 manifest (v=${manifest.v})`);
  const index =
    manifest.index ??
    dagCbor.decode<IndexV2>(await fetchVerified(manifest.indexRoot!, get, verify));

  // Skeleton.
  const parts: Uint8Array[] = [];
  let skelLen = 0;
  for (const cid of manifest.skeletonChunks) {
    const part = await fetchVerified(cid, get, verify);
    parts.push(part);
    skelLen += part.length;
  }
  const skeleton = new Uint8Array(skelLen);
  let w = 0;
  for (const p of parts) {
    skeleton.set(p, w);
    w += p.length;
  }

  // Per-sample PCM.
  const samples: FetchedSampleV2[] = [];
  for (const s of index.samples) {
    const chunks: Uint8Array[] = [];
    let len = 0;
    for (const cid of s.chunks) {
      const c = await fetchVerified(cid, get, verify);
      chunks.push(c);
      len += c.length;
    }
    const pcm = new Uint8Array(len);
    let o = 0;
    for (const c of chunks) {
      pcm.set(c, o);
      o += c.length;
    }
    samples.push({ index: s.index, frames: s.frames, channels: s.channels, bitDepth: s.bitDepth, pcm });
  }

  return { manifest, skeleton, samples, plan: index.plan };
}

/** Recompute the multihash of `bytes` and confirm it matches `cid`. */
async function verifyBlock(cid: CID, bytes: Uint8Array): Promise<void> {
  const digest = await sha256.digest(bytes);
  const got = digest.digest;
  const want = cid.multihash.digest;
  if (got.length !== want.length) throw new Error(`block ${cid} length mismatch`);
  for (let i = 0; i < got.length; i++)
    if (got[i] !== want[i]) throw new Error(`block ${cid} failed CID verification (tampered?)`);
}

async function fetchVerified(cid: CID, get: BlockGetter, verify: boolean): Promise<Uint8Array> {
  const bytes = await get(cid);
  if (verify) await verifyBlock(cid, bytes);
  return bytes;
}

/**
 * Reassemble the exact original module bytes from its DAG, fetching blocks via
 * `get` and verifying each against its CID (self-verifying integrity — a tampered
 * peer-served block fails here). Returns a buffer byte-identical to the original.
 */
export async function reassemble(
  root: CID,
  get: BlockGetter,
  opts: { verify?: boolean } = {},
): Promise<{ bytes: Uint8Array; manifest: Manifest }> {
  const verify = opts.verify ?? true;
  const manifestBytes = await fetchVerified(root, get, verify);
  const manifest = dagCbor.decode<Manifest>(manifestBytes);

  // Skeleton stream.
  let skelLen = 0;
  const skelParts: Uint8Array[] = [];
  for (const cid of manifest.skeletonChunks) {
    const part = await fetchVerified(cid, get, verify);
    skelParts.push(part);
    skelLen += part.length;
  }
  const skeleton = new Uint8Array(skelLen);
  {
    let w = 0;
    for (const p of skelParts) {
      skeleton.set(p, w);
      w += p.length;
    }
  }

  const out = new Uint8Array(manifest.originalLength);
  let skelCursor = 0;
  let prevEnd = 0;
  for (const s of manifest.samples) {
    const gap = s.offset - prevEnd;
    out.set(skeleton.subarray(skelCursor, skelCursor + gap), prevEnd);
    skelCursor += gap;

    const pcmRootBytes = await fetchVerified(s.pcmRoot, get, verify);
    const pcmRoot = dagCbor.decode<{ chunks: CID[]; length: number }>(pcmRootBytes);
    let w = s.offset;
    for (const cid of pcmRoot.chunks) {
      const chunk = await fetchVerified(cid, get, verify);
      out.set(chunk, w);
      w += chunk.length;
    }
    prevEnd = s.offset + s.length;
  }
  // Final skeleton tail after the last sample.
  out.set(skeleton.subarray(skelCursor), prevEnd);

  return { bytes: out, manifest };
}
