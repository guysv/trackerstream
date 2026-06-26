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
  skeletonChunks: CID[]; // structure content chunks of the normalized skeleton (zeros excluded)
  skeletonLayout: number[]; // run-length recipe [nContentChunks, zeroBytes, ...]; zeros synthesized client-side
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
 * loadable module — create_from_memory yields silent, all-pending samples).
 *
 * Returns the skeleton bytes AND the byte ranges that are now all-zero (orphaned
 * compressed regions, zeroed uncompressed regions, and the appended zero-fill
 * tail). The caller splits the skeleton at those zero-span edges into structure
 * segments (transferred as content chunks) and zero runs (encoded as length
 * directives in `skeletonLayout`, synthesized client-side, never transferred).
 * Otherwise FastCDC's 8 KB-min chunks straddle each hole and per-hole remainders
 * are unique-length zero blocks — bloating the skeleton past the original file
 * (the dominant TTFP loss for small compressed ITs). */
function buildSkeletonV2(
  data: Uint8Array,
  streamed: {
    offset: number;
    length: number;
    compressed?: boolean;
    headerOffset?: number;
    compressedBytes?: number;
  }[],
): { skeleton: Uint8Array; zeroSpans: Array<[number, number]> } {
  // Uncompressed slots: zero their on-disk region in place (the header already
  // describes an uncompressed buffer of exactly `length`). Compressed slots: the
  // on-disk bytes are compressed and shorter than the decoded PCM, so we can't zero
  // in place — append an uncompressed zero-fill region of the decoded `length` at
  // EOF, repoint the sample's data pointer (IT header +0x48) there, and clear the IT
  // compression flag (+0x12 bit 0x08). create_from_memory then allocates a decoded-
  // length SILENT buffer that provide_sample overwrites bit-exactly. The orphaned
  // compressed region is ZEROED (when its span is known) so it dedups to a zero block
  // instead of transferring as dead weight — otherwise the skeleton would carry both
  // the compressed source AND the streamed PCM.
  const raw = streamed.filter((s) => !s.compressed);
  const comp = streamed.filter((s) => s.compressed);
  const extra = comp.reduce((n, s) => n + s.length, 0);
  const sk = new Uint8Array(data.length + extra); // tail is zero-initialized
  sk.set(data);
  const zeroSpans: Array<[number, number]> = [];
  for (const s of raw) {
    sk.fill(0, s.offset, s.offset + s.length);
    zeroSpans.push([s.offset, s.offset + s.length]);
  }
  let cursor = data.length;
  for (const s of comp) {
    const ho = s.headerOffset!;
    if (s.compressedBytes) {
      sk.fill(0, s.offset, s.offset + s.compressedBytes); // orphan -> zeros
      zeroSpans.push([s.offset, s.offset + s.compressedBytes]);
    }
    sk[ho + 0x12] &= ~0x08; // clear "compressed" flag -> raw PCM
    sk[ho + 0x48] = cursor & 0xff; // repoint data pointer (u32le) to the appended zero region
    sk[ho + 0x49] = (cursor >>> 8) & 0xff;
    sk[ho + 0x4a] = (cursor >>> 16) & 0xff;
    sk[ho + 0x4b] = (cursor >>> 24) & 0xff;
    cursor += s.length; // region already zero from the fresh allocation
  }
  if (extra > 0) zeroSpans.push([data.length, sk.length]); // appended zero-fill tail
  return { skeleton: sk, zeroSpans };
}

/** Sort + merge zero spans into disjoint, ascending [start,end) ranges clamped to
 * [0,len], coalescing overlapping/adjacent ones. The skeleton recipe walks these
 * to split structure (content chunks) from zero runs (length directives). */
function mergeSpans(spans: Array<[number, number]>, len: number): Array<[number, number]> {
  const norm = spans
    .map(([a, b]) => [Math.max(0, Math.min(a, len)), Math.max(0, Math.min(b, len))] as [number, number])
    .filter(([a, b]) => b > a)
    .sort((x, y) => x[0] - y[0]);
  const out: Array<[number, number]> = [];
  for (const [a, b] of norm) {
    const last = out[out.length - 1];
    if (last && a <= last[1]) last[1] = Math.max(last[1], b);
    else out.push([a, b]);
  }
  return out;
}

/** Slot indices that must be resident to start playback at order 0 — the floor
 * checkpoint at order 0 UNION the next one (mirrors PlanV2::required_at(0) in the
 * client/fence). Used by the bake's compressed-streaming gate to size the warm set. */
function requiredAtZero(plan: PlanV2): Set<number> {
  const cps = [...plan.checkpoints].sort((a, b) => a.order - b.order);
  if (!cps.length) return new Set();
  let floor = -1;
  for (let i = 0; i < cps.length; i++) if (cps[i].order <= 0) floor = i;
  const lo = Math.max(floor, 0);
  const hi = Math.min(Math.max(floor + 1, 0), cps.length - 1);
  const set = new Set<number>();
  for (let k = lo; k <= hi; k++) for (const s of cps[k].samples) set.add(s);
  return set;
}

/**
 * Build the v2 DAG for a parsed module. `decoded` is the per-slot native-layout
 * PCM dumped from libopenmpt by the caller (debug_sample_* accessors). A slot is
 * STREAMED only if we can locate its uncompressed on-disk region AND its length
 * matches the decode; otherwise its real PCM rides in the skeleton (always
 * resident, dropped from the plan) — never wrong audio, just less dedup.
 * Throws if the format can't be parsed (caller falls back to v1 flat DAG).
 */
/** Minimum first-playable byte saving (full-load − streaming warm set) for which we
 * stream a module's COMPRESSED samples. Below it the module is small enough that
 * full-load TTFP already sits at the cold round-trip floor (~0.5 s) and streaming's
 * per-sample coordination loses; measured crossover is ~50–120 KB, so 128 KB is a
 * safe no-regression gate. Uncompressed-sample streaming is unaffected (it carves
 * raw regions with no decoded-PCM inflation, and the zero-fill skeleton only shrinks
 * the fetch). See STREAMING-COMPRESSED-IT-RESEARCH.md. */
const COMPRESSED_STREAM_MIN_SAVING = 128 * 1024;

export async function buildDagV2(
  data: Uint8Array,
  decoded: DecodedSample[],
  cdc: CdcConfig = DEFAULT_CDC,
  opts: { noCompressedStream?: boolean } = {},
): Promise<BuiltDagV2> {
  const parsed = sampleSlots(data);
  if (!parsed) throw new Error("unparseable module (cannot locate sample slots)");
  const { format, slots } = parsed;

  const dumpByIndex = new Map<number, DecodedSample>();
  for (const d of decoded) dumpByIndex.set(d.index, d);

  // Candidate streamed slots: a locatable decoded region whose length matches the
  // dump. (Compressed slots carry the decoded byte length + a skeleton-synth tag.)
  const candidates: { slot: (typeof slots)[number]; dump: DecodedSample }[] = [];
  for (const slot of slots) {
    const dump = dumpByIndex.get(slot.index);
    if (!dump || dump.data.length === 0) continue; // no PCM dumped -> rides in skeleton
    if (dump.data.length !== slot.length) continue; // length disagree -> be safe, don't stream
    candidates.push({ slot, dump });
  }

  // Plan FIRST: we may only stream what the fence can gate. computeSeekTables works
  // in PCM byte offsets; map those to candidate slot indices via the on-disk offset.
  const idxByOffset = new Map<number, number>();
  for (const { slot } of candidates) idxByOffset.set(slot.offset, slot.index);
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

  // A COMPRESSED slot is streamed only if the plan covers it: its decoded PCM rides
  // in a synthesized (uncompressed, zero-filled) skeleton region, so if the fence
  // can't gate it (absent from every checkpoint) it could sound before it arrives —
  // demote to resident (leave the original compressed bytes in the skeleton, exactly
  // as before). Uncompressed slots are unaffected, so non-compressed modules bake to
  // byte-identical roots (no needless re-bake).
  const covered = new Set<number>();
  for (const c of plan.checkpoints) for (const s of c.samples) covered.add(s);
  const streamed = candidates.filter(
    (c) =>
      !c.slot.compressed ||
      (!opts.noCompressedStream && covered.has(c.slot.index)),
  );

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

  // Normalized skeleton (streamed regions zeroed / compressed regions synthesized).
  const { skeleton, zeroSpans } = buildSkeletonV2(
    data,
    streamed.map((s) => s.slot),
  );
  // Zero-fill recipe: the skeleton is structure interspersed with large all-zero
  // regions (orphaned compressed bytes, zeroed PCM, the appended decoded-length
  // tail). Transferring those zeros dominates cold TTFP for compressed ITs — and
  // they DON'T dedup (FastCDC straddles each hole, and per-hole remainders are
  // unique-length zero blocks). So we don't transfer them at all: only the
  // structure segments become content chunks (`skeletonChunks`); the zero runs
  // become length directives in `skeletonLayout` that the client synthesizes.
  // Layout is a flat run-length recipe [nContentChunks, zeroBytes, ...]; the
  // skeleton bytes are unchanged, so parity is bit-exact.
  const skeletonChunks: CID[] = [];
  const skeletonLayout: number[] = [];
  {
    const spans = mergeSpans(zeroSpans, skeleton.length);
    let pos = 0;
    for (const [a, b] of spans) {
      let nc = 0;
      if (a > pos) for (const cid of await chunkToCids(skeleton.subarray(pos, a))) (skeletonChunks.push(cid), nc++);
      skeletonLayout.push(nc, b - a);
      pos = b;
    }
    if (pos < skeleton.length) {
      let nc = 0;
      for (const cid of await chunkToCids(skeleton.subarray(pos))) (skeletonChunks.push(cid), nc++);
      skeletonLayout.push(nc, 0);
    }
  }

  // No-regression gate for COMPRESSED streaming: a compressed sample streams its
  // (larger) decoded PCM, so if the module is too small the streamed warm set
  // doesn't undercut full-load by enough to beat the round-trip floor. Measure the
  // first-playable saving (full-load bytes − streaming warm: structure chunks ∪
  // required-at-0 sample chunks, deduped) and, if a compressed slot was streamed
  // but the saving is below the gate, re-bake with compressed demoted to resident
  // (full-load for those samples — exactly the prior behavior, never worse).
  if (!opts.noCompressedStream && streamed.some((s) => s.slot.compressed)) {
    const warm = new Set<string>(skeletonChunks.map((c) => c.toString()));
    const req = requiredAtZero(plan);
    const byIndex = new Map(samples.map((s) => [s.index, s]));
    for (const ix of req)
      for (const c of byIndex.get(ix)?.chunks ?? []) warm.add(c.toString());
    let warmBytes = 0;
    for (const k of warm) warmBytes += byCid.get(k)?.bytes.length ?? 0;
    if (data.length - warmBytes < COMPRESSED_STREAM_MIN_SAVING)
      return buildDagV2(data, decoded, cdc, { ...opts, noCompressedStream: true });
  }

  // Inline the index; spill to its own block if the manifest gets too big.
  const index: IndexV2 = { samples, plan };
  let manifest: ManifestV2 = { v: MANIFEST_V2, format, cdc, skeletonChunks, skeletonLayout, index };
  let manifestBlock = await cborBlock(manifest);
  let spilled = false;
  if (manifestBlock.bytes.length > INDEX_SPILL_BYTES) {
    const indexBlock = await cborBlock(index);
    add(indexBlock);
    manifest = { v: MANIFEST_V2, format, cdc, skeletonChunks, skeletonLayout, indexRoot: indexBlock.cid };
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

/** Reconstruct the normalized skeleton from its structure content chunks + the
 * zero-fill layout recipe `[nContentChunks, zeroBytes, ...]`. A fresh Uint8Array
 * is zero-initialized, so zero runs are free (no transfer, no copy). Falls back to
 * a plain concat when no layout is present (older manifests). */
export function assembleSkeletonV2(parts: Uint8Array[], layout?: number[]): Uint8Array {
  if (!layout || layout.length === 0) {
    const total = parts.reduce((n, p) => n + p.length, 0);
    const out = new Uint8Array(total);
    let w = 0;
    for (const p of parts) (out.set(p, w), (w += p.length));
    return out;
  }
  let total = 0;
  let ci = 0;
  for (let i = 0; i < layout.length; i += 2) {
    const nc = layout[i] ?? 0;
    const z = layout[i + 1] ?? 0;
    for (let k = 0; k < nc; k++) total += parts[ci++]?.length ?? 0;
    total += z;
  }
  const out = new Uint8Array(total);
  let w = 0;
  ci = 0;
  for (let i = 0; i < layout.length; i += 2) {
    const nc = layout[i] ?? 0;
    const z = layout[i + 1] ?? 0;
    for (let k = 0; k < nc; k++) {
      const p = parts[ci++];
      out.set(p, w);
      w += p.length;
    }
    w += z; // zero run — already zero from allocation
  }
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

  // Skeleton: fetch the structure content chunks, then reconstruct by interleaving
  // synthesized zero runs per the layout recipe (zeros were never transferred).
  const parts: Uint8Array[] = [];
  for (const cid of manifest.skeletonChunks) parts.push(await fetchVerified(cid, get, verify));
  const skeleton = assembleSkeletonV2(parts, manifest.skeletonLayout);

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
