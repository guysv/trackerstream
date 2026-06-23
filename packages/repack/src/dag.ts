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
import { sampleRegions, type Format } from "./parse.ts";

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

export interface Manifest {
  v: number;
  format: string; // parser Format, or a bare extension for flat DAGs
  originalLength: number;
  cdc: CdcConfig;
  skeletonChunks: CID[];
  samples: SampleEntry[];
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
