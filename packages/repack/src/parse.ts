// Per-format sample-PCM locators (ported from lab/cid-dedup.mjs). Each returns
// the byte regions of the raw sample PCM payloads in the file — NOT copies — so
// the DAG builder can carve the module into a compact skeleton (everything that
// is not sample PCM, ~1.5-5.7% of a big module) plus per-sample chunk lists. The
// payload (not the format's sample blob) is what dedupes across modules: loop
// points / C5Speed / name / flags vary per module even when the PCM is identical.

export type Format = "mod" | "it" | "s3m" | "xm" | "mptm" | "mo3";

/** A contiguous run of sample PCM bytes within the module file. */
export interface SampleRegion {
  offset: number;
  length: number;
}

const u16le = (b: Uint8Array, o: number) => b[o] | (b[o + 1] << 8);
const u16be = (b: Uint8Array, o: number) => (b[o] << 8) | b[o + 1];
const u32le = (b: Uint8Array, o: number) =>
  (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;
const tag4 = (b: Uint8Array, o: number) =>
  String.fromCharCode(b[o], b[o + 1], b[o + 2], b[o + 3]);

function modChannels(t: string): number {
  if (t === "M.K." || t === "M!K!" || t === "FLT4" || t === "4CHN") return 4;
  if (t === "6CHN") return 6;
  if (t === "FLT8" || t === "8CHN" || t === "OCTA" || t === "CD81") return 8;
  let m = /^(\d)CHN$/.exec(t);
  if (m) return +m[1];
  m = /^(\d\d)CH$/.exec(t);
  if (m) return +m[1];
  m = /^(\d\d)CN$/.exec(t);
  if (m) return +m[1];
  m = /^TDZ(\d)$/.exec(t);
  if (m) return +m[1];
  return 0;
}

function extractMOD(b: Uint8Array): SampleRegion[] | null {
  if (b.length < 1084) return null;
  const ch = modChannels(tag4(b, 1080));
  if (!ch) return null; // 15-sample / unknown variants skipped
  const lens: number[] = [];
  for (let i = 0; i < 31; i++) lens.push(u16be(b, 20 + i * 30 + 22) * 2);
  const orders = b.subarray(952, 952 + 128);
  let maxPat = 0;
  for (let i = 0; i < 128; i++) if (orders[i] > maxPat) maxPat = orders[i];
  let off = 1084 + (maxPat + 1) * 64 * ch * 4;
  const out: SampleRegion[] = [];
  for (let i = 0; i < 31; i++) {
    const n = lens[i];
    if (n <= 2) continue;
    if (off + n > b.length) {
      out.push({ offset: off, length: b.length - off });
      break;
    }
    out.push({ offset: off, length: n });
    off += n;
  }
  return out;
}

// Core IT sample-region locator. Parses the IT on-disk layout (header @ 0x20:
// ordNum/insNum/smpNum, then the sample-header pointer table) WITHOUT requiring
// the "IMPM" magic at offset 0 — MPTM reuses this exact layout but may carry a
// different 4-byte signature ("tpm.", see extractMPTM), so the magic check lives
// in the per-format wrappers/detectFormat, not here.
function extractITLike(b: Uint8Array): SampleRegion[] | null {
  if (b.length < 0xc0) return null;
  const ordNum = u16le(b, 0x20),
    insNum = u16le(b, 0x22),
    smpNum = u16le(b, 0x24);
  const smpOffBase = 0xc0 + ordNum + insNum * 4;
  const out: SampleRegion[] = [];
  for (let i = 0; i < smpNum; i++) {
    const so = u32le(b, smpOffBase + i * 4);
    if (!so || so + 0x50 > b.length) continue;
    const flg = b[so + 0x12],
      len = u32le(b, so + 0x30),
      ptr = u32le(b, so + 0x48);
    if (!(flg & 1) || !len || !ptr) continue;
    // Skip OpenMPT/IT compressed samples (flag 0x08): the on-disk bytes are not
    // raw PCM, so carving a fixed len*bpf region would be wrong. They fall into
    // the skeleton instead — still byte-exact, just not sample-deduped.
    if (flg & 8) continue;
    const bpf = ((flg & 2) ? 2 : 1) * ((flg & 4) ? 2 : 1);
    const bytes = Math.min(len * bpf, b.length - ptr);
    if (bytes > 0) out.push({ offset: ptr, length: bytes });
  }
  return out;
}

function extractIT(b: Uint8Array): SampleRegion[] | null {
  if (tag4(b, 0) !== "IMPM") return null;
  return extractITLike(b);
}

// MPTM (OpenMPT native) is the IT format with extra OpenMPT chunks appended at
// the end (custom tunings etc.); the IT sample/instrument/pattern layout from
// offset 0 is intact, so the IT locator applies directly. Older MPTM files
// replace the "IMPM" magic with "tpm." (these are the ones detectFormat used to
// miss -> flat DAG); newer ones keep "IMPM" with cwtv in 0x0889..0x0FFF. Either
// way the bytes parse as IT. (wiki.openmpt.org Development:_Formats/MPTM)
function extractMPTM(b: Uint8Array): SampleRegion[] | null {
  if (b.length < 0xc0) return null;
  const magic = tag4(b, 0);
  if (magic !== "tpm." && magic !== "IMPM") return null;
  return extractITLike(b);
}

// MO3 is a COMPRESSED container (un4seen): the module structure and sample data
// are encoded (LZ + optional MP3/OGG for samples), so there are NO raw-PCM byte
// regions to carve and cross-module sample dedup is not applicable without
// decoding (which would break byte-exact reassembly). Returning null routes MO3
// to the whole-file CDC "flat DAG" — fetchable + byte-exact + playable, just
// without sample separation. This is correct by design, not a parser gap.
function extractMO3(_b: Uint8Array): SampleRegion[] | null {
  return null;
}

function extractS3M(b: Uint8Array): SampleRegion[] | null {
  if (b.length < 0x60 || tag4(b, 0x2c) !== "SCRM") return null;
  const ordNum = u16le(b, 0x20),
    insNum = u16le(b, 0x22);
  const paraBase = 0x60 + ordNum;
  const out: SampleRegion[] = [];
  for (let i = 0; i < insNum; i++) {
    const pp = u16le(b, paraBase + i * 2);
    if (!pp) continue;
    const off = pp * 16;
    if (off + 0x50 > b.length) continue;
    if (b[off] !== 1) continue; // type 1 = PCM
    const hi = b[off + 0x0d],
      lo = u16le(b, off + 0x0e);
    const ptr = ((hi << 16) | lo) * 16;
    const len = u32le(b, off + 0x10);
    const flg = b[off + 0x1f];
    if (!len || !ptr) continue;
    const bpf = ((flg & 4) ? 2 : 1) * ((flg & 2) ? 2 : 1);
    const bytes = Math.min(len * bpf, b.length - ptr);
    if (bytes > 0 && ptr < b.length) out.push({ offset: ptr, length: bytes });
  }
  return out;
}

function extractXM(b: Uint8Array): SampleRegion[] | null {
  if (b.length < 80 || tag4(b, 0) !== "Exte") return null; // "Extended Module: "
  const headerSize = u32le(b, 60);
  const npat = u16le(b, 70),
    nins = u16le(b, 72);
  let pos = 60 + headerSize;
  for (let p = 0; p < npat; p++) {
    if (pos + 9 > b.length) return null;
    const phLen = u32le(b, pos),
      packed = u16le(b, pos + 7);
    pos += phLen + packed;
  }
  const out: SampleRegion[] = [];
  for (let ins = 0; ins < nins; ins++) {
    if (pos + 29 > b.length) break;
    const instStart = pos;
    const instSize = u32le(b, pos);
    const numSamp = u16le(b, pos + 27);
    if (numSamp === 0) {
      pos = instStart + instSize;
      continue;
    }
    const shSize = u32le(b, pos + 29);
    let hdr = instStart + instSize;
    const lens: number[] = [];
    for (let s = 0; s < numSamp; s++) {
      if (hdr + 18 > b.length) return out.length ? out : null;
      lens.push(u32le(b, hdr)); // XM length is in BYTES
      hdr += shSize;
    }
    let data = hdr;
    for (let s = 0; s < numSamp; s++) {
      const n = lens[s];
      if (n > 0 && data + n <= b.length) out.push({ offset: data, length: n });
      data += n;
    }
    pos = data;
  }
  return out;
}

// ---------------------------------------------------------------------------
// v2 slot-indexed locator (STREAMING-PARITY-V2-SCHEMA.md)
//
// v1's sampleRegions() sorts by file offset and drops the slot association — it
// only needs to carve a byte partition. v2 keys everything on the *libopenmpt
// sample slot* (the provide_sample index, the slot debug_sample_data reads), so
// it needs each region tagged with its 1-based slot plus the native PCM layout
// (bit depth / channels) so the bake can cross-check the decoded dump and the
// client can size the provide buffer.
//
// A slot appears here if its decoded PCM can be streamed. Uncompressed samples
// carry their on-disk region directly; COMPRESSED (IT flag 0x08) samples carry the
// decoded byte length instead (the on-disk bytes are compressed, so there is no raw
// region to carve) and are tagged `compressed` so the skeleton builder appends an
// uncompressed zero-fill region of that length and repoints the header. Empty /
// unparseable slots are still absent (ride in the skeleton). Never guessed — the
// bake's `dump.data.length === slot.length` check drops any slot whose decoded
// length disagrees, so a parse surprise degrades to resident, never to wrong audio.
export interface SampleSlot {
  index: number; // 1-based libopenmpt sample slot (== provide_sample arg)
  offset: number; // on-disk PCM byte offset (the bake-time join key to the seek table)
  length: number; // decoded PCM byte length (== on-disk length for uncompressed)
  bitDepth: number; // 8 | 16
  channels: number; // 1 | 2
  compressed?: boolean; // IT 0x08 — decoded PCM streamed, skeleton region synthesized
  headerOffset?: number; // IT sample-header start (so) — where skeleton clears 0x08 + repoints
  compressedBytes?: number; // on-disk compressed span — skeleton zeroes it (dedups away)
}

function modSlots(b: Uint8Array): SampleSlot[] {
  const ch = modChannels(tag4(b, 1080));
  if (!ch) return [];
  const lens: number[] = [];
  for (let i = 0; i < 31; i++) lens.push(u16be(b, 20 + i * 30 + 22) * 2);
  const orders = b.subarray(952, 952 + 128);
  let maxPat = 0;
  for (let i = 0; i < 128; i++) if (orders[i] > maxPat) maxPat = orders[i];
  let off = 1084 + (maxPat + 1) * 64 * ch * 4;
  const out: SampleSlot[] = [];
  for (let i = 0; i < 31; i++) {
    const n = lens[i];
    if (n <= 2) continue; // empty slot — libopenmpt still numbers it, but no PCM
    const length = off + n > b.length ? b.length - off : n;
    if (length > 0) out.push({ index: i + 1, offset: off, length, bitDepth: 8, channels: 1 });
    if (off + n > b.length) break;
    off += n;
  }
  return out;
}

// On-disk byte span of an IT215/214-compressed sample, WITHOUT decompressing: the
// data is a series of length-prefixed blocks — uint16 LE compressed-byte count, then
// that many bytes — each decoding to a fixed sample count (0x8000 for 8-bit, 0x4000
// for 16-bit) per channel (stereo = full left channel's blocks then full right).
// Lets the skeleton ZERO the orphaned compressed region (-> dedups to a zero block,
// ~free over the wire) instead of leaving it as transferred dead weight. Returns -1
// if the block walk runs past EOF (corrupt/unknown layout) -> caller leaves it intact.
function itCompressedSpan(b: Uint8Array, ptr: number, frames: number, bitDepth: number, channels: number): number {
  const perBlock = bitDepth === 16 ? 0x4000 : 0x8000;
  let off = ptr;
  for (let ch = 0; ch < channels; ch++) {
    let remaining = frames;
    while (remaining > 0) {
      if (off + 2 > b.length) return -1;
      const blockLen = u16le(b, off);
      off += 2 + blockLen;
      if (off > b.length) return -1;
      remaining -= Math.min(perBlock, remaining);
    }
  }
  return off - ptr;
}

// IT/MPTM: 1-based slot = header order. Uncompressed samples carve their on-disk
// region; compressed (flag 0x08) samples stream their DECODED PCM with a synthesized
// skeleton region (the on-disk bytes are compressed, not raw PCM).
function itLikeSlots(b: Uint8Array): SampleSlot[] {
  const ordNum = u16le(b, 0x20),
    insNum = u16le(b, 0x22),
    smpNum = u16le(b, 0x24);
  const base = 0xc0 + ordNum + insNum * 4;
  const out: SampleSlot[] = [];
  for (let i = 0; i < smpNum; i++) {
    const so = u32le(b, base + i * 4);
    if (!so || so + 0x50 > b.length) continue;
    const flg = b[so + 0x12],
      len = u32le(b, so + 0x30),
      ptr = u32le(b, so + 0x48);
    if (!(flg & 1) || !len || !ptr) continue;
    const bitDepth = flg & 2 ? 16 : 8,
      channels = flg & 4 ? 2 : 1;
    const decodedBytes = len * (bitDepth / 8) * channels; // header len is in frames
    if (flg & 8) {
      // Compressed: stream the decoded PCM (length = decoded bytes). The skeleton
      // appends a zero-fill region of this size + clears 0x08 + repoints `so`, and
      // zeroes the orphaned on-disk compressed span so it dedups away (not dead weight).
      if (decodedBytes > 0) {
        const span = itCompressedSpan(b, ptr, len, bitDepth, channels);
        out.push({
          index: i + 1, offset: ptr, length: decodedBytes, bitDepth, channels,
          compressed: true, headerOffset: so, compressedBytes: span > 0 ? span : undefined,
        });
      }
      continue;
    }
    const bytes = Math.min(decodedBytes, b.length - ptr);
    if (bytes > 0) out.push({ index: i + 1, offset: ptr, length: bytes, bitDepth, channels });
  }
  return out;
}

function s3mSlots(b: Uint8Array): SampleSlot[] {
  const ordNum = u16le(b, 0x20),
    insNum = u16le(b, 0x22);
  const paraBase = 0x60 + ordNum;
  const out: SampleSlot[] = [];
  for (let i = 0; i < insNum; i++) {
    const pp = u16le(b, paraBase + i * 2);
    if (!pp) continue;
    const off = pp * 16;
    if (off + 0x50 > b.length || b[off] !== 1) continue; // type 1 = PCM
    const ptr = ((b[off + 0x0d] << 16) | u16le(b, off + 0x0e)) * 16;
    const len = u32le(b, off + 0x10);
    const flg = b[off + 0x1f];
    if (!len || !ptr) continue;
    const bitDepth = flg & 4 ? 16 : 8,
      channels = flg & 2 ? 2 : 1;
    const bytes = Math.min(len * (bitDepth / 8) * channels, b.length - ptr);
    if (bytes > 0 && ptr < b.length)
      out.push({ index: i + 1, offset: ptr, length: bytes, bitDepth, channels });
  }
  return out;
}

// XM: samples are numbered sequentially across instruments in load order — the
// running global counter IS the libopenmpt slot index.
function xmSlots(b: Uint8Array): SampleSlot[] {
  const headerSize = u32le(b, 60);
  const npat = u16le(b, 70),
    nins = u16le(b, 72);
  let pos = 60 + headerSize;
  for (let p = 0; p < npat; p++) {
    if (pos + 9 > b.length) return [];
    const phLen = u32le(b, pos),
      packed = u16le(b, pos + 7);
    pos += phLen + packed;
  }
  const out: SampleSlot[] = [];
  let slot = 0; // running global sample index (1-based after ++)
  for (let ins = 0; ins < nins; ins++) {
    if (pos + 29 > b.length) break;
    const instStart = pos;
    const instSize = u32le(b, pos);
    const numSamp = u16le(b, pos + 27);
    if (numSamp === 0) {
      pos = instStart + instSize;
      continue;
    }
    const shSize = u32le(b, pos + 29);
    let hdr = instStart + instSize;
    const meta: { length: number; bitDepth: number }[] = [];
    for (let s = 0; s < numSamp; s++) {
      if (hdr + 18 > b.length) break;
      const length = u32le(b, hdr); // XM length is in BYTES
      const typ = b[hdr + 14];
      meta.push({ length, bitDepth: typ & 0x10 ? 16 : 8 }); // XM samples are mono
      hdr += shSize;
    }
    let data = hdr;
    for (let s = 0; s < meta.length; s++) {
      slot++;
      const { length, bitDepth } = meta[s];
      if (length > 0 && data + length <= b.length)
        out.push({ index: slot, offset: data, length, bitDepth, channels: 1 });
      data += length;
    }
    pos = data;
  }
  return out;
}

const SLOT_EXTRACTORS: Record<Format, (b: Uint8Array) => SampleSlot[]> = {
  mod: modSlots,
  it: itLikeSlots,
  mptm: itLikeSlots,
  s3m: s3mSlots,
  xm: xmSlots,
  mo3: () => [],
};

/**
 * Locate each sample slot's uncompressed on-disk PCM region, keyed by the
 * 1-based libopenmpt slot index (v2). Unlike sampleRegions() this does NOT sort
 * or drop overlaps — the slot index is load-bearing. Returns null only for
 * formats/files we can't confidently parse.
 */
export function sampleSlots(
  b: Uint8Array,
  fmtHint?: Format,
): { format: Format; slots: SampleSlot[] } | null {
  const format = fmtHint ?? detectFormat(b);
  if (!format) return null;
  const slots = SLOT_EXTRACTORS[format](b).filter(
    (s) => s.length > 0 && s.offset >= 0 && s.offset + s.length <= b.length,
  );
  return { format, slots };
}

const EXTRACTORS: Record<Format, (b: Uint8Array) => SampleRegion[] | null> = {
  mod: extractMOD,
  it: extractIT,
  s3m: extractS3M,
  xm: extractXM,
  mptm: extractMPTM,
  mo3: extractMO3,
};

export function detectFormat(b: Uint8Array): Format | null {
  // MO3 magic is 3 bytes "MO3" + a version byte (compressed container -> flat).
  if (b.length >= 4 && b[0] === 0x4d && b[1] === 0x4f && b[2] === 0x33) return "mo3";
  if (b.length >= 4 && tag4(b, 0) === "tpm.") return "mptm"; // older MPTM magic
  if (b.length >= 0x2c && tag4(b, 0) === "IMPM") {
    // MPTM (newer) keeps IMPM but tags cwtv (0x28) in 0x0889..0x0FFF. Same
    // sample layout as IT, so the label is cosmetic — both route to extractITLike.
    const cwtv = u16le(b, 0x28);
    return cwtv >= 0x0889 && cwtv <= 0x0fff ? "mptm" : "it";
  }
  if (b.length >= 0x30 && tag4(b, 0x2c) === "SCRM") return "s3m";
  if (b.length >= 17 && tag4(b, 0) === "Exte") return "xm";
  if (b.length >= 1084 && modChannels(tag4(b, 1080))) return "mod";
  return null;
}

/**
 * Locate every sample-PCM region in a module, sorted by offset and clamped to
 * be non-overlapping (parsers can occasionally point two samples at the same
 * shared payload; the skeleton model needs a clean partition). Returns null for
 * formats/files we can't confidently parse — never guessed.
 */
export function sampleRegions(
  b: Uint8Array,
  fmtHint?: Format,
): { format: Format; regions: SampleRegion[] } | null {
  const format = fmtHint ?? detectFormat(b);
  if (!format) return null;
  const raw = EXTRACTORS[format](b);
  if (!raw) return null;
  const sorted = raw
    .filter((r) => r.length > 0 && r.offset >= 0 && r.offset + r.length <= b.length)
    .sort((a, b2) => a.offset - b2.offset);
  // Drop regions that overlap a previously-kept one (shared sample payloads).
  const regions: SampleRegion[] = [];
  let end = 0;
  for (const r of sorted) {
    if (r.offset < end) continue;
    regions.push(r);
    end = r.offset + r.length;
  }
  return { format, regions };
}
