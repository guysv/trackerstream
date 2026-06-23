// Per-format sample-PCM locators (ported from lab/cid-dedup.mjs). Each returns
// the byte regions of the raw sample PCM payloads in the file — NOT copies — so
// the DAG builder can carve the module into a compact skeleton (everything that
// is not sample PCM, ~1.5-5.7% of a big module) plus per-sample chunk lists. The
// payload (not the format's sample blob) is what dedupes across modules: loop
// points / C5Speed / name / flags vary per module even when the PCM is identical.

export type Format = "mod" | "it" | "s3m" | "xm";

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

function extractIT(b: Uint8Array): SampleRegion[] | null {
  if (b.length < 0xc0 || tag4(b, 0) !== "IMPM") return null;
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
    const bpf = ((flg & 2) ? 2 : 1) * ((flg & 4) ? 2 : 1);
    const bytes = Math.min(len * bpf, b.length - ptr);
    if (bytes > 0) out.push({ offset: ptr, length: bytes });
  }
  return out;
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

const EXTRACTORS: Record<Format, (b: Uint8Array) => SampleRegion[] | null> = {
  mod: extractMOD,
  it: extractIT,
  s3m: extractS3M,
  xm: extractXM,
};

export function detectFormat(b: Uint8Array): Format | null {
  if (b.length >= 4 && tag4(b, 0) === "IMPM") return "it";
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
