// Lab: cross-module sample dedup — how much sample PCM is byte-identical across
// the whole Mod Archive corpus? This is the number that decides whether a
// content-addressed (CID) block layer is worth building.
//
// Why it matters (from prior labs): samples are ~94-98% of a module's bytes, so
// ALL dedup payoff is at the sample-PCM level. The tracker scene reuses samples
// heavily (Amiga ST-0x disks, ripped drum hits, an artist's own palette). If a
// large fraction of sample PCM is byte-identical across modules, then:
//   - server storage collapses to the unique set, and
//   - client transfer drops within a listening session (later tracks' samples
//     are already cached) — which compounds with the seek/repack resident-set
//     fetch (its bytes become cache hits).
//
// We measure two ratios, both byte-EXACT (no fuzzy audio dedup — the whole
// streaming story depends on bit-exactness):
//   1. whole-PCM dedup: hash each sample's raw PCM payload (header stripped,
//      since loop/name/C5 vary per module even when PCM is identical).
//   2. sub-sample dedup via content-defined chunking (FastCDC): catches
//      trimmed/extended variants that share interior chunks.
//
// Corpus is double-zipped: outer prefix-zip -> per-module `name.ext.zip` -> module.
//
// Env: EVERY (stride over modules, default 50), LIMIT (max modules, 0=all),
//      FORMATS (csv, default mod,it), CDC (1/0, default 1).

import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import yauzl from 'yauzl';

const ROOT = join(homedir(), 'tmp', 'modarchive');
const EVERY = +(process.env.EVERY ?? 50);
const LIMIT = +(process.env.LIMIT ?? 0);
const FORMATS = (process.env.FORMATS ?? 'mod,it').toLowerCase().split(',');
const DO_CDC = (process.env.CDC ?? '1') !== '0';

const u16le = (b, o) => b[o] | (b[o + 1] << 8);
const u16be = (b, o) => (b[o] << 8) | b[o + 1];
const u32le = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

// fast 64-bit content key (two independent 32-bit FNV-1a-ish lanes), 16-hex.
// No per-call allocation -> cheap over tens of millions of chunks.
function hash64(buf) {
  let a = 0x811c9dc5 >>> 0, b = 0x9e3779b1 >>> 0;
  for (let i = 0; i < buf.length; i++) {
    const c = buf[i];
    a = Math.imul(a ^ c, 16777619) >>> 0;
    b = Math.imul(b ^ c, 2246822519) >>> 0;
  }
  return a.toString(16).padStart(8, '0') + b.toString(16).padStart(8, '0');
}

// ---------- per-format sample-PCM extractors ----------
// Each returns an array of Buffers (raw PCM payloads as stored), or null if the
// file can't be confidently parsed (counted as skipped, never guessed).

function modChannels(tag) {
  if (tag === 'M.K.' || tag === 'M!K!' || tag === 'FLT4' || tag === '4CHN') return 4;
  if (tag === '6CHN') return 6;
  if (tag === 'FLT8' || tag === '8CHN' || tag === 'OCTA' || tag === 'CD81') return 8;
  let m = /^(\d)CHN$/.exec(tag); if (m) return +m[1];
  m = /^(\d\d)CH$/.exec(tag); if (m) return +m[1];
  m = /^(\d\d)CN$/.exec(tag); if (m) return +m[1];
  m = /^TDZ(\d)$/.exec(tag); if (m) return +m[1];
  return 0;
}
function extractMOD(b) {
  if (b.length < 1084) return null;
  const tag = String.fromCharCode(b[1080], b[1081], b[1082], b[1083]);
  const ch = modChannels(tag);
  if (!ch) return null; // 15-sample/unknown variants skipped for now
  const lens = [];
  for (let i = 0; i < 31; i++) lens.push(u16be(b, 20 + i * 30 + 22) * 2);
  const orders = b.subarray(952, 952 + 128);
  let maxPat = 0;
  for (let i = 0; i < 128; i++) if (orders[i] > maxPat) maxPat = orders[i];
  let off = 1084 + (maxPat + 1) * 64 * ch * 4;
  const out = [];
  for (let i = 0; i < 31; i++) {
    const n = lens[i];
    if (n <= 2) { continue; } // empty / 1-word dummy
    if (off + n > b.length) { out.push(b.subarray(off)); off = b.length; break; }
    out.push(b.subarray(off, off + n));
    off += n;
  }
  return out;
}

function extractIT(b) {
  if (b.length < 0xc0 || String.fromCharCode(b[0], b[1], b[2], b[3]) !== 'IMPM') return null;
  const ordNum = u16le(b, 0x20), insNum = u16le(b, 0x22), smpNum = u16le(b, 0x24);
  const smpOffBase = 0xc0 + ordNum + insNum * 4;
  const out = [];
  for (let i = 0; i < smpNum; i++) {
    const so = u32le(b, smpOffBase + i * 4);
    if (!so || so + 0x50 > b.length) continue;
    const flg = b[so + 0x12], len = u32le(b, so + 0x30), ptr = u32le(b, so + 0x48);
    if (!(flg & 1) || !len || !ptr) continue;
    const bpf = ((flg & 2) ? 2 : 1) * ((flg & 4) ? 2 : 1);
    const bytes = Math.min(len * bpf, b.length - ptr);
    if (bytes > 0) out.push(b.subarray(ptr, ptr + bytes));
  }
  return out;
}

function extractS3M(b) {
  if (b.length < 0x60 || String.fromCharCode(b[0x2c], b[0x2d], b[0x2e], b[0x2f]) !== 'SCRM') return null;
  const ordNum = u16le(b, 0x20), insNum = u16le(b, 0x22);
  const paraBase = 0x60 + ordNum;          // instrument parapointers (u16, ×16 bytes)
  const out = [];
  for (let i = 0; i < insNum; i++) {
    const pp = u16le(b, paraBase + i * 2);
    if (!pp) continue;
    const off = pp * 16;
    if (off + 0x50 > b.length) continue;
    if (b[off] !== 1) continue;            // type 1 = PCM sample
    const hi = b[off + 0x0d], lo = u16le(b, off + 0x0e);
    const ptr = ((hi << 16) | lo) * 16;
    const len = u32le(b, off + 0x10);
    const flg = b[off + 0x1f];
    if (!len || !ptr) continue;
    const bpf = ((flg & 4) ? 2 : 1) * ((flg & 2) ? 2 : 1);
    const bytes = Math.min(len * bpf, b.length - ptr);
    if (bytes > 0 && ptr < b.length) out.push(b.subarray(ptr, ptr + bytes));
  }
  return out;
}

function extractXM(b) {
  if (b.length < 80 || String.fromCharCode(...b.subarray(0, 17)) !== 'Extended Module: ') return null;
  const headerSize = u32le(b, 60);
  const npat = u16le(b, 70), nins = u16le(b, 72);
  let pos = 60 + headerSize;
  for (let p = 0; p < npat; p++) {                  // skip patterns
    if (pos + 9 > b.length) return null;
    const phLen = u32le(b, pos), packed = u16le(b, pos + 7);
    pos += phLen + packed;
  }
  const out = [];
  for (let ins = 0; ins < nins; ins++) {
    if (pos + 29 > b.length) break;
    const instStart = pos;
    const instSize = u32le(b, pos);
    const numSamp = u16le(b, pos + 27);
    if (numSamp === 0) { pos = instStart + instSize; continue; }
    const shSize = u32le(b, pos + 29);
    let hdr = instStart + instSize;                 // sample headers begin after instrument header
    const lens = [], bits16 = [];
    for (let s = 0; s < numSamp; s++) {
      if (hdr + 18 > b.length) return out.length ? out : null;
      lens.push(u32le(b, hdr));                      // XM length is in BYTES
      bits16.push((b[hdr + 14] & 0x10) !== 0);
      hdr += shSize;
    }
    let data = hdr;                                  // sample data after all headers
    for (let s = 0; s < numSamp; s++) {
      const n = lens[s];
      if (n > 0 && data + n <= b.length) out.push(b.subarray(data, data + n));
      data += n;
    }
    pos = data;
  }
  return out;
}

const EXTRACTORS = { mod: extractMOD, it: extractIT, s3m: extractS3M, xm: extractXM };

// ---------- FastCDC (normalized, 32-bit gear) ----------
const GEAR = (() => {
  const g = new Uint32Array(256);
  let s = 0x9e3779b9 >>> 0;
  for (let i = 0; i < 256; i++) { s = (Math.imul(s, 1664525) + 1013904223) >>> 0; g[i] = s; }
  return g;
})();
const MIN = 512, AVG = 2048, MAX = 8192;
const BITS = Math.log2(AVG) | 0;
const MASK_S = ((1 << (BITS + 2)) - 1) >>> 0; // stricter before avg
const MASK_L = ((1 << (BITS - 2)) - 1) >>> 0; // looser after avg
function* cdcChunks(buf) {
  let off = 0;
  const n = buf.length;
  while (off < n) {
    let end = Math.min(off + MAX, n);
    const normal = Math.min(off + AVG, n);
    let fp = 0, i = off + MIN;
    if (i < off) i = off;
    let cut = -1;
    for (; i < end; i++) {
      fp = ((fp << 1) + GEAR[buf[i]]) >>> 0;
      const mask = i < normal ? MASK_S : MASK_L;
      if ((fp & mask) === 0) { cut = i + 1; break; }
    }
    const stop = cut === -1 ? end : cut;
    yield buf.subarray(off, stop);
    off = stop;
  }
}

// ---------- zip plumbing (double unzip, streaming) ----------
function openZip(input) {
  return new Promise((res, rej) => {
    const cb = (e, zf) => e ? rej(e) : res(zf);
    if (Buffer.isBuffer(input)) yauzl.fromBuffer(input, { lazyEntries: true }, cb);
    else yauzl.open(input, { lazyEntries: true }, cb);
  });
}
function readEntry(zf, entry) {
  return new Promise((res, rej) => {
    zf.openReadStream(entry, (e, rs) => {
      if (e) return rej(e);
      const chunks = [];
      rs.on('data', d => chunks.push(d));
      rs.on('end', () => res(Buffer.concat(chunks)));
      rs.on('error', rej);
    });
  });
}
// iterate entries of an open zipfile, calling async cb(entry) sequentially
function walkZip(zf, onEntry) {
  return new Promise((resolve, reject) => {
    zf.on('entry', async (entry) => {
      try { await onEntry(entry); zf.readEntry(); }
      catch (e) { reject(e); }
    });
    zf.on('end', resolve);
    zf.on('error', reject);
    zf.readEntry();
  });
}

// ---------- main scan ----------
const seenWhole = new Map();   // pcmHash -> { count, bytes }
let totalSamples = 0, totalBytes = 0;
const cdcSeen = new Set();
let cdcTotalBytes = 0, cdcUniqueBytes = 0;
const perFmt = {};             // fmt -> { mods, samples, bytes }
let modIndex = 0, processed = 0, skipped = 0, errors = 0, nextTick = 2000;
let uniqueBytesLive = 0;
const t0 = process.hrtime.bigint();
const pctLive = () => totalBytes ? (100 * (totalBytes - uniqueBytesLive) / totalBytes).toFixed(1) + '%' : '—';

function rememberSample(pcm, fmt) {
  totalSamples++; totalBytes += pcm.length;
  perFmt[fmt].samples++; perFmt[fmt].bytes += pcm.length;
  const h = hash64(pcm);
  const e = seenWhole.get(h);
  if (e) { e.count++; } else { seenWhole.set(h, { count: 1, bytes: pcm.length }); uniqueBytesLive += pcm.length; }
  if (DO_CDC) {
    for (const c of cdcChunks(pcm)) {
      cdcTotalBytes += c.length;
      const ch = hash64(c);
      if (!cdcSeen.has(ch)) { cdcSeen.add(ch); cdcUniqueBytes += c.length; }
    }
  }
}

async function processModule(name, buf) {
  const ext = (name.split('.').pop() || '').toLowerCase();
  const fn = EXTRACTORS[ext];
  if (!fn) return; // format not parsed in this pass
  perFmt[ext] ??= { mods: 0, samples: 0, bytes: 0 };
  let pcms;
  try { pcms = fn(buf); } catch { errors++; return; }
  if (!pcms) { skipped++; return; }
  perFmt[ext].mods++;
  for (const p of pcms) if (p.length) rememberSample(p, ext);
  processed++;
}

function listOuterZips() {
  const out = [];
  (function rec(d) {
    for (const e of readdirSync(d, { withFileTypes: true })) {
      if (e.name.startsWith('.')) continue;
      const p = join(d, e.name);
      if (e.isDirectory()) rec(p);
      else if (e.name.toLowerCase().endsWith('.zip')) out.push(p);
    }
  })(ROOT);
  return out.sort();
}

console.log(`scan: EVERY=${EVERY} LIMIT=${LIMIT || '∞'} FORMATS=${FORMATS.join(',')} CDC=${DO_CDC ? 'on' : 'off'}`);
const outerZips = listOuterZips();
console.log(`outer zips: ${outerZips.length}`);

outer:
for (const oz of outerZips) {
  let zf;
  try { zf = await openZip(oz); } catch { errors++; continue; }
  await walkZip(zf, async (entry) => {
    const nm = entry.fileName;
    if (!nm.toLowerCase().endsWith('.zip')) return;            // inner must be a module zip
    const ext = nm.slice(0, -4).split('.').pop()?.toLowerCase();
    if (!FORMATS.includes(ext)) return;                         // skip formats we're not scanning
    if (modIndex++ % EVERY !== 0) return;                       // stride sample
    const innerZipBuf = await readEntry(zf, entry);
    let izf;
    try { izf = await openZip(innerZipBuf); } catch { errors++; return; }
    await walkZip(izf, async (me) => {
      const modBuf = await readEntry(izf, me);
      await processModule(me.fileName, modBuf);
    });
  });
  if (LIMIT && processed >= LIMIT) break outer;
  if (processed >= nextTick) {
    nextTick += 2000;
    const dt = Number(process.hrtime.bigint() - t0) / 1e9;
    console.log(`  ${processed} modules, ${totalSamples} samples, ${(totalBytes / 1e6).toFixed(0)}MB, whole-dup ${pctLive()}, ${dt.toFixed(0)}s`);
  }
}

// ---------- report ----------
const dt = Number(process.hrtime.bigint() - t0) / 1e9;
let uniqueBytes = 0, uniqueSamples = seenWhole.size, dupSamples = 0;
const pop = [];
for (const [h, e] of seenWhole) { uniqueBytes += e.bytes; if (e.count > 1) dupSamples += e.count - 1; pop.push(e); }
pop.sort((a, b) => b.count - a.count);

const pct = (a, b) => b ? (100 * a / b).toFixed(1) + '%' : '—';
console.log(`\n\n=== cross-module sample dedup ===  (${dt.toFixed(0)}s)`);
console.log(`modules parsed:   ${processed}   skipped: ${skipped}   errors: ${errors}`);
for (const [f, s] of Object.entries(perFmt)) console.log(`  ${f}: ${s.mods} mods, ${s.samples} samples, ${(s.bytes / 1e6).toFixed(1)}MB`);
console.log(`\nsamples:          ${totalSamples} total, ${uniqueSamples} unique  (${dupSamples} redundant copies)`);
console.log(`whole-PCM bytes:  ${(totalBytes / 1e6).toFixed(1)}MB total -> ${(uniqueBytes / 1e6).toFixed(1)}MB unique`);
console.log(`  WHOLE-SAMPLE DEDUP: ${pct(totalBytes - uniqueBytes, totalBytes)} of sample bytes are byte-identical duplicates`);
if (DO_CDC) {
  console.log(`CDC chunk bytes:  ${(cdcTotalBytes / 1e6).toFixed(1)}MB total -> ${(cdcUniqueBytes / 1e6).toFixed(1)}MB unique`);
  console.log(`  SUB-SAMPLE DEDUP (FastCDC ${MIN}/${AVG}/${MAX}): ${pct(cdcTotalBytes - cdcUniqueBytes, cdcTotalBytes)} of sample bytes redundant`);
}
console.log(`\nmost-reused sample PCM (copies × size):`);
for (const e of pop.slice(0, 12)) console.log(`  ${String(e.count).padStart(5)} copies × ${(e.bytes / 1024).toFixed(1).padStart(7)}KB`);
