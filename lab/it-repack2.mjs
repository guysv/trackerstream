// Corrected IT repack measurement.
//  - needed samples via STATIC first-pattern decode (ground truth), not audio diff
//  - rendering made deterministic by seeding the RNG shim, so verification is valid
//    (beyond_the_network uses IT random vol/pan variation -> Math.random in this build)

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';

// --- deterministic RNG: install BEFORE importing libopenmpt wrapper ---
let _seed = 0;
function reseed() { _seed = 0x2545f4914f6cdd1d >>> 0; }
Math.random = () => { _seed = (Math.imul(_seed, 1103515245) + 12345) & 0x7fffffff; return _seed / 0x7fffffff; };

const ompt = await import('./lib.mjs');
await ompt.load();

const DIR = join(homedir(), 'tmp', 'somemods');
const SIM_OK = 0.98;
const BW = 50e6 / 8;
const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

function header(b) {
  const ordNum = u16(b, 0x20), insNum = u16(b, 0x22), smpNum = u16(b, 0x24), cmwt = u16(b, 0x2a);
  const insOffBase = 0xc0 + ordNum, smpOffBase = insOffBase + insNum * 4, patOffBase = smpOffBase + smpNum * 4;
  return { ordNum, insNum, smpNum, cmwt, insOffBase, smpOffBase, patOffBase, orders: [...b.subarray(0xc0, 0xc0 + ordNum)] };
}
function decodePattern(b, off) {
  if (off === 0) return { instruments: new Set(), notesByIns: new Map() };
  const len = u16(b, off), rows = u16(b, off + 2);
  let p = off + 8; const end = p + len;
  const lastMask = new Array(64).fill(0), lastIns = new Array(64).fill(0), lastNote = new Array(64).fill(0);
  const instruments = new Set(), notesByIns = new Map(); let row = 0;
  while (p < end && row < rows) {
    const cv = b[p++]; if (cv === 0) { row++; continue; }
    const ch = (cv - 1) & 63; let mask = lastMask[ch];
    if (cv & 128) { mask = b[p++]; lastMask[ch] = mask; }
    let note = null, ins = null;
    if (mask & 1) { note = b[p++]; lastNote[ch] = note; }
    if (mask & 2) { ins = b[p++]; lastIns[ch] = ins; }
    if (mask & 4) p++;
    if (mask & 8) p += 2;
    if (mask & 16) note = lastNote[ch];
    if (mask & 32) ins = lastIns[ch];
    if (ins && note !== null && note < 120) {
      instruments.add(ins);
      if (!notesByIns.has(ins)) notesByIns.set(ins, new Set());
      notesByIns.get(ins).add(note);
    }
  }
  return { instruments, notesByIns };
}
function samplesForIns(b, h, ins, notes) {
  const off = u32(b, h.insOffBase + (ins - 1) * 4); if (off === 0) return new Set();
  // keyboard table offset: new instrument format (cmwt>=0x200) @0x40, old @0x2E
  const map = off + (h.cmwt >= 0x200 ? 0x40 : 0x2e), out = new Set();
  for (const n of notes) { if (n >= 0 && n <= 119) { const s = b[map + n * 2 + 1]; if (s > 0) out.add(s - 1); } }
  return out;
}
function sampleSizes(b, h) {
  const sz = []; let total = 0;
  for (let i = 0; i < h.smpNum; i++) {
    const so = u32(b, h.smpOffBase + i * 4), flg = b[so + 0x12], len = u32(b, so + 0x30);
    const has = !!(flg & 1), is16 = !!(flg & 2), st = !!(flg & 4);
    const bytes = (has && len) ? len * (is16 ? 2 : 1) * (st ? 2 : 1) : 0;
    sz[i] = { so, bytes }; total += bytes;
  }
  return { sz, total };
}
function silence(b, so) { const c = Buffer.from(b); c[so + 0x12] &= ~1; c.writeUInt32LE(0, so + 0x30); return c; }
// render exactly the first pattern, deterministically (reseed before create)
function renderFP(b) { reseed(); const m = ompt.create(b); if (!m.ptr) return { a: new Float32Array(0), ms: m.ms }; const a = ompt.renderFirstPattern(m.ptr); ompt.destroy(m.ptr); return { a, ms: m.ms }; }

const its = readdirSync(DIR).filter(f => /\.it$/i.test(f) && !f.startsWith('.') && statSync(join(DIR, f)).isFile())
  .map(f => ({ name: f, buf: readFileSync(join(DIR, f)) })).sort((a, b) => a.buf.length - b.buf.length);

const W = 6;
console.log(`\nIT repack — STATIC needed-sample detection, deterministic render.  window=${W}s sim_ok=${SIM_OK}\n`);
console.log('file                     size   ins(used/tot)  smp(used)  manifest  transferred  sim(repack)  origPrefix   TTFPB orig→repack');
console.log('-'.repeat(126));

for (const { name, buf } of its) {
  const h = header(buf);
  const firstOrder = h.orders.find(o => o < 254);
  const { instruments, notesByIns } = decodePattern(buf, u32(buf, h.patOffBase + firstOrder * 4));
  const needed = new Set();
  for (const ins of instruments) for (const s of samplesForIns(buf, h, ins, notesByIns.get(ins))) needed.add(s);

  const { sz, total } = sampleSizes(buf, h);
  const manifest = buf.length - total;
  const neededBytes = [...needed].reduce((a, i) => a + (sz[i]?.bytes || 0), 0);
  const transferred = manifest + neededBytes;

  // reference = exactly the first pattern, deterministic
  const ref = renderFP(buf);

  // build only-needed and verify
  let only = Buffer.from(buf);
  for (let i = 0; i < h.smpNum; i++) if (!needed.has(i) && sz[i].bytes > 0) only = silence(only, sz[i].so);
  const rep = renderFP(only);
  const simRepack = ompt.similarity(ref.a, rep.a).sim;

  // minimal correct prefix in original order (deterministic)
  let lo = 0, hi = buf.length, correctLen = buf.length;
  for (let it = 0; it < 18 && hi - lo > Math.max(512, buf.length * 0.004); it++) {
    const mid = (lo + hi) >> 1;
    const sim = ompt.similarity(ref.a, renderFP(buf.subarray(0, mid)).a).sim;
    if (sim >= SIM_OK) { correctLen = mid; hi = mid; } else lo = mid;
  }
  const kb = x => (x / 1024 | 0) + 'K';
  console.log(
    name.slice(0, 24).padEnd(25),
    kb(buf.length).padStart(5),
    `${instruments.size}/${h.insNum}`.padStart(12),
    String(needed.size).padStart(8),
    kb(manifest).padStart(9),
    ((transferred / buf.length * 100).toFixed(1) + '%').padStart(11),
    simRepack.toFixed(3).padStart(12),
    ((correctLen / buf.length * 100).toFixed(1) + '%').padStart(11),
    `   ${((correctLen / BW) * 1000 + ref.ms).toFixed(0)}ms → ${((transferred / BW) * 1000 + ref.ms).toFixed(0)}ms`,
  );
}
console.log('-'.repeat(126));
console.log('needed samples = union of (samples mapped by instruments actually triggered in the first pattern, for the notes played).');
