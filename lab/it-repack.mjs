// Repack effectiveness on IT — including the large worst-case files.
//
// IT uses instruments→samples, so we detect the samples the OPENING needs
// empirically: silence one sample (via its header) and see if the first
// pattern's audio changes. Then present ONLY the needed samples and confirm
// stock libopenmpt reproduces the opening — and compare bytes/TTFPB vs the
// minimal correct prefix in original file order.

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';

const DIR = join(homedir(), 'tmp', 'somemods');
const SIM_OK = 0.98;        // "correct opening"
const SIM_NEEDED = 0.999;   // removing a sample below this => it contributes => needed
const BW = 50e6 / 8;        // 50 Mbit, bytes/s

const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

function parseIt(b) {
  if (String.fromCharCode(b[0], b[1], b[2], b[3]) !== 'IMPM') return null;
  const ordNum = u16(b, 0x20), insNum = u16(b, 0x22), smpNum = u16(b, 0x24);
  const smpOffBase = 0xc0 + ordNum + insNum * 4;
  const samples = [];
  let totalData = 0;
  for (let i = 0; i < smpNum; i++) {
    const so = u32(b, smpOffBase + i * 4);
    const flg = b[so + 0x12];
    const len = u32(b, so + 0x30);
    const has = !!(flg & 0x01), is16 = !!(flg & 0x02), stereo = !!(flg & 0x04);
    const bytes = (has && len) ? len * (is16 ? 2 : 1) * (stereo ? 2 : 1) : 0;
    if (bytes) totalData += bytes;
    samples.push({ i, so, len, has, bytes });
  }
  return { smpNum, samples, totalData, manifest: b.length - totalData };
}

// copy of buffer with sample i silenced (Length=0, clear "has sample" flag)
function silence(b, s) {
  const c = Buffer.from(b);
  c[s.so + 0x12] &= ~0x01;
  c.writeUInt32LE(0, s.so + 0x30);
  return c;
}

function firstPatternSim(buf, ref) {
  const { ptr } = ompt.create(buf);
  if (!ptr) return 0;
  const a = ompt.renderFirstPattern(ptr);
  ompt.destroy(ptr);
  return ompt.similarity(ref, a).sim;
}

await ompt.load();

const its = readdirSync(DIR)
  .filter(f => !f.startsWith('.') && statSync(join(DIR, f)).isFile() && /\.it$/i.test(f))
  .map(f => ({ name: f, buf: readFileSync(join(DIR, f)) }))
  .sort((a, b) => a.buf.length - b.buf.length);

console.log(`\nIT repack effectiveness — first-pattern correctness, needed-samples-only.  sim_ok=${SIM_OK}\n`);
console.log('file                       size    smp(needed/has)  manifest  transferred  sim(repack)   origPrefix(correct)   TTFPB orig→repack');
console.log('-'.repeat(124));

for (const { name, buf } of its) {
  const m = parseIt(buf);
  if (!m) { console.log(name.padEnd(26), 'parse skipped'); continue; }

  const full = ompt.create(buf);
  const ref = ompt.renderFirstPattern(full.ptr);
  const parseMs = full.ms;
  ompt.destroy(full.ptr);
  if (ref.length === 0) { console.log(name.padEnd(26), 'no audio'); continue; }

  // empirical needed-sample detection
  const withData = m.samples.filter(s => s.bytes > 0);
  const needed = [];
  for (const s of withData) {
    if (firstPatternSim(silence(buf, s), ref) < SIM_NEEDED) needed.push(s);
  }

  // present ONLY needed samples (silence the rest), render opening
  let onlyNeeded = Buffer.from(buf);
  for (const s of withData) if (!needed.includes(s)) onlyNeeded = silence(onlyNeeded, s);
  const simRepack = firstPatternSim(onlyNeeded, ref);

  const neededBytes = needed.reduce((a, s) => a + s.bytes, 0);
  const transferred = m.manifest + neededBytes;

  // minimal correct prefix in ORIGINAL order (bisection on first-pattern audio)
  let lo = 0, hi = buf.length, correctLen = buf.length;
  for (let it = 0; it < 18 && hi - lo > Math.max(512, buf.length * 0.004); it++) {
    const mid = (lo + hi) >> 1;
    if (firstPatternSim(buf.subarray(0, mid), ref) >= SIM_OK) { correctLen = mid; hi = mid; } else lo = mid;
  }

  const ttfpbOrig = (correctLen / BW) * 1000 + parseMs;
  const ttfpbRepack = (transferred / BW) * 1000 + parseMs;
  const kb = x => (x / 1024 | 0) + 'K';

  console.log(
    name.slice(0, 25).padEnd(26),
    kb(buf.length).padStart(6),
    `${needed.length}/${withData.length}`.padStart(14),
    kb(m.manifest).padStart(9),
    ((transferred / buf.length * 100).toFixed(1) + '%').padStart(12),
    simRepack.toFixed(3).padStart(12),
    ((correctLen / buf.length * 100).toFixed(1) + '%').padStart(20),
    `   ${ttfpbOrig.toFixed(0)}ms → ${ttfpbRepack.toFixed(0)}ms`,
  );
}
console.log('-'.repeat(124));
console.log('sim(repack): opening rendered by stock libopenmpt with ONLY needed samples present.');
console.log('origPrefix: fraction of the file needed for a correct opening in original byte order (the thing repack replaces).');
