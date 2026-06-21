// Repack proof-of-concept on ProTracker MOD (simplest, offset-free layout).
//
// Demonstrates the non-fork + repack mechanism end-to-end:
//   1. Parse the MOD, find which samples the FIRST PATTERN actually triggers.
//   2. Build a "repacked prefix" = the module with ONLY those samples' data
//      present (all other sample data zero-filled = "not transferred yet").
//   3. Feed it to STOCK libopenmpt and confirm the first pattern renders
//      identically to the full file. If it does, the non-fork approach works:
//      we transfer manifest + needed samples first, recreate, and play.
//   4. Control: take the SAME number of bytes but in ORIGINAL file order and
//      show the opening is broken — isolating the repack (reordering) as the win.

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';

const DIR = join(homedir(), 'tmp', 'somemods');
const SIM_OK = 0.98;
const BW = 50e6 / 8; // bytes/sec (50 Mbit)

function parseMod(buf) {
  // 4-char tag at 1080 decides channel count
  const tag = String.fromCharCode(buf[1080], buf[1081], buf[1082], buf[1083]);
  let ch = 4;
  if (/^\d CHN$/.test(tag)) ch = +tag[0];
  else if (/^\d\dCH$/.test(tag)) ch = +tag.slice(0, 2);
  else if (tag === 'M.K.' || tag === 'M!K!' || tag === 'FLT4' || tag === '4CHN') ch = 4;
  else if (tag === '6CHN') ch = 6;
  else if (tag === '8CHN' || tag === 'FLT8' || tag === 'OCTA') ch = 8;
  else return null; // not a recognized MOD

  // 31 sample headers @ 20, 30 bytes each; length is words (big-endian) at +22
  const smpLen = [];
  for (let i = 0; i < 31; i++) {
    const h = 20 + i * 30;
    smpLen[i] = ((buf[h + 22] << 8) | buf[h + 23]) * 2; // bytes
  }
  const songLen = buf[950];
  const order = buf.subarray(952, 952 + 128);
  let numPat = 0;
  for (let i = 0; i < 128; i++) numPat = Math.max(numPat, order[i]);
  numPat += 1;

  const rowBytes = 64 * ch * 4;
  const patBase = 1084;
  const smpDataStart = patBase + numPat * rowBytes;

  // sample-data byte ranges (samples 1..31 concatenated in order)
  const range = []; let off = smpDataStart;
  for (let i = 0; i < 31; i++) { range[i] = { start: off, len: smpLen[i] }; off += smpLen[i]; }

  return { ch, order, numPat, rowBytes, patBase, smpDataStart, range, end: off };
}

// samples triggered in a given pattern index
function samplesInPattern(buf, m, patIdx) {
  const used = new Set();
  const base = m.patBase + patIdx * m.rowBytes;
  for (let cell = 0; cell < 64 * m.ch; cell++) {
    const c = base + cell * 4;
    const s = (buf[c] & 0xf0) | (buf[c + 2] >> 4);
    if (s > 0) used.add(s - 1); // sample numbers are 1-based
  }
  return used;
}

await ompt.load();

const mods = readdirSync(DIR)
  .filter(f => !f.startsWith('.') && statSync(join(DIR, f)).isFile() && /\.mod$/i.test(f))
  .map(f => ({ name: f, buf: readFileSync(join(DIR, f)) }))
  .sort((a, b) => a.buf.length - b.buf.length);

console.log(`\nMOD repack PoC — first-pattern correctness with needed-samples-only.  sim_ok=${SIM_OK}\n`);
console.log('file                       size   ch  smp(used/tot)  transferred   sim(repack)  sim(origSameBytes)   TTFPB orig→repack');
console.log('-'.repeat(116));

for (const { name, buf } of mods) {
  const m = parseMod(buf);
  if (!m || m.smpDataStart > buf.length) { console.log(name.padEnd(26), 'parse skipped'); continue; }

  // reference: full file, first pattern
  const full = ompt.create(buf);
  const ref = ompt.renderFirstPattern(full.ptr);
  ompt.destroy(full.ptr);
  if (ref.length === 0) { console.log(name.padEnd(26), 'no audio'); continue; }

  const firstPat = m.order[0];
  const needed = samplesInPattern(buf, m, firstPat);

  // build repacked prefix: zero out sample data of NON-needed samples
  const repacked = Buffer.from(buf);
  let transferred = m.smpDataStart;           // manifest = headers + patterns + order
  for (let i = 0; i < 31; i++) {
    if (needed.has(i)) { transferred += m.range[i].len; continue; }
    repacked.fill(0, m.range[i].start, m.range[i].start + m.range[i].len);
  }

  const rep = ompt.create(repacked);
  const repAudio = ompt.renderFirstPattern(rep.ptr);
  ompt.destroy(rep.ptr);
  const simRepack = ompt.similarity(ref, repAudio).sim;

  // control: same byte budget but ORIGINAL order (truncate to `transferred`)
  const orig = ompt.create(buf.subarray(0, transferred));
  const origAudio = orig.ptr ? ompt.renderFirstPattern(orig.ptr) : new Float32Array(0);
  ompt.destroy(orig.ptr);
  const simOrig = ompt.similarity(ref, origAudio).sim;

  const ttfpbOrig = (buf.length / BW) * 1000 + full.ms;   // orig order needed ~full file for correct opening (see ttfpb.mjs)
  const ttfpbRepack = (transferred / BW) * 1000 + rep.ms;

  console.log(
    name.slice(0, 25).padEnd(26),
    ((buf.length / 1024) | 0 + 'K').toString().padStart(5) + 'K',
    String(m.ch).padStart(3),
    `${needed.size}/${31 - m.range.filter(r => r.len === 0).length}`.padStart(13),
    (((transferred / buf.length) * 100).toFixed(1) + '%').padStart(12),
    simRepack.toFixed(3).padStart(12),
    simOrig.toFixed(3).padStart(18),
    `   ${ttfpbOrig.toFixed(0)}ms → ${ttfpbRepack.toFixed(0)}ms`.padStart(8),
  );
}
console.log('-'.repeat(116));
console.log('sim(repack) ≈ 1.0 proves stock libopenmpt plays the opening correctly with only needed samples present.');
console.log('sim(origSameBytes) << 1.0 shows the same byte budget in original order is broken → reordering (repack) is the lever.');
