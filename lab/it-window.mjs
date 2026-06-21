// Does the "47/47" for beyond_the_network mean slow start, or just a long, rich
// first pattern? Measure needed-samples / transferred-bytes for increasing
// opening windows. Time-to-START playback only needs the first window's samples;
// the rest stream in as the pattern plays.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';

const DIR = join(homedir(), 'tmp', 'somemods');
const SIM_NEEDED = 0.999;
const BW = 50e6 / 8;
const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

function parseIt(b) {
  const ordNum = u16(b, 0x20), insNum = u16(b, 0x22), smpNum = u16(b, 0x24);
  const smpOffBase = 0xc0 + ordNum + insNum * 4;
  const samples = []; let totalData = 0;
  for (let i = 0; i < smpNum; i++) {
    const so = u32(b, smpOffBase + i * 4);
    const flg = b[so + 0x12], len = u32(b, so + 0x30);
    const has = !!(flg & 0x01), is16 = !!(flg & 0x02), stereo = !!(flg & 0x04);
    const bytes = (has && len) ? len * (is16 ? 2 : 1) * (stereo ? 2 : 1) : 0;
    if (bytes) totalData += bytes;
    samples.push({ i, so, bytes });
  }
  return { samples: samples.filter(s => s.bytes > 0), manifest: b.length - totalData };
}
function silence(b, s) { const c = Buffer.from(b); c[s.so + 0x12] &= ~0x01; c.writeUInt32LE(0, s.so + 0x30); return c; }
function renderW(buf, secs) { const { ptr } = ompt.create(buf); if (!ptr) return new Float32Array(0); const a = ompt.render(ptr, secs); ompt.destroy(ptr); return a; }

await ompt.load();
const WINDOWS = [1, 2, 4, 8];

for (const name of ['bz_pif.it', 'beyond_the_network.it']) {
  const buf = readFileSync(join(DIR, name));
  const m = parseIt(buf);
  // full first-pattern duration for context
  const f = ompt.create(buf); const fp = ompt.renderFirstPattern(f.ptr); ompt.destroy(f.ptr);
  const fpSecs = (fp.length / 48000).toFixed(1);
  console.log(`\n=== ${name} (${buf.length/1024|0}K, ${m.samples.length} samples w/ data, first pattern ≈ ${fpSecs}s) ===`);
  console.log('window   needed   transferred   TTFPB@50Mbit  (to START playing that much)');
  for (const W of WINDOWS) {
    const ref = renderW(buf, W);
    if (ref.length === 0) continue;
    const needed = m.samples.filter(s => ompt.similarity(ref, renderW(silence(buf, s), W)).sim < SIM_NEEDED);
    const transferred = m.manifest + needed.reduce((a, s) => a + s.bytes, 0);
    const ttfpb = (transferred / BW) * 1000;
    console.log(
      `${(W + 's').padStart(5)}   ${String(needed.length).padStart(3)}/${m.samples.length}   `,
      ((transferred / buf.length * 100).toFixed(1) + '%').padStart(8),
      `   ${ttfpb.toFixed(0)}ms`.padStart(10),
    );
  }
}
