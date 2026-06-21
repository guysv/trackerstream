// Per-sample impact distribution for beyond_the_network's opening.
// "needed at threshold T" = removing the sample drops opening similarity below T.
// Shows whether 47/47 is real or an artifact of an over-sensitive threshold.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';

const DIR = join(homedir(), 'tmp', 'somemods');
const BW = 50e6 / 8;
const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;
function parseIt(b) {
  const ordNum = u16(b, 0x20), insNum = u16(b, 0x22), smpNum = u16(b, 0x24);
  const base = 0xc0 + ordNum + insNum * 4; const s = []; let tot = 0;
  for (let i = 0; i < smpNum; i++) {
    const so = u32(b, base + i * 4), flg = b[so + 0x12], len = u32(b, so + 0x30);
    const has = !!(flg & 1), is16 = !!(flg & 2), st = !!(flg & 4);
    const bytes = (has && len) ? len * (is16 ? 2 : 1) * (st ? 2 : 1) : 0;
    if (bytes) tot += bytes; s.push({ i, so, bytes });
  }
  return { samples: s.filter(x => x.bytes > 0), manifest: b.length - tot };
}
function silence(b, s) { const c = Buffer.from(b); c[s.so + 0x12] &= ~1; c.writeUInt32LE(0, s.so + 0x30); return c; }
function renderW(b, sec) { const { ptr } = ompt.create(b); if (!ptr) return new Float32Array(0); const a = ompt.render(ptr, sec); ompt.destroy(ptr); return a; }

await ompt.load();
const buf = readFileSync(join(DIR, 'beyond_the_network.it'));
const m = parseIt(buf);

for (const W of [2, 4]) {
  const ref = renderW(buf, W);
  const drops = m.samples.map(s => ({ i: s.i, bytes: s.bytes, sim: ompt.similarity(ref, renderW(silence(buf, s), W)).sim }))
    .sort((a, b) => a.sim - b.sim);
  console.log(`\n=== beyond_the_network.it — opening ${W}s, per-sample impact (lower sim = more important) ===`);
  console.log('most impactful 12:', drops.slice(0, 12).map(d => `#${d.i}:${d.sim.toFixed(3)}`).join(' '));
  for (const T of [0.90, 0.95, 0.98, 0.999]) {
    const need = drops.filter(d => d.sim < T);
    const xfer = m.manifest + need.reduce((a, d) => a + d.bytes, 0);
    console.log(`  threshold ${T}:  needed ${String(need.length).padStart(2)}/${m.samples.length}   transferred ${(xfer / buf.length * 100).toFixed(1).padStart(5)}%   TTFPB ${((xfer / BW) * 1000).toFixed(0)}ms`);
  }
}
