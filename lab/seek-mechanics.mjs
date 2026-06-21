// Lab: how does libopenmpt seeking actually work?
//
// Mod Archive's player has the WHOLE file in memory, so it just calls
// set_position_seconds / set_position_order_row. Before we can reason about
// seeking in a STREAMED/repacked module (where not all samples are present),
// we need to know what a correct seek requires of the engine:
//
//   Q1 (cost) — does set_position cost grow with the target time? If it does,
//       libopenmpt is *simulating from the start* (replaying pattern/effect
//       state), not jumping. That tells us a correct seek consumes all pattern
//       data up to the target, plus sample data for any notes still ringing.
//   Q2 (accuracy) — is seek sample-accurate? Compare seek-then-render against
//       the same time-slice of a straight full play-through. sim≈1.0 => the
//       engine reconstructs exact state on seek.
//
// Determinism caveat (see FINDINGS.md): IT random vol/pan makes renders
// non-deterministic. We neutralize it (zero instrument bytes 0x1A/0x1B) so the
// A/B comparison is valid; non-IT modules are unaffected.

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';
await ompt.load();

const DIR = join(homedir(), 'tmp', 'somemods');
const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

// neutralize IT random vol/pan so renders are deterministic (no-op for non-IT)
function neutralize(buf) {
  if (buf.length < 4 || String.fromCharCode(buf[0], buf[1], buf[2], buf[3]) !== 'IMPM') return buf;
  const ordNum = u16(buf, 0x20), insNum = u16(buf, 0x22), cmwt = u16(buf, 0x2a);
  if (cmwt < 0x200) return buf;
  const insOffBase = 0xc0 + ordNum;
  const c = Buffer.from(buf);
  for (let i = 0; i < insNum; i++) {
    const off = u32(c, insOffBase + i * 4);
    if (off) { c[off + 0x1a] = 0; c[off + 0x1b] = 0; }
  }
  return c;
}

const SR = 48000;
const WIN = 1.0;   // seconds to render at each seek target for the A/B compare

// Full play-through, recording the EXACT sample offset where each order first
// appears (polled at fine granularity). Aligning on order boundaries removes
// the sub-row seconds-rounding confound that a time-slice comparison suffers.
function playthroughWithOrderOffsets(ptr, dur) {
  const chunk = 64;
  ompt.ensureBufsExport(chunk);
  const cap = Math.ceil(SR * dur) + SR;
  const out = new Float32Array(cap);
  const orderOffset = new Map();
  let n = 0, prevOrder = -1;
  while (n < cap) {
    const o = ompt.curOrder(ptr);
    if (o !== prevOrder) { if (!orderOffset.has(o)) orderOffset.set(o, n); prevOrder = o; }
    const got = ompt.readChunk(ptr, chunk);
    if (got.length === 0) break;
    for (let i = 0; i < got.length && n < cap; i++, n++) out[n] = got[i];
  }
  return { audio: out.subarray(0, n), orderOffset };
}

const files = readdirSync(DIR)
  .filter(f => /\.(it|mod|s3m|xm)$/i.test(f) && !f.startsWith('.') && statSync(join(DIR, f)).isFile())
  .map(f => ({ name: f, buf: neutralize(readFileSync(join(DIR, f))) }))
  .sort((a, b) => a.buf.length - b.buf.length);

console.log('\nSeek mechanics — cost vs target, and seek-vs-playthrough accuracy (order-aligned).\n');
console.log('file                       dur    ord  seek@25%  seek@50%  seek@75%   cost-grows?   accuracy(sim @ ~25/50/75%)');
console.log('-'.repeat(116));

for (const { name, buf } of files) {
  const ref = ompt.create(buf);
  if (!ref.ptr) { console.log(name.padEnd(26), 'create failed'); continue; }
  const dur = ompt.info(ref.ptr).dur;
  const nOrders = ompt.info(ref.ptr).orders;
  if (!(dur > 2) || nOrders < 4) { ompt.destroy(ref.ptr); continue; }

  const { audio: full, orderOffset } = playthroughWithOrderOffsets(ref.ptr, dur);
  ompt.destroy(ref.ptr);

  // target orders at ~25/50/75% of the order list that we have an offset for
  const targetOrders = [0.25, 0.5, 0.75]
    .map(f => Math.round(f * (nOrders - 1)))
    .map(o => { while (o > 0 && !orderOffset.has(o)) o--; return o; });

  const costs = [], sims = [];
  for (const o of targetOrders) {
    const m = ompt.create(buf);
    const { ms } = ompt.seekOrderRow(m.ptr, o, 0);
    costs.push(ms);
    const got = ompt.render(m.ptr, WIN);
    ompt.destroy(m.ptr);
    const start = orderOffset.get(o);
    const slice = full.subarray(start, start + got.length + 1024); // headroom for lag search
    sims.push(ompt.bestSim(slice, got).sim);
  }

  const grows = costs[2] > costs[0] * 1.5 ? 'YES (simulates)' : 'no (jump)';
  console.log(
    name.slice(0, 25).padEnd(26),
    (dur.toFixed(0) + 's').padStart(5),
    String(nOrders).padStart(4),
    ...costs.map(c => (c.toFixed(2) + 'ms').padStart(9)),
    grows.padStart(15),
    '  ' + sims.map(s => s.toFixed(3)).join(' / '),
  );
}
console.log('-'.repeat(116));
console.log('cost grows with target => seek simulates from start (needs pattern data up to target + samples for ringing notes).');
console.log('accuracy sim~1.0 => seek to order:0 is sample-accurate vs a straight play-through reaching that order.');
