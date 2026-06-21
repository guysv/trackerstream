// Clean verification: neutralize IT random vol/pan variation (the only per-note
// randomness), which makes libopenmpt's render deterministic, then confirm that the
// statically-detected first-pattern samples reproduce the opening EXACTLY.

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';
await ompt.load();

const DIR = join(homedir(), 'tmp', 'somemods');
const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

function header(b) {
  const ordNum = u16(b, 0x20), insNum = u16(b, 0x22), smpNum = u16(b, 0x24), cmwt = u16(b, 0x2a);
  const insOffBase = 0xc0 + ordNum, smpOffBase = insOffBase + insNum * 4, patOffBase = smpOffBase + smpNum * 4;
  return { ordNum, insNum, smpNum, cmwt, insOffBase, smpOffBase, patOffBase, orders: [...b.subarray(0xc0, 0xc0 + ordNum)] };
}
// zero Random-Volume (0x1A) and Random-Pan (0x1B) on every instrument (new IT format)
function neutralize(b, h) {
  const c = Buffer.from(b);
  if (h.cmwt < 0x200) return c; // old format has no random variation fields
  for (let i = 0; i < h.insNum; i++) {
    const off = u32(c, h.insOffBase + i * 4);
    if (off) { c[off + 0x1a] = 0; c[off + 0x1b] = 0; }
  }
  return c;
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
    let note = null;
    if (mask & 1) { note = b[p++]; lastNote[ch] = note; }
    if (mask & 2) { lastIns[ch] = b[p++]; }   // sets the channel's persistent instrument
    if (mask & 4) p++;
    if (mask & 8) p += 2;
    if (mask & 16) note = lastNote[ch];
    // a played note uses the channel's CURRENT instrument, whether or not the
    // instrument column is present this row (IT note-without-instrument behavior)
    if (note !== null && note < 120) {
      const ins = lastIns[ch];
      if (ins > 0) {
        instruments.add(ins);
        if (!notesByIns.has(ins)) notesByIns.set(ins, new Set());
        notesByIns.get(ins).add(note);
      }
    }
  }
  return { instruments, notesByIns };
}
function samplesForIns(b, h, ins, notes) {
  const off = u32(b, h.insOffBase + (ins - 1) * 4); if (off === 0) return new Set();
  const map = off + (h.cmwt >= 0x200 ? 0x40 : 0x2e), out = new Set();
  for (const n of notes) { if (n >= 0 && n <= 119) { const s = b[map + n * 2 + 1]; if (s > 0) out.add(s - 1); } }
  return out;
}
function sampleSizes(b, h) {
  const sz = []; let total = 0;
  for (let i = 0; i < h.smpNum; i++) {
    const so = u32(b, h.smpOffBase + i * 4), flg = b[so + 0x12], len = u32(b, so + 0x30);
    const bytes = (flg & 1 && len) ? len * ((flg & 2) ? 2 : 1) * ((flg & 4) ? 2 : 1) : 0;
    sz[i] = { so, bytes }; total += bytes;
  }
  return { sz, total };
}
function silence(b, so) { const c = Buffer.from(b); c[so + 0x12] &= ~1; c.writeUInt32LE(0, so + 0x30); return c; }
function renderFP(b) { const m = ompt.create(b); if (!m.ptr) return new Float32Array(0); const a = ompt.renderFirstPattern(m.ptr); ompt.destroy(m.ptr); return a; }

const its = readdirSync(DIR).filter(f => /\.it$/i.test(f) && !f.startsWith('.') && statSync(join(DIR, f)).isFile())
  .map(f => ({ name: f, buf: readFileSync(join(DIR, f)) })).sort((a, b) => a.buf.length - b.buf.length);

console.log('\nClean verification — random vol/pan neutralized → deterministic render.\n');
console.log('file                     cmwt   determinism(full×2)   samples(needed)   sim(only-needed vs full)   verdict');
console.log('-'.repeat(110));

for (const { name, buf } of its) {
  const h = header(buf);
  const nb = neutralize(buf, h);

  // determinism proof on neutralized buffer
  const d1 = renderFP(nb), d2 = renderFP(nb);
  let bitId = d1.length === d2.length; if (bitId) for (let i = 0; i < d1.length; i++) if (d1[i] !== d2[i]) { bitId = false; break; }
  const detSim = ompt.similarity(d1, d2).sim;

  // static needed set
  const firstOrder = h.orders.find(o => o < 254);
  const { instruments, notesByIns } = decodePattern(nb, u32(nb, h.patOffBase + firstOrder * 4));
  const needed = new Set();
  for (const ins of instruments) for (const s of samplesForIns(nb, h, ins, notesByIns.get(ins))) needed.add(s);

  // only-needed (on neutralized buffer)
  const { sz } = sampleSizes(nb, h);
  let only = Buffer.from(nb);
  for (let i = 0; i < h.smpNum; i++) if (!needed.has(i) && sz[i].bytes > 0) only = silence(only, sz[i].so);

  const ref = renderFP(nb);
  const rep = renderFP(only);
  // drop the final ~64-frame sliver that may overshoot into the next pattern
  const cut = Math.min(ref.length, rep.length) - 64;
  const sim = ompt.similarity(ref.subarray(0, cut), rep.subarray(0, cut)).sim;
  const verdict = sim >= 0.9999 ? 'EXACT ✓' : sim >= 0.999 ? 'clean ✓' : 'MISMATCH ✗';

  console.log(
    name.slice(0, 24).padEnd(25),
    ('0x' + h.cmwt.toString(16)).padStart(6),
    `${bitId ? 'BIT-IDENTICAL' : detSim.toFixed(4)}`.padStart(18),
    String(needed.size).padStart(16),
    sim.toFixed(5).padStart(24),
    '  ' + verdict,
  );
}
console.log('-'.repeat(110));
console.log('sim≈1.0 with neutralized randomness proves the statically-detected first-pattern samples reproduce the opening.');
