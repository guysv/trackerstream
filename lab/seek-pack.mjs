// Lab goal: better time-to-playback on a RANDOM seek into a not-yet-buffered module.
//
// Idea (Level 1, no engine fork): at pack time, statically simulate the song to
// learn, for any seek target order N, the RESIDENT sample set = the samples still
// "live" on each channel entering N (last note-on per channel, minus cuts) plus
// the samples triggered in the render window at N. A cold seek then fetches only
// those few samples, calls stock set_position_order_row(N,0), and plays --
// instead of downloading a whole-file prefix.
//
// We verify correctness by building a module that contains ONLY the resident set
// (others silenced), seeking it to (N,0), and comparing to the full module seeked
// to (N,0). Both take the identical engine path, so a complete resident set =>
// bit-identical render. IT random vol/pan is neutralized for determinism.
//
// Then we size: resident-set bytes vs whole file, and cold-seek TTFPB
// (manifest + resident bytes) vs the naive minimal original-order prefix.

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';
await ompt.load();

const DIR = join(homedir(), 'tmp', 'somemods');
const BW = 50e6 / 8;       // 50 Mbit/s, bytes/s
const WIN = 2.0;           // seconds rendered at the seek target for verification
const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

function header(b) {
  const ordNum = u16(b, 0x20), insNum = u16(b, 0x22), smpNum = u16(b, 0x24), cmwt = u16(b, 0x2a);
  const insOffBase = 0xc0 + ordNum, smpOffBase = insOffBase + insNum * 4, patOffBase = smpOffBase + smpNum * 4;
  return { ordNum, insNum, smpNum, cmwt, insOffBase, smpOffBase, patOffBase, orders: [...b.subarray(0xc0, 0xc0 + ordNum)] };
}
function neutralize(b, h) {
  const c = Buffer.from(b);
  if (h.cmwt < 0x200) return c;
  for (let i = 0; i < h.insNum; i++) { const off = u32(c, h.insOffBase + i * 4); if (off) { c[off + 0x1a] = 0; c[off + 0x1b] = 0; } }
  return c;
}
// Decode one packed IT pattern into per-row channel events: [{row, ch, note, ins, cmd, param}].
// note: 0..119 play, 254 cut, 255 off (>=120 are specials). ins resolved via channel memory.
// cmd/param carry the effect column (IT command is 1-based: A=1 set-speed, T=0x14 tempo).
function decodeEvents(b, off) {
  if (off === 0) return { rows: 64, events: [] };
  const len = u16(b, off), rows = u16(b, off + 2);
  let p = off + 8; const end = p + len;
  const lastMask = new Array(64).fill(0), lastIns = new Array(64).fill(0), lastNote = new Array(64).fill(0);
  const events = []; let row = 0;
  while (p < end && row < rows) {
    const cv = b[p++]; if (cv === 0) { row++; continue; }
    const ch = (cv - 1) & 63; let mask = lastMask[ch];
    if (cv & 128) { mask = b[p++]; lastMask[ch] = mask; }
    let note = null, cmd = 0, param = 0;
    if (mask & 1) { note = b[p++]; lastNote[ch] = note; }
    if (mask & 2) { lastIns[ch] = b[p++]; }
    if (mask & 4) p++;
    if (mask & 8) { cmd = b[p++]; param = b[p++]; }
    if (mask & 16) note = lastNote[ch];
    if (note !== null || cmd) events.push({ row, ch, note, ins: lastIns[ch], cmd, param });
  }
  return { rows, events };
}
function sampleForNote(b, h, ins, note) {
  if (ins <= 0 || note < 0 || note > 119) return -1;
  const off = u32(b, h.insOffBase + (ins - 1) * 4); if (off === 0) return -1;
  const map = off + (h.cmwt >= 0x200 ? 0x40 : 0x2e);
  const s = b[map + note * 2 + 1];
  return s > 0 ? s - 1 : -1;
}
function sampleSizes(b, h) {
  const sz = []; let total = 0;
  for (let i = 0; i < h.smpNum; i++) {
    const so = u32(b, h.smpOffBase + i * 4), flg = b[so + 0x12], len = u32(b, so + 0x30);
    const c5 = u32(b, so + 0x3c) || 8363;       // C5 playback rate (samples/s at note C-5)
    const loops = !!(flg & 0x10) || !!(flg & 0x20); // normal or sustain loop => rings while held
    const bytes = (flg & 1 && len) ? len * ((flg & 2) ? 2 : 1) * ((flg & 4) ? 2 : 1) : 0;
    sz[i] = { so, bytes, len, c5, loops };
    total += bytes;
  }
  return { sz, total };
}
// Playback duration (s) of a one-shot sample played at `note` (IT middle = C-5 = 60).
function sampleSeconds(s, note) {
  if (!s || s.len === 0) return 0;
  const rate = s.c5 * Math.pow(2, (note - 60) / 12);
  return rate > 0 ? s.len / rate : 0;
}
const ROWSEC = (speed, tempo) => speed * 2.5 / tempo; // IT row duration: ticks/row * (2.5/BPM)

// Timing- and duration-aware resident set for a cold seek to order N:
//  - walk 0->N tracking time + per-channel held note (sample, note, onset time),
//    honoring Axx (set speed) / Txx>=0x20 (set tempo);
//  - a held sample is LIVE at N only if it loops, or its one-shot tail hasn't
//    finished (now - onset < duration). Drops "stale" finished notes;
//  - plus the samples actually triggered within the first `winSec` after N.
function residentSet(b, h, patEvents, sz, N, speed0, tempo0, winSec) {
  let speed = speed0, tempo = tempo0, t = 0;
  const held = new Array(64).fill(null); // {sample, note, tOn}
  for (let oi = 0; oi < N; oi++) {
    const pat = h.orders[oi]; if (pat >= 254) continue;
    const ev = patEvents[pat]; let ei = 0;
    for (let row = 0; row < ev.rows; row++) {
      while (ei < ev.events.length && ev.events[ei].row === row) {
        const e = ev.events[ei++];
        if (e.cmd === 1 && e.param) speed = e.param;                 // Axx
        else if (e.cmd === 0x14 && e.param >= 0x20) tempo = e.param; // Txx set
        if (e.note === 254) held[e.ch] = null;                      // cut
        else if (e.note !== null && e.note < 120) {
          const s = sampleForNote(b, h, e.ins, e.note);
          if (s >= 0) held[e.ch] = { sample: s, note: e.note, tOn: t };
        }
      }
      t += ROWSEC(speed, tempo);
    }
  }
  const set = new Set();
  for (const hc of held) {
    if (!hc) continue;
    const s = sz[hc.sample];
    if (s.loops || (t - hc.tOn) < sampleSeconds(s, hc.note)) set.add(hc.sample);
  }
  // forward window: triggers within winSec from N
  let tw = 0;
  for (let oi = N; oi < h.ordNum && tw < winSec; oi++) {
    const pat = h.orders[oi]; if (pat >= 254) continue;
    const ev = patEvents[pat]; let ei = 0;
    for (let row = 0; row < ev.rows && tw < winSec; row++) {
      while (ei < ev.events.length && ev.events[ei].row === row) {
        const e = ev.events[ei++];
        if (e.cmd === 1 && e.param) speed = e.param;
        else if (e.cmd === 0x14 && e.param >= 0x20) tempo = e.param;
        if (e.note !== null && e.note < 120) { const s = sampleForNote(b, h, e.ins, e.note); if (s >= 0) set.add(s); }
      }
      tw += ROWSEC(speed, tempo);
    }
  }
  return set;
}
function silence(b, so) { const c = Buffer.from(b); c[so + 0x12] &= ~1; c.writeUInt32LE(0, so + 0x30); return c; }

// Live sample set entering order N: linear walk of the order list, tracking the
// last-triggered sample per channel (cleared on note-cut). Approximation: ignores
// Bxx/SBx jumps; verified empirically below and padded if needed.
function liveSetEntering(b, h, patEvents, targetOrderIdx) {
  const lastSample = new Array(64).fill(-1);
  for (let oi = 0; oi < targetOrderIdx; oi++) {
    const pat = h.orders[oi];
    if (pat >= 254) continue;
    for (const e of patEvents[pat].events) {
      if (e.note === 254) lastSample[e.ch] = -1;            // note cut
      else if (e.note < 120) { const s = sampleForNote(b, h, e.ins, e.note); if (s >= 0) lastSample[e.ch] = s; }
      // note off (255) / fade: keep (release tail still rings)
    }
  }
  const live = new Set();
  for (const s of lastSample) if (s >= 0) live.add(s);
  return live;
}
// Samples triggered within the render window starting at order N (this + next pattern).
function windowSet(b, h, patEvents, targetOrderIdx) {
  const out = new Set();
  for (let oi = targetOrderIdx; oi < Math.min(targetOrderIdx + 2, h.ordNum); oi++) {
    const pat = h.orders[oi]; if (pat >= 254) continue;
    for (const e of patEvents[pat].events) if (e.note < 120) { const s = sampleForNote(b, h, e.ins, e.note); if (s >= 0) out.add(s); }
  }
  return out;
}

function seekRender(buf, order, secs = WIN) { const m = ompt.create(buf); if (!m.ptr) return new Float32Array(0); ompt.seekOrderRow(m.ptr, order, 0); const a = ompt.render(m.ptr, secs); ompt.destroy(m.ptr); return a; }
function bitEq(a, b) { if (a.length !== b.length) return false; for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false; return true; }

const its = readdirSync(DIR).filter(f => /\.it$/i.test(f) && !f.startsWith('.') && statSync(join(DIR, f)).isFile())
  .map(f => ({ name: f, buf: readFileSync(join(DIR, f)) })).sort((a, b) => a.buf.length - b.buf.length);

console.log(`\nRandom-seek packer — resident-set fetch vs naive prefix.  win=${WIN}s bw=50Mbit\n`);

for (const { name, raw } of its.map(x => ({ name: x.name, raw: x.buf }))) {
  const h0 = header(raw); const buf = neutralize(raw, h0); const h = header(buf);
  const patEvents = []; for (let i = 0; i < 256; i++) patEvents[i] = { rows: 64, events: [] };
  const seenPats = new Set(h.orders.filter(o => o < 254));
  for (const pat of seenPats) patEvents[pat] = decodeEvents(buf, u32(buf, h.patOffBase + pat * 4));
  const { sz, total } = sampleSizes(buf, h);
  const manifest = buf.length - total;
  const SR = 48000;

  // choose seek targets spread across the order list (skip end markers)
  const validOrders = h.orders.map((o, i) => ({ o, i })).filter(x => x.o < 254).map(x => x.i);
  const picks = [0.15, 0.3, 0.45, 0.6, 0.75, 0.9].map(f => validOrders[Math.min(validOrders.length - 1, Math.round(f * (validOrders.length - 1)))]);

  const bytesOf = set => [...set].reduce((a, i) => a + (sz[i]?.bytes || 0), 0);
  const SHORT = 0.5;                 // first-audio window for the empirical minimum
  const speed0 = buf[0x32] || 6, tempo0 = buf[0x33] || 125;

  console.log(`=== ${name}  ${(buf.length / 1024 | 0)}K, ${h.smpNum} samples, ${validOrders.length} orders, ${h.cmwt.toString(16)} ===`);
  console.log('  seek@order   crude%   refined(n,%)   minimal(n,%)   over   refined-ok   naive%    TTFPB naive→refined→min');
  let sumCrude = 0, sumRef = 0, sumMin = 0, sumNaive = 0, nOk = 0;
  for (const N of picks) {
    const crudeSet = new Set([...liveSetEntering(buf, h, patEvents, N), ...windowSet(buf, h, patEvents, N)]);
    const refSet = residentSet(buf, h, patEvents, sz, N, speed0, tempo0, SHORT);

    // ground-truth MINIMAL first-audio set (valid: render is deterministic).
    // A sample is needed iff silencing it alone changes the first SHORT seconds.
    const refShort = seekRender(buf, N, SHORT);
    const minSet = new Set();
    for (let i = 0; i < h.smpNum; i++) {
      if (sz[i].bytes === 0) continue;
      if (!bitEq(seekRender(silence(buf, sz[i].so), N, SHORT), refShort)) minSet.add(i);
    }

    // is the refined set a correct superset for the first-audio window?
    let only = Buffer.from(buf);
    for (let i = 0; i < h.smpNum; i++) if (!refSet.has(i) && sz[i].bytes > 0) only = silence(only, sz[i].so);
    const refOk = bitEq(seekRender(only, N, SHORT), refShort);
    const miss = [...minSet].filter(i => !refSet.has(i)).length; // samples refined wrongly dropped

    const tCrude = manifest + bytesOf(crudeSet), tRef = manifest + bytesOf(refSet), tMin = manifest + bytesOf(minSet);
    // naive baseline: minimal original-order prefix rendering the seek's first audio
    let lo = 0, hi = buf.length, correct = buf.length;
    for (let it = 0; it < 16 && hi - lo > Math.max(1024, buf.length * 0.005); it++) {
      const mid = (lo + hi) >> 1;
      if (bitEq(seekRender(buf.subarray(0, mid), N, SHORT), refShort)) { correct = mid; hi = mid; } else lo = mid;
    }
    const over = refSet.size - [...refSet].filter(i => minSet.has(i)).length; // refined over-fetch count
    const ttNaive = correct / BW * 1000, ttRef = tRef / BW * 1000, ttMin = tMin / BW * 1000;
    sumCrude += tCrude / BW * 1000; sumRef += ttRef; sumMin += ttMin; sumNaive += ttNaive; if (refOk && miss === 0) nOk++;
    console.log(
      `  ${String(N).padStart(8)}    ${(bytesOf(crudeSet) / buf.length * 100).toFixed(0).padStart(3)}%     ${String(refSet.size).padStart(2)} ${(bytesOf(refSet) / buf.length * 100).toFixed(0).padStart(3)}%      ${String(minSet.size).padStart(2)} ${(bytesOf(minSet) / buf.length * 100).toFixed(0).padStart(3)}%    ${String(over).padStart(3)}   ${refOk && miss === 0 ? 'yes' : `MISS ${miss}`.padEnd(6)}    ${(correct / buf.length * 100).toFixed(0).padStart(4)}%    ${ttNaive.toFixed(0)} → ${ttRef.toFixed(0)} → ${ttMin.toFixed(0)}ms`,
    );
  }
  console.log(`  ---- ${nOk}/${picks.length} refined-correct; mean TTFPB naive ${(sumNaive / picks.length).toFixed(0)} → crude ${(sumCrude / picks.length).toFixed(0)} → refined ${(sumRef / picks.length).toFixed(0)} → min ${(sumMin / picks.length).toFixed(0)}ms  (refined ${(sumNaive / sumRef).toFixed(1)}x)\n`);
}
