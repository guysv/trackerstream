// Static analysis of IT pattern data — ground truth for "which instruments/samples
// does the first pattern actually use", independent of any audio rendering.
// Also checks whether libopenmpt rendering is bit-deterministic (if not, the
// similarity-based detection in it-repack.mjs has a noise floor that over-counts).

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';

const DIR = join(homedir(), 'tmp', 'somemods');
const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

function header(b) {
  const ordNum = u16(b, 0x20), insNum = u16(b, 0x22), smpNum = u16(b, 0x24), patNum = u16(b, 0x26);
  const cmwt = u16(b, 0x2a);
  const ordBase = 0xc0;
  const insOffBase = ordBase + ordNum;
  const smpOffBase = insOffBase + insNum * 4;
  const patOffBase = smpOffBase + smpNum * 4;
  const orders = [...b.subarray(ordBase, ordBase + ordNum)];
  return { ordNum, insNum, smpNum, patNum, cmwt, insOffBase, smpOffBase, patOffBase, orders };
}

// decode one packed IT pattern; return { rows, instruments:Set, notesByIns:Map }
function decodePattern(b, off) {
  if (off === 0) return { rows: 64, instruments: new Set(), notesByIns: new Map() };
  const len = u16(b, off), rows = u16(b, off + 2);
  let p = off + 8;
  const end = p + len;
  const lastMask = new Array(64).fill(0);
  const instruments = new Set();
  const notesByIns = new Map();
  let lastIns = new Array(64).fill(0), lastNote = new Array(64).fill(0);
  let row = 0;
  while (p < end && row < rows) {
    const cv = b[p++];
    if (cv === 0) { row++; continue; }
    const ch = (cv - 1) & 63;
    let mask = lastMask[ch];
    if (cv & 128) { mask = b[p++]; lastMask[ch] = mask; }
    let note = null, ins = null;
    if (mask & 1) { note = b[p++]; lastNote[ch] = note; }
    if (mask & 2) { ins = b[p++]; lastIns[ch] = ins; }
    if (mask & 4) p++;            // volume column
    if (mask & 8) p += 2;         // effect + param
    if (mask & 16) note = lastNote[ch];
    if (mask & 32) ins = lastIns[ch];
    // count an instrument as "used" only when a note actually plays it
    const playNote = (mask & 1) || (mask & 16);
    if (ins && playNote && note !== null && note < 120) { // 120..=note off/cut/fade
      instruments.add(ins);
      if (!notesByIns.has(ins)) notesByIns.set(ins, new Set());
      notesByIns.get(ins).add(note);
    }
  }
  return { rows, instruments, notesByIns };
}

// samples an instrument maps for a given set of notes (new IT instrument format)
function samplesForIns(b, h, insNum, notes) {
  const off = u32(b, h.insOffBase + (insNum - 1) * 4);
  if (off === 0) return new Set();
  // new instrument format: note-sample keyboard map at offset 0x40, 120 (note,sample) pairs
  const map = off + 0x40;
  const out = new Set();
  for (const n of notes) {
    if (n < 0 || n > 119) continue;
    const smp = b[map + n * 2 + 1];
    if (smp > 0) out.add(smp - 1); // 0-based sample index
  }
  return out;
}

await ompt.load();

for (const name of ['bz_pif.it', 'beyond_the_network.it']) {
  const b = readFileSync(join(DIR, name));
  const h = header(b);
  const firstOrder = h.orders.find(o => o < 254); // skip +++ / end markers
  const patOff = u32(b, h.patOffBase + firstOrder * 4);
  const { rows, instruments, notesByIns } = decodePattern(b, patOff);

  const samples = new Set();
  for (const ins of instruments) for (const s of samplesForIns(b, h, ins, notesByIns.get(ins))) samples.add(s);

  console.log(`\n=== ${name} ===`);
  console.log(`order[0]=${firstOrder} → pattern at file offset ${patOff}, ${rows} rows`);
  console.log(`instruments used in first pattern: ${instruments.size} / ${h.insNum}  -> [${[...instruments].sort((a,b)=>a-b).join(', ')}]`);
  console.log(`samples those instruments map (for played notes): ${samples.size} / ${h.smpNum}  -> [${[...samples].sort((a,b)=>a-b).join(', ')}]`);

  // determinism check: render the same bytes twice, compare
  const r1 = (() => { const m = ompt.create(b); const a = ompt.render(m.ptr, 4); ompt.destroy(m.ptr); return a; })();
  const r2 = (() => { const m = ompt.create(b); const a = ompt.render(m.ptr, 4); ompt.destroy(m.ptr); return a; })();
  let identical = r1.length === r2.length;
  if (identical) for (let i = 0; i < r1.length; i++) if (r1[i] !== r2[i]) { identical = false; break; }
  console.log(`render determinism (same bytes, two renders): ${identical ? 'BIT-IDENTICAL' : 'NON-DETERMINISTIC'}  sim=${ompt.similarity(r1, r2).sim.toFixed(5)}`);
}
