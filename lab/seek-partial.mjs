// Headroom check: how much of each NEEDED sample is actually read in the first
// audio window after a cold seek? The minimal full-sample resident set is still
// ~50% of a big IT because live samples are large. But rendering 0.5s only reads
// a slice of each sample (a held one-shot from its play position; a looped sample
// around its loop). If that slice is small, a packer that ships sample BYTE RANGES
// (not whole samples) could cut cold-seek TTFPB far below the full-sample floor.
//
// We measure the read fraction empirically (valid: deterministic neutralized
// render): split each needed sample's PCM into K blocks, zero each block alone,
// and see which blocks change the first-window audio. Union = bytes truly read.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';
await ompt.load();

const DIR = join(homedir(), 'tmp', 'somemods');
const BW = 50e6 / 8, SHORT = 0.5, K = 24;
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
function samples(b, h) {
  const out = [];
  for (let i = 0; i < h.smpNum; i++) {
    const so = u32(b, h.smpOffBase + i * 4), flg = b[so + 0x12], len = u32(b, so + 0x30), ptr = u32(b, so + 0x48);
    const bpf = ((flg & 2) ? 2 : 1) * ((flg & 4) ? 2 : 1);
    const bytes = (flg & 1 && len) ? len * bpf : 0;
    out[i] = { so, ptr, bytes, loops: !!(flg & 0x10) || !!(flg & 0x20) };
  }
  return out;
}
function seekRender(buf, order, secs) { const m = ompt.create(buf); if (!m.ptr) return new Float32Array(0); ompt.seekOrderRow(m.ptr, order, 0); const a = ompt.render(m.ptr, secs); ompt.destroy(m.ptr); return a; }
function bitEq(a, b) { if (a.length !== b.length) return false; for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false; return true; }
function zeroRange(b, off, n) { const c = Buffer.from(b); c.fill(0, off, off + n); return c; }

for (const name of ['bz_pif.it', 'beyond_the_network.it']) {
  const raw = readFileSync(join(DIR, name)); const h0 = header(raw); const buf = neutralize(raw, h0); const h = header(buf);
  const smp = samples(buf, h);
  const validOrders = h.orders.map((o, i) => ({ o, i })).filter(x => x.o < 254).map(x => x.i);
  const picks = [0.3, 0.6, 0.9].map(f => validOrders[Math.round(f * (validOrders.length - 1))]);

  console.log(`\n=== ${name}  ${(buf.length / 1024 | 0)}K ===`);
  console.log('  seek@order   needed   fullBytes   readBytes   read%    TTFPB full→partial');
  for (const N of picks) {
    const ref = seekRender(buf, N, SHORT);
    // minimal needed samples (whole-sample granularity)
    const needed = [];
    for (let i = 0; i < h.smpNum; i++) { if (smp[i].bytes === 0) continue; if (!bitEq(seekRender(zeroRange(buf, smp[i].ptr, smp[i].bytes), N, SHORT), ref)) needed.push(i); }

    // block-probe each needed sample: which blocks are actually read in the window
    let fullBytes = 0, readBytes = 0;
    for (const i of needed) {
      const { ptr, bytes } = smp[i]; fullBytes += bytes;
      const blk = Math.max(1, Math.ceil(bytes / K));
      for (let off = 0; off < bytes; off += blk) {
        const n = Math.min(blk, bytes - off);
        if (!bitEq(seekRender(zeroRange(buf, ptr + off, n), N, SHORT), ref)) readBytes += n; // block is read
      }
    }
    const manifest = buf.length - smp.reduce((a, s) => a + s.bytes, 0);
    const tFull = manifest + fullBytes, tPart = manifest + readBytes;
    console.log(
      `  ${String(N).padStart(8)}   ${String(needed.length).padStart(6)}   ${(fullBytes / 1024 | 0 + 'K').toString().padStart(8)}K   ${(readBytes / 1024 | 0).toString().padStart(8)}K   ${(readBytes / fullBytes * 100).toFixed(0).padStart(4)}%    ${(tFull / BW * 1000).toFixed(0)} → ${(tPart / BW * 1000).toFixed(0)}ms`,
    );
  }
}
console.log('\nread% = fraction of needed-sample PCM actually touched in the first', SHORT, 's. Low => big win from shipping byte ranges.');
