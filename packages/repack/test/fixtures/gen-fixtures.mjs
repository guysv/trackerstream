// Deterministic generator for tiny, structurally-valid tracker modules used as
// CI fixtures. These are NOT real songs — they are hand-built byte layouts that
// satisfy the real parsers in src/parse.ts (detectFormat / sampleRegions) so the
// DAG build + reassemble + sample-dedup paths get exercised without bundling any
// copyrighted music.
//
// What it emits into this directory (run: `node gen-fixtures.mjs`):
//   fixture-a.mod   4-channel ProTracker "M.K." module, 2 patterns, 3 samples.
//   fixture-b.mod   a second MOD whose FIRST sample's PCM is byte-identical to
//                   fixture-a's first sample (so the chunk + pcm-root CID dedups
//                   across the two modules — the cross-module dedup oracle), but
//                   with different other samples / patterns / names.
//   fixture.it      minimal Impulse Tracker "IMPM" module, 2 samples (8-bit
//                   mono PCM), one of which is byte-identical to the SHARED
//                   sample below too.
//
// Determinism: every PCM byte is produced by a fixed formula (no RNG), so the
// generated files — and therefore their CIDs — are stable across runs/machines.

import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Deterministic PCM generators. Signed 8-bit (MOD/IT default) stored as bytes.
// Non-trivial content (a couple of cycles of a wave) so chunking has real data.
// ---------------------------------------------------------------------------

/** A signed-8-bit sample: `len` bytes of a phase-shifted triangle-ish wave. */
function wave(len, seed) {
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    // Deterministic, period-varying, full-range-ish signal.
    const tri = ((i * (3 + (seed % 5))) % 256) - 128;
    const v = (tri + seed * 7 + ((i >> 3) * 11)) & 0xff;
    out[i] = v;
  }
  return out;
}

// The SHARED sample payload that appears, byte-identical, in fixture-a.mod,
// fixture-b.mod, and fixture.it — this is what the dedup oracle asserts on.
// Length chosen to be a clean, small power-of-two (single CDC chunk; the dedup
// happens at the chunk + pcm-root CID level regardless of chunk count).
const SHARED = wave(2048, 1);

// ---------------------------------------------------------------------------
// ProTracker .mod builder (4 channels, "M.K.").
//   layout: 20 (title) + 31*30 (sample headers) + 1 (songlen) + 1 (restart)
//           + 128 (orders) + 4 ("M.K.")  = 1084, then pattern data, then PCM.
//   parser (extractMOD): sample length words at 20+i*30+22 (u16be, *2 = bytes),
//   orders at 952..1080, PCM starts at 1084 + (maxPat+1)*64*ch*4.
// ---------------------------------------------------------------------------

function buildMod({ title, samples, numPatterns }) {
  const CH = 4;
  const headerLen = 1084; // through the "M.K." tag
  const patternBytes = numPatterns * 64 * CH * 4;
  let pcmLen = 0;
  for (const s of samples) pcmLen += s.length;
  const total = headerLen + patternBytes + pcmLen;
  const b = new Uint8Array(total);

  // Title (20 bytes, NUL-padded).
  for (let i = 0; i < Math.min(20, title.length); i++) b[i] = title.charCodeAt(i) & 0x7f;

  // 31 sample headers, 30 bytes each starting at offset 20.
  for (let i = 0; i < 31; i++) {
    const base = 20 + i * 30;
    const len = i < samples.length ? samples[i].length : 0;
    const words = len >>> 1; // length is in 16-bit words
    // name: 22 bytes (leave mostly zero; a couple of chars so it's non-empty)
    b[base + 0] = "S".charCodeAt(0);
    b[base + 1] = "0".charCodeAt(0) + (i % 10);
    // length (u16be, in words) at +22
    b[base + 22] = (words >> 8) & 0xff;
    b[base + 23] = words & 0xff;
    // finetune (+24), volume (+25)
    b[base + 24] = 0;
    b[base + 25] = 64;
    // repeat offset (+26 u16be) / repeat length (+28 u16be word, min 1)
    b[base + 26] = 0;
    b[base + 27] = 0;
    b[base + 28] = 0;
    b[base + 29] = 1;
  }

  // Song length (offset 950), restart (951).
  b[950] = numPatterns;
  b[951] = 0x7f;

  // Order table (128 bytes at 952): play patterns 0..numPatterns-1.
  for (let i = 0; i < 128; i++) b[952 + i] = i < numPatterns ? i : 0;
  // maxPat seen by the parser = numPatterns-1, so it allocates exactly our
  // patternBytes — keeping the PCM offset aligned with our layout.

  // "M.K." tag at 1080.
  b[1080] = "M".charCodeAt(0);
  b[1081] = ".".charCodeAt(0);
  b[1082] = "K".charCodeAt(0);
  b[1083] = ".".charCodeAt(0);

  // Pattern data (offsets 1084 .. 1084+patternBytes): a few non-zero notes so
  // it's not all silence (purely cosmetic for the byte-round-trip).
  for (let p = 0; p < numPatterns; p++) {
    const pb = 1084 + p * 64 * CH * 4;
    for (let row = 0; row < 8; row++) {
      const cell = pb + row * CH * 4;
      // one note in channel 0: sample number low nibble, a period value
      b[cell + 0] = 0x10; // sample hi nibble | period hi
      b[cell + 1] = 0x90 + ((row + p) & 0x3f); // period lo
      b[cell + 2] = 0x10; // sample lo nibble | effect
    }
  }

  // Sample PCM, concatenated in order, starting right after pattern data.
  let off = 1084 + patternBytes;
  for (const s of samples) {
    b.set(s, off);
    off += s.length;
  }
  return b;
}

// ---------------------------------------------------------------------------
// Impulse Tracker .it builder (minimal, uncompressed 8-bit mono samples).
//   header: "IMPM" + song name(26) + ... OrdNum(0x20) InsNum(0x22) SmpNum(0x24)
//   then at 0xC0: orders(OrdNum) + instrument offsets(InsNum*4)
//                 + sample-header offsets(SmpNum*4).
//   parser (extractIT): smpOffBase = 0xC0 + ordNum + insNum*4; each sample
//   header at the pointed offset has flags(+0x12), length(+0x30 u32, in SAMPLES),
//   data ptr(+0x48 u32). flags&1 => has-sample, flags&2 => 16-bit (we use 8-bit).
// ---------------------------------------------------------------------------

function buildIt({ title, samples }) {
  const SMP_HDR = 0x50; // we lay each sample header out as 0x50 bytes
  const ordNum = 2; // a tiny order list (incl. the 0xFF end marker)
  const insNum = 0;
  const smpNum = samples.length;

  const ordBase = 0xc0;
  const insOffBase = ordBase + ordNum;
  const smpOffBase = insOffBase + insNum * 4;
  const headerEnd = smpOffBase + smpNum * 4;

  // Lay out sample headers then sample data after them.
  const smpHdrOff = [];
  let cur = headerEnd;
  for (let i = 0; i < smpNum; i++) {
    smpHdrOff.push(cur);
    cur += SMP_HDR;
  }
  const dataOff = [];
  for (let i = 0; i < smpNum; i++) {
    dataOff.push(cur);
    cur += samples[i].length;
  }
  const total = cur;
  const b = new Uint8Array(total);

  // "IMPM"
  b.set([0x49, 0x4d, 0x50, 0x4d], 0);
  // Song name (26 bytes at 0x04).
  for (let i = 0; i < Math.min(25, title.length); i++) b[0x04 + i] = title.charCodeAt(i) & 0x7f;
  // OrdNum / InsNum / SmpNum / PatNum (u16le).
  const w16 = (o, v) => {
    b[o] = v & 0xff;
    b[o + 1] = (v >> 8) & 0xff;
  };
  w16(0x20, ordNum);
  w16(0x22, insNum);
  w16(0x24, smpNum);
  w16(0x26, 0); // PatNum = 0
  b[0x28] = 0x14; // Cwt/v hi
  b[0x29] = 0x02;
  b[0x2a] = 0x14; // Cmwt
  b[0x2b] = 0x02;
  b[0x30] = 128; // global volume
  b[0x31] = 48; // mix volume
  b[0x32] = 6; // initial speed
  b[0x33] = 125; // initial tempo

  // Orders: pattern 0, then 0xFF end marker.
  b[ordBase + 0] = 0;
  b[ordBase + 1] = 0xff;

  const w32 = (o, v) => {
    b[o] = v & 0xff;
    b[o + 1] = (v >> 8) & 0xff;
    b[o + 2] = (v >> 16) & 0xff;
    b[o + 3] = (v >> 24) & 0xff;
  };

  // Sample-header offset table.
  for (let i = 0; i < smpNum; i++) w32(smpOffBase + i * 4, smpHdrOff[i]);

  // Sample headers + data.
  for (let i = 0; i < smpNum; i++) {
    const ho = smpHdrOff[i];
    b.set([0x49, 0x4d, 0x50, 0x53], ho); // "IMPS"
    b[ho + 0x11] = 64; // global volume
    b[ho + 0x12] = 0x01; // flags: bit0 = sample associated (8-bit, mono, uncompressed)
    b[ho + 0x13] = 64; // default volume
    w32(ho + 0x30, samples[i].length); // length in SAMPLES (8-bit => 1 byte each)
    w32(ho + 0x34, 0); // loop begin
    w32(ho + 0x38, 0); // loop end
    w32(ho + 0x3c, 8363); // C5 speed
    w32(ho + 0x48, dataOff[i]); // sample data pointer
    b.set(samples[i], dataOff[i]);
  }
  return b;
}

// ---------------------------------------------------------------------------
// Build + write the fixtures.
// ---------------------------------------------------------------------------

const modA = buildMod({
  title: "TS FIXTURE A",
  numPatterns: 2,
  samples: [SHARED, wave(1500, 9), wave(900, 17)],
});

const modB = buildMod({
  title: "TS FIXTURE B",
  numPatterns: 1,
  // First sample is the SHARED payload (dedup target); the rest differ from A.
  samples: [SHARED, wave(2200, 33), wave(1024, 41)],
});

const it = buildIt({
  title: "TS FIXTURE IT",
  // Second sample is the SHARED payload too (cross-format dedup of the chunk).
  samples: [wave(1200, 55), SHARED],
});

const out = [
  ["fixture-a.mod", modA],
  ["fixture-b.mod", modB],
  ["fixture.it", it],
];

for (const [name, bytes] of out) {
  writeFileSync(join(HERE, name), bytes);
  console.log(`wrote ${name}  ${bytes.length} bytes`);
}

console.log(`shared sample: ${SHARED.length} bytes (deduped across all three)`);
