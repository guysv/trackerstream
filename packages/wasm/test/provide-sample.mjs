// Phase 1 exit criterion (STREAMING-PARITY.md): patching a sample's decoded PCM
// in place via openmpt_module_provide_sample is bit-identical to loading the
// module normally.
//
// For each module we:
//   1. render the original full-load -> PCM A,
//   2. dump every sample's native-layout decoded PCM from a real instance
//      (debug_sample_* accessors),
//   3. build a NORMALIZED SKELETON (same module bytes, every sample's PCM
//      zeroed) and create_from_memory it -> all samples report pending,
//   4. provide_sample each dumped buffer back -> no sample pending,
//   5. render -> PCM B, and assert A == B (bit-exact for deterministic MOD/S3M;
//      within the IT/XM random-pan tolerance reassembly.mjs already uses).
//
// Because the skeleton's samples are ZEROED, a no-op provide would render
// silence and fail the A==B assertion -- so parity here is a real proof that
// the memcpy + PrecomputeLoops in provide_sample reconstructs the exact render,
// including correct behaviour across loop points (8 s of audio crosses loops).
import libopenmptFactory from "../dist/libopenmpt.js";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const SR = 48000;
const RENDER_SECONDS = 8;

const M = await libopenmptFactory({ print: () => {}, printErr: () => {} });

// ---- render helpers (same idioms as reassembly.mjs) ------------------------
function loadMod(bytes) {
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  return mod;
}
function renderMod(mod, seconds) {
  M._openmpt_module_set_render_param(mod, 3, 8); // sinc
  const frames = 4096;
  const lp = M._malloc(frames * 4);
  const rp = M._malloc(frames * 4);
  const want = SR * seconds;
  const out = new Float32Array(want * 2);
  let n = 0;
  while (n < want) {
    const got = M._openmpt_module_read_float_stereo(mod, SR, frames, lp, rp);
    if (got <= 0) break;
    const L = M.HEAPF32.subarray(lp / 4, lp / 4 + got);
    const R = M.HEAPF32.subarray(rp / 4, rp / 4 + got);
    for (let i = 0; i < got && n < want; i++, n++) {
      out[n * 2] = L[i];
      out[n * 2 + 1] = R[i];
    }
  }
  M._free(lp);
  M._free(rp);
  return out.subarray(0, n * 2);
}
function renderPCM(bytes, seconds) {
  const mod = loadMod(bytes);
  if (!mod) return null;
  const out = renderMod(mod, seconds);
  M._openmpt_module_destroy(mod);
  return out;
}
function maxAbsDiff(a, b) {
  let d = 0;
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) d = Math.max(d, Math.abs(a[i] - b[i]));
  return d;
}

// Dump every sample's native-layout decoded PCM from a loaded instance. .slice()
// copies out of the WASM heap immediately (the copies survive later mallocs that
// may grow/relocate the heap).
function dumpSamples(mod) {
  const n = M._openmpt_module_get_num_samples(mod);
  const samples = [];
  for (let i = 1; i <= n; i++) {
    const frames = M._openmpt_module_debug_sample_frames(mod, i);
    const bytes = M._openmpt_module_debug_sample_bytes(mod, i);
    const ptr = M._openmpt_module_debug_sample_data(mod, i);
    if (frames > 0 && bytes > 0 && ptr) {
      samples.push({ i, frames, data: M.HEAPU8.slice(ptr, ptr + bytes) });
    }
  }
  return { n, samples };
}

// ---- skeleton normalizer ---------------------------------------------------
// Per-format sample-PCM locators (ported from packages/repack/src/parse.ts).
// Produces a module whose sample PCM is zeroed but which still loads via stock
// create_from_memory, so provide_sample has something to reconstruct.
const u16le = (b, o) => b[o] | (b[o + 1] << 8);
const u16be = (b, o) => (b[o] << 8) | b[o + 1];
const u32le = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;
const tag4 = (b, o) => String.fromCharCode(b[o], b[o + 1], b[o + 2], b[o + 3]);

function modChannels(t) {
  if (t === "M.K." || t === "M!K!" || t === "FLT4" || t === "4CHN") return 4;
  if (t === "6CHN") return 6;
  if (t === "FLT8" || t === "8CHN" || t === "OCTA" || t === "CD81") return 8;
  let m = /^(\d)CHN$/.exec(t);
  if (m) return +m[1];
  m = /^(\d\d)CH$/.exec(t);
  if (m) return +m[1];
  m = /^(\d\d)CN$/.exec(t);
  if (m) return +m[1];
  m = /^TDZ(\d)$/.exec(t);
  if (m) return +m[1];
  return 0;
}

function detectFormat(b) {
  if (b.length >= 4 && tag4(b, 0) === "tpm.") return "it"; // older MPTM magic
  if (b.length >= 0x2c && tag4(b, 0) === "IMPM") return "it"; // IT / newer MPTM
  if (b.length >= 0x30 && tag4(b, 0x2c) === "SCRM") return "s3m";
  if (b.length >= 17 && tag4(b, 0) === "Exte") return "xm";
  if (b.length >= 1084 && modChannels(tag4(b, 1080))) return "mod";
  return null;
}

function modRegions(b) {
  const ch = modChannels(tag4(b, 1080));
  if (!ch) return [];
  const lens = [];
  for (let i = 0; i < 31; i++) lens.push(u16be(b, 20 + i * 30 + 22) * 2);
  const orders = b.subarray(952, 952 + 128);
  let maxPat = 0;
  for (let i = 0; i < 128; i++) if (orders[i] > maxPat) maxPat = orders[i];
  let off = 1084 + (maxPat + 1) * 64 * ch * 4;
  const out = [];
  for (let i = 0; i < 31; i++) {
    const n = lens[i];
    if (n <= 2) continue;
    if (off + n > b.length) {
      out.push({ offset: off, length: b.length - off });
      break;
    }
    out.push({ offset: off, length: n });
    off += n;
  }
  return out;
}

function s3mRegions(b) {
  const ordNum = u16le(b, 0x20);
  const insNum = u16le(b, 0x22);
  const paraBase = 0x60 + ordNum;
  const out = [];
  for (let i = 0; i < insNum; i++) {
    const pp = u16le(b, paraBase + i * 2);
    if (!pp) continue;
    const off = pp * 16;
    if (off + 0x50 > b.length) continue;
    if (b[off] !== 1) continue; // type 1 = PCM
    const hi = b[off + 0x0d];
    const lo = u16le(b, off + 0x0e);
    const ptr = ((hi << 16) | lo) * 16;
    const len = u32le(b, off + 0x10);
    const flg = b[off + 0x1f];
    if (!len || !ptr) continue;
    const bpf = ((flg & 4) ? 2 : 1) * ((flg & 2) ? 2 : 1);
    const bytes = Math.min(len * bpf, b.length - ptr);
    if (bytes > 0 && ptr < b.length) out.push({ offset: ptr, length: bytes });
  }
  return out;
}

function xmRegions(b) {
  const headerSize = u32le(b, 60);
  const npat = u16le(b, 70);
  const nins = u16le(b, 72);
  let pos = 60 + headerSize;
  for (let p = 0; p < npat; p++) {
    if (pos + 9 > b.length) return [];
    const phLen = u32le(b, pos);
    const packed = u16le(b, pos + 7);
    pos += phLen + packed;
  }
  const out = [];
  for (let ins = 0; ins < nins; ins++) {
    if (pos + 29 > b.length) break;
    const instStart = pos;
    const instSize = u32le(b, pos);
    const numSamp = u16le(b, pos + 27);
    if (numSamp === 0) {
      pos = instStart + instSize;
      continue;
    }
    const shSize = u32le(b, pos + 29);
    let hdr = instStart + instSize;
    const lens = [];
    for (let s = 0; s < numSamp; s++) {
      if (hdr + 18 > b.length) return out;
      lens.push(u32le(b, hdr)); // XM length is in BYTES
      hdr += shSize;
    }
    let data = hdr;
    for (let s = 0; s < numSamp; s++) {
      const n = lens[s];
      if (n > 0 && data + n <= b.length) out.push({ offset: data, length: n });
      data += n;
    }
    pos = data;
  }
  return out;
}

// IT/MPTM uncompressed sample-PCM regions (mirrors parse.ts extractITLike). IT
// compression is a load-time concern only: provide_sample operates on already-
// decoded PCM, and the Phase 2 bake stores samples decoded, so compressed-IT
// never reaches the runtime provide path. We don't normalize it here — if a
// module carries IT-compressed samples those slots simply aren't zeroed.
function itRegions(b) {
  const ordNum = u16le(b, 0x20);
  const insNum = u16le(b, 0x22);
  const smpNum = u16le(b, 0x24);
  const base = 0xc0 + ordNum + insNum * 4;
  const out = [];
  for (let i = 0; i < smpNum; i++) {
    const so = u32le(b, base + i * 4);
    if (!so || so + 0x50 > b.length) continue;
    const flg = b[so + 0x12];
    const len = u32le(b, so + 0x30);
    const ptr = u32le(b, so + 0x48);
    if (!(flg & 1) || !len || !ptr) continue;
    if (flg & 8) continue; // compressed: not a raw-PCM region (see note above)
    const bpf = ((flg & 2) ? 2 : 1) * ((flg & 4) ? 2 : 1);
    const bytes = Math.min(len * bpf, b.length - ptr);
    if (bytes > 0) out.push({ offset: ptr, length: bytes });
  }
  return out;
}

function zeroRegions(b, regions) {
  const sk = new Uint8Array(b);
  let count = 0;
  for (const r of regions) {
    if (r.length <= 0 || r.offset < 0 || r.offset + r.length > sk.length) continue;
    sk.fill(0, r.offset, r.offset + r.length);
    count++;
  }
  return { skeleton: sk, zeroed: count };
}

function normalizeSkeleton(b, fmt) {
  switch (fmt) {
    case "mod":
      return zeroRegions(b, modRegions(b));
    case "s3m":
      return zeroRegions(b, s3mRegions(b));
    case "xm":
      return zeroRegions(b, xmRegions(b));
    case "it":
      return zeroRegions(b, itRegions(b));
    default:
      return null;
  }
}

// ---- run -------------------------------------------------------------------
const mods = process.argv.slice(2);
if (mods.length === 0) {
  const d = join(homedir(), "tmp/somemods");
  mods.push(
    join(d, "space_debris (1).mod"),
    join(d, "ELYSIUM.MOD"),
    join(d, "celestial_fantasia.s3m"),
    join(d, "2nd_pm.s3m"),
    join(d, "a-windf.it"),
    join(d, "elw-sick.xm"),
  );
}

let allOk = true;
for (const path of mods) {
  const name = path.split("/").pop();
  let orig;
  try {
    orig = new Uint8Array(readFileSync(path));
  } catch {
    console.log(`SKIP ${name}  (not found)`);
    continue;
  }
  const fmt = detectFormat(orig);
  if (!fmt) {
    console.log(`SKIP ${name}  (unrecognized format)`);
    continue;
  }
  const deterministic = fmt === "mod" || fmt === "s3m";

  // 1. full-load reference render.
  const A = renderPCM(orig, RENDER_SECONDS);

  // 2. dump decoded PCM from a real instance.
  const real = loadMod(orig);
  if (!real) {
    console.log(`FAIL ${name}  (original failed to load)`);
    allOk = false;
    continue;
  }
  const dump = dumpSamples(real);
  M._openmpt_module_destroy(real);

  // 3. normalized skeleton.
  const norm = normalizeSkeleton(orig, fmt);
  if (!norm) {
    console.log(`SKIP ${name}  (no normalizer for ${fmt})`);
    continue;
  }

  // Informational: render the zeroed skeleton WITHOUT providing, to confirm the
  // zeroing actually changed the audio (so the A==B parity below is meaningful).
  const skel0 = renderPCM(norm.skeleton, RENDER_SECONDS);
  const skelDiff = skel0 ? maxAbsDiff(A, skel0) : NaN;

  // 4. load skeleton, assert pending, provide, assert no longer pending.
  let pass = true;
  let detail = "";
  const smod = loadMod(norm.skeleton);
  if (!smod) {
    pass = false;
    detail = "skeleton failed to load";
  } else {
    if (M._openmpt_module_get_num_samples(smod) !== dump.n) {
      pass = false;
      detail = "skeleton sample count mismatch";
    }
    for (const s of dump.samples) {
      if (M._openmpt_module_is_sample_pending(smod, s.i) !== 1) {
        pass = false;
        detail = `sample ${s.i} not pending before provide`;
        break;
      }
    }
    if (pass) {
      for (const s of dump.samples) {
        const p = M._malloc(s.data.length);
        M.HEAPU8.set(s.data, p);
        const ok = M._openmpt_module_provide_sample(smod, s.i, p, s.frames);
        M._free(p);
        if (ok !== 1) {
          pass = false;
          detail = `provide_sample failed sample ${s.i}`;
          break;
        }
        if (M._openmpt_module_is_sample_pending(smod, s.i) !== 0) {
          pass = false;
          detail = `sample ${s.i} still pending after provide`;
          break;
        }
      }
    }
    // 5. render and compare.
    let B = null;
    if (pass) B = renderMod(smod, RENDER_SECONDS);
    M._openmpt_module_destroy(smod);

    if (pass) {
      const diff = maxAbsDiff(A, B);
      const ok = deterministic ? diff === 0 : diff < 0.05;
      if (!ok) {
        pass = false;
        detail = `parity diff ${diff.toExponential(2)}`;
      } else {
        detail =
          `samples=${dump.samples.length} (zeroed=${norm.zeroed}) ` +
          `parityDiff=${diff.toExponential(2)} skeletonDiff=${skelDiff.toExponential(2)}`;
      }
    }
  }

  allOk &&= pass;
  console.log(
    `${pass ? "OK  " : "FAIL"} ${name}  fmt=${fmt} ` +
      `render=${deterministic ? "deterministic" : "nondet"}  ${detail}`,
  );
}
process.exit(allOk ? 0 : 1);
