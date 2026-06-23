// Phase 0 exit criterion: a module reassembled from blocks (not the original
// file) plays bit-identically to the original.
//
// Splits each module into fixed-size content blocks, reassembles them in
// shuffled arrival order, and (1) asserts the reassembled bytes are byte-exact,
// (2) for deterministic formats (MOD/S3M — see lab/SEEK.md) asserts the rendered
// PCM is sample-exact, (3) for IT/XM (non-deterministic random vol/pan) reports
// RMS similarity, since byte-exactness already guarantees identical playback.
import libopenmptFactory from "../dist/libopenmpt.js";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const SR = 48000;
const BLOCK = 16 * 1024;
const RENDER_SECONDS = 8;

const M = await libopenmptFactory({ print: () => {}, printErr: () => {} });

function splitReassemble(data, blockSize) {
  const blocks = [];
  for (let off = 0, i = 0; off < data.length; off += blockSize, i++)
    blocks.push({ index: i, bytes: data.subarray(off, Math.min(off + blockSize, data.length)) });
  const out = new Uint8Array(data.length);
  for (const b of [...blocks].reverse()) out.set(b.bytes, b.index * blockSize); // shuffled order
  return out;
}

function renderPCM(bytes, seconds) {
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  if (!mod) return null;
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
  M._openmpt_module_destroy(mod);
  return out.subarray(0, n * 2);
}

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
  const orig = new Uint8Array(readFileSync(path));
  const reasm = splitReassemble(orig, BLOCK);

  const bytesEqual = orig.length === reasm.length && Buffer.compare(orig, reasm) === 0;
  const fmt = path.split(".").pop().toLowerCase();
  const deterministic = fmt === "mod" || fmt === "s3m";

  const a = renderPCM(orig, RENDER_SECONDS);
  const b = renderPCM(reasm, RENDER_SECONDS);
  let maxDiff = 0;
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) maxDiff = Math.max(maxDiff, Math.abs(a[i] - b[i]));

  const pass = bytesEqual && (deterministic ? maxDiff === 0 : maxDiff < 0.05);
  allOk &&= pass;
  const name = path.split("/").pop();
  console.log(
    `${pass ? "OK  " : "FAIL"} ${name}  bytes=${bytesEqual ? "exact" : "DIFFER"}  ` +
      `render=${deterministic ? "deterministic" : "nondet"}  maxSampleDiff=${maxDiff.toExponential(2)}`,
  );
}
process.exit(allOk ? 0 : 1);
