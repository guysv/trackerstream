// Phase 2 checkpoint validation (decision 1): a cold seek that provides ONLY
// checkpoint(N)'s samples must render the same as a full-load seek to order N.
//
// This is the test that catches an UNDER-counting checkpoint (the dangerous
// direction — a missing sample plays as silence = a mid-track chop). The
// provide-all parity harness can't see it because it provides everything; here we
// deliberately provide only what the plan claims is resident at N and render the
// window the checkpoint promises to cover (held notes + the 0.5s forward window).
//
// Per module, for each baked checkpoint order N:
//   full:   load original -> set_position_order_row(N,0) -> render WIN seconds.
//   stream: load skeleton  -> provide ONLY checkpoint(N).samples -> seek N -> render.
//   compare. Deterministic (MOD/S3M): max-abs ~0. Nondet (IT/XM): energy ratio
//   near 1 (a missing prominent sample collapses the streamed energy).
import { buildDagV2, fetchV2, type DecodedSample } from "../src/dag.ts";
import { detectFormat } from "../src/parse.ts";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const SR = 48000;
const SPAN_CAP = 15; // cap per-checkpoint render seconds (huge strided spans)
const MAX_TESTS = 12; // checkpoints probed per module

let M: any;
try {
  const factory = (await import("../../wasm/dist/libopenmpt.js")).default;
  M = await factory({ print: () => {}, printErr: () => {} });
} catch (e) {
  console.log(`SKIP seek-diff  (wasm dist unavailable: ${(e as Error).message})`);
  process.exit(0);
}

function loadMod(bytes: Uint8Array): number {
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  return mod;
}
// Render from `startOrder` until the playhead reaches `stopOrder` (the next
// checkpoint) or `maxSec` elapses — small chunks, discarding any chunk that
// crosses into stopOrder so EVERY returned frame is within [startOrder, stopOrder).
// This honors real pattern breaks/jumps instead of a seconds estimate, so the
// test mirrors exactly what the fence covers (checkpoint(N) until the next cp).
function renderRange(mod: number, startOrder: number, stopOrder: number, maxSec: number): Float32Array {
  M._openmpt_module_set_render_param(mod, 3, 8);
  M._openmpt_module_set_position_order_row(mod, startOrder, 0);
  const frames = 256;
  const lp = M._malloc(frames * 4);
  const rp = M._malloc(frames * 4);
  const cap = Math.floor(SR * maxSec);
  const out = new Float32Array(cap * 2);
  let n = 0;
  while (n < cap) {
    const got = M._openmpt_module_read_float_stereo(mod, SR, frames, lp, rp);
    if (got <= 0) break;
    if (M._openmpt_module_get_current_order(mod) >= stopOrder) break; // crossed boundary -> drop chunk
    const L = M.HEAPF32.subarray(lp / 4, lp / 4 + got);
    const R = M.HEAPF32.subarray(rp / 4, rp / 4 + got);
    for (let i = 0; i < got && n < cap; i++, n++) {
      out[n * 2] = L[i];
      out[n * 2 + 1] = R[i];
    }
  }
  M._free(lp);
  M._free(rp);
  return out.subarray(0, n * 2) as Float32Array;
}
function dumpSamples(mod: number): DecodedSample[] {
  const n = M._openmpt_module_get_num_samples(mod);
  const out: DecodedSample[] = [];
  for (let i = 1; i <= n; i++) {
    const frames = M._openmpt_module_debug_sample_frames(mod, i);
    const bytes = M._openmpt_module_debug_sample_bytes(mod, i);
    const ptr = M._openmpt_module_debug_sample_data(mod, i);
    if (frames > 0 && bytes > 0 && ptr) out.push({ index: i, frames, data: M.HEAPU8.slice(ptr, ptr + bytes) });
  }
  return out;
}
function maxAbsDiff(a: Float32Array, b: Float32Array): number {
  let d = 0;
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) d = Math.max(d, Math.abs(a[i] - b[i]));
  return d;
}
function rms(a: Float32Array): number {
  let s = 0;
  for (let i = 0; i < a.length; i++) s += a[i] * a[i];
  return a.length ? Math.sqrt(s / a.length) : 0;
}

const mods = process.argv.slice(2);
if (mods.length === 0) {
  const d = join(homedir(), "tmp/somemods");
  for (const f of ["space_debris (1).mod", "ELYSIUM.MOD", "DOPE.MOD", "GSLINGER.MOD", "celestial_fantasia.s3m", "2nd_pm.s3m", "CTGOBLIN.S3M", "a-windf.it", "bz_pif.it", "elw-sick.xm", "external.xm", "unreeeal_superhero_3.xm"])
    mods.push(join(d, f));
}

let allOk = true;
for (const path of mods) {
  const name = path.split("/").pop();
  let orig: Uint8Array;
  try {
    orig = new Uint8Array(readFileSync(path));
  } catch {
    continue;
  }
  const fmt = detectFormat(orig);
  if (!fmt || fmt === "mo3") continue;
  const deterministic = fmt === "mod" || fmt === "s3m";

  const real = loadMod(orig);
  if (!real) {
    console.log(`FAIL ${name}  (load)`);
    allOk = false;
    continue;
  }
  const dumps = dumpSamples(real);
  M._openmpt_module_destroy(real);

  const dag = await buildDagV2(orig, dumps);
  const blocks = new Map<string, Uint8Array>();
  for (const b of dag.blocks) blocks.set(b.cid.toString(), b.bytes);
  const { skeleton, samples, plan } = await fetchV2(dag.root, (async (cid: { toString(): string }) => {
    const v = blocks.get(cid.toString());
    if (!v) throw new Error("missing");
    return v;
  }) as any);
  const pcmByIndex = new Map<number, { frames: number; pcm: Uint8Array }>();
  for (const s of samples) pcmByIndex.set(s.index, { frames: s.frames, pcm: s.pcm });

  const cps = plan.checkpoints;
  if (!cps.length) {
    console.log(`SKIP ${name}  (no checkpoints, fmt=${fmt})`);
    continue;
  }
  const step = Math.max(1, Math.floor(cps.length / MAX_TESTS));
  let worst = 0;
  let worstWhere = "";
  let pass = true;
  for (let ci = 0; ci < cps.length; ci += step) {
    const cp = cps[ci];
    // Render until the playhead reaches the next checkpoint's order (the exact
    // span checkpoint(N) is the floor for), honoring real pattern breaks/jumps.
    const stopOrder = ci + 1 < cps.length ? cps[ci + 1].order : Number.MAX_SAFE_INTEGER;
    const refMod = loadMod(orig);
    const ref = renderRange(refMod, cp.order, stopOrder, SPAN_CAP);
    M._openmpt_module_destroy(refMod);
    // streamed: skeleton + only this checkpoint's samples.
    const smod = loadMod(skeleton);
    const set = new Set(cp.samples);
    for (const idx of set) {
      const d = pcmByIndex.get(idx);
      if (!d) continue;
      const p = M._malloc(d.pcm.length);
      M.HEAPU8.set(d.pcm, p);
      M._openmpt_module_provide_sample(smod, idx, p, d.frames);
      M._free(p);
    }
    const test = renderRange(smod, cp.order, stopOrder, SPAN_CAP);
    M._openmpt_module_destroy(smod);

    const rr = rms(ref);
    if (deterministic) {
      const d = maxAbsDiff(ref, test);
      if (d > worst) {
        worst = d;
        worstWhere = `order ${cp.order}`;
      }
      if (d > 2e-3) pass = false;
    } else {
      // energy ratio: a missing sample collapses streamed energy.
      const rt = rms(test);
      const ratio = rr > 1e-4 ? rt / rr : 1;
      const dev = Math.abs(1 - ratio);
      if (dev > worst) {
        worst = dev;
        worstWhere = `order ${cp.order} (rmsRef=${rr.toExponential(1)} ratio=${ratio.toFixed(2)})`;
      }
      if (rr > 1e-3 && ratio < 0.5) pass = false; // lost a prominent sample
    }
  }

  allOk &&= pass;
  console.log(
    `${pass ? "OK  " : "FAIL"} ${name}  fmt=${fmt} checkpoints=${cps.length} ` +
      `worst${deterministic ? "MaxDiff" : "RmsDev"}=${worst.toExponential(2)} @ ${worstWhere}`,
  );
}

process.exit(allOk ? 0 : 1);
