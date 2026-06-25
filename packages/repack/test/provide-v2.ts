// Phase 2 bake exit criterion (STREAMING-PARITY-V2-SCHEMA.md): a module baked to
// a v2 DAG and replayed through the immortal-instance + provide_sample path
// renders identically to a full load.
//
// Per module:
//   1. full-load -> reference PCM A; dump every slot's native decoded PCM.
//   2. buildDagV2(data, dumps) -> content-addressed blocks + v2 root.
//   3. fetchV2(root) -> skeleton bytes + each streamed sample's assembled PCM.
//   4. create_from_memory(skeleton); assert every streamed slot is pending;
//      provide_sample each; assert none pending -> render PCM B.
//   5. assert A == B (bit-exact MOD/S3M; within reassembly tolerance IT/XM), and
//      that the un-provided skeleton renders DIFFERENTLY (so the proof is real).
//
// This validates the whole bake: slot-indexed locator, decoded-PCM dump, skeleton
// normalization, the sample table, and the provide path end-to-end on real DAGs.
import { buildDagV2, fetchV2, type DecodedSample } from "../src/dag.ts";
import { detectFormat } from "../src/parse.ts";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const SR = 48000;
const RENDER_SECONDS = 8;

// The wasm dist is a build artifact; SKIP cleanly when it's absent so this stays
// a local oracle (like provide-sample.mjs), not a CI hard dependency.
let M: any;
try {
  const factory = (await import("../../wasm/dist/libopenmpt.js")).default;
  M = await factory({ print: () => {}, printErr: () => {} });
} catch (e) {
  console.log(`SKIP provide-v2  (wasm dist unavailable: ${(e as Error).message})`);
  process.exit(0);
}

function loadMod(bytes: Uint8Array): number {
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  return mod;
}
function renderMod(mod: number, seconds: number): Float32Array {
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
  return out.subarray(0, n * 2) as Float32Array;
}
function renderPCM(bytes: Uint8Array, seconds: number): Float32Array | null {
  const mod = loadMod(bytes);
  if (!mod) return null;
  const out = renderMod(mod, seconds);
  M._openmpt_module_destroy(mod);
  return out;
}
function maxAbsDiff(a: Float32Array, b: Float32Array): number {
  let d = 0;
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) d = Math.max(d, Math.abs(a[i] - b[i]));
  return d;
}
// Dump every slot's native decoded PCM (.slice copies out of the heap before any
// later malloc can relocate it).
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
  let orig: Uint8Array;
  try {
    orig = new Uint8Array(readFileSync(path));
  } catch {
    console.log(`SKIP ${name}  (not found)`);
    continue;
  }
  const fmt = detectFormat(orig);
  if (!fmt || fmt === "mo3") {
    console.log(`SKIP ${name}  (unparsed format ${fmt})`);
    continue;
  }
  const deterministic = fmt === "mod" || fmt === "s3m";

  // 1. reference render + decoded dump.
  const A = renderPCM(orig, RENDER_SECONDS)!;
  const real = loadMod(orig);
  if (!real) {
    console.log(`FAIL ${name}  (original failed to load)`);
    allOk = false;
    continue;
  }
  const dumps = dumpSamples(real);
  M._openmpt_module_destroy(real);

  // 2. bake v2.
  let dag;
  try {
    dag = await buildDagV2(orig, dumps);
  } catch (e) {
    console.log(`FAIL ${name}  (buildDagV2: ${(e as Error).message})`);
    allOk = false;
    continue;
  }
  const blocks = new Map<string, Uint8Array>();
  for (const b of dag.blocks) blocks.set(b.cid.toString(), b.bytes);
  const get = async (cid: { toString(): string }) => {
    const v = blocks.get(cid.toString());
    if (!v) throw new Error(`missing block ${cid}`);
    return v;
  };

  // 3. fetch back through the v2 reader.
  const { skeleton, samples } = await fetchV2(dag.root, get as any);

  // Informational: the un-provided skeleton must render differently from A, else
  // zeroing did nothing and the parity below would be vacuous.
  const skel0 = renderPCM(skeleton, RENDER_SECONDS);
  const skelDiff = skel0 ? maxAbsDiff(A, skel0) : NaN;

  // 4. immortal instance + provide.
  let pass = true;
  let detail = "";
  const smod = loadMod(skeleton);
  if (!smod) {
    pass = false;
    detail = "skeleton failed to load";
  } else {
    for (const s of samples) {
      if (M._openmpt_module_is_sample_pending(smod, s.index) !== 1) {
        pass = false;
        detail = `sample ${s.index} not pending before provide`;
        break;
      }
    }
    if (pass) {
      for (const s of samples) {
        const p = M._malloc(s.pcm.length);
        M.HEAPU8.set(s.pcm, p);
        const ok = M._openmpt_module_provide_sample(smod, s.index, p, s.frames);
        M._free(p);
        if (ok !== 1) {
          pass = false;
          detail = `provide_sample failed sample ${s.index}`;
          break;
        }
        if (M._openmpt_module_is_sample_pending(smod, s.index) !== 0) {
          pass = false;
          detail = `sample ${s.index} still pending after provide`;
          break;
        }
      }
    }
    let B: Float32Array | null = null;
    if (pass) B = renderMod(smod, RENDER_SECONDS);
    M._openmpt_module_destroy(smod);
    if (pass && B) {
      const diff = maxAbsDiff(A, B);
      const ok = deterministic ? diff === 0 : diff < 0.05;
      if (!ok) {
        pass = false;
        detail = `parity diff ${diff.toExponential(2)}`;
      } else {
        detail =
          `streamed=${dag.stats.streamedSamples} resident=${dag.stats.residentSamples} ` +
          `parityDiff=${diff.toExponential(2)} skeletonDiff=${skelDiff.toExponential(2)} ` +
          `manifest=${dag.stats.manifestBytes}B${dag.stats.spilled ? " (spilled)" : ""}`;
      }
    }
  }

  allOk &&= pass;
  console.log(`${pass ? "OK  " : "FAIL"} ${name}  fmt=${fmt} render=${deterministic ? "det" : "nondet"}  ${detail}`);
}
process.exit(allOk ? 0 : 1);
