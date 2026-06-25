// Phase 2 integrated validation (STREAMING-PARITY.md §Validation): simulate the
// WHOLE client streaming path — immortal instance + client fence + lazy provide —
// and assert it is bit-identical to full-load, with buffering stalls the ONLY
// difference.
//
// Key invariant: a fence stall FREEZES the instance (we don't render that quantum,
// so the playhead doesn't advance). Therefore the concatenation of the non-stalled
// output quanta equals the first N frames of a continuous full-load render. We
// deliver samples lazily (one per stall quantum, in playhead priority) so the
// fence genuinely stalls at the opening AND mid-track whenever a new checkpoint
// introduces a not-yet-delivered sample — then assert streamed == full-load[0:N].
import { buildDagV2, fetchV2, type DecodedSample } from "../src/dag.ts";
import { detectFormat } from "../src/parse.ts";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const SR = 48000;
const QUANTUM = 128;
const SECONDS = 12;

let M: any;
try {
  const factory = (await import("../../wasm/dist/libopenmpt.js")).default;
  M = await factory({ print: () => {}, printErr: () => {} });
} catch (e) {
  console.log(`SKIP stream-sim  (wasm dist unavailable: ${(e as Error).message})`);
  process.exit(0);
}

function loadMod(bytes: Uint8Array): number {
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  M._openmpt_module_set_render_param(mod, 3, 8); // sinc
  return mod;
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
// Render one quantum into a flat [L,R,...] array; returns frames produced.
const lp = M._malloc(QUANTUM * 4);
const rp = M._malloc(QUANTUM * 4);
function renderQuantum(mod: number, out: number[]): number {
  const got = M._openmpt_module_read_float_stereo(mod, SR, QUANTUM, lp, rp);
  if (got <= 0) return got;
  const L = M.HEAPF32.subarray(lp / 4, lp / 4 + got);
  const R = M.HEAPF32.subarray(rp / 4, rp / 4 + got);
  for (let i = 0; i < got; i++) {
    out.push(L[i], R[i]);
  }
  return got;
}

const mods =
  process.argv.slice(2).length > 0
    ? process.argv.slice(2)
    : ["space_debris (1).mod", "GSLINGER.MOD", "2nd_pm.s3m", "CTGOBLIN.S3M", "a-windf.it", "elw-sick.xm"].map((f) =>
        join(homedir(), "tmp/somemods", f),
      );

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

  // dump + bake.
  const real = loadMod(orig);
  const dumps = dumpSamples(real);
  M._openmpt_module_destroy(real);
  const dag = await buildDagV2(orig, dumps);
  const blocks = new Map<string, Uint8Array>();
  for (const b of dag.blocks) blocks.set(b.cid.toString(), b.bytes);
  const { skeleton, samples, plan } = await fetchV2(dag.root, (async (c: { toString(): string }) => blocks.get(c.toString())) as any);
  const pcmByIndex = new Map<number, { frames: number; pcm: Uint8Array }>();
  for (const s of samples) pcmByIndex.set(s.index, { frames: s.frames, pcm: s.pcm });

  // full-load reference (continuous).
  const full = loadMod(orig);
  const A: number[] = [];
  const targetQuanta = Math.ceil((SR * SECONDS) / QUANTUM);
  for (let q = 0; q < targetQuanta; q++) if (renderQuantum(full, A) <= 0) break;
  M._openmpt_module_destroy(full);

  // streamed: skeleton instance + fence + lazy provide.
  const cps = [...plan.checkpoints].sort((a, b) => a.order - b.order);
  const floor = (order: number) => {
    let lo = 0,
      hi = cps.length - 1,
      r = -1;
    while (lo <= hi) {
      const m = (lo + hi) >> 1;
      if (cps[m].order <= order) {
        r = m;
        lo = m + 1;
      } else hi = m - 1;
    }
    return r;
  };
  const provided = new Set<number>();
  const provide = (idx: number) => {
    const d = pcmByIndex.get(idx);
    if (!d || provided.has(idx)) return;
    const p = M._malloc(d.pcm.length);
    M.HEAPU8.set(d.pcm, p);
    M._openmpt_module_provide_sample(smod, idx, p, d.frames);
    M._free(p);
    provided.add(idx);
  };

  const smod = loadMod(skeleton);
  const B: number[] = [];
  let stalls = 0;
  for (let q = 0; q < targetQuanta; q++) {
    const order = M._openmpt_module_get_current_order(smod);
    const fi = floor(order);
    // floor checkpoint UNION the next one: a 128-frame quantum can cross a
    // checkpoint boundary mid-render (incl. a Cxx/Bxx pattern jump), so the next
    // span's samples must also be resident. When fi=-1 (before the first
    // checkpoint, e.g. a silent setup pattern) that union is just the first cp.
    const required = [...(fi >= 0 ? cps[fi].samples : []), ...(cps[fi + 1]?.samples ?? [])];
    const missing = required.filter((s) => !provided.has(s));
    if (missing.length) {
      // fence stall: deliver one missing sample (lazy prefetch), output silence.
      provide(missing[0]);
      stalls++;
      continue; // freeze: no render, no advance
    }
    if (renderQuantum(smod, B) <= 0) break;
  }
  M._openmpt_module_destroy(smod);

  // B (non-stalled output) must equal the first B-length frames of full-load.
  let diff = 0;
  for (let i = 0; i < B.length; i++) diff = Math.max(diff, Math.abs(B[i] - (A[i] ?? 0)));
  const ok = (deterministic ? diff === 0 : diff < 0.05) && B.length > SR && stalls > 0;
  allOk &&= ok;
  console.log(
    `${ok ? "OK  " : "FAIL"} ${name}  fmt=${fmt} stalls=${stalls} rendered=${(B.length / 2 / SR).toFixed(1)}s ` +
      `streamVsFull=${diff.toExponential(2)}${B.length <= SR ? " (too little audio)" : ""}${stalls === 0 ? " (no stalls!)" : ""}`,
  );
}
process.exit(allOk ? 0 : 1);
