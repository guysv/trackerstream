// Smoke test: drive the custom libopenmpt build headless from Node to confirm
// the build + exported C API are correct before wiring the AudioWorklet.
import libopenmptFactory from "../dist/libopenmpt.js";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const MODS = process.argv.slice(2);
if (MODS.length === 0) MODS.push(join(homedir(), "tmp/somemods/a-windf.it"));

const M = await libopenmptFactory({ print: () => {}, printErr: () => {} });
console.log("runtime ready; HEAPF32:", !!M.HEAPF32, "ccall:", typeof M.ccall);

// Render-quality constants (openmpt render params).
const RENDER_INTERPOLATIONFILTER_LENGTH = 3; // 8 = sinc (windowed), highest quality
const SR = 48000;

for (const path of MODS) {
  const bytes = new Uint8Array(readFileSync(path));
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  if (!mod) {
    console.log(`FAIL  ${path}: create returned 0`);
    continue;
  }
  M._openmpt_module_set_render_param(mod, RENDER_INTERPOLATIONFILTER_LENGTH, 8);

  // metadata
  const kp = M._malloc(5);
  M.stringToUTF8("type", kp, 5);
  const tp = M._openmpt_module_get_metadata(mod, kp);
  const type = M.UTF8ToString(tp);
  M._openmpt_free_string(tp);
  M._free(kp);

  const dur = M._openmpt_module_get_duration_seconds(mod);
  const ch = M._openmpt_module_get_num_channels(mod);
  const ns = M._openmpt_module_get_num_samples(mod);
  const sub = M._openmpt_module_get_num_subsongs(mod);

  // render ~1s and check we got non-silent audio
  const frames = 4096;
  const lp = M._malloc(frames * 4);
  const rp = M._malloc(frames * 4);
  let total = 0,
    peak = 0;
  for (let i = 0; i < 12; i++) {
    const got = M._openmpt_module_read_float_stereo(mod, SR, frames, lp, rp);
    if (got <= 0) break;
    const L = M.HEAPF32.subarray(lp / 4, lp / 4 + got);
    for (let j = 0; j < got; j++) peak = Math.max(peak, Math.abs(L[j]));
    total += got;
  }
  M._free(lp);
  M._free(rp);
  M._openmpt_module_destroy(mod);

  const ok = total > 0 && peak > 0;
  console.log(
    `${ok ? "OK  " : "FAIL"} ${path.split("/").pop()}  type=${type} dur=${dur.toFixed(1)}s ch=${ch} samples=${ns} subsongs=${sub} peak=${peak.toFixed(3)}`,
  );
}
