// Thin wrapper around the libopenmpt wasm build bundled by chiptune3.
// Drives the C API headless from Node to measure the non-fork streaming approach.
import libopenmptFactory from 'chiptune3/libopenmpt.worklet.js';

let M = null;
export async function load() {
  if (M) return M;
  // silence libopenmpt's stderr warnings (truncated files log a lot)
  M = await libopenmptFactory({ print: () => {}, printErr: () => {} });
  return M;
}

// Create a module from a (possibly truncated) byte buffer.
// Returns { ptr, ms } — ptr === 0 means create failed. Caller must destroy().
export function create(bytes) {
  const filePtr = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, filePtr);
  const t0 = process.hrtime.bigint();
  const ptr = M._openmpt_module_create_from_memory(filePtr, bytes.length, 0, 0, 0);
  const t1 = process.hrtime.bigint();
  M._free(filePtr); // create_from_memory copies the data into its own structures
  return { ptr, ms: Number(t1 - t0) / 1e6 };
}

export function destroy(ptr) { if (ptr) M._openmpt_module_destroy(ptr); }

const SR = 48000;
let lP = 0, rP = 0, CAP = 0;
function ensureBufs(frames) {
  if (frames <= CAP) return;
  if (lP) { M._free(lP); M._free(rP); }
  lP = M._malloc(frames * 4); rP = M._malloc(frames * 4); CAP = frames;
}

// Low-level helpers for callers that need to poll position while rendering.
export function ensureBufsExport(frames) { ensureBufs(frames); }
export function curOrder(ptr) { return M._openmpt_module_get_current_order(ptr); }
// Read one chunk (<= frames) as a mono Float32Array; empty when the song ends.
export function readChunk(ptr, frames) {
  ensureBufs(frames);
  const got = M._openmpt_module_read_float_stereo(ptr, SR, frames, lP, rP);
  if (got <= 0) return new Float32Array(0);
  const L = M.HEAPF32.subarray(lP / 4, lP / 4 + got);
  const R = M.HEAPF32.subarray(rP / 4, rP / 4 + got);
  const out = new Float32Array(got);
  for (let i = 0; i < got; i++) out[i] = (L[i] + R[i]) * 0.5;
  return out;
}

// Render the first `seconds` of audio as a mono Float32Array (left+right)/2.
export function render(ptr, seconds) {
  const chunk = 4096; ensureBufs(chunk);
  const want = Math.ceil(SR * seconds);
  const out = new Float32Array(want);
  let n = 0;
  while (n < want) {
    const got = M._openmpt_module_read_float_stereo(ptr, SR, chunk, lP, rP);
    if (got <= 0) break;
    const L = M.HEAPF32.subarray(lP / 4, lP / 4 + got);
    const R = M.HEAPF32.subarray(rP / 4, rP / 4 + got);
    for (let i = 0; i < got && n < want; i++, n++) out[n] = (L[i] + R[i]) * 0.5;
  }
  return out.subarray(0, n);
}

// Render exactly the first pattern: read until the play position leaves order 0.
// Small chunk => tight boundary (less overshoot into the next pattern).
export function renderFirstPattern(ptr, maxSeconds = 20, chunk = 64) {
  ensureBufs(chunk);
  const cap = Math.ceil(SR * maxSeconds);
  const out = new Float32Array(cap);
  let n = 0;
  while (n < cap) {
    if (M._openmpt_module_get_current_order(ptr) > 0) break;
    const got = M._openmpt_module_read_float_stereo(ptr, SR, chunk, lP, rP);
    if (got <= 0) break;
    const L = M.HEAPF32.subarray(lP / 4, lP / 4 + got);
    const R = M.HEAPF32.subarray(rP / 4, rP / 4 + got);
    for (let i = 0; i < got && n < cap; i++, n++) out[n] = (L[i] + R[i]) * 0.5;
  }
  return out.subarray(0, n);
}

// --- seeking ---
// Returns the actual playback position libopenmpt landed on (it snaps to a row).
export function seekSeconds(ptr, seconds) {
  const t0 = process.hrtime.bigint();
  const at = M._openmpt_module_set_position_seconds(ptr, seconds);
  const t1 = process.hrtime.bigint();
  return { at, ms: Number(t1 - t0) / 1e6 };
}
export function seekOrderRow(ptr, order, row) {
  const t0 = process.hrtime.bigint();
  const at = M._openmpt_module_set_position_order_row(ptr, order, row);
  const t1 = process.hrtime.bigint();
  return { at, ms: Number(t1 - t0) / 1e6 };
}
export function pos(ptr) {
  return {
    seconds: M._openmpt_module_get_position_seconds(ptr),
    order: M._openmpt_module_get_current_order(ptr),
    row: M._openmpt_module_get_current_row(ptr),
    pattern: M._openmpt_module_get_current_pattern(ptr),
  };
}

export function meta(ptr, key) {
  const kPtr = M._malloc(key.length + 1);
  for (let i = 0; i < key.length; i++) M.HEAP8[kPtr + i] = key.charCodeAt(i);
  M.HEAP8[kPtr + key.length] = 0;
  const sPtr = M._openmpt_module_get_metadata(ptr, kPtr);
  const s = M.UTF8ToString(sPtr);
  M._openmpt_free_string(sPtr); M._free(kPtr);
  return s;
}

export function info(ptr) {
  return {
    type: meta(ptr, 'type'),
    dur: M._openmpt_module_get_duration_seconds(ptr),
    patterns: M._openmpt_module_get_num_patterns(ptr),
    orders: M._openmpt_module_get_num_orders(ptr),
    samples: M._openmpt_module_get_num_samples(ptr),
    channels: M._openmpt_module_get_num_channels(ptr),
  };
}

// Alignment-tolerant similarity: slide `test` against `ref` within +/-maxLag
// samples and return the best sim. Two capture paths (chunk-granular playthrough
// vs exact-row seek) can differ by a sub-row offset; that phase shift destroys a
// sample-exact RMS diff even when the music is identical, so we search the lag.
export function bestSim(ref, test, maxLag = 512) {
  let best = 0, bestLag = 0;
  for (let lag = -maxLag; lag <= maxLag; lag++) {
    const a = lag >= 0 ? ref.subarray(lag) : ref;
    const b = lag >= 0 ? test : test.subarray(-lag);
    const s = similarity(a, b).sim;
    if (s > best) { best = s; bestLag = lag; }
  }
  return { sim: best, lag: bestLag };
}

// RMS-based similarity of a truncated render vs the full-file reference.
// 1.0 = identical, 0.0 = silent or totally different.
export function similarity(ref, test) {
  const n = Math.min(ref.length, test.length);
  if (n === 0) return { sim: 0, refRms: 0, testRms: 0 };
  let se = 0, re = 0, te = 0;
  for (let i = 0; i < n; i++) {
    const d = test[i] - ref[i];
    se += d * d; re += ref[i] * ref[i]; te += test[i] * test[i];
  }
  const refRms = Math.sqrt(re / n), testRms = Math.sqrt(te / n), diffRms = Math.sqrt(se / n);
  const sim = refRms === 0 ? (testRms === 0 ? 1 : 0) : Math.max(0, 1 - diffRms / refRms);
  return { sim, refRms, testRms };
}
