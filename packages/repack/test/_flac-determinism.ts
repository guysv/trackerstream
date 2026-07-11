// v4 de-risk: prove the FLAC encode is (1) byte-deterministic run-to-run (required for
// cross-module CID dedup + REBUILD idempotency) and (2) losslessly roundtrips the exact
// native PCM libopenmpt hands provide_sample. Uses the flac CLI (pin this version for prod).
import { readFileSync, readdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { homedir } from "node:os";

let M: any = await (await import("../../wasm/dist/libopenmpt.js")).default({ print() {}, printErr() {} });

function dumpSamples(bytes: Uint8Array) {
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  if (!mod) return null;
  const n = M._openmpt_module_get_num_samples(mod);
  const out: { frames: number; data: Uint8Array }[] = [];
  for (let i = 1; i <= n; i++) {
    const frames = M._openmpt_module_debug_sample_frames(mod, i);
    const bn = M._openmpt_module_debug_sample_bytes(mod, i);
    const ptr = M._openmpt_module_debug_sample_data(mod, i);
    if (frames > 0 && bn > 0 && ptr) out.push({ frames, data: M.HEAPU8.slice(ptr, ptr + bn) });
  }
  M._openmpt_module_destroy(mod);
  return out;
}

// We don't have per-sample bitDepth/channels here without the parser; infer a safe
// mono/8-or-16 test by trying both. For the de-risk we just need SOME valid PCM framings:
// treat each sample as mono and pick bps from bytes/frames.
function bpsOf(frames: number, bytes: number): number {
  const r = bytes / frames;
  if (r === 1) return 8;
  if (r === 2) return 16;
  if (r === 4) return 16; // stereo 16 as mono-of-double-frames; still valid PCM bytes
  return 8;
}

function enc(pcm: Uint8Array, bps: number): Buffer {
  return execFileSync(
    "flac",
    ["--silent", "--no-padding", "--no-seektable", "--force-raw-format", "--endian=little",
     "--sign=signed", "--channels=1", `--bps=${bps}`, "--sample-rate=44100", "-8", "-c", "-"],
    { input: Buffer.from(pcm), maxBuffer: 1 << 28 },
  );
}
function dec(flac: Buffer, bps: number): Buffer {
  return execFileSync(
    "flac", ["-d", "--silent", "--force-raw-format", "--endian=little", "--sign=signed", "-c", "-"],
    { input: flac, maxBuffer: 1 << 28 },
  );
}

const DIR = process.argv[2] ?? join(homedir(), "tmp", "somemods");
const files = readdirSync(DIR).filter((f) => !f.startsWith(".")).sort().slice(0, 60);
let n = 0, detOk = 0, detBad = 0, rtOk = 0, rtBad = 0;
for (const f of files) {
  const dumps = dumpSamples(new Uint8Array(readFileSync(join(DIR, f))));
  if (!dumps) continue;
  for (const s of dumps) {
    if (s.data.length % s.frames !== 0) continue;
    const bps = bpsOf(s.frames, s.data.length);
    if (s.data.length % (bps / 8) !== 0) continue;
    const a = enc(s.data, bps);
    const b = enc(s.data, bps); // second encode, same input
    n++;
    (Buffer.compare(a, b) === 0 ? detOk++ : detBad++);
    const back = dec(a, bps);
    (Buffer.compare(back, Buffer.from(s.data)) === 0 ? rtOk++ : rtBad++);
  }
}
console.log(`\nsamples tested: ${n}`);
console.log(`byte-deterministic (encode x2 identical): ${detOk}/${n}  ${detBad ? "*** " + detBad + " NONDETERMINISTIC ***" : "OK"}`);
console.log(`lossless roundtrip (decode == original PCM): ${rtOk}/${n}  ${rtBad ? "*** " + rtBad + " LOSSY ***" : "OK"}`);
console.log(`flac version: ${execFileSync("flac", ["--version"]).toString().trim()}`);
