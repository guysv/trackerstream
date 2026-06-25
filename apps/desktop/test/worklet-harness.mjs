// Headless validation of the BUNDLED v2 worklet (static/player.worklet.js): mock
// the AudioWorkletGlobalScope, init the immortal instance, drive process(), and
// assert (1) a full module (empty plan) plays, and (2) the fence stalls output to
// silence when a checkpoint needs a sample that was never provided. Exercises the
// real shipped artifact (bundled libopenmpt + polyfills + processor + fence)
// without a WebView.
//
//   node test/worklet-harness.mjs [module-path ...]
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const SR = 48000;
const QUANTUM = 128;

const mods =
  process.argv.slice(2).length > 0
    ? process.argv.slice(2)
    : [
        join(homedir(), "tmp/somemods/a-windf.it"),
        join(homedir(), "tmp/somemods/space_debris (1).mod"),
        join(homedir(), "tmp/somemods/elw-sick.xm"),
        join(homedir(), "tmp/somemods/celestial_fantasia.s3m"),
      ];

// --- mock AudioWorkletGlobalScope ---
globalThis.sampleRate = SR;
let registered = null;
globalThis.registerProcessor = (_name, ctor) => (registered = ctor);
let portForNextProcessor = null;
globalThis.AudioWorkletProcessor = class {
  constructor() {
    this.port = portForNextProcessor;
  }
};

function makePortPair() {
  const a = { onmessage: null, other: null, postMessage(m) { queueMicrotask(() => this.other.onmessage?.({ data: m })); } };
  const b = { onmessage: null, other: null, postMessage(m) { queueMicrotask(() => this.other.onmessage?.({ data: m })); } };
  a.other = b;
  b.other = a;
  return [a, b];
}

const emptyPlan = { orderSeconds: [], checkpoints: [] };

// Drive `seconds` of audio, return peak amplitude + whether any pos arrived.
async function drive(proc, seconds) {
  const left = new Float32Array(QUANTUM);
  const right = new Float32Array(QUANTUM);
  let peak = 0;
  const quanta = Math.ceil((SR * seconds) / QUANTUM);
  for (let q = 0; q < quanta; q++) {
    left.fill(0);
    right.fill(0);
    proc.process([], [[left, right]]);
    for (let i = 0; i < QUANTUM; i++) peak = Math.max(peak, Math.abs(left[i]), Math.abs(right[i]));
    if (q % 8 === 0) await new Promise((r) => setTimeout(r, 0));
  }
  await new Promise((r) => setTimeout(r, 5));
  return peak;
}

async function run(path) {
  const [mainPort, workletPort] = makePortPair();
  portForNextProcessor = workletPort;
  await import("../static/player.worklet.js");
  const proc = new registered();

  const got = { ready: false, info: null, posCount: 0, error: null, buffering: null };
  mainPort.onmessage = ({ data }) => {
    if (data.type === "ready") got.ready = true;
    else if (data.type === "loaded") got.info = data.info;
    else if (data.type === "pos") got.posCount++;
    else if (data.type === "buffering") got.buffering = data.active;
    else if (data.type === "error") got.error = data.message;
  };

  for (let i = 0; i < 2000 && !got.ready && !got.error; i++) await new Promise((r) => setTimeout(r, 1));
  if (!got.ready) return { path, ok: false, why: got.error || "engine never ready" };

  const bytes = new Uint8Array(readFileSync(path));

  // (1) Happy path: full module as skeleton, empty plan -> plays.
  mainPort.postMessage({ type: "init", skeleton: bytes.slice().buffer, plan: emptyPlan });
  mainPort.postMessage({ type: "play" });
  for (let i = 0; i < 1000 && !got.info && !got.error; i++) await new Promise((r) => setTimeout(r, 1));
  if (!got.info) return { path, ok: false, why: got.error || "module never loaded" };
  const peakPlay = await drive(proc, 2);
  if (!(peakPlay > 0 && got.posCount > 0))
    return { path, ok: false, why: `play: peak=${peakPlay.toFixed(3)} pos=${got.posCount}` };

  // (2) Fence: re-init with a checkpoint needing an un-provided sample -> silence.
  got.buffering = null;
  const fencePlan = { orderSeconds: [], checkpoints: [{ order: 0, samples: [9999] }] };
  mainPort.postMessage({ type: "init", skeleton: bytes.slice().buffer, plan: fencePlan });
  mainPort.postMessage({ type: "play" });
  for (let i = 0; i < 1000 && !got.error; i++) await new Promise((r) => setTimeout(r, 1));
  const peakStall = await drive(proc, 0.3);
  if (peakStall !== 0) return { path, ok: false, why: `fence did not stall: peak=${peakStall.toFixed(3)}` };
  if (got.buffering !== true) return { path, ok: false, why: `no buffering event (got ${got.buffering})` };

  return { path, ok: true, info: got.info, peak: peakPlay, posCount: got.posCount };
}

let allOk = true;
for (const m of mods) {
  let r;
  try {
    r = await run(m);
  } catch (e) {
    r = { path: m, ok: false, why: String(e) };
  }
  allOk &&= r.ok;
  const name = m.split("/").pop();
  if (r.ok)
    console.log(`OK   ${name}  type=${r.info.type} ch=${r.info.numChannels} peak=${r.peak.toFixed(3)} pos=${r.posCount} (fence stalls ✓)`);
  else console.log(`FAIL ${name}  ${r.why}`);
}
process.exit(allOk ? 0 : 1);
