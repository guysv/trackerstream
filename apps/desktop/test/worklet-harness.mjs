// Headless validation of the BUNDLED worklet (static/player.worklet.js): mock
// the AudioWorkletGlobalScope, load a module, drive process(), and assert PCM
// comes out + position/VU messages flow. Exercises the real shipped artifact
// (bundled libopenmpt + polyfills + processor) without a WebView.
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
        join(homedir(), "tmp/somemods/beyond_the_network.it"),
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

// Minimal MessageChannel-style pair (Node has MessageChannel, but its port
// semantics differ; a tiny direct-dispatch pair keeps this synchronous-ish).
function makePortPair() {
  const a = { onmessage: null, other: null, postMessage(m) { queueMicrotask(() => this.other.onmessage?.({ data: m })); } };
  const b = { onmessage: null, other: null, postMessage(m) { queueMicrotask(() => this.other.onmessage?.({ data: m })); } };
  a.other = b;
  b.other = a;
  return [a, b];
}

async function run(path) {
  const [mainPort, workletPort] = makePortPair();
  portForNextProcessor = workletPort;

  // Importing the IIFE registers the processor in our mocked global scope.
  await import("../static/player.worklet.js");
  const proc = new registered();

  const got = { ready: false, info: null, posCount: 0, ended: false, error: null };
  mainPort.onmessage = ({ data }) => {
    if (data.type === "ready") got.ready = true;
    else if (data.type === "loaded") got.info = data.info;
    else if (data.type === "pos") got.posCount++;
    else if (data.type === "ended") got.ended = true;
    else if (data.type === "error") got.error = data.message;
  };

  // Wait for the libopenmpt factory to resolve ('ready').
  for (let i = 0; i < 2000 && !got.ready && !got.error; i++) await new Promise((r) => setTimeout(r, 1));
  if (!got.ready) return { path, ok: false, why: got.error || "engine never ready" };

  const bytes = new Uint8Array(readFileSync(path));
  mainPort.postMessage({ type: "load", bytes: bytes.buffer });
  mainPort.postMessage({ type: "play" });
  for (let i = 0; i < 1000 && !got.info && !got.error; i++) await new Promise((r) => setTimeout(r, 1));
  if (!got.info) return { path, ok: false, why: got.error || "module never loaded" };

  // Drive ~2 s of audio through process() and measure output.
  const left = new Float32Array(QUANTUM);
  const right = new Float32Array(QUANTUM);
  let peak = 0;
  let frames = 0;
  const quanta = Math.ceil((SR * 2) / QUANTUM);
  for (let q = 0; q < quanta; q++) {
    left.fill(0);
    right.fill(0);
    proc.process([], [[left, right]]);
    for (let i = 0; i < QUANTUM; i++) peak = Math.max(peak, Math.abs(left[i]), Math.abs(right[i]));
    frames += QUANTUM;
    if (q % 8 === 0) await new Promise((r) => setTimeout(r, 0)); // let port messages flush
  }
  await new Promise((r) => setTimeout(r, 5));

  const ok = peak > 0 && got.posCount > 0;
  return {
    path,
    ok,
    why: ok ? "" : `peak=${peak.toFixed(3)} posMsgs=${got.posCount}`,
    info: got.info,
    peak,
    posCount: got.posCount,
  };
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
    console.log(
      `OK   ${name}  type=${r.info.type} ch=${r.info.numChannels} peak=${r.peak.toFixed(3)} posMsgs=${r.posCount}`,
    );
  else console.log(`FAIL ${name}  ${r.why}`);
}
process.exit(allOk ? 0 : 1);
