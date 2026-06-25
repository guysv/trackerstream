// Validates the recreate-on-grow CROSSFADE: drive playback, fire a `reload`
// mid-stream (same buffer = deterministic MOD -> new module ≈ old), and confirm
// the splice introduces no click (worst single-sample step stays near steady
// state) and the swap completes. A hard cut would spike the step at the splice.
import { readFileSync } from "node:fs";
const SR = 48000, QUANTUM = 128;
globalThis.sampleRate = SR;
let registered = null;
globalThis.registerProcessor = (_n, c) => (registered = c);
let portForNext = null;
globalThis.AudioWorkletProcessor = class { constructor(){ this.port = portForNext; } };
function pair(){const a={onmessage:null,other:null,postMessage(m){queueMicrotask(()=>this.other.onmessage?.({data:m}));}};const b={onmessage:null,other:null,postMessage(m){queueMicrotask(()=>this.other.onmessage?.({data:m}));}};a.other=b;b.other=a;return[a,b];}
const tick = () => new Promise(r=>setTimeout(r,0));

const [main, wp] = pair(); portForNext = wp;
await import("../static/player.worklet.js");
const proc = new registered();
let ready=false, loaded=0, err=null;
main.onmessage = ({data}) => { if(data.type==="ready")ready=true; else if(data.type==="loaded")loaded++; else if(data.type==="error")err=data.message; };
for (let i=0;i<3000 && !ready && !err;i++) await new Promise(r=>setTimeout(r,1));
if(!ready){ console.log("FAIL engine not ready", err); process.exit(1); }

const bytes = new Uint8Array(readFileSync("static/samples/space_debris_1.mod"));
main.postMessage({ type:"load", bytes: bytes.buffer.slice(0) });
main.postMessage({ type:"play" });
for (let i=0;i<1000 && !loaded;i++) await new Promise(r=>setTimeout(r,1));

const stream = [];
const L = new Float32Array(QUANTUM), R = new Float32Array(QUANTUM);
const RELOAD_Q = 120;
let modBefore=null, modAfter=null;
for (let q=0; q<260; q++) {
  if (q === RELOAD_Q) { modBefore = proc.mod; main.postMessage({ type:"reload", bytes: bytes.buffer.slice(0) }); await tick(); await tick(); }
  L.fill(0); R.fill(0);
  proc.process([], [[L, R]]);
  for (let i=0;i<QUANTUM;i++) stream.push((L[i]+R[i])*0.5);
  if (q % 8 === 0) await tick();
}
modAfter = proc.mod;

// worst single-sample step in steady region vs in the splice/crossfade window
const step = (a,b) => { let m=0; for (let i=a+1;i<b;i++) m=Math.max(m, Math.abs(stream[i]-stream[i-1])); return m; };
const reloadFrame = RELOAD_Q*QUANTUM;
const steady = step(20*QUANTUM, 100*QUANTUM);
const splice = step(reloadFrame-QUANTUM, reloadFrame + 480 + QUANTUM*2); // crossfade ~480 frames
const peakAfter = (()=>{let m=0; for(let i=reloadFrame;i<stream.length;i++) m=Math.max(m,Math.abs(stream[i])); return m;})();

console.log(`steady worst-step=${steady.toFixed(4)}  splice worst-step=${splice.toFixed(4)}  peakAfterReload=${peakAfter.toFixed(3)}`);
console.log(`reloads(loaded msgs)=${loaded}  mod swapped=${modBefore!==modAfter}  pendingCleared=${proc.pendingMod===0}`);
const ok = splice <= Math.max(steady*1.5, 0.02) && peakAfter>0.01 && modBefore!==modAfter && proc.pendingMod===0;
console.log(ok ? "OK  crossfade splice is clean (no click)" : "FAIL  splice step too large or swap incomplete");
process.exit(ok?0:1);
