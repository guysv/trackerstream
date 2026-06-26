// trackerstream — AudioWorkletProcessor that synthesizes tracker modules with
// libopenmpt ON THE AUDIO THREAD, so a busy UI thread can never starve audio.
//
// v2 immortal-instance design (STREAMING-PARITY.md): ONE libopenmpt instance is
// created from the normalized skeleton and never destroyed. Decoded sample PCM is
// patched in place via provide_sample as it streams in (race-free: applied in
// onMessage, between process() quanta). A client fence (fence.ts) holds playback
// at an order until that order's samples are resident, so a not-yet-arrived
// sample is an honest buffering pause — never a chopped/silent note or a click.
//
// Bundled by scripts/build-worklet.mjs into a single self-contained classic
// script (WASM inlined) so it loads via audioWorklet.addModule() in any WebView.

import libopenmptFactory, { type LibOpenMPT } from "@trackerstream/wasm/libopenmpt.js";
import {
  RENDER_INTERPOLATIONFILTER_LENGTH,
  SINC_INTERPOLATION,
  type FromWorklet,
  type ModuleInfo,
  type PlanData,
  type Position,
  type ToWorklet,
} from "./messages";
import { Fence } from "./fence";

// --- AudioWorkletGlobalScope ambient declarations ---
declare const sampleRate: number;
declare function registerProcessor(name: string, ctor: unknown): void;
declare class AudioWorkletProcessor {
  readonly port: MessagePort;
  constructor();
}

// Polyfill globals the AudioWorkletGlobalScope lacks but the Emscripten runtime
// touches (only defined if missing — harmless in WebViews that already have them).
const g = globalThis as Record<string, unknown>;
if (typeof g.TextDecoder === "undefined") {
  g.TextDecoder = class {
    decode(buf: Uint8Array): string {
      let s = "";
      for (let i = 0; i < buf.length; ) {
        let c = buf[i++];
        if (c < 0x80) s += String.fromCharCode(c);
        else if (c < 0xe0) s += String.fromCharCode(((c & 0x1f) << 6) | (buf[i++] & 0x3f));
        else if (c < 0xf0)
          s += String.fromCharCode(
            ((c & 0x0f) << 12) | ((buf[i++] & 0x3f) << 6) | (buf[i++] & 0x3f),
          );
        else {
          const cp =
            ((c & 0x07) << 18) |
            ((buf[i++] & 0x3f) << 12) |
            ((buf[i++] & 0x3f) << 6) |
            (buf[i++] & 0x3f);
          const o = cp - 0x10000;
          s += String.fromCharCode(0xd800 | (o >> 10), 0xdc00 | (o & 0x3ff));
        }
      }
      return s;
    }
  };
}
if (typeof g.performance === "undefined") g.performance = { now: () => 0 };
if (typeof g.crypto === "undefined") {
  g.crypto = {
    getRandomValues(a: Uint8Array) {
      for (let i = 0; i < a.length; i++) a[i] = (Math.random() * 256) | 0;
      return a;
    },
  };
}

type PendingInit = { skeleton: ArrayBuffer; plan: PlanData };
type PendingProvide = { index: number; frames: number; pcm: ArrayBuffer };

class TrackerProcessor extends AudioWorkletProcessor {
  private lib: LibOpenMPT | null = null;
  private mod = 0; // the ONE immortal instance
  private fence: Fence | null = null;
  private leftPtr = 0;
  private rightPtr = 0;
  private capFrames = 0;
  private playing = false;
  private numChannels = 0;
  private posCounter = 0;
  private wasBuffering = false;
  // Whether we've emitted an authoritative buffering state for the current
  // instance yet. The main thread shows buffering optimistically at track start;
  // if the opening is already resident the fence never stalls, so we must still
  // emit one explicit buffering:false to clear that UI (else it sticks on).
  private bufferingReported = false;
  // Buffered until the wasm runtime / instance exists (Rust sends init before any
  // provideSample, but guard against arrival before libopenmpt finished loading).
  private pendingInit: PendingInit | null = null;
  private pendingProvides: PendingProvide[] = [];

  constructor() {
    super();
    this.port.onmessage = (e: MessageEvent<ToWorklet>) => this.onMessage(e.data);
    libopenmptFactory({ print: () => {}, printErr: () => {} })
      .then((lib) => {
        this.lib = lib;
        const v = lib.UTF8ToString(lib._openmpt_get_string(this.cstr(lib, "library_version")));
        this.send({ type: "ready", version: v });
        if (this.pendingInit) {
          const p = this.pendingInit;
          this.pendingInit = null;
          this.doInit(p.skeleton, p.plan);
        }
      })
      .catch((err) => this.send({ type: "error", message: String(err) }));
  }

  private send(msg: FromWorklet) {
    this.port.postMessage(msg);
  }

  // Allocate a NUL-terminated C string (caller leaks tiny key strings; fine here).
  private cstr(lib: LibOpenMPT, s: string): number {
    const n = lib.lengthBytesUTF8(s) + 1;
    const p = lib._malloc(n);
    lib.stringToUTF8(s, p, n);
    return p;
  }

  private ensureBufs(frames: number) {
    if (!this.lib || frames <= this.capFrames) return;
    if (this.leftPtr) {
      this.lib._free(this.leftPtr);
      this.lib._free(this.rightPtr);
    }
    this.leftPtr = this.lib._malloc(frames * 4);
    this.rightPtr = this.lib._malloc(frames * 4);
    this.capFrames = frames;
  }

  private onMessage(msg: ToWorklet) {
    const lib = this.lib;
    switch (msg.type) {
      case "init":
        if (!lib) this.pendingInit = { skeleton: msg.skeleton, plan: msg.plan };
        else this.doInit(msg.skeleton, msg.plan);
        break;
      case "provideSample":
        if (!lib || !this.mod) this.pendingProvides.push(msg);
        else this.applyProvide(msg.index, msg.frames, msg.pcm);
        break;
      case "play":
        this.playing = this.mod !== 0;
        break;
      case "pause":
        this.playing = false;
        break;
      case "stop":
        this.playing = false;
        if (lib && this.mod) lib._openmpt_module_set_position_order_row(this.mod, 0, 0);
        break;
      case "seekOrderRow":
        if (lib && this.mod) lib._openmpt_module_set_position_order_row(this.mod, msg.order, msg.row);
        break;
      case "seekSeconds":
        if (lib && this.mod) lib._openmpt_module_set_position_seconds(this.mod, msg.seconds);
        break;
      case "selectSubsong":
        if (lib && this.mod) lib._openmpt_module_select_subsong(this.mod, msg.index);
        break;
      case "setInterpolation":
        if (lib && this.mod)
          lib._openmpt_module_set_render_param(this.mod, RENDER_INTERPOLATIONFILTER_LENGTH, msg.value);
        break;
      case "setRepeat":
        if (lib && this.mod) lib._openmpt_module_set_repeat_count(this.mod, msg.count);
        break;
    }
  }

  // Create the one immortal instance from the normalized skeleton. Every streamed
  // sample loads ZEROED (all pending) until provide_sample patches it in.
  private doInit(skeleton: ArrayBuffer, plan: PlanData) {
    const lib = this.lib!;
    if (this.mod) {
      lib._openmpt_module_destroy(this.mod);
      this.mod = 0;
    }
    this.playing = false;
    this.wasBuffering = false;
    this.bufferingReported = false;
    const arr = new Uint8Array(skeleton);
    const p = lib._malloc(arr.byteLength);
    lib.HEAPU8.set(arr, p);
    const mod = lib._openmpt_module_create_from_memory(p, arr.byteLength, 0, 0, 0);
    lib._free(p);
    if (!mod) {
      this.send({ type: "error", message: "openmpt_module_create_from_memory failed (skeleton)" });
      return;
    }
    this.mod = mod;
    lib._openmpt_module_set_render_param(mod, RENDER_INTERPOLATIONFILTER_LENGTH, SINC_INTERPOLATION);
    lib._openmpt_module_set_repeat_count(mod, 0);
    this.numChannels = lib._openmpt_module_get_num_channels(mod);
    this.fence = new Fence(plan);
    // Flush any provideSample that beat init.
    for (const pp of this.pendingProvides) this.applyProvide(pp.index, pp.frames, pp.pcm);
    this.pendingProvides = [];
    this.send({ type: "loaded", info: this.info(mod) });
  }

  // Patch one sample's decoded PCM into the immortal instance in place, and mark
  // it resident in the fence. Safe without a lock: we are on the audio thread,
  // between process() quanta (Phase 1 safety contract).
  private applyProvide(index: number, frames: number, pcm: ArrayBuffer) {
    const lib = this.lib!;
    const arr = new Uint8Array(pcm);
    const p = lib._malloc(arr.byteLength);
    lib.HEAPU8.set(arr, p);
    const ok = lib._openmpt_module_provide_sample(this.mod, index, p, frames);
    lib._free(p);
    if (ok === 1) this.fence?.provide(index);
    else this.send({ type: "error", message: `provide_sample failed (slot ${index})` });
  }

  // Render `frames` of the instance into the wasm scratch, copying to L/R JS
  // buffers (zero-padded if it returned fewer). Returns frames produced.
  private renderInto(frames: number, l: Float32Array, r: Float32Array): number {
    const lib = this.lib!;
    this.ensureBufs(frames);
    const got = lib._openmpt_module_read_float_stereo(this.mod, sampleRate, frames, this.leftPtr, this.rightPtr);
    if (got <= 0) return got;
    const lh = this.leftPtr >> 2;
    const rh = this.rightPtr >> 2;
    l.set(lib.HEAPF32.subarray(lh, lh + got));
    r.set(lib.HEAPF32.subarray(rh, rh + got), 0);
    if (got < frames) {
      l.fill(0, got);
      r.fill(0, got);
    }
    return got;
  }

  private info(mod: number): ModuleInfo {
    const lib = this.lib!;
    const tp = lib._openmpt_module_get_metadata(mod, this.cstr(lib, "type"));
    const type = lib.UTF8ToString(tp);
    lib._openmpt_free_string(tp);
    return {
      type,
      durationSeconds: lib._openmpt_module_get_duration_seconds(mod),
      numChannels: lib._openmpt_module_get_num_channels(mod),
      numOrders: lib._openmpt_module_get_num_orders(mod),
      numPatterns: lib._openmpt_module_get_num_patterns(mod),
      numSamples: lib._openmpt_module_get_num_samples(mod),
      numInstruments: lib._openmpt_module_get_num_instruments(mod),
      numSubsongs: lib._openmpt_module_get_num_subsongs(mod),
      sampleRate,
    };
  }

  process(_inputs: Float32Array[][], outputs: Float32Array[][]): boolean {
    const out = outputs[0];
    const left = out[0];
    const right = out[1] ?? out[0];
    const lib = this.lib;
    if (!lib || !this.mod || !this.playing) {
      left.fill(0);
      if (right !== left) right.fill(0);
      return true;
    }

    // The fence: hold output (silence, no advance) until the current order's
    // samples are resident. Freezing instead of rendering keeps the instance
    // sample-accurate, so resume is seamless — a held note continues, not a flam.
    const order = lib._openmpt_module_get_current_order(this.mod);
    if (this.fence && !this.fence.ready(order)) {
      if (!this.wasBuffering) {
        this.wasBuffering = true;
        this.bufferingReported = true;
        this.send({ type: "buffering", active: true, order });
      }
      left.fill(0);
      if (right !== left) right.fill(0);
      return true;
    }
    // Emit buffering:false when leaving a stall OR on the first playable frame
    // (bufferingReported still false), so the UI's optimistic "buffering" always
    // gets an explicit clear even when the opening never had to stall.
    if (this.wasBuffering || !this.bufferingReported) {
      this.wasBuffering = false;
      this.bufferingReported = true;
      this.send({ type: "buffering", active: false, order });
    }

    const got = this.renderInto(left.length, left, right);
    if (got <= 0) {
      left.fill(0);
      if (right !== left) right.fill(0);
      this.playing = false;
      this.send({ type: "ended" });
      return true;
    }
    // Post position + VU ~45x/s (every ~4 quanta @128 frames/48kHz).
    if (++this.posCounter >= 4) {
      this.posCounter = 0;
      this.send({ type: "pos", pos: this.position() });
    }
    return true;
  }

  private position(): Position {
    const lib = this.lib!;
    const mod = this.mod;
    const vu: number[] = new Array(this.numChannels);
    for (let i = 0; i < this.numChannels; i++)
      vu[i] = lib._openmpt_module_get_current_channel_vu_mono(mod, i);
    return {
      order: lib._openmpt_module_get_current_order(mod),
      row: lib._openmpt_module_get_current_row(mod),
      pattern: lib._openmpt_module_get_current_pattern(mod),
      seconds: lib._openmpt_module_get_position_seconds(mod),
      speed: lib._openmpt_module_get_current_speed(mod),
      tempo: lib._openmpt_module_get_current_tempo2(mod),
      vu,
    };
  }
}

registerProcessor("tracker-processor", TrackerProcessor);
