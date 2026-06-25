// trackerstream — AudioWorkletProcessor that synthesizes tracker modules with
// libopenmpt ON THE AUDIO THREAD, so a busy UI thread can never starve audio
// (the failure mode the reference Mod Archive player warns about).
//
// Bundled by scripts/build-worklet.mjs into a single self-contained classic
// script (WASM inlined) so it loads via audioWorklet.addModule() in any WebView
// without ES-module-worklet support.

import libopenmptFactory, { type LibOpenMPT } from "@trackerstream/wasm/libopenmpt.js";
import {
  RENDER_INTERPOLATIONFILTER_LENGTH,
  SINC_INTERPOLATION,
  type FromWorklet,
  type ModuleInfo,
  type Position,
  type ToWorklet,
} from "./messages";

// Crossfade length (frames) for recreate-on-grow during streaming. Each reload
// destroys the module and recreates it from the larger buffer, then re-seeks to
// the play position — the new module's channel/interpolation state isn't sample-
// aligned with the old one, so a hard cut clicks. We render both across ~10ms and
// equal-power crossfade, masking the discontinuity. Inaudible smear, no pop.
const XFADE_FRAMES = 480; // ~10ms @ 48kHz

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

class TrackerProcessor extends AudioWorkletProcessor {
  private lib: LibOpenMPT | null = null;
  private mod = 0;
  private leftPtr = 0;
  private rightPtr = 0;
  private capFrames = 0;
  private playing = false;
  private numChannels = 0;
  private posCounter = 0;
  private pendingLoad: ArrayBuffer | null = null;
  // Crossfade state for click-free recreate-on-grow (see XFADE_FRAMES).
  private pendingMod = 0; // the freshly-recreated module being faded IN
  private xfadePos = 0; // frames elapsed in the current crossfade
  private xfL: Float32Array | null = null; // JS scratch for the outgoing render
  private xfR: Float32Array | null = null;

  constructor() {
    super();
    this.port.onmessage = (e: MessageEvent<ToWorklet>) => this.onMessage(e.data);
    libopenmptFactory({ print: () => {}, printErr: () => {} })
      .then((lib) => {
        this.lib = lib;
        const v = lib.UTF8ToString(lib._openmpt_get_string(this.cstr(lib, "library_version")));
        this.send({ type: "ready", version: v });
        if (this.pendingLoad) {
          const b = this.pendingLoad;
          this.pendingLoad = null;
          this.load(b, false);
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
      case "load":
        if (!lib) this.pendingLoad = msg.bytes;
        else this.load(msg.bytes, false);
        break;
      case "reload":
        // recreate-on-grow: while playing, crossfade the larger buffer in to
        // avoid a click; otherwise (paused) just swap immediately.
        if (lib && this.mod) {
          if (this.playing) this.beginReload(msg.bytes);
          else this.load(msg.bytes, true);
        }
        break;
      case "play":
        this.playing = this.mod !== 0;
        break;
      case "pause":
        this.playing = false;
        break;
      case "stop":
        this.playing = false;
        if (lib && this.cur()) lib._openmpt_module_set_position_order_row(this.cur(), 0, 0);
        break;
      case "seekOrderRow":
        if (lib && this.cur()) lib._openmpt_module_set_position_order_row(this.cur(), msg.order, msg.row);
        break;
      case "seekSeconds":
        if (lib && this.cur()) lib._openmpt_module_set_position_seconds(this.cur(), msg.seconds);
        break;
      case "selectSubsong":
        if (lib && this.cur()) lib._openmpt_module_select_subsong(this.cur(), msg.index);
        break;
      case "setInterpolation":
        if (lib && this.cur())
          lib._openmpt_module_set_render_param(this.cur(), RENDER_INTERPOLATIONFILTER_LENGTH, msg.value);
        break;
      case "setRepeat":
        if (lib && this.cur()) lib._openmpt_module_set_repeat_count(this.cur(), msg.count);
        break;
    }
  }

  // load(preserve=false) starts fresh; preserve=true keeps the current playback
  // position + playing state (recreate-on-grow during streaming).
  private load(bytes: ArrayBuffer, preserve: boolean) {
    const lib = this.lib!;
    let resumeSeconds = 0;
    if (this.mod) {
      if (preserve) resumeSeconds = lib._openmpt_module_get_position_seconds(this.mod);
      lib._openmpt_module_destroy(this.mod);
      this.mod = 0;
    }
    if (!preserve) this.playing = false;
    const arr = new Uint8Array(bytes);
    const p = lib._malloc(arr.byteLength);
    lib.HEAPU8.set(arr, p);
    const mod = lib._openmpt_module_create_from_memory(p, arr.byteLength, 0, 0, 0);
    lib._free(p);
    if (!mod) {
      this.send({ type: "error", message: "openmpt_module_create_from_memory failed" });
      return;
    }
    this.mod = mod;
    // Desktop-quality render: 8-tap sinc interpolation (volume ramping is on by default).
    lib._openmpt_module_set_render_param(mod, RENDER_INTERPOLATIONFILTER_LENGTH, SINC_INTERPOLATION);
    lib._openmpt_module_set_repeat_count(mod, 0);
    this.numChannels = lib._openmpt_module_get_num_channels(mod);
    if (preserve && resumeSeconds > 0) lib._openmpt_module_set_position_seconds(mod, resumeSeconds);
    this.send({ type: "loaded", info: this.info(mod) });
  }

  // The module that control ops + position should target: the incoming one once
  // a crossfade is in flight (that's where playback is headed), else the current.
  private cur(): number {
    return this.pendingMod || this.mod;
  }

  // Recreate-on-grow WITHOUT a click: build the larger module, seek it to the
  // current play position, and stage it as `pendingMod`. process() then renders
  // both and equal-power crossfades over XFADE_FRAMES before retiring the old one.
  private beginReload(bytes: ArrayBuffer) {
    const lib = this.lib!;
    // If a previous crossfade hasn't finished, retire its old module now so we
    // never juggle three at once (reloads are ~700ms apart, fades ~10ms).
    if (this.pendingMod) {
      lib._openmpt_module_destroy(this.mod);
      this.mod = this.pendingMod;
      this.pendingMod = 0;
    }
    const resumeSeconds = lib._openmpt_module_get_position_seconds(this.mod);
    const arr = new Uint8Array(bytes);
    const p = lib._malloc(arr.byteLength);
    lib.HEAPU8.set(arr, p);
    const mod = lib._openmpt_module_create_from_memory(p, arr.byteLength, 0, 0, 0);
    lib._free(p);
    if (!mod) {
      this.send({ type: "error", message: "openmpt_module_create_from_memory failed" });
      return;
    }
    lib._openmpt_module_set_render_param(mod, RENDER_INTERPOLATIONFILTER_LENGTH, SINC_INTERPOLATION);
    lib._openmpt_module_set_repeat_count(mod, 0);
    if (resumeSeconds > 0) lib._openmpt_module_set_position_seconds(mod, resumeSeconds);
    this.numChannels = Math.max(this.numChannels, lib._openmpt_module_get_num_channels(mod));
    this.pendingMod = mod;
    this.xfadePos = 0;
    this.send({ type: "loaded", info: this.info(mod) });
  }

  // Render `frames` of `mod` into the wasm scratch, copying to the given L/R JS
  // buffers (zero-padded if the module returned fewer). Returns frames produced.
  private renderInto(mod: number, frames: number, l: Float32Array, r: Float32Array): number {
    const lib = this.lib!;
    this.ensureBufs(frames);
    const got = lib._openmpt_module_read_float_stereo(mod, sampleRate, frames, this.leftPtr, this.rightPtr);
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
    const frames = left.length;
    const lib = this.lib;
    if (!lib || !this.mod || !this.playing) {
      left.fill(0);
      if (right !== left) right.fill(0);
      return true;
    }
    if (this.pendingMod) {
      // Crossfade: render outgoing into JS scratch, incoming into the output,
      // then equal-power blend across XFADE_FRAMES. Masks the recreate click.
      if (!this.xfL || this.xfL.length < frames) {
        this.xfL = new Float32Array(frames);
        this.xfR = new Float32Array(frames);
      }
      const gotOld = this.renderInto(this.mod, frames, this.xfL, this.xfR!);
      const gotNew = this.renderInto(this.pendingMod, frames, left, right);
      if (gotNew <= 0) {
        left.fill(0);
        if (right !== left) right.fill(0);
        this.playing = false;
        this.send({ type: "ended" });
        return true;
      }
      for (let i = 0; i < frames; i++) {
        let t = (this.xfadePos + i) / XFADE_FRAMES;
        if (t > 1) t = 1;
        const a = Math.cos((t * Math.PI) / 2); // outgoing gain
        const b = Math.sin((t * Math.PI) / 2); // incoming gain
        const ol = gotOld > 0 ? this.xfL[i] : 0;
        const or = gotOld > 0 ? this.xfR![i] : 0;
        left[i] = ol * a + left[i] * b;
        if (right !== left) right[i] = or * a + right[i] * b;
      }
      this.xfadePos += frames;
      if (this.xfadePos >= XFADE_FRAMES) {
        lib._openmpt_module_destroy(this.mod);
        this.mod = this.pendingMod;
        this.pendingMod = 0;
      }
    } else {
      const got = this.renderInto(this.mod, frames, left, right);
      if (got <= 0) {
        left.fill(0);
        if (right !== left) right.fill(0);
        this.playing = false;
        this.send({ type: "ended" });
        return true;
      }
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
    const mod = this.cur();
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
