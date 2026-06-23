// trackerstream — main-thread controller for the AudioWorklet playback engine.
// Owns the AudioContext + worklet node, exposes reactive ($state) playback state,
// and translates method calls into worklet messages. All synthesis happens on the
// audio thread (player.worklet.ts); this side never touches PCM.

import type { FromWorklet, ModuleInfo, Position, ToWorklet } from "./messages";

const WORKLET_URL = "/player.worklet.js"; // pre-bundled by scripts/build-worklet.mjs
const SAMPLE_RATE = 48000;

export class ModPlayer {
  ready = $state(false);
  loading = $state(false);
  info = $state<ModuleInfo | null>(null);
  pos = $state<Position | null>(null);
  playing = $state(false);
  error = $state<string | null>(null);

  /** Called when a track plays to its end (drives gapless auto-advance). */
  onEnded: (() => void) | null = null;
  volume = $state(1);
  interpolation = $state(8); // 8-tap sinc

  private ctx: AudioContext | null = null;
  private node: AudioWorkletNode | null = null;
  private gain: GainNode | null = null;

  /** Lazily create the audio graph. Must be called from a user gesture. */
  async init(): Promise<void> {
    if (this.ctx) return;
    const ctx = new AudioContext({ sampleRate: SAMPLE_RATE });
    await ctx.audioWorklet.addModule(WORKLET_URL);
    const node = new AudioWorkletNode(ctx, "tracker-processor", {
      numberOfInputs: 0,
      numberOfOutputs: 1,
      outputChannelCount: [2],
    });
    const gain = ctx.createGain();
    gain.gain.value = 1;
    node.connect(gain).connect(ctx.destination);
    node.port.onmessage = (e: MessageEvent<FromWorklet>) => this.onMessage(e.data);
    this.ctx = ctx;
    this.node = node;
    this.gain = gain;
  }

  private onMessage(msg: FromWorklet) {
    switch (msg.type) {
      case "ready":
        this.ready = true;
        break;
      case "loaded":
        this.info = msg.info;
        this.loading = false;
        this.error = null;
        break;
      case "pos":
        this.pos = msg.pos;
        break;
      case "ended":
        this.playing = false;
        this.onEnded?.();
        break;
      case "error":
        this.error = msg.message;
        this.loading = false;
        break;
    }
  }

  private send(msg: ToWorklet, transfer: Transferable[] = []) {
    this.node?.port.postMessage(msg, transfer);
  }

  async load(bytes: ArrayBuffer): Promise<void> {
    await this.init();
    this.loading = true;
    this.info = null;
    this.pos = null;
    this.playing = false;
    // Transfer the buffer to the audio thread (zero-copy).
    this.send({ type: "load", bytes }, [bytes]);
  }

  /** Recreate from a grown buffer mid-playback (streaming), preserving position. */
  reload(bytes: ArrayBuffer): void {
    this.send({ type: "reload", bytes }, [bytes]);
  }

  async play(): Promise<void> {
    await this.init();
    await this.ctx?.resume();
    this.playing = true;
    this.send({ type: "play" });
  }

  pause(): void {
    this.playing = false;
    this.send({ type: "pause" });
  }

  toggle(): void {
    if (this.playing) this.pause();
    else void this.play();
  }

  seekOrderRow(order: number, row: number): void {
    this.send({ type: "seekOrderRow", order, row });
  }

  seekSeconds(seconds: number): void {
    this.send({ type: "seekSeconds", seconds });
  }

  selectSubsong(index: number): void {
    this.send({ type: "selectSubsong", index });
  }

  setInterpolation(value: number): void {
    this.interpolation = value;
    this.send({ type: "setInterpolation", value });
    this.persist();
  }

  setVolume(v: number): void {
    this.volume = v;
    if (this.gain) this.gain.gain.value = v;
    this.persist();
  }

  /** Persist + restore playback settings (volume, interpolation). */
  private persist(): void {
    try {
      localStorage.setItem(
        "ts.settings",
        JSON.stringify({ volume: this.volume, interpolation: this.interpolation }),
      );
    } catch {
      /* no localStorage (e.g. headless) */
    }
  }

  loadSettings(): void {
    try {
      const s = JSON.parse(localStorage.getItem("ts.settings") ?? "{}");
      if (typeof s.volume === "number") this.setVolume(s.volume);
      if (typeof s.interpolation === "number") this.interpolation = s.interpolation;
    } catch {
      /* ignore */
    }
  }
}
