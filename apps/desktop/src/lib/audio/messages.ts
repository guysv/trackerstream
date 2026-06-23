// Message protocol between the main thread (ModPlayer) and the AudioWorklet
// (player.worklet). Kept in one file so both sides stay in sync.

// openmpt render-param ids (libopenmpt openmpt.h).
export const RENDER_MASTERGAIN_MILLIBEL = 1;
export const RENDER_STEREOSEPARATION_PERCENT = 2;
export const RENDER_INTERPOLATIONFILTER_LENGTH = 3;
export const RENDER_VOLUMERAMPING_STRENGTH = 4;

// Desktop-quality defaults (match OpenMPT, not throttled web defaults).
export const SINC_INTERPOLATION = 8; // 8-tap windowed sinc

export interface ModuleInfo {
  type: string;
  durationSeconds: number;
  numChannels: number;
  numOrders: number;
  numPatterns: number;
  numSamples: number;
  numInstruments: number;
  numSubsongs: number;
  sampleRate: number;
}

export interface Position {
  order: number;
  row: number;
  pattern: number;
  seconds: number;
  speed: number;
  tempo: number;
  vu: number[]; // per-channel mono VU, length == numChannels
}

// main -> worklet
export type ToWorklet =
  | { type: "load"; bytes: ArrayBuffer }
  // recreate from a grown buffer, preserving playback position (streaming)
  | { type: "reload"; bytes: ArrayBuffer }
  | { type: "play" }
  | { type: "pause" }
  | { type: "stop" }
  | { type: "seekOrderRow"; order: number; row: number }
  | { type: "seekSeconds"; seconds: number }
  | { type: "selectSubsong"; index: number }
  | { type: "setInterpolation"; value: number }
  | { type: "setRepeat"; count: number };

// worklet -> main
export type FromWorklet =
  | { type: "ready"; version: string }
  | { type: "loaded"; info: ModuleInfo }
  | { type: "pos"; pos: Position }
  | { type: "ended" }
  | { type: "error"; message: string };
