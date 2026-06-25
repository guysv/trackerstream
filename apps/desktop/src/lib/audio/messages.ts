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

// The v2 planning index the worklet fence needs (subset of repack PlanV2).
export interface PlanData {
  orderSeconds: { order: number; seconds: number }[];
  checkpoints: { order: number; samples: number[] }[];
}

// main -> worklet (v2 immortal-instance protocol).
export type ToWorklet =
  // Create the one immortal instance from the normalized skeleton + plan.
  | { type: "init"; skeleton: ArrayBuffer; plan: PlanData }
  // Patch one sample's decoded PCM into the instance in place (race-free: applied
  // between process() quanta on the audio thread — Phase 1 safety contract).
  | { type: "provideSample"; index: number; frames: number; pcm: ArrayBuffer }
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
  // Fence state: playback is held (active=true) because order `order`'s samples
  // are not yet resident, or resumed (active=false). Drives the buffering UI.
  | { type: "buffering"; active: boolean; order: number }
  | { type: "ended" }
  | { type: "error"; message: string };
