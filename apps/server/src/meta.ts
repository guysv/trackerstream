// Module metadata extraction via libopenmpt (the custom WASM build, headless in
// Node). Pulls the catalog/search fields: title, duration, channel/sample/
// instrument counts, subsongs, the instrument+sample name text, and the song
// comment — the Mod Archive search axes.
import libopenmptFactory, { type LibOpenMPT } from "@trackerstream/wasm/libopenmpt.js";

export interface ModuleMeta {
  type: string;
  title: string;
  duration: number;
  channels: number;
  numSamples: number;
  numInstruments: number;
  numSubsongs: number;
  instruments: string;
  comment: string;
}

let M: LibOpenMPT | null = null;

export async function initMeta(): Promise<void> {
  if (!M) M = await libopenmptFactory({ print: () => {}, printErr: () => {} });
}

function cstr(s: string): number {
  const n = M!.lengthBytesUTF8(s) + 1;
  const p = M!._malloc(n);
  M!.stringToUTF8(s, p, n);
  return p;
}

function meta(mod: number, key: string): string {
  const kp = cstr(key);
  const sp = M!._openmpt_module_get_metadata(mod, kp);
  const s = M!.UTF8ToString(sp);
  M!._openmpt_free_string(sp);
  M!._free(kp);
  return s;
}

function name(fn: "sample" | "instrument", mod: number, i: number): string {
  const sp =
    fn === "sample"
      ? M!._openmpt_module_get_sample_name(mod, i)
      : M!._openmpt_module_get_instrument_name(mod, i);
  const s = M!.UTF8ToString(sp);
  M!._openmpt_free_string(sp);
  return s;
}

/** Extract metadata; returns null if libopenmpt can't open the buffer. */
export function extractMeta(bytes: Uint8Array): ModuleMeta | null {
  if (!M) throw new Error("call initMeta() first");
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  if (!mod) return null;
  try {
    const numSamples = M._openmpt_module_get_num_samples(mod);
    const numInstruments = M._openmpt_module_get_num_instruments(mod);
    const names: string[] = [];
    for (let i = 0; i < numInstruments; i++) {
      const n = name("instrument", mod, i).trim();
      if (n) names.push(n);
    }
    for (let i = 0; i < numSamples; i++) {
      const n = name("sample", mod, i).trim();
      if (n) names.push(n);
    }
    return {
      type: meta(mod, "type"),
      title: meta(mod, "title").trim(),
      duration: M._openmpt_module_get_duration_seconds(mod),
      channels: M._openmpt_module_get_num_channels(mod),
      numSamples,
      numInstruments,
      numSubsongs: M._openmpt_module_get_num_subsongs(mod),
      instruments: names.join(" "),
      comment: meta(mod, "message").trim(),
    };
  } finally {
    M._openmpt_module_destroy(mod);
  }
}
