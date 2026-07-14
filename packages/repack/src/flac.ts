// FLAC codec for v4 sample leaves. v4 optionally FLAC-compresses each streamed sample's decoded
// native PCM (the same planar buffer v2/v3 chunk raw). FLAC is lossless, so playback stays bit-exact
// after decode; the win is ~2x fewer sample-block bytes (measured ~0.47 ratio corpus-wide, ~52%
// smaller first-playable set). See _flac-lab.ts for the numbers.
//
// ENCODER: libflacjs (mmig/libflac.js) — an Emscripten build of libFLAC, PINNED via pnpm-lock. We
// use the asm.js variant (pure JS): it loads in Node with no wasm-fetch fragility and rsyncs to the
// prod tree with no native/.wasm artifacts. Determinism is what makes v4 viable — a fixed libflacjs
// version + params yields byte-identical output, so identical sample PCM -> identical FLAC -> identical
// CID (cross-module dedup) and REBUILD stays idempotent. BUMPING libflacjs changes every FLAC leaf
// CID and REQUIRES a full corpus re-bake; treat its version as part of the bake's on-wire identity.
//
// LAYOUT: libopenmpt hands us PLANAR native PCM (all of ch0, then ch1) for stereo. We feed libFLAC
// per-channel (isInterleaved=false), so no manual interleave here; on the client, claxon yields
// interleaved samples that ipfs.rs::flac_decode re-planarizes. The bake's decode oracle (flacDecode)
// uses libflacjs's decoder, which returns planar per-channel directly.
//
// DETERMINISM + claxon-compat are asserted by test/v4-roundtrip.ts (two bakes -> identical roots) and
// the Rust flac_decode_matches_bake interop test (fixtures via test/gen-flac-fixtures.ts).
import { decodeFlacStream } from "./flac-decoder.ts";

/** Pinned encoder identity — bump only with a full corpus re-bake (every FLAC leaf CID changes). */
export const FLAC_CODEC = 1; // SampleV4.encCodec value for FLAC-compressed leaves
export const FLAC_RAW = 0; // encCodec value for uncompressed (planar native) leaves
const COMPRESSION = 8; // libFLAC preset (0..8); 8 = best, free at bake time

/** Cumulative encode wall-time + call count (for bake profiling; PROFILE=1 in the ingest). */
export const flacStats = { ms: 0, calls: 0, bytesIn: 0 };

// libflacjs is CommonJS + async wasm/asm init. Load once, reuse the ready module for every call.
let Flac: any = null;
let Encoder: any = null;

/** Load + ready libFLAC *including the encoder*. Bake-only (buildDagV4 does this at its entry).
 *  Clients must use initFlacDecoder — see below. */
export async function initFlac(): Promise<void> {
  await initFlacDecoder();
  if (Encoder) return;
  // Node-only: libflacjs/lib/* is UMD that bundlers cannot link (see flac-decoder.ts). Only the
  // bake reaches this, and the bake only ever runs in Node.
  Encoder = interop(await import("libflacjs/lib/encoder.js")).Encoder;
}

/** Decode-only init — the CLIENT path (desktop and browser). Split from initFlac because only the
 *  bake encodes, and libflacjs's `lib/` wrappers are unbundleable UMD: keeping them off this path
 *  is what lets a browser bundle contain a working FLAC decoder at all. */
export async function initFlacDecoder(): Promise<void> {
  if (Flac) return;
  // Dynamic import, NOT createRequire: this module has to load in a browser bundle too. The
  // specifier MUST stay literal — a variable specifier is opaque to bundlers, which then leave the
  // bare name for the browser to resolve, and it can't. `dist/libflac.js` is plain asm.js and
  // bundles cleanly; we drive its raw C-API ourselves via decodeFlacStream.
  const F = interop(await import("libflacjs/dist/libflac.js"));
  await new Promise<void>((res) => {
    if (F.isReady && F.isReady()) return res();
    F.on("ready", () => res());
  });
  Flac = F;
}

// libflacjs is CJS, so the namespace carries module.exports on `.default` under both Node-ESM and a
// bundler's interop; fall back to the namespace itself for interop shims that re-export directly.
function interop(ns: any): any {
  return ns?.default ?? ns;
}

function ready(): void {
  if (!Flac) throw new Error("flac: call initFlac() before flacEncode/flacDecode");
}

/** Split planar native PCM into one sign-extended Int32Array per channel (libFLAC input). */
function toChannels(planar: Uint8Array, channels: number, bitDepth: number): Int32Array[] {
  const bps = bitDepth === 16 ? 2 : 1;
  const frames = planar.length / (channels * bps);
  const out: Int32Array[] = [];
  for (let c = 0; c < channels; c++) {
    const a = new Int32Array(frames);
    const base = c * frames * bps;
    for (let f = 0; f < frames; f++) {
      if (bitDepth === 16) {
        const lo = planar[base + f * 2];
        const hi = planar[base + f * 2 + 1];
        a[f] = ((lo | (hi << 8)) << 16) >> 16; // sign-extend i16
      } else {
        a[f] = (planar[base + f] << 24) >> 24; // sign-extend i8
      }
    }
    out.push(a);
  }
  return out;
}

/** Encode PLANAR native PCM to a FLAC stream (deterministic; pinned libflacjs + fixed params). */
export function flacEncode(planarPcm: Uint8Array, channels: number, bitDepth: number): Uint8Array {
  ready();
  const t0 = Date.now();
  const bps = bitDepth === 16 ? 2 : 1;
  const frames = planarPcm.length / (channels * bps);
  const enc = new Encoder(Flac, {
    sampleRate: 44100, // arbitrary; not audio-timed, does not affect the lossless bytes we care about
    channels,
    bitsPerSample: bitDepth,
    compression: COMPRESSION,
    totalSamples: frames,
  });
  try {
    enc.encode(toChannels(planarPcm, channels, bitDepth), frames, false);
    enc.encode(); // finish
    return new Uint8Array(enc.getSamples());
  } finally {
    enc.destroy();
    flacStats.ms += Date.now() - t0;
    flacStats.calls++;
    flacStats.bytesIn += planarPcm.length;
  }
}

/** Decode a FLAC stream back to PLANAR native PCM (bake-side oracle; the prod client uses claxon).
 *  libflacjs's decoder returns non-interleaved (planar) per-channel bytes, but at a FIXED 16-bit
 *  output width regardless of the stream's bit depth (an 8-bit sample comes back sign-extended to
 *  i16). We down-convert to the native `bitDepth` by taking the low LE bytes of each decoded sample
 *  (exact, since the value fits in `bitDepth` bits), then concatenate channels into native planar. */
export function flacDecode(flacBytes: Uint8Array, _channels: number, bitDepth: number): Uint8Array {
  ready();
  const { channels: chans, totalSamples } = decodeFlacStream(Flac, flacBytes);
  const bps = bitDepth === 16 ? 2 : 1;
  const frames = totalSamples || (chans[0] ? chans[0].length / 2 : 0);
  const decW = frames ? chans[0].length / frames : bps; // decoder bytes-per-sample (16-bit => 2)
  const parts = chans.map((ch) => {
    if (decW === bps) return ch;
    const n = ch.length / decW;
    const o = new Uint8Array(n * bps);
    for (let f = 0; f < n; f++) for (let b = 0; b < bps; b++) o[f * bps + b] = ch[f * decW + b]; // low LE bytes
    return o;
  });
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let w = 0;
  for (const p of parts) {
    out.set(p, w);
    w += p.length;
  }
  return out;
}
