// Generate FLAC interop fixtures for the Rust client test: for each (channels,bitDepth) case,
// deterministic PLANAR native PCM -> flacEncode (the bake's exact encoder) -> write `<case>.flac`
// + `<case>.planar`. The Rust test (ipfs.rs) claxon-decodes each .flac and asserts it equals the
// .planar bytes — proving the client reproduces the bake's PCM byte-for-byte (packing + endianness
// + de-interleave). Re-run after any flac.ts encoder change. Output: src-tauri/src/testdata/flac/.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { flacEncode, initFlac } from "../src/flac.ts";

await initFlac();

const OUT = join(import.meta.dirname, "../../../apps/desktop/src-tauri/src/testdata/flac");
mkdirSync(OUT, { recursive: true });

// Deterministic native PCM, planar (all ch0 frames, then ch1). Smooth-ish so FLAC engages, with
// per-channel phase offset so stereo decorrelation + de-interleave are actually exercised.
function planar(frames: number, channels: number, bitDepth: number): Uint8Array {
  const bps = bitDepth === 16 ? 2 : 1;
  const out = new Uint8Array(frames * channels * bps);
  let w = 0;
  for (let c = 0; c < channels; c++) {
    const ph = c * 1.7;
    for (let f = 0; f < frames; f++) {
      const t = f + 1;
      if (bitDepth === 16) {
        let v = Math.round(20000 * Math.sin(t * 0.011 + ph) + 3000 * Math.sin(t * 0.13 + ph) + ((t * 37) % 17) - 8);
        v = Math.max(-32768, Math.min(32767, v)) & 0xffff;
        out[w++] = v & 0xff;
        out[w++] = (v >> 8) & 0xff;
      } else {
        let v = Math.round(100 * Math.sin(t * 0.05 + ph) + ((t * 13) % 7) - 3);
        v = Math.max(-128, Math.min(127, v)) & 0xff;
        out[w++] = v;
      }
    }
  }
  return out;
}

const cases = [
  { name: "c1_b8", ch: 1, bd: 8, frames: 1500 },
  { name: "c1_b16", ch: 1, bd: 16, frames: 1500 },
  { name: "c2_b8", ch: 2, bd: 8, frames: 1200 },
  { name: "c2_b16", ch: 2, bd: 16, frames: 1200 },
  { name: "c2_b16_odd", ch: 2, bd: 16, frames: 777 }, // odd frame count (block-boundary edge)
];
for (const c of cases) {
  const pcm = planar(c.frames, c.ch, c.bd);
  const flac = flacEncode(pcm, c.ch, c.bd);
  writeFileSync(join(OUT, `${c.name}.planar`), pcm);
  writeFileSync(join(OUT, `${c.name}.flac`), flac);
  console.log(`${c.name.padEnd(11)} ch=${c.ch} bd=${c.bd} frames=${c.frames}  planar=${pcm.length}B  flac=${flac.length}B  ratio=${(flac.length / pcm.length).toFixed(3)}`);
}
console.log(`\nwrote ${cases.length} fixtures to ${OUT}`);
