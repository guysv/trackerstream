# @trackerstream/wasm — custom Emscripten libopenmpt

A purpose-built [libopenmpt](https://lib.openmpt.org/libopenmpt/) WebAssembly module
for trackerstream playback. We own the build (rather than inheriting a prebuilt one)
so we control the exported C API, the codec set, and the output shape.

## What it produces

`build.sh` downloads pinned **libopenmpt 0.8.7** source and builds the
`audioworkletprocessor` Emscripten target → `dist/libopenmpt.js`: a single-file ES6
module factory with the WASM **inlined** (no separate `.wasm` fetch — required inside
an AudioWorklet). `dist/libopenmpt.d.ts` (colocated types) is copied alongside.

- **Off-thread by design:** the build targets `AudioWorkletProcessor`; the desktop
  client bundles it into a self-contained classic worklet script.
- **Codecs:** libopenmpt's bundled **minimp3 + stb_vorbis** (public-domain,
  AGPL-clean) decode MP3/OGG-compressed samples and MO3 sample data. Built-in MO3
  decoder, so no `unmo3`. (`PORTS=1 bash build.sh` instead links mpg123 + libvorbis
  via Emscripten ports.)
- **Exports:** the exact openmpt C API surface trackerstream drives — create/destroy,
  `read_float_stereo`, position/seek, render params (sinc interpolation), per-channel
  VU, subsong + name getters. See `EXPORTS` in `build.sh`.

## Build

```
bash build.sh            # needs ~/emsdk (emscripten); first build ~1–2 min
```

## Test (regression oracles)

```
pnpm test                # smoke (renders each format) + reassembly (bit-exact)
node test/smoke.mjs ~/tmp/somemods/*          # render every sample, report peak
node test/reassembly.mjs                      # block round-trip = bit-identical PCM
```

Verified across MOD / XM / S3M / IT / **MPTM** / **MO3** (compressed samples) — see
the Phase 0 notes in `apps/desktop/README.md`.
