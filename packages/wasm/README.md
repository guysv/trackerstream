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

## Patches (`patches/*.patch`)

We carry a tiny patch series on top of pristine upstream rather than vendoring the
whole tree. `build.sh` applies every `patches/*.patch` (in order) right after
`tar xzf`, before `make`.

- **`0001-provide-sample.patch`** — the streaming fork (STREAMING-PARITY.md Phase 1).
  Adds `openmpt_module_provide_sample(mod, sample_index, data, frames)`, which
  overwrites one sample's decoded PCM in place (memcpy into the already-allocated,
  guard-padded `ModSample` buffer + `PrecomputeLoops`) so a single immortal instance
  can be fed samples as they stream in — no destroy/recreate. Also `…_is_sample_pending`
  and harness-only `…_debug_sample_{data,bytes,frames}` accessors. New symbols must be
  added to both the patch and the `EXPORTS` list in `build.sh` (and `libopenmpt.d.ts`).

### Rebasing onto a new libopenmpt release

The patch is developed in a throwaway clone (sibling of this repo), not committed here:

```
git clone https://github.com/OpenMPT/openmpt ~/git/libopenmpt
cd ~/git/libopenmpt && git checkout libopenmpt-0.8.7   # the tag VERSION pins in build.sh
# ... edit libopenmpt/* on a branch ...
git diff libopenmpt-0.8.7 > ~/git/trackerstream/packages/wasm/patches/0001-provide-sample.patch
```

To move to a newer libopenmpt: bump the tag in the clone, re-apply/port the diff,
re-export the patch, and bump `VERSION` in `build.sh`. The patch is a `-p1` diff
(`a/libopenmpt/… b/libopenmpt/…`); `build.sh` applies it with `patch -p1`.

## Build

```
bash build.sh            # needs ~/emsdk (emscripten); first build ~1–2 min
```

## Test (regression oracles)

```
pnpm test                # smoke + reassembly (bit-exact) + provide-sample (parity)
node test/smoke.mjs ~/tmp/somemods/*          # render every sample, report peak
node test/reassembly.mjs                      # block round-trip = bit-identical PCM
node test/provide-sample.mjs                  # provide_sample == load-normally (bit-exact)
```

`provide-sample.mjs` is the Phase 1 exit criterion: it zeroes every sample in a
normalized skeleton, `provide_sample`s the real decoded PCM back, and asserts the
render is bit-identical to a full load (`skeletonDiff` shows the zeroing took
effect; `parityDiff` is the proof) across MOD / S3M / XM / IT. Sample *compression*
(e.g. IT2.14) is a load-time concern only — `provide_sample` operates on already-
decoded PCM and the Phase 2 bake stores samples decoded, so it's out of scope here.

Verified across MOD / XM / S3M / IT / **MPTM** / **MO3** (compressed samples) — see
the Phase 0 notes in `apps/desktop/README.md`.
