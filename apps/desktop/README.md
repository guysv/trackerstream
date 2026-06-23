# @trackerstream/desktop

The trackerstream desktop client — **Tauri** (Rust shell) + **Svelte 5 / SvelteKit**
(SPA via `adapter-static`) + an **AudioWorklet** playback engine driving
[`@trackerstream/wasm`](../../packages/wasm) (custom libopenmpt).

## Phase 0 — playback spike (status: ✅ met)

Goal: prove a tracker module plays glitch-free **off the main thread**, and that a
module **reassembled from content blocks** plays bit-identically.

| Exit criterion | Status |
|---|---|
| Complex `.it` plays while the UI thread is saturated, no drop-outs | engine runs on the audio thread (worklet); UI-stress button in-app |
| Format smoke: MOD / XM / S3M / IT / MPTM | ✅ all render audibly |
| One MO3 + one compressed-sample module | ✅ MO3 (minimp3/stb_vorbis sample codecs) plays |
| Module reassembled from blocks plays bit-identically | ✅ bit-exact PCM (MOD/S3M), byte-exact reassembly all formats |

### Architecture

- **`src/lib/audio/player.worklet.ts`** — `AudioWorkletProcessor`; synthesizes PCM
  with libopenmpt on the audio thread, posts `order:row` + per-channel VU to the UI.
  Pre-bundled by **`scripts/build-worklet.mjs`** (esbuild → IIFE, WASM inlined) into
  `static/player.worklet.js`, so `audioWorklet.addModule()` works in any WebView
  without ES-module-worklet support. Polyfills the few globals the worklet scope lacks
  (`TextDecoder` / `crypto` / `performance`).
- **`src/lib/audio/ModPlayer.svelte.ts`** — main-thread controller (Svelte 5 runes);
  owns the `AudioContext` + worklet node, exposes reactive playback state.
- **`src/lib/audio/blocks.ts`** — fixed-size block split/reassemble (stand-in for the
  Phase 1+ CID fetch path; the reassembly contract is identical).
- Render quality: **8-tap sinc interpolation, 48 kHz, volume ramping** (desktop-grade).

## Run

```
pnpm install                                  # from repo root
pnpm --filter @trackerstream/wasm build       # build libopenmpt (once; needs emsdk)
pnpm --filter @trackerstream/desktop tauri dev # launch the app
```

In the app: pick a bundled sample (or a file), **▶ play**, then hit **🔥 stress UI
thread** — audio keeps playing while the main thread is pegged. Tick **load via block
reassembly** to play a module rebuilt from 16 KB blocks.

> Demo modules under `static/samples/` are third-party (author terms apply) and are
> git-ignored; populate from `~/tmp/somemods` + the corpus, or load your own files.

## Test (headless, no WebView)

```
pnpm test    # builds the worklet, then drives it through a mocked AudioWorkletGlobalScope
```

Validates the *actual shipped worklet artifact* (bundled libopenmpt + polyfills +
processor): loads each format, drives `process()`, asserts PCM output + position/VU.
