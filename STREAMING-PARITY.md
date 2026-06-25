# Streaming Parity — Plan

Status: **Phase 1 landed · Phase 2 proposed** · Author: design discussion
2026-06-25 · Supersedes the recreate-on-grow streaming path (commits efa4827
gating, 48cfd62 crossfade).

> **Phase 1 (the libopenmpt fork) is implemented and proven.** `provide_sample`
> is bit-exact vs full-load across MOD / S3M / XM / IT (including loop
> boundaries), verified through a clean re-extract + patch + full rebuild. See
> the Phase 1 section for what shipped vs. the original plan. Phase 2 (wire
> format, immortal-instance client, fence, prefetch, seek, rollout) is unbuilt.

## The goal: 1:1 parity

Streaming playback should be **perceptually identical** (ideally bit-identical)
to playing the fully-downloaded module from the same start position. The only
permitted deviation from full-load is an honest **buffer underrun** (a clean
pause with a "buffering" indicator) when the network can't keep the playhead
fed — never a click, a chopped/silent sample, a flam, or a partially-loaded
sample sounding as if it were complete.

## Why the current approach can't get there (root cause)

The streaming path rebuilds the libopenmpt module from a growing byte buffer on
every version bump (`stream_reassemble` bumps `b.version` per sample,
`apps/desktop/src-tauri/src/ipfs.rs`; the worklet destroys + recreates +
re-seeks in `apps/desktop/src/lib/audio/player.worklet.ts`). Two defects are
inherent to that design and unfixable by tuning:

1. **Recreate-on-grow discards DSP state.** `openmpt_module_set_position_seconds`
   re-simulates row state but is tick/row-accurate, *not* sample-accurate, and
   cannot restore per-channel sample cursors mid-note, volume/pan ramps,
   vibrato/tremolo/autovibrato phase, filter state, NNA/background channels, or
   active fades. The recreated instance is a *different render*. The 10 ms
   crossfade (`XFADE_FRAMES`) blends two genuinely different signals — that's
   not a click, but it isn't parity.

2. **The gate only protects the opening.** `segment0` gating holds the first
   `playable=true` until pattern 0's samples are resident, but after playback
   starts every version bump reloads regardless of the playhead. When the
   playhead reaches a note triggering a not-yet-fetched sample, libopenmpt plays
   it as zeros (skeleton zero-fills sample regions), and a later recreate
   splices it in mid-note → the "partial samples played as if full" + click.

The fix is to stop recreating: keep **one immortal `CSoundFile`** for the whole
stream and **patch sample PCM into it in place** as data arrives. libopenmpt has
no public API for exactly that one operation — so that one operation is the
entire fork.

## Architecture in one paragraph

One immortal libopenmpt instance is created (via the *existing*
`create_from_memory`) from a **skeleton**: a structurally-valid module whose
sample headers are normalized to uncompressed PCM of the decoded length, with
the PCM bytes zero-filled. As decoded PCM streams in, the client calls the **one
new API**, `provide_sample`, to memcpy it over the zeros in the already-allocated
`ModSample` buffer (and re-run that sample's loop/guard precompute). Recreate-on-
grow is gone. Correctness — never sounding a not-yet-provided sample — is handled
**on the client**, not in the mixer: a fence reuses the per-order `checkpoints`
table we already bake and refuses to let the playhead enter order N until
`checkpoint[N]`'s samples are all provided, stalling into "buffering" otherwise.
A **predictive prefetch** scheduler (driven by the checkpoint table + live
playhead position) reorders fetches so the fence rarely fires. **Seeking** is
`set_position_order_row` on the same instance (state is computed from skeleton-
only, always instant and exact) plus a reseed of the prefetch queue; the same
fence covers audibility. State is free; audio is fenced.

Two invariants worth carving in stone:
- **State is free, audio is fenced.** Any seek is immediately state-exact from
  the skeleton; only sound waits, and only via the fence.
- **Samples are atomic.** A sample is exposed to the instance only when fully
  resident. No sub-sample progressive fill (a half-loaded sample = artifact).

Why the client-side fence is parity-correct (not a compromise):
- It is *conservative* — every sample order N can trigger is provided before any
  of order N sounds, so no pending sample ever triggers. It can only ever make a
  buffer slightly longer, never produce a wrong or missing note.
- Tracker playback is **deterministic**, and the checkpoint table is built by the
  *same* libopenmpt simulation, so "what order N needs" matches at runtime. No
  drift for the fence to miss.

---

# Phase 1 — The libopenmpt fork (one API) — ✅ DONE

Self-contained, independently validatable, touches only `packages/wasm`. Goal:
prove that patching a sample in place via `provide_sample` is bit-identical to
loading it normally, before we change the wire format or the corpus.

**What shipped** (commit on `fix/chopped-opening-segment0`):
`packages/wasm/patches/0001-provide-sample.patch` (git-style, applied by
`build.sh` on a clean extract) adds `module_impl::provide_sample` (memcpy into
the already-allocated, guard-padded `ModSample` buffer + `PrecomputeLoops`),
`is_sample_pending`, and three **harness-only** debug accessors
(`debug_sample_{data,bytes,frames}`) used by the parity test. New harness
`packages/wasm/test/provide-sample.mjs`, wired into `pnpm test`. Result:
**`parityDiff = 0.0` (bit-exact) for every format**, while `skeletonDiff` (0.07–
1.9) confirms the samples were genuinely zeroed first — so the proof is strict.
Bundle stayed in size class (1.526 MB → 1.528 MB).

> **Carry into Phase 2 — the safety contract.** The no-lock design is correct
> *only because* `provide_sample` is called from the worklet thread, between
> `process()` quanta. When 2.2 wires `provideSample` into `onMessage`, keep it
> there — never call it from the render callback or another thread. Likewise
> `PrecomputeLoops(…, false)` (skip active-channel update) is safe *only because*
> the client fence (2.2) guarantees a sample is resident before its first
> trigger. Both invariants are *assumed* by Phase 1 and must be *established* by
> Phase 2's client code.

> **Note on the fork's scope.** This is a deliberate **vendor patch**, not an
> upstream-ready contribution: it leans on trackerstream-only invariants
> (single worklet thread; native-layout PCM supplied by our own bake; fence
> guarantees) and exposes raw-pointer debug accessors. Upstreaming to libopenmpt
> would be a separate redesign (ext-interface form, a stable PCM input format,
> the `CriticalSection`, no debug pointers) — see "Upstream rebase" in Risks.

## 1.1 Vendor the source for patching

`build.sh` downloads `libopenmpt-0.8.7+release.makefile.tar.gz` at build time and
extracts it under `packages/wasm/build/`. Two separate locations, two roles:

- **Dev clone (where we write the patch): `~/git/libopenmpt`.** Clone upstream
  OpenMPT next to the other repos (sibling of `~/git/trackerstream`, same as the
  existing `~/git/schismtracker` reference checkout):
  ```
  git clone https://github.com/OpenMPT/openmpt ~/git/libopenmpt
  cd ~/git/libopenmpt && git checkout libopenmpt-0.8.7   # pin to the built release tag
  ```
  Develop `provide_sample` here on a branch, then export the diff:
  ```
  git diff libopenmpt-0.8.7 > ~/git/trackerstream/packages/wasm/patches/0001-provide-sample.patch
  ```
  This clone is a *workspace*, not committed to trackerstream and not on the build
  path.
- **Committed patch series: `packages/wasm/patches/*.patch`.** `build.sh` applies
  these after `tar xzf` (before `make`). This keeps the diff against upstream tiny
  and reviewable, makes rebasing onto a new libopenmpt release a `git apply`, and
  keeps the trackerstream repo small. (Vendoring the full extracted tree is the
  alternative; rejected — bloats the repo.)
- Pin the exact upstream version (already 0.8.7); document the rebase procedure
  (bump the tag in the clone, re-export the patch, bump `VERSION` in `build.sh`).

## 1.2 The one new C API

Add to `EXPORTS` in `build.sh`:

```c
// Overwrite one sample's DECODED PCM in place. `data` matches the ModSample's
// native in-memory layout (bit depth, channel interleave) exactly, so the body
// is: locate ModSample[index] -> memcpy into the already-allocated pSample (the
// skeleton allocated full length at load) -> re-run that sample's loop/guard-
// sample precompute so the interpolator reads correct data at loop/end. Nothing
// else in CSoundFile is touched. Single-threaded: the worklet calls this in
// onMessage, between process() quanta, so there is no race with the mixer.
int openmpt_module_provide_sample(openmpt_module *mod, int32_t sample_index,
                                  const void *data, size_t frames);
```

Optional, cheap, nice-to-have (still no mixer changes):
```c
int openmpt_module_is_sample_pending(openmpt_module *mod, int32_t sample_index);
```

**As built**, the patch also adds three **harness-only** accessors so the Phase 1
parity test can dump a sample's decoded PCM (native layout) and re-provide it:
```c
const void * openmpt_module_debug_sample_data(openmpt_module *mod, int32_t i);  // heap ptr
size_t       openmpt_module_debug_sample_bytes(openmpt_module *mod, int32_t i); // GetSampleSizeInBytes
size_t       openmpt_module_debug_sample_frames(openmpt_module *mod, int32_t i);// nLength
```
These are test scaffolding (they return raw internal pointers) — fine for our
internal bundle, but they are *not* a real API and would not survive upstreaming.

That is the whole fork. No `create_streaming`, no fenced-read entry point, no
per-format loader edits, no mixer changes — those were avoidable (see "What we
deliberately are NOT forking").

## 1.3 What we deliberately are NOT forking

- **No `create_streaming` / no loader changes.** The skeleton is normalized at
  bake time (Phase 2) so every sample header declares *uncompressed* PCM of the
  decoded length with zero bytes present. `create_from_memory` then loads it via
  the stock path — full-length zero buffers, no decompression edge cases — and
  `provide_sample` overwrites the zeros. (Normalizing to uncompressed is what lets
  us skip per-format loader work; the alternative — making each `Load_*.cpp` defer
  its sample read — is more fork surface and is not needed.)
- **No in-mixer reactive fence / no fenced read.** Correctness lives in the client
  fence on the checkpoint table (parity-correct because conservative +
  deterministic, see architecture). The in-mixer fence remains an **optional
  future hardening** for hypothetical nondeterminism only — not built for v1.

## 1.4 Bake-time decoded-PCM extraction (used by Phase 2, validated here)

To make `provide_sample` a pure memcpy and dedup canonical, sample PCM is stored
**decoded** in the exact `ModSample` in-memory layout. Extract it at bake time by
loading the real module with libopenmpt and reading out each sample's decoded
buffer (we already run libopenmpt at ingest for metadata,
`apps/server/src/meta.ts`). Phase 1 just needs a way to dump these buffers for the
parity harness.

## 1.5 Parity harness (the Phase 1 exit criterion) — as built

`packages/wasm/test/provide-sample.mjs`, alongside `reassembly.mjs`, per module:
1. Load a real module fully → render to PCM `A`; dump each sample's decoded PCM
   via the debug accessors.
2. Build a **normalized skeleton** (the original with every sample's PCM zeroed,
   via locators ported from `repack/parse.ts`) → `create_from_memory` → assert it
   loads and every sample reports `is_sample_pending`. `provide_sample` each
   dumped buffer (assert no longer pending) → render to PCM `B`.
3. Assert `A == B` (bit-exact). Because the skeleton is **zeroed**, a no-op
   provide would render silence and fail — so parity is a real proof of the
   memcpy + `PrecomputeLoops` path, across loop points (8 s crosses loops).
4. Format spread **MOD / S3M / XM / IT**. Wired into `pnpm test` after
   `reassembly.mjs`. (Argv-overridable; the default corpus is the local
   `~/tmp/somemods/` set, so the test `SKIP`s — not fails — when absent, like
   `reassembly.mjs`; it is a local oracle, not CI-gating.)

**Sample compression dropped from Phase 1.** The original plan listed
compressed-IT in the spread. It was removed: `provide_sample` operates on
*already-decoded* PCM, and the Phase 2 bake stores samples decoded (skeleton
headers uncompressed by construction), so a compressed sample never reaches the
runtime provide path. Whether the bake's libopenmpt decode handles compressed-IT
is a Phase 2 concern (stock libopenmpt already does — it's why the corpus plays).

**Phase 1 done — exit criterion met:** parity diff is **bit-exact** across
MOD / S3M / XM / IT including loop boundaries (every format `parityDiff = 0.0`),
the bundle builds via `build.sh` (patch applies on a clean `build/`) and stays in
the same size class, and `smoke.mjs` / `reassembly.mjs` still pass.

---

# Phase 2 — Format, client, seek, rollout

Depends only on Phase 1's `provide_sample`. Reshapes the wire format around
planning + seeking and rewrites the client streaming path onto the immortal
instance + client fence.

## 2.1 Repack v2 — the object tree

Today the DAG reconstructs the file byte-exact (`offset`/`length` are file
positions; `reassemble` splices PCM back; `packages/repack/src/dag.ts`). v2 feeds
decoded PCM by sample index, so byte-exact reconstruction is no longer a goal —
the tree is organized for **planning + seeking**, like a media index (MP4 `moov`
/ DASH `sidx`): one front-loaded index from which the client knows everything it
will need and which playback region each blob serves.

Layers:
1. **Skeleton** — headers + orders + patterns + sample/instrument metadata, with
   **sample headers normalized to uncompressed PCM of the decoded length and the
   PCM zero-filled** (this is what lets Phase 1 stay a one-API fork). Fetched
   fully up front; the zeros dedup/compress to almost nothing.
2. **Sample table** — `index → { decodedLength, channels, bitDepth, pcmRoot }`.
   PCM stored **decoded** (extracted via libopenmpt at bake). Drop file `offset`.
   Canonical dedup: the same instrument shared between e.g. a delta-MOD and a
   raw-IT now collides (raw-file-byte storage can't do this).
3. **Per-sample PCM subtrees** — CDC chunks (keep for cross-module dedup); the
   sample is the atomic unit.
4. **Planning index** — `checkpoint[order] → sample-set`, stored as **deltas**
   (order N *adds* samples beyond N−1) and **pre-resolved to leaf CIDs** so that,
   given playhead order N, the client computes the `[N, N+lookahead]` union and
   issues one batched bitswap want-list — no per-sample pcmRoot round-trips. (If
   the index bloats huge modules, split it into its own object fetched right after
   the manifest — still one extra hop, still before audio.) This same table is the
   client fence's source of truth.

Manifest gets `v: 2`; clients dispatch on it (v1 keeps the recreate path).
Schema draft is the first concrete deliverable of Phase 2 (lock before coding).

## 2.2 Client — immortal instance + provide-sample (`player.worklet.ts`)

- Create once via `create_from_memory(skeleton)`. Never destroy/recreate.
- Replace the `reload`-with-full-buffer message with `provideSample{ index, pcm }`
  messages applied in `onMessage` (audio thread, between quanta — race-free).
- Delete `XFADE_FRAMES`, `pendingMod`, the crossfade blend, `beginReload`,
  recreate-on-grow, and the `cur()` indirection — all obsolete.
- The fence: before advancing, the worklet checks the current order against the
  checkpoint table; if `checkpoint[order]` (or `[order, order+lookahead]`) has any
  not-yet-provided sample, hold output (silence) and raise `buffering` instead of
  rendering. Resume when the needed samples have been provided.

## 2.3 Client — playhead-driven prefetch scheduler (`ipfs.rs`)

- Replace the static `stream_fetch_order` vector with a **priority queue keyed by
  distance from the playhead in playback order**, seeded by `checkpoint[startOrder]`.
- Subscribe to the worklet's `pos` (already posted ~45×/s) to reprioritize as the
  playhead advances; make the fetch loop **preemptible** (cancel/repriority on
  seek).
- Fetch a sample's chunks, then send `provideSample` (no shared byte buffer, no
  `version`). `stream_reassemble`/`reassemble`/`seek_module` collapse toward one
  path. `segment0` counting and the `playable` flag are removed (the fence owns
  buffering now).

## 2.4 Seek — unify with streaming

- Seek = `set_position_order_row(N,0)` on the immortal instance (state from
  skeleton-only → instant, exact) + reseed prefetch with `checkpoint[N]` + reset
  rebuffer hysteresis. No fresh buffer, no recreate.
- `seek_module` (the separate cold-seek buffer builder, `ipfs.rs`) is **deleted**;
  cold seek, play-from-start, and resume-saved-position become the same pipeline
  differing only in initial `set_position` + prefetch seed.
- Backward seek is instant (samples stay patched / blockstore-cached); forward
  seek into unstreamed territory stalls then fills. Instance residency grows
  monotonically toward full-load over a session.

## 2.5 Buffering UX + hysteresis

- `playable`/`segment0` gating in `ipfs.rs` is removed; `buffering` is raised by
  the client fence, can appear mid-track, and is honest. Reuse the existing
  `buffering` plumbing (efa4827, `NowPlaying.svelte`) but drive it from the worklet.
- **Rebuffer hysteresis** to avoid play/stall oscillation on marginal links:
  after a stall, resume only when a lookahead margin is resident (start with
  "next ~2–3 orders' checkpoint sets in"). Reset hysteresis on explicit seek.
- **Stall granularity: full-instance stall** (freeze whole mixer) — a held note
  pauses/resumes cleanly, artifact-free. (Per-channel continue would be smoother
  but drops a note → not parity. Documented as the rejected alternative.)

## 2.6 Corpus re-bake + rollout

- v2 is a manifest schema break → re-bake the corpus (mechanism exists:
  `REBUILD=1`, `apps/server/bin/ingest.ts`; see `corpus-missing-seek-tables`).
- **Versioned rollout:** client supports v1 (current recreate path) and v2
  (immortal-instance path), dispatching on `manifest.v`. Shipped clients keep
  working on v1 roots; new client + re-baked v2 roots use the new path. Retire v1
  once the new desktop release is broadly adopted.
- Keep a **small-module fast path**: below some size, just fetch the whole DAG and
  play full-load — also the mitigation for a single sample larger than the
  prefetch lead.

---

## Validation strategy

- **Phase 1:** ✅ bit-exact parity diff (provide-in-place ≡ load-normally),
  including loop boundaries, across MOD / S3M / XM / IT (1.5), in `pnpm test`.
- **Phase 2:** (a) headless end-to-end against a local kubo: stream a v2 root and
  assert rendered PCM matches full-load within rounding, including a forced
  mid-track stall; (b) seek diff: seek to order N, assert output matches a
  full-load module seeked to N; (c) network-throttled soak to confirm buffering is
  the *only* artifact and hysteresis doesn't oscillate.

## Risks & open questions

- **Skeleton normalization to uncompressed PCM** — *partly retired.* Phase 1
  proved `create_from_memory` loads an all-zero-PCM skeleton and `provide_sample`
  reconstructs it bit-exact for MOD / S3M / XM / **uncompressed IT** (the harness
  zeroes sample regions in place). Still open for Phase 2's bake: the actual
  header rewrite for **compressed-IT** (clear the compression flag, length =
  decoded frames) — not exercised in Phase 1 because no compressed-IT exists in
  the corpus and it's a bake-time concern, not a `provide_sample` concern.
- **Decoded layout must match `ModSample` exactly** (bit depth, signedness, stereo
  interleave, libopenmpt's interpolation guard-sample padding). The bake extracts
  via libopenmpt itself to guarantee this; `provide_sample` re-runs the guard/loop
  precompute.
- **Determinism** — the client fence trusts that runtime triggering matches the
  bake-time checkpoint simulation. True for tracker playback (deterministic). If a
  rogue nondeterministic effect ever broke this, the optional in-mixer reactive
  fence is the fallback (not built for v1).
- **Manifest size** with a pre-resolved leaf index on huge modules — measure;
  split into a separate index object if needed.
- **Upstream rebase** — the committed patch is a deliberate *vendor* patch, not
  an upstream-ready contribution. Floating `provide_sample` to libopenmpt would be
  a separate redesign, not a cleanup: upstream would require an **ext interface**
  (`openmpt_module_ext_get_interface`, not a top-level `openmpt_module_*` symbol),
  a **stable documented PCM input format** instead of native-`ModSample`-layout
  coupling, the **`CriticalSection`** we dropped (a general lib can't assume a
  single audio thread), `PrecomputeLoops(…, true)` (no fence to lean on), and
  **no debug pointer accessors** — plus a use-case discussion first (the library
  is intentionally read-only; deferred-sample loading has no analog). Rebasing
  onto a newer libopenmpt release, by contrast, is cheap: re-export the patch,
  bump `VERSION` in `build.sh` (procedure in `packages/wasm/README.md`).

## Decisions locked in this design

- The fork is **one new API** (`openmpt_module_provide_sample`) + optional
  `is_sample_pending` (+ harness-only `debug_sample_*`). No `create_streaming`, no
  fenced read, no mixer/loader edits. **Shipped & proven (Phase 1).**
- Skeleton normalized to uncompressed zero PCM so the stock loader path works
  unmodified; decoded canonical PCM stored in the DAG.
- Correctness fence lives on the **client**, reusing the per-order checkpoint
  table; parity-correct because conservative + deterministic. In-mixer reactive
  fence deferred as optional hardening only.
- Predictive checkpoint/playhead prefetch for smoothness; checkpoints serve both
  prefetch and the fence.
- Full-instance stall for parity; order-based rebuffer hysteresis.
- Stream / cold-seek / resume unified into one pipeline; `seek_module` deleted.
- Versioned manifest (`v:2`) with v1 fallback; corpus re-bake via `REBUILD=1`.

## Decisions still open (pick before Phase 2 coding)

- Exact v2 manifest schema (field names; pre-resolved leaf index inline vs
  separate object).
- Fence lookahead + rebuffer hysteresis threshold (orders vs seconds; default).
- Small-module fast-path size cutoff.
- ~~Patch-series vs vendored-source for the fork.~~ **Decided: patch-series**
  (`packages/wasm/patches/*.patch`, applied by `build.sh`).
