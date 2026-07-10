# Rebuild — v3: v2 streaming + byte-exact reassembly from the SAME blocks

Status: **code landed + verified (local) · re-bake gated** · 2026-07-10

Goal: a client holding a module's CID chunks can rebuild the **byte-exact original** (MD5
parity), so the desktop app offers *right-click → Open with → schismtracker / milkytracker*,
downloading the rebuilt module to `~/Downloads` and launching an external tracker — **without
hurting v2 streaming**.

## The idea

The shipped **v2** bake is a *streaming* format that discards byte-exactness: it stores
libopenmpt-**decoded** PCM by sample slot + a mutated/zero-filled skeleton
(`STREAMING-PARITY-V2-SCHEMA.md`). But the decoded native PCM it stores is a **deterministic,
invertible transform** of the on-disk sample bytes — identity (MOD), sign-flip (S3M/unsigned-IT),
re-delta (XM), stereo de-interleave (16-bit IT), and combinations. So the original is
reconstructible **from the exact blocks v2 already streams**, given back:

- per streamed sample: its on-disk file **`offset`** + an **`enc`** transform tag,
- the file **`originalLength`**.

**v3 = v2 + those three additions.** It streams **byte-identically to v2** (the playback path
parses v3 as v2 and ignores the extra fields); it only *adds* a reassembly path. No sample bytes
are duplicated; cross-module dedup on the streamed PCM is unchanged.

The transform tag is chosen **empirically at bake** and byte-checked against the on-disk region,
so reassembly is byte-exact **by construction** — a wrong/unmodeled encoding can never corrupt
output, at worst it leaves a rare sample resident in the skeleton.

## Routing (bake)

- **MOD / S3M / XM / uncompressed-IT → v3** (`buildDagV3`): v2 streaming blocks + `{offset, enc}`
  + `originalLength`.
- **Compressed-IT → v1** (`buildDag`, byte-exact, still reassemble-able): a compressed sample
  (IT 0x08) can't be re-derived from decoded PCM (non-canonical recompression + the v2 skeleton
  mutates its header), so `buildDagV3` throws and the module falls to v1. "Compressed IT stays on
  v1." Consequence: compressed-ITs revert from v2 streaming to v1 full-load.
- **mo3 / unparseable → v1 flat** (`buildFlatDag`), unchanged.

Every root is reassemble-able: v3 via `reassembleV3`, v1/flat via `reassemble`.

## The enc transform (`applyEnc`, mirrored in TS + Rust)

Bitmask applied `DEINT → SIGN → DELTA`, using the `bitDepth`/`channels` already in the sample
entry (so it's format-agnostic):
- `ENC_SIGN 0x1` — 8-bit `^0x80` / 16-bit flip high byte (signed↔unsigned).
- `ENC_DELTA 0x2` — per-channel delta encode.
- `ENC_DEINT 0x4` — stereo interleaved → planar.

## What landed

- `packages/repack/src/dag.ts`: `buildDagV3` / `reassembleV3` / `applyEnc` (+ `ENC_*`),
  `MANIFEST_V3`, `SampleV3`/`ManifestV3`. **v2 untouched.** Test `test/v3-roundtrip.ts`.
- `apps/server/src/ingest.ts`: route `buildDagV3` → `buildDag` (compressed/parse-fail) →
  `buildFlatDag`. No catalog change (the module's own `rootCid` is the reassemble-able root).
- `apps/desktop/src-tauri/src/ipfs.rs`: `ManifestV3`/`SampleV3`, `apply_enc` (mirrors TS),
  `reassemble_v3`, `reassemble_any` (version dispatch); streaming accepts `v==3` (routes to the
  v2 path, extra fields ignored).
- `apps/desktop/src-tauri/src/lib.rs`: `download_and_open` (root CID → `reassemble_any` → md5
  gate → write Downloads → opener launch); Cargo `+md5`.
- `apps/desktop/src/lib/{p2p,menus}.ts`: `downloadAndOpen` + "Open with →" submenu, keyed off the
  row's own `rootCid`/`md5`/`filename`.

## Verification (local)

- `buildDagV3`→`reassembleV3` in-memory: **18/18 MD5-exact** across MOD/S3M/XM/IT (incl. 16-bit
  stereo), and v3's **streaming blocks are byte-identical to v2** (skeleton + per-sample PCM leaf
  CIDs) for every module — streaming provably unchanged.
- The transform crux was proven per-format before coding (identity/sign/delta/deinterleave).

## Rollout (gated — needs go-ahead)

v3 changes every parsed root's CID → a **re-bake** (`REBUILD=1`, `ingest.ts`) + catalog republish;
compressed-ITs move to v1 roots. Then ship the desktop build (old clients keep streaming v2 roots
until re-bake; after re-bake they stream v3 as v2 and just don't offer download until updated).

## Open

- Widen `enc` coverage if a re-bake surfaces a sample encoding outside the current bitmask (it
  falls back to resident/v1 safely meanwhile).
- External-app UX when a tracker isn't installed (currently: file still saved, `launched=false`).
