# Repack v2 — Manifest schema (locked)

Status: **locked, pre-code** · 2026-06-25 · The "first concrete deliverable" of
Phase 2 (STREAMING-PARITY.md §2.1). Code against this; changes here are schema
breaks and must bump nothing else (the manifest CID *is* the version — `v` is the
in-band discriminant).

This supersedes the v1 manifest (`packages/repack/src/dag.ts:45`) for parsed
modules. **v1 is not deleted** — `mo3` / unparseable modules keep shipping as v1
flat DAGs, and shipped clients keep playing v1 roots (STREAMING-PARITY.md §2.6).
Clients dispatch on `manifest.v`.

---

## What changed from v1, in one breath

v1 reconstructs the **original file bytes** byte-exact: `samples[]` are keyed by
**file offset**, PCM is stored **on-disk** (compressed-IT skipped into the
skeleton), checkpoints are baked as **indices into `samples[]`** (an offset↔index
translation seam, `dag.ts:188`), and only **IT/MPTM** have a checkpoint table.

v2 stops reconstructing the file. It feeds **decoded PCM by libopenmpt sample
index** into one immortal instance. So:

- **Single key everywhere: the 1-based libopenmpt sample slot.** The offset↔index
  seam is gone. `samples[].index`, `plan.checkpoints[].samples`, and the
  `provide_sample(mod, index, …)` arg are all the same integer.
- **PCM is stored decoded**, in native `ModSample` in-memory layout (the bytes
  `provide_sample` memcpy's). Extracted at bake via the `debug_sample_*`
  accessors. Canonical dedup now collides across delta-MOD vs raw-IT copies of
  the same instrument (v1's on-disk bytes couldn't).
- **The skeleton is normalized**: sample headers rewritten to *uncompressed*, PCM
  length = *decoded* frames, PCM bytes *zero-filled*. `create_from_memory` loads
  it via the stock path; the zeros CDC-dedup to ~one chunk corpus-wide.
- **Checkpoints extend to all parsed formats** (MOD/S3M/XM/IT/MPTM), full
  resident set per strided order, keyed by sample index.
- **The leaf index is pre-resolved**: each sample's chunk CIDs are inlined in the
  sample table, so the up-front index fetch already names every leaf the client
  will ever want — prefetch issues batched want-lists with zero per-sample
  round-trips.
- Dropped: `originalLength`, per-sample `offset`/`length`, the `pcmRoot`
  indirection node, the separate `segment0` field (it's just `checkpoints[0]`).

---

## Object tree

```
manifest (dag-cbor 0x71, root)
  v, format, cdc
  skeletonChunks: [CID]              → raw leaves (normalized skeleton bytes)
  index: INLINE  |  indexRoot: CID   → exactly one is present (spill rule below)
     samples: [SampleV2]             → each carries its own [CID] leaf list
     plan:    PlanV2
  chunk (raw leaf 0x55)              → decoded native-layout PCM, or skeleton bytes
                                        SHARED across modules
```

`index` (the `{ samples, plan }` pair) lives **inline in the manifest** by
default. If the encoded manifest would exceed **`INDEX_SPILL_BYTES` (256 KiB,
provisional)**, the index is emitted as its own dag-cbor block and the manifest
carries `indexRoot: CID` instead — one extra hop, still fetched fully before any
audio. Exactly one of `index` / `indexRoot` is present. (256 KiB is well under
kubo's block ceiling and covers all but pathological many-sample MPTMs; tune
against the corpus during bake.)

---

## Types

```ts
const MANIFEST_V2 = 2;

interface ManifestV2 {
  v: 2;
  /** "mod" | "it" | "s3m" | "xm" | "mptm". mo3/unparseable never reach v2. */
  format: Format;
  /** Informational: CDC params used for skeleton + PCM chunking. */
  cdc: CdcConfig;
  /**
   * Normalized skeleton, CDC-chunked: a structurally-valid module whose sample
   * headers declare UNCOMPRESSED PCM of the DECODED frame length, PCM bytes
   * zero-filled. create_from_memory(concat(skeletonChunks)) yields the immortal
   * instance with every sample pending. Zero-runs dedup to ~one chunk.
   */
  skeletonChunks: CID[];

  /** Inline index (default). Exactly one of index / indexRoot is present. */
  index?: IndexV2;
  /** Spilled index (manifest exceeded INDEX_SPILL_BYTES). Fetch right after the
   *  manifest, before audio. The block decodes to IndexV2. */
  indexRoot?: CID;
}

interface IndexV2 {
  samples: SampleV2[];
  plan: PlanV2;
}

interface SampleV2 {
  /** 1-based libopenmpt sample slot. THE key. == provide_sample's sample_index,
   *  == the integers in plan.checkpoints[].samples. Slots with no PCM are
   *  omitted entirely (never appear here or in any checkpoint). */
  index: number;
  /** Decoded nLength (sample frames). == provide_sample's `frames` arg. */
  frames: number;
  /** Native ModSample interleave: 1 (mono) | 2 (stereo). */
  channels: number;
  /** Native sample bit depth: 8 | 16. PCM byte length = frames*channels*bitDepth/8.
   *  (Guard-sample padding is re-applied inside provide_sample; not stored.) */
  bitDepth: number;
  /** Ordered raw-leaf CIDs of this sample's DECODED native-layout PCM.
   *  Concatenated in order == the exact provide_sample buffer. This is the
   *  pre-resolved leaf index — no pcmRoot hop. */
  chunks: CID[];
}

interface PlanV2 {
  /**
   * T<->order map for seek-by-seconds. Cumulative seconds at the start of each
   * valid order. Computed for ALL parsed formats in v2 (v1 had it for IT only).
   */
  orderSeconds: { order: number; seconds: number }[];
  /**
   * Full resident set per strided order (NOT delta-encoded — residency is
   * non-monotonic). checkpoints[k].samples = every sample index still AUDIBLE
   * at that order (held/looping notes still ringing) UNION a ~0.5s forward
   * window of imminent triggers. Strided to <= MAX_CHECKPOINTS (512).
   *
   * Both consumers read this directly:
   *   - fence:    before entering order N, require checkpoint(N).samples all provided.
   *   - prefetch: union checkpoint(N..N+lookahead).samples, batched want-list.
   *   - cold-seek to N: fetch checkpoint(N).samples, set_position_order_row(N,0).
   * checkpoints[0] subsumes v1's segment0 (the opening resident set).
   */
  checkpoints: { order: number; samples: number[] }[];
}
```

### Reading a checkpoint for an arbitrary order

Checkpoints are strided, so order N may not have its own entry. The lookup is
**floor**: `checkpoint(N)` = the entry with the greatest `order <= N`. Because the
resident set is computed as "audible at this order plus a forward window," the
floor entry is a conservative (superset) cover for the orders between it and the
next checkpoint — the fence can only over-wait, never under-fetch. (Same
floor-lookup v1's `seek_module` already does, `ipfs.rs:500`.)

---

## Invariants this schema encodes

1. **Samples are atomic.** A `SampleV2` is provided only when all its `chunks` are
   resident; no sub-sample progressive fill. The fence keys on whole-sample
   provision.
2. **State is free, audio is fenced.** The skeleton alone is enough to load,
   seek, and compute position exactly. Only *sounding* a sample waits — and only
   via `checkpoints`. Nothing in the schema gates seeking.
3. **One key.** Every sample reference in the manifest is the libopenmpt slot
   index. No offsets, no `samples[]`-position indirection.
4. **Conservative by construction.** Checkpoints over-count (forward window +
   reuse-last + held-note over-approximation per `seek.ts`), so the fence can
   delay but never chop. Under-counting would re-introduce the chopped opening.

---

## Bake-side work this schema implies (for the §2 build, not part of the schema)

- **Decode + dump PCM per sample** via `debug_sample_{data,bytes,frames}` at
  ingest (wire into `apps/server/src/meta.ts`, which today extracts metadata
  only). `.slice()` out of the heap before the next malloc (see
  `test/provide-sample.mjs`).
- **Normalize the skeleton**: rewrite each format's sample headers to
  uncompressed / decoded-length / zero PCM. The zeroing locators already exist in
  `test/provide-sample.mjs`; the *header rewrite* (esp. clearing the IT
  compression flag and resizing to decoded frames) is new and is the one
  remaining open risk for compressed-IT (STREAMING-PARITY.md Risks).
- **Extend checkpoint computation to MOD/S3M/XM** (decision 1): port the IT
  held-note + timing simulation (`seek.ts computeItTables`) to the MOD/S3M/XM
  decoders, which today only produce `segment0`. Output `orderSeconds` +
  full `checkpoints` for all five parsed formats.
- **Drop the offset→index translation** in `dag.ts:188` — checkpoints are emitted
  as sample indices directly from the simulation.

## Decisions locked here

- Single key = libopenmpt sample index; offset/length/pcmRoot/originalLength
  dropped; PCM stored decoded native-layout.
- Checkpoints: **full resident set per strided order** (non-delta), extended to
  **all parsed formats**.
- Index **inline by default, spill to `indexRoot` object above
  `INDEX_SPILL_BYTES`** (256 KiB provisional).
- `segment0` folded into `checkpoints[0]`.

## Still open (not schema-blocking; pick before the consuming code)

- `INDEX_SPILL_BYTES` final value — measure against the corpus.
- Fence **lookahead** (orders vs seconds) + rebuffer **hysteresis** threshold —
  client-side tuning, default "next ~2–3 checkpoints resident."
- Small-module fast-path cutoff (fetch whole DAG, full-load) — also the
  mitigation for a single sample larger than the prefetch lead.
