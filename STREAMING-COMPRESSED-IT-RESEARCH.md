# Beating full-load TTFP for compressed-sample IT modules with streaming

Status: **research proposal** · Author: design discussion 2026-06-26 ·
Baseline branch: `research/compressed-it-streaming` · Depends on the v2
immortal-instance + fence (see `STREAMING-PARITY.md`,
`STREAMING-PARITY-V2-SCHEMA.md`).

## Abstract

IT modules saved by IT 2.14+/OpenMPT compress their sample data (header flag
`0x08`). The v2 bake deliberately **does not stream** compressed samples — it
can't carve a raw-PCM region from compressed bytes — so they ride whole in the
skeleton (`itLikeSlots` skips `flg & 8`). A corpus scan found this is **183 of
281 IT roots (65%)** baking to *zero* streamed samples. We hypothesised these
were a streaming-coverage gap and tried to stream them. They are **not** a gap:
when we did stream them, cold time-to-first-playable (TTFP) got **worse**, not
better. This paper records exactly why, models the crossover, and proposes the
experiments that could actually make streaming win for this module class.

## Background

TTFP = wall-clock from stream start until the client fence opens at order 0:
skeleton resident **and** every sample in `requiredAt(0)` (floor∪next checkpoint
union) resident, fetched cold over Bitswap. Two regimes:

- **Full-load (today, for compressed ITs):** the skeleton *is* the whole compact
  compressed module. One mostly-batched fetch → play. Few round-trips.
- **Streaming:** fetch a normalized skeleton (sample regions zeroed) + the
  `requiredAt(0)` decoded-PCM samples. Fewer skeleton bytes, but extra per-sample
  fetches.

## What we measured (the negative result)

We implemented compressed-sample streaming end-to-end, bit-exact (oracle
`provide-v2` parity `0.00e+0` on ctf/aft0hrs/acsid/energ + uncompressed
controls; fence coverage clean; uncompressed roots byte-identical). Two variants,
both deployed to prod and benchmarked cold, then reverted:

| ctf.it (505 KB on disk) | skeleton (distinct, deduped) | order-0 samples | cold TTFP (3 runs) |
|---|---|---|---|
| **Full-load (0-streamed)** | 505 KB | — | **981 / 1035 / 972 ms** |
| Streaming, naïve | 1028 KB (kept dead compressed bytes!) | 3 / 21 KB | 2727 ms |
| Streaming, zeroed-orphan | **307 KB** | 3 / 21 KB | **1744 / 1615 / 1805 ms** |

The "zeroed-orphan" variant (zero the orphaned compressed region so it dedups to
a shared zero-block; append an uncompressed zero-fill region of the decoded
length + repoint the header + clear `0x08`) **did** shrink the skeleton fetch
(829 ms vs 981 ms). But TTFP still **lost by ~700 ms**, because fetching the 3
order-0 samples added **~900 ms** — and that is pure round-trip latency, not
bytes (21 KB shouldn't cost ~900 ms). The client (`ipfs::stream_v2`) fetches
samples **sequentially**, one `assemble`/`fetch_many` per sample, on a cold
Bitswap session.

### Root cause

Cold TTFP ≈ `skeleton_fetch + Σ order0_sample_fetches`. For a *compressed* IT:

1. **Bytes go the wrong way.** Decoded PCM is *larger* than the compressed
   source (ctf: streamed PCM 522 KB vs 505 KB whole file). The on-disk compressed
   module is already the compact representation; streaming transfers *more* total.
2. **Round-trips dominate at small sizes.** Skeleton savings were ~150 ms; even a
   single order-0 sample batch costs ~300 ms of cold-session round-trip. Below
   the crossover, latency beats byte savings.
3. **Sequential fetch multiplies it.** N order-0 samples = N serial round-trips
   today, not one batch — despite the v2 index pre-resolving every leaf CID
   (the schema *claims* "batched want-lists with zero per-sample round-trips";
   the implementation doesn't do it).

**Conclusion:** the original design keeping compressed samples resident is
correct for the common (small/medium) case. Streaming can only win where the
*unfetched* fraction at order 0 is large enough to repay the extra round-trips.

## Crossover model

Let `S_full` = full module bytes, `S_skel` = streamed-skeleton distinct bytes,
`B0` = order-0 required sample bytes, `T` = effective throughput, `r` = cold
round-trips added by streaming, `L` = per-round-trip latency. Streaming wins iff

```
(S_full − S_skel − B0)/T  >  r·L
```

For compressed ITs `B0` is *decoded* (inflated) and `r` is large (sequential),
so the LHS is small/negative and the RHS large. The levers below each attack one
term.

## Hypotheses (levers)

- **H1 — Batched order-0 prefetch (drives `r`→~1).** Issue ONE Bitswap want-list
  for the skeleton chunks ∪ all `requiredAt(0)` sample-leaf CIDs (all known
  up-front from the index). Expected the single biggest win; likely necessary
  but maybe not sufficient for compressed ITs (still pays the byte penalty).
- **H2 — Warm session (drives `L`).** The real player holds a persistent master
  peer; the cold-node-per-track bench over-charges handshake latency. Measure
  the warm delta to know how much of the loss is a bench artifact.
- **H3 — Minimize the order-0 set at bake (drives `B0`).** Checkpoint placement
  so order 0 requires the fewest/smallest samples; demote rarely-needed openers.
- **H4 — Stream COMPRESSED bytes, decode on the client (drives the byte term
  decisively).** Instead of streaming decoded PCM (larger), stream the *original
  compressed* sample bytes (smaller than full-load's whole-file, and smaller than
  decoded), and add a `provide_compressed_sample` path that decompresses into the
  ModSample buffer client-side. This makes streaming transfer *less* than
  full-load by construction. Requires an IT215/214 decompressor in the
  immortal-instance fork (the bake already walks compressed block lengths —
  `itCompressedSpan` — so the carve is solved; only the client decode is new).
  **This is the most promising path to a real win.**
- **H5 — Smaller skeleton.** The normalized skeleton is structure + zeros; verify
  CDC + cross-module zero-block dedup is optimal, and that headers/patterns
  (the irreducible part) aren't themselves the floor.

## Experimental plan

- **E0 — Reproduce + instrument.** Re-enable the `research/compressed-it-streaming`
  bake (flag-gated), re-bake a sample set, capture the per-phase breakdown
  (manifest, index, skeleton, each order-0 sample) with timestamps. Confirm the
  ~900 ms is `r` serial round-trips.
- **E1 — Implement H1** in `ipfs::stream_v2`: a single `fetch_many` over skeleton
  ∪ order-0 leaves before the per-sample loop. Re-measure ctf + the size sweep.
- **E2 — Crossover sweep.** Bake a size-graded set of compressed ITs both ways
  (full-load vs streaming+H1); plot TTFP vs `S_full` and vs order-0 fraction;
  locate the break-even module size. (The largest 0-streamed corpus root is 219
  skeleton chunks ≈ multi-MB — the most likely first winner.)
- **E3 — Warm vs cold (H2).** Repeat E1 with a shared persistent node
  (`--shared-node` already exists) to separate handshake from transfer.
- **E4 — H4 prototype.** Add `provide_compressed_sample` to the libopenmpt fork;
  bake streaming the compressed bytes; prove bit-exact via the oracle; measure.
  If TTFP < full-load across the size range, this is the answer.

## Success criteria

Streaming (with the winning lever set) yields **cold TTFP < full-load** for a
defined module class (e.g. compressed ITs above the crossover size), with **zero
parity regression** (oracle `provide-v2` bit-exact) and **fence coverage intact**
(every streamed sample in a checkpoint). Net wire bytes to first-playable must
not exceed full-load.

## Methodology & threats to validity

- Gate: the `provide-v2` wasm oracle (bit-exact render) on every bake change;
  fence-coverage check (no streamed sample absent from all checkpoints).
- Bench: `examples/bench.rs` (TTFP) + `examples/seek.rs` (cold-seek), cold
  node-per-track, `--jobs`/`--ttfp-only`/`--shared-node`/`--seek` (this session).
- Threats: network variance (multi-run medians); cold-session warmup (H2
  isolates it); the master's per-subnet ConnLimiter caps single-IP parallelism
  (keep `--jobs` low) — see `[[master-connection-storms]]`.

## Artifacts from this session

- Streaming bake (experimental baseline; E0 adds the off-by-default gate before any
  prod consideration): `packages/repack/src/{parse,dag,seek}.ts` on
  `research/compressed-it-streaming` — `itLikeSlots` emits compressed slots,
  `itCompressedSpan` walks IT215 block lengths, `buildSkeletonV2` zeros-orphan +
  appends + repoints, `buildDagV2` streams only plan-covered candidates.
- TTFP/seek benchmark harness: `apps/desktop/src-tauri/examples/{bench,seek}.rs`
  + `examples/common/`, with `PlanV2::required_at(order)` + tolerant `orderSeconds`.
- Corpus fact: 183/281 IT roots are 0-streamed (all-compressed); largest ≈ multi-MB.
