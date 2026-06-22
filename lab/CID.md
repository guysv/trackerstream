# Lab: module → content-addressed (CID) DAG, and cross-module sample dedup

**Question.** If we decompose modules into content-addressed blocks (a Merkle
DAG) and dedup identical sample PCM across the whole archive, how much do we
actually save — and at what granularity should blocks live?

**Why it matters.** Prior labs established samples are **~94–98% of a module's
bytes** (the manifest — orders/patterns/headers — is 1.5–5.7%). So *all* dedup
payoff is at the sample-PCM level. The tracker scene reuses samples heavily
(Amiga ST-0x sample disks, ripped drum hits, an artist's own palette). This lab
measures that reuse on the **full local Mod Archive** (52 GB, double-zipped),
byte-exact — no fuzzy audio matching, because the streaming story depends on
bit-exactness.

Script: `cid-dedup.mjs`. Parsers for MOD/IT/XM/S3M (~97% of files) extract each
sample's **raw PCM payload** (format header stripped — loop points, C5Speed,
name and flags vary per module even when the PCM is identical, so you must
address the payload, not the format's sample blob).

## Method note — dedup is a *collision* property, scan densely

A sparse stride (every Nth module) **severely undercounts** dedup: sampling
0.4% of the corpus sees ~0.4%² of potential collisions. Confirmed empirically —
a stride-50 sample reported 1.2% whole-sample dedup; a *contiguous* 5k-module
block of the same corpus reported 8.1%; the full corpus, 29.8%. You must scan
contiguously (ideally everything). The live ratio climbs monotonically with
module count and only flattens near full coverage:

```
  2k modules →  9.8%      40k → 21.1%      90k → 27.3%
 10k modules → 14.4%      60k → 23.6%     108k → 28.6%
 20k modules → 16.3%      72k → 25.3%     122k → 29.8% (final)
```

## Result — full-corpus dedup (122,272 modules, 48.4 GB sample PCM)

| metric | total | unique | **redundant** |
|---|---|---|---|
| **whole-sample** (byte-identical PCM payloads) | 48.4 GB | 33.9 GB | **29.8%** |
| **sub-sample** (FastCDC 512/2048/8192) | 48.4 GB | 28.7 GB | **40.6%** |

- 1,959,192 samples → 1,202,025 unique. **757,167 sample copies are redundant.**
- 1,084 modules skipped (15-sample MODs / exotic tags / non-parsed formats), **0 errors**.

Per-format sample-byte mass (XM dominates the corpus by bytes):

| fmt | modules | samples | sample PCM |
|---|---|---|---|
| mod | 67,691 | 907,577 | 8.3 GB |
| xm | 36,049 | 715,020 | 27.7 GB |
| it | 9,924 | 193,863 | 10.1 GB |
| s3m | 8,608 | 142,732 | 2.3 GB |

Most-reused PCM payloads (copies × size) — the classic shared sample-disk/drum hits:

```
  1375 ×  0.9KB     1002 ×  40.3KB  ← ~40 MB collapses to 40 KB
  1198 ×  2.0KB      453 ×   8.3KB
  1054 ×  0.0KB      432 ×  25.8KB
                     412 ×  21.5KB
```

## Two readings of the number

1. **CDC beats whole-sample by ~11 points (40.6% vs 29.8%).** Whole-sample CID
   catches exact rips; the gap is the *trimmed/edited* family — same source
   sample cut shorter or extended, not byte-identical as a blob but sharing
   interior chunks. Content-defined chunking catches those because boundaries
   follow content, not byte offset.
2. **40.6% is a lower bound.** Byte-exact only; cross-format reuse (the same
   sound stored as MOD signed-8-bit vs XM delta-coded) is *not* counted, and
   CDC params weren't tuned (smaller avg chunk → more dedup, more overhead).

## Design — the DAG that falls out of this

Content-address at the **PCM-chunk** level. Whole-module CID ≈ 0% dedup (every
module is unique); whole-sample CID = 29.8%; **chunk CID = 40.6%** — and the
chunk is *also* exactly the unit the seek/repack labs want for partial/streamed
fetch (a byte-range → a covering run of chunks). One chunking scheme, two
payoffs: dedup **and** streaming.

```
module-root (manifest — tiny, fetched first):
  format + global params, order list
  pattern-block CIDs        ← module-unique (no cross-module dedup)
  instrument-block CIDs      ← mostly module-unique
  sample table: per sample → { header inline (loop/C5/flags/name), pcm-root CID }
  seek tables (timing map + per-checkpoint resident sets)   ← from lab/SEEK.md
pcm-root (per sample): [ chunk CIDs ] + length    ← fetched when that sample is needed
chunk (leaf): raw PCM bytes, content-addressed     ← SHARED across modules
```

Key decisions, each forced by a measured fact:
- **Address the PCM payload, not the sample blob.** Headers vary per module; only
  the payload dedups. (Addressing the blob → ~0%.)
- **Chunk-level (CDC), not whole-sample.** +11 points, and it's the seek-lab's
  partial-fetch unit.
- **Two-level manifest.** Root lists one `pcm-root` CID per sample (cheap); the
  chunk list lives under the pcm-root and is fetched only when that sample is
  needed. Keeps cold-start manifest tiny — a 10 MB-sample IT would otherwise
  inline ~5,000 chunk CIDs (~180 KB) into the root. Need-based fetch already
  drives which pcm-roots to resolve (repack/seek resident set).
- **Client reassembles.** The DAG is a storage/delivery representation; the
  client materializes a valid module buffer (manifest + fetched chunks) before
  `create_from_memory` — proven viable on stock libopenmpt (`lab/FINDINGS.md`).
  **No engine change.**

Chunk-index overhead is negligible: ~14M unique chunks corpus-wide × ~32 B CID ≈
450 MB of index against 48 GB of PCM — and it buys back ~19.6 GB.

## What this is and isn't

- **Is:** server-side **storage + origin-bandwidth** dedup — store/serve the
  unique chunk set once (28.7 GB instead of 48.4 GB of sample PCM, −40.6%).
- **Isn't (yet):** client per-session **transfer** savings. Those depend on
  cache *locality* — how often the samples in the tracks a user actually plays in
  sequence recur. The global pool says the redundancy exists; a warm-cache
  simulation over realistic listening sessions (same artist / scene / playlist)
  is the natural follow-up, and it compounds with the seek/repack resident-set
  fetch (those bytes become cache hits).

## Bearing on the MVP

The MVP defers CID/IPFS dedup ("a later swap of the delivery backend") and keeps
"instruments referenced by opaque id — the seam where IPFS substitutes CIDs."
That seam is now **quantified**: swapping opaque ids for chunk CIDs reclaims
~30–41% of sample storage and origin bandwidth, byte-exact. Still post-MVP, but
the payoff is no longer a guess.

## Reproduce

```
cd lab
npm install
# full corpus, all four formats (~9 min, peaks a few GB RAM):
EVERY=1 LIMIT=0 FORMATS=mod,xm,it,s3m node --max-old-space-size=16384 cid-dedup.mjs
# fast contiguous slice (seconds): EVERY=1 LIMIT=5000 node cid-dedup.mjs
# env: EVERY (stride; use 1 — striding undercounts), LIMIT (0=all),
#      FORMATS (csv), CDC (1/0)
```
