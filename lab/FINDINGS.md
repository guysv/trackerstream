# Lab: non-fork streaming viability (WASM) — findings

**Question.** Can we hit low *time-to-first-pattern-playback* (TTFPB) by streaming a
module to the client and playing it before the whole file arrives, **without forking
libopenmpt** — i.e. by feeding stock libopenmpt a growing buffer and recreating the
module as bytes arrive?

**Setup.** Stock libopenmpt (the WASM build bundled by `chiptune3`, current upstream)
driven headless from Node via its C API. 18 real modules from `~/tmp/somemods`
(20 KB S3M → 4.5 MB IT). Audio rendered at 48 kHz; a truncated render is "correct"
when its RMS-similarity to the full-file render is ≥ 0.98.

Scripts: `lib.mjs` (C-API wrapper), `ttfpb.mjs` (truncation sweep, all formats),
`mod-repack.mjs` (repack proof-of-concept on MOD).

## Result 1 — the mechanism works

- **`create_from_memory` tolerates truncation.** It succeeds from ~1% of the file for
  every module tested; missing sample data plays as silence, no crash/garbage.
- **Recreate is cheap.** Full-file parse is 0.9–25 ms (≤8 ms for everything under 1 MB;
  25 ms for the 4.5 MB IT). Recreating on each arriving segment costs ~nothing.

So "grow the buffer, recreate, keep playing" is mechanically sound on **stock** libopenmpt.

## Result 2 — original byte order is the bottleneck

Minimal prefix (original file order) for a *correct* 5 s opening:

| module | size | min-correct prefix | TTFPB @50 Mbit | whole-file |
|---|---|---|---|---|
| a-windf.it | 198 K | 45% | 17 ms | 35 ms |
| celestial_fantasia.s3m | 421 K | 22% | 17 ms | 71 ms |
| elw-sick.xm | 783 K | 34% | 51 ms | 136 ms |
| bz_pif.it | 3.3 M | **94%** | 514 ms | 549 ms |
| beyond_the_network.it | 4.5 M | **94%** | 708 ms | 748 ms |

Small/medium modules already win. But the **large ITs — the cases that actually hurt —
need ~94% of the file** before the opening is correct, because sample data is stored
scattered/at the end while the opening uses only a few instruments. Original-order
streaming barely beats downloading the whole file.

## Result 3 — repack closes the gap (proven on MOD, stock engine)

`mod-repack.mjs`: parse the MOD, find the samples the **first pattern** triggers, present
**only those** sample-data blocks (zero-fill the rest = "not transferred yet"), and render
with stock libopenmpt.

| module | ch | samples used/total | bytes transferred | sim(repack) | sim(same bytes, orig order) | TTFPB orig→repack |
|---|---|---|---|---|---|---|
| tempest-acidjazz.mod | 4 | 10/19 | 86% | **1.000** | 0.673 | 33→11 ms |
| hymn_to_aurora.mod | 4 | 5/12 | 66% | **1.000** | 0.117 | 20→13 ms |
| ELYSIUM.MOD | 4 | 5/16 | 64% | **1.000** | 0.589 | 22→14 ms |
| space_debris.mod | 4 | 3/17 | 39% | 0.937 | 0.247 | 57→23 ms |
| GSLINGER.MOD | 4 | 5/31 | 25% | **0.984** | 0.341 | 67→18 ms |
| DOPE.MOD | 28 | 2/23 | 62% | **0.989** | 0.000 | 162→104 ms |

- **`sim(repack) ≈ 1.0`** → stock libopenmpt renders the first pattern correctly with only
  the needed samples present. The mechanism needs **no engine fork**.
- **`sim(same bytes, original order) ≪ 1.0`** → the identical byte budget in file order is
  broken. The win is the **reordering**, not just "send less."

(Two files land at 0.86/0.94: the first-pattern sample scan doesn't yet handle MOD's
"sample-number 0 = reuse last sample on channel" case, so it under-counts needed samples.
Detection bug, not a mechanism limit.)

## Result 4 — IT, including the large worst cases (the point of the exercise)

MOD was a proxy; the modules that actually hurt are the big ITs.

**Methodology correction.** A first attempt (`it-repack.mjs`) detected needed samples
*empirically* — silence a sample's header, see if the opening's audio changes. This is
**invalid**: libopenmpt's render is **not bit-deterministic** for modules that use IT
random volume/pan variation. Two renders of the *same bytes* of `beyond_the_network`
match at only ~0.97 similarity (`it-static.mjs`), so the audio-diff method's noise floor
flagged nearly every sample as "needed" (a bogus 47/47). `bz_pif` renders bit-identically,
which is why it gave the correct answer there. **Ground truth is the pattern data**, not
the audio.

`it-static.mjs` / `it-repack2.mjs` decode the first pattern directly — instruments
actually triggered → samples those instruments map for the notes played — then build a
"needed-only" module and verify (with a seeded RNG to cut the noise):

| IT | size | instruments used | samples | repack transferred | orig prefix | TTFPB orig→repack |
|---|---|---|---|---|---|---|
| a-windf.it | 197 K | 3/15 | 3 | 37% | 45% | 40→37 ms |
| **bz_pif.it** | 3.3 M | 4/14 | **4** | **29%** | 94% | **514→164 ms (3.1×)** |
| **beyond_the_network.it** | 4.5 M | 11/95 | **10** | **41%** | 98% | **737→324 ms (2.3×)** |

`beyond_the_network`'s first pattern uses **11 instruments → 10 samples**, not 47 — repack
cuts its 98% worst case to 41%, ~2.3× faster.

**Clean verification (`it-verify.mjs`).** Zeroing each instrument's Random-Volume (0x1A)
and Random-Pan (0x1B) bytes removes the only per-note randomness, making the render
BIT-IDENTICAL across runs. With a tight first-pattern boundary, the only-needed render then
matches the full render **exactly**:

| IT | samples (static) | sim(only-needed vs full) |
|---|---|---|
| a-windf.it | 3 | **1.00000** |
| bz_pif.it | 4 | **1.00000** |
| beyond_the_network.it | 10 | **1.00000** |

So the statically-detected first-pattern sample set is exactly right — the earlier 0.959 was
(a) random-variation noise and (b) a ~21 ms render overshoot into the next pattern, not
missing audio.

**Takeaway:** repack is effective across all large ITs tested (2.3–3.1× faster to a correct
opening). Effectiveness scales with how sparse the opening is; front-loading the
first-pattern samples is the lever. (Static pattern decode is the correct detector; audio
diffing is not, for modules with random variation.)

## Conclusion

The **non-fork approach is viable** and meets the goal:

1. libopenmpt tolerates partial buffers and recreates cheaply → stream + recreate is free.
2. The only thing standing between us and low TTFPB on big modules is **byte order**.
3. A **repack that front-loads first-pattern instruments** drops TTFPB substantially with
   **stock** libopenmpt — no fork required. The repack container = manifest (headers +
   patterns + order) followed by sample-data blocks in playback-need order; the client
   reassembles a valid module with not-yet-arrived samples zero-filled and recreates as
   blocks land.

The **size** of the win is module-dependent (Result 4) but consistently large for the
files that hurt: 2.3–3.1× faster to a correct opening on the big ITs.

**Recommendation.** Drop the fork from the critical path. Pursue:
1. A real repacker (MOD/S3M/XM/IT) that **statically decodes the first pattern(s)** to find
   the instruments→samples needed, and front-loads those sample blocks after the manifest.
2. Client-side "reassemble with not-yet-arrived samples zero-filled + recreate on segment
   arrival"; seek recreates with the segments needed at the target order.
3. For richer openings, tier the tail (required-now / prefetch / on-demand per the README)
   so playback starts on the first-pattern set and the rest streams in.

Revisit a fork only if a format's reassembly proves infeasible — none seen so far.

**Methodology note for future work:** verify repack correctness by **static pattern
decode**, not audio diffing — libopenmpt rendering is non-deterministic for modules using
IT random vol/pan variation (~0.97 self-similarity), which silently corrupts audio-based
detection.

## Reproduce

```
cd lab
npm install
node ttfpb.mjs       # truncation sweep across all formats
node mod-repack.mjs  # repack proof-of-concept on MOD
node it-static.mjs   # static first-pattern decode + determinism check
node it-repack2.mjs  # IT repack sizing via static detection
node it-verify.mjs   # AUTHORITATIVE: clean exact verification (randomness neutralized)
# superseded (kept for the record — empirical audio-diff method, invalid under non-determinism):
#   it-repack.mjs, it-window.mjs, it-impact.mjs
```
