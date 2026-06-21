# Lab: seeking in a streamed module — findings

**Question.** How does the Mod Archive player handle seeking, and what must *we*
do to seek in a **streamed/repacked** module that doesn't yet have all samples?

**Setup.** Same headless libopenmpt (WASM) harness as [`FINDINGS.md`](FINDINGS.md).
18 real modules from `~/tmp/somemods`. IT random vol/pan neutralized (bytes
0x1A/0x1B zeroed) so renders are deterministic and comparisons are valid.

Script: `seek-mechanics.mjs`.

## How Mod Archive does it

No magic: its player (chiptune2/chiptune3) loads the **whole file into memory**,
then calls `openmpt_module_set_position_seconds` / `..._set_position_order_row`.
That's the entire trick — full file resident, one C call. The interesting work is
all on *our* side, where the file isn't fully present yet.

## Result 1 — seek *simulates from the start* (but it's cheap)

`set_position` cost grows with the target position on nearly every module — it is
**not** a constant-time jump; libopenmpt replays the module to rebuild channel
state. But the cost is trivial: **≤ ~7 ms even for the 31-minute
`beyond_the_network`**, sub-millisecond for typical songs.

| module | dur | seek@25% | seek@50% | seek@75% |
|---|---|---|---|---|
| hymn_to_aurora.mod | 253s | 0.13ms | 0.18ms | 0.21ms |
| beyond_the_network.it | 1857s | 1.58ms | 2.51ms | 3.59ms |
| DOPE.MOD | 515s | 1.31ms | 2.09ms | 3.67ms |

**Implication.** Seeking re-simulates pattern/effect state from order 0. That
consumes **pattern data** (always in the manifest — cheap) but needs **sample
PCM** only for notes that are actually *audible at the seek target* — not for
every instrument used between 0 and the target. So the data a correct seek
requires is small (≈ the set of notes currently held across channels), which is
exactly what makes streamed seeking tractable.

## Result 2 — seek is deterministic, and accurate for MOD/S3M

Seeking to the same `order:row` twice is **bit-identical**. Comparing seek-then-
render against the same order-boundary slice of a straight play-through (aligned
with a lag search to absorb sub-row offset):

| format | accuracy (sim, seek vs play-through) |
|---|---|
| MOD / S3M | **1.000** (sample-exact) |
| XM | ~0.67–0.95 |
| IT | ~0.66–0.85 |

MOD/S3M seeks reconstruct exact state. **XM/IT seeks are approximate** — libopenmpt
restores channel state but not byte-perfectly for envelope/NNA/filter-rich formats
(verified real, not a measurement artifact: the gap survives an ±8192-sample lag
search, and repeated seeks are bit-identical, so it's genuine seek-vs-play
divergence, not phase noise).

**This is a property of libopenmpt, not of streaming.** Mod Archive, holding the
whole file, gets the *same* approximate seek. Streaming can't do worse on accuracy
as long as we hand the engine the same bytes.

## Result 3 — random-seek packer: resident-set fetch (`seek-pack.mjs`)

Goal: faster time-to-playback on a **random seek into a not-yet-buffered module**.
At pack time, statically compute the **resident set** for a seek to order N: the
samples still *live* on each channel entering N (last note-on per channel, minus
cuts) plus the samples *triggered* in the first-audio window. A cold seek fetches
only those, calls stock `set_position_order_row(N,0)`, and plays — no whole-file
prefix.

Verified by building a resident-only module (others silenced), seeking it, and
comparing to the full-file seek. Same engine path ⇒ a complete resident set is
**bit-identical**. IT random vol/pan neutralized for determinism (it changes
volume/pan, never *which* sample plays, so the resident set is valid for real
playback too).

Two refinements over a naive "all live samples" set, both needed because the
crude set over-fetches the big sustained samples (50–95% of bytes):

1. **Drop stale notes.** A held one-shot whose tail already finished before N
   isn't needed. Walk 0→N with a tempo/speed timeline (`Axx`/`Txx`), track each
   note's onset time, and keep a held sample only if it loops or
   `now − onset < sampleDuration(note)`. (This timeline is also the
   second→`order:row` map the seek bar needs — one computation, two uses.)
2. **Bound the window** to the actual first-audio seconds, not a fixed pattern
   count.

Result — refined resident set is **6/6 bit-correct** and over-fetches only 0–7
samples vs. the true minimum. The true minimum is computable at pack time
empirically (silence each sample, see if the first 0.5s changes — exact because
channels sum and the render is deterministic), so the packer can ship it directly:

| IT | naive prefix | crude set | refined (static) | **minimal (bakeable)** |
|---|---|---|---|---|
| a-windf.it | 26 ms | 23 | 19 | **18 ms** (1.4×) |
| bz_pif.it | 521 ms | 469 | 364 | **294 ms** (1.8×) |
| beyond_the_network.it | 699 ms | 468 | 376 | **318 ms** (2.2×) |

(mean TTFPB over 6 seek points each, 50 Mbit. Naive prefix is the minimal
original-order download that makes the seek correct — 73–100% of the file, since
live samples are scattered to the end.)

## Result 4 — partial-sample (byte-range) fetch (`seek-partial.mjs`)

The manifest is **not** the floor (1.5–5.7% of a big IT — patterns + headers).
Samples are: even the minimal resident set is ~50% of the file because live
samples are large. But rendering the first 0.5 s only *reads a slice* of each
sample (a held one-shot from its play position; a looped sample around its loop).
Block-probing each needed sample (zero a block, see if the window audio changes):

| IT | seek | needed samples | full bytes | **read bytes** | read% | TTFPB full→partial |
|---|---|---|---|---|---|---|
| bz_pif.it | order 28 | 17 | 2056K | 1033K | 50% | 345 → **178 ms** |
| beyond_the_network.it | order 207 | 11 | 1312K | 628K | 48% | 256 → **144 ms** |

Only **48–63%** of needed-sample PCM is touched in the first window, so shipping
byte *ranges* (and rewriting sample length/loop in the manifest) buys another
~1.6–1.9× on top of the resident set.

**Combined: cold random-seek TTFPB drops ~3× vs. the naive prefix** (up to ~7× on
late seeks), with stock libopenmpt — no fork. e.g. bz_pif ~530 → ~160 ms, beyond
~700 → ~100–240 ms.

## Packer recommendation (random seek)

- Bake a **timing map** (per order + tempo change) → instant `T ↔ order:row` and
  the staleness/window timing.
- Bake, per seek checkpoint, the **minimal resident set** (computed at pack time
  with the deterministic engine); fall back to the static refined heuristic if
  pack-time rendering is too costly. Reference samples by opaque id (the existing
  manifest seam).
- For the largest wins, bake **per-sample byte ranges** for each checkpoint and
  have the client assemble sparse samples (length/loop rewritten), streaming the
  remainder via normal lookahead.
- A fork is still *not* required for fast seeks; it remains the route only to
  sample-*exact* XM/IT seek and instant scrub (see "fork" discussion), which are
  post-MVP polish.

## Reproduce

```
cd lab
npm install
node seek-mechanics.mjs   # seek cost vs target + seek-vs-playthrough accuracy
node seek-pack.mjs        # random-seek resident-set packer: crude vs refined vs minimal
node seek-partial.mjs     # partial-sample (byte-range) read fraction headroom
```
