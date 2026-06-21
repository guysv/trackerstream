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

## Open question for the next probe — "what samples must be resident to seek to order N?"

Result 1 says a correct post-seek render needs PCM only for the notes audible at
the target. The next step is to make that precise and verify it:

- statically walk the pattern/order stream from 0→N, tracking per channel the
  current (last-triggered, not-yet-cut) sample → the **resident set** at N;
- build a module containing only those samples (rest zero-filled), seek to N,
  and confirm the post-seek opening matches the full-file seek;
- size the resident set vs. channel count and vs. "everything used up to N".

If the resident set is ~channel-count small (expected), streamed seeking is a
matter of: fetch those few segments, `set_position`, play — while normal lookahead
fetches the rest. That's the client-side seek strategy for Phase 2/4.

## Reproduce

```
cd lab
npm install
node seek-mechanics.mjs   # seek cost vs target + seek-vs-playthrough accuracy
```
