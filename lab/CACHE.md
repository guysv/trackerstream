# Lab: warm-cache simulation — per-session client *transfer* savings

**Question.** [`CID.md`](CID.md) measured **global** dedup — store/serve the
unique chunk set once (~41% sub-sample redundancy corpus-wide). That is a
**server storage / origin-bandwidth** number. It says nothing about what a
*client* actually downloads in a listening session, because that depends on cache
**locality**: how often the chunks of the next track are already resident from
earlier tracks. CID.md flagged this as the open follow-up. This lab closes it.

**Why the cache is free.** The persistent rust-ipfs blockstore *is* the client
cache. Once a chunk CID is fetched for any module it stays local, so a later
module that shares that CID is a **local hit by construction** — no second
download, no engine change. The only question is how often that happens over a
realistic session.

Script: `warm-cache.mjs`. It builds the **real production DAG** for each sampled
module via repack (`buildDag` / `buildFlatDag`), so the block CIDs are exactly
the ones the client fetches (CDC `8K/16K/64K`, the locked Phase-1 transport
default — *not* the 512/2048/8192 the dedup lab measured at, so absolute dedup is
lower here by construction). It then models a client with an **empty** persistent
cache that plays a SESSION (an ordered list of modules), and per module records:
total DAG bytes, bytes already resident (CIDs seen earlier this session), and
**NET bytes transferred**.

## Method — sessions, and what "affinity" means here

A session is `SESSLEN` modules played in order. We compare four
session-construction strategies, each over `SESSIONS` independent sessions:

| strategy | how the session is built | models |
|---|---|---|
| **random** | `SESSLEN` distinct modules, uniform | the cache-locality floor |
| **browse order** | contiguous run in corpus (filename) order | scrolling/playing a page of the archive |
| **format-grouped** | all same tracker format (mod/it/s3m/xm) | shared format-typical sample palettes |
| **affinity (title)** | seeded from a multi-module **title cluster**, topped up with browse neighbours | same artist / remix set / playlist |

**Note on affinity signal.** The 2007 Mod Archive snapshot is sorted by
**filename** and carries **no author metadata** (the only consistent extra field
is the tracker name, e.g. "FastTracker v2.00"). So "same uploader" isn't directly
present. The realized stand-in is the **normalized title key**: modules whose
titles collide after stripping `(part 2)` / `remix` / version suffixes are
remixes, parts, or re-uploads by the same hand ("Eternal Flames" + "Eternal
Flames (Part 2)"; "00320 Helsinki" shipped as both `.mod` and `.it`). That is the
closest the corpus gets to an artist/playlist grouping.

## Result — 1,508-module sample, 300 sessions × 12 modules each (defaults)

Pool dedup of this sample (all 1,508 modules as one global pool):
**9.2%** (617 MB → 560 MB unique). This is low *on purpose* — these are the
alphabetically-first modules (a sparse slice), and CID.md showed a contiguous 2k
slice is ~9.8% while the full corpus is ~41% (dedup is a collision property; a
small slice undercounts). The warm-cache savings below are the **realized
fraction of whatever pool dedup exists**, so they scale with it.

| strategy | warm-cache hit-rate | naive MB | net MB |
|---|---|---|---|
| random (uniform draw) | **0.0%** | 1452 | 1451 |
| browse order (contiguous) | **2.8%** | 1502 | 1459 |
| format-grouped | 0.8% | 1791 | 1777 |
| **affinity (title cluster)** | **7.2%** | 1543 | 1431 |

Savings grow monotonically as the session lengthens (cumulative, % saved after N
modules) — the cache warms:

```
after N mods    1    2    3    4    5    6    7    8    9   10   11   12
random        0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0
browse        0.0  0.9  0.8  2.3  2.0  1.7  1.7  1.7  1.7  2.2  2.5  2.8
format        0.0  0.0  0.1  0.4  0.5  0.5  0.6  0.6  0.6  0.7  0.7  0.8
affinity      0.0  0.7  2.5  3.5  3.6  3.9  4.0  4.5  5.3  5.5  5.9  7.2
```

## Three readings of the number

1. **Affinity is the whole story.** Random sessions of uncorrelated tracks share
   *almost nothing* (0.0% — two strangers' tracks don't reuse the same drum hit).
   Grouping by artist/remix turns that into **7.2%** and climbing — the only
   strategy whose curve rises steeply with session length. Format alone is weak
   (0.8%); browse-order adjacency catches some re-uploads (2.8%).
2. **It's a realized fraction of pool dedup, and pool dedup is the ceiling.** At
   a denser 6,407-module sample the pool dedup rises to 12.1% and affinity tracks
   it (5.2%); the 18 hand-picked distinct tracks in `~/tmp/somemods` have **0.0%**
   pool dedup and therefore **0.0%** warm-cache across every strategy — the clean
   control showing the mechanism. As the served pool approaches full-corpus
   density (~41% dedup, CID.md), an affinity-grouped session realizes a
   correspondingly larger slice.
3. **This compounds with seek/repack.** The resident-set / byte-range fetch
   (`SEEK.md`) already decides *which* chunks a cold seek pulls; every one of
   those that's a warm-cache hit is free. Warm-cache and partial-fetch multiply,
   they don't overlap.

## What this is and isn't

- **Is:** a byte-exact, production-CID simulation of client per-session transfer
  under a persistent block cache, quantifying that **affinity-grouped sessions
  realize a measurable warm-cache saving (~5–7% on these sparse samples, growing
  with session length) where random sessions realize ~0%.**
- **Isn't:** a full-corpus number. Run on a sparse alphabetical slice (so pool
  dedup is ~9–12%, vs ~41% full-corpus); the warm-cache figure is a *fraction* of
  pool dedup and rises with it. It also isn't a real user trace — affinity is
  modeled from title-cluster + browse-order, the strongest signal the corpus
  metadata supports, not from observed listening logs. A production recommender
  (genre/artist/"more like this") would group more tightly than a title key and
  should beat these numbers.
- **Doesn't** assume any cache eviction — the rust-ipfs blockstore is persistent,
  so within a session (and across sessions, not modeled here) hits only
  accumulate. Cross-session warming (a user's cache from yesterday) would push the
  realized fraction higher still.

## Bearing on the MVP

CID.md quantified the *storage/origin* win of swapping opaque ids for chunk CIDs
(~30–41%, byte-exact). This lab adds the *client* half: the same chunk layer,
delivered through the persistent blockstore, turns artist/playlist listening into
free local hits — **0% for random, up to ~7% and climbing for affinity sessions
on a sparse slice, scaling toward the full-corpus dedup ceiling as the served
pool densifies.** No engine change, no extra fetch — the cache is the blockstore
that already has to exist.

## Reproduce

```
cd lab
npm install
node warm-cache.mjs                       # defaults: 1500 modules, 300×12 sessions (~3s)
SAMPLE=6000 SESSIONS=400 node warm-cache.mjs   # denser pool -> higher pool dedup (~14s)
CORPUS=somemods SAMPLE=18 SESSLEN=8 node warm-cache.mjs   # control: distinct tracks -> 0%
# env: SAMPLE (modules to build DAGs for), SESSIONS, SESSLEN (modules/session),
#      FORMATS (csv), SEED, CORPUS (modarchive|somemods)
```
