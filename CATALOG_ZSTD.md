# TSZCAT — per-page-zstd catalog (traffic reduction over the Bitswap VFS)

**Status:** LIVE on prod (2026-07-07 hard cut — no clients in the wild). The `catalog` IPNS key
now resolves to a TSZCAT manifest; the raw SQLite catalog was replaced. Live CID
`bafybeihf364mqgbent3ewy3akitoyuswqyaxr5zcrgvjkq6ovje4x4twqa`. Rollback anchor (raw detail=none,
still pinned) `bafybeigweozn6frfdd4nkymzbkoe2aur3odat5n3dq2xev6p5p7uipqlv4`. WAN-measured
steady-state **2.2–2.35×** page-traffic reduction (first query pays the 0.61 MB manifest once),
waves unchanged, all searches return correct results. See CATALOG_ZSTD.md#deployed below.

## What it is

The catalog is published as a single SQLite file chunked into 16 KB UnixFS leaves (one leaf per
SQLite page); the client reads only the pages a query touches, over a Bitswap-backed VFS. Bitswap
fetches **whole blocks**, so plain zstd of the file saves nothing on the wire — a compressed page
still lives inside a full 16 KB leaf. TSZCAT fixes that: **each 16 KB page is zstd-compressed into
its own block**, and a small manifest lists the page block CIDs.

**Format** (the published catalog root):
```
[8]  magic "TSZCAT1\n"
[4]  page_size  u32 LE   (16384)
[4]  page_count u32 LE
[page_count × 36]  page block CIDs (CIDv1 raw sha2-256, binary)
```
Client (`apps/desktop/src-tauri/src/catalog.rs`, `IpfsVfs`): `open` probes the root's first bytes
— `SQLite format 3\0` → raw path (unchanged); `TSZCAT1\n` → fetch the manifest once (cached), then
each page read does `block/get(page_cid)` + `zstd::decompress`. **One client binary reads both
formats.** Lazy paging, prefetch, page cache, cancel — all preserved.

## Measured (lab, `catalog_zstd_bench.rs`)

- Per-page ceiling **2.21×**; manifest **0.61 MB** (16,852 pages).
- Full cold session (browse + 3 searches + detail): raw **3.29 MB** → zstd **1.92 MB** = **1.71×**
  (manifest dilutes the page ratio on small sessions; larger sessions and the master's aggregate
  serving load approach 2.2×).
- **Waves identical** (188 vs 187) — this is TRAFFIC-only, latency-neutral by design.
- **Parity ok** — identical search result sets (a gate in the bench asserts it).

## Components

- **Producer** — `publishZstdCatalog(rpc, snapshot, keyName)` in `apps/server/src/ingest.ts`.
  `publishCatalog` branches on **`ZSTD_CATALOG=1`**: publish the zstd manifest under the main
  `CATALOG_KEY_NAME` (the hard cut), else the raw SQLite. zstd each page → `block/put-many`
  (batched) → manifest → `addFile` → **pin page blocks** → `namePublish`.
- **Client** — `apps/desktop/src-tauri/src/catalog.rs` VFS auto-detects raw vs TSZCAT by the root
  magic, so no client key change is needed. `CATALOG_Z_IPNS_KEY` (config) + the frontend `||`
  fallback are vestigial after the hard cut (empty key → main key → zstd); harmless, left in place.

## Deployed — HARD CUT (2026-07-07)

No clients in the wild, so we published TSZCAT under the MAIN `catalog` key (not a second key) and
dropped the raw publish. `publishCatalog` branches on `ZSTD_CATALOG=1`: zstd manifest under
`CATALOG_KEY_NAME`, else raw. The `CATALOG_Z_IPNS_KEY` config + frontend `|| ` fallback are now
vestigial (empty key → main key → the client's VFS auto-detects TSZCAT); harmless, left in place.

What was done on prod:
1. Deployed `apps/server/src/ingest.ts` to `/opt/trackerstream` (catalog.ts detail=none already live).
2. `ZSTD_CATALOG=1` added to `/etc/trackerstream/server.env` so routine (timer) ingests keep
   publishing zstd instead of reverting to raw.
3. `REINDEX_FTS=1 ZSTD_CATALOG=1` ingest (as root) → republished `catalog` as the TSZCAT manifest;
   IPNS verified → manifest, magic `TSZCAT1`, timer re-armed.

**Rollback:** re-run `REINDEX_FTS=1` WITHOUT `ZSTD_CATALOG` (remove it from server.env) → republishes
the raw detail=none catalog under `catalog`. The rollback-anchor raw CID is still pinned; DB backup
`catalog.db.bak-pre-detailnone-*`, prod ingest.ts backup `/tmp/ingest.ts.bak-prezstd`.

## Follow-ups

- **Cold ms rose** vs raw (block/get-per-page has more per-call overhead than a ranged `cat` DAG
  walk; waves are unchanged so it's not extra round-trips). Traffic-only change and latency is
  `detail=none`'s domain, but worth a `block/get-many` batch RPC on tsnode + client to close it.

## Caveats / follow-ups

- **Traffic-only.** Cuts bytes (master upload bandwidth, seeding, metered clients) — NOT round
  trips, so it does not change cold-search *latency* (that's `detail=none`'s domain).
- **Pin housekeeping on rebake.** Each republish produces NEW page block CIDs (content changed);
  the old zstd page blocks become stale-pinned and accumulate. Add old-page unpinning to the
  republish (mirror the raw catalog's old-root unpin), or periodic GC of unpinned blocks. ~125 MB
  of page blocks per catalog version on the master until then.
- **Bake cost:** ~30–60 s zstd + `block/put-many` + ~16.8 k pins per ingest (pins run in parallel
  batches). Acceptable for the routine ingest cadence.
