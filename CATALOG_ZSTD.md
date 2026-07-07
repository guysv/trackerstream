# TSZCAT — per-page-zstd catalog (traffic reduction over the Bitswap VFS)

**Status:** built + lab-validated on `attempt/catalog-4x-seed-2`; **not yet flipped on prod.**
The raw SQLite catalog is untouched and remains the default. See [[catalog-packing-lab]].

## What it is

The catalog is published as a single SQLite file chunked into 16 KB UnixFS leaves (one leaf per
SQLite page); the client reads only the pages a query touches, over a Bitswap-backed VFS. Bitswap
fetches **whole blocks**, so plain zstd of the file saves nothing on the wire — a compressed page
still lives inside a full 16 KB leaf. TSZCAT fixes that: **each 16 KB page is zstd-compressed into
its own block**, and a small manifest lists the page block CIDs.

**Format** (the published root under `catalog-z`):
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

- **Producer** — `publishZstdCatalog` in `apps/server/src/ingest.ts`, dual-published from
  `publishCatalog` when **`ZSTD_CATALOG=1`** (env-gated; dormant otherwise). zstd each page →
  `block/put-many` (batched) → manifest → `addFile` → **pin page blocks** → `namePublish` under
  keystore key `catalog-z`. The raw catalog is ALWAYS published too, so this never risks the live
  path (a zstd failure just logs).
- **Config** — `CATALOG_Z_IPNS_KEY` in `packages/config/index.js` (empty = dormant).
- **Client preference** — `apps/desktop/src/lib/catalog.ts` resolves `CATALOG_Z_IPNS_KEY ||
  CATALOG_IPNS_KEY`. Empty → raw; set → zstd.

## Rollout — soft migration (NO client breaks)

The raw catalog under `catalog` keeps being published in parallel, so old clients never break and
rollback is instant.

1. **Master, one-time:** run one ingest with `ZSTD_CATALOG=1` (as root, server.env). It generates
   the `catalog-z` key (idempotent `keyGen`), publishes the manifest, and **logs the PeerId**.
2. Paste that PeerId into `CATALOG_Z_IPNS_KEY` in `packages/config/index.js`.
3. Set `ZSTD_CATALOG=1` permanently in `/etc/trackerstream/server.env` so every ingest
   dual-publishes (raw + zstd).
4. **Ship a client build** (this branch): it has the VFS decoder and prefers `catalog-z`. New
   clients get ~1.7–2.2× less catalog traffic; clients on the old build keep using `catalog` (raw).
5. (Later, optional) once adoption is high, stop the raw publish. Keeping both is cheap insurance.

**Rollback:** clear `CATALOG_Z_IPNS_KEY` (clients fall back to raw) or unset `ZSTD_CATALOG`. The
raw catalog is always live.

## Caveats / follow-ups

- **Traffic-only.** Cuts bytes (master upload bandwidth, seeding, metered clients) — NOT round
  trips, so it does not change cold-search *latency* (that's `detail=none`'s domain).
- **Pin housekeeping on rebake.** Each republish produces NEW page block CIDs (content changed);
  the old zstd page blocks become stale-pinned and accumulate. Add old-page unpinning to the
  republish (mirror the raw catalog's old-root unpin), or periodic GC of unpinned blocks. ~125 MB
  of page blocks per catalog version on the master until then.
- **Bake cost:** ~30–60 s zstd + `block/put-many` + ~16.8 k pins per ingest (pins run in parallel
  batches). Acceptable for the routine ingest cadence.
