# TMA genre enrichment artifacts

Banked snapshots from The Mod Archive XML API (Level 3), swept via
`scripts/tma-genre-sweep.sh --all` and flattened by `scripts/tma-genre-extract.py`.

Genre is a browse axis on TMA (`request=search&type=genre&query=<genreid>`, 40 modules
per page), so one request classifies 40 modules — the whole archive sweeps in ~780
requests. Each module carries its md5 hash, which is the intended join key against the
corpus catalog (`apps/server/src/catalog.ts`). Note: as of this snapshot the catalog has
no md5 column, so wiring the join in still requires an md5 backfill over the source zips.

## Snapshot: 2026-07-04

- `tma-genre-raw-2026-07-04.zip` — 779 raw genre pages + `view_genres.xml` (the source of truth; re-extractable)
- `tma-genres-2026-07-04.tsv` — `md5 ⇥ genreid ⇥ genre ⇥ moduleid ⇥ filename`
- `tma-genres-2026-07-04.json` — same, keyed by md5

Stats: 77 genres, 779 pages, **29,642 unique md5→genre mappings**, 0 md5 collisions,
788/1500 API requests used. Top genres: Chiptune (4536), Electronic-Techno (1928),
Electronic-Dance (1609), Demo Style (1550).

## Regenerating / refreshing

```sh
scripts/tma-genre-sweep.sh --all     # resumable; ~780 requests, pace stays under the 1500/hr cap
scripts/tma-genre-extract.py         # -> tma-genres.tsv / .json
```

Only dated snapshots (`tma-genres-*.{tsv,json}`, `tma-genre-raw-*.zip`) and this README are
tracked; the loose working `genre-*.xml` / `view_genres.xml` / `sweep.log` are gitignored.
