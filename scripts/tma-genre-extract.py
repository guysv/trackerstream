#!/usr/bin/env python3
"""Flatten the raw TMA genre-sweep XML dumps into a reusable md5 -> genre table.

Reads every api-dumps/genre-*-page-*.xml produced by tma-genre-sweep.sh and emits:
  api-dumps/tma-genres.tsv    md5<TAB>genreid<TAB>genretext<TAB>moduleid<TAB>filename
  api-dumps/tma-genres.json   { "<md5>": {"genreid":N,"genre":"...","id":N,"filename":"..."} }

md5 is the join key against the corpus. A handful of modules can appear under more than one
genre; we keep the first seen and count collisions rather than guessing a primary.

Usage: scripts/tma-genre-extract.py [--dir api-dumps]
"""
import sys, os, re, json, glob
from collections import Counter

d = "api-dumps"
if "--dir" in sys.argv:
    d = sys.argv[sys.argv.index("--dir") + 1]

MOD = re.compile(r"<module>(.*?)</module>", re.S)
def field(block, tag):
    m = re.search(rf"<{tag}>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{tag}>", block, re.S)
    return m.group(1).strip() if m else ""

by_md5 = {}
dup_md5 = 0
genre_counts = Counter()
files = sorted(glob.glob(os.path.join(d, "genre-*-page-*.xml")))
if not files:
    sys.exit(f"no genre dumps found in {d}/ — run scripts/tma-genre-sweep.sh --all first")

for path in files:
    xml = open(path, encoding="utf-8", errors="replace").read()
    for block in MOD.findall(xml):
        md5 = field(block, "hash").lower()
        if not md5:
            continue
        rec = {
            "genreid": int(field(block, "genreid") or 0),
            "genre": field(block, "genretext"),
            "id": int(field(block, "id") or 0),
            "filename": field(block, "filename"),
        }
        genre_counts[rec["genre"]] += 1
        if md5 in by_md5:
            if by_md5[md5]["genre"] != rec["genre"]:
                dup_md5 += 1
            continue
        by_md5[md5] = rec

os.makedirs(d, exist_ok=True)
tsv = os.path.join(d, "tma-genres.tsv")
with open(tsv, "w", encoding="utf-8") as f:
    f.write("md5\tgenreid\tgenre\tmoduleid\tfilename\n")
    for md5, r in sorted(by_md5.items()):
        f.write(f"{md5}\t{r['genreid']}\t{r['genre']}\t{r['id']}\t{r['filename']}\n")

js = os.path.join(d, "tma-genres.json")
with open(js, "w", encoding="utf-8") as f:
    json.dump(by_md5, f, ensure_ascii=False, indent=0)

print(f"dump files read : {len(files)}")
print(f"unique md5s     : {len(by_md5)}")
print(f"cross-genre md5s: {dup_md5} (kept first seen)")
print(f"wrote           : {tsv}")
print(f"                  {js}")
print("top genres:")
for g, n in genre_counts.most_common(10):
    print(f"  {n:6}  {g}")
