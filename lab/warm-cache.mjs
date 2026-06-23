// Lab: warm-cache simulation — per-session client TRANSFER savings.
//
// CID.md measured the *global* dedup: store/serve the unique chunk set once
// (~41% sub-sample redundancy corpus-wide). That is a SERVER storage / origin-
// bandwidth number. It says nothing yet about what a *client* downloads in a
// real listening session, because that depends on cache LOCALITY — how often the
// chunks of the next track are already resident from earlier tracks.
//
// The persistent rust-ipfs blockstore IS the client cache: once a chunk CID is
// fetched for any module it stays local, so a later module that shares that CID
// is a local hit by construction. This script quantifies that. We:
//
//   1. Build the REAL production DAG for each sampled module via repack
//      (buildDag / buildFlatDag) — so chunk CIDs are exactly the ones the
//      client would fetch. Each module -> ordered list of block CIDs + bytes.
//   2. Model a client with an EMPTY persistent cache that plays a SESSION (an
//      ordered list of modules). Per module: total DAG bytes, bytes already
//      resident (chunk CIDs seen earlier this session), NET bytes transferred.
//   3. Compare session-construction strategies (random vs affinity-grouped) to
//      show affinity raises the warm-cache hit rate, and report how savings grow
//      as the session lengthens.
//
// Affinity axes available from the corpus (the 2007 snapshot is sorted by
// FILENAME, has no author metadata, so "same uploader" isn't directly present):
//   - format   : same tracker format -> shared format-typical sample palettes.
//   - title    : modules whose normalized title collides are remixes / parts /
//                re-uploads by the same hand ("Eternal Flames" + "...(Part 2)",
//                "00320 Helsinki" as .mod and .it) -> heavy sample reuse. This is
//                the closest realized stand-in for an artist/playlist grouping.
//
// Env knobs:
//   SAMPLE=   modules to sample from the corpus     (default 1500)
//   SESSIONS= number of sessions per strategy        (default 200)
//   SESSLEN=  modules per session                     (default 12)
//   FORMATS=  csv of formats to include               (default mod,it,s3m,xm)
//   SEED=     PRNG seed                                (default 1)
//   CORPUS=   modarchive | somemods                   (default modarchive)
//
// Run:  cd lab && node warm-cache.mjs

import { homedir } from "node:os";
import { join } from "node:path";
import { readdirSync, readFileSync } from "node:fs";
import { forEachModule } from "/Users/guysviry/git/trackerstream/apps/server/src/corpus.ts";
import {
  buildDag,
  buildFlatDag,
} from "/Users/guysviry/git/trackerstream/packages/repack/src/index.ts";

const SAMPLE = +(process.env.SAMPLE ?? 1500);
const SESSIONS = +(process.env.SESSIONS ?? 300);
const SESSLEN = +(process.env.SESSLEN ?? 12);
const FORMATS = (process.env.FORMATS ?? "mod,it,s3m,xm").toLowerCase().split(",");
const SEED = +(process.env.SEED ?? 1);
const CORPUS = process.env.CORPUS ?? "modarchive";

// ---- deterministic PRNG (mulberry32) ----
function mulberry32(a) {
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
function shuffle(arr, rnd) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = (rnd() * (i + 1)) | 0;
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// ---- title extraction (per format) for the affinity grouping ----
const ascii = (b, off, len) => {
  let s = "";
  for (let i = 0; i < len && off + i < b.length; i++) {
    const c = b[off + i];
    if (c === 0) break;
    if (c >= 32 && c < 127) s += String.fromCharCode(c);
  }
  return s.trim();
};
function titleOf(b, ext) {
  try {
    if (ext === "mod") return ascii(b, 0, 20);
    if (ext === "it") return ascii(b, 4, 26);
    if (ext === "s3m") return ascii(b, 0, 28);
    if (ext === "xm") return ascii(b, 17, 20);
  } catch {}
  return "";
}
// Normalize a title into an artist/work affinity key: lowercase, strip part/
// remix/version suffixes and non-alphanumerics. Empty / too-generic -> null.
function titleKey(title) {
  if (!title) return null;
  let t = title.toLowerCase();
  t = t.replace(/\(.*?\)/g, " "); // (part 2), (remix)
  t = t.replace(/\b(part|pt|remix|mix|rmx|ver|version|v|edit|final|demo)\b.*$/g, " ");
  t = t.replace(/[^a-z0-9]+/g, "");
  if (t.length < 4) return null; // "00", "003" etc. -> not a real artist key
  return t;
}

// ---- build the DAG for one module, return its block fingerprint ----
async function fingerprint(bytes, ext) {
  const u8 = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let dag;
  try {
    dag = await buildDag(u8); // sample-level DAG (production path)
  } catch {
    try {
      dag = await buildFlatDag(u8, ext); // fallback for unparsed formats
    } catch {
      return null;
    }
  }
  // The client fetches every block in the DAG: manifest, skeleton chunks,
  // pcm-roots, sample chunks. Cache hit = CID already resident. Use the deduped
  // block set (dag.blocks is already unique-by-CID within the module) with each
  // block's byte length — that is exactly the on-wire transfer per block.
  const cids = [];
  const sizes = new Map();
  let totalBytes = 0;
  for (const blk of dag.blocks) {
    const k = blk.cid.toString();
    cids.push(k);
    if (!sizes.has(k)) {
      sizes.set(k, blk.bytes.length);
      totalBytes += blk.bytes.length;
    }
  }
  return { cids, sizes, totalBytes, ext, originalLength: u8.length };
}

// ---- collect a bounded module sample ----
async function collectModarchive() {
  const ROOT = join(homedir(), "tmp", "modarchive");
  const mods = [];
  await forEachModule(
    ROOT,
    async (m) => {
      const ext = (m.name.split(".").pop() || "").toLowerCase();
      const fp = await fingerprint(m.bytes, ext);
      if (!fp) return;
      fp.name = m.name;
      fp.title = titleOf(m.bytes, ext);
      fp.tkey = titleKey(fp.title);
      mods.push(fp);
    },
    { formats: FORMATS, limit: SAMPLE },
  );
  return mods;
}
async function collectSomemods() {
  const ROOT = join(homedir(), "tmp", "somemods");
  const mods = [];
  for (const name of readdirSync(ROOT).sort()) {
    if (name.startsWith(".")) continue;
    const ext = (name.split(".").pop() || "").toLowerCase();
    if (!FORMATS.includes(ext)) continue;
    const bytes = readFileSync(join(ROOT, name));
    const fp = await fingerprint(bytes, ext);
    if (!fp) continue;
    fp.name = name;
    fp.title = titleOf(bytes, ext);
    fp.tkey = titleKey(fp.title);
    mods.push(fp);
    if (mods.length >= SAMPLE) break;
  }
  return mods;
}

// ---- play one session: empty cache, return per-step + totals ----
function playSession(session) {
  const cache = new Set();
  let naive = 0; // sum of full DAG bytes (no cache)
  let net = 0; // bytes actually transferred (cache-aware)
  const cum = []; // cumulative {naive, net} after each module
  for (const m of session) {
    naive += m.totalBytes;
    for (const k of m.cids) {
      if (!cache.has(k)) {
        net += m.sizes.get(k);
        cache.add(k);
      }
    }
    cum.push({ naive, net });
  }
  return { naive, net, cum };
}

// ---- run a strategy: many sessions, average the savings ----
function runStrategy(buildSessions) {
  const sessions = buildSessions();
  let naive = 0,
    net = 0;
  // per-position cumulative savings, averaged across sessions
  const posNaive = new Array(SESSLEN).fill(0);
  const posNet = new Array(SESSLEN).fill(0);
  const posCount = new Array(SESSLEN).fill(0);
  let totalModules = 0;
  for (const s of sessions) {
    const r = playSession(s);
    naive += r.naive;
    net += r.net;
    totalModules += s.length;
    for (let i = 0; i < r.cum.length; i++) {
      posNaive[i] += r.cum[i].naive;
      posNet[i] += r.cum[i].net;
      posCount[i]++;
    }
  }
  const savings = naive ? 1 - net / naive : 0;
  // cumulative savings curve vs session position (1-indexed module count)
  const curve = [];
  for (let i = 0; i < SESSLEN; i++) {
    if (!posCount[i]) break;
    curve.push(posNaive[i] ? 1 - posNet[i] / posNaive[i] : 0);
  }
  return { nSessions: sessions.length, totalModules, naive, net, savings, curve };
}

// ---- main ----
console.log(
  `warm-cache: CORPUS=${CORPUS} SAMPLE=${SAMPLE} SESSIONS=${SESSIONS} SESSLEN=${SESSLEN} FORMATS=${FORMATS.join(",")} SEED=${SEED}`,
);
const t0 = process.hrtime.bigint();
const mods =
  CORPUS === "somemods" ? await collectSomemods() : await collectModarchive();
const tBuild = Number(process.hrtime.bigint() - t0) / 1e9;
console.log(
  `built DAGs for ${mods.length} modules in ${tBuild.toFixed(0)}s  (${(
    mods.reduce((a, m) => a + m.totalBytes, 0) / 1e6
  ).toFixed(0)}MB of DAG blocks)`,
);

// corpus-wide whole-sample-of-this-sample chunk dedup, for the tie-back to CID.md
{
  const seen = new Set();
  let total = 0,
    uniq = 0;
  for (const m of mods)
    for (const k of m.cids) {
      total += m.sizes.get(k);
      if (!seen.has(k)) {
        seen.add(k);
        uniq += m.sizes.get(k);
      }
    }
  const dedup = total ? 1 - uniq / total : 0;
  console.log(
    `sample-pool chunk dedup (all ${mods.length} modules as one pool): ${(dedup * 100).toFixed(1)}%  ` +
      `(${(total / 1e6).toFixed(0)}MB -> ${(uniq / 1e6).toFixed(0)}MB unique)`,
  );
}

// per-format index + title-cluster index, for affinity sessions
const byFormat = new Map();
const byTitle = new Map();
for (const m of mods) {
  if (!byFormat.has(m.ext)) byFormat.set(m.ext, []);
  byFormat.get(m.ext).push(m);
  if (m.tkey) {
    if (!byTitle.has(m.tkey)) byTitle.set(m.tkey, []);
    byTitle.get(m.tkey).push(m);
  }
}

const rndBase = mulberry32(SEED);

// Strategy A — RANDOM: each session is SESSLEN DISTINCT modules drawn uniformly
// (no replaying the same track within a session). The cache-locality floor.
function buildRandom() {
  const rnd = mulberry32(SEED * 7 + 1);
  const out = [];
  for (let s = 0; s < SESSIONS; s++) {
    out.push(shuffle(mods, rnd).slice(0, SESSLEN));
  }
  return out;
}

// Strategy B — BROWSE ORDER: contiguous run in corpus (filename) order. The
// natural "scroll the archive / play the page" session; near-dup re-uploads and
// remixes sit next to each other alphabetically.
function buildBrowse() {
  const rnd = mulberry32(SEED * 7 + 2);
  const out = [];
  const maxStart = Math.max(1, mods.length - SESSLEN);
  for (let s = 0; s < SESSIONS; s++) {
    const start = (rnd() * maxStart) | 0;
    out.push(mods.slice(start, start + SESSLEN));
  }
  return out;
}

// Strategy C — FORMAT: each session is SESSLEN modules of one format (shared
// format-typical sample palette — e.g. the Amiga ST-0x disks behind MODs).
function buildFormat() {
  const rnd = mulberry32(SEED * 7 + 3);
  const fmts = [...byFormat.keys()].filter((f) => byFormat.get(f).length >= 2);
  const out = [];
  for (let s = 0; s < SESSIONS; s++) {
    const f = fmts[(rnd() * fmts.length) | 0];
    const pool = byFormat.get(f);
    out.push(shuffle(pool, rnd).slice(0, SESSLEN));
  }
  return out;
}

// Strategy D — AFFINITY (title cluster): seed each session from a multi-module
// title cluster (an artist's parts/remixes/re-uploads), then top up with nearest
// browse-order neighbours of the seed. This is the realized "same artist /
// playlist" session — the strongest real affinity the corpus metadata supports.
function buildAffinity() {
  const rnd = mulberry32(SEED * 7 + 4);
  const clusters = [...byTitle.values()].filter((c) => c.length >= 2);
  if (clusters.length === 0) return buildFormat(); // corpus too small for title clusters
  // index modules by identity for neighbour lookup
  const idx = new Map(mods.map((m, i) => [m, i]));
  const out = [];
  for (let s = 0; s < SESSIONS; s++) {
    const c = clusters[(rnd() * clusters.length) | 0];
    const seen = new Set();
    const sess = [];
    for (const m of c) {
      if (sess.length >= SESSLEN) break;
      if (!seen.has(m)) { seen.add(m); sess.push(m); }
    }
    // top up from browse-order neighbours of the cluster's first member
    // (alphabetically adjacent uploads — the same "page" of the archive)
    let p = idx.get(c[0]) ?? 0;
    let guard = 0;
    while (sess.length < SESSLEN && guard++ < mods.length) {
      p = (p + 1) % mods.length;
      const m = mods[p];
      if (!seen.has(m)) { seen.add(m); sess.push(m); }
    }
    out.push(shuffle(sess, rnd));
  }
  return out;
}

const strategies = [
  ["random (uniform draw)", buildRandom],
  ["browse order (contiguous)", buildBrowse],
  ["format-grouped", buildFormat],
  ["affinity (title cluster)", buildAffinity],
];

const results = strategies.map(([name, fn]) => [name, runStrategy(fn)]);

// ---- report ----
const pct = (x) => (x * 100).toFixed(1) + "%";
const pad = (s, n) => String(s).padEnd(n);
const padL = (s, n) => String(s).padStart(n);

console.log(`\n=== per-session warm-cache transfer savings ===`);
console.log(
  pad("strategy", 28) +
    padL("hit-rate", 10) +
    padL("naive MB", 12) +
    padL("net MB", 10) +
    padL("saved", 9),
);
console.log("-".repeat(69));
for (const [name, r] of results) {
  console.log(
    pad(name, 28) +
      padL(pct(r.savings), 10) +
      padL((r.naive / 1e6).toFixed(0), 12) +
      padL((r.net / 1e6).toFixed(0), 10) +
      padL(pct(r.savings), 9),
  );
}

console.log(`\n=== savings grow as the session lengthens (cumulative, by module #) ===`);
const head = ["after N mods"].concat(
  Array.from({ length: SESSLEN }, (_, i) => String(i + 1)),
);
const widths = head.map((h) => Math.max(6, h.length));
const row = (cells) =>
  cells.map((c, i) => padL(c, widths[i])).join(" ");
console.log(row(head));
for (const [name, r] of results) {
  const cells = [name.slice(0, 26)].concat(
    r.curve.map((v) => pct(v).replace("%", "")),
  );
  // re-pad first col wider
  console.log(pad(cells[0], 26) + " " + cells.slice(1).map((c, i) => padL(c, widths[i + 1])).join(" "));
}

const dt = Number(process.hrtime.bigint() - t0) / 1e9;
console.log(`\ndone in ${dt.toFixed(0)}s`);
