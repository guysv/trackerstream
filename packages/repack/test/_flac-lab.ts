// LAB (throwaway): measure FLAC compression of the v2/v3 decoded-PCM sample leaves
// (the "v4" idea). For every streamed sample we compare raw native PCM vs FLAC, and
// model the corpus-level dedup interaction that decides whether v4 is worth a re-bake.
//
// Outputs:
//  1. per-format / per-bitDepth FLAC ratio on sample bytes
//  2. corpus sample-block bytes under 4 dedup models:
//       A raw no-dedup   B raw whole-sample-dedup   C raw CDC-dedup (PRODUCTION today)
//       D flac whole-sample-dedup + per-sample gate (the v4 candidate)
//     verdict = D vs C
//  3. order-0 (first-playable) bytes per module: raw vs flac  (the TTFP proxy; dedup-free)
//
// Run: node --experimental-strip-types packages/repack/test/_flac-lab.ts [corpusDir]
import { readFileSync, readdirSync, existsSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { homedir } from "node:os";
import { buildDagV2, type DecodedSample, type SampleV2, type PlanV2 } from "../src/dag.ts";
import { sampleSlots } from "../src/parse.ts";
import { cdcChunks } from "../src/cdc.ts";

let M: any;
try {
  M = await (await import("../../wasm/dist/libopenmpt.js")).default({ print() {}, printErr() {} });
} catch (e) {
  console.log(`SKIP flac-lab (wasm dist unavailable: ${(e as Error).message})`);
  process.exit(0);
}

const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

function dumpSamples(bytes: Uint8Array): DecodedSample[] | null {
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  if (!mod) return null;
  const n = M._openmpt_module_get_num_samples(mod);
  const out: DecodedSample[] = [];
  for (let i = 1; i <= n; i++) {
    const frames = M._openmpt_module_debug_sample_frames(mod, i);
    const bn = M._openmpt_module_debug_sample_bytes(mod, i);
    const ptr = M._openmpt_module_debug_sample_data(mod, i);
    if (frames > 0 && bn > 0 && ptr) out.push({ index: i, frames, data: M.HEAPU8.slice(ptr, ptr + bn) });
  }
  M._openmpt_module_destroy(mod);
  return out;
}

// planar -> interleaved so FLAC's inter-channel decorrelation applies (deterministic;
// inverted on decode). mono passes through.
function interleave(data: Uint8Array, channels: number, bitDepth: number): Uint8Array {
  if (channels !== 2) return data;
  const bps = bitDepth / 8;
  const frames = data.length / (channels * bps);
  const out = new Uint8Array(data.length);
  const L = 0,
    R = frames * bps;
  for (let f = 0; f < frames; f++) {
    for (let k = 0; k < bps; k++) {
      out[(f * 2 + 0) * bps + k] = data[L + f * bps + k];
      out[(f * 2 + 1) * bps + k] = data[R + f * bps + k];
    }
  }
  return out;
}

// flac-length cache keyed by sha(rawPCM)+level — makes re-runs instant.
const CACHE = process.env.FLAC_CACHE ?? "/tmp/flac-lab-cache.json";
const cache: Record<string, number> = existsSync(CACHE) ? JSON.parse(readFileSync(CACHE, "utf8")) : {};
let cacheDirty = 0;

// FLAC-encode raw signed PCM; return encoded byte length. --no-padding/--no-seektable
// for the minimal deterministic size v4 would ship. level 8 (best).
function flacLenRaw(pcm: Uint8Array, channels: number, bitDepth: number, level: number): number {
  const il = interleave(pcm, channels, bitDepth);
  const out = execFileSync(
    "flac",
    [
      "--silent", "--no-padding", "--no-seektable",
      "--force-raw-format", "--endian=little", "--sign=signed",
      `--channels=${channels}`, `--bps=${bitDepth}`, "--sample-rate=44100",
      `-${level}`, "-c", "-",
    ],
    { input: Buffer.from(il), maxBuffer: 1 << 28 },
  );
  return out.length;
}
function flacLen(pcm: Uint8Array, shaHex: string, channels: number, bitDepth: number, level: number): number {
  const k = `${shaHex}:${level}`;
  let v = cache[k];
  if (v === undefined) {
    v = flacLenRaw(pcm, channels, bitDepth, level);
    cache[k] = v;
    if (++cacheDirty % 500 === 0) writeFileSync(CACHE, JSON.stringify(cache));
  }
  return v;
}

// order-0 required slot set (mirror of requiredAtZero in dag.ts: floor cp <=0 UNION next).
function requiredAtZero(plan: PlanV2): Set<number> {
  const cps = [...plan.checkpoints].sort((a, b) => a.order - b.order);
  if (!cps.length) return new Set();
  let floor = -1;
  for (let i = 0; i < cps.length; i++) if (cps[i].order <= 0) floor = i;
  const lo = Math.max(floor, 0);
  const hi = Math.min(Math.max(floor + 1, 0), cps.length - 1);
  const set = new Set<number>();
  for (let k = lo; k <= hi; k++) for (const s of cps[k].samples) set.add(s);
  return set;
}

const DIR = process.argv[2] ?? join(homedir(), "tmp", "somemods");
if (!existsSync(DIR)) {
  console.log(`SKIP flac-lab (corpus dir ${DIR} absent)`);
  process.exit(0);
}
const LEVEL = Number(process.env.FLAC_LEVEL ?? 8);

type Row = { fmt: string; bd: number; ch: number; raw: number; flac: number; n: number };
const perKey = new Map<string, Row>();

// order-0 per module
let o0raw = 0,
  o0flac = 0;
const o0perFmt = new Map<string, { raw: number; flac: number }>();

// per-sample records in module order — drives the dedup-scaling curve.
type Rec = { sha: string; raw: number; eff: number; chunks: string[] };
const recs: Rec[][] = []; // one array per module

let modOk = 0,
  modSkip = 0,
  sampleCount = 0;
const files = readdirSync(DIR)
  .filter((f) => !f.startsWith("."))
  .sort();

for (const name of files) {
  const data = new Uint8Array(readFileSync(join(DIR, name)));
  const dumps = dumpSamples(data);
  if (!dumps) {
    modSkip++;
    continue;
  }
  let dag;
  try {
    dag = await buildDagV2(data, dumps);
  } catch {
    modSkip++;
    continue;
  }
  const idx = dag.manifest.index;
  if (!idx) {
    modSkip++;
    continue;
  } // spilled index (rare) — skip in lab
  const slots = sampleSlots(data);
  const fmt = dag.manifest.format;
  const bySlot = new Map<number, SampleV2>(idx.samples.map((s) => [s.index, s]));
  const dumpByIdx = new Map<number, DecodedSample>(dumps.map((d) => [d.index, d]));
  const o0 = requiredAtZero(idx.plan);

  const flacBySlot = new Map<number, { raw: number; flac: number; eff: number }>();
  const modRecs: Rec[] = [];
  for (const s of idx.samples) {
    const d = dumpByIdx.get(s.index)!;
    const raw = d.data.length;
    const h = sha(d.data);
    const f = flacLen(d.data, h, s.channels, s.bitDepth, LEVEL);
    const eff = Math.min(f, raw); // per-sample gate: never ship FLAC bigger than raw
    flacBySlot.set(s.index, { raw, flac: f, eff });
    sampleCount++;

    // per-format/bitdepth
    const key = `${fmt}|${s.bitDepth}|${s.channels}`;
    let row = perKey.get(key);
    if (!row) perKey.set(key, (row = { fmt, bd: s.bitDepth, ch: s.channels, raw: 0, flac: 0, n: 0 }));
    row.raw += raw;
    row.flac += f;
    row.n++;

    const chunks: string[] = [];
    for (const c of cdcChunks(d.data)) chunks.push(sha(c) + ":" + c.length);
    modRecs.push({ sha: h, raw, eff, chunks });
  }
  recs.push(modRecs);

  // order-0 (TTFP proxy)
  let mr = 0,
    mf = 0;
  for (const si of o0) {
    const v = flacBySlot.get(si);
    if (v) {
      mr += v.raw;
      mf += v.eff;
    }
  }
  o0raw += mr;
  o0flac += mf;
  let of = o0perFmt.get(fmt);
  if (!of) o0perFmt.set(fmt, (of = { raw: 0, flac: 0 }));
  of.raw += mr;
  of.flac += mf;

  modOk++;
  if (modOk % 50 === 0) process.stderr.write(`\r[${modOk}/${files.length}] samples=${sampleCount}   `);
}

if (cacheDirty) writeFileSync(CACHE, JSON.stringify(cache));

// Compute the four dedup models over the FIRST `nMods` modules (module order is a
// random draw from the corpus, so growing nMods traces dedup vs corpus size).
function models(nMods: number) {
  const cdcSeen = new Set<string>();
  const wholeSeen = new Set<string>();
  let rawNoDedup = 0,
    cdcBytes = 0,
    rawWhole = 0,
    flacWhole = 0;
  for (let m = 0; m < nMods && m < recs.length; m++) {
    for (const r of recs[m]) {
      rawNoDedup += r.raw;
      if (!wholeSeen.has(r.sha)) {
        wholeSeen.add(r.sha);
        rawWhole += r.raw;
        flacWhole += r.eff;
      }
      for (const c of r.chunks) {
        if (!cdcSeen.has(c)) {
          cdcSeen.add(c);
          cdcBytes += Number(c.slice(c.indexOf(":") + 1));
        }
      }
    }
  }
  return { rawNoDedup, cdcBytes, rawWhole, flacWhole };
}
const full = models(recs.length);
const rawNoDedup = full.rawNoDedup,
  cdcBytes = full.cdcBytes,
  rawWhole = full.rawWhole,
  flacWhole = full.flacWhole;

const MB = (n: number) => (n / 1048576).toFixed(2);
const pct = (a: number, b: number) => (b ? ((1 - a / b) * 100).toFixed(1) + "%" : "-");
const ratio = (a: number, b: number) => (b ? (a / b).toFixed(3) : "-");

const out: string[] = [];
const p = (s = "") => {
  out.push(s);
  console.log(s);
};

p(`\n=== FLAC lab  (corpus=${DIR}  files=${files.length}  baked=${modOk}  skipped=${modSkip}  samples=${sampleCount}  flac-level=${LEVEL}) ===\n`);

p(`--- 1. per format/bitDepth/ch : FLAC ratio on sample bytes (no dedup) ---`);
p(`fmt   bd ch    n     raw(MB)  flac(MB)  flac/raw`);
for (const r of [...perKey.values()].sort((a, b) => b.raw - a.raw)) {
  p(
    `${r.fmt.padEnd(5)} ${String(r.bd).padStart(2)} ${r.ch}  ${String(r.n).padStart(5)}   ${MB(r.raw).padStart(7)}  ${MB(r.flac).padStart(7)}   ${ratio(r.flac, r.raw)}`,
  );
}

p(`\n--- 2. corpus sample-block bytes under dedup models (all ${modOk} modules) ---`);
p(`A raw   no-dedup           : ${MB(rawNoDedup).padStart(8)} MB`);
p(`B raw   whole-sample-dedup : ${MB(rawWhole).padStart(8)} MB   (saved ${pct(rawWhole, rawNoDedup)} vs A)`);
p(`C raw   CDC-dedup  [PROD]  : ${MB(cdcBytes).padStart(8)} MB   (saved ${pct(cdcBytes, rawNoDedup)} vs A)`);
p(`D flac  whole+gate [v4]    : ${MB(flacWhole).padStart(8)} MB   (saved ${pct(flacWhole, rawNoDedup)} vs A)`);
p(``);
p(`   partial-only dedup FLAC forfeits (B-C) : ${MB(rawWhole - cdcBytes)} MB = ${pct(cdcBytes, rawWhole)} of raw; ${((rawNoDedup - cdcBytes) ? (100 * (rawWhole - cdcBytes) / (rawNoDedup - cdcBytes)).toFixed(0) : "-")}% of all dedup savings`);
p(`   >>> VERDICT  D vs C (PROD)  : ${MB(flacWhole)} vs ${MB(cdcBytes)} MB   -> v4 saves ${pct(flacWhole, cdcBytes)} vs prod`);

p(`\n--- 2b. dedup SCALING (random draw; growing nMods -> approaches corpus behaviour) ---`);
p(`nMods    A raw     C cdc-dedup    B whole-dedup   partial/total-dedup   D flac-v4   v4 vs C`);
for (const n of [128, 256, 512, 1024, recs.length].filter((x, i, a) => a.indexOf(x) === i && x <= recs.length)) {
  const m = models(n);
  const totDed = m.rawNoDedup - m.cdcBytes;
  const partial = m.rawWhole - m.cdcBytes;
  p(
    `${String(n).padStart(5)}  ${MB(m.rawNoDedup).padStart(8)}  ${MB(m.cdcBytes).padStart(8)} ${pct(m.cdcBytes, m.rawNoDedup).padStart(6)}  ${MB(m.rawWhole).padStart(8)} ${pct(m.rawWhole, m.rawNoDedup).padStart(6)}   ${(totDed ? (100 * partial / totDed).toFixed(0) + "%" : "-").padStart(6)}            ${MB(m.flacWhole).padStart(7)}  ${pct(m.flacWhole, m.cdcBytes).padStart(6)}`,
  );
}
// Sensitivity: v4/prod = r*(1 + partialFrac/(1-dCdc)). Hold r + whole:partial split, sweep total dedup.
const r = flacWhole / rawWhole; // flac ratio on unique whole samples (with gate)
const dCdc0 = 1 - cdcBytes / rawNoDedup;
const wholeShare = (rawNoDedup - rawWhole) / (rawNoDedup - cdcBytes); // whole / total dedup
p(`\n--- 2c. sensitivity: if FULL-corpus dedup is higher than this slice's ${(100 * dCdc0).toFixed(0)}% ---`);
p(`(holding flac ratio r=${r.toFixed(3)} on unique samples, and whole=${(100 * wholeShare).toFixed(0)}% of dedup)`);
p(`assumed_total_dedup   ->   v4 storage vs prod`);
for (const dCdc of [dCdc0, 0.2, 0.3, 0.41, 0.5]) {
  const dWhole = dCdc * wholeShare;
  const v4 = (1 - dWhole) * r; // fraction of raw-no-dedup
  const prod = 1 - dCdc;
  p(`  ${(100 * dCdc).toFixed(0).padStart(3)}% total dedup   ->   v4 saves ${(100 * (1 - v4 / prod)).toFixed(0)}% vs prod sample-block storage`);
}

p(`\n--- 3. order-0 first-playable bytes (TTFP proxy; cold, dedup-free) ---`);
p(`total raw ${MB(o0raw)} MB   flac ${MB(o0flac)} MB   -> ${pct(o0flac, o0raw)} smaller first-playable`);
p(`per-format:`);
for (const [f, v] of [...o0perFmt.entries()].sort((a, b) => b[1].raw - a[1].raw)) {
  p(`  ${f.padEnd(5)} raw ${MB(v.raw).padStart(7)}  flac ${MB(v.flac).padStart(7)}  -> ${pct(v.flac, v.raw)}`);
}

writeFileSync(process.env.OUT ?? "/tmp/flac-lab-out.txt", out.join("\n"));
