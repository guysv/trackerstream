// Time-To-First-Pattern-Playback lab for the NON-FORK streaming approach.
//
// Hypothesis: stock libopenmpt tolerates a truncated module buffer and renders
// the opening correctly as long as the samples used by early patterns are
// present. If so, we can stream a growing buffer and recreate the module as
// bytes arrive — no engine fork needed. The smaller the prefix needed for
// correct opening audio, the lower the time-to-first-pattern-playback.
//
// For each module we find the minimal leading prefix such that:
//   - create()    succeeds at all
//   - "playable"  opening is non-silent (>=25% of reference loudness)
//   - "correct"   opening matches the full-file render (similarity >= 0.98)
// then derive TTFPB = download(prefix) + parse(prefix) at sample bandwidths.

import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import * as ompt from './lib.mjs';

const DIR = join(homedir(), 'tmp', 'somemods');
const WINDOW = 5;            // seconds of opening audio compared (proxy for "first pattern")
const SIM_OK = 0.98;         // similarity threshold for "correct"
const PLAYABLE = 0.25;       // fraction of reference RMS to count as "non-silent"
const BW = { '10 Mbit': 10e6 / 8, '50 Mbit': 50e6 / 8 };  // bytes/sec

const fmt = (n, w = 7) => String(n).padStart(w);
const pct = (x) => (x * 100).toFixed(1) + '%';
const kb = (b) => (b / 1024).toFixed(0) + 'K';

await ompt.load();

const files = readdirSync(DIR).filter(f => !f.startsWith('.') && statSync(join(DIR, f)).isFile())
  .map(f => ({ name: f, size: statSync(join(DIR, f)).size }))
  .sort((a, b) => a.size - b.size);

// render the opening from a prefix of `bytes` of given length; returns {ok, audio, parseMs}
function renderPrefix(bytes, len) {
  const { ptr, ms } = ompt.create(bytes.subarray(0, len));
  if (!ptr) return { ok: false, ms };
  const audio = ompt.render(ptr, WINDOW);
  ompt.destroy(ptr);
  return { ok: true, audio, ms };
}

const rows = [];
for (const { name, size } of files) {
  const bytes = readFileSync(join(DIR, name));

  // full-file reference (also average parse time over a few runs)
  let parseMs = Infinity;
  for (let i = 0; i < 3; i++) { const r = ompt.create(bytes); parseMs = Math.min(parseMs, r.ms); if (i < 2) ompt.destroy(r.ptr); else { var full = r.ptr; } }
  const nfo = ompt.info(full);
  const ref = ompt.render(full, WINDOW);
  ompt.destroy(full);
  const { refRms } = ompt.similarity(ref, ref);

  // coarse scan for create-success and playable thresholds
  const fracs = [0.01,0.02,0.03,0.05,0.08,0.12,0.18,0.25,0.35,0.5,0.7,0.85,1.0];
  let minCreate = null, minPlay = null;
  for (const f of fracs) {
    const len = Math.max(1, Math.floor(size * f));
    const r = renderPrefix(bytes, len);
    if (!r.ok) continue;
    if (minCreate === null) minCreate = f;
    const { testRms } = ompt.similarity(ref, r.audio);
    if (minPlay === null && testRms >= PLAYABLE * refRms) { minPlay = f; }
    if (minPlay !== null) break; // playable found; correctness pinned via bisection below
  }

  // bisection for minimal "correct" prefix (similarity >= SIM_OK)
  let lo = Math.floor(size * (minPlay ?? 0.5)), hi = size, correctLen = size;
  // ensure hi is correct; if even full minus tail differs, fall back to size
  for (let it = 0; it < 16 && hi - lo > Math.max(256, size * 0.005); it++) {
    const mid = (lo + hi) >> 1;
    const r = renderPrefix(bytes, mid);
    const sim = r.ok ? ompt.similarity(ref, r.audio).sim : 0;
    if (sim >= SIM_OK) { correctLen = mid; hi = mid; } else { lo = mid; }
  }

  const createLen = Math.floor(size * (minCreate ?? 1));
  const playLen = Math.floor(size * (minPlay ?? 1));
  rows.push({ name, size, type: nfo.type, dur: nfo.dur, patterns: nfo.patterns, samples: nfo.samples,
    parseMs, createLen, playLen, correctLen });
}

// ---- report ----
console.log(`\nwindow=${WINDOW}s  sim_ok=${SIM_OK}  files=${rows.length}\n`);

console.log('file                          fmt      size   parse   minCreate   minPlay   minCORRECT(prefix)');
console.log('-'.repeat(100));
for (const r of rows) {
  console.log(
    r.name.slice(0, 28).padEnd(29),
    (r.type || '?').padEnd(5),
    fmt(kb(r.size), 7),
    fmt(r.parseMs.toFixed(1) + 'ms', 8),
    fmt(pct(r.createLen / r.size), 9),
    fmt(pct(r.playLen / r.size), 9),
    fmt(pct(r.correctLen / r.size) + ' (' + kb(r.correctLen) + ')', 18),
  );
}

console.log('\n=== Time-to-first-pattern-playback (TTFPB) using ORIGINAL byte order ===');
console.log('TTFPB = download(correct-prefix) + parse(prefix).  Compared to whole-file baseline.\n');
console.log('file                          correctPfx   ' + Object.keys(BW).map(k => `TTFPB@${k}`.padStart(16)).join('') + '     whole@50Mbit');
console.log('-'.repeat(108));
let sumPrefix = 0, sumWhole = 0;
for (const r of rows) {
  const cells = Object.values(BW).map(bps => {
    const t = (r.correctLen / bps) * 1000 + r.parseMs;
    return (t.toFixed(0) + 'ms').padStart(16);
  });
  const whole = (r.size / BW['50 Mbit']) * 1000 + r.parseMs;
  sumPrefix += (r.correctLen / BW['50 Mbit']) * 1000 + r.parseMs;
  sumWhole += whole;
  console.log(r.name.slice(0, 28).padEnd(29), pct(r.correctLen / r.size).padStart(8), '  ', cells.join(''), (whole.toFixed(0) + 'ms').padStart(16));
}
console.log('-'.repeat(108));
console.log(`mean TTFPB @50Mbit (correct prefix): ${(sumPrefix / rows.length).toFixed(0)}ms   vs whole-file: ${(sumWhole / rows.length).toFixed(0)}ms`);
console.log(`\nNote: prefix uses ORIGINAL file order. A repack that front-loads early-needed`);
console.log(`instruments would shrink "minCORRECT" further — that's the repack upside to quantify next.`);
