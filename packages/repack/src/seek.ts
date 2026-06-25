// Seek-support tables (MVP-FOLLOWUP B1/B2/B3), baked into the manifest at ingest.
//
// Pure static analysis of the pattern/instrument data — NO audio rendering —
// producing up to three things for a module:
//
//   * timing map   — cumulative seconds at each order entry (the T <-> order:row
//                    map the seek bar needs, and the staleness/window clock).
//   * segment0     — the sample set the FIRST played pattern actually triggers,
//                    so the streamer can fetch the audible opening first (B2).
//                    This is the lever for the chopped-opening fix: the client
//                    holds playback until exactly these samples are resident.
//   * checkpoints  — per order, the RESIDENT sample set for a cold seek to that
//                    order (held-live + forward-window samples), so a seek fetches
//                    only those, not a whole-file prefix (B1/B3).
//
// IT-family (it/mptm) gets all three (ported from lab/seek-pack.mjs +
// lab/it-static.mjs, see lab/SEEK.md). MOD/S3M/XM get segment0 only (the
// first-pattern → sample scan; MOD logic ported from lab/mod-repack.mjs, XM/S3M
// decoders added here) — enough to gate the opening; cold-seek checkpoints for
// those formats are a follow-up. Everything else returns null.
//
// Sets are emitted as PCM byte OFFSETS (each sample's on-disk data pointer),
// which dag.ts maps to indices into manifest.samples[] (an offset-keyed subset).
// Samples with no raw PCM (compressed / absent) simply don't resolve and are
// dropped from the set — safe, they were never fetchable as chunks anyway.
//
// SAFETY INVARIANT for segment0: OVER-counting is safe (the client just waits
// for a few extra samples before starting); UNDER-counting reintroduces the
// chop. So the decoders track per-channel "last sample" to cover the
// note-with-no-sample (reuse-last) case, and any structural surprise returns
// null (module ships without tables and streams in file order, as before).

import type { Format } from "./parse.ts";

const u16 = (b: Uint8Array, o: number) => b[o] | (b[o + 1] << 8);
const u16be = (b: Uint8Array, o: number) => (b[o] << 8) | b[o + 1];
const u32 = (b: Uint8Array, o: number) =>
  (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;
const tag4 = (b: Uint8Array, o: number) =>
  String.fromCharCode(b[o], b[o + 1], b[o + 2], b[o + 3]);

// MOD channel count from the 4-char tag at 1080 (mirror of parse.ts modChannels).
function modChannels(t: string): number {
  if (t === "M.K." || t === "M!K!" || t === "FLT4" || t === "4CHN") return 4;
  if (t === "6CHN") return 6;
  if (t === "FLT8" || t === "8CHN" || t === "OCTA" || t === "CD81") return 8;
  let m = /^(\d)CHN$/.exec(t);
  if (m) return +m[1];
  m = /^(\d\d)CH$/.exec(t);
  if (m) return +m[1];
  m = /^(\d\d)CN$/.exec(t);
  if (m) return +m[1];
  m = /^TDZ(\d)$/.exec(t);
  if (m) return +m[1];
  return 0;
}

export interface SeekCheckpoint {
  order: number; // order-list index
  residentOffsets: number[]; // PCM offsets of samples resident at a seek to this order
}

export interface SeekTables {
  /** Cumulative seconds at the start of each order entry (valid orders only). */
  orderSeconds: { order: number; seconds: number }[];
  /** PCM offsets of samples the first played pattern triggers (audible opening). */
  segment0Offsets: number[];
  /** Per-order resident sample sets (PCM offsets) for cold seeks. */
  checkpoints: SeekCheckpoint[];
}

interface ItHeader {
  ordNum: number;
  insNum: number;
  smpNum: number;
  cmwt: number;
  useInstruments: boolean;
  insOffBase: number;
  smpOffBase: number;
  patOffBase: number;
  orders: number[];
  speed0: number;
  tempo0: number;
}

function readHeader(b: Uint8Array): ItHeader | null {
  if (b.length < 0xc0) return null;
  const ordNum = u16(b, 0x20),
    insNum = u16(b, 0x22),
    smpNum = u16(b, 0x24);
  const flags = u16(b, 0x2c >= b.length ? 0 : 0x2c);
  const cmwt = u16(b, 0x2a);
  const ordBase = 0xc0;
  const insOffBase = ordBase + ordNum;
  const smpOffBase = insOffBase + insNum * 4;
  const patOffBase = smpOffBase + smpNum * 4;
  if (patOffBase > b.length || smpNum === 0 || ordNum === 0) return null;
  const orders: number[] = [];
  for (let i = 0; i < ordNum; i++) orders.push(b[ordBase + i]);
  const speed0 = b[0x32] || 6,
    tempo0 = b[0x33] || 125;
  return {
    ordNum,
    insNum,
    smpNum,
    cmwt,
    useInstruments: (flags & 0x04) !== 0 && insNum > 0,
    insOffBase,
    smpOffBase,
    patOffBase,
    orders,
    speed0,
    tempo0,
  };
}

interface Ev {
  row: number;
  ch: number;
  note: number | null;
  ins: number;
  cmd: number;
  param: number;
}

// Decode one packed IT pattern into per-row channel events (ported verbatim from
// lab/seek-pack.mjs decodeEvents). note: 0..119 play, 254 cut, 255 off.
function decodeEvents(b: Uint8Array, off: number): { rows: number; events: Ev[] } {
  if (off === 0 || off + 8 > b.length) return { rows: 64, events: [] };
  const len = u16(b, off),
    rows = u16(b, off + 2);
  let p = off + 8;
  const end = Math.min(p + len, b.length);
  const lastMask = new Array(64).fill(0),
    lastIns = new Array(64).fill(0),
    lastNote = new Array(64).fill(0);
  const events: Ev[] = [];
  let row = 0;
  while (p < end && row < rows) {
    const cv = b[p++];
    if (cv === 0) {
      row++;
      continue;
    }
    const ch = (cv - 1) & 63;
    let mask = lastMask[ch];
    if (cv & 128) {
      mask = b[p++];
      lastMask[ch] = mask;
    }
    let note: number | null = null,
      cmd = 0,
      param = 0;
    if (mask & 1) {
      note = b[p++];
      lastNote[ch] = note;
    }
    if (mask & 2) {
      lastIns[ch] = b[p++];
    }
    if (mask & 4) p++; // volume column
    if (mask & 8) {
      cmd = b[p++];
      param = b[p++];
    }
    if (mask & 16) note = lastNote[ch];
    if (note !== null || cmd) events.push({ row, ch, note, ins: lastIns[ch], cmd, param });
  }
  return { rows, events };
}

// Resolve the sample (0-based) a (instrument, note) plays. In instrument mode,
// via the instrument's note->sample keyboard map; in sample mode, the "instrument"
// number IS the 1-based sample.
function sampleForNote(b: Uint8Array, h: ItHeader, ins: number, note: number): number {
  if (note < 0 || note > 119) return -1;
  if (!h.useInstruments) return ins > 0 ? ins - 1 : -1;
  if (ins <= 0) return -1;
  const off = u32(b, h.insOffBase + (ins - 1) * 4);
  if (off === 0) return -1;
  const map = off + (h.cmwt >= 0x200 ? 0x40 : 0x2e);
  const mo = map + note * 2 + 1;
  if (mo >= b.length) return -1;
  const s = b[mo];
  return s > 0 ? s - 1 : -1;
}

interface SmpInfo {
  offset: number; // PCM data pointer (manifest.samples[].offset)
  len: number; // sample length in frames
  c5: number; // C-5 playback rate
  loops: boolean;
  hasPcm: boolean;
}

function sampleInfos(b: Uint8Array, h: ItHeader): SmpInfo[] {
  const out: SmpInfo[] = [];
  for (let i = 0; i < h.smpNum; i++) {
    const so = u32(b, h.smpOffBase + i * 4);
    if (!so || so + 0x50 > b.length) {
      out.push({ offset: -1, len: 0, c5: 8363, loops: false, hasPcm: false });
      continue;
    }
    const flg = b[so + 0x12],
      len = u32(b, so + 0x30),
      ptr = u32(b, so + 0x48),
      c5 = u32(b, so + 0x3c) || 8363;
    const loops = !!(flg & 0x10) || !!(flg & 0x20);
    // hasPcm mirrors extractITLike: associated (1), has len+ptr, NOT compressed (8).
    const hasPcm = !!(flg & 1) && !!len && !!ptr && !(flg & 8);
    out.push({ offset: hasPcm ? ptr : -1, len, c5, loops, hasPcm });
  }
  return out;
}

const ROWSEC = (speed: number, tempo: number) => (speed * 2.5) / tempo; // ticks/row * 2.5/BPM
const MAX_CHECKPOINTS = 512; // cap manifest growth for pathological order lists
function sampleSeconds(s: SmpInfo, note: number): number {
  if (!s || s.len === 0) return 0;
  const rate = s.c5 * Math.pow(2, (note - 60) / 12);
  return rate > 0 ? s.len / rate : 0;
}

/**
 * Build seek-support tables for a module. IT-family (it/mptm) gets the full
 * tables (timing map + segment0 + cold-seek checkpoints); MOD/S3M/XM get
 * segment0 only (the first-pattern sample set, enough to gate the opening).
 * Returns null for anything else, or on any structural surprise.
 */
export function computeSeekTables(b: Uint8Array, format: Format): SeekTables | null {
  try {
    if (format === "it" || format === "mptm") return computeItTables(b);
    if (format === "mod") return modTables(b);
    if (format === "s3m") return s3mTables(b);
    if (format === "xm") return xmTables(b);
  } catch {
    return null; // any decode surprise -> ship without tables (file-order stream)
  }
  return null;
}

// ===========================================================================
// Generic held-note checkpoint engine (v2, decision 1: full checkpoints for all
// parsed formats). A format adapter decodes its patterns into NORMALIZED events
// and supplies a per-sample duration; this engine runs the timing + held-note +
// forward-window simulation identically for MOD/S3M/XM. IT keeps its own
// (already-proven) path above. Over-counting is safe, under-counting chops — so
// durations err long (loops => Infinity, non-loops padded) and the forward
// window covers imminent triggers.
// ===========================================================================

interface SimEvent {
  row: number;
  ch: number;
  explicitSample: number; // 0-based resolved sample, or -1 (none this cell)
  hasNote: boolean; // a note actually triggers on this cell
  note: number; // duration key (period for MOD, semitone index for S3M/XM)
  off: boolean; // note-off / cut -> channel goes silent
  setSpeed: number; // 0 = none
  setTempo: number; // 0 = none
}

interface SimModel {
  orderPats: { order: number; pat: number }[]; // valid orders, in play sequence
  speed0: number;
  tempo0: number;
  numChannels: number;
  decode: (pat: number) => { rows: number; events: SimEvent[] } | null; // row-sorted, memoized
  sampleSeconds: (sample: number, note: number) => number; // Infinity if looping
  offsetOf: (sample: number) => number; // PCM offset, or -1
}

type HeldNote = { sample: number; note: number; tOn: number } | null;

// Advance per-channel held/latch state for one event at song-time t.
function applySim(e: SimEvent, held: HeldNote[], latch: number[], t: number): void {
  if (e.off) held[e.ch] = null;
  if (e.explicitSample >= 0) latch[e.ch] = e.explicitSample;
  if (e.hasNote) {
    const s = e.explicitSample >= 0 ? e.explicitSample : latch[e.ch];
    if (s >= 0) {
      held[e.ch] = { sample: s, note: e.note, tOn: t };
      latch[e.ch] = s;
    }
  }
}

function simulateTables(m: SimModel): SeekTables {
  // --- timing map: cumulative seconds at the start of each played order ---
  const orderSeconds: { order: number; seconds: number }[] = [];
  {
    let speed = m.speed0,
      tempo = m.tempo0,
      t = 0;
    for (const { order, pat } of m.orderPats) {
      orderSeconds.push({ order, seconds: t });
      const ev = m.decode(pat);
      const rows = ev ? ev.rows : 64;
      let ei = 0;
      for (let row = 0; row < rows; row++) {
        if (ev)
          while (ei < ev.events.length && ev.events[ei].row === row) {
            const e = ev.events[ei++];
            if (e.setSpeed) speed = e.setSpeed;
            if (e.setTempo) tempo = e.setTempo;
          }
        t += ROWSEC(speed, tempo);
      }
    }
  }

  const stride = Math.max(1, Math.ceil(m.orderPats.length / MAX_CHECKPOINTS));
  const checkpoints: SeekCheckpoint[] = [];
  for (let vi = 0; vi < m.orderPats.length; vi += stride) {
    const N = m.orderPats[vi].order;
    let speed = m.speed0,
      tempo = m.tempo0,
      t = 0;
    const held: HeldNote[] = new Array(m.numChannels).fill(null);
    const latch: number[] = new Array(m.numChannels).fill(-1);
    // walk every played order strictly before position vi
    for (let pi = 0; pi < vi; pi++) {
      const ev = m.decode(m.orderPats[pi].pat);
      const rows = ev ? ev.rows : 64;
      let ei = 0;
      for (let row = 0; row < rows; row++) {
        if (ev)
          while (ei < ev.events.length && ev.events[ei].row === row) {
            const e = ev.events[ei++];
            applySim(e, held, latch, t);
            if (e.setSpeed) speed = e.setSpeed;
            if (e.setTempo) tempo = e.setTempo;
          }
        t += ROWSEC(speed, tempo);
      }
    }
    // A channel's last note-on is potentially-ringing until cut or replaced; keep
    // it resident regardless of any computed duration. Over-counting is safe (a
    // finished one-shot just lingers in the set, bounded by channel count) and it
    // removes all pitch/rate-reference risk — the source of mid-track chops.
    const resident = new Set<number>();
    for (const hc of held) {
      if (!hc) continue;
      const o = m.offsetOf(hc.sample);
      if (o >= 0) resident.add(o);
    }
    // Forward: every sample triggered between this checkpoint and the next, so
    // checkpoint(N) covers the ENTIRE span it is the floor for — a gap-free
    // continuous fence, not just a 0.5s cold-seek window. (held-at-N above + all
    // triggers in [N, nextCheckpoint) = everything audible across the span.)
    {
      const fl = latch.slice();
      const nextPos = Math.min(vi + stride, m.orderPats.length);
      for (let pi = vi; pi < nextPos; pi++) {
        const ev = m.decode(m.orderPats[pi].pat);
        if (!ev) continue;
        for (const e of ev.events) {
          if (e.explicitSample >= 0) fl[e.ch] = e.explicitSample;
          if (e.hasNote) {
            const s = e.explicitSample >= 0 ? e.explicitSample : fl[e.ch];
            const o = m.offsetOf(s);
            if (o >= 0) resident.add(o);
          }
        }
      }
    }
    checkpoints.push({ order: N, residentOffsets: [...resident].sort((a, c) => a - c) });
  }

  // segment0 = the first played order's triggered samples.
  const segment0 = new Set<number>();
  if (m.orderPats.length) {
    const ev = m.decode(m.orderPats[0].pat);
    const latch: number[] = new Array(m.numChannels).fill(-1);
    if (ev)
      for (const e of ev.events) {
        if (e.explicitSample >= 0) latch[e.ch] = e.explicitSample;
        if (e.hasNote) {
          const s = e.explicitSample >= 0 ? e.explicitSample : latch[e.ch];
          const o = m.offsetOf(s);
          if (o >= 0) segment0.add(o);
        }
      }
  }

  return {
    orderSeconds,
    segment0Offsets: [...segment0].sort((a, c) => a - c),
    checkpoints,
  };
}

const DUR_PAD = 1.3; // conservative pad on non-loop durations (pitch-ref slack)

/** Full IT-family tables: timing map + segment0 + cold-seek checkpoints. */
function computeItTables(b: Uint8Array): SeekTables | null {
  let h: ItHeader | null;
  try {
    h = readHeader(b);
  } catch {
    return null;
  }
  if (!h) return null;

  // Decode every pattern referenced by the order list, once.
  const patEvents = new Map<number, { rows: number; events: Ev[] }>();
  const decode = (pat: number) => {
    if (pat >= 254) return null;
    let e = patEvents.get(pat);
    if (!e) {
      const po = u32(b, h!.patOffBase + pat * 4);
      e = decodeEvents(b, po);
      patEvents.set(pat, e);
    }
    return e;
  };

  const smp = sampleInfos(b, h);
  const offOf = (s: number) => (s >= 0 && s < smp.length && smp[s].hasPcm ? smp[s].offset : -1);

  // --- timing map + per-order resident sets (single forward walk) ---
  const orderSeconds: { order: number; seconds: number }[] = [];
  const checkpoints: SeekCheckpoint[] = [];
  // held[ch] = { sample, note, tOn } — last note-on still ringing on the channel.
  type Held = { sample: number; note: number; tOn: number } | null;

  // For each valid order index, compute the resident set entering it. We do a
  // fresh walk 0..N for correctness (orders are few; total work stays modest and
  // bounded below). To keep it linear-ish we also accumulate the timing in one
  // pass and reuse it.
  // First pass: cumulative time at the start of each order entry.
  {
    let speed = h.speed0,
      tempo = h.tempo0,
      t = 0;
    for (let oi = 0; oi < h.ordNum; oi++) {
      const pat = h.orders[oi];
      if (pat >= 254) continue;
      orderSeconds.push({ order: oi, seconds: t });
      const ev = decode(pat);
      if (!ev) continue;
      let ei = 0;
      for (let row = 0; row < ev.rows; row++) {
        while (ei < ev.events.length && ev.events[ei].row === row) {
          const e = ev.events[ei++];
          if (e.cmd === 1 && e.param) speed = e.param; // Axx set speed
          else if (e.cmd === 0x14 && e.param >= 0x20) tempo = e.param; // Txx set tempo
        }
        t += ROWSEC(speed, tempo);
      }
    }
  }

  const validOrders = orderSeconds.map((x) => x.order);
  const stride = Math.max(1, Math.ceil(validOrders.length / MAX_CHECKPOINTS));

  for (let vi = 0; vi < validOrders.length; vi += stride) {
    const N = validOrders[vi];
    // walk 0..N tracking held notes + time, honoring Axx/Txx
    let speed = h.speed0,
      tempo = h.tempo0,
      t = 0;
    const held: Held[] = new Array(64).fill(null);
    for (let oi = 0; oi < N; oi++) {
      const pat = h.orders[oi];
      if (pat >= 254) continue;
      const ev = decode(pat);
      if (!ev) continue;
      let ei = 0;
      for (let row = 0; row < ev.rows; row++) {
        while (ei < ev.events.length && ev.events[ei].row === row) {
          const e = ev.events[ei++];
          if (e.cmd === 1 && e.param) speed = e.param;
          else if (e.cmd === 0x14 && e.param >= 0x20) tempo = e.param;
          if (e.note === 254 || e.note === 255) held[e.ch] = null; // cut / off
          else if (e.note !== null && e.note < 120) {
            const s = sampleForNote(b, h, e.ins, e.note);
            if (s >= 0) held[e.ch] = { sample: s, note: e.note, tOn: t };
          }
        }
        t += ROWSEC(speed, tempo);
      }
    }
    // Last note-on per channel is potentially-ringing until cut/replaced — keep it
    // resident regardless of duration (over-count safe, no pitch-ref risk).
    const resident = new Set<number>();
    for (const hc of held) {
      if (!hc) continue;
      const o = offOf(hc.sample);
      if (o >= 0) resident.add(o);
    }
    // Forward: every sample triggered between N and the next checkpoint's order,
    // so checkpoint(N) covers the whole span it is the floor for (gap-free
    // continuous fence, not just a 0.5s cold-seek window).
    {
      const nextOrder = vi + stride < validOrders.length ? validOrders[vi + stride] : h.ordNum;
      for (let oi = N; oi < nextOrder; oi++) {
        const pat = h.orders[oi];
        if (pat >= 254) continue;
        const ev = decode(pat);
        if (!ev) continue;
        for (const e of ev.events) {
          if (e.note !== null && e.note < 120) {
            const o = offOf(sampleForNote(b, h, e.ins, e.note));
            if (o >= 0) resident.add(o);
          }
        }
      }
    }
    checkpoints.push({ order: N, residentOffsets: [...resident].sort((a, c) => a - c) });
  }

  // segment0 = resident set of the first played order (== checkpoints[0] if vi=0
  // started there). Compute explicitly from the first valid order's pattern for
  // clarity (the audible opening: every sample its events trigger).
  const segment0 = new Set<number>();
  if (validOrders.length) {
    const ev = decode(h.orders[validOrders[0]]);
    if (ev) for (const e of ev.events) {
      if (e.note !== null && e.note < 120) {
        const o = offOf(sampleForNote(b, h, e.ins, e.note));
        if (o >= 0) segment0.add(o);
      }
    }
  }

  return {
    orderSeconds,
    segment0Offsets: [...segment0].sort((a, c) => a - c),
    checkpoints,
  };
}


// --- MOD (ProTracker) full tables ----------------------------------------------
// Sample data is concatenated after the patterns in sample order; each 4-byte
// cell: sample = (b0&0xf0)|(b2>>4), period = ((b0&0xf)<<8)|b1, effect = b2&0xf /
// b3. A period with sample 0 reuses the channel's latched sample. Duration uses
// the period directly (PAL paula rate), so MOD needs no pitch reference.
function modTables(b: Uint8Array): SeekTables | null {
  if (b.length < 1084) return null;
  const ch = modChannels(tag4(b, 1080));
  if (!ch) return null;
  const songLen = b[950];
  let maxPat = 0;
  for (let i = 0; i < 128; i++) if (b[952 + i] > maxPat) maxPat = b[952 + i];
  const rowBytes = 64 * ch * 4;
  const smpDataStart = 1084 + (maxPat + 1) * rowBytes;

  const frames = new Array(31).fill(0);
  const loops = new Array(31).fill(false);
  const offsetOf = new Array(31).fill(-1);
  {
    let off = smpDataStart;
    for (let i = 0; i < 31; i++) {
      const n = u16be(b, 20 + i * 30 + 22) * 2;
      const replen = u16be(b, 20 + i * 30 + 28);
      if (n <= 2) continue;
      frames[i] = n; // 8-bit mono -> 1 byte/frame
      loops[i] = replen > 1;
      offsetOf[i] = off;
      if (off + n > b.length) {
        frames[i] = b.length - off;
        break;
      }
      off += n;
    }
  }

  const orderPats: { order: number; pat: number }[] = [];
  for (let i = 0; i < songLen && i < 128; i++) {
    const pat = b[952 + i];
    if (pat <= maxPat) orderPats.push({ order: i, pat });
  }

  const cache = new Map<number, { rows: number; events: SimEvent[] }>();
  const decode = (pat: number) => {
    let e = cache.get(pat);
    if (e) return e;
    const base = 1084 + pat * rowBytes;
    const events: SimEvent[] = [];
    if (base + rowBytes <= b.length) {
      for (let row = 0; row < 64; row++) {
        for (let c = 0; c < ch; c++) {
          const cell = base + (row * ch + c) * 4;
          const sample = (b[cell] & 0xf0) | (b[cell + 2] >> 4);
          const period = ((b[cell] & 0x0f) << 8) | b[cell + 1];
          const cmd = b[cell + 2] & 0x0f;
          const param = b[cell + 3];
          let setSpeed = 0,
            setTempo = 0;
          if (cmd === 0x0f && param > 0) {
            if (param < 0x20) setSpeed = param;
            else setTempo = param;
          }
          const hasNote = period > 0;
          const explicitSample = sample > 0 ? sample - 1 : -1;
          if (hasNote || explicitSample >= 0 || setSpeed || setTempo)
            events.push({ row, ch: c, explicitSample, hasNote, note: period, off: false, setSpeed, setTempo });
        }
      }
    }
    e = { rows: 64, events };
    cache.set(pat, e);
    return e;
  };

  return simulateTables({
    orderPats,
    speed0: 6,
    tempo0: 125,
    numChannels: ch,
    decode,
    offsetOf: (s) => (s >= 0 && s < 31 ? offsetOf[s] : -1),
    sampleSeconds: (s, period) => {
      if (s < 0 || s >= 31 || frames[s] === 0) return 0;
      if (loops[s]) return Infinity;
      if (period <= 0) return Infinity; // unknown pitch -> keep resident (safe)
      return ((frames[s] * period) / 3546895) * DUR_PAD; // PAL paula clock
    },
  });
}

// --- S3M (ScreamTracker 3) full tables -----------------------------------------
// Sample-mode: a cell's instrument number IS the 1-based sample. Packed pattern:
// per row, bytes until a 0 terminator; flags select note+instrument (2), volume
// (1), command (2). Effect A (1) sets speed, T (20) sets tempo. C2SPD drives the
// per-note rate (ref C-5 = semitone 60, as IT).
function s3mTables(b: Uint8Array): SeekTables | null {
  if (b.length < 0x60 || tag4(b, 0x2c) !== "SCRM") return null;
  const ordNum = u16(b, 0x20),
    insNum = u16(b, 0x22);
  const speed0 = b[0x31] || 6,
    tempo0 = b[0x32] || 125;
  const paraInsBase = 0x60 + ordNum;
  const paraPatBase = paraInsBase + insNum * 2;

  const frames = new Array(insNum).fill(0);
  const c2spd = new Array(insNum).fill(8363);
  const loops = new Array(insNum).fill(false);
  const offsetOf = new Array(insNum).fill(-1);
  for (let i = 0; i < insNum; i++) {
    const pp = u16(b, paraInsBase + i * 2);
    if (!pp) continue;
    const off = pp * 16;
    if (off + 0x50 > b.length || b[off] !== 1) continue; // type 1 = PCM
    const ptr = ((b[off + 0x0d] << 16) | u16(b, off + 0x0e)) * 16;
    const len = u32(b, off + 0x10);
    const flg = b[off + 0x1f];
    const spd = u32(b, off + 0x20) || 8363;
    if (!len || !ptr) continue;
    frames[i] = len; // length is in frames
    c2spd[i] = spd;
    loops[i] = !!(flg & 1);
    offsetOf[i] = ptr < b.length ? ptr : -1;
  }

  const orderPats: { order: number; pat: number }[] = [];
  for (let i = 0; i < ordNum; i++) {
    const o = b[0x60 + i];
    if (o < 254) orderPats.push({ order: i, pat: o });
  }

  const cache = new Map<number, { rows: number; events: SimEvent[] }>();
  const decode = (pat: number) => {
    let e = cache.get(pat);
    if (e) return e;
    const events: SimEvent[] = [];
    const ppat = u16(b, paraPatBase + pat * 2);
    if (ppat) {
      const po = ppat * 16;
      if (po + 2 <= b.length) {
        const packLen = u16(b, po);
        let p = po + 2;
        const end = Math.min(po + 2 + packLen, b.length);
        let row = 0;
        while (p < end && row < 64) {
          const what = b[p++];
          if (what === 0) {
            row++;
            continue;
          }
          const chan = what & 31;
          let note = -1,
            ins = 0,
            cmd = 0,
            info = 0,
            hasCmd = false;
          if (what & 32) {
            note = b[p++];
            ins = b[p++];
          }
          if (what & 64) p++; // volume column
          if (what & 128) {
            cmd = b[p++];
            info = b[p++];
            hasCmd = true;
          }
          let setSpeed = 0,
            setTempo = 0;
          if (hasCmd) {
            if (cmd === 1 && info) setSpeed = info; // A: set speed
            else if (cmd === 20 && info >= 0x20) setTempo = info; // T: set tempo
          }
          const off = note === 254 || note === 255; // cut / off
          const hasNote = note >= 0 && note < 254;
          const noteNum = hasNote ? (note >> 4) * 12 + (note & 0x0f) : 0;
          const explicitSample = ins > 0 ? ins - 1 : -1;
          if (hasNote || explicitSample >= 0 || off || setSpeed || setTempo)
            events.push({ row, ch: chan, explicitSample, hasNote, note: noteNum, off, setSpeed, setTempo });
        }
      }
    }
    e = { rows: 64, events };
    cache.set(pat, e);
    return e;
  };

  return simulateTables({
    orderPats,
    speed0,
    tempo0,
    numChannels: 32,
    decode,
    offsetOf: (s) => (s >= 0 && s < insNum ? offsetOf[s] : -1),
    sampleSeconds: (s, noteNum) => {
      if (s < 0 || s >= insNum || frames[s] === 0) return 0;
      if (loops[s]) return Infinity;
      const rate = c2spd[s] * Math.pow(2, (noteNum - 60) / 12);
      return rate > 0 ? (frames[s] / rate) * DUR_PAD : Infinity;
    },
  });
}

// --- XM (FastTracker 2) full tables --------------------------------------------
// Instrument-based: (note, instrument) -> sample via the instrument's 96-byte
// note->sample keymap; samples are numbered sequentially across instruments (the
// global slot). Effect F (0x0f) sets speed (<0x20) or BPM (>=0x20). Duration uses
// the XM linear-frequency period (relative note + finetune). Note-without-
// instrument falls back to the channel's latched sample (engine), an accepted
// approximation for multi-sampled instruments.
function xmTables(b: Uint8Array): SeekTables | null {
  if (b.length < 80 || tag4(b, 0) !== "Exte") return null;
  const headerSize = u32(b, 60);
  const songLen = u16(b, 64);
  const numCh = u16(b, 68);
  const npat = u16(b, 70),
    nins = u16(b, 72);
  if (!numCh || numCh > 64) return null;
  const speed0 = u16(b, 76) || 6,
    tempo0 = u16(b, 78) || 125;

  let pos = 60 + headerSize;
  const pats: { dataStart: number; rows: number; packed: number }[] = [];
  for (let p = 0; p < npat; p++) {
    if (pos + 9 > b.length) return null;
    const phLen = u32(b, pos),
      rows = u16(b, pos + 5),
      packed = u16(b, pos + 7);
    pats.push({ dataStart: pos + phLen, rows, packed });
    pos += phLen + packed;
  }

  // Global sample arrays + per-instrument firstSlot/keymap (sequential slots).
  const sFrames: number[] = [],
    sLoops: boolean[] = [],
    sRel: number[] = [],
    sFine: number[] = [],
    sOff: number[] = [];
  const insFirst: number[] = [],
    insKeymap: Uint8Array[] = [];
  for (let ins = 0; ins < nins; ins++) {
    if (pos + 29 > b.length) break;
    const instStart = pos;
    const instSize = u32(b, pos);
    const numSamp = u16(b, pos + 27);
    insFirst.push(sFrames.length);
    if (numSamp === 0) {
      insKeymap.push(new Uint8Array(96));
      pos = instStart + instSize;
      continue;
    }
    const shSize = u32(b, pos + 29);
    const km = b.subarray(instStart + 33, instStart + 33 + 96);
    insKeymap.push(km.length === 96 ? new Uint8Array(km) : new Uint8Array(96));
    let hdr = instStart + instSize;
    const meta: { len: number; bits: number; rel: number; fine: number; loop: boolean }[] = [];
    for (let s = 0; s < numSamp; s++) {
      if (hdr + 18 > b.length) break;
      const len = u32(b, hdr);
      const fine = (b[hdr + 13] << 24) >> 24; // s8
      const type = b[hdr + 14];
      const rel = (b[hdr + 16] << 24) >> 24; // s8
      meta.push({ len, bits: type & 0x10 ? 16 : 8, rel, fine, loop: (type & 3) !== 0 });
      hdr += shSize;
    }
    let data = hdr;
    for (const mt of meta) {
      const bpf = mt.bits / 8;
      sFrames.push(mt.len > 0 ? mt.len / bpf : 0);
      sLoops.push(mt.loop);
      sRel.push(mt.rel);
      sFine.push(mt.fine);
      sOff.push(mt.len > 0 && data + mt.len <= b.length ? data : -1);
      data += mt.len;
    }
    pos = data;
  }

  const orderPats: { order: number; pat: number }[] = [];
  for (let i = 0; i < songLen && i < 256; i++) {
    const o = b[80 + i];
    if (o < npat) orderPats.push({ order: i, pat: o });
  }

  const cache = new Map<number, { rows: number; events: SimEvent[] }>();
  const decode = (pat: number) => {
    let e = cache.get(pat);
    if (e) return e;
    const pi = pats[pat];
    const events: SimEvent[] = [];
    if (pi) {
      let p = pi.dataStart;
      const end = Math.min(pi.dataStart + pi.packed, b.length);
      for (let row = 0; row < pi.rows && p < end; row++) {
        for (let c = 0; c < numCh && p < end; c++) {
          let note = 0,
            ins = 0,
            hasNoteByte = false,
            hasIns = false,
            efftype = 0,
            effparam = 0,
            hasEff = false;
          const first = b[p++];
          if (first & 0x80) {
            if (first & 0x01) {
              note = b[p++];
              hasNoteByte = true;
            }
            if (first & 0x02) {
              ins = b[p++];
              hasIns = true;
            }
            if (first & 0x04) p++; // volume
            if (first & 0x08) {
              efftype = b[p++];
              hasEff = true;
            }
            if (first & 0x10) {
              effparam = b[p++];
              hasEff = true;
            }
          } else {
            note = first;
            hasNoteByte = true;
            ins = b[p++];
            hasIns = true;
            p++; // volume
            efftype = b[p++];
            effparam = b[p++];
            hasEff = true;
          }
          let setSpeed = 0,
            setTempo = 0;
          if (hasEff && efftype === 0x0f) {
            if (effparam < 0x20) setSpeed = effparam;
            else setTempo = effparam;
          }
          const off = note === 97; // key off
          const hasNote = hasNoteByte && note > 0 && note < 97;
          let explicitSample = -1;
          if (hasNote && hasIns && ins > 0 && ins <= nins) {
            const map = insKeymap[ins - 1];
            const local = map ? map[note - 1] : 0;
            explicitSample = insFirst[ins - 1] + local;
            if (explicitSample >= sFrames.length) explicitSample = -1;
          }
          const noteNum = hasNote ? note : 0;
          if (hasNote || explicitSample >= 0 || off || setSpeed || setTempo)
            events.push({ row, ch: c, explicitSample, hasNote, note: noteNum, off, setSpeed, setTempo });
        }
      }
    }
    e = { rows: pi ? pi.rows : 64, events };
    cache.set(pat, e);
    return e;
  };

  return simulateTables({
    orderPats,
    speed0,
    tempo0,
    numChannels: numCh,
    decode,
    offsetOf: (s) => (s >= 0 && s < sOff.length ? sOff[s] : -1),
    sampleSeconds: (s, note) => {
      if (s < 0 || s >= sFrames.length || sFrames[s] === 0) return 0;
      if (sLoops[s]) return Infinity;
      const period = 7680 - (note - 1 + sRel[s]) * 64 - sFine[s] / 2; // XM linear
      const rate = 8363 * Math.pow(2, (4608 - period) / 768);
      return rate > 0 ? (sFrames[s] / rate) * DUR_PAD : Infinity;
    },
  });
}
