// Seek-support tables (MVP-FOLLOWUP B1/B2/B3), baked into the manifest at ingest.
//
// Ported from lab/seek-pack.mjs + lab/it-static.mjs (see lab/SEEK.md). Pure
// static analysis of the IT-family pattern/instrument data — NO audio rendering —
// producing three things for a module:
//
//   * timing map   — cumulative seconds at each order entry (the T <-> order:row
//                    map the seek bar needs, and the staleness/window clock).
//   * segment0     — the sample set the FIRST played pattern actually triggers,
//                    so the streamer can fetch the audible opening first (B2).
//   * checkpoints  — per order, the RESIDENT sample set for a cold seek to that
//                    order (held-live + forward-window samples), so a seek fetches
//                    only those, not a whole-file prefix (B1/B3).
//
// Sets are emitted as PCM byte OFFSETS (each IT sample's on-disk data pointer),
// which dag.ts maps to indices into manifest.samples[] (an offset-keyed subset).
// Samples with no raw PCM (compressed / absent) simply don't resolve and are
// dropped from the set — safe, they were never fetchable as chunks anyway.
//
// IT-family only (it / mptm); returns null otherwise. Heavily guarded: any
// structural surprise returns null, so the module just ships without tables
// (streams in file order, exactly as before).

import type { Format } from "./parse.ts";

const u16 = (b: Uint8Array, o: number) => b[o] | (b[o + 1] << 8);
const u32 = (b: Uint8Array, o: number) =>
  (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

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
function sampleSeconds(s: SmpInfo, note: number): number {
  if (!s || s.len === 0) return 0;
  const rate = s.c5 * Math.pow(2, (note - 60) / 12);
  return rate > 0 ? s.len / rate : 0;
}

/**
 * Build the seek tables for an IT-family module. `format` gates this to it/mptm.
 */
export function computeSeekTables(b: Uint8Array, format: Format): SeekTables | null {
  if (format !== "it" && format !== "mptm") return null;
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
  const WIN = 0.5; // first-audio window seconds (lab SHORT)
  const MAX_CHECKPOINTS = 512; // cap manifest growth for pathological order lists
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
    const resident = new Set<number>();
    for (const hc of held) {
      if (!hc) continue;
      const s = smp[hc.sample];
      if (s && (s.loops || t - hc.tOn < sampleSeconds(s, hc.note))) {
        const o = offOf(hc.sample);
        if (o >= 0) resident.add(o);
      }
    }
    // forward window: triggers within WIN seconds from N
    {
      let tw = 0;
      for (let oi = N; oi < h.ordNum && tw < WIN; oi++) {
        const pat = h.orders[oi];
        if (pat >= 254) continue;
        const ev = decode(pat);
        if (!ev) continue;
        let ei = 0,
          sp = speed,
          tp = tempo;
        for (let row = 0; row < ev.rows && tw < WIN; row++) {
          while (ei < ev.events.length && ev.events[ei].row === row) {
            const e = ev.events[ei++];
            if (e.cmd === 1 && e.param) sp = e.param;
            else if (e.cmd === 0x14 && e.param >= 0x20) tp = e.param;
            if (e.note !== null && e.note < 120) {
              const s = sampleForNote(b, h, e.ins, e.note);
              const o = offOf(s);
              if (o >= 0) resident.add(o);
            }
          }
          tw += ROWSEC(sp, tp);
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
