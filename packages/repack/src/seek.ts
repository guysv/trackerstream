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

/** A segment0-only result (no timing map / checkpoints) for MOD/S3M/XM. */
function seg0Only(offsets: number[]): SeekTables {
  const uniq = [...new Set(offsets)].filter((o) => o >= 0).sort((a, b) => a - b);
  return { orderSeconds: [], segment0Offsets: uniq, checkpoints: [] };
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
    if (format === "mod") return modSegment0(b);
    if (format === "s3m") return s3mSegment0(b);
    if (format === "xm") return xmSegment0(b);
  } catch {
    return null; // any decode surprise -> ship without tables (file-order stream)
  }
  return null;
}

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

// --- MOD (ProTracker) segment0 (ported from lab/mod-repack.mjs) ----------------
// Sample data is concatenated after the patterns in sample order; each cell is 4
// bytes, sample number = (b0 & 0xf0) | (b2 >> 4), period = ((b0 & 0xf) << 8) | b1.
// A cell with a period but sample 0 reuses the channel's last sample.
function modSegment0(b: Uint8Array): SeekTables | null {
  if (b.length < 1084) return null;
  const ch = modChannels(tag4(b, 1080));
  if (!ch) return null; // 15-sample / unknown variants -> file order

  const lens: number[] = [];
  for (let i = 0; i < 31; i++) lens.push(u16be(b, 20 + i * 30 + 22) * 2);
  let maxPat = 0;
  for (let i = 0; i < 128; i++) if (b[952 + i] > maxPat) maxPat = b[952 + i];
  const rowBytes = 64 * ch * 4;
  const smpDataStart = 1084 + (maxPat + 1) * rowBytes;

  // Per-sample PCM offsets, mirroring parse.ts extractMOD exactly (samples with
  // <= 2 bytes contribute no data and don't advance the cursor).
  const offsetOf = new Array(31).fill(-1);
  {
    let off = smpDataStart;
    for (let i = 0; i < 31; i++) {
      const n = lens[i];
      if (n <= 2) continue;
      offsetOf[i] = off;
      if (off + n > b.length) break; // last sample clamped to EOF
      off += n;
    }
  }

  const ord0 = b[952]; // first order entry
  if (ord0 > maxPat) return seg0Only([]);
  const patBase = 1084 + ord0 * rowBytes;
  if (patBase + rowBytes > b.length) return null;

  const used = new Set<number>();
  const lastSample = new Array(ch).fill(-1);
  for (let row = 0; row < 64; row++) {
    for (let c = 0; c < ch; c++) {
      const cell = patBase + (row * ch + c) * 4;
      const s = (b[cell] & 0xf0) | (b[cell + 2] >> 4);
      const period = ((b[cell] & 0x0f) << 8) | b[cell + 1];
      if (s > 0) lastSample[c] = s - 1;
      if (period > 0 || s > 0) {
        const si = s > 0 ? s - 1 : lastSample[c];
        if (si >= 0 && offsetOf[si] >= 0) used.add(offsetOf[si]);
      }
    }
  }
  return seg0Only([...used]);
}

// --- S3M (ScreamTracker 3) segment0 --------------------------------------------
// Sample-mode: a row event's instrument number IS the 1-based sample. Patterns
// are packed: per row, bytes until a 0 terminator; flags select note+instrument
// (2 bytes), volume (1), command (2). PCM offset comes from the sample header's
// parapointer (matching parse.ts extractS3M), so it's read straight from disk.
function s3mSegment0(b: Uint8Array): SeekTables | null {
  if (b.length < 0x60 || tag4(b, 0x2c) !== "SCRM") return null;
  const ordNum = u16(b, 0x20),
    insNum = u16(b, 0x22);
  const ordBase = 0x60;
  const paraInsBase = ordBase + ordNum; // instrument parapointers (2 bytes each)
  const paraPatBase = paraInsBase + insNum * 2; // pattern parapointers

  const offsetOf = new Array(insNum).fill(-1);
  for (let i = 0; i < insNum; i++) {
    const pp = u16(b, paraInsBase + i * 2);
    if (!pp) continue;
    const off = pp * 16;
    if (off + 0x50 > b.length || b[off] !== 1) continue; // type 1 = PCM
    const ptr = ((b[off + 0x0d] << 16) | u16(b, off + 0x0e)) * 16;
    const len = u32(b, off + 0x10);
    if (len && ptr) offsetOf[i] = ptr;
  }

  // First played order (skip 254 = marker, 255 = end).
  let firstPat = -1;
  for (let k = 0; k < ordNum; k++) {
    const o = b[ordBase + k];
    if (o < 254) {
      firstPat = o;
      break;
    }
  }
  if (firstPat < 0) return seg0Only([]);
  const ppat = u16(b, paraPatBase + firstPat * 2);
  if (!ppat) return seg0Only([]);
  const po = ppat * 16;
  if (po + 2 > b.length) return null;

  const used = new Set<number>();
  const lastIns = new Array(32).fill(-1);
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
    if (what & 32) {
      const note = b[p++];
      const ins = b[p++];
      if (ins > 0) lastIns[chan] = ins - 1;
      if (note < 254) {
        const si = ins > 0 ? ins - 1 : lastIns[chan];
        if (si >= 0 && si < insNum && offsetOf[si] >= 0) used.add(offsetOf[si]);
      }
    }
    if (what & 64) p++; // volume column
    if (what & 128) p += 2; // command + info
  }
  return seg0Only([...used]);
}

// --- XM (FastTracker 2) segment0 -----------------------------------------------
// Instrument-based: a (note, instrument) plays a sample via the instrument's
// note->sample map. We take the SAFE over-approximation — include every sample
// of any instrument triggered in the first pattern — which avoids decoding the
// keyboard map (most XM instruments have one sample anyway). Offsets mirror
// parse.ts extractXM (samples concatenated per instrument, in order).
function xmSegment0(b: Uint8Array): SeekTables | null {
  if (b.length < 80 || tag4(b, 0) !== "Exte") return null;
  const headerSize = u32(b, 60);
  const songLen = u16(b, 64);
  const numCh = u16(b, 68);
  const npat = u16(b, 70),
    nins = u16(b, 72);
  if (!numCh || numCh > 64) return null;

  // Walk pattern headers, recording each pattern's packed-data start/rows/size.
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

  // Walk instruments, recording each instrument's sample PCM offsets (mirror of
  // parse.ts extractXM: per-instrument sample headers then concatenated data).
  const instSamples: number[][] = [];
  for (let ins = 0; ins < nins; ins++) {
    if (pos + 29 > b.length) break;
    const instStart = pos;
    const instSize = u32(b, pos);
    const numSamp = u16(b, pos + 27);
    if (numSamp === 0) {
      instSamples.push([]);
      pos = instStart + instSize;
      continue;
    }
    const shSize = u32(b, pos + 29);
    let hdr = instStart + instSize;
    const lens: number[] = [];
    for (let s = 0; s < numSamp; s++) {
      if (hdr + 18 > b.length) break;
      lens.push(u32(b, hdr));
      hdr += shSize;
    }
    let data = hdr;
    const offs: number[] = [];
    for (let s = 0; s < lens.length; s++) {
      const n = lens[s];
      if (n > 0 && data + n <= b.length) offs.push(data);
      data += n;
    }
    instSamples.push(offs);
    pos = data;
  }

  // First played order (entries >= npat are skip markers).
  let firstPat = -1;
  for (let k = 0; k < songLen && k < 256; k++) {
    const o = b[80 + k];
    if (o < npat) {
      firstPat = o;
      break;
    }
  }
  if (firstPat < 0 || firstPat >= pats.length) return seg0Only([]);

  // Decode the XM pattern: row-major note slots. A leading byte with 0x80 set is
  // a bit-mask of present fields; otherwise it IS the note and all 5 fields follow.
  const pi = pats[firstPat];
  const used = new Set<number>(); // instrument indices
  const lastIns = new Array(numCh).fill(-1);
  let p = pi.dataStart;
  const end = Math.min(pi.dataStart + pi.packed, b.length);
  for (let row = 0; row < pi.rows && p < end; row++) {
    for (let c = 0; c < numCh && p < end; c++) {
      let note = 0,
        ins = 0,
        hasNote = false,
        hasIns = false;
      const first = b[p++];
      if (first & 0x80) {
        if (first & 0x01) {
          note = b[p++];
          hasNote = true;
        }
        if (first & 0x02) {
          ins = b[p++];
          hasIns = true;
        }
        if (first & 0x04) p++; // volume
        if (first & 0x08) p++; // effect type
        if (first & 0x10) p++; // effect param
      } else {
        note = first;
        hasNote = true;
        ins = b[p++];
        hasIns = true;
        p += 3; // volume, effect type, effect param
      }
      if (hasIns && ins > 0) lastIns[c] = ins - 1;
      if (hasNote && note > 0 && note < 97) {
        const ii = hasIns && ins > 0 ? ins - 1 : lastIns[c];
        if (ii >= 0 && ii < instSamples.length) for (const o of instSamples[ii]) used.add(o);
      }
    }
  }
  return seg0Only([...used]);
}
