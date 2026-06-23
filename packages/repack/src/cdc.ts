// FastCDC content-defined chunking (normalized, 32-bit gear) — ported from
// lab/cid-dedup.mjs. The chunk is simultaneously the dedup unit, the partial-
// fetch granule, and the seek resident-set unit (lab/CID.md), so one scheme
// serves all three.
//
// Size target (MVP.md Phase 1): the dedup labs measured at 512/2048/8192, but
// for the P2P TRANSPORT block we want a low-tens-of-KB target to balance dedup
// against Bitswap round-trips/wantlist overhead. Default below; the final value
// is locked in Phase 1 against the live swarm measurement.

export interface CdcConfig {
  min: number;
  avg: number;
  max: number;
}

// Locked default for Phase 1 transport blocks (revisit with swarm numbers).
export const DEFAULT_CDC: CdcConfig = { min: 8 * 1024, avg: 16 * 1024, max: 64 * 1024 };

const GEAR = (() => {
  const g = new Uint32Array(256);
  let s = 0x9e3779b9 >>> 0;
  for (let i = 0; i < 256; i++) {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    g[i] = s;
  }
  return g;
})();

/** Yield content-defined chunks of `buf` (subarrays — no copies). */
export function* cdcChunks(buf: Uint8Array, cfg: CdcConfig = DEFAULT_CDC): Generator<Uint8Array> {
  const { min, avg, max } = cfg;
  const bits = Math.log2(avg) | 0;
  const maskS = ((1 << (bits + 2)) - 1) >>> 0; // stricter before avg
  const maskL = ((1 << (bits - 2)) - 1) >>> 0; // looser after avg
  const n = buf.length;
  let off = 0;
  while (off < n) {
    const end = Math.min(off + max, n);
    const normal = Math.min(off + avg, n);
    let fp = 0;
    let cut = -1;
    let i = off + min;
    if (i < off) i = off;
    for (; i < end; i++) {
      fp = ((fp << 1) + GEAR[buf[i]]) >>> 0;
      const mask = i < normal ? maskS : maskL;
      if ((fp & mask) === 0) {
        cut = i + 1;
        break;
      }
    }
    const stop = cut === -1 ? end : cut;
    yield buf.subarray(off, stop);
    off = stop;
  }
}
