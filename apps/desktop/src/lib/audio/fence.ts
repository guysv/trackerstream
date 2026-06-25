// The client fence (STREAMING-PARITY.md §2.2). Decides whether playback may
// proceed at a given order, given which samples have been provided into the
// immortal instance so far. Parity-correct because the checkpoint table is
// conservative + the simulation is deterministic: checkpoint(N) lists every
// sample audible from order N until the next checkpoint, so requiring it resident
// before entering N guarantees no not-yet-provided sample ever sounds.
//
// Pure + framework-free so it unit-tests in Node and runs unchanged on the audio
// thread inside player.worklet.ts.

import type { PlanData } from "./messages";

export class Fence {
  private provided = new Set<number>();
  private cps: { order: number; samples: number[] }[];
  private stalled = false;
  private lookahead: number;

  /** @param lookahead extra checkpoints whose samples must also be resident to
   *  RESUME after a stall (rebuffer hysteresis — avoids play/stall oscillation on
   *  marginal links). Does not affect the initial stall decision. */
  constructor(plan: PlanData, lookahead = 1) {
    this.cps = [...(plan.checkpoints ?? [])].sort((a, b) => a.order - b.order);
    this.lookahead = lookahead;
  }

  /** Mark a sample slot as resident (called when provideSample is applied). */
  provide(index: number): void {
    this.provided.add(index);
  }

  isProvided(index: number): boolean {
    return this.provided.has(index);
  }

  get buffering(): boolean {
    return this.stalled;
  }

  /** Greatest checkpoint index with order <= `order` (floor), or -1. */
  private floorIdx(order: number): number {
    let lo = 0,
      hi = this.cps.length - 1,
      res = -1;
    while (lo <= hi) {
      const m = (lo + hi) >> 1;
      if (this.cps[m].order <= order) {
        res = m;
        lo = m + 1;
      } else hi = m - 1;
    }
    return res;
  }

  private allProvided(samples: number[]): boolean {
    for (const s of samples) if (!this.provided.has(s)) return false;
    return true;
  }

  // Union of checkpoint sample-sets over indices [start, end] (clamped to valid
  // range). start may be -1 (before the first checkpoint) -> clamps to 0.
  private unionRange(start: number, end: number): number[] {
    const set = new Set<number>();
    const lo = Math.max(0, start);
    const hi = Math.min(end, this.cps.length - 1);
    for (let k = lo; k <= hi; k++) for (const s of this.cps[k].samples) set.add(s);
    return [...set];
  }

  /**
   * Sample slots that must be resident to play at `order`. A render quantum can
   * cross ONE checkpoint boundary mid-buffer (e.g. a Cxx/Bxx pattern jump out of a
   * silent setup pattern), so this is the floor checkpoint UNION the next one —
   * which also covers the pre-first-checkpoint case (floor = -1 -> the first cp).
   */
  requiredAt(order: number): number[] {
    const i = this.floorIdx(order);
    return this.unionRange(i, i + 1);
  }

  /**
   * May playback proceed at `order`? Mutates internal stall state so the caller
   * can detect transitions (buffering on/off). With no checkpoints (or none in
   * range that need samples) returns true — nothing to gate.
   */
  ready(order: number): boolean {
    const i = this.floorIdx(order);
    const base = this.unionRange(i, i + 1); // floor + next (boundary crossing)
    if (base.length === 0) return true;
    if (!this.stalled) {
      if (this.allProvided(base)) return true;
      this.stalled = true;
      return false;
    }
    // Stalled: resume only when floor+next AND the lookahead margin are resident.
    if (this.allProvided(this.unionRange(i, i + 1 + this.lookahead))) {
      this.stalled = false;
      return true;
    }
    return false;
  }
}
