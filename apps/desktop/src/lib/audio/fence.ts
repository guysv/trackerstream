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

  private unionFrom(i: number, span: number): number[] {
    const set = new Set<number>();
    const end = Math.min(i + span, this.cps.length - 1);
    for (let k = i; k <= end; k++) for (const s of this.cps[k].samples) set.add(s);
    return [...set];
  }

  /** Sample slots that must be resident to play at `order` (the floor checkpoint). */
  requiredAt(order: number): number[] {
    const i = this.floorIdx(order);
    return i < 0 ? [] : this.cps[i].samples;
  }

  /**
   * May playback proceed at `order`? Mutates internal stall state so the caller
   * can detect transitions (buffering on/off). Before the first checkpoint, or
   * with no checkpoints, returns true (nothing to gate — those samples ride in the
   * skeleton / there is nothing to wait for).
   */
  ready(order: number): boolean {
    const i = this.floorIdx(order);
    if (i < 0) return true;
    if (!this.stalled) {
      if (this.allProvided(this.cps[i].samples)) return true;
      this.stalled = true;
      return false;
    }
    // Stalled: resume only when this checkpoint AND the lookahead margin are in.
    if (this.allProvided(this.unionFrom(i, this.lookahead))) {
      this.stalled = false;
      return true;
    }
    return false;
  }
}
