// Unit test for the client fence (src/lib/audio/fence.ts) — pure, no wasm/WebView.
// Validates floor-lookup over strided checkpoints, the provide/ready gating, and
// rebuffer hysteresis (once stalled, require a lookahead margin to resume).
import { Fence } from "../src/lib/audio/fence.ts";

let fails = 0;
const ok = (cond, msg) => {
  if (!cond) {
    console.log(`FAIL ${msg}`);
    fails++;
  } else console.log(`OK   ${msg}`);
};

// checkpoints at orders 0,4,8 (strided); each needs distinct samples.
const plan = {
  orderSeconds: [],
  checkpoints: [
    { order: 0, samples: [1, 2] },
    { order: 4, samples: [2, 3] },
    { order: 8, samples: [4] },
  ],
};

// Floor-lookup: order 2 -> checkpoint(0); order 7 -> checkpoint(4).
{
  const f = new Fence(plan, 0);
  ok(JSON.stringify(f.requiredAt(2)) === JSON.stringify([1, 2]), "floor: order 2 -> cp(0)");
  ok(JSON.stringify(f.requiredAt(7)) === JSON.stringify([2, 3]), "floor: order 7 -> cp(4)");
  ok(JSON.stringify(f.requiredAt(100)) === JSON.stringify([4]), "floor: order 100 -> cp(8)");
}

// Gating: not ready until the floor checkpoint's samples are all provided.
{
  const f = new Fence(plan, 0);
  ok(f.ready(0) === false, "order 0 stalls with nothing provided");
  f.provide(1);
  ok(f.ready(0) === false, "order 0 still stalls with only sample 1");
  f.provide(2);
  ok(f.ready(0) === true, "order 0 ready once 1+2 provided");
}

// Before the first checkpoint / no checkpoints -> never gates.
{
  const f = new Fence({ orderSeconds: [], checkpoints: [] }, 0);
  ok(f.ready(0) === true && f.ready(50) === true, "empty plan never gates");
}

// Hysteresis: with lookahead=1, resuming from a stall at order 4 needs cp(4) AND
// cp(8) resident — not just cp(4).
{
  const f = new Fence(plan, 1);
  f.provide(2);
  f.provide(3); // cp(4) satisfied
  ok(f.ready(4) === true, "order 4 ready (not yet stalled, only cp(4) needed)");
  // force a stall: order 8 needs sample 4 (not provided)
  ok(f.ready(8) === false, "order 8 stalls (sample 4 missing)");
  f.provide(4); // cp(8) satisfied, but hysteresis wants cp(8) lookahead too (== cp(8), the last)
  ok(f.ready(8) === true, "order 8 resumes once sample 4 in (last cp, no further lookahead)");
}

// Hysteresis margin actually delays resume mid-table.
{
  const f = new Fence(plan, 1);
  // satisfy cp(0) so order 0 plays, then stall at order 4 missing sample 3.
  f.provide(1);
  f.provide(2);
  ok(f.ready(0) === true, "hysteresis: order 0 plays");
  ok(f.ready(4) === false, "hysteresis: order 4 stalls (sample 3 missing)");
  f.provide(3); // cp(4) now satisfied, but lookahead also wants cp(8)=sample 4
  ok(f.ready(4) === false, "hysteresis: still stalled — lookahead wants cp(8) too");
  f.provide(4);
  ok(f.ready(4) === true, "hysteresis: resumes once cp(4)+cp(8) margin resident");
}

if (fails) {
  console.log(`\n${fails} fence assertion(s) failed`);
  process.exit(1);
}
console.log("\nall fence assertions passed");
