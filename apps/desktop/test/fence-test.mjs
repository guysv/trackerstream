// Unit test for the client fence (src/lib/audio/fence.ts) — pure, no wasm/WebView.
// Validates floor∪next lookup over strided checkpoints (a render quantum can cross
// one checkpoint boundary mid-buffer, incl. a pattern jump out of a silent setup
// pattern, so the next checkpoint's samples must also be resident — the parity bug
// the integrated stream-sim caught), the provide/ready gating, and rebuffer
// hysteresis (resume needs an extra lookahead margin).
import { Fence } from "../src/lib/audio/fence.ts";

let fails = 0;
const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const ok = (cond, msg) => {
  if (!cond) {
    console.log(`FAIL ${msg}`);
    fails++;
  } else console.log(`OK   ${msg}`);
};

// checkpoints at orders 0,2,4,6 — one distinct sample each.
const plan = {
  orderSeconds: [],
  checkpoints: [
    { order: 0, samples: [1] },
    { order: 2, samples: [2] },
    { order: 4, samples: [3] },
    { order: 6, samples: [4] },
  ],
};

// requiredAt = floor checkpoint UNION the next (boundary crossing).
{
  const f = new Fence(plan, 0);
  ok(eq(f.requiredAt(0), [1, 2]), "requiredAt(0) = cp0 ∪ cp2 = [1,2]");
  ok(eq(f.requiredAt(3), [2, 3]), "requiredAt(3) = cp2 ∪ cp4 = [2,3]");
  ok(eq(f.requiredAt(5), [3, 4]), "requiredAt(5) = cp4 ∪ cp6 = [3,4]");
  ok(eq(f.requiredAt(100), [4]), "requiredAt(100) = cp6 (last) = [4]");
}

// Gating: order 0 needs BOTH its own and the next checkpoint's samples resident.
{
  const f = new Fence(plan, 0);
  ok(f.ready(0) === false, "order 0 stalls with nothing provided");
  f.provide(1);
  ok(f.ready(0) === false, "order 0 still stalls (next cp's sample 2 missing)");
  f.provide(2);
  ok(f.ready(0) === true, "order 0 ready once 1+2 (floor ∪ next) provided");
}

// floor = -1 (before the first checkpoint, e.g. a silent setup pattern that Cxx-
// jumps into the first real order): must still require the FIRST checkpoint. This
// is the 2nd_pm.s3m bug the stream simulation surfaced.
{
  const late = { orderSeconds: [], checkpoints: [{ order: 5, samples: [9] }] };
  const f = new Fence(late, 0);
  ok(eq(f.requiredAt(0), [9]), "pre-first-cp: requiredAt(0) pulls in the first cp");
  ok(f.ready(0) === false, "order 0 stalls before the first checkpoint's sample is in");
  f.provide(9);
  ok(f.ready(0) === true, "order 0 ready once the first checkpoint's sample is in");
}

// No checkpoints -> never gates.
{
  const f = new Fence({ orderSeconds: [], checkpoints: [] }, 0);
  ok(f.ready(0) === true && f.ready(50) === true, "empty plan never gates");
}

// Hysteresis: once stalled, resume requires an extra lookahead checkpoint beyond
// floor∪next.
{
  const f = new Fence(plan, 1);
  f.provide(1);
  f.provide(2); // floor∪next for order 0 satisfied
  ok(f.ready(0) === true, "hysteresis: order 0 plays (1+2 in)");
  // stall at order 2: needs cp2∪cp4 = [2,3]; 3 missing.
  ok(f.ready(2) === false, "hysteresis: order 2 stalls (sample 3 missing)");
  f.provide(3); // floor∪next now satisfied, but hysteresis also wants cp6 (=4)
  ok(f.ready(2) === false, "hysteresis: still stalled — margin wants the next cp too");
  f.provide(4);
  ok(f.ready(2) === true, "hysteresis: resumes once the lookahead margin is resident");
}

if (fails) {
  console.log(`\n${fails} fence assertion(s) failed`);
  process.exit(1);
}
console.log("\nall fence assertions passed");
