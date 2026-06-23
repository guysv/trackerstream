// Driven by swarm.sh with two live kubo daemons (TS_A = master, TS_B = client).
// Builds a module's DAG, loads+pins it on A, connects B->A, then fetches the
// whole DAG from B over Bitswap, verifying + reassembling exact bytes.
import { readFileSync } from "node:fs";
import { buildDag, reassemble, allCids, prefetch } from "../src/dag.ts";
import type { CID } from "multiformats/cid";
import { KuboRpc, loadDagToKubo } from "../src/kubo.ts";

const A = new KuboRpc(process.env.TS_A!);
const B = new KuboRpc(process.env.TS_B!);
const modulePath = process.argv[2];

const orig = new Uint8Array(readFileSync(modulePath));
const name = modulePath.split("/").pop();

const dag = await buildDag(orig);
console.log(
  `module: ${name}  ${(orig.length / 1024).toFixed(0)}KB  ${dag.blocks.length} blocks  ` +
    `root=${dag.root.toString().slice(0, 16)}…`,
);

// Load + pin on A (master).
const t0 = performance.now();
const { mismatched } = await loadDagToKubo(A, dag.blocks, dag.root);
if (mismatched.length) {
  console.log(`FAIL CID mismatch on block put:\n  ${mismatched.slice(0, 3).join("\n  ")}`);
  process.exit(1);
}
console.log(`A: put+pinned ${dag.blocks.length} blocks in ${(performance.now() - t0).toFixed(0)}ms`);

// Connect B -> A.
const aId = await A.id();
const aAddr = aId.Addresses.find((a) => a.includes("127.0.0.1") && a.includes("/tcp/"));
if (!aAddr) {
  console.log(`FAIL no dialable A address: ${JSON.stringify(aId.Addresses)}`);
  process.exit(1);
}
await B.swarmConnect(aAddr);
const peers = await B.swarmPeers();
console.log(`B: connected to ${peers.length} peer(s)`);

// Cold fetch on B (no local blocks) -> Bitswap pulls every block from A.
// Concurrent prefetch (the Phase 3 fetch-plan shape) then verify+reassemble
// from the local cache; far better than 311 serial round-trips.
const tc0 = performance.now();
const cids = await allCids(dag.root, B.getter());
const cache = await prefetch(cids, B.getter(), 32);
const cacheGetter = async (cid: CID) => cache.get(cid.toString())!;
const cold = await reassemble(dag.root, cacheGetter, { verify: true });
const coldMs = performance.now() - tc0;
const coldOk = cold.bytes.length === orig.length && Buffer.compare(cold.bytes, orig) === 0;

// Warm fetch on B (blocks now in B's blockstore) via concurrent prefetch.
const tw0 = performance.now();
const warmCache = await prefetch(cids, B.getter(), 32);
const warm = await reassemble(dag.root, async (c: CID) => warmCache.get(c.toString())!, {
  verify: true,
});
const warmMs = performance.now() - tw0;
const warmOk = warm.bytes.length === orig.length && Buffer.compare(warm.bytes, orig) === 0;

console.log(
  `\n${coldOk ? "OK  " : "FAIL"} cold  (B<-A Bitswap, ${cids.length} blocks, concurrent, verified): ${coldMs.toFixed(0)}ms  bytes=${coldOk ? "exact" : "DIFFER"}`,
);
console.log(
  `${warmOk ? "OK  " : "FAIL"} warm  (B local cache):                                  ${warmMs.toFixed(0)}ms  bytes=${warmOk ? "exact" : "DIFFER"}`,
);
console.log(
  `\n100% of module bytes sourced from CID blocks over libp2p; every block CID-verified.`,
);
process.exit(coldOk && warmOk ? 0 : 1);
