// v3 byte-exact reassembly oracle: buildDagV3 -> reassembleV3 from an in-memory blockstore ->
// assert md5(reassembled) === md5(original). Also asserts v3's STREAMING blocks (skeleton +
// per-sample decoded PCM chunk CIDs) are IDENTICAL to buildDagV2's for the same module — proof
// that adding reassembly (offset+enc, originalLength) did not perturb the streaming path.
//
// Modules with compressed samples throw from buildDagV3 (route to v1); those reassemble via the
// v1 buildDag/reassemble path, also asserted here. Needs the wasm dist (decoded-PCM dump); SKIPs
// cleanly when absent, like provide-v2.ts. Default corpus: ~/tmp/somemods.
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { homedir } from "node:os";
import { buildDag, buildDagV2, buildDagV3, reassemble, reassembleV3, type DecodedSample } from "../src/dag.ts";
import type { CID } from "multiformats/cid";

let M: any;
try {
  M = await (await import("../../wasm/dist/libopenmpt.js")).default({ print() {}, printErr() {} });
} catch (e) {
  console.log(`SKIP v3-roundtrip  (wasm dist unavailable: ${(e as Error).message})`);
  process.exit(0);
}

const md5 = (b: Uint8Array) => createHash("md5").update(b).digest("hex");

function dumpSamples(bytes: Uint8Array): DecodedSample[] | null {
  const p = M._malloc(bytes.length);
  M.HEAPU8.set(bytes, p);
  const mod = M._openmpt_module_create_from_memory(p, bytes.length, 0, 0, 0);
  M._free(p);
  if (!mod) return null;
  const n = M._openmpt_module_get_num_samples(mod);
  const out: DecodedSample[] = [];
  for (let i = 1; i <= n; i++) {
    const frames = M._openmpt_module_debug_sample_frames(mod, i);
    const bn = M._openmpt_module_debug_sample_bytes(mod, i);
    const ptr = M._openmpt_module_debug_sample_data(mod, i);
    if (frames > 0 && bn > 0 && ptr) out.push({ index: i, frames, data: M.HEAPU8.slice(ptr, ptr + bn) });
  }
  M._openmpt_module_destroy(mod);
  return out;
}

const memStore = (blocks: { cid: CID; bytes: Uint8Array }[]) => {
  const m = new Map(blocks.map((b) => [b.cid.toString(), b.bytes]));
  return async (cid: CID) => {
    const v = m.get(cid.toString());
    if (!v) throw new Error(`missing block ${cid}`);
    return v;
  };
};

const DIR = process.argv[2] ?? join(homedir(), "tmp", "somemods");
if (!existsSync(DIR)) {
  console.log(`SKIP v3-roundtrip  (corpus dir ${DIR} absent)`);
  process.exit(0);
}

let v3ok = 0,
  v1ok = 0,
  fail = 0,
  streamMismatch = 0;
for (const name of readdirSync(DIR).sort()) {
  if (name.startsWith(".")) continue;
  const data = new Uint8Array(readFileSync(join(DIR, name)));
  const orig = md5(data);
  const dumps = dumpSamples(data);

  let v3;
  try {
    if (!dumps) throw new Error("no wasm load");
    v3 = await buildDagV3(data, dumps);
  } catch {
    // Route to v1 (compressed sample / unparseable) — byte-exact via buildDag/reassemble.
    const dag = await buildDag(data).catch(() => null);
    if (!dag) {
      console.log(`FAIL ${name} (neither v3 nor v1 built)`);
      fail++;
      continue;
    }
    const { bytes } = await reassemble(dag.root, memStore(dag.blocks), { verify: true });
    const ok = md5(bytes) === orig;
    console.log(`${ok ? "v1 OK" : "v1 FAIL"} ${name.padEnd(26)} (routed to v1)`);
    ok ? v1ok++ : fail++;
    continue;
  }

  // Byte-exact reassembly from v3 blocks.
  const { bytes } = await reassembleV3(v3.root, memStore(v3.blocks), { verify: true });
  const parity = md5(bytes) === orig && bytes.length === data.length;

  // Streaming parity: v3's skeleton + per-sample PCM leaf CIDs must equal v2's exactly.
  const v2 = await buildDagV2(data, dumps);
  const sameSkel =
    v3.manifest.skeletonChunks.length === v2.manifest.skeletonChunks.length &&
    v3.manifest.skeletonChunks.every((c, i) => c.toString() === v2.manifest.skeletonChunks[i].toString());
  const v2samples = v2.manifest.index?.samples ?? [];
  const v3samples = v3.manifest.index?.samples ?? [];
  const sameSamples =
    v2samples.length === v3samples.length &&
    v3samples.every((s, i) => {
      const t = v2samples[i];
      return t && t.index === s.index && t.chunks.length === s.chunks.length && t.chunks.every((c, k) => c.toString() === s.chunks[k].toString());
    });
  const streamOk = sameSkel && sameSamples;
  if (!streamOk) streamMismatch++;

  console.log(
    `${parity ? "v3 OK" : "v3 FAIL"} ${name.padEnd(26)} stream:${streamOk ? "==v2" : "DIFF"} ` +
      `streamed=${v3.stats.streamedSamples} resident=${v3.stats.residentSamples} md5=${orig.slice(0, 8)}`,
  );
  parity && streamOk ? v3ok++ : fail++;
}

console.log(`\nv3: ${v3ok} OK  |  v1-routed: ${v1ok} OK  |  FAIL: ${fail}  |  stream!=v2: ${streamMismatch}`);
process.exit(fail || streamMismatch ? 1 : 0);
