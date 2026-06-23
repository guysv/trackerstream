// In-memory DAG roundtrip: build a module's CID-DAG, reassemble it from an
// in-memory block store with CID verification, and assert the result is
// byte-identical to the original (so playback is bit-identical). Also exercises
// cross-module chunk dedup by building two modules into a shared store.
import { readFileSync, existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import type { CID } from "multiformats/cid";
import { buildDag, reassemble } from "../src/dag.ts";

const DIR = join(homedir(), "tmp/somemods");
const DEFAULT = [
  "space_debris (1).mod",
  "ELYSIUM.MOD",
  "celestial_fantasia.s3m",
  "2nd_pm.s3m",
  "a-windf.it",
  "beyond_the_network.it",
  "elw-sick.xm",
];

const paths = process.argv.slice(2);
if (paths.length === 0) for (const f of DEFAULT) paths.push(join(DIR, f));

const store = new Map<string, Uint8Array>(); // shared block store (cross-module dedup)
const get = async (cid: CID): Promise<Uint8Array> => {
  const b = store.get(cid.toString());
  if (!b) throw new Error(`missing block ${cid}`);
  return b;
};

let allOk = true;
let totalChunkBytes = 0;
for (const path of paths) {
  if (!existsSync(path)) {
    console.log(`SKIP ${path} (not found)`);
    continue;
  }
  const orig = new Uint8Array(readFileSync(path));
  let dag;
  try {
    dag = await buildDag(orig);
  } catch (e) {
    console.log(`FAIL ${path.split("/").pop()}  build: ${e}`);
    allOk = false;
    continue;
  }
  for (const blk of dag.blocks) store.set(blk.cid.toString(), blk.bytes);

  const { bytes } = await reassemble(dag.root, get, { verify: true });
  const equal = bytes.length === orig.length && Buffer.compare(bytes, orig) === 0;
  allOk &&= equal;

  const s = dag.stats;
  const manifestPct = ((s.skeletonBytes / s.originalLength) * 100).toFixed(1);
  totalChunkBytes += s.sampleBytes;
  console.log(
    `${equal ? "OK  " : "FAIL"} ${path.split("/").pop()}  ` +
      `${(s.originalLength / 1024).toFixed(0)}KB  samples=${s.numSamples}  chunks=${s.numChunks}  ` +
      `skeleton=${manifestPct}%  manifest=${s.manifestBytes}B  root=${dag.root.toString().slice(0, 12)}…`,
  );
}

// Tampering must be caught by CID verification.
{
  const orig = new Uint8Array(readFileSync(join(DIR, "ELYSIUM.MOD")));
  const dag = await buildDag(orig);
  const tampered = new Map(store);
  // Flip a byte in the largest chunk block.
  let victim: string | null = null;
  let max = -1;
  for (const blk of dag.blocks) {
    const k = blk.cid.toString();
    if (blk.bytes.length > max) {
      max = blk.bytes.length;
      victim = k;
    }
  }
  const bad = Uint8Array.from(tampered.get(victim!)!);
  bad[0] ^= 0xff;
  tampered.set(victim!, bad);
  let caught = false;
  try {
    await reassemble(dag.root, async (cid) => tampered.get(cid.toString())!, { verify: true });
  } catch {
    caught = true;
  }
  console.log(`${caught ? "OK  " : "FAIL"} tamper detection: corrupted block ${caught ? "rejected" : "ACCEPTED (!)"}`);
  allOk &&= caught;
}

console.log(`\nstore: ${store.size} unique blocks, ${(totalChunkBytes / 1e6).toFixed(1)}MB sample PCM`);
process.exit(allOk ? 0 : 1);
