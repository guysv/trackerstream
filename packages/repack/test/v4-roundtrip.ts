// v4 oracle: buildDagV4 -> reassembleV4 from an in-memory blockstore -> md5(reassembled) ==
// md5(original). Also asserts:
//   * DETERMINISM: two independent bakes produce the IDENTICAL root CID (required for cross-module
//     dedup + REBUILD idempotency).
//   * STREAMING PARITY vs v3: the skeleton + sample metadata are byte-identical to v3's, and each
//     v4 sample's leaves DECODE (per encCodec) to exactly the native PCM v3 chunks raw — playback
//     bit-exact. (NOT compared to v2: v3/v4 stream only enc-taggable samples, v2 streams any
//     locatable one, so v2's skeleton legitimately differs when a sample has no enc tag.)
//   * v1/flat ROUTING: compressed/unparseable modules throw from buildDagV4 and reassemble via v1.
// Needs the wasm dist (decoded-PCM dump) and the `flac` CLI; SKIPs cleanly when either is absent.
// Default corpus: ~/tmp/somemods.
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { homedir } from "node:os";
import { buildDag, buildFlatDag, buildDagV3, buildDagV4, reassemble, reassembleV4, decodeV4Sample, type DecodedSample } from "../src/dag.ts";
import { FLAC_CODEC } from "../src/flac.ts";
import type { CID } from "multiformats/cid";

let M: any;
try {
  M = await (await import("../../wasm/dist/libopenmpt.js")).default({ print() {}, printErr() {} });
} catch (e) {
  console.log(`SKIP v4-roundtrip  (wasm dist unavailable: ${(e as Error).message})`);
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
const eq = (a: Uint8Array, b: Uint8Array) => a.length === b.length && a.every((x, i) => x === b[i]);

const DIR = process.argv[2] ?? join(homedir(), "tmp", "somemods");
if (!existsSync(DIR)) {
  console.log(`SKIP v4-roundtrip  (corpus dir ${DIR} absent)`);
  process.exit(0);
}

let v4ok = 0, v1ok = 0, fail = 0, detBad = 0, streamBad = 0;
let rawLeafBytes = 0, flacLeafBytes = 0, nFlac = 0, nRaw = 0;
for (const name of readdirSync(DIR).sort()) {
  if (name.startsWith(".")) continue;
  const data = new Uint8Array(readFileSync(join(DIR, name)));
  const orig = md5(data);
  const dumps = dumpSamples(data);

  let v4;
  try {
    if (!dumps) throw new Error("no wasm load");
    v4 = await buildDagV4(data, dumps);
  } catch {
    // Mirror ingest routing: compressed/unparseable -> v1 buildDag, else flat DAG (ext as format).
    const ext = name.split(".").pop()?.toLowerCase() ?? "";
    const dag = (await buildDag(data).catch(() => null)) ?? (await buildFlatDag(data, ext).catch(() => null));
    if (!dag) { console.log(`FAIL ${name} (neither v4 nor v1/flat built)`); fail++; continue; }
    const { bytes } = await reassemble(dag.root, memStore(dag.blocks), { verify: true });
    const ok = md5(bytes) === orig;
    console.log(`${ok ? "v1 OK" : "v1 FAIL"} ${name.padEnd(26)} (routed to v1/flat)`);
    ok ? v1ok++ : fail++;
    continue;
  }

  // 1. byte-exact reassembly
  const { bytes } = await reassembleV4(v4.root, memStore(v4.blocks), { verify: true });
  const parity = md5(bytes) === orig && bytes.length === data.length;

  // 2. determinism: a second independent bake yields the identical root
  const v4b = await buildDagV4(data, dumps!);
  const deterministic = v4b.root.toString() === v4.root.toString();
  if (!deterministic) detBad++;

  // 3. streaming parity vs v3: v4 == v3 except leaves are FLAC(decoded). So the skeleton + sample
  //    metadata (index/offset/enc) must be IDENTICAL to v3's, and each sample's leaves must DECODE
  //    to exactly the native PCM (dump.data) that v3 chunks raw -> playback is bit-exact.
  const v3 = await buildDagV3(data, dumps!);
  const s3 = v3.manifest.index!.samples, s4 = v4.manifest.index!.samples;
  const sameSkel = v4.manifest.skeletonChunks.length === v3.manifest.skeletonChunks.length &&
    v4.manifest.skeletonChunks.every((c, i) => c.toString() === v3.manifest.skeletonChunks[i].toString()) &&
    s4.length === s3.length &&
    s4.every((s, i) => s.index === s3[i].index && s.offset === s3[i].offset && s.enc === s3[i].enc);
  const dumpByIdx = new Map(dumps!.map((d) => [d.index, d]));
  const store = memStore(v4.blocks);
  let decodeOk = true;
  for (const s of v4.manifest.index!.samples) {
    const leaves: Uint8Array[] = [];
    let n = 0;
    for (const cid of s.chunks) { const c = await store(cid); leaves.push(c); n += c.length; }
    const buf = new Uint8Array(n); let w = 0; for (const c of leaves) { buf.set(c, w); w += c.length; }
    const pcm = decodeV4Sample(buf, s);
    if (!eq(pcm, dumpByIdx.get(s.index)!.data)) decodeOk = false;
    if (s.encCodec === FLAC_CODEC) { nFlac++; flacLeafBytes += n; } else { nRaw++; rawLeafBytes += n; }
  }
  const streamOk = sameSkel && decodeOk;
  if (!streamOk) streamBad++;

  console.log(
    `${parity ? "v4 OK" : "v4 FAIL"} ${name.padEnd(26)} det:${deterministic ? "yes" : "NO"} ` +
      `stream:${streamOk ? "==v3" : "DIFF"} streamed=${v4.stats.streamedSamples} flac=${v4.manifest.index!.samples.filter((s) => s.encCodec === FLAC_CODEC).length}`,
  );
  parity && deterministic && streamOk ? v4ok++ : fail++;
}

const MB = (n: number) => (n / 1048576).toFixed(2);
console.log(`\nv4: ${v4ok} OK  |  v1-routed: ${v1ok} OK  |  FAIL: ${fail}  |  nondeterministic: ${detBad}  |  stream!=v3: ${streamBad}`);
console.log(`leaves: ${nFlac} flac (${MB(flacLeafBytes)} MB) + ${nRaw} raw (${MB(rawLeafBytes)} MB)`);
process.exit(fail || detBad || streamBad ? 1 : 0);
