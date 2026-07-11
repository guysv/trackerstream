// Bake worker: runs the CPU-heavy half of ingest (libopenmpt decode -> buildDagV4 FLAC-encode +
// CDC chunk) off the main thread, so N of them saturate the cores while the main thread keeps the
// shared-resource I/O (block/put to the one tsnode datastore + the one SQLite catalog) serialized.
// That's the split the profile pointed at: decode(20%)+encode(31%)+cdc(4%) parallelize cleanly;
// block/put(31%) does NOT (it's the shared datastore — parallelizing it is what sank multi-process
// sharding). Gated by BAKE_WORKERS=N in the ingest.
import { parentPort } from "node:worker_threads";
import { buildDag, buildDagV3, buildDagV4, buildFlatDag, detectFormat, initFlac } from "@trackerstream/repack";
import { initMeta, extractModule } from "./meta.ts";

await initMeta();
await initFlac(); // pre-warm libflacjs so the first job isn't skewed by lazy load

const bakeV4 = process.env.BAKE_V4 === "1";

parentPort!.on("message", async (msg: { id: number; bytes: Uint8Array; name: string }) => {
  const { id, name } = msg;
  const data = msg.bytes instanceof Uint8Array ? msg.bytes : new Uint8Array(msg.bytes); // structured-cloned input
  const ext = name.split(".").pop()?.toLowerCase() ?? "";
  try {
    const mod = extractModule(data);
    const fmt = detectFormat(data);
    let dag: { root: { toString(): string }; blocks: { cid: { toString(): string }; bytes: Uint8Array }[] } | undefined;
    let isFlat = false;
    if (mod && fmt && fmt !== "mo3") {
      try {
        dag = bakeV4 ? await buildDagV4(data, mod.decoded) : await buildDagV3(data, mod.decoded);
      } catch {
        /* compressed / unparseable slots -> v1 below */
      }
    }
    if (!dag) {
      try {
        dag = await buildDag(data);
      } catch {
        /* -> flat */
      }
    }
    if (!dag) {
      dag = await buildFlatDag(data, ext);
      isFlat = true;
    }
    // Send block bytes by structured clone (copy). Some blocks are views into WASM heap memory
    // (libopenmpt/libflacjs), which can be cloned but NOT transferred — and the copy is cheap
    // (~module size) relative to the decode+encode we just parallelized.
    const blocks = dag.blocks.map((b) => ({ cid: b.cid.toString(), bytes: b.bytes }));
    parentPort!.postMessage({ id, ok: true, root: dag.root.toString(), isFlat, blocks });
  } catch (e) {
    parentPort!.postMessage({ id, ok: false, error: String(e) });
  }
});
