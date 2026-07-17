// Warm-bundle generator (run on the box that holds the catalog in kubo — after a bake, or ad hoc).
//
// Produces the two files the web client's warm-bundle loader (apps/web/src/lib/warmbundle.ts) fetches
// over HTTP at boot to pre-load the catalog's hot FTS pages, so a fresh browser profile's first search
// skips ~26 serialized cold Bitswap round-trips. It runs the SAME wa-sqlite engine + VFS the browser
// runs (packages/catalog-web), so the pages it captures are byte-for-byte the ones the client fetches.
//
// Mechanism: open the engine against the published catalog root (manifest whole-read + per-page
// block/get, both via the local kubo RPC), then execute a canonical query workload — each query run
// COLD (clearPageCache between them) so we capture its FULL page set, not just the incremental pages a
// prior query left uncached. The union is the bundle: every workload term is then fully covered on a
// cold client, and the shared FTS-dictionary b-tree (hit by every query) covers arbitrary terms too.
//
// Output (to --out, default ./warm-bundle) — serve these from tsedge next to /bootstrap.json:
//   catalog-warm.json  { root, url:"/catalog-warm.bin", blocks, bytes, cids:[...] }
//   catalog-warm.bin   "TSWARM1\n" + u32 count + [ u32 len, page bytes ]...
//
// Usage: node apps/server/src/gen-warm-bundle.ts --root <catalogRootCID> [--kubo http://127.0.0.1:5001] [--out DIR]
import { mkdir, writeFile, readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { CID } from "multiformats/cid";
import { KuboRpc } from "@trackerstream/repack/kubo";
import { CatalogEngine } from "@trackerstream/catalog-web/engine.ts";
import { clearPageCache } from "@trackerstream/catalog-web/scheduler.ts";

// Common query terms whose posting lists + descent we want resident. Broad words maximise shared-page
// coverage; a handful of genre-ish terms broaden the leaf coverage a little. Order is irrelevant (each
// runs cold). Keep the set small — the bundle is one HTTP GET at boot.
const WORKLOAD = ["the", "a", "mix", "remix", "love", "dream", "space", "dance", "dark", "epic", "intro", "theme", "song", "final"];

/**
 * Generate the warm bundle for catalog `rootStr` by running the client engine against the pages the
 * local kubo/tsnode RPC serves, and write catalog-warm.{bin,json} into `outDir` (the Caddy web root).
 * Returns the page count. Callable from the bake (best-effort) or the CLI below.
 */
export async function generateWarmBundle(kuboUrl: string, rootStr: string, outDir: string): Promise<number> {
  const rpc = new KuboRpc(kuboUrl);
  const root = CID.parse(rootStr);
  const manBytes = await rpc.cat(root); // the TSZCAT manifest, whole

  // Capturing block source: every page the engine reads, keyed by CID (dedup across the workload).
  const captured = new Map<string, Uint8Array>();
  const getBlock = async (cid: CID): Promise<Uint8Array> => {
    const key = cid.toString();
    const hit = captured.get(key);
    if (hit) return hit;
    const bytes = await rpc.blockGet(cid);
    captured.set(key, bytes);
    return bytes;
  };

  const require = createRequire(import.meta.url);
  const wasmBinary = await readFile(require.resolve("@journeyapps/wa-sqlite/dist/wa-sqlite-async.wasm"));

  const engine = await CatalogEngine.open(rootStr, async () => manBytes, getBlock, { wasmBinary });

  for (const q of WORKLOAD) {
    clearPageCache(); // run each term truly cold so we capture its full page set
    await engine.searchStream({ q, limit: 50 }, () => {});
  }
  // The home page also reads the precomputed meta aggregates + browse index roots; a cheap browse
  // list warms those pages too.
  clearPageCache();
  await engine.query({ op: "formats" });
  await engine.query({ op: "genres" });
  await engine.query({ op: "list", sort: "latest", limit: 100, no_genre: false });

  const pages = [...captured.entries()]; // [cidString, bytes]
  if (pages.length === 0) throw new Error("captured no pages — wrong root, or the RPC can't serve the catalog");

  // Blob: "TSWARM1\n" + u32 count + [ u32 len, bytes ]...
  const magic = new TextEncoder().encode("TSWARM1\n");
  let total = magic.length + 4;
  for (const [, b] of pages) total += 4 + b.length;
  const bin = new Uint8Array(total);
  const dv = new DataView(bin.buffer);
  bin.set(magic, 0);
  let off = magic.length;
  dv.setUint32(off, pages.length, true);
  off += 4;
  for (const [, b] of pages) {
    dv.setUint32(off, b.length, true);
    off += 4;
    bin.set(b, off);
    off += b.length;
  }

  await mkdir(outDir, { recursive: true });
  await writeFile(`${outDir}/catalog-warm.bin`, bin);
  await writeFile(
    `${outDir}/catalog-warm.json`,
    JSON.stringify({ root: rootStr, url: "/catalog-warm.bin", blocks: pages.length, bytes: bin.length, cids: pages.map(([c]) => c) }),
  );
  return pages.length;
}

// CLI: node apps/server/src/gen-warm-bundle.ts --root <cid> [--kubo URL] [--out DIR]
function arg(name: string, def?: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && i + 1 < process.argv.length ? process.argv[i + 1] : def;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const rootStr = arg("root");
  if (!rootStr) {
    console.error("usage: gen-warm-bundle --root <catalogRootCID> [--kubo URL] [--out DIR]");
    process.exit(1);
  }
  const n = await generateWarmBundle(arg("kubo", "http://127.0.0.1:5001")!, rootStr, arg("out", "./warm-bundle")!);
  console.log(`wrote catalog-warm.{bin,json}: ${n} pages, root ${rootStr}`);
}
