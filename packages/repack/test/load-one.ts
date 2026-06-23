// Build a module's DAG and load+pin it into a running kubo node, printing the
// root CID as `ROOT=<cid>`. Used by test/rust-interop.sh.
//   node test/load-one.ts <module-file> <kubo-api-url>
import { readFileSync } from "node:fs";
import { buildDag } from "../src/dag.ts";
import { KuboRpc, loadDagToKubo } from "../src/kubo.ts";

const [modulePath, api] = process.argv.slice(2);
const rpc = new KuboRpc(api);
const dag = await buildDag(new Uint8Array(readFileSync(modulePath)));
const { mismatched } = await loadDagToKubo(rpc, dag.blocks, dag.root);
if (mismatched.length) {
  console.error(`CID mismatch: ${mismatched[0]}`);
  process.exit(1);
}
console.error(`loaded ${dag.blocks.length} blocks`);
console.log(`ROOT=${dag.root.toString()}`);
