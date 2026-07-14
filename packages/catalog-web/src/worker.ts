// The catalog worker.
//
// SQLite runs here, not on the main thread, for a hard reason: the wa-sqlite async build uses
// Asyncify, which BLOCKS its thread while a VFS read is in flight. On the main thread that would
// freeze the UI for the entire duration of a query — which, over a network-backed VFS, is seconds.
//
// libp2p/Helia stay on the main thread (they own the connections), so block fetches RPC back out
// over postMessage. That adds a structured-clone hop per block, which is noise next to a network
// round-trip, and it keeps the 16-way fetch concurrency intact.
import { CID } from "multiformats/cid";
import { CatalogEngine } from "./engine.ts";
import type { WorkerIn, WorkerOut } from "./protocol.ts";

let engine: CatalogEngine | null = null;
let nextFetchId = 0;
const pending = new Map<number, { resolve: (b: Uint8Array) => void; reject: (e: Error) => void }>();

const post = (m: WorkerOut, transfer: Transferable[] = []): void =>
  (self as unknown as Worker).postMessage(m, transfer);

/** Ask the main thread (which owns Helia) for bytes. */
function remoteFetch(kind: "block" | "root", cid: CID): Promise<Uint8Array> {
  const id = nextFetchId++;
  return new Promise<Uint8Array>((resolve, reject) => {
    pending.set(id, { resolve, reject });
    post({ t: "fetch", id, kind, cid: cid.toString() });
  });
}

self.onmessage = async (e: MessageEvent<WorkerIn>) => {
  const m = e.data;
  try {
    switch (m.t) {
      case "open":
        engine = await CatalogEngine.open(
          m.rootCid,
          (cid) => remoteFetch("root", cid),
          (cid) => remoteFetch("block", cid),
        );
        post({ t: "opened" });
        return;

      case "fetched": {
        const p = pending.get(m.id);
        if (!p) return;
        pending.delete(m.id);
        if (m.error) p.reject(new Error(m.error));
        else p.resolve(m.bytes!);
        return;
      }

      case "cancel":
        engine?.cancel();
        return;

      case "query": {
        if (!engine) throw new Error("catalog: query before open");
        const value = await engine.query(m.req);
        post({ t: "done", id: m.id, value, stats: engine.stats() });
        return;
      }

      case "search": {
        if (!engine) throw new Error("catalog: search before open");
        const total = await engine.searchStream(m.opts, (hit) => post({ t: "row", id: m.id, hit }));
        post({ t: "done", id: m.id, value: total, stats: engine.stats() });
        return;
      }
    }
  } catch (err) {
    // `open` carries no id; its failure must land on the open-promise, which the main thread
    // registers under -1. Anything else routes back to its own call.
    const id = "id" in m ? (m.id as number) : -1;
    post({ t: "err", id, message: err instanceof Error ? err.message : String(err) });
  }
};
