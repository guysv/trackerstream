// Main-thread handle to the catalog worker.
//
// Owns the worker's lifecycle and services its block fetches from Helia. Presents exactly the
// NodeClient.catalog surface, so the web shell's adapter is a two-line delegation.
import type { CID } from "multiformats/cid";
import { CID as CIDParser } from "multiformats/cid";
import type { CatalogStats } from "./engine.ts";
import type { WorkerIn, WorkerOut } from "./protocol.ts";

export type { CatalogStats } from "./engine.ts";
export { buildMatchstr } from "./search.ts";
export { isTszcat, parseTszcat } from "./tszcat.ts";

// DEV-only fetch-latency accounting (per query): counts, summed/max ms, bytes. Reset on each `done`.
const DBG = Boolean((import.meta as unknown as { env?: { DEV?: boolean } }).env?.DEV);
let _fc = 0;
let _fb = 0;
let _fms = 0;
let _fmax = 0;

export interface CatalogSources {
  /** Whole-file read of the TSZCAT manifest (the only UnixFS read in the catalog path). */
  readRoot(cid: CID): Promise<Uint8Array>;
  /** One raw block by CID, over Bitswap. */
  getBlock(cid: CID): Promise<Uint8Array>;
}

export class CatalogClient {
  private worker: Worker;
  private nextId = 0;
  private calls = new Map<
    number,
    { resolve: (v: unknown) => void; reject: (e: Error) => void; onRow?: (h: unknown) => void }
  >();
  private opened: Promise<void>;
  private lastStats: CatalogStats | null = null;

  private readonly src: CatalogSources;

  constructor(rootCid: string, src: CatalogSources) {
    this.src = src;
    this.worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
    this.worker.onmessage = (e: MessageEvent<WorkerOut>) => void this.onMessage(e.data);
    // A FAILED open must REJECT this promise, not resolve it. Resolving would let every subsequent
    // query through to a worker with no engine, which reports "search before open" — burying the
    // actual reason the catalog didn't open under a misleading one.
    let ready!: () => void;
    let fail!: (e: Error) => void;
    this.opened = new Promise<void>((res, rej) => {
      ready = res;
      fail = rej;
    });
    this.calls.set(-1, { resolve: () => ready(), reject: (e) => fail(e) });
    this.post({ t: "open", rootCid });
  }

  private post(m: WorkerIn, transfer: Transferable[] = []): void {
    this.worker.postMessage(m, transfer);
  }

  private async onMessage(m: WorkerOut): Promise<void> {
    switch (m.t) {
      case "opened":
        this.calls.get(-1)?.resolve(undefined);
        this.calls.delete(-1);
        return;
      case "fetch": {
        // The worker has no libp2p — it asks us. Errors are forwarded, not thrown, so one bad
        // block fails its query instead of killing the worker.
        try {
          const cid = CIDParser.parse(m.cid);
          const _t = DBG ? performance.now() : 0;
          const bytes = m.kind === "root" ? await this.src.readRoot(cid) : await this.src.getBlock(cid);
          if (DBG) {
            const dt = performance.now() - _t;
            _fc++;
            _fb += bytes.length;
            _fms += dt;
            if (dt > _fmax) _fmax = dt;
          }
          // Structured-CLONE, never transfer. These bytes are Helia's — the blockstore hands out its
          // own cached buffer, and transferring it detaches the copy Helia still holds, so the next
          // read of that block throws "ArrayBuffer is already detached". The clone costs a memcpy;
          // the transfer costs correctness.
          this.post({ t: "fetched", id: m.id, bytes });
        } catch (e) {
          this.post({ t: "fetched", id: m.id, error: e instanceof Error ? e.message : String(e) });
        }
        return;
      }
      case "row":
        this.calls.get(m.id)?.onRow?.(m.hit);
        return;
      case "done": {
        this.lastStats = m.stats;
        if (DBG) {
          const s = m.stats;
          console.info(
            `[catalog] query done: blocks=${s.blocks} waves=${s.waves} wire=${Math.round(s.wireBytes / 1024)}KB | ` +
              `helia fetch: n=${_fc} sum=${Math.round(_fms)}ms max=${Math.round(_fmax)}ms avg=${_fc ? Math.round(_fms / _fc) : 0}ms bytes=${Math.round(_fb / 1024)}KB`,
          );
          _fc = _fb = _fms = _fmax = 0;
        }
        const c = this.calls.get(m.id);
        this.calls.delete(m.id);
        c?.resolve(m.value);
        return;
      }
      case "err": {
        const c = this.calls.get(m.id);
        this.calls.delete(m.id);
        c?.reject(new Error(m.message));
        return;
      }
    }
  }

  private call<T>(send: (id: number) => WorkerIn, onRow?: (h: unknown) => void): Promise<T> {
    const id = this.nextId++;
    return this.opened.then(
      () =>
        new Promise<T>((resolve, reject) => {
          this.calls.set(id, { resolve: resolve as (v: unknown) => void, reject, onRow });
          this.post(send(id));
        }),
    );
  }

  query<T>(req: Record<string, unknown>): Promise<T> {
    return this.call<T>((id) => ({ t: "query", id, req }));
  }

  searchStream(
    opts: { q: string; limit: number; after?: number; namesOnly?: boolean },
    onRow: (hit: any) => void,
  ): Promise<number> {
    return this.call<number>((id) => ({ t: "search", id, opts }), onRow as (h: unknown) => void);
  }

  /** Supersede the in-flight query. Cheap and fire-and-forget — the user just typed. */
  cancel(): void {
    this.post({ t: "cancel" });
  }

  /** Resolve the schema + FTS upper tree so the first keystroke descends from warm pages. */
  async warm(): Promise<void> {
    await this.query({ op: "search", q: "the", limit: 1 }).catch(() => {});
  }

  /** Wire bytes / waves of the last completed query — the numbers that decide whether the
   *  scheduler is doing its job. Exposed for the perf harness, not the UI. */
  stats(): CatalogStats | null {
    return this.lastStats;
  }

  close(): void {
    this.worker.terminate();
  }
}
