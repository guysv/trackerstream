// Worker-pool primitives for the parallel bake (BAKE_WORKERS=N). A fixed pool of bake-worker
// threads does the CPU work; a Semaphore bounds how many modules are in flight (backpressure, so
// we don't build the whole corpus into memory ahead of the serialized I/O); a Mutex serializes the
// shared-resource I/O (tsnode block/put + SQLite catalog) on the main thread.
import { Worker } from "node:worker_threads";

/** Counting semaphore. `acquire()` resolves when a slot is free; `release()` hands a slot to the
 *  next waiter (or frees it). Slot count == number of holders. */
export class Semaphore {
  private max: number;
  private cur = 0;
  private q: Array<() => void> = [];
  constructor(max: number) {
    this.max = max;
  }
  acquire(): Promise<void> {
    return new Promise((res) => {
      if (this.cur < this.max) {
        this.cur++;
        res();
      } else {
        this.q.push(res);
      }
    });
  }
  release(): void {
    const w = this.q.shift();
    if (w) w(); // hand the slot to a waiter (cur unchanged)
    else this.cur--;
  }
}

/** Mutex (Semaphore(1)) with a run-exclusive helper. */
export class Mutex {
  private sem = new Semaphore(1);
  async run<T>(fn: () => Promise<T> | T): Promise<T> {
    await this.sem.acquire();
    try {
      return await fn();
    } finally {
      this.sem.release();
    }
  }
}

export interface BakeResult {
  id: number;
  ok: boolean;
  root?: string;
  isFlat?: boolean;
  blocks?: { cid: string; bytes: Uint8Array }[];
  error?: string;
}

/** Fixed worker pool. `run(bytes, name)` queues a module and resolves with the worker's BakeResult
 *  when a worker has built its DAG. Workers are reused; jobs queue when all are busy. */
export class BakePool {
  private workers: Worker[] = [];
  private free: Worker[] = [];
  private jobs = new Map<number, (r: BakeResult) => void>();
  private busy = new Map<Worker, number>(); // worker -> the job id it is currently running
  private pending: Array<{ id: number; bytes: Uint8Array; name: string }> = [];
  private nextId = 0;
  private closed = false;

  constructor(
    n: number,
    private workerUrl: URL,
  ) {
    for (let i = 0; i < n; i++) this.spawn();
  }

  private spawn(): void {
    const w = new Worker(this.workerUrl);
    w.on("message", (m: BakeResult) => {
      const resolve = this.jobs.get(m.id);
      this.jobs.delete(m.id);
      this.busy.delete(w);
      this.free.push(w);
      resolve?.(m);
      this.pump();
    });
    w.on("error", (e) => {
      // A worker crash (uncaught throw / OOM) fires here with NO message, so the in-flight job's
      // resolver would never be called and `Promise.all` over the bakes would hang forever. Settle
      // that job as a failure, drop the dead worker, and respawn to keep pool capacity.
      const jobId = this.busy.get(w);
      this.busy.delete(w);
      this.workers = this.workers.filter((x) => x !== w);
      this.free = this.free.filter((x) => x !== w);
      if (jobId !== undefined) {
        const resolve = this.jobs.get(jobId);
        this.jobs.delete(jobId);
        resolve?.({ id: jobId, ok: false, error: `bake worker crashed: ${String(e)}` });
      }
      console.error(`[bake] worker crashed${jobId !== undefined ? ` (job ${jobId})` : ""}: ${e}`);
      if (!this.closed) {
        this.spawn();
        this.pump();
      }
    });
    this.workers.push(w);
    this.free.push(w);
  }

  run(bytes: Uint8Array, name: string): Promise<BakeResult> {
    return new Promise((resolve) => {
      const id = this.nextId++;
      this.jobs.set(id, resolve);
      this.pending.push({ id, bytes, name });
      this.pump();
    });
  }

  private pump(): void {
    while (this.free.length && this.pending.length) {
      const w = this.free.pop()!;
      const job = this.pending.shift()!;
      this.busy.set(w, job.id);
      // Structured-clone the input (no transfer): module bytes come from a pooled Node Buffer whose
      // ArrayBuffer is shared, so transferring it would corrupt siblings. The copy is ~module-size.
      w.postMessage({ id: job.id, bytes: job.bytes, name: job.name });
    }
  }

  get size(): number {
    return this.workers.length;
  }

  async close(): Promise<void> {
    this.closed = true; // stop the error handler from respawning during teardown
    await Promise.all(this.workers.map((w) => w.terminate()));
  }
}
