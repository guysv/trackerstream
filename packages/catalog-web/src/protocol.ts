// The main-thread <-> worker wire. Kept in one file so the two halves cannot drift.
import type { CatalogStats } from "./engine.ts";

export type WorkerIn =
  | { t: "open"; rootCid: string }
  | { t: "query"; id: number; req: Record<string, unknown> }
  | { t: "search"; id: number; opts: { q: string; limit: number; after?: number; namesOnly?: boolean } }
  | { t: "cancel" }
  /** The main thread's answer to a `fetch` — it owns Helia, so all bytes come from there. */
  | { t: "fetched"; id: number; bytes?: Uint8Array; error?: string };

export type WorkerOut =
  | { t: "opened" }
  | { t: "fetch"; id: number; kind: "block" | "root"; cid: string }
  /** Streamed search hits: emitted as their pages land, so the UI paints progressively. */
  | { t: "row"; id: number; hit: unknown }
  | { t: "done"; id: number; value: unknown; stats: CatalogStats }
  | { t: "err"; id: number; message: string };
