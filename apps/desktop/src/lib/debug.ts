// Streaming-state tracer. The webview console isn't visible in the dev terminal,
// so we mirror each transition to the Rust backend (debug_log -> stderr) AND to
// the webview console. Flip DEBUG off to make every call a no-op.
import { invoke } from "@tauri-apps/api/core";

export const DEBUG = false;

let t0 = 0;
function clock(): string {
  const now = typeof performance !== "undefined" ? performance.now() : 0;
  if (!t0) t0 = now;
  return `+${((now - t0) / 1000).toFixed(2)}s`;
}

/** Trace a streaming/UI state transition. `tag` groups the source, `data` is any
 *  small JSON-able payload. Fire-and-forget; never throws into the caller. */
export function dbg(tag: string, data?: Record<string, unknown>): void {
  if (!DEBUG) return;
  const payload = data ? " " + JSON.stringify(data) : "";
  const line = `${clock()} ${tag}${payload}`;
  // eslint-disable-next-line no-console
  console.debug("[UIDBG]", line);
  // Mirror to the dev terminal via the backend (best-effort; absent in a browser).
  try {
    void invoke("debug_log", { line }).catch(() => {});
  } catch {
    /* not running under Tauri */
  }
}
