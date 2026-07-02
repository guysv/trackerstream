// Streaming-state tracer. Each transition goes to the webview console AND into the
// app's log file via tauri-plugin-log at debug level — so whether a trace is recorded
// is a runtime decision (TS_LOG=debug, or any dev build), not a rebuild.
import { debug } from "@tauri-apps/plugin-log";

let t0 = 0;
function clock(): string {
  const now = typeof performance !== "undefined" ? performance.now() : 0;
  if (!t0) t0 = now;
  return `+${((now - t0) / 1000).toFixed(2)}s`;
}

/** Trace a streaming/UI state transition. `tag` groups the source, `data` is any
 *  small JSON-able payload. Fire-and-forget; never throws into the caller. */
export function dbg(tag: string, data?: Record<string, unknown>): void {
  const payload = data ? " " + JSON.stringify(data) : "";
  const line = `${clock()} ${tag}${payload}`;
  // eslint-disable-next-line no-console
  console.debug("[UIDBG]", line);
  // Into the shared log file (best-effort; absent in a plain browser).
  try {
    void debug(line).catch(() => {});
  } catch {
    /* not running under Tauri */
  }
}
