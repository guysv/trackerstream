// Frontend logging. Traces (dbg) and failures (logWarn/logError) both go to the webview
// console AND into the app's shared log file via tauri-plugin-log — so whether a line is
// recorded is a runtime decision (TS_LOG level, or any dev build), not a rebuild. Failures
// land in the SAME rotating log file as the Rust/Go logs, so a swallowed catch is finally
// diagnosable from one place. Fire-and-forget; these never throw back into the caller.
import { debug, warn, error } from "@tauri-apps/plugin-log";

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

/** Render an unknown thrown value for a log line — keeps an Error's stack, which the
 *  common `String(e)` / `${e}` (used all over the UI) throws away. */
function errString(err: unknown): string {
  if (err instanceof Error) return err.stack ?? `${err.name}: ${err.message}`;
  if (typeof err === "string") return err;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}

function record(
  sink: (msg: string) => Promise<void>,
  con: (...a: unknown[]) => void,
  prefix: string,
  tag: string,
  err?: unknown,
  data?: Record<string, unknown>,
): void {
  const parts = [tag];
  if (err !== undefined) parts.push(errString(err));
  if (data) parts.push(JSON.stringify(data));
  const line = parts.join(" ");
  con(prefix, line);
  try {
    void sink(line).catch(() => {});
  } catch {
    /* not running under Tauri */
  }
}

/** Log a recoverable failure (a degraded-but-continuing path). `tag` groups the source;
 *  pass the caught `err` so its detail/stack is preserved, not stringified away. */
export function logWarn(tag: string, err?: unknown, data?: Record<string, unknown>): void {
  // eslint-disable-next-line no-console
  record(warn, console.warn, "[UIWARN]", tag, err, data);
}

/** Log a genuine failure with the real error (incl. stack). Use at every catch that used to
 *  drop the error — even when a fallback keeps the UI working, the cause now reaches the log
 *  file. For failures the user should also SEE, use `reportError` in toast.svelte.ts. */
export function logError(tag: string, err?: unknown, data?: Record<string, unknown>): void {
  // eslint-disable-next-line no-console
  record(error, console.error, "[UIERR]", tag, err, data);
}
