// General toast surface: transient, user-facing failure/notice messages that any code can
// raise — not just playback (playback keeps its own persistent indicator via nowPlaying.error).
// A module-level rune store so it survives client navigation; +layout.svelte renders the stack.
import { logError } from "./debug";

export type Toast = { id: number; message: string; kind: "error" | "info" };

export const toasts = $state<Toast[]>([]);

let seq = 0;

/** Show a transient toast. Auto-dismisses after `ttlMs` (pass 0 to keep it until dismissed). */
export function toast(message: string, kind: Toast["kind"] = "error", ttlMs = 6000): void {
  const id = ++seq;
  toasts.push({ id, message, kind });
  if (ttlMs > 0) {
    setTimeout(() => dismiss(id), ttlMs);
  }
}

/** Remove a toast early (e.g. the user clicked it). No-op if already gone. */
export function dismiss(id: number): void {
  const i = toasts.findIndex((t) => t.id === id);
  if (i !== -1) toasts.splice(i, 1);
}

/** Log a failure to console + the shared log file AND surface it to the user as a toast.
 *  `userMessage` is the human-readable text shown; the raw `err` (with stack) goes only to the
 *  log, so we don't leak internals into the UI. Use this at catches where the user has a right
 *  to know the action failed (an explicit action they took, or a load that left the view empty). */
export function reportError(tag: string, err: unknown, userMessage: string): void {
  logError(tag, err);
  toast(userMessage, "error");
}
