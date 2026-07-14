// The injected NodeClient singleton.
//
// A module-level singleton, deliberately NOT Svelte context: the stores (player, playlists, peers)
// hold module-level rune state and run outside any component tree, so a context-scoped client would
// force all three to be rewritten as factories. One client per JS realm is exactly right here — the
// app IS one node.
//
// Each shell calls setClient() once, before mount:
//   desktop  -> setClient(new TauriClient())
//   web      -> setClient(await createWebClient())
import type { NodeClient } from "./NodeClient.ts";

let current: NodeClient | null = null;

export function setClient(c: NodeClient): void {
  current = c;
}

/** The active client. Throws if the shell forgot to install one — a programming error, loud on
 *  purpose: every data path in the UI runs through here, so a silent null would surface as a dozen
 *  unrelated failures instead of one clear message. */
export function client(): NodeClient {
  if (!current) {
    throw new Error("NodeClient not installed — the shell must call setClient() before mount");
  }
  return current;
}

/** Capability probe, safe to call before a client is installed (returns false). Lets a component
 *  render its degraded form during boot instead of throwing. */
export function can(cap: keyof NodeClient["caps"]): boolean {
  return current?.caps[cap] ?? false;
}

/** The client if installed, else null — never throws. For code that legitimately runs before the
 *  shell installs one (logging, most obviously: a boot-time failure must still reach the console
 *  even though there is no node yet to send it to). */
export function maybeClient(): NodeClient | null {
  return current;
}

export type { NodeClient, Capabilities, CatalogSearchOpts } from "./NodeClient.ts";
export type * from "./types.ts";
