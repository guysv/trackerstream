// Public surface of the shared UI.
//
// Both shells (Tauri desktop, js-libp2p web) mount the same components against the same stores;
// the ONLY thing they supply differently is a NodeClient. See ./client/NodeClient.ts.
export { can, client, maybeClient, setClient } from "./client/index.ts";
export type { Capabilities, CatalogSearchOpts, NodeClient } from "./client/NodeClient.ts";
export type * from "./client/types.ts";

// Data facades (identical on both shells — they route through the injected client).
export * from "./catalog.ts";
export * from "./p2p.ts";
export * from "./playlists.svelte.ts";

// Stores.
export * from "./player.svelte.ts";
export * from "./peers.svelte.ts";
export * from "./ui.svelte.ts";
export * from "./toast.svelte.ts";
export * from "./contextmenu.svelte.ts";

// Utilities.
export * from "./cidCache.ts";
export * from "./format.ts";
export * from "./menus.ts";
export * from "./debug.ts";
export * from "./deeplink.ts";
export * from "./mediaKeys.ts";

// Views — the whole app, minus the SvelteKit route files that mount them. Each shell keeps thin
// +page/+layout wrappers so routing stays the app's business, not the library's.
export { default as AppShell } from "./views/AppShell.svelte";
export { default as HomeView } from "./views/HomeView.svelte";
export { default as SearchView } from "./views/SearchView.svelte";
export { default as PlaylistView } from "./views/PlaylistView.svelte";
