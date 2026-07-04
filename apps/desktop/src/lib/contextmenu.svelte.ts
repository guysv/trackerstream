// The app's right-click menu (replaces the webview default). A themed <div> menu — not a
// native Tauri Menu — so it matches the app's look and supports submenus / danger styling.
//
// Reactivity contract (the whole point): the store holds a BUILDER CLOSURE over the stable
// target, never a snapshot of items. <ContextMenu> re-derives `build()` on every render, so
// an open menu reflects live state — a track liked elsewhere, a playlist synced in from the
// network (Phase 1's `playlists:changed`) — without being reopened.

export type MenuItem =
  | {
      kind: "action";
      label: string;
      icon?: string;
      danger?: boolean;
      disabled?: boolean;
      onSelect: () => void | Promise<void>;
    }
  | {
      kind: "submenu";
      label: string;
      icon?: string;
      /** A static list, or an async loader (e.g. the user's playlists) resolved on hover. */
      items: MenuItem[] | (() => Promise<MenuItem[]>);
    }
  | { kind: "separator" }
  | { kind: "header"; label: string };

export interface OpenMenu {
  x: number;
  y: number;
  build: () => MenuItem[];
}

// Holder object so the nullable can be reassigned (a bare `export const … = $state(null)`
// can't be reassigned; a field on a stable object can).
export const menu = $state<{ open: OpenMenu | null }>({ open: null });

/** Open the menu at the event position. `build` is re-invoked reactively while open. */
export function openContextMenu(e: MouseEvent, build: () => MenuItem[]): void {
  e.preventDefault();
  e.stopPropagation();
  menu.open = { x: e.clientX, y: e.clientY, build };
}

export function closeContextMenu(): void {
  menu.open = null;
}
