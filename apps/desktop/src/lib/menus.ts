// Menu-item builders — the single source of truth for "what can I do to a track / playlist".
// Reused by the right-click ContextMenu today; the panel button rows can fold onto these
// same builders later. Each builder READS reactive state (isLiked, plState.version) when
// called, so invoking it inside <ContextMenu>'s $derived makes the open menu live-update.

import type { MenuItem } from "$lib/contextmenu.svelte";
import type { ModuleHit } from "$lib/catalog";
import { downloadAndOpen } from "$lib/p2p";
import { enqueue, playList } from "$lib/player.svelte";
import {
  plState,
  plList,
  plCreate,
  addTrackTo,
  hitTuple,
  isLiked,
  toggleLike,
  plBump,
  playPlaylist,
  plGet,
  plHold,
  duplicatePlaylist,
  plCopyLink,
  type PlaylistMeta,
} from "$lib/playlists.svelte";

export interface TrackCtx {
  /** Right-clicked inside a playlist you own — offer "Remove from this playlist". */
  inPlaylistYouOwn?: boolean;
  /** Right-clicked inside the queue — offer "Remove from queue". */
  inQueue?: boolean;
  /** The context-specific removal handler for the two cases above. */
  onRemove?: () => void;
}

// External trackers the "Open with" submenu offers. `app` is the opener target handed to the OS
// (macOS `open -a <app>`, Linux the binary name); label is what the menu shows.
const OPEN_WITH_APPS: { label: string; app: string }[] = [
  { label: "SchismTracker", app: "schismtracker" },
  { label: "MilkyTracker", app: "milkytracker" },
];

/** Reassemble `h`'s byte-exact original and open it in an external tracker. The backend fetches
 *  the module's own rootCid, reassembles (v3 or v1, byte-exact), verifies MD5 parity vs the
 *  catalog, saves to ~/Downloads, and launches `app`. Fire-and-forget like the other menu
 *  actions; failures are logged (the app has no toast surface). */
async function openWithTracker(h: ModuleHit, app: string): Promise<void> {
  try {
    const res = await downloadAndOpen({ root: h.rootCid, md5: h.md5, filename: h.filename, openWith: app });
    if (!res.launched) {
      console.error(`[open-with] saved ${res.path} but could not launch ${app}: ${res.launch_error ?? "unknown"}`);
    }
  } catch (e) {
    console.error(`[open-with] failed for ${h.filename}:`, e);
  }
}

export function trackMenuItems(h: ModuleHit, ctx: TrackCtx = {}): MenuItem[] {
  void plState.version; // subscribe: a network sync / like elsewhere re-derives this menu
  const liked = isLiked(h.md5);
  const items: MenuItem[] = [
    { kind: "action", label: "Play", icon: "▶", onSelect: () => playList([h], 0) },
    { kind: "action", label: "Play next", onSelect: () => enqueue(h, true) },
    { kind: "action", label: "Add to queue", onSelect: () => enqueue(h) },
    { kind: "separator" },
    {
      kind: "submenu",
      label: "Add to playlist",
      items: async () => {
        const mine = (await plList().catch(() => [])).filter((p) => p.isMine && !p.liked);
        const out: MenuItem[] = mine.map((p) => ({
          kind: "action",
          label: p.title || "(untitled)",
          onSelect: () => addTrackTo(p.name, h),
        }));
        out.push({ kind: "separator" });
        out.push({
          kind: "action",
          label: "＋ New playlist",
          onSelect: async () => {
            await plCreate(h.title || h.filename, [hitTuple(h)]);
            plBump();
          },
        });
        return out;
      },
    },
    {
      kind: "action",
      label: liked ? "Remove from Liked Tracks" : "Save to Liked Tracks",
      icon: liked ? "♥" : "♡",
      onSelect: () => toggleLike(h),
    },
  ];
  if (ctx.inPlaylistYouOwn && ctx.onRemove) {
    items.push(
      { kind: "separator" },
      { kind: "action", danger: true, label: "Remove from this playlist", onSelect: ctx.onRemove },
    );
  }
  if (ctx.inQueue && ctx.onRemove) {
    items.push(
      { kind: "separator" },
      { kind: "action", danger: true, label: "Remove from queue", onSelect: ctx.onRemove },
    );
  }
  items.push(
    { kind: "separator" },
    {
      kind: "submenu",
      label: "Open with",
      items: OPEN_WITH_APPS.map(
        (a): MenuItem => ({
          kind: "action",
          label: a.label,
          onSelect: () => void openWithTracker(h, a.app),
        }),
      ),
    },
    {
      kind: "action",
      label: "Copy CID",
      onSelect: () => void navigator.clipboard.writeText(h.rootCid).catch(() => {}),
    },
  );
  return items;
}

async function playByName(name: string): Promise<void> {
  const d = await plGet(name);
  if (d) await playPlaylist(d);
}

async function copyPlaylistLink(name: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(await plCopyLink(name));
  } catch {
    /* no live record / clipboard denied — best-effort */
  }
}

// Playlist menu: the safe, no-confirmation actions. Destructive / stateful ones (share,
// make-private, delete, rename) keep their explanatory confirm UI in PlaylistDetailPanel;
// folding those onto this builder is the follow-up once that confirm is a reusable piece.
export function playlistMenuItems(p: PlaylistMeta): MenuItem[] {
  void plState.version; // re-derive if the row's tier/flags change under the network
  const items: MenuItem[] = [
    { kind: "action", label: "Play", icon: "▶", disabled: p.tracks === 0, onSelect: () => playByName(p.name) },
  ];
  if (!(p.isMine && p.liked)) {
    if (!p.isMine) {
      items.push(
        { kind: "separator" },
        {
          kind: "action",
          label: p.held ? "Remove from library" : "Add to library",
          onSelect: async () => {
            await plHold(p.name, !p.held);
            plBump();
          },
        },
        {
          kind: "action",
          label: "Duplicate to mine",
          onSelect: async () => {
            const d = await plGet(p.name);
            if (d) await duplicatePlaylist(d);
          },
        },
      );
    }
    if (p.published && !p.dormant) {
      items.push({ kind: "action", label: "Copy link", onSelect: () => copyPlaylistLink(p.name) });
    }
  }
  return items;
}
