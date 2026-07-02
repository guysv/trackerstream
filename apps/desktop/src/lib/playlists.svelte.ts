// Playlists client (PLAYLISTS.md): thin wrappers over the playlist_* Tauri commands
// plus the play/add/save helpers the components share. The durable store is Rust's
// playlists.db; this layer is stateless except for a bump counter that tells views to
// refresh after a mutation.
import { invoke } from "@tauri-apps/api/core";
import { getModule, type ModuleHit } from "$lib/catalog";
import { playList, queue } from "$lib/player.svelte";

export interface PlaylistMeta {
  name: string;
  title: string;
  tracks: number;
  isMine: boolean;
  held: boolean;
  published: boolean;
  sizeBytes: number;
  lastUpdateAt: number;
  lastPlayedAt: number | null;
}

export interface PlaylistItem {
  id: number;
  modName: string;
  title: string;
}

export interface PlaylistDetail extends PlaylistMeta {
  items: PlaylistItem[];
}

export interface PlaylistSyncStatus {
  total: number;
  mine: number;
  bytes: number;
  budget: number;
}

/** The compact doc's track tuple: [catalog id, module name, song title]. */
export type TrackTuple = [number, string, string];

export type PlaylistScope = "library" | "seen" | "all";

export const plSearch = (q: string) => invoke<PlaylistMeta[]>("playlist_search", { q });
export const plList = (scope: PlaylistScope = "all") =>
  invoke<PlaylistMeta[]>("playlist_list", { scope });
/** Add/remove a foreign playlist to/from the library — the "holder" tier: backed
 * (re-announced), never evicted. Distinct from duplicate (fork) and delete. */
export const plHold = (name: string, held: boolean) =>
  invoke<void>("playlist_hold", { name, held });
export const plGet = (name: string) => invoke<PlaylistDetail | null>("playlist_get", { name });
export const plCreate = (title: string, tracks: TrackTuple[]) =>
  invoke<PlaylistMeta>("playlist_create", { title, tracks });
export const plUpdate = (name: string, title: string, tracks: TrackTuple[]) =>
  invoke<void>("playlist_update", { name, title, tracks });
export const plDelete = (name: string) => invoke<void>("playlist_delete", { name });
/** The explicit "Share" action — the only thing that makes a playlist public. */
export const plPublish = (name: string) => invoke<void>("playlist_publish", { name });
export const plStatus = () => invoke<PlaylistSyncStatus>("playlist_sync_status");

export const hitTuple = (h: ModuleHit): TrackTuple => [h.id, h.filename, h.title];
export const itemTuples = (items: PlaylistItem[]): TrackTuple[] =>
  items.map((i) => [i.id, i.modName, i.title]);

/** Mutation counter: components re-query when this bumps. */
export const plState = $state({ version: 0 });
export const plBump = () => plState.version++;

// The Rust sync loop writes into playlists.db behind the UI's back (20s poll of the
// sidecar), so bump the counter on a timer too — otherwise a playlist synced from the
// network stays invisible until some local mutation happens to refresh the views.
// Queries only actually run while playlist components are mounted, and they're local
// SQLite reads. (window guard: this module also loads during SSR prerender.)
if (typeof window !== "undefined") {
  setInterval(plBump, 10_000);
}

// Playlist docs carry no root CIDs (by design — tracks resolve via the catalog only at
// play time), so playing a playlist resolves entries through `getModule` first. Bounded
// like the results table: a >200-track playlist plays its first 200.
const RESOLVE_CAP = 200;

export async function playPlaylist(detail: PlaylistDetail, index = 0): Promise<void> {
  const slice = detail.items.slice(0, RESOLVE_CAP);
  const wantedId = detail.items[Math.min(index, slice.length - 1)]?.id;
  const hits = (
    await Promise.all(slice.map((i) => getModule(i.id).catch(() => null)))
  ).filter(Boolean) as ModuleHit[];
  if (!hits.length) throw new Error("no playable tracks in playlist");
  const at = Math.max(0, hits.findIndex((h) => h.id === wantedId));
  playList(hits, at);
  void invoke("playlist_played", { name: detail.name }).catch(() => {});
}

/** Append a track to one of my playlists (no-op if it's already in). */
export async function addTrackTo(name: string, h: ModuleHit): Promise<void> {
  const d = await plGet(name);
  if (!d) return;
  const tuples = itemTuples(d.items);
  if (!tuples.some((t) => t[0] === h.id)) tuples.push(hitTuple(h));
  await plUpdate(name, d.title, tuples);
  plBump();
}

export async function saveQueueAsPlaylist(title: string): Promise<void> {
  await plCreate(title, queue.items.map(hitTuple));
  plBump();
}

/** Copy a foreign playlist into my own editable one. */
export async function duplicatePlaylist(detail: PlaylistDetail): Promise<void> {
  await plCreate(`${detail.title || "playlist"} (copy)`, itemTuples(detail.items));
  plBump();
}
