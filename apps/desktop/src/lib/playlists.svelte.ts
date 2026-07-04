// Playlists client (PLAYLISTS.md): thin wrappers over the playlist_* Tauri commands
// plus the play/add/save helpers the components share. The durable store is Rust's
// playlists.db; this layer is stateless except for a bump counter that tells views to
// refresh after a mutation.
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { getModule, type ModuleHit } from "$lib/catalog";
import { playList, queue, setOnQueueSourceChange } from "$lib/player.svelte";

export interface PlaylistMeta {
  name: string;
  title: string;
  tracks: number;
  isMine: boolean;
  held: boolean;
  published: boolean;
  /** No longer propagating (record expired or author tombstoned it): still playable,
   * the UI nudges toward "duplicate to mine". */
  dormant: boolean;
  /** The author published a deletion; library copies are preserved dormant. */
  tombstoned: boolean;
  /** The private per-client "Liked Tracks" playlist (Spotify-style) — own, never
   * published; the UI pins it and hides share/delete. */
  liked: boolean;
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
  held: number;
  seen: number;
  dormant: number;
  /** Seen-tier bytes — what the budget governs (library rows are exempt). */
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
/** "Make private again": best-effort tombstone retract, keep the data local (published=0). */
export const plUnpublish = (name: string) => invoke<void>("playlist_unpublish", { name });
export const plStatus = () => invoke<PlaylistSyncStatus>("playlist_sync_status");

// ---- liked tracks (the private "Liked Tracks" playlist — Spotify-style ♥) ----

/** Toggle a track in "Liked Tracks" (creates the private playlist on first use).
 * Returns true if the track is now liked. */
export const plLikeToggle = (track: TrackTuple) =>
  invoke<boolean>("playlist_like_toggle", { track });
/** The catalog ids currently liked — what the heart buttons check membership against. */
export const plLikedIds = () => invoke<number[]>("playlist_liked_ids");
/** The liked playlist's name (creating it if needed) — for opening it in the view. */
export const plLikedName = () => invoke<string>("playlist_liked_name");

// ---- deep links ----

export interface LinkStatus {
  name: string;
  /** "ready" = row present locally; "pending" = name-only link, chasing via gossip. */
  status: "ready" | "pending";
}

/** Ingest an incoming playlist link (payload verified in Rust; name-only pends). */
export const plIngestLink = (url: string) =>
  invoke<LinkStatus>("playlist_ingest_link", { url });
/** Shareable HTTPS link for a stored playlist (errors if it has no live record). */
export const plCopyLink = (name: string) => invoke<string>("playlist_copy_link", { name });
/** Names from name-only links still waiting on gossip (the "syncing…" placeholder). */
export const plPending = () => invoke<string[]>("playlist_pending");

/** Hold-beacon backer counts (windowed distinct holders) for the given names.
 * 0 = no beacon heard — normal for the first hour after startup. */
export const plBackers = (names: string[]) =>
  invoke<Record<string, number>>("playlist_backers", { names });

export const hitTuple = (h: ModuleHit): TrackTuple => [h.id, h.filename, h.title];
export const itemTuples = (items: PlaylistItem[]): TrackTuple[] =>
  items.map((i) => [i.id, i.modName, i.title]);

/** Mutation counter: components re-query when this bumps. */
export const plState = $state({ version: 0 });
export const plBump = () => plState.version++;

// The set of catalog ids in the private "Liked Tracks" playlist. Every ♥ button across
// the UI reads this reactively; kept warm by refreshLiked() on the sync cadence and
// flipped optimistically on toggle so the heart reacts instantly.
export const liked = $state<{ ids: Set<number> }>({ ids: new Set() });
export const isLiked = (id: number): boolean => liked.ids.has(id);

export async function refreshLiked(): Promise<void> {
  try {
    liked.ids = new Set(await plLikedIds());
  } catch {
    /* keep the last known set on a transient failure */
  }
}

/** Toggle a track's membership in "Liked Tracks". Reassigns the reactive set so the
 * heart flips without waiting for the next refresh. */
export async function toggleLike(h: ModuleHit): Promise<void> {
  const nowLiked = await plLikeToggle(hitTuple(h));
  const ids = new Set(liked.ids);
  if (nowLiked) ids.add(h.id);
  else ids.delete(h.id);
  liked.ids = ids;
  plBump();
}

// The Rust sync loop writes into playlists.db behind the UI's back (20s poll of the
// sidecar). It now emits a coalesced `playlists:changed` whenever a tick actually changed
// the store (synced/updated/tombstoned playlist, budget eviction, or seen-tier decay), so
// the UI reconciles the instant the network moves — no longer up to 10s stale. Local
// mutations keep bumping plState directly (optimistic + instant); this is the SAME channel
// for network events. (window guard: this module also loads during SSR prerender.)
if (typeof window !== "undefined") {
  void refreshLiked();
  listen("playlists:changed", () => {
    plBump();
    void refreshLiked();
  }).catch(() => {
    /* off-Tauri (SSR/browser preview): no event bus — the safety-net timer below covers it */
  });
  // Backstop only (was the 10s primary signal): a slow reconcile in case an emit is ever
  // missed. Queries run only while playlist components are mounted, and they're local
  // SQLite reads, so a 60s catch-all is cheap.
  setInterval(() => {
    plBump();
    void refreshLiked(); // catches unlikes done from the playlist detail (splice + update)
  }, 60_000);
}

// Play-time pin: while the queue is sourced from a playlist, Rust protects that row
// from decay / budget eviction / tombstone deletion so it can't vanish under playback
// (a transient pin — NOT the held tier; nothing is backed or re-announced). Playing
// anything else, or clearing the queue, releases it.
setOnQueueSourceChange(() => void invoke("playlist_pin", { name: null }).catch(() => {}));

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
  playList(hits, at); // fires the source-change hook first, releasing any prior pin
  void invoke("playlist_pin", { name: detail.name }).catch(() => {});
  // Bump only after the mark lands, so the library re-query reads the fresh
  // last_played_at and the just-played playlist visibly rises to the top of its tier.
  void invoke("playlist_played", { name: detail.name }).then(plBump).catch(() => {});
}

/** Append a track to one of my playlists (no-op if it's already in). */
export async function addTrackTo(name: string, h: ModuleHit): Promise<void> {
  const d = await plGet(name);
  if (!d) return;
  const tuples = itemTuples(d.items);
  if (!tuples.some((t) => t[0] === h.id)) tuples.push(hitTuple(h));
  await plUpdate(name, d.title, tuples);
  // If this is the private Liked Tracks playlist, flip the ♥ optimistically (reassign
  // the reactive set) instead of waiting on the 10s refreshLiked() poll — mirrors toggleLike().
  if (d.liked && !liked.ids.has(h.id)) liked.ids = new Set(liked.ids).add(h.id);
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
