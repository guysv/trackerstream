// Playlists client (PLAYLISTS.md): thin wrappers over the node's playlist surface plus the
// play/add/save helpers the components share. The durable store lives BELOW this layer — Rust's
// playlists.db on desktop, wa-sqlite over OPFS in the browser — and this layer is stateless except
// for a bump counter that tells views to refresh after a mutation.
import { client } from "./client/index.ts";
import { getModuleByMd5 } from "./catalog.ts";
import { cachedHit, rememberHit, rememberHits } from "./cidCache.ts";
import { playList, queue, setOnQueueSourceChange } from "./player.svelte.ts";
import { logWarn } from "./debug.ts";
import type { ModuleHit } from "./client/types.ts";

export type {
  LinkStatus,
  PlaylistDetail,
  PlaylistItem,
  PlaylistMeta,
  PlaylistScope,
  PlaylistSyncStatus,
  TrackTuple,
} from "./client/types.ts";
import type {
  LinkStatus,
  PlaylistDetail,
  PlaylistItem,
  PlaylistMeta,
  PlaylistScope,
  PlaylistSyncStatus,
  TrackTuple,
} from "./client/types.ts";

export const plSearch = (q: string): Promise<PlaylistMeta[]> => client().playlists.search(q);
export const plList = (scope: PlaylistScope = "all"): Promise<PlaylistMeta[]> =>
  client().playlists.list(scope);
/** Add/remove a foreign playlist to/from the library — the "holder" tier: backed
 * (re-announced), never evicted. Distinct from duplicate (fork) and delete. */
export const plHold = (name: string, held: boolean): Promise<void> =>
  client().playlists.hold(name, held);
export const plGet = (name: string): Promise<PlaylistDetail | null> => client().playlists.get(name);
export const plCreate = (title: string, tracks: TrackTuple[]): Promise<PlaylistMeta> =>
  client().playlists.create(title, tracks);
export const plUpdate = (name: string, title: string, tracks: TrackTuple[]): Promise<void> =>
  client().playlists.update(name, title, tracks);
export const plDelete = (name: string): Promise<void> => client().playlists.remove(name);
/** The explicit "Share" action — the only thing that makes a playlist public. */
export const plPublish = (name: string): Promise<void> => client().playlists.publish(name);
/** "Make private again": best-effort tombstone retract, keep the data local (published=0). */
export const plUnpublish = (name: string): Promise<void> => client().playlists.unpublish(name);
export const plStatus = () => client().playlists.syncStatus();

// ---- liked tracks (the private "Liked Tracks" playlist — Spotify-style ♥) ----

/** Toggle a track in "Liked Tracks" (creates the private playlist on first use).
 * Returns true if the track is now liked. */
export const plLikeToggle = (track: TrackTuple): Promise<boolean> =>
  client().playlists.likeToggle(track);
/** The track md5s currently liked — what the heart buttons check membership against. */
export const plLikedIds = (): Promise<string[]> => client().playlists.likedIds();
/** The liked playlist's name (creating it if needed) — for opening it in the view. */
export const plLikedName = (): Promise<string> => client().playlists.likedName();

// ---- deep links ----

/** Ingest an incoming playlist link (payload verified in Rust; name-only pends). */
export const plIngestLink = (url: string): Promise<LinkStatus> => client().playlists.ingestLink(url);
/** Shareable HTTPS link for a stored playlist (errors if it has no live record). */
export const plCopyLink = (name: string): Promise<string> => client().playlists.copyLink(name);
/** Names from name-only links still waiting on gossip (the "syncing…" placeholder). */
export const plPending = (): Promise<string[]> => client().playlists.pending();

/** Hold-beacon backer counts (windowed distinct holders) for the given names.
 * 0 = no beacon heard — normal for the first hour after startup. */
export const plBackers = (names: string[]): Promise<Record<string, number>> =>
  client().playlists.backers(names);

export const hitTuple = (h: ModuleHit): TrackTuple => [h.md5, h.filename, h.title];
export const itemTuples = (items: PlaylistItem[]): TrackTuple[] =>
  items.map((i) => [i.md5, i.modName, i.title]);

/** Mutation counter: components re-query when this bumps. */
export const plState = $state({ version: 0 });
export const plBump = () => plState.version++;

// The set of track md5s in the private "Liked Tracks" playlist. Every ♥ button across
// the UI reads this reactively; kept warm by refreshLiked() on the sync cadence and
// flipped optimistically on toggle so the heart reacts instantly.
export const liked = $state<{ ids: Set<string> }>({ ids: new Set() });
export const isLiked = (md5: string): boolean => liked.ids.has(md5);

export async function refreshLiked(): Promise<void> {
  try {
    liked.ids = new Set(await plLikedIds());
  } catch (e) {
    logWarn("playlists:refreshLiked", e); // keep the last known set on a transient failure
  }
}

/** Toggle a track's membership in "Liked Tracks". Reassigns the reactive set so the
 * heart flips without waiting for the next refresh. */
export async function toggleLike(h: ModuleHit): Promise<void> {
  const nowLiked = await plLikeToggle(hitTuple(h));
  const ids = new Set(liked.ids);
  if (nowLiked) ids.add(h.md5);
  else ids.delete(h.md5);
  liked.ids = ids;
  plBump();
}

// Wire the playlist store's live signals. Called by the shell AFTER setClient() — deliberately not
// a module-level side effect: these touch client(), and module init runs at IMPORT time, before any
// shell has installed a node. An import-time client() would throw on the very first component that
// pulls this module in.
//
// The node's sync loop writes into the playlist store behind the UI's back, and emits a coalesced
// `playlists:changed` whenever a tick actually changed it (synced/updated/tombstoned playlist,
// budget eviction, or seen-tier decay), so the UI reconciles the instant the network moves. Local
// mutations keep bumping plState directly (optimistic + instant); this is the SAME channel for
// network events.
export function initPlaylists(): () => void {
  void refreshLiked();
  const off = client().events.on("playlists:changed", () => {
    plBump();
    void refreshLiked();
  });
  // Backstop only: a slow reconcile in case an emit is ever missed. Queries run only while playlist
  // components are mounted, and they're local DB reads, so a 60s catch-all is cheap.
  const timer = setInterval(() => {
    plBump();
    void refreshLiked(); // catches unlikes done from the playlist detail (splice + update)
  }, 60_000);

  // Play-time pin: while the queue is sourced from a playlist, the node protects that row from
  // decay / budget eviction / tombstone deletion so it can't vanish under playback (a transient pin
  // — NOT the held tier; nothing is backed or re-announced). Playing anything else, or clearing the
  // queue, releases it.
  setOnQueueSourceChange(
    () => void client().playlists.pin(null).catch((e) => logWarn("playlist_pin:release", e)),
  );

  return () => {
    off();
    clearInterval(timer);
  };
}

// Playlist docs carry no root CIDs (by design — tracks resolve via the catalog only at
// play time), so playing a playlist resolves entries through `getModule` first. Bounded
// like the results table: a >200-track playlist plays its first 200.
const RESOLVE_CAP = 200;

// A playlist item carries no CID, only md5 + display strings. Online we resolve the current
// hit through the catalog (which also refreshes the cache); when the catalog is unreachable
// we fall back to the last-known-good hit this md5 resolved to (cidCache.ts) — its CID drives
// playback and its metadata (format/duration/title) drives the offline display.
async function resolveItem(i: PlaylistItem): Promise<ModuleHit | null> {
  // Catalog unreachable / row gone falls back to the cache below — expected offline, so this
  // is a warn (filtered by TS_LOG), not an error; it fires per unresolved track.
  const d = await getModuleByMd5(i.md5).catch((e) => (logWarn("playlists:resolveItem", e, { md5: i.md5 }), null));
  if (d?.rootCid) return d;
  return cachedHit(i.md5) ?? null; // catalog unreachable / row gone — use the last hit that worked
}

export async function playPlaylist(detail: PlaylistDetail, index = 0): Promise<void> {
  const slice = detail.items.slice(0, RESOLVE_CAP);
  const wantedMd5 = detail.items[Math.min(index, slice.length - 1)]?.md5;
  const hits = (await Promise.all(slice.map(resolveItem))).filter(Boolean) as ModuleHit[];
  if (!hits.length) throw new Error("no playable tracks in playlist");
  const at = Math.max(0, hits.findIndex((h) => h.md5 === wantedMd5));
  playList(hits, at); // fires the source-change hook first, releasing any prior pin
  void client().playlists.pin(detail.name).catch((e) => logWarn("playlist_pin:set", e));
  // Bump only after the mark lands, so the library re-query reads the fresh
  // last_played_at and the just-played playlist visibly rises to the top of its tier.
  void client()
    .playlists.played(detail.name)
    .then(plBump)
    .catch((e) => logWarn("playlist_played", e));
}

/** Append a track to one of my playlists (no-op if it's already in). */
export async function addTrackTo(name: string, h: ModuleHit): Promise<void> {
  const d = await plGet(name);
  if (!d) return;
  const tuples = itemTuples(d.items);
  if (!tuples.some((t) => t[0] === h.md5)) tuples.push(hitTuple(h));
  rememberHit(h); // this track is now offline-playable + displayable from this hit
  await plUpdate(name, d.title, tuples);
  // If this is the private Liked Tracks playlist, flip the ♥ optimistically (reassign
  // the reactive set) instead of waiting on the 10s refreshLiked() poll — mirrors toggleLike().
  if (d.liked && !liked.ids.has(h.md5)) liked.ids = new Set(liked.ids).add(h.md5);
  plBump();
}

export async function saveQueueAsPlaylist(title: string): Promise<void> {
  rememberHits(queue.items); // the queued hits carry live CIDs — keep them playable offline
  await plCreate(title, queue.items.map(hitTuple));
  plBump();
}

/** Copy a foreign playlist into my own editable one. */
export async function duplicatePlaylist(detail: PlaylistDetail): Promise<void> {
  await plCreate(`${detail.title || "playlist"} (copy)`, itemTuples(detail.items));
  plBump();
}
