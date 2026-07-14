// Local "last known good" md5 -> catalog hit cache.
//
// Playlist docs store only a track's content md5 (stable across corpus rebakes), never a
// CID or metadata — so playing/showing a saved playlist resolves each md5 through the
// catalog DB over the Bitswap VFS (catalog.ts::getModuleByMd5). That resolution needs the
// network: when the catalog is unreachable the lookup fails, the track is dropped as
// unplayable, and we have no title/format/duration to show either.
//
// This cache remembers the full catalog hit (root CID + display metadata) the catalog ever
// hands us, keyed by md5, so the player can fall back to the last hit that worked when
// resolution fails — playing from the last-known-good CID and displaying real track info
// offline. It is purely local — persisted in localStorage exactly like the play queue
// (ts.queue) and NEVER advertised or written into the (published) playlist doc. Losing it
// just means you must be online once to repopulate it; playlists themselves are untouched.
import type { ModuleHit } from "./catalog";

const KEY = "ts.cidcache";
// Enough for a heavy library many times over; beyond this the oldest-touched entries are
// evicted so the blob can't grow without bound. Hits are small (no instrument/comment blobs).
const CAP = 8000;

// The lean hit shape (no instruments/comment) plus a touch timestamp for LRU eviction.
type Entry = { hit: ModuleHit; at: number };
type Store = Record<string, Entry>;

// Trim any ModuleDetail down to the hit fields we persist (mirrors player.svelte's toHit) so
// we never stash the heavy instruments/comment strings.
function lean(h: ModuleHit): ModuleHit {
  return {
    id: h.id,
    md5: h.md5,
    filename: h.filename,
    format: h.format,
    title: h.title,
    duration: h.duration,
    channels: h.channels,
    rootCid: h.rootCid,
  };
}

// Loaded once, mutated in place, written through on change. `null` until first access (and
// re-null on environments without localStorage, e.g. SSR prerender, so it retries later).
let mem: Store | null = null;

function load(): Store {
  if (mem) return mem;
  if (typeof localStorage === "undefined") return {};
  try {
    const s = JSON.parse(localStorage.getItem(KEY) ?? "");
    mem = s && typeof s === "object" ? (s as Store) : {};
  } catch {
    mem = {};
  }
  return mem;
}

function persist(s: Store): void {
  if (typeof localStorage === "undefined") return;
  const keys = Object.keys(s);
  if (keys.length > CAP) {
    // Evict oldest-touched first so a live playlist's recently-resolved hits survive.
    keys.sort((a, b) => s[a].at - s[b].at);
    for (const k of keys.slice(0, keys.length - CAP)) delete s[k];
  }
  try {
    localStorage.setItem(KEY, JSON.stringify(s));
  } catch {
    /* storage full / headless — a miss just falls back to online resolution */
  }
}

/** Record a single catalog hit (write-through). Requires md5 + rootCid. */
export function rememberHit(h: ModuleHit): void {
  if (!h?.md5 || !h.rootCid) return;
  const s = load();
  s[h.md5] = { hit: lean(h), at: Date.now() };
  persist(s);
}

/** Record a batch of catalog hits in one write (search/list/queue results). */
export function rememberHits(hits: ModuleHit[]): void {
  if (!hits?.length) return;
  const s = load();
  const now = Date.now();
  let changed = false;
  for (const h of hits) {
    if (h?.md5 && h.rootCid) {
      s[h.md5] = { hit: lean(h), at: now };
      changed = true;
    }
  }
  if (changed) persist(s);
}

/** The last catalog hit this md5 resolved to (CID + display metadata), if any. */
export function cachedHit(md5: string): ModuleHit | undefined {
  return load()[md5]?.hit;
}
