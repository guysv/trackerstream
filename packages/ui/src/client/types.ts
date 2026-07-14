// Wire types shared by every NodeClient implementation.
//
// These are lifted verbatim from the desktop's lib/p2p.ts + lib/catalog.ts, which defined them
// against the Tauri command surface. They are NOT Tauri-specific — they describe the node's data
// model — so they live here and both shells (Tauri-invoke, js-libp2p) speak them.
import type { PlanData } from "../audio/messages.ts";

// ---- node / peers ----

export interface NodeInfo {
  peer_id: string;
  listening: string[];
}

/** Offload role: "master" (always-on seed), "warm" (a peer-assist holder we pre-connected),
 *  or "other". Lets the peers pane prove offload. */
export type PeerRole = "master" | "warm" | "other";

export interface PeerEntry {
  id: string;
  down: number; // cumulative Bitswap bytes down from this peer
  up: number; // cumulative Bitswap bytes up to this peer
  connected: boolean;
  role: PeerRole;
}

export interface PeerStats {
  connected: number; // currently-connected count (the `peers · N` toggle)
  peers: PeerEntry[]; // connected ∪ ever-transferred (disconnected ones retained)
  // AutoNAT verdict: true = public (directly reachable), false = private (behind NAT,
  // relay/DCUtR only), null = undecided. Drives the reachability badge.
  reachable: boolean | null;
}

export interface PeerDetail {
  id: string;
  connected: boolean;
  role: PeerRole;
  warm_reason: string[]; // root CID(s) or "roster"; empty if not warm
  down: number;
  up: number;
  addrs: string[]; // live connected multiaddrs
  relayed: boolean; // every connection is via the master's circuit relay
  transport: string; // quic | tcp | webrtc-direct | relay | direct | unknown
  agent: string | null; // identify user-agent
  protocols: string[];
  observed_addr: string | null;
  rtt_ms: number | null;
}

// ---- media / streaming ----

/** Control events from a v2+ stream. Metadata ONLY — the binary skeleton and per-sample PCM are
 *  pulled separately via getSkeleton/getSample. That split is what lets the worklet stay transport
 *  agnostic: it receives bytes over postMessage and never knows whether they came from a Tauri
 *  Channel or a Bitswap block fetch. */
export type StreamEvent =
  | { type: "skeleton"; plan: PlanData; samples: number }
  | { type: "sample"; index: number; frames: number }
  | { type: "complete" }
  | { type: "error"; message: string };

/** An external tracker detected on this machine. Carries no launch path on purpose: the resolved
 *  target is a program the backend executes, so it never enters the webview (and so can't come back
 *  from it). Pass the opaque `id` to openInTracker; the backend re-resolves it. */
export interface TrackerInfo {
  id: string;
  label: string;
}

/** Where a rebuilt original landed + whether the external tracker launched. */
export interface DownloadResult {
  path: string;
  launched: boolean;
  launch_error: string | null;
}

// ---- catalog ----

export interface ModuleHit {
  id: number;
  /** Content md5 (lowercase hex) — the stable, cross-rebake key. The rowid `id` is reassigned on
   *  every full re-ingest, so persist md5 (playlists, likes), never id. */
  md5: string;
  filename: string;
  format: string;
  title: string;
  duration: number;
  channels: number;
  rootCid: string;
}

export interface ModuleDetail extends ModuleHit {
  numSamples: number;
  numInstruments: number;
  numSubsongs: number;
  sizeBytes: number;
  instruments: string;
  comment: string;
  /** TMA genre label (via the genreid->genres join); null for un-genred tracks. */
  genre: string | null;
}

export interface FormatCount {
  format: string;
  count: number;
}

export interface GenreCount {
  genreid: number;
  genre: string;
  count: number;
}

// ---- playlists ----

export type PlaylistScope = "library" | "seen" | "all";
/** The compact doc's track tuple: [content md5, module name, song title]. */
export type TrackTuple = [string, string, string];

export interface PlaylistMeta {
  name: string;
  title: string;
  tracks: number;
  isMine: boolean;
  held: boolean;
  published: boolean;
  /** No longer propagating (record expired or author tombstoned it): still playable, the UI
   *  nudges toward "duplicate to mine". */
  dormant: boolean;
  /** The author published a deletion; library copies are preserved dormant. */
  tombstoned: boolean;
  /** The private per-client "Liked Tracks" playlist — own, never published; the UI pins it and
   *  hides share/delete. */
  liked: boolean;
  sizeBytes: number;
  lastUpdateAt: number;
  lastPlayedAt: number | null;
}

export interface PlaylistItem {
  /** Content md5 — the stable key this row resolves to a CID with at play time. */
  md5: string;
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

export interface LinkStatus {
  name: string;
  /** "ready" = row present locally; "pending" = name-only link, chasing via gossip. */
  status: "ready" | "pending";
}

/** One playlist a peer discloses (held + published-mine only), joined with local library state so
 *  the UI can mark "in your library" / offer "get". */
export interface PeerPlaylistEntry {
  name: string;
  seq: number;
  title: string;
  have: boolean;
  held: boolean;
  mine: boolean;
}

export interface PeerPlaylists {
  /** false = an old build, or the seed (which does not implement playlist-list by design).
   *  A normal answer, NOT an error — the UI must render "no playlists disclosed", not a failure. */
  supported: boolean;
  playlists: PeerPlaylistEntry[];
}

// ---- platform ----

/** What the OS Now Playing widget (or navigator.mediaSession) shows. Tracker modules have no artist
 *  field, so the format stands in as the subtitle.
 *
 *  Publishing this non-null is ALSO the "first actual playback" signal: the desktop shell defers
 *  grabbing the shared, single-occupant media keys until this fires, so an idle trackerstream never
 *  silently swallows the key the user meant for Spotify. Passing null hands the slot back. */
export interface NowPlayingMeta {
  title: string;
  artist: string;
  playing: boolean;
  duration: number;
  elapsed: number;
  hasNext: boolean;
  hasPrev: boolean;
}

/** `play` and `pause` are deliberately NOT collapsed into `playpause`: the OS tells us which one it
 *  wants, and a toggle is only correct while the OS's view of our play state matches ours. The
 *  moment they drift, a `play` arriving while we already think we're playing would *pause* us. */
export type MediaKeyAction = "play" | "pause" | "playpause" | "next" | "prev";
export type Unsub = () => void;
