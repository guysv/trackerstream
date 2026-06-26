// Frontend bridge to the in-process rust-ipfs node in the Tauri backend
// (src-tauri/src/ipfs.rs). Module bytes come from CID blocks over libp2p — the
// frontend never fetches a module file over HTTP.
import { Channel, invoke } from "@tauri-apps/api/core";
import type { PlanData } from "./audio/messages";

export interface NodeInfo {
  peer_id: string;
  listening: string[];
}

// Control events from a v2 stream (binary skeleton / sample PCM pulled separately
// via getSkeleton / getSample). Mirrors ipfs::StreamEvent (serde tag "type").
export type StreamEvent =
  | { type: "skeleton"; plan: PlanData; samples: number }
  | { type: "sample"; index: number; frames: number }
  | { type: "complete" }
  | { type: "error"; message: string };

export const nodeInfo = (): Promise<NodeInfo> => invoke<NodeInfo>("node_info");

/** Offload role from the backend: "master" (always-on seed), "warm" (a peer-assist
 *  holder we pre-connected), or "other". Lets the peers pane prove offload. */
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
}

/** Snapshot of per-peer cumulative up/down bytes + connected state (peers pane). */
export const peerStats = (): Promise<PeerStats> => invoke<PeerStats>("peer_stats");

export const connectPeer = (addr: string): Promise<void> => invoke("connect_peer", { addr });

/** Queue-driven pre-connection: ask the tracker who holds `root` and warm-connect
 *  them BEFORE playback reaches it, so Bitswap finds the blocks on already-connected
 *  peers and the master is bypassed. Fire-and-forget; degrades to the master on any
 *  failure. Call when a root ENTERS the queue (not when it plays). */
export const warmRoot = (root: string): Promise<void> => invoke("warm_root", { root });

/** Pin a persistent, auto-reconnecting connection to the master (call once at
 *  startup with the bootstrap addrs) so it isn't dialed lazily + pruned when idle. */
export const keepaliveMaster = (addrs: string[]): Promise<void> =>
  invoke("keepalive_master", { addrs });

/** Resolve a v1 root -> exact module bytes (ArrayBuffer). v2 roots stream instead. */
export const fetchModule = (root: string): Promise<ArrayBuffer> =>
  invoke<ArrayBuffer>("fetch_module", { root });

/** Begin a v2 stream; `onEvent` ticks Skeleton -> Sample… -> Complete. */
export function startStream(root: string, onEvent: (e: StreamEvent) => void): Promise<void> {
  const ch = new Channel<StreamEvent>();
  ch.onmessage = onEvent;
  return invoke("start_stream", { root, onEvent: ch });
}

/** Assembled skeleton bytes (init the immortal instance). Ready after Skeleton. */
export const getSkeleton = (root: string): Promise<ArrayBuffer> =>
  invoke<ArrayBuffer>("get_skeleton", { root });

/** One streamed sample's decoded PCM. Ready after its Sample event. */
export const getSample = (root: string, index: number): Promise<ArrayBuffer> =>
  invoke<ArrayBuffer>("get_sample", { root, index });

/** Push the live playhead order so the prefetch scheduler reprioritizes (closed
 *  loop; also reseeds the queue on seek). Fire-and-forget. */
export const setPlayhead = (root: string, order: number): Promise<void> =>
  invoke("set_playhead", { root, order });
