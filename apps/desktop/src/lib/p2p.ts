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

export const connectPeer = (addr: string): Promise<void> => invoke("connect_peer", { addr });

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
