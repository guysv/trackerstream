// Frontend bridge to the in-process rust-ipfs node in the Tauri backend
// (src-tauri/src/ipfs.rs). Module bytes come from CID blocks over libp2p — the
// frontend never fetches a module file over HTTP.
import { Channel, invoke } from "@tauri-apps/api/core";

export interface NodeInfo {
  peer_id: string;
  listening: string[];
}

export interface StreamProgress {
  version: number;
  pct: number;
  playable: boolean;
  complete: boolean;
}

export const nodeInfo = (): Promise<NodeInfo> => invoke<NodeInfo>("node_info");

export const connectPeer = (addr: string): Promise<void> => invoke("connect_peer", { addr });

/** Resolve a module root CID -> exact module bytes (ArrayBuffer), 100% P2P. */
export const fetchModule = (root: string): Promise<ArrayBuffer> =>
  invoke<ArrayBuffer>("fetch_module", { root });

/** Begin a progressive fetch; `onProgress` ticks as the partial buffer grows. */
export function startStream(root: string, onProgress: (p: StreamProgress) => void): Promise<void> {
  const ch = new Channel<StreamProgress>();
  ch.onmessage = onProgress;
  return invoke("start_stream", { root, onProgress: ch });
}

/** Current bytes of an in-flight/finished streaming fetch. */
export const getStreamBuffer = (root: string): Promise<ArrayBuffer> =>
  invoke<ArrayBuffer>("get_stream_buffer", { root });

/**
 * Cold seek (B1): resolve a module to a partial buffer playable at `order` —
 * skeleton + only that seek target's resident sample set (far smaller than the
 * whole DAG, lab-measured ~3x faster time-to-playback). Load the returned bytes
 * and call set_position(order); normal streaming then fills the rest. Falls back
 * to a full fetch for modules without a seek table. UX wiring (mapping a seek-bar
 * position in seconds to an order via the manifest timing map, then a worklet
 * set_position) is the remaining integration step.
 */
export const seekModule = (root: string, order: number): Promise<ArrayBuffer> =>
  invoke<ArrayBuffer>("seek_module", { root, order });
