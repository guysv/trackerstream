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
