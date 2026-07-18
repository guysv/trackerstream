// The tab-to-tab RPC wire.
//
// Two tabs on one origin share IndexedDB and, critically, our persisted PeerId (ts:hostkey). Two
// live libp2p nodes under one identity is a split-brain: relay reservations, DHT provider records
// and playlist beacons are all keyed by PeerId, so the two tabs would overwrite each other's
// network state and offload dials would land nondeterministically on either one. So exactly ONE tab
// (the Web-Locks leader) runs the node; the others are UI-only followers that call the leader's
// NodeClient over a BroadcastChannel.
//
// The boundary is the NodeClient interface itself, NOT the libp2p/helia/bitswap objects: those never
// escape WebClient, and the whole UI already talks only to NodeClient. So a request names a method
// path ("catalog.searchStream") and carries its args; the leader dispatches it onto its real
// WebClient and ships the result back.

/** BroadcastChannel name. Versioned so a future wire change can't have a stale tab answer with the
 *  wrong shape mid-deploy. */
export const RPC_CHANNEL = "ts:node-rpc/v1";

/** Methods whose signature includes a streaming callback, mapped to the callback's ARG INDEX in the
 *  original signature. The callback can't cross a BroadcastChannel (functions aren't cloneable), so
 *  the follower strips it, the leader re-inserts a channel-posting stand-in at this index, and each
 *  invocation is relayed back as a `cb` message. */
export const STREAMING_ARG: Record<string, number> = {
  "catalog.searchStream": 1, // (opts, onRow)
  "media.startStream": 1, // (root, onEvent)
};

// ----- messages -----

/** Follower -> leader: invoke a method. `from` is the caller's id so the leader can address replies;
 *  `id` correlates the reply. `args` has any streaming callback removed (see STREAMING_ARG). */
export interface Req {
  t: "req";
  from: string;
  id: number;
  method: string;
  args: unknown[];
}

/** Leader -> follower: one invocation of a streaming callback (searchStream row / startStream event). */
export interface Cb {
  t: "cb";
  to: string;
  id: number;
  arg: unknown;
}

/** Leader -> follower: the method resolved. */
export interface Ok {
  t: "ok";
  to: string;
  id: number;
  value: unknown;
}

/** Leader -> follower: the method threw. Only the message survives the boundary (Error isn't usefully
 *  cloneable), which is enough — the UI shows a message, it doesn't branch on error types. */
export interface Err {
  t: "err";
  to: string;
  id: number;
  message: string;
}

/** Follower -> everyone: "is there a ready leader?" Sent at boot and re-sent until one answers, so a
 *  follower that started while the leader was still booting its node gets unblocked. */
export interface Probe {
  t: "probe";
  from: string;
}

/** Leader -> everyone: "I am the ready leader." Announced when the leader's server starts and in
 *  reply to any probe. A follower that sees the `leader` id change knows the leadership moved (the
 *  old tab died and a new one promoted) and fails its in-flight requests. */
export interface Hello {
  t: "hello";
  leader: string;
}

/** Leader -> everyone: playlists changed under the node (gossip ingest). Fans the desktop's
 *  event-bus signal out to every follower. */
export interface Changed {
  t: "changed";
}

export type Msg = Req | Cb | Ok | Err | Probe | Hello | Changed;

/** Split "group.fn" into ["group", "fn"]. Method names have exactly one dot (node.info,
 *  catalog.searchStream, …). */
export function splitMethod(method: string): [string, string] {
  const i = method.indexOf(".");
  return [method.slice(0, i), method.slice(i + 1)];
}

/** Special method (not on NodeClient): reassemble a module's bytes for a follower to download in its
 *  own tab. saveModule can't be a plain passthrough — the leader running the DOM download would drop
 *  the file into a background tab — so the leader returns bytes and the follower does the Blob. */
export const SAVE_MODULE_BYTES = "media.saveModuleBytes";
