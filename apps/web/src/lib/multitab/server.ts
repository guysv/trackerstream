// The leader side of the tab RPC: serve follower requests off the real WebClient.
//
// Runs only in the tab that holds the leader lock. It listens on the shared BroadcastChannel,
// dispatches each request onto the WebClient's NodeClient surface, and ships results (and streaming
// callbacks) back addressed to the caller.
import type { WebClient } from "../client/web.ts";
import {
  RPC_CHANNEL,
  SAVE_MODULE_BYTES,
  STREAMING_ARG,
  splitMethod,
  type Cb,
  type Changed,
  type Err,
  type Hello,
  type Msg,
  type Ok,
  type Req,
} from "./protocol.ts";

/** Start serving followers off `wc`. Returns a stop function (used on teardown). */
export function startLeaderServer(wc: WebClient): () => void {
  const ch = new BroadcastChannel(RPC_CHANNEL);
  // A per-leadership id so followers can detect when leadership moves to a new tab.
  const leaderId = crypto.randomUUID();

  const announce = () => ch.postMessage({ t: "hello", leader: leaderId } satisfies Hello);

  // Fan the node's playlists:changed signal out to every follower (their local gossip ingest lives
  // here, on the leader, so this is their only source of the event).
  const off = wc.events.on("playlists:changed", () => ch.postMessage({ t: "changed" } satisfies Changed));

  ch.onmessage = (ev) => {
    const m = ev.data as Msg;
    if (m.t === "probe") {
      announce(); // a follower that booted while we were still starting the node — tell it we're up
      return;
    }
    if (m.t === "req") void handle(m);
  };

  async function handle(req: Req): Promise<void> {
    const onCb = (arg: unknown) => ch.postMessage({ t: "cb", to: req.from, id: req.id, arg } satisfies Cb);
    try {
      const value = await invoke(wc, req.method, req.args, onCb);
      ch.postMessage({ t: "ok", to: req.from, id: req.id, value } satisfies Ok);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      ch.postMessage({ t: "err", to: req.from, id: req.id, message } satisfies Err);
    }
  }

  announce(); // unblock any followers already waiting
  return () => {
    off();
    ch.close();
  };
}

/** Dispatch one method call onto the WebClient. */
async function invoke(
  wc: WebClient,
  method: string,
  args: unknown[],
  onCb: (arg: unknown) => void,
): Promise<unknown> {
  // saveModule can't run on the leader (the DOM download would land in the wrong tab), so the
  // follower asks for bytes and downloads them itself. Reassembly needs the node, so it happens here.
  if (method === SAVE_MODULE_BYTES) {
    return wc.reassembleForDownload((args[0] as { root: string }).root);
  }
  const [group, fn] = splitMethod(method);
  const target = (wc as unknown as Record<string, Record<string, (...a: unknown[]) => unknown>>)[group];
  const streamIdx = STREAMING_ARG[method];
  if (streamIdx != null) {
    // Re-insert a channel-posting stand-in where the caller's streaming callback used to be.
    const withCb = args.slice();
    withCb.splice(streamIdx, 0, onCb);
    return target[fn](...withCb);
  }
  return target[fn](...args);
}
