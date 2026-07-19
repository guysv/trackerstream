// Boot the right NodeClient for this tab.
//
// Elect a leader over Web Locks: the winner boots the real js-libp2p node (WebClient) and serves the
// rest; everyone else gets a FollowerClient that proxies to it. On failover a follower promotes in
// place. This is the whole reason two tabs no longer fight over one PeerId — exactly one node runs.
import type { NodeClient } from "@trackerstream/ui/client";
import { WebClient } from "../client/web.ts";
import { canElect, elect } from "./election.ts";
import { FollowerClient } from "./follower.ts";
import { startLeaderServer } from "./server.ts";

/** Elect, then return this tab's NodeClient. The leader also starts serving followers. */
export async function bootClient(): Promise<NodeClient> {
  // A bfcache restore (back/forward) resurrects a FROZEN page, and none of its coordination state
  // survives usefully: the leader's node is dead (webrtc does not survive a freeze) yet its Web Lock
  // was released on pagehide, so it is a phantom leader holding no lock; a follower's BroadcastChannel
  // proxy points at a leader that may be long gone. In-place repair is fragile — reload to re-run
  // election from a clean slate. `persisted` is true ONLY on a bfcache restore, so a normal load never
  // pays this. Registered before election so it is armed for the life of the page.
  addEventListener("pageshow", (e) => {
    if ((e as PageTransitionEvent).persisted) location.reload();
  });

  // No Web Locks: fall back to the pre-coordination behavior (every tab a standalone node). Only
  // ancient browsers land here, and they can't run our WebRTC stack anyway.
  if (!canElect()) return WebClient.create();

  let follower: FollowerClient | null = null;
  const isLeader = await elect(() => void follower?.promote());

  if (isLeader) {
    const wc = await WebClient.create();
    startLeaderServer(wc);
    return wc;
  }

  // Follower path. Assigned synchronously before any await, so the promotion hook above can't fire
  // against a null (promotion only happens on leader death, long after this).
  follower = new FollowerClient();
  await follower.waitReady();
  return follower;
}
