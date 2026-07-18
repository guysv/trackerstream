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
