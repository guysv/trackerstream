// Leader election over the Web Locks API.
//
// The browser is the arbiter: all tabs share one origin, so a single exclusive lock elects exactly
// one node-owner with no consensus protocol to write. This is the same pattern RxDB and wa-sqlite
// converged on for "one shared backend, N tabs" — and it's the only option here anyway, because the
// SharedWorker route can't run our WebRTC transports (RTCPeerConnection is [Exposed=Window] only).

const LEADER_LOCK = "ts:leader/v1";

/** Hold a lock until this tab is going away. `pagehide` (not `unload`) so bfcache-evicted tabs
 *  release cleanly. When it resolves, the Web Locks manager hands the lock to the next waiter — that
 *  is the failover trigger. */
function holdUntilPagehide(): Promise<void> {
  return new Promise((resolve) => {
    addEventListener("pagehide", () => resolve(), { once: true });
  });
}

/** Decide this tab's role and wire up failover.
 *
 *  Resolves `true` if this tab is the initial leader (it now holds the lock and will keep it until it
 *  closes), or `false` if a leader already exists. In the follower case, a persistent lock request is
 *  queued so that when the current leader's tab dies and releases the lock, `onPromote` fires — the
 *  follower then boots the node IN PLACE and keeps holding the lock. Holding it is what stops the
 *  next queued follower from also promoting (a reload-on-promote scheme would cascade with 3+ tabs).
 *
 *  The `ifAvailable` probe is decisive with no timeout race: the lock manager serializes concurrent
 *  requests, so when several tabs open at once exactly one sees the lock available and the rest see
 *  `null`. */
export function elect(onPromote: () => void): Promise<boolean> {
  return new Promise<boolean>((resolveRole) => {
    void navigator.locks.request(LEADER_LOCK, { mode: "exclusive", ifAvailable: true }, (lock) => {
      if (lock) {
        // We got it → we are the initial leader. Keep the lock for the tab's lifetime.
        resolveRole(true);
        return holdUntilPagehide();
      }
      // Someone else holds it → we are a follower.
      resolveRole(false);
      // Queue behind the current leader. This callback fires only when the leader releases the lock,
      // i.e. its tab closed — at which point we promote and take over leadership.
      void navigator.locks.request(LEADER_LOCK, { mode: "exclusive" }, () => {
        onPromote();
        return holdUntilPagehide();
      });
      // Release the probe immediately (returning ends this request without holding anything).
      return undefined;
    });
  });
}

/** Whether this browser can coordinate tabs at all. Every browser that supports our WebRTC/wasm
 *  stack also supports Web Locks (Chrome 69+, Firefox 96+, Safari 15.4+), but guard rather than
 *  throw: without it we fall back to the pre-coordination single-tab behavior. */
export function canElect(): boolean {
  return typeof navigator !== "undefined" && "locks" in navigator;
}
