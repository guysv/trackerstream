// Live per-peer bandwidth for the peers pane + the `peers · N` toggle. Polls the
// embedded node's `peer_stats` (~1/s) and derives per-peer up/down SPEED from the
// cumulative byte-counter deltas (the backend exposes totals). The backend returns
// the union of connected peers and any peer that ever transferred, so a peer that
// did up/down then dropped stays in the list (connected:false → grayed) with its
// totals intact, and continues from them on reconnect. Polling runs for the app's
// lifetime so the toggle count stays live with the pane closed.
import { peerStats, type PeerRole } from "./p2p";

export interface PeerRow {
  id: string;
  down: number;
  up: number;
  connected: boolean;
  role: PeerRole;
  speedDown: number; // bytes/sec over the last poll interval
  speedUp: number;
}

export const peers = $state<{
  connected: number; // currently-connected count (toggle)
  rows: PeerRow[];
  totalDown: number;
  totalUp: number;
  speedDown: number; // aggregate over all peers
  speedUp: number;
  // Offload proof: cumulative down bytes from NON-master peers. >0 means blocks
  // came from a peer, not the master (PEER-ASSIST.md §B6 / verification).
  offloadDown: number;
}>({ connected: 0, rows: [], totalDown: 0, totalUp: 0, speedDown: 0, speedUp: 0, offloadDown: 0 });

let timer: ReturnType<typeof setInterval> | null = null;
let prev = new Map<string, { down: number; up: number }>();
let lastT = 0;

async function tick(): Promise<void> {
  try {
    const s = await peerStats();
    const now = Date.now();
    const dt = lastT ? (now - lastT) / 1000 : 0;
    let totalDown = 0;
    let totalUp = 0;
    let aggDown = 0;
    let aggUp = 0;
    let offloadDown = 0;
    const rows: PeerRow[] = s.peers.map((p) => {
      const was = prev.get(p.id);
      // max(0, …): totals only grow; guard a node-restart counter reset.
      const speedDown = was && dt > 0 ? Math.max(0, (p.down - was.down) / dt) : 0;
      const speedUp = was && dt > 0 ? Math.max(0, (p.up - was.up) / dt) : 0;
      totalDown += p.down;
      totalUp += p.up;
      aggDown += speedDown;
      aggUp += speedUp;
      if (p.role !== "master") offloadDown += p.down;
      return {
        id: p.id,
        down: p.down,
        up: p.up,
        connected: p.connected,
        role: p.role,
        speedDown,
        speedUp,
      };
    });
    // Connected first, then most-transferred first, then peer id as a STABLE
    // tiebreaker. Without the id tiebreaker, peers with equal totals (e.g. warm
    // roster peers at 0 bytes) keep the backend's order, which is a Rust HashSet
    // iteration — randomized per poll — so the list reshuffles every second.
    rows.sort(
      (a, b) =>
        Number(b.connected) - Number(a.connected) ||
        b.down + b.up - (a.down + a.up) ||
        a.id.localeCompare(b.id),
    );
    prev = new Map(s.peers.map((p) => [p.id, { down: p.down, up: p.up }]));
    lastT = now;
    peers.connected = s.connected;
    peers.rows = rows;
    peers.totalDown = totalDown;
    peers.totalUp = totalUp;
    peers.speedDown = aggDown;
    peers.speedUp = aggUp;
    peers.offloadDown = offloadDown;
  } catch {
    // Node not ready yet — keep the last snapshot, retry next tick.
  }
}

/** Begin polling; returns a stop fn. Safe to call once on app mount. */
export function startPeerPolling(intervalMs = 1000): () => void {
  stopPeerPolling();
  void tick();
  timer = setInterval(() => void tick(), intervalMs);
  return stopPeerPolling;
}

export function stopPeerPolling(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
