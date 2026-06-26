// Peer-assist tracker: a presence-first, BitTorrent-shaped coordination service.
// In-memory only (presence is ephemeral, heartbeat-driven) — no DB. Two views
// over the same announces:
//   Presence  peerId -> { addrs, heldRoots, lastSeen }   (the backbone)
//   Interest  rootCid -> Set<peerId>                       (the offload index)
// Clients POST /announce every ~30s (doubles as heartbeat); GET /peers?root=X
// returns a bounded random subset of holders to warm-connect. A sweep drops rows
// that stop heartbeating. See PEER-ASSIST.md §2.2.

export interface PresenceEntry {
  addrs: string[];
  heldRoots: string[];
  lastSeen: number;
}

export interface PeerRef {
  peerId: string;
  addrs: string[];
}

export class Tracker {
  private presence = new Map<string, PresenceEntry>();
  private interest = new Map<string, Set<string>>();

  /** Upsert a peer's presence + re-index the roots it holds. Returns lastSeen. */
  announce(peerId: string, addrs: string[], heldRoots: string[], now = Date.now()): number {
    const prev = this.presence.get(peerId);
    if (prev) this.deindex(peerId, prev.heldRoots);
    this.presence.set(peerId, { addrs, heldRoots, lastSeen: now });
    for (const root of heldRoots) {
      let set = this.interest.get(root);
      if (!set) this.interest.set(root, (set = new Set()));
      set.add(peerId);
    }
    return now;
  }

  /** Bounded random subset of peers holding `root` (excluding `self` if given). */
  peers(root: string, limit = 50, self?: string): PeerRef[] {
    const holders = this.interest.get(root);
    if (!holders) return [];
    const ids: string[] = [];
    for (const id of holders) if (id !== self) ids.push(id);
    // Bounded random subset: partial Fisher-Yates over the first `limit` slots.
    for (let i = 0; i < Math.min(limit, ids.length); i++) {
      const j = i + Math.floor(Math.random() * (ids.length - i));
      [ids[i], ids[j]] = [ids[j], ids[i]];
    }
    return ids.slice(0, limit).map((peerId) => ({
      peerId,
      addrs: this.presence.get(peerId)?.addrs ?? [],
    }));
  }

  /** Presence-only view (who's online) — the social-backbone seam (Phase 4). */
  roster(): Array<{ peerId: string; addrs: string[]; lastSeen: number }> {
    return [...this.presence.entries()].map(([peerId, p]) => ({
      peerId,
      addrs: p.addrs,
      lastSeen: p.lastSeen,
    }));
  }

  /** Drop peers that haven't heartbeated within `ttlMs`, with their interest rows. */
  sweep(ttlMs: number, now = Date.now()): number {
    let dropped = 0;
    for (const [peerId, p] of this.presence) {
      if (now - p.lastSeen > ttlMs) {
        this.deindex(peerId, p.heldRoots);
        this.presence.delete(peerId);
        dropped++;
      }
    }
    return dropped;
  }

  /** { peers, roots } counts — for /healthz visibility. */
  stats(): { peers: number; roots: number } {
    return { peers: this.presence.size, roots: this.interest.size };
  }

  private deindex(peerId: string, roots: string[]): void {
    for (const root of roots) {
      const set = this.interest.get(root);
      if (!set) continue;
      set.delete(peerId);
      if (set.size === 0) this.interest.delete(root);
    }
  }
}
