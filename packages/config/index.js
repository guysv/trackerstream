// trackerstream — server-address config (MVP).
//
// SINGLE SOURCE OF TRUTH for the master node's literal addresses. There is no
// domain/DNS yet, so the desktop client ships these baked in (MVP shortcut,
// MVP.md "No domain yet"). Keep the indirection clean: every call site reads
// from here, never a scattered literal, so a hostname / DigitalOcean Reserved
// IP can replace these later without touching call sites.
//
// The Tauri Rust backend mirrors these values in apps/desktop/src-tauri (Rust
// can't import JS); when these change, update that mirror too. This file stays
// the canonical reference.

// The fra1 droplet (single master node).
export const MASTER_IPV4 = "165.227.155.138";
export const MASTER_IPV6 = "2a03:b0c0:3:f0:0:2:959c:b000";

// HTTP control-plane API (catalog/search/accounts/playlists/social).
export const API_PORT = 8080;
export const API_BASE_URL = `http://${MASTER_IPV4}:${API_PORT}`;

// libp2p bootstrap / DHT. The master is a Kademlia bootstrap peer and always-on
// provider for every archive CID. PeerID is filled in once the master kubo node
// is initialized (Phase 1/2) — placeholder until then.
export const LIBP2P_SWARM_PORT = 4001;
export const MASTER_PEER_ID = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL"; // fra1 master
export const BOOTSTRAP_MULTIADDRS = MASTER_PEER_ID
  ? [
      `/ip4/${MASTER_IPV4}/tcp/${LIBP2P_SWARM_PORT}/p2p/${MASTER_PEER_ID}`,
      `/ip6/${MASTER_IPV6}/tcp/${LIBP2P_SWARM_PORT}/p2p/${MASTER_PEER_ID}`,
    ]
  : [];

// STUN endpoint for NAT traversal (circuit-relay fallback for symmetric NATs).
export const STUN_PORT = 3478;
export const STUN_ENDPOINT = `${MASTER_IPV4}:${STUN_PORT}`;
