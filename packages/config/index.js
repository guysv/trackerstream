// trackerstream — server-address config.
//
// SINGLE SOURCE OF TRUTH for the master node's addresses. The desktop client
// ships these baked in; every call site reads from here, never a scattered
// literal, so the hostname / a DigitalOcean Reserved IP can replace the
// underlying machine without touching call sites.
//
// Addressing is now DNS-based (MVP-FOLLOWUP A1 + D1): `trackerstream.xyz`
// resolves (A/AAAA) to the master droplet, so both planes survive an IP change
// once DNS is updated (point the domain at a Reserved IP and a droplet rebuild
// needs nothing here). The literal IPs below are kept only as reference/fallback.
//
// The Tauri Rust backend does NOT mirror these: the Svelte frontend passes the
// bootstrap multiaddr to Rust via the `connect_peer` command, so this file is
// the only place addresses live.

// Canonical hostname for the master node (A + AAAA -> the droplet / Reserved IP).
export const MASTER_HOST = "trackerstream.xyz";

// Literal addresses of the master box (Hetzner nbg1 — reference/fallback only;
// prefer the DNS name above so an IP swap doesn't require a client rebuild).
export const MASTER_IPV4 = "5.75.131.145";
export const MASTER_IPV6 = "2a01:4f8:1c1f:9120::1";

// The Node.js control-plane API was retired with the Go cutover: catalog/search and IPNS
// distribution moved entirely into libp2p (custom DHT + gossipsub on tsnode), so the client
// makes no HTTPS request to the box. The domain still resolves (DNS A/AAAA) for the /dns4
// bootstrap multiaddr below; Caddy keeps a static 200 on :443 for the cert/uptime probe only.
export const API_BASE_URL = `https://${MASTER_HOST}`;

// libp2p bootstrap. The master is an always-on provider for every archive CID.
// The client tries EVERY entry until one connects (player.ensureConnected).
// /dns4 + /dns6 are listed FIRST and are the durable path: the embedded node now
// enables the DNS transport (src-tauri/src/ipfs.rs `.enable_dns()`), so it resolves
// trackerstream.xyz at dial time and a master IP change needs NO client rebuild.
// The literal /ip4 + /ip6 entries follow as a fallback only (used if DNS is
// momentarily unavailable); keep them pointed at the current master.
export const LIBP2P_SWARM_PORT = 4001;
export const MASTER_PEER_ID = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL"; // master identity (stable across IP moves)
export const BOOTSTRAP_MULTIADDRS = MASTER_PEER_ID
  ? [
      `/dns4/${MASTER_HOST}/tcp/${LIBP2P_SWARM_PORT}/p2p/${MASTER_PEER_ID}`,
      `/dns4/${MASTER_HOST}/udp/${LIBP2P_SWARM_PORT}/quic-v1/p2p/${MASTER_PEER_ID}`,
      `/dns6/${MASTER_HOST}/tcp/${LIBP2P_SWARM_PORT}/p2p/${MASTER_PEER_ID}`,
      `/dns6/${MASTER_HOST}/udp/${LIBP2P_SWARM_PORT}/quic-v1/p2p/${MASTER_PEER_ID}`,
      `/ip4/${MASTER_IPV4}/tcp/${LIBP2P_SWARM_PORT}/p2p/${MASTER_PEER_ID}`,
      `/ip4/${MASTER_IPV4}/udp/${LIBP2P_SWARM_PORT}/quic-v1/p2p/${MASTER_PEER_ID}`,
      `/ip6/${MASTER_IPV6}/tcp/${LIBP2P_SWARM_PORT}/p2p/${MASTER_PEER_ID}`,
      `/ip6/${MASTER_IPV6}/udp/${LIBP2P_SWARM_PORT}/quic-v1/p2p/${MASTER_PEER_ID}`,
    ]
  : [];

// STUN/TURN endpoint for NAT traversal (coturn; circuit-relay v2 + DCUtR are the
// primary path, this is the symmetric-NAT fallback). Hostname-based for durability.
export const STUN_PORT = 3478;
export const STUN_ENDPOINT = `${MASTER_HOST}:${STUN_PORT}`;

// IPNS name (base58 PeerId, `12D3…`) the master publishes the catalog SQLite DB
// under (R1). Generated ONCE on the master with kubo `key gen catalog` — the ingest
// publish step prints the PeerId on first run; paste it here and ship a client build.
// The client resolves /ipns/<this> and verifies the record's signature against it, so
// it MUST be the base58 (`b58mh`) form, matching what the ingest stores it under.
// Empty -> the client catalog-over-IPNS path stays dormant (HTTP /catalog still works).
export const CATALOG_IPNS_KEY = "12D3KooWDb53qFZvANj5kDCr3riMhT2HJG32i5xqFKhvBtzh7wPC";
