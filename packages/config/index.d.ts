// Types for the server-address config (see index.js).
export const MASTER_HOST: string;
export const MASTER_IPV4: string;
export const MASTER_IPV6: string;
export const API_BASE_URL: string;
export const LIBP2P_SWARM_PORT: number;
export const MASTER_PEER_ID: string;
export const BOOTSTRAP_MULTIADDRS: string[];
/** Browser bootstrap discovery (see index.js): fetch on boot AND re-fetch on any dial failure. */
export const BOOTSTRAP_URL: string;
export const STUN_PORT: number;
export const STUN_ENDPOINT: string;
export const CATALOG_IPNS_KEY: string;
export const CATALOG_Z_IPNS_KEY: string;
