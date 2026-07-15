// Package tsnode is the trackerstream custom IPFS node — go-libp2p (host) + boxo
// (bitswap / unixfs / ipns / blockstore), assembled by us. The same binary runs as
// the server master (bootstrap + seeder) and the desktop client sidecar, with custom
// protocols (private DHT + IPNS-over-pubsub) we can set because it's our own binary.
package tsnode

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/protocol"
	mh "github.com/multiformats/go-multihash"
)

// Custom protocol / topic identifiers. The DHT prefix yields `/trackerstream/kad/1.0.0`
// (a private routing table — libp2p only routes to peers speaking the matching protocol,
// so no public-IPFS crawl). The catalog topic carries the signed IPNS record push-style.
//
// EVOLUTION POLICY (wire-version hardening): DHTPrefix is the network's name AND the routing
// partition key — it is IMMORTAL; changing it forks the network and strands every existing
// install (same for the master identity). Every OTHER topic/protocol below is frozen by
// default: these strings are matched by EXACT string (multistream-select for streams, topic
// name for pubsub) with no runtime negotiation, so flipping one to a new /x.y.z silently
// partitions every peer still on the old string. When one genuinely must evolve, DUAL-REGISTER
// (old + new simultaneously) via registerStreamHandler / subscribeTopics and retire the old ID
// only after AgentVersion telemetry shows the old version is gone — NEVER a hard flip. A flip
// with no overlap window is a release-blocking bug.
const (
	DHTPrefix    protocol.ID = "/trackerstream"
	CatalogTopic             = "/trackerstream/catalog/1.0.0"
	// PlaylistTopic carries {name, signed IPNS record, playlist doc} envelopes — the doc
	// travels INLINE (no bitswap fetch path exists for playlists); see playlist.go.
	PlaylistTopic = "/trackerstream/playlist/1.0.0"
	// PlaylistListProtocol is the direct "what playlists do you hold?" request/response
	// stream (disclosure set = held + published-mine only; see playlistlist.go).
	PlaylistListProtocol protocol.ID = "/trackerstream/playlist-list/1.0.0"
	// PlaylistBeaconTopic carries hourly hold beacons — truncated name-hashes of each
	// client's disclosure set, counted locally for popularity (see beacon.go).
	PlaylistBeaconTopic = "/trackerstream/playlist-beacon/1.0.0"
	// FwdProtocol is the public, content-addressed block-forwarding stream (R5; see fwd.go).
	FwdProtocol protocol.ID = "/trackerstream/fwd/1.0.0"
)

// Version is the trackerstream app version, stamped at build time via
//
//	-ldflags "-X github.com/trackerstream/tsnode.Version=$(git describe --tags --always)"
//
// Plain `go build` / `go test` leave the "dev" fallback. This is the SINGLE source of truth
// for the version string: it feeds both the libp2p UserAgent (node.go) and the RPC /version
// response (rpc.go), so the two can never drift. The agent string is ADVISORY only — never
// gate correctness or security on it; the enforceable "you must update" signal is the
// separate signed min_client_version (tracked outside this change).
var Version = "dev"

// agentRole is the role token in the libp2p UserAgent. The master seeds the corpus, so it
// advertises "seed"; every other node is a "client".
func (r Role) agentRole() string {
	if r == RoleServer {
		return "seed"
	}
	return "client"
}

// donorRendezvous is the stable CID that publicly-reachable donors Provide so a NAT'd peer's
// AutoRelay peer source can discover them (R5 Phase B). Deterministic — every node computes the
// same key from the same constant string; nothing is ever bitswapped for it.
var donorRendezvous = mustDonorRendezvous()

func mustDonorRendezvous() cid.Cid {
	h, err := mh.Sum([]byte("trackerstream/donors/v1"), mh.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

// Role selects the node posture: the server master is a DHT server + seeder + relay
// server + IPNS publisher; the client is a lazy fetcher + DHT client + AutoRelay.
type Role string

const (
	RoleServer Role = "server"
	RoleClient Role = "client"
)

// Config is the full node configuration. An empty RepoPath means an ephemeral,
// in-memory node (tests).
type Config struct {
	Role        Role
	RepoPath    string   // datastore + identity.key; "" = ephemeral/in-memory (tests)
	ListenAddrs []string // libp2p listen multiaddrs
	Bootstrap   []string // bootstrap multiaddrs (the box, for clients)
	// DisableNATPortMap turns OFF the client's UPnP/NAT-PMP port mapping (the swarm-port
	// IGD map from 9f2e8b6). It's an escape hatch for routers where UPnP misbehaves
	// (duplicate/leaking maps, buggy IGD firmware) — the node then relies purely on
	// relay+DCUtR for NAT traversal, exactly as it did before that feature landed.
	// Server-irrelevant (the master never maps). Off by default (mapping stays enabled).
	DisableNATPortMap bool
	// STUNServers are the ICE STUN servers the private-to-private WebRTC transport
	// (webrtcprivate) uses to gather server-reflexive candidates when a browser dials a
	// NATed desktop over /p2p-circuit/webrtc. STUN only — never TURN (see deploy/turnserver.conf):
	// STUN tells a peer its own public address for a DIRECT connection; it relays no media.
	// Empty disables the transport. Client-relevant only (the master is reached over webrtc-direct
	// directly and never needs to be dialled over a relay).
	STUNServers []string
}

// DefaultConfig builds a config for a role. swarmPort 0 = OS-assigned (ephemeral);
// the server master pins its swarm port via TS_SWARM_PORT (deploy sets :5478 — a
// non-default port, since the overlay is private and never on the public IPFS DHT).
func DefaultConfig(role Role, repo string, swarmPort int) Config {
	return Config{
		Role:     role,
		RepoPath: repo,
		ListenAddrs: []string{
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", swarmPort),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", swarmPort),
			fmt.Sprintf("/ip6/::/tcp/%d", swarmPort),
			fmt.Sprintf("/ip6/::/udp/%d/quic-v1", swarmPort),
			// Browser-dialable. A browser can speak NEITHER TCP nor QUIC, so without this the whole
			// web client is unreachable. go-libp2p's default transport set already registers WebRTC —
			// listening is the entire opt-in, there is no new dependency, and it shares the QUIC UDP
			// socket when both sit on the same port (so the master needs no new firewall rule).
			//
			// Enabled for CLIENTS too, not just the seed, and that is the point: a publicly-reachable
			// desktop becomes a browser-dialable seed for free. The connection is DIRECT, so it is not
			// flagged Limited and bitswap flows over it — no relay, no STUN, no reservation, no
			// coturn. It is the cheapest offload tier that exists, and it costs one listen address.
			// (The NATed majority simply never become reachable, exactly as with QUIC/TCP today.)
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/webrtc-direct", swarmPort),
			fmt.Sprintf("/ip6/::/udp/%d/webrtc-direct", swarmPort),
		},
		// Only the client (a potentially-NATed desktop) listens for private-to-private WebRTC
		// dials from browsers; the master is reached directly. STUN-only, no TURN.
		STUNServers: stunServersFor(role),
	}
}

// stunServersFor returns the default ICE STUN servers for a role. Clients need STUN to gather
// server-reflexive candidates for private-to-private WebRTC; the server does not. Overridable at
// deploy via the --stun flag / TS_STUN_SERVERS env (cmd/tsnode/main.go); an explicit empty value
// disables the webrtcprivate transport.
func stunServersFor(role Role) []string {
	if role == RoleServer {
		return nil
	}
	return []string{"stun:trackerstream.xyz:3478"}
}
