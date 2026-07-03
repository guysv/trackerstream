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
const (
	DHTPrefix    protocol.ID = "/trackerstream"
	CatalogTopic             = "/trackerstream/catalog/1.0.0"
	// PlaylistTopic carries {name, signed IPNS record, playlist doc} envelopes — the doc
	// travels INLINE (no bitswap fetch path exists for playlists); see playlist.go.
	PlaylistTopic             = "/trackerstream/playlist/1.0.0"
	PeerProtocol  protocol.ID = "/trackerstream/peer/1.0.0"
	// PlaylistListProtocol is the direct "what playlists do you hold?" request/response
	// stream (disclosure set = held + published-mine only; see playlistlist.go).
	PlaylistListProtocol protocol.ID = "/trackerstream/playlist-list/1.0.0"
	// PlaylistBeaconTopic carries hourly hold beacons — truncated name-hashes of each
	// client's disclosure set, counted locally for popularity (see beacon.go).
	PlaylistBeaconTopic = "/trackerstream/playlist-beacon/1.0.0"
	// FwdProtocol is the public, content-addressed block-forwarding stream (R5; see fwd.go).
	FwdProtocol protocol.ID = "/trackerstream/fwd/1.0.0"
)

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
		},
	}
}
