// Package tsnode is the trackerstream custom IPFS node — go-libp2p (host) + boxo
// (bitswap / unixfs / ipns / blockstore), assembled by us. The same binary runs as
// the server master (bootstrap + seeder) and the desktop client sidecar, with custom
// protocols (private DHT + IPNS-over-pubsub) we can set because it's our own binary.
package tsnode

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/protocol"
)

// Custom protocol / topic identifiers. The DHT prefix yields `/trackerstream/kad/1.0.0`
// (a private routing table — libp2p only routes to peers speaking the matching protocol,
// so no public-IPFS crawl). The catalog topic carries the signed IPNS record push-style.
const (
	DHTPrefix     protocol.ID = "/trackerstream"
	CatalogTopic              = "/trackerstream/catalog/1.0.0"
	PresenceTopic             = "/trackerstream/presence/1.0.0"
	PeerProtocol  protocol.ID = "/trackerstream/peer/1.0.0"
)

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
	RelayServer bool     // run the clamped circuit-relay v2 service (server master / willing client)
}

// DefaultConfig builds a config for a role. swarmPort 0 = OS-assigned (ephemeral);
// the server master pins 4001 to match the kubo deployment it replaces.
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
		RelayServer: role == RoleServer,
	}
}
