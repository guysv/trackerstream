package tsnode

// Mesh membership (R6, star→mesh): a client normally holds one link — to the master — so the
// catalog gossipsub "mesh" is really a STAR and all propagation funnels through the box. Gossipsub
// can only graft a mesh link to a peer it already has a connection to, so the whole job is to give
// each client a few DIRECT connections to OTHER clients. This loop is that: it samples the donor
// rendezvous (the same bucket AutoRelay's peer source reads, but a SEPARATE consumer) and dials up
// to meshTargetK not-yet-connected clients. Once the connections exist, gossipsub grafts them and
// peer-exchange (pubsub.WithPeerExchange, see pubsub.go) sustains the mesh without re-hitting the
// DHT — so this loop is really just bootstrap. It runs on CLIENTS only; the server is already
// connected to everyone.

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	meshTargetK     = 4                // desired DIRECT client↔client edges (3–5 for desktops)
	meshDialConc    = 4                // bounded concurrent dials per sweep
	meshDialTimeout = 15 * time.Second // per-dial budget
	meshFindTimeout = 30 * time.Second // the donor-rendezvous provider walk is slow on a sparse overlay
)

// vars (not consts) so tests can shrink the cadence.
var (
	meshLoopInterval = 45 * time.Second
	meshLoopJitter   = 30 * time.Second
)

// meshDirectEdges counts the edges gossipsub can actually graft over: non-self, non-seed peers at
// network.Connected. Relay-only (network.Limited) peers are DELIBERATELY excluded — pubsub refuses
// to graft over a Limited conn, so counting them toward K would let the loop stop dialing at K
// non-grafting connections and starve the mesh of real edges. Counting direct-only keeps the loop
// dialing (each dial drives a DCUtR upgrade attempt) until K genuinely graftable edges exist.
func (n *Node) meshDirectEdges() int {
	self := n.host.ID()
	c := 0
	for _, p := range n.host.Network().Peers() {
		if p == self {
			continue
		}
		if _, seed := n.seeds[p]; seed {
			continue
		}
		if n.host.Network().Connectedness(p) == network.Connected {
			c++
		}
	}
	return c
}

// meshDialCandidates samples the donor rendezvous and returns dial candidates sorted cheapest-link
// first. It skips self, seeds, and peers we already hold a DIRECT (Connected) edge to — but KEEPS
// peers we're only Limited-connected to, since redialing them drives a DCUtR upgrade to direct.
func (n *Node) meshDialCandidates(ctx context.Context, num int) []peer.AddrInfo {
	if n.dht == nil {
		return nil
	}
	self := n.host.ID()
	fctx, cancel := context.WithTimeout(ctx, meshFindTimeout)
	defer cancel()

	var cands []peer.AddrInfo
	for p := range n.dht.FindProvidersAsync(fctx, donorRendezvous, num) {
		if p.ID == self || len(p.Addrs) == 0 {
			continue
		}
		if _, seed := n.seeds[p.ID]; seed {
			continue // the seed(s) are already connected — not a client↔client edge
		}
		if n.host.Network().Connectedness(p.ID) == network.Connected {
			continue // already have a direct edge to this peer
		}
		cands = append(cands, p)
	}
	// Cheapest link first: a same-LAN direct dial beats a public direct dial beats a circuit hole-punch.
	sort.SliceStable(cands, func(i, j int) bool {
		return meshLinkRank(cands[i]) < meshLinkRank(cands[j])
	})
	return cands
}

// meshLinkRank scores an AddrInfo by cheapest reachable link: 0 = a private/LAN direct addr
// (same-network, no relay), 1 = a public direct addr, 2 = circuit-only (needs a relayed hole-punch).
func meshLinkRank(ai peer.AddrInfo) int {
	hasPublicDirect := false
	for _, a := range ai.Addrs {
		if hasCircuit(a) {
			continue
		}
		if manet.IsPrivateAddr(a) {
			return 0
		}
		if manet.IsPublicAddr(a) {
			hasPublicDirect = true
		}
	}
	if hasPublicDirect {
		return 1
	}
	return 2
}

// meshLoop keeps up to meshTargetK direct client↔client edges alive. On a jittered timer it counts
// graftable edges and, if short, dials the shortfall from FRESH rendezvous records (better addrs
// than a stale peerstore). Meshed peers are NOT warmed: mesh topology is fluid and self-healing —
// when an edge drops the next sweep redials, and once the network is large enough PX refills it.
func (n *Node) meshLoop(ctx context.Context) {
	if n.dht == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(meshLoopInterval, meshLoopJitter)):
		}
		have := n.meshDirectEdges()
		if have >= meshTargetK {
			continue
		}
		need := meshTargetK - have
		cands := n.meshDialCandidates(ctx, need*4) // over-fetch: most sweeps dial only a few
		if len(cands) == 0 {
			continue
		}
		if len(cands) > need {
			cands = cands[:need]
		}
		sem := make(chan struct{}, meshDialConc)
		var wg sync.WaitGroup
		for _, ai := range cands {
			wg.Add(1)
			sem <- struct{}{}
			go func(ai peer.AddrInfo) {
				defer wg.Done()
				defer func() { <-sem }()
				cctx, cc := context.WithTimeout(ctx, meshDialTimeout)
				defer cc()
				if err := n.host.Connect(cctx, ai); err != nil {
					n.logf("mesh dial %s failed: %v", ai.ID, err)
				}
			}(ai)
		}
		wg.Wait()
	}
}
