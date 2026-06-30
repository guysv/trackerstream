package tsnode

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	ma "github.com/multiformats/go-multiaddr"
)

// control is the trackerstream control plane that the Rust client used to run (peer.rs /
// lib.rs background loops): reachability verdict, relay-hop classification, the warm set, and
// presence beaconing. It lives in the Go node now (Go has the libp2p access); the Rust UI
// reads its summary over `node/status` and issues high-level commands.
type control struct {
	node *Node

	mu          sync.RWMutex
	reachable   network.Reachability // AutoNAT verdict
	relayStats  RelayStats
	warm        map[peer.ID]struct{} // keepalive set (warm holders + master)
	master      peer.ID
	masterKnown bool
}

// RelayStats counts how connections are carried — the peers-pane "direct / peer-relay /
// master-relay" breakdown (RelayStats in lib.rs), inferred from connection multiaddrs.
type RelayStats struct {
	Direct      int `json:"direct"`
	PeerRelay   int `json:"peer_relay"`
	MasterRelay int `json:"master_relay"`
}

func newControl(n *Node) *control {
	return &control{node: n, reachable: network.ReachabilityUnknown, warm: map[peer.ID]struct{}{}}
}

// start wires the event subscription + connection notifiee and launches the presence loop.
func (c *control) start(ctx context.Context) error {
	sub, err := c.node.host.EventBus().Subscribe(
		new(event.EvtLocalReachabilityChanged), eventbus.BufSize(16))
	if err != nil {
		return err
	}
	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-sub.Out():
				if !ok {
					return
				}
				ev := e.(event.EvtLocalReachabilityChanged)
				c.mu.Lock()
				c.reachable = ev.Reachability
				c.mu.Unlock()
				c.node.logf("reachability → %s", ev.Reachability)
				// A publicly-reachable CLIENT is now a usable block-forwarding donor — advertise the
				// rendezvous so NAT'd peers' AutoRelay peer source can find it (R5). The reprovide
				// loop refreshes it (and stops on a public→private flap). The seed never advertises as
				// a donor — it doesn't forward.
				if ev.Reachability == network.ReachabilityPublic && c.node.cfg.Role == RoleClient {
					c.node.provideNow(donorRendezvous)
				}
			}
		}
	}()

	c.node.host.Network().Notify(c.notifiee())
	go c.keepaliveLoop(ctx)
	return nil
}

// SetMaster records the master PeerId so relay-hop classification can distinguish a
// master-carried circuit from a peer-carried one (the master is the coordination-floor relay).
func (c *control) SetMaster(id peer.ID) {
	c.mu.Lock()
	c.master = id
	c.masterKnown = true
	c.warm[id] = struct{}{} // master is always kept warm
	c.mu.Unlock()
}

// Reachable returns the current AutoNAT verdict (public/private/unknown) as a string.
func (c *control) Reachable() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	switch c.reachable {
	case network.ReachabilityPublic:
		return "public"
	case network.ReachabilityPrivate:
		return "private"
	default:
		return "unknown"
	}
}

// Stats returns a copy of the relay-hop classification computed from live connections.
func (c *control) Stats() RelayStats {
	stats := RelayStats{}
	c.mu.RLock()
	master, known := c.master, c.masterKnown
	c.mu.RUnlock()
	for _, conn := range c.node.host.Network().Conns() {
		switch hopKind(conn.RemoteMultiaddr(), conn.RemotePeer(), master, known) {
		case hopMaster:
			stats.MasterRelay++
		case hopPeer:
			stats.PeerRelay++
		default:
			stats.Direct++
		}
	}
	return stats
}

type hop int

const (
	hopDirect hop = iota
	hopPeer
	hopMaster
)

// hopKind classifies a connection's transport: a /p2p-circuit multiaddr is relayed; if the
// relay hop is the master it's master-relay, else peer-relay; otherwise direct.
func hopKind(addr ma.Multiaddr, _ peer.ID, master peer.ID, masterKnown bool) hop {
	s := addr.String()
	if !strings.Contains(s, "/p2p-circuit") {
		return hopDirect
	}
	if masterKnown && strings.Contains(s, master.String()) {
		return hopMaster
	}
	return hopPeer
}

func (c *control) notifiee() network.Notifiee {
	return &network.NotifyBundle{
		ConnectedF: func(_ network.Network, conn network.Conn) {
			c.node.logf("connected %s via %s", conn.RemotePeer(), conn.RemoteMultiaddr())
		},
	}
}

// Warm records a peer as keepalive-worthy (warm holder). The keepalive loop redials warm
// peers that drop, with jittered backoff (no thundering herd).
func (c *control) Warm(id peer.ID) {
	c.mu.Lock()
	c.warm[id] = struct{}{}
	c.mu.Unlock()
}

func (c *control) warmSet() []peer.ID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]peer.ID, 0, len(c.warm))
	for id := range c.warm {
		out = append(out, id)
	}
	return out
}

// keepaliveLoop redials warm peers (master + holders) that have dropped, on a jittered
// cadence. Replaces ipfs.rs keepalive_master + warm_connect (connexa has no built-in
// reconnect). It only ever dials peers we've explicitly warmed — never a stranger.
func (c *control) keepaliveLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(20*time.Second, 10*time.Second)):
		}
		for _, id := range c.warmSet() {
			if c.node.host.Network().Connectedness(id) == network.Connected {
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			if err := c.node.host.Connect(cctx, peer.AddrInfo{ID: id}); err != nil {
				c.node.logf("keepalive redial %s failed: %v", id, err)
			}
			cancel()
		}
	}
}
