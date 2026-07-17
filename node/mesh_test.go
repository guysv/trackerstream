package tsnode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func mustAddr(t *testing.T, s string) ma.Multiaddr {
	t.Helper()
	a, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatalf("bad multiaddr %q: %v", s, err)
	}
	return a
}

func TestHasCircuit(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"/ip4/1.2.3.4/tcp/4001", false},                   // plain public direct
		{"/ip4/192.168.1.5/udp/4001/quic-v1", false},       // plain private direct
		{"/ip4/1.2.3.4/tcp/4001/p2p-circuit", true},        // bare reservation (endsInCircuit true too)
		{"/ip4/1.2.3.4/tcp/4001/p2p-circuit/webrtc", true}, // encapsulated — endsInCircuit would MISS this
	}
	for _, c := range cases {
		if got := hasCircuit(mustAddr(t, c.addr)); got != c.want {
			t.Errorf("hasCircuit(%s) = %v, want %v", c.addr, got, c.want)
		}
	}
}

func TestHasPublicDirectAddr(t *testing.T) {
	cases := []struct {
		name  string
		addrs []string
		want  bool
	}{
		{"public direct kept", []string{"/ip4/1.2.3.4/tcp/4001"}, true},
		{"private only dropped", []string{"/ip4/192.168.1.5/tcp/4001", "/ip4/10.0.0.2/tcp/4001"}, false},
		{"loopback only dropped", []string{"/ip4/127.0.0.1/tcp/4001"}, false},
		// A circuit addr carries the RELAY's public IP, not the peer's — must NOT count as public-direct.
		{"circuit-only dropped", []string{"/ip4/1.2.3.4/tcp/4001/p2p-circuit/webrtc"}, false},
		{"mixed: public wins", []string{"/ip4/192.168.1.5/tcp/4001", "/ip4/1.2.3.4/tcp/4001"}, true},
	}
	for _, c := range cases {
		ai := peer.AddrInfo{ID: randPeerID(t)}
		for _, s := range c.addrs {
			ai.Addrs = append(ai.Addrs, mustAddr(t, s))
		}
		if got := hasPublicDirectAddr(ai); got != c.want {
			t.Errorf("%s: hasPublicDirectAddr = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestMeshLinkRank(t *testing.T) {
	cases := []struct {
		name  string
		addrs []string
		want  int
	}{
		{"LAN direct = 0", []string{"/ip4/192.168.1.5/tcp/4001"}, 0},
		{"public direct = 1", []string{"/ip4/1.2.3.4/tcp/4001"}, 1},
		{"circuit-only = 2", []string{"/ip4/1.2.3.4/tcp/4001/p2p-circuit/webrtc"}, 2},
		{"LAN beats public", []string{"/ip4/1.2.3.4/tcp/4001", "/ip4/192.168.1.5/tcp/4001"}, 0},
	}
	for _, c := range cases {
		ai := peer.AddrInfo{ID: randPeerID(t)}
		for _, s := range c.addrs {
			ai.Addrs = append(ai.Addrs, mustAddr(t, s))
		}
		if got := meshLinkRank(ai); got != c.want {
			t.Errorf("%s: meshLinkRank = %d, want %d", c.name, got, c.want)
		}
	}
}

// selfDialable gates the donor-rendezvous self-list: a fresh loopback client is neither public nor
// holding a relay reservation, so it must NOT advertise; forcing it public flips the gate on.
func TestSelfDialableGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	n := mkEphemeral(t, ctx, RoleClient)
	if n.selfDialable() {
		t.Fatalf("fresh loopback client should not be selfDialable (no public reachability, no circuit addr)")
	}
	forcePublic(t, n)
	if !n.selfDialable() {
		t.Fatalf("a public client must be selfDialable")
	}
}

// meshDirectEdges counts only DIRECT, non-seed, non-self peers. A client bootstrapped to the server
// (its seed) and directly connected to another client must count exactly ONE edge (the other client),
// excluding the always-connected seed.
func TestMeshDirectEdgesExcludesSeed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	server, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	defer server.Close()

	boot := fmt.Sprintf("%s/p2p/%s", loopbackAddr(t, server), server.ID())
	mkClient := func() *Node {
		cfg := DefaultConfig(RoleClient, "", 0)
		cfg.Bootstrap = []string{boot}
		n, err := New(ctx, cfg)
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		return n
	}
	a := mkClient()
	defer a.Close()
	b := mkClient()
	defer b.Close()

	// a bootstraps to the server → server is in a.seeds. With only the seed connected, no mesh edges.
	if err := waitConnected(ctx, a, server.ID()); err != nil {
		t.Fatalf("a→server: %v", err)
	}
	if got := a.meshDirectEdges(); got != 0 {
		t.Fatalf("with only the seed connected, meshDirectEdges = %d, want 0 (seed excluded)", got)
	}

	// Now give a a direct edge to b (a non-seed client). That is the one graftable mesh edge.
	connectNodes(t, a, b)
	if err := waitConnected(ctx, a, b.ID()); err != nil {
		t.Fatalf("a→b: %v", err)
	}
	if got := a.meshDirectEdges(); got != 1 {
		t.Fatalf("with the seed + one client connected, meshDirectEdges = %d, want 1", got)
	}
}

// The core star→mesh proof: a server plus three clients, each self-listed on the donor rendezvous,
// form DIRECT client↔client edges purely via the eager-dial mesh loop — no client is handed another
// client's address, they discover each other through the rendezvous and dial.
func TestClientMeshFormsViaRendezvous(t *testing.T) {
	// Shrink the loop cadence so edges form within the test window.
	origInterval, origJitter := meshLoopInterval, meshLoopJitter
	meshLoopInterval, meshLoopJitter = 1*time.Second, 500*time.Millisecond
	t.Cleanup(func() { meshLoopInterval, meshLoopJitter = origInterval, origJitter })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	server, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	defer server.Close()

	boot := fmt.Sprintf("%s/p2p/%s", loopbackAddr(t, server), server.ID())
	var clients []*Node
	for i := 0; i < 3; i++ {
		cfg := DefaultConfig(RoleClient, "", 0)
		cfg.Bootstrap = []string{boot}
		n, err := New(ctx, cfg)
		if err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
		defer n.Close()
		forcePublic(t, n) // so each is a legitimate self-lister on loopback
		clients = append(clients, n)
	}

	if err := waitRoutingTable(ctx, clients...); err != nil {
		t.Fatalf("routing tables did not fill: %v", err)
	}
	for _, n := range append([]*Node{server}, clients...) {
		_ = n.DHT().Bootstrap(ctx)
	}

	// Each client advertises itself to the donor rendezvous (the mesh loop's discovery source).
	for _, n := range clients {
		pctx, pcancel := context.WithTimeout(ctx, 20*time.Second)
		if err := n.DHT().Provide(pctx, donorRendezvous, true); err != nil {
			pcancel()
			t.Fatalf("client %s provide donorRendezvous: %v", n.ID(), err)
		}
		pcancel()
	}

	// Poll until EVERY client holds at least one DIRECT edge to another client (not just the seed).
	deadline := time.Now().Add(60 * time.Second)
	for {
		allMeshed := true
		for _, n := range clients {
			if n.meshDirectEdges() < 1 {
				allMeshed = false
			}
		}
		if allMeshed {
			return // mesh formed via the rendezvous — the star is now a mesh
		}
		if time.Now().After(deadline) {
			for i, n := range clients {
				t.Logf("client %d (%s): meshDirectEdges=%d", i, n.ID(), n.meshDirectEdges())
			}
			t.Fatalf("clients did not form client↔client mesh edges within deadline")
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ctx: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitConnected blocks until n reports a Connected edge to id (or the deadline elapses).
func waitConnected(ctx context.Context, n *Node, id peer.ID) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		if n.host.Network().Connectedness(id) == network.Connected {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %s to connect to %s", n.ID(), id)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}
