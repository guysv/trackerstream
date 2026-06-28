package tsnode

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/multiformats/go-multiaddr"
)

// loopbackAddr returns a dialable 127.0.0.1/::1 listen multiaddr for n (the test harness
// runs on one host; the machine's other interface addrs, e.g. Tailscale, just dial-backoff).
func loopbackAddr(t *testing.T, n *Node) multiaddr.Multiaddr {
	t.Helper()
	for _, a := range n.Addrs() {
		s := a.String()
		if strings.Contains(s, "/127.0.0.1/") || strings.Contains(s, "/ip6/::1/") {
			return a
		}
	}
	t.Fatalf("no loopback listen addr for %s", n.ID())
	return nil
}

// Provider discovery over the CUSTOM DHT: a server (DHT server / bootstrap) plus two clients
// bootstrapped to it. One client provides a CID; the other finds it as a provider purely via
// the `/trackerstream/kad/1.0.0` routing — the box-down discovery path for cold/rare roots.
func TestCustomDHTProvideAndFind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	provider := mkClient()
	defer provider.Close()
	seeker := mkClient()
	defer seeker.Close()

	// Wait for both clients to populate their routing table with the server (DHT-server peer
	// added after the identify handshake confirms the /trackerstream/kad protocol).
	if err := waitRoutingTable(ctx, provider, seeker); err != nil {
		t.Fatalf("routing table did not fill: %v", err)
	}
	for _, n := range []*Node{server, provider, seeker} {
		_ = n.DHT().Bootstrap(ctx)
	}

	blk := rawBlock(t, []byte("cold root only the provider holds"))
	root := blk.Cid()
	if err := provider.PutBlock(ctx, blk); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Advertise the root to the custom DHT.
	pctx, pcancel := context.WithTimeout(ctx, 20*time.Second)
	if err := provider.DHT().Provide(pctx, root, true); err != nil {
		pcancel()
		t.Fatalf("provide: %v", err)
	}
	pcancel()

	// The seeker resolves the provider purely via the custom DHT (no direct connection hint).
	fctx, fcancel := context.WithTimeout(ctx, 20*time.Second)
	defer fcancel()
	found := false
	for p := range seeker.DHT().FindProvidersAsync(fctx, root, 1) {
		if p.ID == provider.ID() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("seeker did not find the provider for %s over the custom DHT", root)
	}
}

// waitRoutingTable blocks until every node has at least one peer in its DHT routing table (or
// ctx/deadline elapses).
func waitRoutingTable(ctx context.Context, nodes ...*Node) error {
	deadline := time.Now().Add(25 * time.Second)
	for {
		all := true
		for _, n := range nodes {
			if n.DHT().RoutingTable().Size() == 0 {
				all = false
			}
		}
		if all {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}
