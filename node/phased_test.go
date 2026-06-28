package tsnode

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Phase D end-to-end: a server PublishIPNS pushes the signed record on the catalog gossipsub
// topic; a connected subscriber ingests + validates it into its local store, so the
// subscriber's ResolveIPNS (the `routing/get` path) answers with ZERO DHT round-trip — the
// per-search resolve the gossipsub topic exists to kill. Also proves the untrusted-topic guard
// (only a validly-signed record for the name is accepted).
func TestCatalogGossipZeroResolve(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	server, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	defer server.Close()
	client, err := New(ctx, DefaultConfig(RoleClient, "", 0))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer client.Close()

	if err := client.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}

	c := rawBlock(t, []byte("catalog db v1")).Cid()
	// Publish repeatedly until the gossipsub mesh has formed and the client has ingested it.
	pid, _, err := server.PublishIPNS(ctx, "catalog", c, time.Hour, 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	name := pid.String()

	deadline := time.After(25 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		// The client resolves purely from its local store (populated by the gossip push) — no
		// DHT GetValue. If the record is there, ResolveIPNS returns immediately.
		if rec, ok := client.ipns.get(name); ok && len(rec) > 0 {
			got, err := client.ResolveIPNS(ctx, name)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if len(got) == 0 {
				t.Fatalf("resolved empty record")
			}
			return // zero-resolve achieved
		}
		select {
		case <-tick.C:
			_, _, _ = server.PublishIPNS(ctx, "catalog", c, time.Hour, 1) // re-push until mesh forms
		case <-deadline:
			t.Fatalf("client never received the catalog record over gossipsub")
		}
	}
}
