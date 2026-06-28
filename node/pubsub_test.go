package tsnode

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// The catalog gossipsub topic must deliver a pushed IPNS record from a publisher (the box) to
// a subscribed client — the zero-resolve distribution path. Two real nodes, connected, one
// publishes, the other's catalog sink must receive the exact record bytes.
func TestCatalogTopicDelivers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	got := make(chan []byte, 1)
	client.PubSub().OnCatalogRecord(func(_ string, record []byte) {
		select {
		case got <- record:
		default:
		}
	})

	if err := client.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Gossipsub needs a moment to form the mesh after connect before a publish is delivered.
	record := []byte("signed-ipns-record-bytes")
	deadline := time.After(20 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := server.PubSub().PublishIPNS(ctx, "catalog-name", record); err != nil {
			t.Fatalf("publish: %v", err)
		}
		select {
		case r := <-got:
			if !bytes.Equal(r, record) {
				t.Fatalf("delivered %q, want %q", r, record)
			}
			return
		case <-tick.C:
			continue
		case <-deadline:
			t.Fatalf("catalog record not delivered within deadline")
		}
	}
}
