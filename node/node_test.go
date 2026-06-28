package tsnode

import (
	"context"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

// rawBlock builds a CIDv1 / sha2-256 / raw (0x55) block — the exact scheme the Rust
// client + repack DAG builder use, so CIDs are byte-identical across the stacks.
func rawBlock(t *testing.T, data []byte) blocks.Block {
	t.Helper()
	h, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash: %v", err)
	}
	c := cid.NewCidV1(cid.Raw, h)
	b, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		t.Fatalf("block: %v", err)
	}
	return b
}

// The core data-plane claim: a block held by A is fetched by B over Bitswap once B is
// directly connected to A (no provider hint) — the "connect-is-enough" offload path the
// Rust client relies on, now on the Go node.
func TestBlockExchangeBetweenTwoNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("node A: %v", err)
	}
	defer a.Close()
	b, err := New(ctx, DefaultConfig(RoleClient, "", 0))
	if err != nil {
		t.Fatalf("node B: %v", err)
	}
	defer b.Close()

	data := []byte("trackerstream go node — block exchange")
	blk := rawBlock(t, data)
	if err := a.PutBlock(ctx, blk); err != nil {
		t.Fatalf("A put: %v", err)
	}

	if err := b.Connect(ctx, peer.AddrInfo{ID: a.ID(), Addrs: a.Addrs()}); err != nil {
		t.Fatalf("B connect A: %v", err)
	}

	got, err := b.GetBlock(ctx, blk.Cid())
	if err != nil {
		t.Fatalf("B get: %v", err)
	}
	if string(got.RawData()) != string(data) {
		t.Fatalf("got %q, want %q", got.RawData(), data)
	}
}

// The custom-prefix DHT must yield `/trackerstream/kad/1.0.0`, not the public
// `/ipfs/kad/1.0.0` — the isolation that keeps the routing table trackerstream-only.
func TestCustomDHTProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	n, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer n.Close()

	var found bool
	for _, p := range n.DHT().Host().Mux().Protocols() {
		if p == "/trackerstream/kad/1.0.0" {
			found = true
		}
		if p == "/ipfs/kad/1.0.0" {
			t.Fatalf("public IPFS DHT protocol is registered — routing table is not private")
		}
	}
	if !found {
		t.Fatalf("custom DHT protocol /trackerstream/kad/1.0.0 not registered")
	}
}
