package tsnode

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Mirror the binary's setup (persistent leveldb repos, distinct nodes) to isolate whether the
// cross-process block-fetch hang is the datastore or the process boundary.
func TestBlockExchangePersistentRepos(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a, err := New(ctx, DefaultConfig(RoleServer, t.TempDir(), 0))
	if err != nil {
		t.Fatalf("node A: %v", err)
	}
	defer a.Close()
	b, err := New(ctx, DefaultConfig(RoleClient, t.TempDir(), 0))
	if err != nil {
		t.Fatalf("node B: %v", err)
	}
	defer b.Close()

	blk := rawBlock(t, []byte("persistent-repo block exchange"))
	if err := a.PutBlock(ctx, blk); err != nil {
		t.Fatalf("A put: %v", err)
	}
	if err := b.Connect(ctx, peer.AddrInfo{ID: a.ID(), Addrs: a.Addrs()}); err != nil {
		t.Fatalf("B connect: %v", err)
	}
	got, err := b.GetBlock(ctx, blk.Cid())
	if err != nil {
		t.Fatalf("B get: %v", err)
	}
	if string(got.RawData()) != "persistent-repo block exchange" {
		t.Fatalf("bytes mismatch")
	}
}
