package tsnode

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
)

// Pinset persistence: the legacy flat [cid,...] format migrates to KindRoot, and the typed format
// round-trips kinds across reload.
func TestPinsetTypedPersistenceAndMigration(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())

	// Seed the datastore with the LEGACY flat format.
	legacy := rawBlock(t, []byte("legacy-root"))
	blob, _ := json.Marshal([]string{legacy.Cid().String()})
	if err := store.Put(context.Background(), pinKey, blob); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}

	p, err := newPinset(store)
	if err != nil {
		t.Fatalf("load legacy: %v", err)
	}
	if got := p.CountByKind(); got[KindRoot] != 1 {
		t.Fatalf("legacy CID should load as KindRoot, got %v", got)
	}

	// Add typed entries, persist, reload — kinds must survive.
	tr := rawBlock(t, []byte("track-root"))
	cp := rawBlock(t, []byte("catalog-piece"))
	if err := p.AddTrackRoot(context.Background(), tr.Cid()); err != nil {
		t.Fatalf("add track: %v", err)
	}
	if err := p.AddCatalogPiece(context.Background(), cp.Cid()); err != nil {
		t.Fatalf("add piece: %v", err)
	}

	p2, err := newPinset(store)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := p2.CountByKind()
	fmt.Printf("PROVIDE pinset kinds after reload: root=%d trackRoot=%d catalogPiece=%d\n",
		got[KindRoot], got[KindTrackRoot], got[KindCatalogPiece])
	if got[KindRoot] != 1 || got[KindTrackRoot] != 1 || got[KindCatalogPiece] != 1 {
		t.Fatalf("kinds did not round-trip: %v", got)
	}
	if len(p2.Roots()) != 3 {
		t.Fatalf("reprovide set should contain all 3 CIDs, got %d", len(p2.Roots()))
	}
}

// End-to-end: a client advertises a track root (whole-track granularity) and a catalog piece
// (page granularity); another client discovers both as providers over the custom DHT.
func TestProvideTrackRootAndCatalogPieceDiscoverable(t *testing.T) {
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

	if err := waitRoutingTable(ctx, provider, seeker); err != nil {
		t.Fatalf("routing table: %v", err)
	}

	trackRoot := rawBlock(t, []byte("a track manifest root the provider holds"))
	catalogPiece := rawBlock(t, []byte("a catalog page the provider fetched"))
	if err := provider.PutBlock(ctx, trackRoot); err != nil {
		t.Fatalf("put track root: %v", err)
	}
	if err := provider.PutBlock(ctx, catalogPiece); err != nil {
		t.Fatalf("put catalog piece: %v", err)
	}
	if err := provider.ProvideTrackRoot(ctx, trackRoot.Cid()); err != nil {
		t.Fatalf("ProvideTrackRoot: %v", err)
	}
	if err := provider.ProvideCatalogPiece(ctx, catalogPiece.Cid()); err != nil {
		t.Fatalf("ProvideCatalogPiece: %v", err)
	}

	for _, tc := range []struct {
		label  string
		target cid.Cid
	}{
		{"track-root", trackRoot.Cid()},
		{"catalog-piece", catalogPiece.Cid()},
	} {
		// provideNow is fire-and-forget; poll until the advertisement lands (or time out).
		found := false
		deadline := time.Now().Add(20 * time.Second)
		for !found && time.Now().Before(deadline) {
			fctx, fcancel := context.WithTimeout(ctx, 5*time.Second)
			for pr := range seeker.DHT().FindProvidersAsync(fctx, tc.target, 1) {
				if pr.ID == provider.ID() {
					found = true
					break
				}
			}
			fcancel()
			if !found {
				time.Sleep(500 * time.Millisecond)
			}
		}
		fmt.Printf("PROVIDE seeker found provider for %s = %v\n", tc.label, found)
		if !found {
			t.Fatalf("seeker did not find the provider for %s", tc.label)
		}
	}

	kinds := provider.Pins().CountByKind()
	if kinds[KindTrackRoot] != 1 || kinds[KindCatalogPiece] != 1 {
		t.Fatalf("provider pinset kinds wrong: %v", kinds)
	}
}
