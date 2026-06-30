package tsnode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/network"
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

// DialProviders connects a fetcher to the NON-seed provider of a CID (the seed is excluded), so
// bitswap can then pull from the peer — the "try other peers" hook. A then serves a block to C
// that C could only have gotten by connecting to A (they don't share it via the seed here).
func TestDialProvidersConnectsNonSeedPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	defer seed.Close()
	boot := fmt.Sprintf("%s/p2p/%s", loopbackAddr(t, seed), seed.ID())

	mkClient := func() *Node {
		cfg := DefaultConfig(RoleClient, "", 0)
		cfg.Bootstrap = []string{boot}
		n, err := New(ctx, cfg)
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		return n
	}
	provider := mkClient() // "A" — holds + provides a block
	defer provider.Close()
	fetcher := mkClient() // "C" — wants it, should reach A (not the seed) for it
	defer fetcher.Close()

	if err := waitRoutingTable(ctx, provider, fetcher); err != nil {
		t.Fatalf("routing table: %v", err)
	}

	// A block ONLY the provider holds (the seed never has it), advertised by the provider.
	blk := rawBlock(t, []byte("a block only the non-seed provider holds"))
	if err := provider.PutBlock(ctx, blk); err != nil {
		t.Fatalf("provider put: %v", err)
	}
	if err := provider.ProvideTrackRoot(ctx, blk.Cid()); err != nil {
		t.Fatalf("provide: %v", err)
	}

	// Let the provider record land, then dial providers from the fetcher.
	deadline := time.Now().Add(20 * time.Second)
	var connected int
	for time.Now().Before(deadline) {
		connected = fetcher.DialProviders(ctx, blk.Cid())
		if connected > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("DIALPROV fetcher connected to %d non-seed provider(s)\n", connected)
	if connected == 0 {
		t.Fatalf("DialProviders did not connect the fetcher to the non-seed provider")
	}
	if fetcher.Host().Network().Connectedness(provider.ID()) != network.Connected {
		t.Fatalf("fetcher should be connected to the provider after DialProviders")
	}
	// The seed was excluded (never dialed as a provider).
	if fetcher.Host().Network().Connectedness(seed.ID()) != network.Connected {
		t.Fatalf("sanity: fetcher should still be connected to the seed (bootstrap)")
	}

	// Now the fetcher can bitswap the block — only the provider has it.
	got, err := fetcher.GetBlock(ctx, blk.Cid())
	if err != nil {
		t.Fatalf("fetch after dial: %v", err)
	}
	if string(got.RawData()) != string(blk.RawData()) {
		t.Fatalf("bytes mismatch")
	}
}

// CatCatalog advertises the LEAF page blocks it fetched as catalog pieces (the node-side half of
// the strategy — the client only knows the catalog root + byte offsets, so per-page providing has
// to happen here). Interior index nodes are not advertised, and a re-read does not re-advertise.
func TestCatCatalogAdvertisesLeafPages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	n, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer n.Close()

	// A multi-block UnixFS file (small chunks → several leaf pages + an interior root). Each
	// 1KB chunk gets distinct bytes so the leaves are distinct CIDs (identical chunks would
	// dedup to one block).
	blob := make([]byte, 8*1024)
	for i := range blob {
		blob[i] = byte(i/1024)*37 + byte(i)
	}
	root, err := n.AddUnixFS(ctx, bytes.NewReader(blob), AddOptions{ChunkSize: 1024, RawLeaves: false, Pin: false})
	if err != nil {
		t.Fatalf("add: %v", err)
	}

	got, err := n.CatCatalog(ctx, root, 0, -1)
	if err != nil {
		t.Fatalf("CatCatalog: %v", err)
	}
	if len(got) != len(blob) {
		t.Fatalf("cat returned %d bytes, want %d", len(got), len(blob))
	}
	kinds := n.Pins().CountByKind()
	pieces := kinds[KindCatalogPiece]
	fmt.Printf("PROVIDE CatCatalog tagged %d leaf pages + %d catalog source root\n", pieces, kinds[KindRoot])
	if pieces < 2 {
		t.Fatalf("expected several leaf pages advertised, got %d", pieces)
	}
	// The catalog root is advertised once as a source (the dialable rendezvous); interior index
	// nodes are NOT (only the root + leaf pages).
	if kinds[KindRoot] != 1 {
		t.Fatalf("expected exactly the catalog root advertised as source, got %d", kinds[KindRoot])
	}

	// Re-read: dedup — no new pieces.
	if _, err := n.CatCatalog(ctx, root, 0, -1); err != nil {
		t.Fatalf("CatCatalog re-read: %v", err)
	}
	if again := n.Pins().CountByKind()[KindCatalogPiece]; again != pieces {
		t.Fatalf("re-read re-advertised pieces: %d -> %d", pieces, again)
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
