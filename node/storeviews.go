package tsnode

// Composable blockstore views (PLAYLISTS.md Phase 0). boxo's blockservice
// unconditionally persists every exchange-fetched block into its blockstore, and the
// node runs GC-disabled — so whichever store a blockservice WRITES to is where fetched
// blocks live forever. These views split the read path from the write target, letting
// the fwd donor path serve from everything we hold while its transitive fetches land
// only in a bounded in-memory store.

import (
	"context"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// tieredBlockstore reads through `read` in order (first hit wins); writes and deletes go
// only to `write`. read=[main,fwd] write=main is bitswap's serve view (donor-cached
// blocks are servable, wants never write); read=[main,fwd] write=fwd is the donor fetch
// path (serve what we hold, cache what we fetch, never grow the main store).
type tieredBlockstore struct {
	read  []blockstore.Blockstore
	write blockstore.Blockstore
}

var _ blockstore.Blockstore = (*tieredBlockstore)(nil)

func (t *tieredBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	for _, bs := range t.read {
		b, err := bs.Get(ctx, c)
		if err == nil {
			return b, nil
		}
		if !ipld.IsNotFound(err) {
			return nil, err
		}
	}
	return nil, ipld.ErrNotFound{Cid: c}
}

func (t *tieredBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	for _, bs := range t.read {
		ok, err := bs.Has(ctx, c)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (t *tieredBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	for _, bs := range t.read {
		n, err := bs.GetSize(ctx, c)
		if err == nil {
			return n, nil
		}
		if !ipld.IsNotFound(err) {
			return 0, err
		}
	}
	return 0, ipld.ErrNotFound{Cid: c}
}

func (t *tieredBlockstore) Put(ctx context.Context, b blocks.Block) error {
	return t.write.Put(ctx, b)
}

func (t *tieredBlockstore) PutMany(ctx context.Context, bs []blocks.Block) error {
	return t.write.PutMany(ctx, bs)
}

func (t *tieredBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return t.write.DeleteBlock(ctx, c)
}

// AllKeysChan enumerates the WRITE store only: enumeration is a "what do I own" query
// (GC/reprovide-shaped), not a serve-path lookup, and transient read tiers must not
// leak into it.
func (t *tieredBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return t.write.AllKeysChan(ctx)
}

// lruBlockstore is an in-memory, entry- and TTL-bounded blockstore over an expirable
// LRU — the fwd donor cache's semantics (size + TTL) behind the Blockstore interface, so
// a blockservice can use it as a write target. Never enumerated, never advertised.
type lruBlockstore struct {
	lru *expirable.LRU[string, []byte]
}

var _ blockstore.Blockstore = (*lruBlockstore)(nil)

func newLRUBlockstore(size int, ttl time.Duration) *lruBlockstore {
	return &lruBlockstore{lru: expirable.NewLRU[string, []byte](size, nil, ttl)}
}

func (l *lruBlockstore) Get(_ context.Context, c cid.Cid) (blocks.Block, error) {
	data, ok := l.lru.Get(c.KeyString())
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	return blocks.NewBlockWithCid(data, c)
}

func (l *lruBlockstore) Has(_ context.Context, c cid.Cid) (bool, error) {
	return l.lru.Contains(c.KeyString()), nil
}

func (l *lruBlockstore) GetSize(_ context.Context, c cid.Cid) (int, error) {
	data, ok := l.lru.Get(c.KeyString())
	if !ok {
		return 0, ipld.ErrNotFound{Cid: c}
	}
	return len(data), nil
}

func (l *lruBlockstore) Put(_ context.Context, b blocks.Block) error {
	l.lru.Add(b.Cid().KeyString(), b.RawData())
	return nil
}

func (l *lruBlockstore) PutMany(ctx context.Context, bs []blocks.Block) error {
	for _, b := range bs {
		_ = l.Put(ctx, b)
	}
	return nil
}

func (l *lruBlockstore) DeleteBlock(_ context.Context, c cid.Cid) error {
	l.lru.Remove(c.KeyString())
	return nil
}

func (l *lruBlockstore) AllKeysChan(context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid)
	close(ch)
	return ch, nil
}
