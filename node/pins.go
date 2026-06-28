package tsnode

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-cid"
)

// pinKey namespaces the persisted pinset in the datastore.
var pinKey = ds.NewKey("/trackerstream/pins")

// Pinset is the durable set of recursively-pinned root CIDs. Pins are the durability
// contract (the node runs GC-disabled, exactly like kubo `daemon --enable-gc=false`), and —
// matching kubo's `Provide.Strategy=roots` — only these roots are advertised to the DHT, not
// every leaf block. Persisted to the datastore so it survives restarts.
type Pinset struct {
	mu    sync.RWMutex
	roots map[cid.Cid]struct{}
	ds    ds.Batching
}

func newPinset(store ds.Batching) (*Pinset, error) {
	p := &Pinset{roots: map[cid.Cid]struct{}{}, ds: store}
	data, err := store.Get(context.Background(), pinKey)
	if err == ds.ErrNotFound {
		return p, nil
	}
	if err != nil {
		return nil, err
	}
	var saved []string
	if json.Unmarshal(data, &saved) == nil {
		for _, s := range saved {
			if c, err := cid.Decode(s); err == nil {
				p.roots[c] = struct{}{}
			}
		}
	}
	return p, nil
}

func (p *Pinset) persist(ctx context.Context) error {
	p.mu.RLock()
	out := make([]string, 0, len(p.roots))
	for c := range p.roots {
		out = append(out, c.String())
	}
	p.mu.RUnlock()
	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	return p.ds.Put(ctx, pinKey, data)
}

// Add pins a root (idempotent) and persists.
func (p *Pinset) Add(ctx context.Context, c cid.Cid) error {
	p.mu.Lock()
	p.roots[c] = struct{}{}
	p.mu.Unlock()
	return p.persist(ctx)
}

// Remove unpins a root (idempotent — a missing root is not an error, matching kubo's
// swallow-"not pinned") and persists.
func (p *Pinset) Remove(ctx context.Context, c cid.Cid) error {
	p.mu.Lock()
	delete(p.roots, c)
	p.mu.Unlock()
	return p.persist(ctx)
}

// Has reports whether c is pinned.
func (p *Pinset) Has(c cid.Cid) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.roots[c]
	return ok
}

// Roots returns the pinned root CIDs (the reprovide set + `verify-pinset` source).
func (p *Pinset) Roots() []cid.Cid {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]cid.Cid, 0, len(p.roots))
	for c := range p.roots {
		out = append(out, c)
	}
	return out
}

// reprovideLoop advertises every pinned root to the custom DHT on `interval` (kubo's
// `Provide.DHT.Interval`, 22h in prod) so cold/rare roots stay discoverable box-down. The
// first sweep runs shortly after start, not after a full interval.
func (n *Node) reprovideLoop(ctx context.Context, interval time.Duration) {
	if n.dht == nil || n.pins == nil {
		return
	}
	t := time.NewTimer(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, root := range n.pins.Roots() {
				cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
				if err := n.dht.Provide(cctx, root, true); err != nil {
					n.logf("reprovide %s failed: %v", root, err)
				}
				cancel()
			}
			t.Reset(interval)
		}
	}
}
