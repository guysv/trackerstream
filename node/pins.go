package tsnode

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
)

// pinKey namespaces the persisted pinset in the datastore.
var pinKey = ds.NewKey("/trackerstream/pins")

// PinKind tags WHY a CID is held, which drives the reprovide GRANULARITY:
//   - KindRoot: a durability root (the seed's catalog root + track roots from ingest). Provided.
//   - KindTrackRoot: a track manifest root a client holds. Provided — peers want the whole track,
//     so advertising the root is enough; bitswap pulls the interior DAG from the provider. We do
//     NOT advertise track interior blocks (keeps the DHT light).
//   - KindCatalogPiece: an individual catalog block a client fetched. Provided — catalog access is
//     random/partial, so peers sharing pages offloads the seed. These are CACHE (the client may
//     Remove them when evicting), not a durability promise.
type PinKind uint8

const (
	KindRoot PinKind = iota
	KindTrackRoot
	KindCatalogPiece
)

// Pinset is the set of CIDs this node holds and ADVERTISES to the DHT, each tagged with a
// PinKind. The node runs GC-disabled (like kubo `daemon --enable-gc=false`), so a held block is
// retained and bitswap-servable regardless; the set is what we reprovide. Persisted across
// restarts. Only members are advertised — never arbitrary interior blocks (kubo's
// `Provide.Strategy=roots`, generalised per kind).
type Pinset struct {
	mu    sync.RWMutex
	roots map[cid.Cid]PinKind
	ds    ds.Batching
}

func newPinset(store ds.Batching) (*Pinset, error) {
	p := &Pinset{roots: map[cid.Cid]PinKind{}, ds: store}
	data, err := store.Get(context.Background(), pinKey)
	if err == ds.ErrNotFound {
		return p, nil
	}
	if err != nil {
		return nil, err
	}
	// New format: {cid: kind}. Legacy format: [cid,...] (all KindRoot). Try the map first; an
	// array fails to unmarshal into a map, so the fallback discriminates cleanly.
	var kinds map[string]PinKind
	if json.Unmarshal(data, &kinds) == nil {
		for s, k := range kinds {
			if c, err := cid.Decode(s); err == nil {
				p.roots[c] = k
			}
		}
		return p, nil
	}
	var saved []string
	if json.Unmarshal(data, &saved) == nil {
		for _, s := range saved {
			if c, err := cid.Decode(s); err == nil {
				p.roots[c] = KindRoot
			}
		}
	}
	return p, nil
}

func (p *Pinset) persist(ctx context.Context) error {
	p.mu.RLock()
	out := make(map[string]PinKind, len(p.roots))
	for c, k := range p.roots {
		out[c.String()] = k
	}
	p.mu.RUnlock()
	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	return p.ds.Put(ctx, pinKey, data)
}

// Add pins a CID as a durability root (idempotent) and persists.
func (p *Pinset) Add(ctx context.Context, c cid.Cid) error { return p.add(ctx, c, KindRoot) }

// AddTrackRoot records a track manifest root to advertise (whole-track discovery).
func (p *Pinset) AddTrackRoot(ctx context.Context, c cid.Cid) error {
	return p.add(ctx, c, KindTrackRoot)
}

// AddCatalogPiece records an individual catalog block to advertise (page sharing).
func (p *Pinset) AddCatalogPiece(ctx context.Context, c cid.Cid) error {
	return p.add(ctx, c, KindCatalogPiece)
}

func (p *Pinset) add(ctx context.Context, c cid.Cid, k PinKind) error {
	p.mu.Lock()
	p.roots[c] = k
	p.mu.Unlock()
	return p.persist(ctx)
}

// CountByKind returns how many CIDs are held per kind (for node/status metrics).
func (p *Pinset) CountByKind() map[PinKind]int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := map[PinKind]int{}
	for _, k := range p.roots {
		out[k]++
	}
	return out
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
			// Donor rendezvous (R5): advertise iff we're a public CLIENT donor (the seed never forwards,
			// so it never advertises as a donor). Gating each sweep on the LIVE reachability verdict
			// means a public→private flap simply stops re-advertising on the next sweep — auto-
			// deadvertise without bookkeeping. NOT added to the pinset (would pollute pin ls /
			// verify-pinset); providing a CID we don't hold is fine (it's a peer assertion).
			if n.cfg.Role == RoleClient && n.control.Reachable() == "public" {
				cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
				if err := n.dht.Provide(cctx, donorRendezvous, true); err != nil {
					n.logf("reprovide donor-rendezvous failed: %v", err)
				}
				cancel()
			}
			t.Reset(interval)
		}
	}
}
