package tsnode

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PubSub wraps gossipsub for the catalog topic we own: signed IPNS records pushed
// publish-style — kills the per-search resolve round-trip. Because both ends run our binary
// on a topic we name, this is fully under our control.
type PubSub struct {
	ps      *pubsub.PubSub
	catalog *pubsub.Topic

	mu       sync.Mutex
	onRecord func(name string, record []byte) // catalog-record sink (the client IPNS cache)
}

// catalogMsg is the gossipsub payload on the catalog topic: a name + its marshaled signed
// IPNS record. The receiver verifies the record itself (untrusted transport).
type catalogMsg struct {
	Name   string `json:"name"`
	Record []byte `json:"record"`
}

func newPubSub(ctx context.Context, h host.Host) (*PubSub, error) {
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, err
	}
	p := &PubSub{ps: ps}
	if p.catalog, err = ps.Join(CatalogTopic); err != nil {
		return nil, fmt.Errorf("join catalog topic: %w", err)
	}
	return p, nil
}

// OnCatalogRecord registers the sink invoked for every valid-shaped catalog message received
// (the client wires this to its IPNS cache; verification happens there).
func (p *PubSub) OnCatalogRecord(fn func(name string, record []byte)) {
	p.mu.Lock()
	p.onRecord = fn
	p.mu.Unlock()
}

// SubscribeCatalog starts draining the catalog topic until ctx is cancelled, dispatching each
// message to the registered sink.
func (p *PubSub) SubscribeCatalog(ctx context.Context) error {
	sub, err := p.catalog.Subscribe()
	if err != nil {
		return err
	}
	go func() {
		defer sub.Cancel()
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return // ctx cancelled / topic closed
			}
			var cm catalogMsg
			if json.Unmarshal(msg.Data, &cm) != nil || cm.Name == "" {
				continue
			}
			p.mu.Lock()
			fn := p.onRecord
			p.mu.Unlock()
			if fn != nil {
				fn(cm.Name, cm.Record)
			}
		}
	}()
	return nil
}

// PublishIPNS pushes a signed record onto the catalog topic.
func (p *PubSub) PublishIPNS(ctx context.Context, name string, record []byte) error {
	data, err := json.Marshal(catalogMsg{Name: name, Record: record})
	if err != nil {
		return err
	}
	return p.catalog.Publish(ctx, data)
}

// CatalogPeers lists peers currently subscribed to the catalog topic (gossipsub mesh view).
func (p *PubSub) CatalogPeers() []peer.ID { return p.catalog.ListPeers() }
