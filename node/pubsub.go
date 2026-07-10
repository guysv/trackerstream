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

// PubSub wraps gossipsub for the topics we own: the catalog topic (signed IPNS records
// pushed publish-style — kills the per-search resolve round-trip) and the playlist topic
// (signed records with the playlist doc INLINE; see playlist.go). Because both ends run
// our binary on topics we name, the envelopes are fully under our control.
type PubSub struct {
	ps       *pubsub.PubSub
	catalog  *pubsub.Topic
	playlist *pubsub.Topic
	beacon   *pubsub.Topic

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
	// 2 MiB max message: a max-size playlist doc (1 MiB, validator-capped) + record +
	// envelope framing must fit; the gossipsub default is 1 MiB.
	ps, err := pubsub.NewGossipSub(ctx, h, pubsub.WithMaxMessageSize(2<<20))
	if err != nil {
		return nil, err
	}
	p := &PubSub{ps: ps}
	if p.catalog, err = ps.Join(CatalogTopic); err != nil {
		return nil, fmt.Errorf("join catalog topic: %w", err)
	}
	return p, nil
}

// joinAndDrain registers val + joins + drains a LIST of gossipsub topics, feeding every
// subscription into one drain fn. Validator is registered BEFORE Join so nothing unvalidated
// ever enters. It returns the PRIMARY topic (topics[0]) for publishing.
//
// Dual-register scaffolding (wire-version hardening): today each list has one entry, but the
// shape means a future topic rename ships as {old, new} here — we SUBSCRIBE to all listed
// topics but PUBLISH only to the primary, so publish moves to the new name only after adoption,
// never a hard flip that partitions old peers. See the protocol-evolution policy in config.go.
func (p *PubSub) joinAndDrain(ctx context.Context, topics []string, val pubsub.ValidatorEx, drain func(*pubsub.Message)) (*pubsub.Topic, error) {
	var primary *pubsub.Topic
	for i, name := range topics {
		if err := p.ps.RegisterTopicValidator(name, val); err != nil {
			return nil, fmt.Errorf("validator %s: %w", name, err)
		}
		t, err := p.ps.Join(name)
		if err != nil {
			return nil, fmt.Errorf("join %s: %w", name, err)
		}
		if i == 0 {
			primary = t
		}
		sub, err := t.Subscribe()
		if err != nil {
			return nil, err
		}
		go func() {
			defer sub.Cancel()
			for {
				msg, err := sub.Next(ctx)
				if err != nil {
					return // ctx cancelled / topic closed
				}
				drain(msg)
			}
		}()
	}
	return primary, nil
}

// SetupPlaylist registers the playlist topic validator, joins the topic, and starts
// draining it into sink. ALL roles subscribe — gossipsub only forwards on subscribed topics,
// so the seed's subscription IS its "forward but don't save" role (its sink stores records only).
func (p *PubSub) SetupPlaylist(ctx context.Context, val pubsub.ValidatorEx, sink func(name string, rec, doc []byte)) error {
	t, err := p.joinAndDrain(ctx, []string{PlaylistTopic}, val, func(msg *pubsub.Message) {
		// The validator already accepted this message; decode cannot fail here
		// short of a race on tunables, so a failure is just dropped.
		if name, rec, doc, err := decodePlaylistMsg(msg.Data); err == nil {
			sink(name, rec, doc)
		}
	})
	if err != nil {
		return err
	}
	p.playlist = t
	return nil
}

// PublishPlaylist pushes an encoded {name, record, doc} envelope onto the playlist topic.
// The local validator runs on our own publishes too — an invalid envelope errors here
// instead of entering the mesh.
func (p *PubSub) PublishPlaylist(ctx context.Context, data []byte) error {
	if p.playlist == nil {
		return fmt.Errorf("playlist topic not set up")
	}
	return p.playlist.Publish(ctx, data)
}

// SetupBeacon mirrors SetupPlaylist for the hold-beacon topic: validator before Join,
// then a drain goroutine feeding the sink with the message's authenticated ORIGIN
// (GetFrom — the signer under default StrictSign, not the last hop). ALL roles
// subscribe: subscription is what makes a node forward the mesh; the seed counts
// uselessly but relays usefully.
func (p *PubSub) SetupBeacon(ctx context.Context, val pubsub.ValidatorEx, sink func(origin peer.ID, data []byte)) error {
	t, err := p.joinAndDrain(ctx, []string{PlaylistBeaconTopic}, val, func(msg *pubsub.Message) {
		sink(msg.GetFrom(), msg.Data)
	})
	if err != nil {
		return err
	}
	p.beacon = t
	return nil
}

// PublishBeacon pushes an encoded hold beacon onto the beacon topic.
func (p *PubSub) PublishBeacon(ctx context.Context, data []byte) error {
	if p.beacon == nil {
		return fmt.Errorf("beacon topic not set up")
	}
	return p.beacon.Publish(ctx, data)
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
