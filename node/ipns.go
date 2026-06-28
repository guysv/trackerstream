package tsnode

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ipnsStore caches the latest signed record we've published (or pulled) per IPNS name, so
// `routing/get` can answer from memory and the gossipsub topic can re-broadcast. Records are
// also written to the DHT (custom-prefix `/ipns` namespace) as the box-down fallback.
type ipnsStore struct {
	mu      sync.RWMutex
	records map[string][]byte // base58 name → marshaled signed record
}

func newIpnsStore() *ipnsStore { return &ipnsStore{records: map[string][]byte{}} }

func (s *ipnsStore) get(name string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[name]
	return rec, ok
}

func (s *ipnsStore) put(name string, rec []byte) {
	s.mu.Lock()
	s.records[name] = rec
	s.mu.Unlock()
}

// ingestGossip stores a record received over the catalog gossipsub topic, but ONLY if it
// validates under `name`'s key and is newer (higher sequence) than what we hold — the topic is
// untrusted, so a peer can't poison the cache with a forged or stale record. This is the
// zero-resolve path: a publish propagates to every subscriber's local store, so the client's
// `routing/get` answers instantly with no DHT round-trip.
func (s *ipnsStore) ingestGossip(name string, rec []byte) error {
	pid, err := peer.Decode(name)
	if err != nil {
		return fmt.Errorf("bad ipns name %q: %w", name, err)
	}
	incoming, err := ipns.UnmarshalRecord(rec)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if err := ipns.ValidateWithName(incoming, ipns.NameFromPeer(pid)); err != nil {
		return fmt.Errorf("invalid record for %s: %w", name, err)
	}
	inSeq, err := incoming.Sequence()
	if err != nil {
		return fmt.Errorf("sequence: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.records[name]; ok {
		if curRec, err := ipns.UnmarshalRecord(cur); err == nil {
			if curSeq, err := curRec.Sequence(); err == nil && curSeq >= inSeq {
				return nil // not newer — keep what we have
			}
		}
	}
	s.records[name] = rec
	return nil
}

// PublishIPNS signs an IPNS record for `keyName` pointing at root `c` (value `/ipfs/<cid>`),
// stores it locally, puts it to the custom DHT, and pushes it on the catalog gossipsub topic.
// Mirrors kubo `name/publish key=<keyName> lifetime=<lifetime>` — but the record is the same
// standard IPNS v2 record the Rust client already verifies in prod (verify stays in Rust).
// Returns the publisher PeerId (the `/ipns/<name>` the client resolves) and the record bytes.
func (n *Node) PublishIPNS(ctx context.Context, keyName string, c cid.Cid, lifetime time.Duration, seq uint64) (peer.ID, []byte, error) {
	key, err := n.keystore.GetOrCreate(keyName)
	if err != nil {
		return "", nil, err
	}
	pid, err := peer.IDFromPublicKey(key.GetPublic())
	if err != nil {
		return "", nil, err
	}
	rec, err := signIPNS(key, c, lifetime, seq)
	if err != nil {
		return "", nil, err
	}
	marshaled, err := ipns.MarshalRecord(rec)
	if err != nil {
		return "", nil, err
	}

	name := pid.String()
	n.ipns.put(name, marshaled)

	// DHT put (box-down fallback resolve). Best-effort: the gossipsub push + local store are
	// the primary distribution, so a DHT error (e.g. no peers yet) must not fail publish.
	if n.dht != nil {
		rk := string(ipns.NameFromPeer(pid).RoutingKey())
		if err := n.dht.PutValue(ctx, rk, marshaled); err != nil {
			n.logf("ipns: dht put for %s failed (non-fatal): %v", name, err)
		}
	}
	// Gossipsub push (zero-resolve distribution to subscribed clients).
	if n.pubsub != nil {
		if err := n.pubsub.PublishIPNS(ctx, name, marshaled); err != nil {
			n.logf("ipns: gossipsub push for %s failed (non-fatal): %v", name, err)
		}
	}
	return pid, marshaled, nil
}

// signIPNS builds a signed IPNS v2 record. The value is `/ipfs/<cid>` (path.FromCid) — the
// exact form kubo's `name/publish` produces and `rust_ipns` parses. seq 0 means "next after
// whatever we last published" is the caller's job; we pass it straight through.
func signIPNS(key crypto.PrivKey, c cid.Cid, lifetime time.Duration, seq uint64) (*ipns.Record, error) {
	if lifetime <= 0 {
		lifetime = ipns.DefaultRecordLifetime
	}
	eol := time.Now().Add(lifetime)
	value := path.FromCid(c)
	return ipns.NewRecord(key, value, seq, eol, ipns.DefaultRecordTTL)
}

// ResolveIPNS returns the marshaled signed record for `name` (base58 PeerId): local cache
// first, then the custom DHT. The CALLER (Rust) verifies the signature — we are an untrusted
// cache, exactly like the tracker was.
func (n *Node) ResolveIPNS(ctx context.Context, name string) ([]byte, error) {
	if rec, ok := n.ipns.get(name); ok {
		return rec, nil
	}
	pid, err := peer.Decode(name)
	if err != nil {
		return nil, fmt.Errorf("bad ipns name %q: %w", name, err)
	}
	if n.dht == nil {
		return nil, fmt.Errorf("no record for %s and no DHT", name)
	}
	rk := string(ipns.NameFromPeer(pid).RoutingKey())
	rec, err := n.dht.GetValue(ctx, rk)
	if err != nil {
		return nil, err
	}
	n.ipns.put(name, rec)
	return rec, nil
}

// recordBase64 is the `routing/get` Extra payload (base64 of the marshaled record), matching
// what kubo returns and what the ingest forwards to the server's /ipns store.
func recordBase64(rec []byte) string { return base64.StdEncoding.EncodeToString(rec) }
