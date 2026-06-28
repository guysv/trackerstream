package tsnode

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Keystore holds named ed25519 keys, mirroring kubo's keystore (`$IPFS_PATH/keystore/`).
// The catalog is published under a named key ("catalog"); the ingest creates/looks it up
// via the `key/gen` + `key/list` RPC. Keys are persisted as libp2p protobuf-marshaled
// private keys — the SAME bytes `ipfs key export` writes — so the existing prod `catalog`
// key (PeerId 12D3KooWDb53…wPC) can be dropped in verbatim and IPNS signatures stay valid
// for already-shipped clients.
type Keystore struct {
	dir string // "" = in-memory only (tests)
	mu  sync.Mutex
	mem map[string]crypto.PrivKey
}

// NewKeystore opens (creating if needed) the keystore under repo/keystore. An empty repo
// yields an in-memory keystore (tests).
func NewKeystore(repo string) (*Keystore, error) {
	ks := &Keystore{mem: map[string]crypto.PrivKey{}}
	if repo == "" {
		return ks, nil
	}
	ks.dir = filepath.Join(repo, "keystore")
	if err := os.MkdirAll(ks.dir, 0o700); err != nil {
		return nil, err
	}
	return ks, nil
}

func (ks *Keystore) path(name string) string { return filepath.Join(ks.dir, name) }

// Get returns the named key, loading it from disk on first use. Missing → (nil, nil).
func (ks *Keystore) Get(name string) (crypto.PrivKey, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if k, ok := ks.mem[name]; ok {
		return k, nil
	}
	if ks.dir == "" {
		return nil, nil
	}
	data, err := os.ReadFile(ks.path(name))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	k, err := crypto.UnmarshalPrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal key %q: %w", name, err)
	}
	ks.mem[name] = k
	return k, nil
}

// GetOrCreate returns the named ed25519 key, minting + persisting it if absent. This is the
// `key/gen` primitive (idempotent — a second call returns the existing key, like kubo).
func (ks *Keystore) GetOrCreate(name string) (crypto.PrivKey, error) {
	if k, err := ks.Get(name); err != nil || k != nil {
		return k, err
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := ks.put(name, priv); err != nil {
		return nil, err
	}
	return priv, nil
}

// Import stores a key under name from its libp2p protobuf-marshaled bytes (an
// `ipfs key export` blob) — used to bring the prod swarm/catalog identities into tsnode.
func (ks *Keystore) Import(name string, marshaled []byte) error {
	priv, err := crypto.UnmarshalPrivateKey(marshaled)
	if err != nil {
		return fmt.Errorf("import key %q: %w", name, err)
	}
	return ks.put(name, priv)
}

func (ks *Keystore) put(name string, priv crypto.PrivKey) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.mem[name] = priv
	if ks.dir == "" {
		return nil
	}
	data, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return err
	}
	return os.WriteFile(ks.path(name), data, 0o600)
}

// List returns each key name with its base58 PeerId (the `key/list` RPC shape).
func (ks *Keystore) List() ([]KeyInfo, error) {
	names := map[string]struct{}{}
	ks.mu.Lock()
	for n := range ks.mem {
		names[n] = struct{}{}
	}
	ks.mu.Unlock()
	if ks.dir != "" {
		entries, err := os.ReadDir(ks.dir)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() {
				names[e.Name()] = struct{}{}
			}
		}
	}
	out := make([]KeyInfo, 0, len(names))
	for n := range names {
		k, err := ks.Get(n)
		if err != nil || k == nil {
			continue
		}
		id, err := peer.IDFromPublicKey(k.GetPublic())
		if err != nil {
			continue
		}
		out = append(out, KeyInfo{Name: n, ID: id.String()})
	}
	return out, nil
}

// KeyInfo is a keystore entry (name + base58 PeerId), the `key/list`/`key/gen` response row.
type KeyInfo struct {
	Name string
	ID   string
}
