package tsnode

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ImportIdentity writes the node's swarm identity (repo/identity.key) from an
// `ipfs key export self` blob — a libp2p protobuf-marshaled private key. Used at deploy time
// to reproduce the prod master PeerId (12D3KooWGb7e…XvzL) on tsnode without re-init. Returns
// the resulting PeerId for verification against the expected value.
func ImportIdentity(repo string, marshaled []byte) (peer.ID, error) {
	priv, err := crypto.UnmarshalPrivateKey(marshaled)
	if err != nil {
		return "", fmt.Errorf("unmarshal identity: %w", err)
	}
	if err := os.MkdirAll(repo, 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(repo, "identity.key"), marshaled, 0o600); err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(priv.GetPublic())
}

// ImportNamedKey installs a keystore key (e.g. "catalog") from an `ipfs key export <name>`
// blob, returning its base58 PeerId (the IPNS name). Used at deploy time to preserve the
// catalog IPNS name (12D3KooWDb53…wPC) so already-shipped clients keep resolving it.
func ImportNamedKey(repo, name string, marshaled []byte) (peer.ID, error) {
	ks, err := NewKeystore(repo)
	if err != nil {
		return "", err
	}
	if err := ks.Import(name, marshaled); err != nil {
		return "", err
	}
	k, err := ks.Get(name)
	if err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(k.GetPublic())
}
