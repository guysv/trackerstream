package tsnode

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// fixtureCID is a stable CIDv1/sha2-256/raw used by the cross-stack IPNS fixture (also
// embedded in the Rust ipns.rs test).
const fixtureCID = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy"

// deterministicKey builds the same ed25519 libp2p key from a fixed 32-byte seed in both Go
// and Rust, so a Go-signed record can be verified by the Rust `verify_b64` against the same
// PeerId. The seed is all-0x42.
func deterministicKey(t *testing.T) (crypto.PrivKey, peer.ID) {
	t.Helper()
	seed := bytes.Repeat([]byte{0x42}, 32)
	edPriv := ed25519.NewKeyFromSeed(seed)
	priv, err := crypto.UnmarshalEd25519PrivateKey(edPriv)
	if err != nil {
		t.Fatalf("unmarshal ed25519: %v", err)
	}
	pid, err := peer.IDFromPublicKey(priv.GetPublic())
	if err != nil {
		t.Fatalf("peer id: %v", err)
	}
	return priv, pid
}

// A tsnode-signed IPNS record must be a standard, self-consistent IPNS v2 record: it
// validates under boxo's own validator (the same spec rust_ipns implements), points at the
// `/ipfs/<cid>` value, and round-trips the sequence. This is the local half of the
// cross-stack gate; the Rust half lives in apps/desktop/src-tauri/src/ipns.rs.
func TestIPNSRecordIsStandardAndValid(t *testing.T) {
	priv, pid := deterministicKey(t)
	c, err := cid.Decode(fixtureCID)
	if err != nil {
		t.Fatalf("cid: %v", err)
	}
	rec, err := signIPNS(priv, c, time.Hour, 7)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if err := ipns.Validate(rec, priv.GetPublic()); err != nil {
		t.Fatalf("boxo Validate rejected our record: %v", err)
	}
	val, err := rec.Value()
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	if want := "/ipfs/" + fixtureCID; val.String() != want {
		t.Fatalf("value = %q, want %q", val.String(), want)
	}
	if seq, _ := rec.Sequence(); seq != 7 {
		t.Fatalf("sequence = %d, want 7", seq)
	}
	// The name the client resolves under is the publisher PeerId.
	if name := ipns.NameFromPeer(pid).String(); name == "" {
		t.Fatalf("empty IPNS name")
	}
}

// TestDumpRustFixture is not an assertion — run with TS_DUMP_FIXTURE=1 to emit the
// (name, base64-record) pair pasted into the Rust cross-stack test. The record carries a
// far-future EOL so the committed Rust fixture never expires.
func TestDumpRustFixture(t *testing.T) {
	if os.Getenv("TS_DUMP_FIXTURE") == "" {
		t.Skip("set TS_DUMP_FIXTURE=1 to regenerate the Rust cross-stack fixture")
	}
	priv, pid := deterministicKey(t)
	c, _ := cid.Decode(fixtureCID)
	// EOL far in the future (year ~2125) so the fixture is durable.
	eol := time.Date(2125, 1, 1, 0, 0, 0, 0, time.UTC)
	rec, err := ipns.NewRecord(priv, path.FromCid(c), 1, eol, ipns.DefaultRecordTTL)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	marshaled, err := ipns.MarshalRecord(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	fmt.Printf("\n=== RUST IPNS FIXTURE ===\nNAME=%s\nRECORD_B64=%s\n=========================\n",
		pid.String(), base64.StdEncoding.EncodeToString(marshaled))
}

// TestPublishResolveRoundTrip exercises the full node path: publish under a named key →
// resolve returns the same marshaled record (from the local store).
func TestPublishResolveRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	n, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer n.Close()

	c := rawBlock(t, []byte("catalog db root")).Cid()
	pid, marshaled, err := n.PublishIPNS(ctx, "catalog", c, time.Hour, 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	got, err := n.ResolveIPNS(ctx, pid.String())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !bytes.Equal(got, marshaled) {
		t.Fatalf("resolved record differs from published")
	}
	// key/gen is idempotent: a second publish under "catalog" keeps the same name.
	pid2, _, err := n.PublishIPNS(ctx, "catalog", c, time.Hour, 2)
	if err != nil {
		t.Fatalf("republish: %v", err)
	}
	if pid2 != pid {
		t.Fatalf("catalog key name changed: %s vs %s", pid2, pid)
	}
}
