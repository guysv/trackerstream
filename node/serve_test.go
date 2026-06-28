package tsnode

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Reproduce the binary's exact serving path: node A stores a block via the RPC `block/put`
// handler (not the in-process PutBlock helper), then node B fetches it over Bitswap. This is
// the cross-process scenario the client-seam integration exercises — if the engine can't serve
// an RPC-stored block, this fails in-process where it's debuggable.
func TestRPCStoredBlockServedOverBitswap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a, err := New(ctx, DefaultConfig(RoleServer, t.TempDir(), 0))
	if err != nil {
		t.Fatalf("node A: %v", err)
	}
	defer a.Close()
	b, err := New(ctx, DefaultConfig(RoleClient, t.TempDir(), 0))
	if err != nil {
		t.Fatalf("node B: %v", err)
	}
	defer b.Close()

	// Store on A via the RPC handler (the binary's path).
	ts := httptest.NewServer(NewRPCServer(a).Handler())
	defer ts.Close()
	data := []byte("served-over-bitswap from an RPC-stored block")
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("data", "block")
	_, _ = fw.Write(data)
	_ = mw.Close()
	req, _ := http.NewRequest("POST", ts.URL+"/api/v0/block/put?cid-codec=raw", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("block/put: %v", err)
	}
	resp.Body.Close()

	// Compute the same CID locally to ask for it.
	blk := rawBlock(t, data)

	// A can serve it locally.
	if _, err := a.GetBlock(ctx, blk.Cid()); err != nil {
		t.Fatalf("A cannot get its own RPC-stored block: %v", err)
	}

	// B connects and fetches over Bitswap.
	if err := b.Connect(ctx, peer.AddrInfo{ID: a.ID(), Addrs: a.Addrs()}); err != nil {
		t.Fatalf("B connect: %v", err)
	}
	got, err := b.GetBlock(ctx, blk.Cid())
	if err != nil {
		t.Fatalf("B get over bitswap: %v", err)
	}
	if !bytes.Equal(got.RawData(), data) {
		t.Fatalf("bytes mismatch")
	}
}
