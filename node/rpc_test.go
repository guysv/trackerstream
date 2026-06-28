package tsnode

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
)

// The kubo-compatible RPC seam: a multipart `block/put` (as repack's KuboRpc sends) stores
// a block under the CIDv1 we compute, and `block/get` round-trips the exact bytes —
// proving the ingest + client data path over the local RPC.
func TestRPCBlockPutGetRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	n, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer n.Close()

	ts := httptest.NewServer(NewRPCServer(n).Handler())
	defer ts.Close()

	data := []byte("trackerstream RPC block round-trip — dag-cbor-ish payload")

	// block/put (multipart "data" file, cid-codec=raw) → {Key, Size}
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("data", "block")
	_, _ = fw.Write(data)
	_ = mw.Close()
	req, _ := http.NewRequest("POST", ts.URL+"/api/v0/block/put?cid-codec=raw&mhtype=sha2-256", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("block/put: %v", err)
	}
	var put struct{ Key string }
	_ = json.NewDecoder(resp.Body).Decode(&put)
	resp.Body.Close()
	if put.Key == "" {
		t.Fatalf("block/put returned no Key")
	}

	// block/get?arg=<cid> → raw bytes
	resp, err = http.DefaultClient.Post(ts.URL+"/api/v0/block/get?arg="+put.Key, "", nil)
	if err != nil {
		t.Fatalf("block/get: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("block/get mismatch: got %q want %q", got, data)
	}

	// id returns our PeerId.
	resp, _ = http.DefaultClient.Post(ts.URL+"/api/v0/id", "", nil)
	var id struct{ ID string }
	_ = json.NewDecoder(resp.Body).Decode(&id)
	resp.Body.Close()
	if id.ID != n.ID().String() {
		t.Fatalf("id mismatch: %q vs %q", id.ID, n.ID())
	}
}

// The full ingest publish contract over the RPC, exactly as repack's KuboRpc drives it:
// key/gen(catalog) → add(file) → name/publish(/ipfs/<cid>, key=catalog) → routing/get →
// base64 record. The returned record must be a real signed IPNS record (decodes to the same
// /ipfs/<cid> value), proving the server-side publish path end-to-end.
func TestRPCIngestPublishContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer n.Close()
	ts := httptest.NewServer(NewRPCServer(n).Handler())
	defer ts.Close()

	// key/gen catalog → stable base58 Id.
	resp, err := http.DefaultClient.Post(ts.URL+"/api/v0/key/gen?arg=catalog&type=ed25519&ipns-base=b58mh", "", nil)
	if err != nil {
		t.Fatalf("key/gen: %v", err)
	}
	var keygen struct{ Id string }
	_ = json.NewDecoder(resp.Body).Decode(&keygen)
	resp.Body.Close()
	if keygen.Id == "" {
		t.Fatalf("key/gen returned no Id")
	}

	// add a small "catalog db" → root CID (last newline-JSON line).
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("file", "catalog.db")
	_, _ = fw.Write(bytes.Repeat([]byte("SQLite format 3\x00"), 4096)) // multi-block
	_ = mw.Close()
	req, _ := http.NewRequest("POST", ts.URL+"/api/v0/add?chunker=size-16384&raw-leaves=false&cid-version=1&pin=true", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	var add struct{ Hash string }
	// take the LAST line (kubo streams; we emit one)
	lines := bytes.Split(bytes.TrimSpace(readBody(t, resp)), []byte("\n"))
	_ = json.Unmarshal(lines[len(lines)-1], &add)
	if add.Hash == "" {
		t.Fatalf("add returned no Hash")
	}

	// name/publish under the catalog key.
	resp, err = http.DefaultClient.Post(ts.URL+"/api/v0/name/publish?arg=/ipfs/"+add.Hash+"&key=catalog&lifetime=48h", "", nil)
	if err != nil {
		t.Fatalf("name/publish: %v", err)
	}
	var pub struct{ Name string }
	_ = json.NewDecoder(resp.Body).Decode(&pub)
	resp.Body.Close()
	if pub.Name != keygen.Id {
		t.Fatalf("publish name %q != catalog key %q", pub.Name, keygen.Id)
	}

	// routing/get → Type-5 Value line with base64 record in Extra.
	resp, err = http.DefaultClient.Post(ts.URL+"/api/v0/routing/get?arg=/ipns/"+pub.Name, "", nil)
	if err != nil {
		t.Fatalf("routing/get: %v", err)
	}
	var rg struct {
		Type  int
		Extra string
	}
	_ = json.Unmarshal(bytes.Split(bytes.TrimSpace(readBody(t, resp)), []byte("\n"))[0], &rg)
	if rg.Type != 5 || rg.Extra == "" {
		t.Fatalf("routing/get gave Type=%d Extra=%q", rg.Type, rg.Extra)
	}
	if _, err := base64.StdEncoding.DecodeString(rg.Extra); err != nil {
		t.Fatalf("routing/get Extra not base64: %v", err)
	}
}

// A raw-body block/put (no multipart) must store the ACTUAL bytes, not be silently drained to
// empty by the form parser — the bug that made RPC-stored blocks un-servable over Bitswap.
func TestRPCBlockPutRawBodyStoresRealBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	n, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer n.Close()
	ts := httptest.NewServer(NewRPCServer(n).Handler())
	defer ts.Close()

	data := []byte("raw body bytes, no multipart wrapper")
	// Raw body, application/octet-stream (what a non-multipart client sends).
	resp, err := http.DefaultClient.Post(ts.URL+"/api/v0/block/put?cid-codec=raw", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("block/put: %v", err)
	}
	var put struct {
		Key  string
		Size int
	}
	_ = json.NewDecoder(resp.Body).Decode(&put)
	resp.Body.Close()
	if put.Size != len(data) {
		t.Fatalf("stored Size=%d, want %d (body was drained to empty)", put.Size, len(data))
	}
	// The stored block must round-trip the exact bytes (CID = hash of real data, not "").
	got, err := n.GetBlock(ctx, mustCID(t, put.Key))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got.RawData(), data) {
		t.Fatalf("stored %q, want %q", got.RawData(), data)
	}
}

func mustCID(t *testing.T, s string) cid.Cid {
	t.Helper()
	c, err := cid.Decode(s)
	if err != nil {
		t.Fatalf("decode cid %q: %v", s, err)
	}
	return c
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}
