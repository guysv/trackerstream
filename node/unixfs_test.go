package tsnode

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// AddUnixFS must build a multi-block UnixFS DAG (catalog-publish settings: 16KB chunks,
// dag-pb leaves, CIDv1) and Cat must read arbitrary byte ranges out of it fetching only the
// covered leaves — the catalog VFS primitive. Verifies both whole-file and ranged reads.
func TestAddAndRangedCat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n, err := New(ctx, DefaultConfig(RoleServer, "", 0))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer n.Close()

	// 100KB of position-dependent bytes spanning several 16KB leaves.
	data := make([]byte, 100*1024)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	root, err := n.AddUnixFS(ctx, bytes.NewReader(data), AddOptions{ChunkSize: 16384, Pin: true})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if !n.Pins().Has(root) {
		t.Fatalf("root not pinned after add")
	}

	// Whole-file read.
	whole, err := n.Cat(ctx, root, 0, -1)
	if err != nil {
		t.Fatalf("cat whole: %v", err)
	}
	if !bytes.Equal(whole, data) {
		t.Fatalf("whole-file mismatch (%d vs %d bytes)", len(whole), len(data))
	}

	// Ranged reads across leaf boundaries (mirrors the SQLite page fetch).
	for _, tc := range []struct{ off, length int64 }{
		{0, 4096}, {16384, 100}, {16000, 1024}, {99000, 2000 /* overhangs EOF */}, {50000, 16384},
	} {
		got, err := n.Cat(ctx, root, tc.off, tc.length)
		if err != nil {
			t.Fatalf("cat [%d,+%d): %v", tc.off, tc.length, err)
		}
		end := tc.off + tc.length
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		want := data[tc.off:end]
		if !bytes.Equal(got, want) {
			t.Fatalf("range [%d,+%d): got %d bytes, want %d", tc.off, tc.length, len(got), len(want))
		}
	}
	_ = time.Second
}
