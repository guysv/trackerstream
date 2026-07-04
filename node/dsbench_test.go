package tsnode

// Throwaway lab benchmark (guarded by DSBENCH=1) comparing block-write strategies
// against the EXACT datastore+blockstore stack tsnode uses, to quantify the
// fsync-per-block floor and the batched-PutMany fix. Run:
//   DSBENCH=1 DSBENCH_N=20000 go test -run TestBulkIngestBench -v -timeout 20m
import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	levelds "github.com/ipfs/go-ds-leveldb"
)

// makeBlocks builds n blocks with realistic module-chunk sizes (~avg 60 KB,
// varied) and incompressible-ish content so leveldb can't cheat via snappy.
func makeBlocks(n int) []blocks.Block {
	rng := rand.New(rand.NewSource(1)) // fixed seed: reproducible, no wall-clock
	out := make([]blocks.Block, n)
	for i := 0; i < n; i++ {
		size := 8*1024 + rng.Intn(120*1024) // 8 KB .. 128 KB
		buf := make([]byte, size)
		rng.Read(buf)
		out[i] = blocks.NewBlock(buf) // CIDv0 sha256 of content
	}
	return out
}

func openBS(t *testing.T, noSync bool) (blockstore.Blockstore, func()) {
	dir, err := os.MkdirTemp("", "dsbench-")
	if err != nil {
		t.Fatal(err)
	}
	var opts *levelds.Options
	if noSync {
		opts = &levelds.Options{NoSync: true}
	}
	dstore, err := levelds.NewDatastore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	bs := blockstore.NewBlockstore(dstore)
	return bs, func() { dstore.Close(); os.RemoveAll(dir) }
}

func TestBulkIngestBench(t *testing.T) {
	if os.Getenv("DSBENCH") != "1" {
		t.Skip("set DSBENCH=1 to run the lab benchmark")
	}
	n := 20000
	if v := os.Getenv("DSBENCH_N"); v != "" {
		fmt.Sscanf(v, "%d", &n)
	}
	batch := 512
	if v := os.Getenv("DSBENCH_BATCH"); v != "" {
		fmt.Sscanf(v, "%d", &batch)
	}
	ctx := context.Background()
	blks := makeBlocks(n)
	var totalBytes int64
	for _, b := range blks {
		totalBytes += int64(len(b.RawData()))
	}
	fmt.Printf("\n=== bulk-ingest bench: %d blocks, %.1f MB, batch=%d ===\n",
		n, float64(totalBytes)/1e6, batch)

	report := func(label string, d time.Duration) {
		fmt.Printf("  %-28s %7.1f blk/s  %6.1f MB/s  (%.1fs)\n",
			label, float64(n)/d.Seconds(),
			float64(totalBytes)/1e6/d.Seconds(), d.Seconds())
	}

	// A) CURRENT: one Put per block, Sync:true (fsync per block).
	{
		bs, cleanup := openBS(t, false)
		t0 := time.Now()
		for _, b := range blks {
			if err := bs.Put(ctx, b); err != nil {
				t.Fatal(err)
			}
		}
		report("A) Put-per-block (Sync)", time.Since(t0))
		cleanup()
	}

	// B) FIX: PutMany in batches — one leveldb Batch (one fsync) per batch, durable.
	{
		bs, cleanup := openBS(t, false)
		t0 := time.Now()
		for i := 0; i < len(blks); i += batch {
			end := i + batch
			if end > len(blks) {
				end = len(blks)
			}
			if err := bs.PutMany(ctx, blks[i:end]); err != nil {
				t.Fatal(err)
			}
		}
		report("B) PutMany batched (Sync)", time.Since(t0))
		cleanup()
	}

	// C) REFERENCE: Put per block but datastore NoSync (fast, but NOT durable on crash).
	{
		bs, cleanup := openBS(t, true)
		t0 := time.Now()
		for _, b := range blks {
			if err := bs.Put(ctx, b); err != nil {
				t.Fatal(err)
			}
		}
		report("C) Put-per-block (NoSync)", time.Since(t0))
		cleanup()
	}
}
