package tsnode

import (
	"context"
	"io"

	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uih "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// Cat reads a byte range of a UnixFS file by CID, fetching only the leaf blocks the
// range covers (boxo's DAG reader is a seekable ReadSeeker over Bitswap). This is the
// primitive the catalog VFS rides — `cat?offset&length` → a few index/page blocks, not
// the whole DB. `offset` >= 0; `length` < 0 means "to EOF".
func (n *Node) Cat(ctx context.Context, c cid.Cid, offset, length int64) ([]byte, error) {
	dserv := merkledag.NewDAGService(n.bserv)
	nd, err := dserv.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	r, err := uio.NewDagReader(ctx, nd, dserv)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if _, err := r.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	if length < 0 {
		return io.ReadAll(r)
	}
	buf := make([]byte, length)
	read, err := io.ReadFull(r, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// A short read at EOF is fine — return what we got (the VFS asks for a fixed
		// page size that may overhang the file's last page).
		return buf[:read], nil
	}
	if err != nil {
		return nil, err
	}
	return buf[:read], nil
}

// AddOptions controls how AddUnixFS builds the DAG. The defaults mirror the catalog ingest
// publish (kubo `add --chunker=size-16384 --raw-leaves=false --cid-version=1`), so the root
// CID is byte-identical to what prod publishes and shipped clients can read it.
type AddOptions struct {
	ChunkSize int64 // splitter size in bytes (0 → 16384, page-aligned for the SQLite VFS)
	RawLeaves bool  // false → dag-pb leaves (rust-unixfs can't walk raw leaves)
	Pin       bool  // pin (and provide) the resulting root
}

// AddUnixFS builds a UnixFS DAG from r, stores every block, and returns the root CID. This is
// the `add` RPC primitive (catalog DB → IPFS). CIDv1 / sha2-256 / dag-pb throughout.
func (n *Node) AddUnixFS(ctx context.Context, r io.Reader, opts AddOptions) (cid.Cid, error) {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 16384
	}
	dserv := merkledag.NewDAGService(n.bserv)
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return cid.Undef, err
	}
	prefix.MhType = mh.SHA2_256
	dbp := uih.DagBuilderParams{
		Maxlinks:   uih.DefaultLinksPerBlock,
		RawLeaves:  opts.RawLeaves,
		CidBuilder: &prefix,
		Dagserv:    dserv,
	}
	db, err := dbp.New(chunker.NewSizeSplitter(r, opts.ChunkSize))
	if err != nil {
		return cid.Undef, err
	}
	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}
	root := nd.Cid()
	if opts.Pin {
		if err := n.Pin(ctx, root); err != nil {
			return cid.Undef, err
		}
	}
	return root, nil
}
