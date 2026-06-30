package tsnode

import (
	"context"
	"io"
	"sync"

	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uih "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

// Cat reads a byte range of a UnixFS file by CID, fetching only the leaf blocks the
// range covers (boxo's DAG reader is a seekable ReadSeeker over Bitswap). This is the
// primitive the catalog VFS rides — `cat?offset&length` → a few index/page blocks, not
// the whole DB. `offset` >= 0; `length` < 0 means "to EOF".
func (n *Node) Cat(ctx context.Context, c cid.Cid, offset, length int64) ([]byte, error) {
	return n.catRead(ctx, merkledag.NewDAGService(n.bserv), c, offset, length)
}

// CatCatalog is Cat for the catalog read path: it additionally advertises the LEAF page blocks
// it fetched as catalog pieces, so this node becomes a discoverable provider of the catalog
// pages it holds (peer page-sharing offloads the seed). The client can't do this itself — it
// only knows the catalog root and byte offsets; the per-page CIDs live here, where the DAG is
// walked. Interior index nodes are not advertised (only the data pages peers actually want).
func (n *Node) CatCatalog(ctx context.Context, c cid.Cid, offset, length int64) ([]byte, error) {
	rec := &leafRecorder{DAGService: merkledag.NewDAGService(n.bserv)}
	data, err := n.catRead(ctx, rec, c, offset, length)
	if err == nil {
		n.provideCatalogPieces(rec.take())
		// Advertise the catalog root as a source so peers' dial-providers(catalogRoot) can find
		// + connect to us (clients only know the root, not page CIDs — the root is the dialable
		// rendezvous, the catalog analog of a track root).
		n.provideCatalogSource(c)
	}
	return data, err
}

func (n *Node) catRead(ctx context.Context, dserv ipld.DAGService, c cid.Cid, offset, length int64) ([]byte, error) {
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

// leafRecorder wraps a DAGService and records the CIDs of LEAF nodes (no links — the UnixFS data
// pages) it serves, so the catalog read path can advertise exactly the pages it fetched.
type leafRecorder struct {
	ipld.DAGService
	mu     sync.Mutex
	leaves []cid.Cid
}

func (g *leafRecorder) note(nd ipld.Node, err error) {
	if err == nil && nd != nil && len(nd.Links()) == 0 {
		g.mu.Lock()
		g.leaves = append(g.leaves, nd.Cid())
		g.mu.Unlock()
	}
}

func (g *leafRecorder) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	nd, err := g.DAGService.Get(ctx, c)
	g.note(nd, err)
	return nd, err
}

func (g *leafRecorder) GetMany(ctx context.Context, cs []cid.Cid) <-chan *ipld.NodeOption {
	in := g.DAGService.GetMany(ctx, cs)
	out := make(chan *ipld.NodeOption)
	go func() {
		defer close(out)
		for opt := range in {
			if opt != nil {
				g.note(opt.Node, opt.Err)
			}
			out <- opt
		}
	}()
	return out
}

func (g *leafRecorder) take() []cid.Cid {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leaves
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
