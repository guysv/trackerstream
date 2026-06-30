package tsnode

// Block-forwarding (R5): a public, content-addressed alternative to an open relay. A donor node,
// on a /trackerstream/fwd/1.0.0 stream, fetches a CID on a caller's behalf (plain bitswap over its
// own connections) and streams back the HASH-VERIFIED block. The caller re-verifies. Safety is
// structural — only verifiable, size-capped blocks can cross, never an opaque tunnel — plus a
// per-peer rate cap + a global concurrency semaphore. No ACL: the protocol is public. See P2P-FWD.md.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"
)

const (
	fwdMaxFrame  = 4 << 20 // bound the reader: fits any block, caps a malicious varint length prefix
	fwdMaxCids   = 256     // max CIDs per Fetch (bounds donor fan-out)
	fwdCacheSize = 512     // forwarded-block cache entries (bounded — the node runs GC-disabled)
	fwdGlobalSem = 32      // global concurrent transitive fetches on the DONOR (the real Sybil backstop)
	fwdPeerLims  = 4096    // bounded per-peer limiter table (else a Sybil bloats it)
	fwdProvSeen  = 256     // bounded set of discovered content providers (forwarding-suppression input)
	fwdOutSem    = 16      // bound concurrent OUTBOUND forwards (caps a catalog GetMany fan-out)

	// per-peer cid-rate: tokens are CIDs; sustained 256/s, burst 512.
	fwdPeerRate  = rate.Limit(256)
	fwdPeerBurst = 512

	// response frame tags: [1-byte tag][body].
	fwdTagBlock byte = 1 // body = uvarint(len(cid)) ++ cid ++ rawData
	fwdTagMiss  byte = 2 // body = cid bytes
	fwdTagBusy  byte = 3 // body = empty
)

// Tunables kept as vars so tests can shrink them.
var (
	fwdServeTimeout = 15 * time.Second // donor per-cid GetBlock bound + stream deadline
	fwdCacheTTL     = 2 * time.Minute  // forwarded-block cache entry lifetime
)

// fwdState holds the donor-side rate cap + forwarded-block cache, plus the set of discovered content
// providers used to suppress forwarding once a hole-punch lands. Built in New().
type fwdState struct {
	cache *expirable.LRU[string, []byte]     // cid.KeyString() → raw block; size + TTL bounded
	sem   chan struct{}                      // donor-side global concurrent-fetch semaphore
	out   chan struct{}                      // caller-side outbound-forward semaphore (fan-out cap)
	lims  *lru.Cache[peer.ID, *rate.Limiter] // per-peer token buckets, bounded
	provs *lru.Cache[peer.ID, struct{}]      // content providers seen via DialProviders (bounded)
}

func newFwdState() *fwdState {
	lims, _ := lru.New[peer.ID, *rate.Limiter](fwdPeerLims)
	provs, _ := lru.New[peer.ID, struct{}](fwdProvSeen)
	return &fwdState{
		cache: expirable.NewLRU[string, []byte](fwdCacheSize, nil, fwdCacheTTL),
		sem:   make(chan struct{}, fwdGlobalSem),
		out:   make(chan struct{}, fwdOutSem),
		lims:  lims,
		provs: provs,
	}
}

// recordProvider notes a peer discovered as a content provider (via DialProviders) — the input to
// forwardingSuppressed's hole-punch-takeover check.
func (f *fwdState) recordProvider(p peer.ID) { f.provs.Add(p, struct{}{}) }

// allow charges n CIDs against the caller's per-peer bucket. The global semaphore (acquired per
// fetch in handleFwd) is the real backstop, since a Sybil can mint peer IDs to dodge this.
func (f *fwdState) allow(p peer.ID, n int) bool {
	l, ok := f.lims.Get(p)
	if !ok {
		l = rate.NewLimiter(fwdPeerRate, fwdPeerBurst)
		f.lims.Add(p, l)
	}
	return l.AllowN(time.Now(), n)
}

func (f *fwdState) cacheGet(c cid.Cid) []byte {
	if v, ok := f.cache.Get(c.KeyString()); ok {
		return v
	}
	return nil
}

func (f *fwdState) cachePut(c cid.Cid, data []byte) { f.cache.Add(c.KeyString(), data) }

// fwdRequest is the single request frame (JSON): the caller's wanted CIDs.
type fwdRequest struct {
	Cids []string `json:"cids"`
}

// handleFwd is the donor side. INVARIANT: it calls plain n.GetBlock — NEVER GetBlockAssisted —
// or a Fetch to A would trigger A→B→C… transitive forwarding recursion.
func (n *Node) handleFwd(s network.Stream) {
	defer s.Close()
	// Donor gate: only a publicly-reachable node serves forwarding (the seed never even registers
	// this handler). A node not confirmed public isn't a useful donor — refuse so the caller falls
	// through to a real public donor. Mirrors when the infinite-limit relay activates.
	if n.control.Reachable() != "public" {
		s.Reset()
		return
	}
	p := s.Conn().RemotePeer()
	_ = s.SetDeadline(time.Now().Add(fwdServeTimeout))

	r := msgio.NewVarintReaderSize(s, fwdMaxFrame)
	reqBytes, err := r.ReadMsg()
	if err != nil {
		s.Reset()
		return
	}
	var req fwdRequest
	jerr := json.Unmarshal(reqBytes, &req)
	r.ReleaseMsg(reqBytes)
	if jerr != nil {
		s.Reset()
		return
	}
	if len(req.Cids) > fwdMaxCids {
		req.Cids = req.Cids[:fwdMaxCids]
	}

	w := msgio.NewVarintWriter(s)
	if !n.fwd.allow(p, len(req.Cids)) {
		_ = w.WriteMsg([]byte{fwdTagBusy})
		return
	}

	for _, cs := range req.Cids {
		c, err := cid.Decode(cs)
		if err != nil {
			continue // undecodable cid → no response; caller sees EOF → miss
		}
		data := n.fwdServe(c)
		if data == nil {
			_ = writeFrame(w, fwdTagMiss, c.Bytes())
			continue
		}
		_ = writeBlockFrame(w, c, data)
	}
}

// fwdServe returns the raw bytes for c from the cache, else a plain transitive bitswap fetch
// (bounded by the global semaphore + a short timeout). nil = miss. Caches hits.
func (n *Node) fwdServe(c cid.Cid) []byte {
	if data := n.fwd.cacheGet(c); data != nil {
		return data
	}
	select {
	case n.fwd.sem <- struct{}{}:
	default:
		return nil // global cap reached → treat as miss (caller backs off / tries elsewhere)
	}
	defer func() { <-n.fwd.sem }()

	// ctxNoForward keeps this fetch on RAW bitswap — the donor must never re-forward (anti-
	// amplification). n.GetBlock rides the assisted exchange, so without this marker it would recurse.
	ctx, cancel := context.WithTimeout(ctxNoForward(context.Background()), fwdServeTimeout)
	defer cancel()
	b, err := n.GetBlock(ctx, c)
	if err != nil {
		return nil
	}
	data := b.RawData()
	n.fwd.cachePut(c, data)
	return data
}

// fwdHit pairs a forwarded block with the donor that served it (so we can keep it warm).
type fwdHit struct {
	blk  blocks.Block
	from peer.ID
}

// FwdFetch asks connected forwarders for c and returns the first verified block. Provider/DHT-
// independent: it queries CONNECTED donors directly, so a non-root interior block that is never
// advertised on the DHT is still served (the keystone for interior track blocks).
func (n *Node) FwdFetch(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	targets := n.fwdTargets()
	if len(targets) == 0 {
		return nil, fmt.Errorf("fwd: no targets for %s", c)
	}
	// Bound concurrent outbound forwards (caps a catalog GetMany fan-out); over cap → skip, the raw
	// bitswap arm still runs.
	select {
	case n.fwd.out <- struct{}{}:
		defer func() { <-n.fwd.out }()
	default:
		return nil, fmt.Errorf("fwd: outbound cap reached")
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	res := make(chan fwdHit, len(targets))
	var wg sync.WaitGroup
	for _, p := range targets {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			if b, err := n.fwdFetchFrom(cctx, p, c); err == nil && b != nil {
				select {
				case res <- fwdHit{b, p}:
				case <-cctx.Done():
				}
			}
		}(p)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case h := <-res:
		n.control.Warm(h.from) // keep the forwarder for the rest of the track's interior blocks
		return h.blk, nil
	case <-done:
		return nil, fmt.Errorf("fwd: all %d targets missed %s", len(targets), c)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// fwdTargets is the connected, non-seed, non-self peer set. The seed is excluded — it's reachable
// by direct bitswap already (GetBlockAssisted runs that in parallel); fwd is for peer offload.
// Protocol support is probed lazily by fwdFetchFrom's NewStream (negotiation failure → skip),
// which avoids racing identify in tests.
func (n *Node) fwdTargets() []peer.ID {
	self := n.host.ID()
	var out []peer.ID
	for _, p := range n.host.Network().Peers() {
		if p == self {
			continue
		}
		if _, isSeed := n.seeds[p]; isSeed {
			continue
		}
		out = append(out, p)
	}
	return out
}

// fwdFetchFrom requests c from one donor over /trackerstream/fwd/1.0.0 and returns the block iff
// the returned bytes hash to c. The hash check IS the safety model (do not rely on bitswap's
// donor-side verification — the bytes crossed an untrusted stream after that).
func (n *Node) fwdFetchFrom(ctx context.Context, p peer.ID, c cid.Cid) (blocks.Block, error) {
	s, err := n.host.NewStream(ctx, p, FwdProtocol)
	if err != nil {
		return nil, err // unsupported / unreachable → skip this target
	}
	defer s.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = s.SetDeadline(dl)
	} else {
		_ = s.SetDeadline(time.Now().Add(fwdServeTimeout))
	}

	w := msgio.NewVarintWriter(s)
	req, _ := json.Marshal(fwdRequest{Cids: []string{c.String()}})
	if err := w.WriteMsg(req); err != nil {
		return nil, err
	}
	_ = s.CloseWrite()

	r := msgio.NewVarintReaderSize(s, fwdMaxFrame)
	for {
		frame, err := r.ReadMsg()
		if err != nil {
			return nil, err // EOF before a Block → miss
		}
		if len(frame) == 0 {
			r.ReleaseMsg(frame)
			continue
		}
		tag, body := frame[0], frame[1:]
		switch tag {
		case fwdTagBusy:
			r.ReleaseMsg(frame)
			return nil, fmt.Errorf("fwd: %s busy", p)
		case fwdTagMiss:
			r.ReleaseMsg(frame)
			continue
		case fwdTagBlock:
			cidLen, nn := binary.Uvarint(body)
			if nn <= 0 || uint64(len(body)-nn) < cidLen {
				r.ReleaseMsg(frame)
				return nil, fmt.Errorf("fwd: malformed block frame from %s", p)
			}
			data := append([]byte(nil), body[nn+int(cidLen):]...) // copy out before ReleaseMsg
			r.ReleaseMsg(frame)
			// Verify: bytes MUST hash to the CID we asked for. cid.Prefix().Sum is the multihash check.
			chk, err := c.Prefix().Sum(data)
			if err != nil || !chk.Equals(c) {
				return nil, fmt.Errorf("fwd: hash mismatch for %s from %s", c, p)
			}
			blk, _ := blocks.NewBlockWithCid(data, c)
			_ = n.PutBlock(context.Background(), blk) // cache locally + notify bitswap
			return blk, nil
		default:
			r.ReleaseMsg(frame)
		}
	}
}

// forwardingSuppressed reports that a direct hole-punched path to a content provider exists, so the
// query node should leave fetching to bitswap (direct, one hop) and not touch donors. True iff at
// least one discovered provider is directly Connected AND none is still relay-only (Limited). A
// Limited provider (hole-punch pending) keeps forwarding on; the instant DCUtR upgrades it to
// Connected this flips — so direct takes over with zero lag. Empty/unknown ⇒ not suppressed
// (forward eagerly — the initial window, before any provider is discovered).
func (n *Node) forwardingSuppressed() bool {
	sawDirect := false
	for _, p := range n.fwd.provs.Keys() {
		switch n.host.Network().Connectedness(p) {
		case network.Limited:
			return false // a provider reachable only via relay → keep forwarding
		case network.Connected:
			sawDirect = true
		}
	}
	return sawDirect
}

// noForwardKey marks a context whose fetches must use RAW bitswap (no forwarding). The donor's own
// transitive fetch uses it, so serving a Fetch can never trigger another Fetch (anti-amplification).
type noForwardKey struct{}

func ctxNoForward(ctx context.Context) context.Context {
	return context.WithValue(ctx, noForwardKey{}, true)
}
func isNoForward(ctx context.Context) bool { v, _ := ctx.Value(noForwardKey{}).(bool); return v }

// fwdExchange wraps the bitswap exchange so EVERY blockservice fetch — single (block/get), batch,
// and session (the catalog DAG walk via blockservice sessions) — transparently gains the forwarding
// fallback. It's the single chokepoint for all of the node's bitswap fetches. The donor's own
// transitive fetch passes a ctxNoForward context, so it stays on raw bitswap.
type fwdExchange struct {
	exchange.SessionExchange
	n *Node
}

func (e *fwdExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return assistGet(ctx, c, e.n, e.SessionExchange.GetBlock)
}

func (e *fwdExchange) GetBlocks(ctx context.Context, ks []cid.Cid) (<-chan blocks.Block, error) {
	return assistGetMany(ctx, ks, e.n, e.SessionExchange.GetBlocks)
}

func (e *fwdExchange) NewSession(ctx context.Context) exchange.Fetcher {
	return &fwdFetcher{Fetcher: e.SessionExchange.NewSession(ctx), n: e.n}
}

// fwdFetcher applies the same forwarding fallback inside a bitswap SESSION — the catalog DAG-walk
// path (merkledag GetMany → blockservice session → exchange.NewSession).
type fwdFetcher struct {
	exchange.Fetcher
	n *Node
}

func (f *fwdFetcher) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return assistGet(ctx, c, f.n, f.Fetcher.GetBlock)
}

func (f *fwdFetcher) GetBlocks(ctx context.Context, ks []cid.Cid) (<-chan blocks.Block, error) {
	return assistGetMany(ctx, ks, f.n, f.Fetcher.GetBlocks)
}

// assistGet races a raw single-block fetch with block-forwarding (grace 0ms). Forwarding is skipped
// when the ctx is no-forward (donor path), forwarding isn't applicable (no node yet), or a
// hole-punch has taken over (forwardingSuppressed) — then it's a plain raw fetch. A forwarded
// PutBlock also satisfies the in-flight raw fetch via NotifyNewBlocks, so either arm may win; once a
// direct path exists, raw bitswap wins in one hop anyway.
func assistGet(ctx context.Context, c cid.Cid, n *Node, raw func(context.Context, cid.Cid) (blocks.Block, error)) (blocks.Block, error) {
	if n == nil || isNoForward(ctx) || n.forwardingSuppressed() {
		return raw(ctx, c)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	res := make(chan blocks.Block, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if b, err := raw(cctx, c); err == nil {
			select {
			case res <- b:
			case <-cctx.Done():
			}
		}
	}()
	go func() {
		defer wg.Done()
		if b, err := n.FwdFetch(cctx, c); err == nil && b != nil {
			select {
			case res <- b:
			case <-cctx.Done():
			}
		}
	}()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case b := <-res:
		return b, nil
	case <-done:
		return nil, fmt.Errorf("get block %s: not found via bitswap or forwarding", c)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// assistGetMany returns raw bitswap's batch channel and, in parallel (unless no-forward/suppressed),
// asks donors for each cid; a forwarded PutBlock surfaces on the raw channel via NotifyNewBlocks.
// Bounded by fwd.out (excess cids just skip forwarding and rely on bitswap).
func assistGetMany(ctx context.Context, ks []cid.Cid, n *Node, raw func(context.Context, []cid.Cid) (<-chan blocks.Block, error)) (<-chan blocks.Block, error) {
	out, err := raw(ctx, ks)
	if err != nil || n == nil || isNoForward(ctx) || n.forwardingSuppressed() {
		return out, err
	}
	for _, c := range ks {
		c := c
		go func() { _, _ = n.FwdFetch(ctx, c) }()
	}
	return out, nil
}

// writeFrame writes a [tag][body] response frame.
func writeFrame(w msgio.Writer, tag byte, body []byte) error {
	buf := make([]byte, 0, 1+len(body))
	buf = append(buf, tag)
	buf = append(buf, body...)
	return w.WriteMsg(buf)
}

// writeBlockFrame writes [fwdTagBlock][uvarint(len(cid))][cid][rawData].
func writeBlockFrame(w msgio.Writer, c cid.Cid, data []byte) error {
	cb := c.Bytes()
	var hdr [binary.MaxVarintLen64]byte
	nn := binary.PutUvarint(hdr[:], uint64(len(cb)))
	buf := make([]byte, 0, 1+nn+len(cb)+len(data))
	buf = append(buf, fwdTagBlock)
	buf = append(buf, hdr[:nn]...)
	buf = append(buf, cb...)
	buf = append(buf, data...)
	return w.WriteMsg(buf)
}

// donorPeerSource feeds AutoRelay relay candidates (R5 Phase B): the bootstrap seeds FIRST — so
// master-as-relay survives even when the rendezvous is cold (a NAT'd client with no reservation
// would otherwise have no circuit addr, stranding DCUtR) — then publicly-reachable donors found via
// the donorRendezvous provider records. It captures &idht because idht is assigned inside the
// Routing closure DURING libp2p.New; the source is only CALLED later by AutoRelay, so it's set. The
// channel MUST be closed (else AutoRelay parks forever and never re-queries) and every send selects
// on ctx.Done (the reader can stop reading).
func donorPeerSource(idhtPtr **dht.IpfsDHT, self peer.ID, bootstrap []peer.AddrInfo) func(context.Context, int) <-chan peer.AddrInfo {
	return func(ctx context.Context, num int) <-chan peer.AddrInfo {
		out := make(chan peer.AddrInfo)
		go func() {
			defer close(out)
			sent := 0
			emit := func(ai peer.AddrInfo) bool {
				if sent >= num {
					return false
				}
				select {
				case out <- ai:
					sent++
					return true
				case <-ctx.Done():
					return false
				}
			}
			seen := map[peer.ID]struct{}{self: {}}
			for _, ai := range bootstrap {
				if _, dup := seen[ai.ID]; dup {
					continue
				}
				seen[ai.ID] = struct{}{}
				if !emit(ai) {
					return
				}
			}
			idht := *idhtPtr
			if idht == nil {
				return
			}
			fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			for p := range idht.FindProvidersAsync(fctx, donorRendezvous, num) {
				if _, dup := seen[p.ID]; dup {
					continue
				}
				seen[p.ID] = struct{}{}
				if !emit(p) {
					return
				}
			}
		}()
		return out
	}
}

// relayHopID extracts B from a /…/p2p/<B>/p2p-circuit/… addr — the donor that fronts a NAT'd
// provider. Every tsnode donor runs both the clamped relay and the forwarder, so the circuit-relay
// hop IS the forwarder; this is how a fetcher learns which donor to keep warm (the /p2p-fwd hint).
func relayHopID(a ma.Multiaddr) (peer.ID, bool) {
	s := a.String()
	idx := strings.Index(s, "/p2p-circuit")
	if idx < 0 {
		return "", false
	}
	before := s[:idx]
	j := strings.LastIndex(before, "/p2p/")
	if j < 0 {
		return "", false
	}
	pid, err := peer.Decode(before[j+len("/p2p/"):])
	if err != nil {
		return "", false
	}
	return pid, true
}
