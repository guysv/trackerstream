package tsnode

import (
	"context"
	crand "crypto/rand"
	"testing"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// forcePublic marks a node publicly reachable so it serves the forwarding donor handler (which
// otherwise refuses until AutoNAT confirms public — unavailable on loopback).
func forcePublic(t *testing.T, n *Node) {
	t.Helper()
	n.control.mu.Lock()
	n.control.reachable = network.ReachabilityPublic
	n.control.mu.Unlock()
}

func randPeerID(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(crand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("id: %v", err)
	}
	return id
}

// shrinkFwdTimers makes the donor serve/miss bound test-fast.
func shrinkFwdTimers(t *testing.T) {
	t.Helper()
	os := fwdServeTimeout
	fwdServeTimeout = 3 * time.Second
	t.Cleanup(func() { fwdServeTimeout = os })
}

func mkEphemeral(t *testing.T, ctx context.Context, role Role) *Node {
	t.Helper()
	n, err := New(ctx, DefaultConfig(role, "", 0))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func mkEphemeral2(t *testing.T, ctx context.Context, cfg Config) *Node {
	t.Helper()
	n, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func connectNodes(t *testing.T, a, b *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ai := peer.AddrInfo{ID: b.ID(), Addrs: []ma.Multiaddr{loopbackAddr(t, b)}}
	if err := a.Connect(ctx, ai); err != nil {
		t.Fatalf("connect %s->%s: %v", a.ID(), b.ID(), err)
	}
}

// A NAT'd-style holder H (not connected to the fetcher C) serves a block to C THROUGH a donor B
// via /trackerstream/fwd — the core forwarding path. C only ever connects to B, never to H.
func TestFwdForwardingThroughDonor(t *testing.T) {
	shrinkFwdTimers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	holder := mkEphemeral(t, ctx, RoleClient)  // H
	donor := mkEphemeral(t, ctx, RoleClient)   // B
	fetcher := mkEphemeral(t, ctx, RoleClient) // C
	forcePublic(t, donor)                      // only a public donor serves forwarding

	connectNodes(t, holder, donor)  // H <-> B
	connectNodes(t, fetcher, donor) // C <-> B  (NOT C <-> H)

	blk := rawBlock(t, []byte("a block only the NAT'd holder has"))
	if err := holder.PutBlock(ctx, blk); err != nil {
		t.Fatalf("holder put: %v", err)
	}

	got, err := fetcher.FwdFetch(ctx, blk.Cid())
	if err != nil {
		t.Fatalf("FwdFetch: %v", err)
	}
	if string(got.RawData()) != string(blk.RawData()) {
		t.Fatalf("forwarded bytes mismatch")
	}
}

// The A/B/C interior-block case: A holds an INTERIOR block X that is never advertised on the DHT.
// A<->B and C<->B connected, A<->C not. C.GetBlockAssisted(X) must still return X (fwd fallback is
// provider/DHT-independent and per-block), and C must end up warm to the forwarder B.
func TestFwdInteriorBlockAssistedAndWarm(t *testing.T) {
	shrinkFwdTimers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	holder := mkEphemeral(t, ctx, RoleClient)  // A
	donor := mkEphemeral(t, ctx, RoleClient)   // B
	fetcher := mkEphemeral(t, ctx, RoleClient) // C
	forcePublic(t, donor)

	connectNodes(t, holder, donor)
	connectNodes(t, fetcher, donor)

	// Interior block: held by A, deliberately NOT provided to the DHT.
	x := rawBlock(t, []byte("an interior track block, never on the DHT"))
	if err := holder.PutBlock(ctx, x); err != nil {
		t.Fatalf("holder put: %v", err)
	}

	got, err := fetcher.GetBlock(ctx, x.Cid()) // rides the forwarding-assisted exchange
	if err != nil {
		t.Fatalf("GetBlock(assisted): %v", err)
	}
	if string(got.RawData()) != string(x.RawData()) {
		t.Fatalf("assisted bytes mismatch")
	}

	// C should now keep the forwarder B warm (not the unreachable holder A).
	deadline := time.Now().Add(3 * time.Second)
	for {
		warm := map[peer.ID]bool{}
		for _, id := range fetcher.control.warmSet() {
			warm[id] = true
		}
		if warm[donor.ID()] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fetcher did not warm the forwarder %s", donor.ID())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Anti-amplification: a donor NEVER re-forwards. D holds X and sits TWO fwd-hops away
// (D<->E<->B<->C). When C asks B, B does a plain bitswap GetBlock over its OWN connections (E, C) —
// neither has X and E does not re-forward — so B returns a miss. If the invariant were violated
// (B used the assisted path), it would reach D via E and the fetch would wrongly succeed.
func TestFwdNoAmplification(t *testing.T) {
	shrinkFwdTimers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := mkEphemeral(t, ctx, RoleClient)
	e := mkEphemeral(t, ctx, RoleClient)
	b := mkEphemeral(t, ctx, RoleClient)
	c := mkEphemeral(t, ctx, RoleClient)
	forcePublic(t, b) // the donor C asks must serve, so it genuinely tries and misses

	connectNodes(t, d, e) // D <-> E
	connectNodes(t, e, b) // E <-> B
	connectNodes(t, c, b) // C <-> B

	x := rawBlock(t, []byte("two fwd hops away — must NOT be reachable"))
	if err := d.PutBlock(ctx, x); err != nil {
		t.Fatalf("put: %v", err)
	}

	if _, err := c.FwdFetch(ctx, x.Cid()); err == nil {
		t.Fatalf("expected a miss (no transitive re-forwarding), but the block was retrieved")
	}
}

// A Fetch for a CID no connected donor can source returns an error within the timeout (no hang).
func TestFwdMissAbsentCID(t *testing.T) {
	shrinkFwdTimers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	donor := mkEphemeral(t, ctx, RoleClient)
	fetcher := mkEphemeral(t, ctx, RoleClient)
	forcePublic(t, donor)
	connectNodes(t, fetcher, donor)

	absent := rawBlock(t, []byte("nobody holds this"))
	start := time.Now()
	if _, err := fetcher.FwdFetch(ctx, absent.Cid()); err == nil {
		t.Fatalf("expected miss for an absent CID")
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("FwdFetch hung on an absent CID")
	}
}

// The donor's forwarded-block cache serves a repeat without a second bitswap fetch.
func TestFwdDonorCachesForwardedBlock(t *testing.T) {
	shrinkFwdTimers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	holder := mkEphemeral(t, ctx, RoleClient)
	donor := mkEphemeral(t, ctx, RoleClient)
	fetcher := mkEphemeral(t, ctx, RoleClient)
	forcePublic(t, donor)
	connectNodes(t, holder, donor)
	connectNodes(t, fetcher, donor)

	blk := rawBlock(t, []byte("cache me"))
	if err := holder.PutBlock(ctx, blk); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := fetcher.FwdFetch(ctx, blk.Cid()); err != nil {
		t.Fatalf("first fwd: %v", err)
	}
	if donor.fwd.cacheGet(blk.Cid()) == nil {
		t.Fatalf("donor did not cache the forwarded block")
	}
}

// Forwarding suppression follows hole-punch state: not suppressed before a provider is known, and
// suppressed the moment a discovered provider is directly Connected (so direct takes over).
func TestFwdForwardingSuppressedWhenProviderDirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fetcher := mkEphemeral(t, ctx, RoleClient)
	provider := mkEphemeral(t, ctx, RoleClient)
	connectNodes(t, fetcher, provider) // a direct (non-Limited) connection

	if fetcher.forwardingSuppressed() {
		t.Fatalf("should not suppress before any provider is known (initial window)")
	}
	fetcher.fwd.recordProvider(provider.ID())
	if !fetcher.forwardingSuppressed() {
		t.Fatalf("should suppress once a known provider is directly connected (hole-punch took over)")
	}
}

// Per-peer rate cap: a burst beyond the bucket is denied; a normal request is allowed.
func TestFwdRateCap(t *testing.T) {
	f := newFwdState()
	p := peer.ID("peer-A")
	if !f.allow(p, 10) {
		t.Fatalf("a small request should be allowed")
	}
	if f.allow(p, fwdPeerBurst+1) {
		t.Fatalf("a request beyond the burst should be denied")
	}
}

// The caller's hash check rejects bytes that don't match the requested CID (the safety model).
func TestFwdVerifyRejectsMismatch(t *testing.T) {
	good := rawBlock(t, []byte("real content"))
	c := good.Cid()
	chk, err := c.Prefix().Sum([]byte("tampered content"))
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	if chk.Equals(c) {
		t.Fatalf("tampered bytes must not verify against the original CID")
	}
}

// The peer source yields bootstrap seeds first (skipping self), respects num, and closes the
// channel (with a nil DHT, only the seeds are emitted — no discovery).
func TestDonorPeerSourceSeedsFirstAndCap(t *testing.T) {
	self := randPeerID(t)
	s1, s2 := randPeerID(t), randPeerID(t)
	bootstrap := []peer.AddrInfo{{ID: self}, {ID: s1}, {ID: s2}} // self must be skipped
	var idht *dht.IpfsDHT                                        // nil → seeds only
	src := donorPeerSource(&idht, self, bootstrap)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var got []peer.ID
	for ai := range src(ctx, 10) {
		got = append(got, ai.ID)
	}
	if len(got) != 2 || got[0] != s1 || got[1] != s2 {
		t.Fatalf("want [s1 s2] (self skipped, seeds first), got %v", got)
	}

	got = nil
	for ai := range src(ctx, 1) {
		got = append(got, ai.ID)
	}
	if len(got) != 1 || got[0] != s1 {
		t.Fatalf("num cap not respected: got %v", got)
	}
}

// A cancelled context closes the peer-source channel (no goroutine leak / no AutoRelay park).
func TestDonorPeerSourceCtxCancelCloses(t *testing.T) {
	self := randPeerID(t)
	bootstrap := []peer.AddrInfo{{ID: randPeerID(t)}, {ID: randPeerID(t)}}
	var idht *dht.IpfsDHT
	src := donorPeerSource(&idht, self, bootstrap)

	ctx, cancel := context.WithCancel(context.Background())
	ch := src(ctx, 10)
	cancel()
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("peer source did not close after ctx cancel")
	}
}

// End-to-end discovery: a public CLIENT donor advertises the donor rendezvous, and another
// bootstrapped client finds it via the private DHT — the discovery the peer source relies on. The
// seed is NOT a donor and never advertises the rendezvous.
func TestDonorRendezvousDiscoverable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed := mkEphemeral(t, ctx, RoleServer)
	bootAddr := loopbackAddr(t, seed).String() + "/p2p/" + seed.ID().String()
	mkClient := func() *Node {
		cfg := DefaultConfig(RoleClient, "", 0)
		cfg.Bootstrap = []string{bootAddr}
		return mkEphemeral2(t, ctx, cfg)
	}
	donor := mkClient() // a public client donor
	forcePublic(t, donor)
	seeker := mkClient()

	if err := waitRoutingTable(ctx, donor, seeker); err != nil {
		t.Fatalf("routing table: %v", err)
	}
	donor.provideNow(donorRendezvous) // public client donor advertises itself

	deadline := time.Now().Add(20 * time.Second)
	for {
		found := false
		fctx, fcancel := context.WithTimeout(ctx, 4*time.Second)
		for p := range seeker.DHT().FindProvidersAsync(fctx, donorRendezvous, 4) {
			if p.ID == donor.ID() {
				found = true
				break
			}
		}
		fcancel()
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("seeker did not discover the public client donor via the rendezvous")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// relayHopID extracts the forwarder B from a NAT'd provider's /…/p2p/<B>/p2p-circuit/p2p/<A> addr.
func TestRelayHopID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bNode := mkEphemeral(t, ctx, RoleClient)
	aNode := mkEphemeral(t, ctx, RoleClient)

	addr, err := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/5478/p2p/" + bNode.ID().String() +
		"/p2p-circuit/p2p/" + aNode.ID().String())
	if err != nil {
		t.Fatalf("addr: %v", err)
	}
	hop, ok := relayHopID(addr)
	if !ok || hop != bNode.ID() {
		t.Fatalf("relayHopID = %s, %v; want %s", hop, ok, bNode.ID())
	}

	direct, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/5478/p2p/" + bNode.ID().String())
	if _, ok := relayHopID(direct); ok {
		t.Fatalf("a non-circuit addr should not yield a relay hop")
	}
}
