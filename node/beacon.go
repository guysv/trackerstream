package tsnode

// Hold beacons (PLAYLISTS.md §10, shipped): announce suppression deliberately hides
// holders (~one voice per playlist per cycle), so popularity is invisible. This side
// channel restores the signal cheaply: each client broadcasts a small jittered hourly
// manifest of truncated 8-byte name-hashes of its DISCLOSURE SET (held + published-mine
// — the same set the playlist-list protocol serves; never private, never seen-tier).
// Every node counts hash → distinct origins over a 24h sliding window, locally and
// independently — no consensus, none needed. Gossipsub's default StrictSign
// authenticates the origin for free; Sybil-gameability is accepted until the identity
// layer lands (only the WEIGHTING of this feed changes then, never the feed itself).

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

const (
	beaconVersion byte = 0x01
	// beaconMaxHashes bounds one beacon message: honest libraries are hundreds of
	// playlists; 4096 × 8 B ≈ 32 KiB is already implausibly large. Bigger ⇒ Reject.
	beaconMaxHashes = 4096
	beaconHashCap   = 16384 // outer LRU: distinct playlist hashes we count for
	// beaconOriginsPerHash caps the per-playlist origin set — the count SATURATES
	// there ("~512+ holders" needs no more precision, and a Sybil can't bloat it).
	beaconOriginsPerHash = 512
	beaconPeerLims       = 4096 // bounded per-peer limiter table (fwd.go doctrine)
)

// Tunables kept as vars so tests can shrink them.
var (
	beaconInterval   = 60 * time.Minute // base publish cadence
	beaconJitter     = 15 * time.Minute // ± uniform jitter on the cadence
	beaconFirstDelay = 60 * time.Second // first beacon 1–2 min after a manifest exists
	beaconKickMin    = 10 * time.Minute // manifest-change kick floor (else wait for cycle)
	beaconWindow     = 24 * time.Hour   // sliding count window
	// beaconOriginMinGap Ignores repeat beacons from one origin: an honest node beacons
	// hourly; anything under this is either a kick burst or a count-inflation attempt.
	beaconOriginMinGap = 2 * time.Minute
	// First-hop rate caps (charged to the peer we received from, own publishes exempt).
	beaconPeerMsgRate   = rate.Limit(0.1) // sustained msgs/s per peer
	beaconPeerMsgBurst  = 8
	beaconPeerByteRate  = rate.Limit(64 << 10) // sustained bytes/s per peer
	beaconPeerByteBurst = 256 << 10
)

// nameHash8 is the beacon identifier for a playlist name: sha256(name)[:8]. Truncation
// keeps beacons small; collisions just merge two counts, harmless for a ranking signal.
func nameHash8(name string) [8]byte {
	var h [8]byte
	s := sha256.Sum256([]byte(name))
	copy(h[:], s[:8])
	return h
}

// encodeBeacon frames [version][uvarint N][N × 8-byte hash].
func encodeBeacon(hashes [][8]byte) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(hashes)*8)
	buf = append(buf, beaconVersion)
	buf = binary.AppendUvarint(buf, uint64(len(hashes)))
	for _, h := range hashes {
		buf = append(buf, h[:]...)
	}
	return buf
}

// decodeBeacon rejects anything but an exact-length, cap-respecting frame.
func decodeBeacon(b []byte) ([][8]byte, error) {
	if len(b) == 0 || b[0] != beaconVersion {
		return nil, fmt.Errorf("bad beacon version")
	}
	count, n := binary.Uvarint(b[1:])
	if n <= 0 || count == 0 || count > beaconMaxHashes {
		return nil, fmt.Errorf("bad beacon count")
	}
	body := b[1+n:]
	if uint64(len(body)) != count*8 {
		return nil, fmt.Errorf("beacon length mismatch")
	}
	hashes := make([][8]byte, count)
	for i := range hashes {
		copy(hashes[i][:], body[i*8:])
	}
	return hashes, nil
}

// originSet is one playlist-hash's holder ledger: origin → last beacon time, capped
// (count saturates) and pruned to the window on every touch/read.
type originSet struct {
	mu sync.Mutex
	m  map[peer.ID]time.Time
}

func (o *originSet) touch(p peer.ID, now time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for id, at := range o.m {
		if now.Sub(at) > beaconWindow {
			delete(o.m, id)
		}
	}
	if _, ok := o.m[p]; !ok && len(o.m) >= beaconOriginsPerHash {
		return // saturated
	}
	o.m[p] = now
}

func (o *originSet) count(now time.Time) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	c := 0
	for _, at := range o.m {
		if now.Sub(at) <= beaconWindow {
			c++
		}
	}
	return c
}

// beaconState holds the counting store + the serving-side rate tables + publish clock.
type beaconState struct {
	counts   *lru.Cache[[8]byte, *originSet]
	origLast *lru.Cache[peer.ID, time.Time] // per-ORIGIN min-gap ledger (count integrity)
	lims     *lru.Cache[peer.ID, *plLimiter] // per-FIRST-HOP rate buckets (flood cap)

	mu      sync.Mutex
	lastPub time.Time
}

func newBeaconState() *beaconState {
	return &beaconState{
		counts:   mustLRU[[8]byte, *originSet](beaconHashCap),
		origLast: mustLRU[peer.ID, time.Time](beaconHashCap),
		lims:     mustLRU[peer.ID, *plLimiter](beaconPeerLims),
	}
}

func (s *beaconState) allow(from peer.ID, size int) bool {
	l, ok := s.lims.Get(from)
	if !ok {
		l = &plLimiter{
			msgs:  rate.NewLimiter(beaconPeerMsgRate, beaconPeerMsgBurst),
			bytes: rate.NewLimiter(beaconPeerByteRate, beaconPeerByteBurst),
		}
		s.lims.Add(from, l)
	}
	now := time.Now()
	return l.msgs.AllowN(now, 1) && l.bytes.AllowN(now, size)
}

// record counts every hash for one authenticated origin.
func (s *beaconState) record(origin peer.ID, hashes [][8]byte) {
	now := time.Now()
	for _, h := range hashes {
		set, ok := s.counts.Get(h)
		if !ok {
			set = &originSet{m: map[peer.ID]time.Time{}}
			s.counts.Add(h, set)
		}
		set.touch(origin, now)
	}
}

// backers returns the current windowed holder count for each name (0 = unknown).
func (s *beaconState) backers(names []string) map[string]int {
	now := time.Now()
	out := make(map[string]int, len(names))
	for _, name := range names {
		c := 0
		if set, ok := s.counts.Get(nameHash8(name)); ok {
			c = set.count(now)
		}
		out[name] = c
	}
	return out
}

// beaconValidator gates the topic: first-hop rate (Ignore), format (Reject), and a
// per-origin minimum gap (Ignore — repeats can't inflate counts anyway, this just
// keeps them off the mesh). Own publishes exempt from rate policy.
func (n *Node) beaconValidator(_ context.Context, from peer.ID, m *pubsub.Message) pubsub.ValidationResult {
	if from != n.host.ID() && !n.beacons.allow(from, len(m.Data)) {
		return pubsub.ValidationIgnore
	}
	if _, err := decodeBeacon(m.Data); err != nil {
		return pubsub.ValidationReject
	}
	origin := m.GetFrom() // authenticated under gossipsub's default StrictSign
	if origin != n.host.ID() {
		if at, ok := n.beacons.origLast.Get(origin); ok && time.Since(at) < beaconOriginMinGap {
			return pubsub.ValidationIgnore
		}
		n.beacons.origLast.Add(origin, time.Now())
	}
	return pubsub.ValidationAccept
}

// beaconSink counts a validator-passed beacon for its origin.
func (n *Node) beaconSink(origin peer.ID, data []byte) {
	hashes, err := decodeBeacon(data)
	if err != nil {
		return // validator raced a tunable swap; just drop
	}
	n.beacons.record(origin, hashes)
}

// publishBeacon broadcasts the current disclosure-set manifest as hashes. No-op when
// the manifest is empty (nothing held ⇒ nothing to say).
func (n *Node) publishBeacon(ctx context.Context) {
	entries := n.plList.snapshot(beaconMaxHashes)
	if len(entries) == 0 {
		return
	}
	hashes := make([][8]byte, len(entries))
	for i, e := range entries {
		hashes[i] = nameHash8(e.Name)
	}
	if err := n.pubsub.PublishBeacon(ctx, encodeBeacon(hashes)); err != nil {
		n.logf("beacon publish failed: %v", err)
		return
	}
	n.beacons.mu.Lock()
	n.beacons.lastPub = time.Now()
	n.beacons.mu.Unlock()
}

// KickBeacon publishes ahead of the cycle after a manifest change, floor-limited so a
// hold/unhold flurry can't turn into a beacon flood. (The manifest RPC calls this; the
// desktop only posts manifests that actually changed.)
func (n *Node) KickBeacon(ctx context.Context) {
	n.beacons.mu.Lock()
	recent := time.Since(n.beacons.lastPub) < beaconKickMin
	n.beacons.mu.Unlock()
	if recent {
		return
	}
	n.publishBeacon(ctx)
}

// beaconLoop is the client's jittered hourly publish cadence. The first beacon goes
// out 1–2 minutes after the manifest first becomes non-empty (the mesh needs a moment,
// and so does the desktop's first manifest push).
func (n *Node) beaconLoop(ctx context.Context) {
	next := beaconFirstDelay + time.Duration(rand.Int63n(int64(beaconFirstDelay)))
	t := time.NewTimer(next)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.beacons.mu.Lock()
		due := time.Since(n.beacons.lastPub) >= beaconKickMin // a kick may have just fired
		n.beacons.mu.Unlock()
		if due {
			n.publishBeacon(ctx)
		}
		jitter := time.Duration(rand.Int63n(int64(2*beaconJitter))) - beaconJitter
		t.Reset(beaconInterval + jitter)
	}
}
