package tsnode

import (
	"context"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func shrinkBeaconTimers(t *testing.T) {
	t.Helper()
	oi, oj, of, ok := beaconInterval, beaconJitter, beaconFirstDelay, beaconKickMin
	og := beaconOriginMinGap
	beaconInterval, beaconJitter, beaconFirstDelay = 300*time.Millisecond, 100*time.Millisecond, 50*time.Millisecond
	beaconKickMin, beaconOriginMinGap = 0, 0
	t.Cleanup(func() {
		beaconInterval, beaconJitter, beaconFirstDelay = oi, oj, of
		beaconKickMin, beaconOriginMinGap = ok, og
	})
}

// Codec: roundtrip; malformed / oversized / trailing-bytes frames are all errors.
func TestBeaconCodec(t *testing.T) {
	hashes := [][8]byte{nameHash8("a"), nameHash8("b"), nameHash8("c")}
	got, err := decodeBeacon(encodeBeacon(hashes))
	if err != nil || len(got) != 3 || got[1] != nameHash8("b") {
		t.Fatalf("roundtrip: %v %v", got, err)
	}

	if _, err := decodeBeacon(nil); err == nil {
		t.Fatalf("empty frame must fail")
	}
	if _, err := decodeBeacon([]byte{0x02, 1, 0, 0, 0, 0, 0, 0, 0, 0}); err == nil {
		t.Fatalf("unknown version must fail")
	}
	trailing := append(encodeBeacon(hashes), 0)
	if _, err := decodeBeacon(trailing); err == nil {
		t.Fatalf("trailing bytes must fail")
	}
	over := make([][8]byte, beaconMaxHashes+1)
	if _, err := decodeBeacon(encodeBeacon(over)); err == nil {
		t.Fatalf("over-count must fail")
	}
	if _, err := decodeBeacon(encodeBeacon(hashes)[:5]); err == nil {
		t.Fatalf("truncated body must fail")
	}
}

// Counting store: distinct origins count, repeats don't inflate, the per-hash origin
// cap saturates, and entries outside the window decay.
func TestBeaconCounting(t *testing.T) {
	s := newBeaconState()
	h := nameHash8("popular")
	s.record("origin-1", [][8]byte{h})
	s.record("origin-1", [][8]byte{h}) // repeat: same origin, still 1
	s.record("origin-2", [][8]byte{h})
	if c := s.backers([]string{"popular"})["popular"]; c != 2 {
		t.Fatalf("want 2 distinct origins, got %d", c)
	}
	if c := s.backers([]string{"unknown"})["unknown"]; c != 0 {
		t.Fatalf("unknown name must count 0, got %d", c)
	}

	// Saturation: the origin set caps (count never exceeds beaconOriginsPerHash).
	set, _ := s.counts.Get(h)
	for i := 0; i < beaconOriginsPerHash+50; i++ {
		set.touch(peer.ID(string(rune(i))+"-sybil"), time.Now())
	}
	if c := s.backers([]string{"popular"})["popular"]; c > beaconOriginsPerHash {
		t.Fatalf("origin set must saturate at %d, got %d", beaconOriginsPerHash, c)
	}

	// Window decay.
	oldWindow := beaconWindow
	beaconWindow = time.Millisecond
	t.Cleanup(func() { beaconWindow = oldWindow })
	time.Sleep(5 * time.Millisecond)
	if c := s.backers([]string{"popular"})["popular"]; c != 0 {
		t.Fatalf("counts must decay past the window, got %d", c)
	}
}

// Two clients holding the same playlist: after their beacons cross the mesh, BOTH
// count 2 backers for the shared name and 1 for a solo one. Malformed beacons are
// rejected by our own validator before entering the mesh.
func TestBeaconPropagatesAndCounts(t *testing.T) {
	shrinkBeaconTimers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	a := mkEphemeral(t, ctx, RoleClient)
	b := mkEphemeral(t, ctx, RoleClient)
	connectNodes(t, a, b)

	shared, solo := "12D3KooWSharedName", "12D3KooWSoloName"
	a.plList.setManifest([]PlaylistListEntry{{Name: shared, Seq: 1}, {Name: solo, Seq: 1}})
	b.plList.setManifest([]PlaylistListEntry{{Name: shared, Seq: 1}})

	// Publish-until-mesh (beaconOriginMinGap is 0 under shrunk timers).
	deadline := time.After(30 * time.Second)
	for {
		aCounts := a.beacons.backers([]string{shared, solo})
		bCounts := b.beacons.backers([]string{shared, solo})
		if aCounts[shared] == 2 && bCounts[shared] == 2 && aCounts[solo] == 1 && bCounts[solo] == 1 {
			break
		}
		a.publishBeacon(ctx)
		b.publishBeacon(ctx)
		select {
		case <-deadline:
			t.Fatalf("beacon counts never converged: a=%v b=%v", aCounts, bCounts)
		case <-time.After(300 * time.Millisecond):
		}
	}

	// A malformed beacon never enters the mesh: our own validator refuses it.
	if err := a.pubsub.PublishBeacon(ctx, []byte{0x7f, 0xff}); err == nil {
		t.Fatalf("malformed beacon must be rejected by the local validator")
	}
}

// Validator policy, driven directly: valid Accepts; an immediate repeat from the same
// ORIGIN is Ignored (min gap — repeats can't inflate counts, and they must not ride
// the mesh either); malformed Rejects.
func TestBeaconValidatorOriginMinGap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	n := mkEphemeral(t, ctx, RoleClient)

	origin := randPeerID(t)
	hop := randPeerID(t)
	mkMsg := func(data []byte) *pubsub.Message {
		from := []byte(origin)
		return &pubsub.Message{
			Message:      &pubsub_pb.Message{Data: data, From: from},
			ReceivedFrom: hop,
		}
	}
	valid := encodeBeacon([][8]byte{nameHash8("x")})
	if r := n.beaconValidator(ctx, hop, mkMsg(valid)); r != pubsub.ValidationAccept {
		t.Fatalf("first beacon must Accept, got %v", r)
	}
	if r := n.beaconValidator(ctx, hop, mkMsg(valid)); r != pubsub.ValidationIgnore {
		t.Fatalf("repeat within the origin min gap must Ignore, got %v", r)
	}
	if r := n.beaconValidator(ctx, hop, mkMsg([]byte{0x7f})); r != pubsub.ValidationReject {
		t.Fatalf("malformed beacon must Reject, got %v", r)
	}
}
