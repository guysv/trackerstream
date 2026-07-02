package tsnode

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/time/rate"
)

// shrinkAnnounceWindow makes suppression observable inside a test.
func shrinkAnnounceWindow(t *testing.T, d time.Duration) {
	t.Helper()
	old := playlistAnnounceWindow
	playlistAnnounceWindow = d
	t.Cleanup(func() { playlistAnnounceWindow = old })
}

// Playlist publish propagates {record, doc} through the seed's mesh to another client,
// and the seed itself holds the record but NEVER the doc (forward, don't save).
func TestPlaylistPublishPropagatesAndSeedStoresRecordsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	seed := mkEphemeral(t, ctx, RoleServer)
	a := mkEphemeral(t, ctx, RoleClient)
	b := mkEphemeral(t, ctx, RoleClient)
	connectNodes(t, a, seed)
	connectNodes(t, b, seed)

	doc := []byte(`{"v":1,"t":"test list","ts":[[1,"a.it","A"],[2,"b.mod","B"]]}`)
	pid, _, err := a.PublishPlaylist(ctx, "playlist-test", doc, time.Hour, 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	name := pid.String()

	// Re-publish until the gossipsub mesh forms (same trick as the phase-D catalog test).
	deadline := time.After(30 * time.Second)
	for {
		if rec, ok := b.playlists.getRecord(name); ok && rec != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("playlist record never reached client B")
		case <-time.After(300 * time.Millisecond):
			_, _, _ = a.PublishPlaylist(ctx, "playlist-test", doc, time.Hour, 1)
		}
	}

	// Client B holds the doc verbatim.
	_, recs := b.playlists.since(0)
	var got *PlaylistRecord
	for i := range recs {
		if recs[i].Name == name {
			got = &recs[i]
		}
	}
	if got == nil || string(got.Doc) != string(doc) {
		t.Fatalf("client B does not hold the playlist doc: %+v", got)
	}

	// The seed holds the record but no doc bytes.
	deadline = time.After(10 * time.Second)
	for {
		if _, ok := seed.playlists.getRecord(name); ok {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("playlist record never reached the seed")
		case <-time.After(200 * time.Millisecond):
		}
	}
	_, srecs := seed.playlists.since(0)
	for _, e := range srecs {
		if e.Name == name && len(e.Doc) != 0 {
			t.Fatalf("seed stored playlist doc bytes (%d) — must store records only", len(e.Doc))
		}
	}

	// Seq recovery seam: the publisher resolves its own playlist name locally.
	if rec, err := a.ResolveIPNS(ctx, name); err != nil || rec == nil {
		t.Fatalf("ResolveIPNS on own playlist name: %v", err)
	}
}

// A tampered envelope (valid record, doc bytes that don't hash to the record's CID) is
// rejected by the topic validator — our own publish refuses it before it enters the mesh.
func TestPlaylistValidatorRejectsTamperedDoc(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	a := mkEphemeral(t, ctx, RoleClient)

	doc := []byte(`{"v":1,"t":"real","ts":[]}`)
	key, err := a.keystore.GetOrCreate("playlist-tamper")
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	h, _ := mh.Sum(doc, mh.SHA2_256, -1)
	rec, err := signIPNS(key, cid.NewCidV1(cid.Raw, h), time.Hour, 1)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	marshaled, _ := ipns.MarshalRecord(rec)
	pid, err := peer.IDFromPublicKey(key.GetPublic())
	if err != nil {
		t.Fatalf("pid: %v", err)
	}
	name := pid.String()

	tampered := encodePlaylistMsg(name, marshaled, []byte(`{"v":1,"t":"evil","ts":[]}`))
	if err := a.pubsub.PublishPlaylist(ctx, tampered); err == nil {
		t.Fatalf("tampered playlist message was accepted by the local validator")
	}
	if _, ok := a.playlists.getRecord(name); ok {
		t.Fatalf("tampered message reached the store")
	}
}

// Per-peer rate buckets: burst passes, sustained flood is throttled, byte bucket bounds
// fat-doc floods independently, and distinct peers get distinct buckets.
func TestPlaylistRateLimiter(t *testing.T) {
	oldB, oldR := plPeerMsgBurst, plPeerMsgRate
	plPeerMsgBurst, plPeerMsgRate = 3, rate.Limit(0.0001) // no refill within the test
	t.Cleanup(func() { plPeerMsgBurst, plPeerMsgRate = oldB, oldR })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	n := mkEphemeral(t, ctx, RoleClient)

	spammer := peer.ID("spammer-1")
	for i := 0; i < 3; i++ {
		if !n.plAllow(spammer, 100) {
			t.Fatalf("message %d within burst must pass", i+1)
		}
	}
	if n.plAllow(spammer, 100) {
		t.Fatalf("message beyond burst must be throttled")
	}
	if !n.plAllow(peer.ID("honest-2"), 100) {
		t.Fatalf("another peer must have its own bucket")
	}

	// Byte bucket is independent: 3 × 8 MiB = 24 MiB exceeds the 16 MiB byte burst
	// before the (shrunken) 3-message bucket runs out.
	fat := peer.ID("fatposter-3")
	if !n.plAllow(fat, 8<<20) || !n.plAllow(fat, 8<<20) {
		t.Fatalf("two fat docs within byte burst must pass")
	}
	if n.plAllow(fat, 8<<20) {
		t.Fatalf("byte bucket must throttle a fat-doc flood")
	}
}

// Newest-seq-wins, since= cursor semantics, and announce suppression on the store.
func TestPlaylistStoreSeqSinceAndSuppression(t *testing.T) {
	shrinkAnnounceWindow(t, 50*time.Millisecond)
	s := newPlaylistStore(true)

	if !s.ingest("n1", []byte("rec2"), []byte("doc2"), 2) {
		t.Fatalf("first ingest not stored")
	}
	if s.ingest("n1", []byte("rec1"), []byte("doc1"), 1) {
		t.Fatalf("stale seq overwrote a newer record")
	}
	if rec, _ := s.getRecord("n1"); string(rec) != "rec2" {
		t.Fatalf("store lost the newest record")
	}

	ver, recs := s.since(0)
	if len(recs) != 1 || recs[0].Seq != 2 {
		t.Fatalf("since(0): %+v", recs)
	}
	if _, recs = s.since(ver); len(recs) != 0 {
		t.Fatalf("since(current) must be empty")
	}

	// Just ingested → seen just now → suppressed; after the window → announceable.
	if s.shouldAnnounce("n1") {
		t.Fatalf("fresh name must be suppressed")
	}
	time.Sleep(60 * time.Millisecond)
	if !s.shouldAnnounce("n1") {
		t.Fatalf("name unseen for a full window must be announceable")
	}
	if !s.shouldAnnounce("unknown") {
		t.Fatalf("unknown names always announce")
	}
}

// Entry-cap and doc-byte-budget eviction drop the least-recently-seen entries.
func TestPlaylistStoreEviction(t *testing.T) {
	oldEnts, oldBytes := playlistStoreEnts, playlistStoreBytes
	playlistStoreEnts, playlistStoreBytes = 2, 10
	t.Cleanup(func() { playlistStoreEnts, playlistStoreBytes = oldEnts, oldBytes })

	s := newPlaylistStore(true)
	s.ingest("a", []byte("r"), []byte("12345"), 1) // 5 doc bytes
	time.Sleep(2 * time.Millisecond)
	s.ingest("b", []byte("r"), []byte("12345"), 1) // 10 total — at budget
	time.Sleep(2 * time.Millisecond)
	s.ingest("c", []byte("r"), []byte("12345"), 1) // over both caps → evict oldest (a)

	if _, ok := s.getRecord("a"); ok {
		t.Fatalf("oldest entry survived eviction")
	}
	if _, ok := s.getRecord("c"); !ok {
		t.Fatalf("newest entry was evicted")
	}
	if s.docBytes > playlistStoreBytes {
		t.Fatalf("doc budget exceeded after eviction: %d", s.docBytes)
	}
}

// AnnouncePlaylists re-gossips verbatim records with suppression: a fresh pair of nodes
// learns a playlist via announce, and an immediate second announce is suppressed.
func TestPlaylistAnnounceDeliversAndSuppresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// Author signs a playlist on an isolated node; we carry its {record, doc} verbatim.
	author := mkEphemeral(t, ctx, RoleClient)
	doc := []byte(`{"v":1,"t":"carried","ts":[[3,"c.xm","C"]]}`)
	pid, _, err := author.PublishPlaylist(ctx, "playlist-carry", doc, time.Hour, 7)
	if err != nil {
		t.Fatalf("author publish: %v", err)
	}
	_, recs := author.playlists.since(0)
	if len(recs) != 1 {
		t.Fatalf("author store: %+v", recs)
	}
	entry := recs[0]

	// A disjoint holder/receiver pair: the holder re-announces the carried entry.
	holder := mkEphemeral(t, ctx, RoleClient)
	rcv := mkEphemeral(t, ctx, RoleClient)
	connectNodes(t, holder, rcv)

	deadline := time.After(30 * time.Second)
	for {
		if _, ok := rcv.playlists.getRecord(pid.String()); ok {
			break
		}
		// Fresh announce attempts need suppression off for the retry loop: age the entry.
		shrinkAnnounceWindow(t, 0)
		if _, _, rej := holder.AnnouncePlaylists(ctx, []PlaylistRecord{entry}); rej != 0 {
			t.Fatalf("valid carried entry rejected")
		}
		select {
		case <-deadline:
			t.Fatalf("announced playlist never reached the receiver")
		case <-time.After(300 * time.Millisecond):
		}
	}

	// With a real window, an immediate re-announce is suppressed.
	shrinkAnnounceWindow(t, time.Hour)
	if a, sup, _ := holder.AnnouncePlaylists(ctx, []PlaylistRecord{entry}); a != 0 || sup != 1 {
		t.Fatalf("expected suppression, got announced=%d suppressed=%d", a, sup)
	}
}
