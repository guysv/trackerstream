package tsnode

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// shrinkWantCooldown makes the forced-reannounce cooldown observable inside a test.
func shrinkWantCooldown(t *testing.T, d time.Duration) {
	t.Helper()
	old := plWantCooldown
	plWantCooldown = d
	t.Cleanup(func() { plWantCooldown = old })
}

// unthrottlePlList lifts the per-peer request cap so retry loops don't self-throttle.
func unthrottlePlList(t *testing.T) {
	t.Helper()
	oldR, oldB := plListPeerRate, plListPeerBurst
	plListPeerRate, plListPeerBurst = rate.Limit(1000), 10000
	t.Cleanup(func() { plListPeerRate, plListPeerBurst = oldR, oldB })
}

// The manifest posted on A is exactly what B gets back over the stream — and an empty
// manifest answers empty, not with an error.
func TestPlaylistListServesManifest(t *testing.T) {
	unthrottlePlList(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a := mkEphemeral(t, ctx, RoleClient)
	b := mkEphemeral(t, ctx, RoleClient)
	connectNodes(t, b, a)

	// Empty manifest first.
	entries, reann, err := b.PeerPlaylists(ctx, a.ID(), nil)
	if err != nil {
		t.Fatalf("empty-manifest list: %v", err)
	}
	if len(entries) != 0 || reann != 0 {
		t.Fatalf("empty manifest must answer empty, got %+v", entries)
	}

	want := []PlaylistListEntry{
		{Name: "12D3KooWFakeName1", Seq: 3, Title: "chiptune bangers"},
		{Name: "12D3KooWFakeName2", Seq: 1, Title: "late night xm"},
	}
	if n := a.plList.setManifest(want); n != 2 {
		t.Fatalf("setManifest kept %d", n)
	}
	entries, _, err = b.PeerPlaylists(ctx, a.ID(), nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 2 || entries[0] != want[0] || entries[1] != want[1] {
		t.Fatalf("disclosure set mismatch: %+v", entries)
	}
}

// The seed never registers the handler — asking it reads as unsupported, not an error.
func TestPlaylistListSeedUnsupported(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seed := mkEphemeral(t, ctx, RoleServer)
	b := mkEphemeral(t, ctx, RoleClient)
	connectNodes(t, b, seed)

	_, _, err := b.PeerPlaylists(ctx, seed.ID(), nil)
	if !errors.Is(err, ErrPlaylistListUnsupported) {
		t.Fatalf("expected ErrPlaylistListUnsupported from the seed, got %v", err)
	}
}

// `want` bypasses announce suppression: a playlist whose lastSeen is fresh (suppressed
// for the normal cycle) is still re-gossiped on request, and the requester's own store
// receives it through the mesh. An immediate repeat is stopped by the per-name cooldown.
func TestPlaylistListWantBypassesSuppression(t *testing.T) {
	unthrottlePlList(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	a := mkEphemeral(t, ctx, RoleClient)
	b := mkEphemeral(t, ctx, RoleClient)
	connectNodes(t, b, a)

	// A publishes → its store's lastSeen is NOW → shouldAnnounce is false for a real
	// window. The normal announce path would suppress; want must not.
	shrinkAnnounceWindow(t, time.Hour)
	doc := []byte(`{"v":1,"t":"wanted","ts":[[9,"w.it","W"]]}`)
	pid, _, err := a.PublishPlaylist(ctx, "playlist-want", doc, time.Hour, 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	name := pid.String()
	if a.playlists.shouldAnnounce(name) {
		t.Fatalf("precondition: name must be suppressed for the normal cycle")
	}
	a.plList.setManifest([]PlaylistListEntry{{Name: name, Seq: 1, Title: "wanted"}})

	// Cooldown off for the mesh-formation retry loop.
	shrinkWantCooldown(t, 0)
	deadline := time.After(30 * time.Second)
	for {
		if _, ok := b.playlists.getRecord(name); ok {
			break
		}
		if _, reann, err := b.PeerPlaylists(ctx, a.ID(), []string{name}); err != nil {
			t.Fatalf("want request: %v", err)
		} else if reann != 1 {
			t.Fatalf("suppression-bypassing want must reannounce, got %d", reann)
		}
		select {
		case <-deadline:
			t.Fatalf("wanted playlist never reached the requester")
		case <-time.After(300 * time.Millisecond):
		}
	}

	// Real cooldown: an immediate repeat serves the list but reannounces nothing.
	shrinkWantCooldown(t, time.Hour)
	a.plList.cool.Add(name, time.Now())
	if _, reann, err := b.PeerPlaylists(ctx, a.ID(), []string{name}); err != nil || reann != 0 {
		t.Fatalf("cooldown must stop a repeat want: reann=%d err=%v", reann, err)
	}

	// A name outside the manifest is never served, even if the buffer holds it.
	a.plList.setManifest(nil)
	shrinkWantCooldown(t, 0)
	if _, reann, _ := b.PeerPlaylists(ctx, a.ID(), []string{name}); reann != 0 {
		t.Fatalf("non-manifest want must not reannounce")
	}
}

// Serving-side bounds: per-peer request bucket, manifest clamps, snapshot cap.
func TestPlaylistListStateBounds(t *testing.T) {
	s := newPlListState()

	// Request bucket: burst passes, then throttled; distinct peers independent.
	for i := 0; i < plListPeerBurst; i++ {
		if !s.allow("peer-a") {
			t.Fatalf("request %d within burst must pass", i+1)
		}
	}
	if s.allow("peer-a") {
		t.Fatalf("request beyond burst must be throttled")
	}
	if !s.allow("peer-b") {
		t.Fatalf("another peer must have its own bucket")
	}

	// Manifest clamps: oversized title truncated, empty/oversized names dropped,
	// entry count capped.
	long := strings.Repeat("x", plListTitleMax+50)
	entries := make([]PlaylistListEntry, 0, plListManifestMax+10)
	entries = append(entries,
		PlaylistListEntry{Name: "ok", Seq: 1, Title: long},
		PlaylistListEntry{Name: "", Seq: 1, Title: "dropped"},
		PlaylistListEntry{Name: strings.Repeat("n", playlistNameMax+1), Seq: 1, Title: "dropped"},
	)
	for i := 0; i < plListManifestMax+7; i++ {
		entries = append(entries, PlaylistListEntry{Name: "bulk", Seq: uint64(i)})
	}
	kept := s.setManifest(entries)
	if kept > plListManifestMax {
		t.Fatalf("manifest cap exceeded: %d", kept)
	}
	snap := s.snapshot(plListMaxEntries)
	if len(snap) > plListMaxEntries {
		t.Fatalf("snapshot cap exceeded: %d", len(snap))
	}
	if len(snap[0].Title) != plListTitleMax {
		t.Fatalf("title not clamped: %d", len(snap[0].Title))
	}
	for _, e := range snap {
		if e.Name == "" || len(e.Name) > playlistNameMax {
			t.Fatalf("bad name survived the manifest clamp: %q", e.Name)
		}
	}

	// Cooldown records on first use.
	shrinkWantCooldown(t, time.Hour)
	if !s.wantAllowed("n") {
		t.Fatalf("first want must be allowed")
	}
	if s.wantAllowed("n") {
		t.Fatalf("second want within cooldown must be denied")
	}
}
