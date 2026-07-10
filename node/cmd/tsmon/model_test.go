package main

import (
	"os"
	"testing"
	"time"
)

// recompute joins swarm/peers ∪ bandwidth ∪ ledger into rows, tags seeds, and derives the
// per-peer serve rate by diffing cumulative ledger bytes across two polls.
func TestRecomputeJoinAndServeRate(t *testing.T) {
	t0 := time.Now()
	seedID, clientID := "12D3KooWSeed", "12D3KooWClient"

	snap1 := &Snapshot{
		At: t0,
		Status: &SeedStatus{
			Role: "server", Peers: 2, Seeds: []string{seedID},
		},
		Peers: []swarmPeer{
			{Peer: seedID, Addr: "/ip4/1.2.3.4/tcp/4001"},
			{Peer: clientID, Addr: "/ip4/5.6.7.8/udp/4001/quic-v1/p2p-circuit"},
		},
		Bw: map[string]bwEntry{
			clientID: {TotalIn: 100, TotalOut: 200, RateIn: 10, RateOut: 20},
		},
		Ledger: map[string]ledgerEntry{
			clientID: {Sent: 1000, Recv: 5, WantlistLen: 3},
		},
	}

	m := newModel(nil, time.Second, "test")
	m.snap = snap1
	m.recompute()

	if len(m.rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(m.rows))
	}
	byID := map[string]peerRow{}
	for _, r := range m.rows {
		byID[r.id] = r
	}
	if got := byID[seedID]; got.role != "seed" {
		t.Fatalf("seed peer role=%q want seed", got.role)
	}
	c := byID[clientID]
	if !c.relayed {
		t.Fatalf("client conn is /p2p-circuit — should be relayed")
	}
	if c.served != 1000 || c.wantlist != 3 || c.downRate != 10 {
		t.Fatalf("client join wrong: served=%d want=%d downRate=%v", c.served, c.wantlist, c.downRate)
	}
	if c.servedR != 0 {
		t.Fatalf("first poll serve rate should be 0, got %v", c.servedR)
	}

	// second poll 2s later: client got 4000 more bytes served → 2000 B/s
	snap2 := &Snapshot{
		At:     t0.Add(2 * time.Second),
		Status: snap1.Status,
		Peers:  snap1.Peers,
		Bw:     snap1.Bw,
		Ledger: map[string]ledgerEntry{
			clientID: {Sent: 5000, Recv: 5, WantlistLen: 1},
		},
	}
	m.prevSnap = snap1
	m.snap = snap2
	m.recompute()
	for _, r := range m.rows {
		if r.id == clientID {
			if r.servedR != 2000 {
				t.Fatalf("serve rate = %v B/s, want 2000", r.servedR)
			}
			if len(m.sparks[clientID]) != 2 {
				t.Fatalf("sparkline history = %d samples, want 2", len(m.sparks[clientID]))
			}
		}
	}
}

// View() must render every state (peers, detail, backers, legacy, unreachable) without
// panicking on a fully-populated model.
func TestViewRendersAllStates(t *testing.T) {
	m := newModel(nil, time.Second, "test")
	m.width, m.height = 120, 30
	m.snap = &Snapshot{
		At: time.Now(),
		Status: &SeedStatus{
			ID: "12D3KooWMk7yXP8FWenYFKrEvgvfpbsLFoDSZ9Bh5qLyYiFgTDX8", Role: "server",
			AgentVersion: "trackerstream/dev/seed", Reachability: "public", Peers: 3,
			Pins: 42, PinsByKind: map[string]int{"root": 2, "track_root": 30, "catalog_piece": 10},
			Reprovide: struct {
				LastUnix    int64 `json:"LastUnix"`
				IntervalSec int64 `json:"IntervalSec"`
			}{LastUnix: time.Now().Add(-time.Hour).Unix(), IntervalSec: 79200},
		},
		Peers:   []swarmPeer{{Peer: "12D3KooWClient", Addr: "/ip4/5.6.7.8/tcp/4001"}},
		Bw:      map[string]bwEntry{"12D3KooWClient": {TotalIn: 100, RateIn: 10}},
		Ledger:  map[string]ledgerEntry{"12D3KooWClient": {Sent: 9999, WantlistLen: 2}},
		Bitswap: &BitswapStat{BlocksSent: 5, DataSent: 4096, BlocksReceived: 3},
		Backers: map[string]int{"chiptune classics": 7, "demoscene": 3},
	}
	m.recompute()

	mustRender := func(label string, mm model) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("View panicked in %s: %v", label, r)
			}
		}()
		if out := mm.View(); out == "" {
			t.Fatalf("%s: empty render", label)
		}
	}
	mustRender("peers", m)
	dm := m
	dm.detail = true
	mustRender("detail", dm)

	if os.Getenv("TSMON_DUMP") != "" {
		t.Logf("\n----- PEERS -----\n%s\n----- DETAIL -----\n%s", m.View(), dm.View())
	}
	bm := m
	bm.view = viewBackers
	mustRender("backers", bm)
	lm := m
	lm.snap.Status.legacy = true
	mustRender("legacy", lm)
	em := newModel(nil, time.Second, "test")
	em.width, em.height = 120, 30
	em.snap = &Snapshot{At: time.Now()}
	em.err = context_error()
	mustRender("unreachable", em)
}

func context_error() error { return &net_error{} }

type net_error struct{}

func (*net_error) Error() string { return "connection refused" }

func TestFmtHelpers(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{512, "512B"},
		{1024, "1.0KB"},
		{1536, "1.5KB"},
		{1024 * 1024, "1.0MB"},
	}
	for _, c := range cases {
		if got := fmtBytes(c.n); got != c.want {
			t.Errorf("fmtBytes(%d)=%q want %q", c.n, got, c.want)
		}
	}
	if fmtRate(0.4) != "·" {
		t.Errorf("sub-1 B/s rate should be a dot")
	}
	if sparkline(nil) != "—" {
		t.Errorf("empty sparkline should be a dash")
	}
	if got := shortID("12D3KooWMk7yXP8FWenYFKrEvgvfpbsLFoDSZ9Bh5qLyYiFgTDX8"); got != "12D3Ko…gTDX8" {
		t.Errorf("shortID = %q", got)
	}
}
