package tsnode

import (
	"sync/atomic"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const testMasterID = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL"

// TestWebRTCCircuitAddrsFactory checks that the addrs factory makes a NATed client discoverable over
// private-to-private WebRTC: every "/p2p-circuit" reservation gains a "…/p2p-circuit/webrtc" variant,
// the bare undialable "/webrtc" is dropped, and every other address passes through untouched.
func TestWebRTCCircuitAddrsFactory(t *testing.T) {
	const (
		circuit   = "/dns4/trackerstream.xyz/udp/5478/quic-v1/p2p/" + testMasterID + "/p2p-circuit"
		bareWRTC  = "/webrtc"
		directQ   = "/ip4/1.2.3.4/udp/5478/quic-v1"
		wrtcDirct = "/ip4/1.2.3.4/udp/5478/webrtc-direct/certhash/uEiCGOyTDAg14lkqZ4SUclH5kOD5BNPM81Ska7n2AHBDabA"
	)
	in := []ma.Multiaddr{
		ma.StringCast(circuit),
		ma.StringCast(bareWRTC),
		ma.StringCast(directQ),
		ma.StringCast(wrtcDirct),
	}

	// No standing reservation (held=false) and no extra: pure rewrite behavior.
	factory := makeWebrtcCircuitAddrsFactory(nil, &atomic.Bool{})
	out := factory(in)
	got := map[string]int{}
	for _, a := range out {
		got[a.String()]++
	}

	// The circuit reservation is kept AND gains a /p2p-circuit/webrtc variant.
	if got[circuit] != 1 {
		t.Errorf("expected the /p2p-circuit reservation addr kept once, got %d", got[circuit])
	}
	circuitWebRTC := circuit + "/webrtc"
	if got[circuitWebRTC] != 1 {
		t.Errorf("expected exactly one %q, got %d (out=%v)", circuitWebRTC, got[circuitWebRTC], out)
	}
	// The bare, undialable /webrtc listener addr is dropped.
	if got[bareWRTC] != 0 {
		t.Errorf("expected bare %q dropped, still present %d time(s)", bareWRTC, got[bareWRTC])
	}
	// Direct addresses pass through untouched, and never grow a circuit/webrtc variant.
	if got[directQ] != 1 {
		t.Errorf("expected direct quic addr kept once, got %d", got[directQ])
	}
	if got[wrtcDirct] != 1 {
		t.Errorf("expected webrtc-direct addr kept once, got %d", got[wrtcDirct])
	}
	if got[directQ+"/webrtc"] != 0 || got[wrtcDirct+"/webrtc"] != 0 {
		t.Errorf("a non-circuit addr must not gain a /webrtc variant: out=%v", out)
	}

	// Idempotence: a "/p2p-circuit/webrtc" addr is not double-encapsulated (it does not end in
	// /p2p-circuit), so re-running the factory adds nothing new.
	out2 := factory(out)
	if len(out2) != len(out) {
		t.Errorf("factory is not idempotent: %d -> %d addrs", len(out), len(out2))
	}
}

// TestWebRTCCircuitAddrsFactoryHeld checks the fix: while the standing master reservation is held,
// the factory appends the master circuit path even when the input has NO /p2p-circuit addr (the
// Public-reachability case where the address manager surfaced none) — and appends nothing when not held.
func TestWebRTCCircuitAddrsFactoryHeld(t *testing.T) {
	mid, err := peer.Decode(testMasterID)
	if err != nil {
		t.Fatalf("decode master id: %v", err)
	}
	master := peer.AddrInfo{ID: mid, Addrs: []ma.Multiaddr{
		ma.StringCast("/ip4/5.75.131.145/udp/5478/quic-v1"),
	}}
	extra := circuitWebRTCAddrs(master)
	if len(extra) != 2 { // <addr>/p2p-circuit and <addr>/p2p-circuit/webrtc
		t.Fatalf("expected 2 circuit addrs, got %d: %v", len(extra), extra)
	}
	wantCircuitWebRTC := "/ip4/5.75.131.145/udp/5478/quic-v1/p2p/" + testMasterID + "/p2p-circuit/webrtc"

	// Public-like input: only a direct webrtc-direct addr, no /p2p-circuit at all.
	in := []ma.Multiaddr{ma.StringCast("/ip4/1.2.3.4/udp/5478/webrtc-direct/certhash/uEiCGOyTDAg14lkqZ4SUclH5kOD5BNPM81Ska7n2AHBDabA")}

	var held atomic.Bool
	factory := makeWebrtcCircuitAddrsFactory(extra, &held)

	// Not held: the relay fallback is NOT advertised.
	present := func(out []ma.Multiaddr, s string) bool {
		for _, a := range out {
			if a.String() == s {
				return true
			}
		}
		return false
	}
	if present(factory(in), wantCircuitWebRTC) {
		t.Errorf("relay fallback advertised while reservation not held")
	}
	// Held: the browser-dialable /p2p-circuit/webrtc IS advertised despite no circuit addr in input.
	held.Store(true)
	if !present(factory(in), wantCircuitWebRTC) {
		t.Errorf("expected %q advertised while reservation held (Public case)", wantCircuitWebRTC)
	}
}

// TestMasterAddrInfo folds the per-transport bootstrap entries for the box into one AddrInfo.
func TestMasterAddrInfo(t *testing.T) {
	mid, err := peer.Decode(testMasterID)
	if err != nil {
		t.Fatalf("decode master id: %v", err)
	}
	boot := []peer.AddrInfo{
		{ID: mid, Addrs: []ma.Multiaddr{ma.StringCast("/ip4/5.75.131.145/tcp/5478")}},
		{ID: mid, Addrs: []ma.Multiaddr{ma.StringCast("/ip4/5.75.131.145/udp/5478/quic-v1")}},
	}
	m := masterAddrInfo(boot)
	if m.ID != mid {
		t.Errorf("master id = %s, want %s", m.ID, mid)
	}
	if len(m.Addrs) != 2 {
		t.Errorf("expected 2 folded addrs, got %d: %v", len(m.Addrs), m.Addrs)
	}
	if got := masterAddrInfo(nil); got.ID != "" {
		t.Errorf("empty bootstrap should yield zero AddrInfo, got %v", got)
	}
}
