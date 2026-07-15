package tsnode

import (
	"testing"

	ma "github.com/multiformats/go-multiaddr"
)

// TestWebRTCCircuitAddrsFactory checks that the addrs factory makes a NATed client discoverable over
// private-to-private WebRTC: every "/p2p-circuit" reservation gains a "…/p2p-circuit/webrtc" variant,
// the bare undialable "/webrtc" is dropped, and every other address passes through untouched.
func TestWebRTCCircuitAddrsFactory(t *testing.T) {
	const (
		circuit   = "/dns4/trackerstream.xyz/udp/5478/quic-v1/p2p/12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL/p2p-circuit"
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

	out := webrtcCircuitAddrsFactory(in)
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
	out2 := webrtcCircuitAddrsFactory(out)
	if len(out2) != len(out) {
		t.Errorf("factory is not idempotent: %d -> %d addrs", len(out), len(out2))
	}
}
