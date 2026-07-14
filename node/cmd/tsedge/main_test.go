package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

const testPeerID = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL"

// fakeSeed stands in for tsnode's loopback RPC. `addrs` is swapped between calls to model a
// seed restart (new certhash); `down` models the seed being wedged/restarting.
type fakeSeed struct {
	addrs []string
	down  bool
}

func (f *fakeSeed) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.down {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ID": testPeerID, "Addresses": f.addrs})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A realistic certhash addr as go-libp2p emits it.
const wrtc = "/ip4/5.75.131.145/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ"

func TestBrowserDialable(t *testing.T) {
	tests := []struct {
		name  string
		addrs []string
		want  []string
	}{{
		name:  "keeps public webrtc-direct and appends /p2p",
		addrs: []string{wrtc},
		want:  []string{wrtc + "/p2p/" + testPeerID},
	}, {
		name: "drops every non-webrtc-direct transport",
		addrs: []string{
			"/ip4/5.75.131.145/tcp/5478",
			"/ip4/5.75.131.145/udp/5478/quic-v1",
			"/ip4/5.75.131.145/tcp/5478/ws",
			"/ip6/2a01:4f8:1c1f:9120::1/udp/5478/quic-v1",
			wrtc,
		},
		want: []string{wrtc + "/p2p/" + testPeerID},
	}, {
		name: "drops loopback / private / link-local webrtc-direct",
		addrs: []string{
			"/ip4/127.0.0.1/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
			"/ip4/10.0.0.7/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
			"/ip4/172.17.0.1/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
			"/ip4/192.168.1.20/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
			"/ip4/169.254.3.4/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
			"/ip6/::1/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
			"/ip6/fe80::1/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
		},
		want: []string{},
	}, {
		name: "keeps a public v6 webrtc-direct",
		addrs: []string{
			"/ip6/2a01:4f8:1c1f:9120::1/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
		},
		want: []string{
			"/ip6/2a01:4f8:1c1f:9120::1/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ/p2p/" + testPeerID,
		},
	}, {
		name:  "skips garbage rather than failing the whole doc",
		addrs: []string{"not-a-multiaddr", wrtc},
		want:  []string{wrtc + "/p2p/" + testPeerID},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := browserDialable(tc.addrs, testPeerID)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("browserDialable()\n got %#v\nwant %#v", got, tc.want)
			}
		})
	}
}

// TestTurnCredential pins the coturn use-auth-secret vector: username = "<unix>:web",
// credential = base64(HMAC-SHA1(secret, username)). If this ever changes, every browser
// silently loses its TURN relay — so it is asserted against a literal, not recomputed.
// STUN only, never TURN. TURN would mean running an open relay for the internet and handing every
// visitor a credential for it; we deliberately don't. This test is the guard against someone
// "helpfully" adding it back.
func TestICEServersAreStunOnlyAndNeverTurn(t *testing.T) {
	s := newServer("http://127.0.0.1:1", "trackerstream.xyz")
	ice := s.iceServers()
	if len(ice) != 1 {
		t.Fatalf("iceServers = %d entries)", len(ice))
	}
	if got, want := ice[0].URLs[0], "stun:trackerstream.xyz:3478"; got != want {
		t.Errorf("stun url = %q, want %q", got, want)
	}
	for _, e := range ice {
		for _, u := range e.URLs {
			if strings.HasPrefix(u, "turn:") || strings.HasPrefix(u, "turns:") {
				t.Fatalf("a TURN server was advertised (%q) — we do not run an open relay", u)
			}
		}
		if e.Username != "" || e.Credential != "" {
			t.Fatal("an ICE credential was handed out; STUN needs none")
		}
	}
}

func TestBootstrapDoc(t *testing.T) {
	seed := &fakeSeed{addrs: []string{
		"/ip4/127.0.0.1/udp/5478/webrtc-direct/certhash/uEiDcBGDpX1zPvsGYaZLBWO1FCUgOZ9pZRXQVOF9ObSFXPQ",
		"/ip4/5.75.131.145/udp/5478/quic-v1",
		wrtc,
	}}
	up := seed.start(t)

	s := newServer(up.URL, "trackerstream.xyz")
	if err := s.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	doc := get(t, s, http.MethodGet, http.StatusOK)
	if doc.PeerID != testPeerID {
		t.Errorf("peerId = %q, want %q", doc.PeerID, testPeerID)
	}
	if want := []string{wrtc + "/p2p/" + testPeerID}; !reflect.DeepEqual(doc.Addrs, want) {
		t.Errorf("addrs = %#v, want %#v", doc.Addrs, want)
	}
	if doc.TTL != 60 {
		t.Errorf("ttl = %d, want 60", doc.TTL)
	}
	// Exactly one entry, and it is STUN. See TestICEServersAreStunOnlyAndNeverTurn.
	if len(doc.ICEServers) != 1 || doc.ICEServers[0].URLs[0] != "stun:trackerstream.xyz:3478" {
		t.Errorf("iceServers = %#v, want a single STUN entry", doc.ICEServers)
	}
}

// TestServesCacheWhenUpstreamDown is the whole point of the ticker+cache: a seed restart (or a
// wedged RPC) must not take /bootstrap.json down with it.
func TestServesCacheWhenUpstreamDown(t *testing.T) {
	seed := &fakeSeed{addrs: []string{wrtc}}
	up := seed.start(t)

	s := newServer(up.URL, "trackerstream.xyz")
	if err := s.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	seed.down = true
	if err := s.refresh(context.Background()); err == nil {
		t.Fatal("refresh() = nil, want an error while upstream is down")
	}

	doc := get(t, s, http.MethodGet, http.StatusOK)
	if want := []string{wrtc + "/p2p/" + testPeerID}; !reflect.DeepEqual(doc.Addrs, want) {
		t.Fatalf("stale addrs not served: %#v", doc.Addrs)
	}
}

// A never-primed cache is the one case that 503s: an empty addr list would be cached by the
// client for the full TTL, which is strictly worse than telling it to come back.
func TestUnprimedCacheIs503(t *testing.T) {
	s := newServer("http://127.0.0.1:1", "trackerstream.xyz")
	get(t, s, http.MethodGet, http.StatusServiceUnavailable)
}

func TestCORSAndMethods(t *testing.T) {
	s := newServer("http://127.0.0.1:1", "trackerstream.xyz")

	for _, tc := range []struct {
		method string
		want   int
	}{
		{http.MethodOptions, http.StatusNoContent},
		{http.MethodPost, http.StatusMethodNotAllowed},
	} {
		rec := httptest.NewRecorder()
		s.handleBootstrap(rec, httptest.NewRequest(tc.method, "/bootstrap.json", nil))
		if rec.Code != tc.want {
			t.Errorf("%s -> %d, want %d", tc.method, rec.Code, tc.want)
		}
		// The browser reads this cross-origin, so ACAO must be present on EVERY response —
		// including the preflight and the rejections.
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("%s: Access-Control-Allow-Origin = %q, want *", tc.method, got)
		}
	}
}

// get drives handleBootstrap, asserts the status, and decodes the doc on 200.
func get(t *testing.T, s *server, method string, wantCode int) bootstrapDoc {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleBootstrap(rec, httptest.NewRequest(method, "/bootstrap.json", nil))
	if rec.Code != wantCode {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, wantCode, rec.Body.String())
	}
	if rec.Code != http.StatusOK {
		return bootstrapDoc{}
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "max-age=30" {
		t.Errorf("Cache-Control = %q, want max-age=30", got)
	}
	var doc bootstrapDoc
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode: %v (body %s)", err, rec.Body.String())
	}
	return doc
}
