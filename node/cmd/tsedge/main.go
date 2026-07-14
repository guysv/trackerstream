// Command tsedge is the browser bootstrap endpoint for the master seed: the one public
// HTTP surface a web client needs before it can dial anything.
//
// Why a separate process instead of a public route on tsnode's RPC: that RPC is
// loopback-only BY DESIGN. It is an unauthenticated, POST-only kubo-compatible surface with
// block/put, pin/rm, name/publish — reachable from the open internet it would be a way to
// scribble on the datastore. tsedge holds no state, opens no libp2p host, and touches no
// repo: it polls the seed's `id` over loopback and re-serves a filtered projection of it.
// It can be restarted (or fall over) without the seed noticing, and it cannot corrupt
// anything if it is compromised.
//
// Why the endpoint has to exist at all: go-libp2p regenerates its WebRTC TLS certificate on
// every process start, so the `/certhash/…` in the seed's webrtc-direct multiaddr CHANGES on
// every restart. A browser therefore cannot ship a baked-in bootstrap multiaddr the way the
// desktop client does (packages/config BOOTSTRAP_MULTIADDRS) — it must learn the live addr at
// page load, and re-learn it if a dial fails.
//
//	tsedge --rpc 127.0.0.1:5001 --listen 127.0.0.1:5090
//
// Caddy reverse-proxies https://<host>/bootstrap.json to it (deploy/Caddyfile).
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// Version is stamped at build time (`-X main.Version=...`), mirroring tsnode. tsedge
// deliberately does NOT import the tsnode package for its Version var: that would drag the
// whole libp2p+boxo dependency tree into a process whose entire job is one JSON document.
var Version = "dev"

const (
	// pollInterval is how often we refresh the seed's addrs from the loopback RPC. The
	// certhash only changes on a seed RESTART, so this is about bounding staleness after
	// one, not tracking a moving target. Serving from this cache is also what keeps a
	// flash-crowd of browsers off the seed: N browsers/second still cost the seed one
	// `id` call per interval.
	pollInterval = 15 * time.Second

	// bootstrapTTL is advertised to the client as `ttl` — how long it may keep using the
	// addrs before re-fetching. A client that fails to dial should re-fetch IMMEDIATELY
	// regardless (a restarted seed invalidates the certhash mid-TTL).
	bootstrapTTL = 60 * time.Second

	// turnTTL is the lifetime of the ephemeral TURN credential we mint. coturn's
	// use-auth-secret scheme puts the expiry in the username, so the credential is
	// self-limiting: leaking one costs at most this much relay time.
	turnTTL = time.Hour

	// upstreamTimeout bounds a single `id` poll — the seed is on loopback, so anything
	// slower than this means it is wedged and we should keep serving the cached answer.
	upstreamTimeout = 5 * time.Second
)

func main() {
	log.SetFlags(0) // journald stamps every line already (same reasoning as tsnode)

	rpcAddr := flag.String("rpc", envOr("TS_RPC", "127.0.0.1:5001"), "upstream tsnode RPC addr (loopback)")
	listen := flag.String("listen", envOr("TSEDGE_LISTEN", "127.0.0.1:5090"), "HTTP listen addr (Caddy proxies to this)")
	secretFile := flag.String("turn-secret-file", envOr("TS_TURN_SECRET_FILE", "/etc/trackerstream/turn.secret"), "coturn use-auth-secret shared secret")
	turnHost := flag.String("turn-host", envOr("TS_TURN_HOST", "trackerstream.xyz"), "STUN/TURN hostname advertised to browsers")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := newServer("http://"+*rpcAddr, *turnHost, readTurnSecret(*secretFile))

	// Prime the cache before we start listening so the very first browser doesn't get a 503.
	// A cold seed (tsedge started first) is not fatal — we log and let the poller catch up.
	if err := srv.refresh(ctx); err != nil {
		log.Printf("[tsedge] initial id poll failed (will retry): %v", err)
	}
	go srv.poll(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/bootstrap.json", srv.handleBootstrap)
	h := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Printf("[tsedge] %s serving /bootstrap.json on http://%s (upstream %s)", Version, *listen, *rpcAddr)
		if err := h.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[tsedge] listen: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	_ = h.Shutdown(shutdown)
	log.Println("[tsedge] shutting down")
}

// bootstrapDoc is the wire contract with the browser client (packages/config BOOTSTRAP_URL).
type bootstrapDoc struct {
	PeerID     string      `json:"peerId"`
	Addrs      []string    `json:"addrs"`
	ICEServers []iceServer `json:"iceServers"`
	TTL        int         `json:"ttl"`
}

// iceServer matches the browser's RTCIceServer shape verbatim — js-libp2p hands this array
// straight to the WebRTC transport, so the field names are not ours to choose.
type iceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type server struct {
	rpcBase    string
	turnHost   string
	turnSecret []byte // nil = secret unreadable; we degrade to STUN-only
	client     *http.Client
	now        func() time.Time // seam: tests pin the TURN expiry

	mu     sync.RWMutex
	peerID string
	addrs  []string
	// fetched is when `addrs` last came off the wire. We serve stale data indefinitely
	// rather than failing: a browser dialing a dead certhash simply retries, whereas a 503
	// leaves it with nothing to try at all.
	fetched time.Time
}

func newServer(rpcBase, turnHost string, turnSecret []byte) *server {
	return &server{
		rpcBase:    rpcBase,
		turnHost:   turnHost,
		turnSecret: turnSecret,
		client:     &http.Client{Timeout: upstreamTimeout},
		now:        time.Now,
	}
}

// poll refreshes the cache on a ticker until ctx dies. Upstream errors are logged and
// dropped — the cached answer stays served (see `fetched`).
func (s *server) poll(ctx context.Context) {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.refresh(ctx); err != nil {
				log.Printf("[tsedge] id poll: %v (serving cache from %s ago)", err, s.age().Truncate(time.Second))
			}
		}
	}
}

// refresh pulls `POST /api/v0/id` from the seed and installs the filtered addrs. A response
// with a peer id but ZERO usable addrs still updates the cache: that is the honest answer
// after a seed restarts without TS_LISTEN_EXTRA, and pretending otherwise would hand browsers
// a certhash that no longer exists.
func (s *server) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.rpcBase+"/api/v0/id", nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("id: upstream status %d", resp.StatusCode)
	}
	var id struct {
		ID        string   `json:"ID"`
		Addresses []string `json:"Addresses"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&id); err != nil {
		return fmt.Errorf("id: decode: %w", err)
	}
	if id.ID == "" {
		return fmt.Errorf("id: upstream returned no peer id")
	}
	addrs := browserDialable(id.Addresses, id.ID)

	s.mu.Lock()
	changed := s.peerID != id.ID || !equal(s.addrs, addrs)
	s.peerID, s.addrs, s.fetched = id.ID, addrs, s.now()
	s.mu.Unlock()
	if changed {
		// The certhash rotating IS the event this whole service exists for — log it so a
		// "browsers can't connect" report can be correlated against a seed restart.
		log.Printf("[tsedge] bootstrap changed: peer=%s addrs=%v", id.ID, addrs)
	}
	return nil
}

func (s *server) age() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.fetched.IsZero() {
		return 0
	}
	return s.now().Sub(s.fetched)
}

func (s *server) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	// The browser client is served from a different origin (and, in dev, from localhost), so
	// this must be readable cross-origin. It is a public document — a peer id and the addrs
	// the seed already announces to every peer it meets — so `*` gives away nothing.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	doc := bootstrapDoc{
		PeerID: s.peerID,
		Addrs:  append([]string(nil), s.addrs...),
		TTL:    int(bootstrapTTL / time.Second),
	}
	s.mu.RUnlock()

	if doc.PeerID == "" {
		// Never polled successfully — we have literally nothing to say. This is the one
		// case where failing is more useful than answering (an empty doc looks like a
		// seed with no browser transport, which a client would cache for the TTL).
		http.Error(w, "bootstrap unavailable", http.StatusServiceUnavailable)
		return
	}
	if doc.Addrs == nil {
		doc.Addrs = []string{}
	}
	doc.ICEServers = s.iceServers(s.now().Add(turnTTL))

	// Short cache: long enough to absorb a reload storm at the CDN/browser, short enough
	// that a seed restart is invisible to a user who reloads the page.
	w.Header().Set("Cache-Control", "max-age=30")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(doc)
}

// iceServers builds the RTCIceServer list. STUN is always offered; TURN only when we hold the
// shared secret, because a TURN entry without valid credentials is worse than no TURN entry
// (the browser burns its connectivity checks on 401s).
func (s *server) iceServers(expiry time.Time) []iceServer {
	out := []iceServer{{URLs: []string{"stun:" + s.turnHost + ":" + strconv.Itoa(stunPort)}}}
	if len(s.turnSecret) == 0 {
		return out
	}
	user, cred := turnCredential(s.turnSecret, expiry)
	out = append(out, iceServer{
		URLs:       []string{"turn:" + s.turnHost + ":" + strconv.Itoa(stunPort) + "?transport=udp"},
		Username:   user,
		Credential: cred,
	})
	return out
}

const stunPort = 3478 // coturn listening-port (deploy/turnserver.conf; packages/config STUN_PORT)

// turnCredential implements coturn's `use-auth-secret` REST scheme (the TURN REST API draft):
// the username carries its own expiry, and the password is an HMAC of that username under a
// secret only coturn and we know. This is the ONLY way to hand a browser TURN access without
// publishing a permanent credential to the open web — coturn verifies the HMAC itself, so
// there is no per-user state anywhere and a stolen credential dies at `expiry`.
//
// The realm suffix ("web") is free-form; coturn ignores everything after the colon.
//
// expiry is a parameter, not time.Now(), so the credential is a pure function of (secret,
// expiry) and the tests can assert an exact vector.
func turnCredential(secret []byte, expiry time.Time) (username, credential string) {
	username = strconv.FormatInt(expiry.Unix(), 10) + ":web"
	mac := hmac.New(sha1.New, secret)
	mac.Write([]byte(username))
	return username, base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// readTurnSecret loads the coturn shared secret, returning nil (STUN-only) if it can't. This
// MUST degrade rather than fail: coturn is still on `lt-cred-mech` with a static user today,
// so on the current box the secret may be unreadable — and /bootstrap.json is load-bearing
// for the browser client with or without a TURN relay.
func readTurnSecret(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[tsedge] TURN secret %s unreadable (%v) — serving STUN-only iceServers", path, err)
		return nil
	}
	// The file is written by `openssl rand -hex 24 > …` (deploy/install.sh), so it carries a
	// trailing newline that coturn does not include in its HMAC key.
	return []byte(strings.TrimSpace(string(b)))
}

// browserDialable projects the seed's announced addrs down to the ones a BROWSER can actually
// use, with /p2p/<id> appended so each entry is a complete dial target:
//
//   - webrtc-direct only. The seed also listens on TCP and QUIC; a browser can dial neither.
//   - public IPs only. The seed announces its loopback and (on a dev box) LAN addrs too;
//     to a browser on someone else's network those are guaranteed dial failures that just
//     burn the connection budget before it reaches the addr that works.
func browserDialable(addrs []string, peerID string) []string {
	out := make([]string, 0, 2)
	for _, s := range addrs {
		a, err := ma.NewMultiaddr(s)
		if err != nil || !isWebRTCDirect(a) || !manet.IsPublicAddr(a) {
			continue
		}
		out = append(out, s+"/p2p/"+peerID)
	}
	return out
}

func isWebRTCDirect(a ma.Multiaddr) bool {
	for _, p := range a.Protocols() {
		if p.Code == ma.P_WEBRTC_DIRECT {
			return true
		}
	}
	return false
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
