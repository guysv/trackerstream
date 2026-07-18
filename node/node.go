package tsnode

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipns"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	levelds "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	webrtcprivate "github.com/libp2p/go-libp2p/p2p/transport/webrtcprivate"
	"github.com/multiformats/go-multiaddr"
	"github.com/pion/webrtc/v4"
)

// Node is the assembled trackerstream node: a go-libp2p host with a custom-prefix
// Kademlia DHT, plus boxo's bitswap + blockstore data plane. Higher layers (gossipsub
// IPNS, unixfs cat, the RPC, the control plane) build on this.
type Node struct {
	cfg       Config
	host      host.Host
	dht       *dht.IpfsDHT
	bswap     *bitswap.Bitswap
	bstore    blockstore.Blockstore
	bserv     blockservice.BlockService
	fwdBstore blockstore.Blockstore     // bounded (entries+TTL) donor-fetch store — never the main leveldb
	fwdServ   blockservice.BlockService // donor path: reads main→fwd, writes fwd, raw bitswap
	bwc       *metrics.BandwidthCounter
	ds        ds.Batching
	keystore  *Keystore
	ipns      *ipnsStore
	playlists *playlistStore                  // bounded playlist relay buffer + announce-suppression ledger
	plLims    *lru.Cache[peer.ID, *plLimiter] // per-peer playlist-topic rate buckets (first-hop flood cap)
	plList    *plListState                    // disclosure-set manifest + playlist-list serving state
	beacons   *beaconState                    // hold-beacon counts (hash → origins, 24h window)
	pubsub    *PubSub
	pins      *Pinset
	control   *control
	fwd       *fwdState            // block-forwarding donor state (rate cap + bounded cache)
	seeds     map[peer.ID]struct{} // bootstrap (seed) peer IDs — excluded from peer-provider dialing

	// Provide queue: catalog page-sharing advertises every fetched leaf CID, so a single
	// broad query could fire hundreds of DHT Provides at once (one goroutine each) —
	// a storm that saturates the DHT and, when NAT'd, floods the log with timeouts. All
	// advertisements now funnel through this bounded, deduped, fixed-concurrency queue
	// instead: page-sharing is preserved, but the advertise rate is capped by construction.
	provideQ  chan cid.Cid                 // bounded; provideNow enqueues non-blocking (drops when full)
	provDedup *lru.Cache[string, struct{}] // recently-Provided CIDs — skip the redundant DHT walk
	provOK    atomic.Int64                 // advertise successes (summarised, not per-CID logged)
	provFail  atomic.Int64                 // advertise failures

	startedAt         time.Time     // process start (seed/status uptime)
	reprovideUnix     atomic.Int64  // unix seconds of the last completed reprovide sweep (0 = never)
	reprovideInterval time.Duration // reprovide period (for next-sweep ETA in seed/status)
}

const (
	provideQueueSize = 8192    // pending advertisements before overflow drops (best-effort)
	provideWorkers   = 4       // concurrent DHT Provides — bounds the goroutine fan-out
	provideDedupSize = 1 << 16 // recently-Provided CIDs remembered to coalesce re-fetches
)

// logf is the node's structured-ish log sink (stderr). Kept trivial; the deploy captures
// stderr via journald.
func (n *Node) logf(format string, args ...any) {
	log.Printf("[tsnode] "+format, args...)
}

// hpTracer logs DCUtR hole-punch outcomes so a relay-coordinated punch is distinguishable
// from a plain direct-dial failure. A "dial provider … all dials failed" with NO matching
// `[holepunch]` line for that peer means the punch never started (no /p2p-circuit to coordinate
// through — a remote AutoRelay-reservation gap, not a punch that failed). An explicit
// `[holepunch] END … success=false` is a genuine failed punch. Constructed before the Node
// exists, so it logs via the package logger directly (same stderr/journald sink as logf).
type hpTracer struct{}

func (hpTracer) Trace(e *holepunch.Event) {
	switch v := e.Evt.(type) {
	case *holepunch.StartHolePunchEvt:
		log.Printf("[tsnode] [holepunch] start remote=%s addrs=%v", e.Remote, v.RemoteAddrs)
	case *holepunch.HolePunchAttemptEvt:
		log.Printf("[tsnode] [holepunch] attempt=%d remote=%s", v.Attempt, e.Remote)
	case *holepunch.EndHolePunchEvt:
		log.Printf("[tsnode] [holepunch] END remote=%s success=%t elapsed=%s err=%q",
			e.Remote, v.Success, v.EllapsedTime, v.Error)
	}
}

// New assembles and starts the node. Cancelling ctx (or calling Close) tears it down.
func New(ctx context.Context, cfg Config) (*Node, error) {
	priv, err := loadOrCreateKey(cfg.RepoPath)
	if err != nil {
		return nil, fmt.Errorf("identity: %w", err)
	}
	datastore, err := openDatastore(cfg.RepoPath)
	if err != nil {
		return nil, fmt.Errorf("datastore: %w", err)
	}
	bootstrap, err := parseAddrInfos(cfg.Bootstrap)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}

	// Browser-reachability relay path (R6). A NATed client is reachable by a browser ONLY over its
	// /p2p-circuit/webrtc address, but go-libp2p's AutoRelay + address manager surface circuit addrs
	// ONLY while reachability is Private — so a (mis)flap to Public (a sample-starved AutoNAT, or a
	// UPnP guess that isn't actually inbound-dialable) tears down the reservation and DELETES the
	// browser's only route. We decouple the two: hold our OWN standing reservation on the master
	// (reservationLoop) and advertise the circuit addrs while it's held (makeWebrtcCircuitAddrsFactory),
	// regardless of AutoNAT. ModeAuto is untouched — DHT self-promotion and direct-addr advertisement
	// still track reachability; we only stop a Public verdict from removing the relay FALLBACK.
	var reservationHeld atomic.Bool
	master := masterAddrInfo(bootstrap)
	relayFallback := cfg.Role == RoleClient && len(bootstrap) > 0 && len(cfg.STUNServers) > 0
	var relayCircuitAddrs []multiaddr.Multiaddr // /p2p/<master>/p2p-circuit(/webrtc) — advertised while held
	if relayFallback {
		relayCircuitAddrs = circuitWebRTCAddrs(master)
	}

	bwc := metrics.NewBandwidthCounter()

	var idht *dht.IpfsDHT
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		// Stamp the app version + role into identify's AgentVersion (e.g.
		// "trackerstream/0.3.1/client"). Advisory/observability only — lets a future release
		// read the network's version distribution and avoid offering old peers a newer
		// protocol variant. Never gate correctness on it. Both roles set it.
		libp2p.UserAgent("trackerstream/" + Version + "/" + cfg.Role.agentRole()),
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.BandwidthReporter(bwc),
		libp2p.EnableNATService(),                                   // AutoNAT (reachability)
		libp2p.EnableHolePunching(holepunch.WithTracer(hpTracer{})), // DCUtR (+ per-attempt trace)
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			// Server is always public → forced ModeServer (deterministic, no AutoNAT wait).
			// Clients run ModeAuto: the NATed majority stay clients, but a peer AutoNAT confirms
			// is publicly reachable promotes itself to a DHT server — decentralising the table
			// off the single seed. Relay-only reachability never promotes (stays client).
			mode := dht.ModeAuto
			if cfg.Role == RoleServer {
				mode = dht.ModeServer
			}
			idht, err = dht.New(ctx, h,
				dht.Mode(mode),
				// Custom protocol prefix → `/trackerstream/kad/1.0.0`. A distinct id keeps
				// the routing table trackerstream-only (libp2p only routes to peers speaking
				// the matching protocol), so no public-IPFS crawl.
				dht.ProtocolPrefix(DHTPrefix),
				// PERSIST DHT records (IPNS, provider) in the node's datastore. Default is an
				// in-memory store, which loses every published IPNS record on restart — the
				// master would then answer `routing/get` (and serve clients' GetValue) with
				// "not found" until the next republish, reading as "catalog offline". A shared
				// leveldb is safe: DHT keys (/ipns/.., /pk/..) don't collide with /blocks/.. .
				dht.Datastore(datastore),
				// NOTE: deliberately NOT dht.BootstrapPeers(bootstrap...). That option makes
				// the DHT dial the bootstrap peer DURING libp2p.New — i.e. BEFORE bitswap.New
				// registers its connection notifiee below — so Bitswap never learns the peer is
				// connected and its want-broadcast reaches nobody (block fetch hangs forever).
				// We instead dial bootstrap explicitly AFTER Bitswap is up (see the goroutines
				// at the end of New), so Bitswap sees the Connected event; the DHT's own
				// notifiee then seeds its routing table from that same connection.
				// IPNS record validation in the custom DHT's `/ipns` namespace, so signed
				// records put/get correctly as the box-down resolve fallback.
				dht.NamespacedValidator("ipns", ipns.Validator{KeyBook: h.Peerstore()}),
			)
			return idht, err
		}),
	}
	if cfg.Role == RoleServer {
		// The seed's relay is the CLAMPED coordination floor (default 128KB/2min). It faces the
		// whole swarm, so it must NOT carry bulk — it exists only as the always-reachable DCUtR
		// rendezvous. The clamp flags relayed conns Limited, which bitswap refuses (by design).
		//
		// KEEP THE CLAMP. Browsers now reserve here too — a web peer needs a reservation to be
		// dialable at all, and the relayed connection is what carries the WebRTC SDP/ICE signalling
		// for browser<->browser (a few KB, done in seconds). Raising the clamp so "relayed bitswap
		// would work" is the tempting, wrong move: the Limited flag is precisely what keeps bulk
		// traffic OFF the master, and lifting it turns the seed into everyone's data pipe.
		//
		// The RESERVATION ceilings, though, were sized for a handful of NATed desktops and are now
		// the cap on concurrent dialable WEB peers. The defaults are actively harmful here:
		//   - MaxReservations 128:  the whole web audience, on one number.
		//   - PerIP 8:              a self-DoS. One university/office/CGNAT pool exhausts it and
		//                           everyone behind it silently becomes undialable.
		//   - PerASN 32:            entire mobile carriers sit behind a single ASN.
		// The table itself is trivial (~200B/entry); the real cost is that each reserving browser
		// holds an open webrtc-direct conn (pion DTLS+SCTP, roughly 100-300KB RSS). 1024 is ~250MB
		// of pion state before headroom — measure RSS/conn on the box before raising it further.
		rc := relay.DefaultResources()
		rc.MaxReservations = 1024
		rc.MaxReservationsPerIP = 64
		rc.MaxReservationsPerASN = 512
		opts = append(opts, libp2p.EnableRelayService(relay.WithResources(rc)))
	} else {
		// Clients offer a GENEROUS (infinite-limit) relay — but go-libp2p only STARTS it once
		// AutoNAT confirms the node is publicly reachable, so the NATed majority never relay and
		// the reachable minority offload bulk for others. Infinite limits are required: any finite
		// limit flags the connection Limited and bitswap won't traverse it (it's a binary gate).
		opts = append(opts, libp2p.EnableRelayService(relay.WithInfiniteLimits()))
		// AutoRelay: reserve a slot so a NATed client gets a /p2p-circuit address (the DCUtR
		// coordination path) AND keeps a live connection to its donor (which the block-forwarder
		// rides). The peer source yields the bootstrap seed(s) FIRST (preserving master-as-relay),
		// then reachable peer donors discovered via the donorRendezvous (R5 Phase B). AutoRelay runs
		// only while reachability is Private/Unknown and stops once Public — the mirror of the
		// relay-service gate. Skipped when there's no bootstrap (ephemeral/in-memory test nodes).
		if len(bootstrap) > 0 {
			self, _ := peer.IDFromPrivateKey(priv)
			opts = append(opts, libp2p.EnableAutoRelayWithPeerSource(donorPeerSource(&idht, self, bootstrap)))
		}
		// Make a NATed client discoverable over private-to-private WebRTC: rewrite the advertised
		// addrs so each "/p2p-circuit" reservation also surfaces as "…/p2p-circuit/webrtc" (the
		// address a browser dials to hole-punch to us). Without this go-libp2p advertises only a
		// bare, undialable "/webrtc" and never combines it with the reservation. It also appends the
		// master circuit path unconditionally while reservationLoop holds our standing reservation —
		// so a flap to Public can't strip the browser's only route. See makeWebrtcCircuitAddrsFactory.
		if len(cfg.STUNServers) > 0 {
			opts = append(opts, libp2p.AddrsFactory(makeWebrtcCircuitAddrsFactory(relayCircuitAddrs, &reservationHeld)))
		}
		// UPnP / NAT-PMP: opportunistically map the swarm ports on a UPnP-capable home
		// router so a NATed client becomes directly reachable — no relay/DCUtR needed.
		// When it succeeds AutoNAT flips to Public, which cascades: DHT self-promotion
		// (ModeAuto) + the infinite-limit client relay activates. Where there's no IGD
		// (CGNAT, UPnP disabled) it's a silent no-op and we fall back to relay+DCUtR.
		// Client-only: the server is direct-bound on a public IP with no gateway to map.
		// Gated by cfg.DisableNATPortMap (TS_NO_NATPORTMAP) so a host with a misbehaving
		// IGD can fall back to pure relay+DCUtR at runtime, no rebuild.
		if !cfg.DisableNATPortMap {
			opts = append(opts, libp2p.NATPortMap())
		}
	}
	if cfg.Role == RoleServer {
		// The master is an always-on bootstrap + seeder facing a large, churny inbound swarm
		// (incl. residual public-IPFS dials on the well-known PeerId/port). go-libp2p's DEFAULT
		// resource-manager limits are far too low for that: the Transient (pre-identify upgrade)
		// scope fills under a dial storm and the master RESETS legitimate client connections
		// mid-handshake ("failed to negotiate security protocol") — the catalog then reads
		// offline. Raise System/Transient/Peer conn+stream ceilings well above defaults (mirrors
		// the kubo rcmgr override in deploy/install.sh); Memory/FD stay at the scaled default as
		// the real backstop. Plus a high-watermark conn manager so a flash crowd is HELD, not
		// trimmed. See memory: master-connection-storms.
		rm, err := serverResourceManager()
		if err != nil {
			return nil, fmt.Errorf("resource manager: %w", err)
		}
		cm, err := connmgr.NewConnManager(1000, 3000, connmgr.WithGracePeriod(60*time.Second))
		if err != nil {
			return nil, fmt.Errorf("conn manager: %w", err)
		}
		opts = append(opts, libp2p.ResourceManager(rm), libp2p.ConnectionManager(cm))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("libp2p host: %w", err)
	}

	// Private-to-private WebRTC (webrtcprivate): lets a browser dial THIS node over
	// /p2p-circuit/webrtc when it's NATed and thus not browser-dialable directly. The transport
	// listens on /webrtc (over the existing AutoRelay reservation) and hole-punches to a DIRECT
	// datachannel for bitswap. Gated on STUN being configured, which stunServersFor restricts to
	// the client role — the master is reached over webrtc-direct and never needs this. gater=nil:
	// we run no connection gater (the overlay is private by DHT-prefix, not by dial filtering).
	if len(cfg.STUNServers) > 0 {
		ice := make([]webrtc.ICEServer, 0, len(cfg.STUNServers))
		for _, s := range cfg.STUNServers {
			ice = append(ice, webrtc.ICEServer{URLs: []string{s}})
		}
		if _, err := webrtcprivate.AddTransport(h, nil, ice); err != nil {
			return nil, fmt.Errorf("webrtcprivate transport: %w", err)
		}
	}

	bstore := blockstore.NewBlockstore(datastore)
	// Bounded donor-fetch store (Phase 0, PLAYLISTS.md §3): forwarded blocks land here —
	// NEVER in the main GC-disabled leveldb. Same entry+TTL semantics as the old fwd LRU,
	// behind the Blockstore interface so a blockservice can write to it.
	fwdBstore := newLRUBlockstore(fwdCacheSize, fwdCacheTTL)
	net := bsnet.NewFromIpfsHost(h)
	// Bitswap serves from main ∪ fwd (donor-cached blocks are servable to peers) AND fetches;
	// the DHT is the content router for provider discovery, but wants also broadcast to
	// connected peers (the offload path).
	bswap := bitswap.New(ctx, net, idht, &tieredBlockstore{
		read:  []blockstore.Blockstore{bstore, fwdBstore},
		write: bstore,
	})
	// Wrap the exchange so EVERY blockservice fetch (block/get, batch, and the catalog DAG-walk
	// sessions) gains the block-forwarding fallback (R5) — one chokepoint, not per-handler. The donor's
	// own transitive fetch passes a no-forward context to stay raw. `fex.n` is late-bound below (the
	// node doesn't exist yet); no fetch runs until New returns.
	fex := &fwdExchange{SessionExchange: bswap}
	bserv := blockservice.New(bstore, fex)
	// Donor-path blockservice (fwdServe): reads main→fwd, WRITES fwd (bounded), and rides the
	// RAW bitswap exchange — never fex — so serving a Fetch can structurally never trigger
	// another Fetch (anti-amplification by construction; ctxNoForward remains belt-and-braces).
	fwdServ := blockservice.New(&tieredBlockstore{
		read:  []blockstore.Blockstore{bstore, fwdBstore},
		write: fwdBstore,
	}, bswap)

	keystore, err := NewKeystore(cfg.RepoPath)
	if err != nil {
		return nil, fmt.Errorf("keystore: %w", err)
	}
	pins, err := newPinset(datastore)
	if err != nil {
		return nil, fmt.Errorf("pinset: %w", err)
	}
	ps, err := newPubSub(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("pubsub: %w", err)
	}

	n := &Node{
		cfg:       cfg,
		host:      h,
		dht:       idht,
		bswap:     bswap,
		bstore:    bstore,
		bserv:     bserv,
		fwdBstore: fwdBstore,
		fwdServ:   fwdServ,
		bwc:       bwc,
		ds:        datastore,
		keystore:  keystore,
		ipns:      newIpnsStore(),
		// Playlist docs are buffered on CLIENTS only: the seed forwards playlist gossip
		// (it must subscribe to relay the mesh) but saves records alone — never content.
		playlists: newPlaylistStore(cfg.Role == RoleClient),
		plLims:    mustLRU[peer.ID, *plLimiter](plPeerLims),
		plList:    newPlListState(),
		beacons:   newBeaconState(),
		pubsub:    ps,
		pins:      pins,
		fwd:       newFwdState(),
		seeds:     map[peer.ID]struct{}{},
		startedAt: time.Now(),
	}
	for _, ai := range bootstrap {
		n.seeds[ai.ID] = struct{}{}
	}
	fex.n = n // late-bind: the assisted exchange can now reach the node (forwarding + suppression)
	n.control = newControl(n)
	if err := n.control.start(ctx); err != nil {
		return nil, fmt.Errorf("control plane: %w", err)
	}
	// Block-forwarding donor handler (R5) — CLIENTS only. The master seed never forwards: it serves
	// its OWN pinned content via bitswap but is not a proxy for peer-to-peer user traffic. handleFwd
	// further gates on public reachability, so only public client donors actually carry forwarding.
	if cfg.Role == RoleClient {
		n.setStreamHandler(n.handleFwd, FwdProtocol)
		// Playlist-list serving is also client-only: the seed holds no library
		// (its manifest is forever empty), so it never answers.
		n.setStreamHandler(n.handlePlaylistList, PlaylistListProtocol)
	}
	// Zero-resolve wiring (Phase D): every signed catalog record pushed on the gossipsub topic
	// is validated + stored locally (newest-seq wins), so a client's `routing/get` answers
	// instantly from this store with no DHT round-trip. The topic is untrusted; ingestGossip
	// checks the signature against the name and the consumer (Rust) re-verifies on use.
	n.pubsub.OnCatalogRecord(func(name string, record []byte) {
		if err := n.ipns.ingestGossip(name, record); err != nil {
			n.logf("catalog gossip ingest for %s rejected: %v", name, err)
		}
	})
	if err := n.pubsub.SubscribeCatalog(ctx); err != nil {
		return nil, fmt.Errorf("catalog subscribe: %w", err)
	}
	// Playlist topic (PLAYLISTS.md): validator makes every message self-certifying at the
	// first hop (signature + EOL + doc-hash-vs-CID); the sink fills the bounded relay
	// buffer that the desktop drains via `playlist/records`.
	if err := n.pubsub.SetupPlaylist(ctx, n.playlistValidator, n.playlistSink); err != nil {
		return nil, fmt.Errorf("playlist topic: %w", err)
	}
	// Hold-beacon topic (PLAYLISTS.md §10): every role subscribes (relay + count);
	// only clients publish — the seed holds no library, its manifest stays empty.
	if err := n.pubsub.SetupBeacon(ctx, n.beaconValidator, n.beaconSink); err != nil {
		return nil, fmt.Errorf("beacon topic: %w", err)
	}
	if cfg.Role == RoleClient {
		go n.beaconLoop(ctx)
	}
	// Start the bounded provide queue before anything can advertise (CatCatalog / Provide*
	// only run after New returns, so this is race-free).
	n.provideQ = make(chan cid.Cid, provideQueueSize)
	n.provDedup, _ = lru.New[string, struct{}](provideDedupSize)
	for i := 0; i < provideWorkers; i++ {
		go n.provideWorker(ctx)
	}
	go n.provideStatsLoop(ctx)

	// Reprovide pinned roots to the custom DHT (Provide.Strategy=roots; 22h in prod). The loop also
	// advertises the donor rendezvous (R5) while this node is a public CLIENT donor — the seed never
	// advertises itself as a donor, because it never forwards.
	go n.reprovideLoop(ctx, 22*time.Hour)

	// Mesh membership (R6 star→mesh): keep a few DIRECT edges to OTHER clients so gossipsub grafts
	// client↔client links instead of funneling everything through the master. Clients only — the
	// server is already connected to everyone; and only with bootstrap (nothing to discover otherwise).
	if cfg.Role == RoleClient && len(bootstrap) > 0 {
		go n.meshLoop(ctx)
	}

	// Standing relay reservation on the master (R6 browser-reachability): hold a reservation
	// independent of AutoNAT so /p2p-circuit/webrtc stays advertised even when reachability flaps to
	// Public (go-libp2p's AutoRelay drops its reservation there, which would leave a browser with no
	// dialable path to this NATed client). Reserving eagerly also removes the cold-start window where
	// a just-started client has no circuit addr yet. Runs alongside AutoRelay (both refresh the one
	// per-peer reservation when Private; this is the sole keeper when Public).
	if relayFallback {
		go n.reservationLoop(ctx, master, &reservationHeld)
	}

	// Dial the configured bootstrap peers (the box) AFTER Bitswap is up, so its connection
	// notifiee catches the Connected event (the broadcast-want path depends on it). The DHT's
	// own notifiee seeds the routing table from the same connection; we then kick a routing
	// refresh so provider lookups work promptly.
	if len(bootstrap) > 0 {
		go func() {
			var wg sync.WaitGroup
			for _, ai := range bootstrap {
				ai := ai
				wg.Add(1)
				go func() {
					defer wg.Done()
					cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()
					if err := h.Connect(cctx, ai); err != nil {
						n.logf("bootstrap dial %s failed: %v", ai.ID, err)
					}
				}()
			}
			wg.Wait()
			if idht != nil {
				_ = idht.Bootstrap(ctx)
			}
		}()
	}
	return n, nil
}

// setStreamHandler registers h under a LIST of protocol IDs. Today each call passes a single
// ID, but the signature takes a variadic so the FIRST real protocol bump is a one-line change
// — add the new ID alongside the old (`n.setStreamHandler(h, FooProto, FooProtoV2)`) so both
// are served during the overlap window — instead of flipping a string and partitioning every
// peer still on the old ID. See the protocol-evolution policy in config.go.
func (n *Node) setStreamHandler(h network.StreamHandler, ids ...protocol.ID) {
	for _, id := range ids {
		n.host.SetStreamHandler(id, h)
	}
}

// ID is the node's libp2p PeerId.
func (n *Node) ID() peer.ID { return n.host.ID() }

// Host exposes the libp2p host (for gossipsub, relay, the control plane).
func (n *Node) Host() host.Host { return n.host }

// DHT exposes the custom-prefix Kademlia DHT (providers + IPNS records + peer routing).
func (n *Node) DHT() *dht.IpfsDHT { return n.dht }

// Addrs is the node's current listen multiaddrs.
func (n *Node) Addrs() []multiaddr.Multiaddr { return n.host.Addrs() }

// Bandwidth exposes the per-peer/per-protocol byte counter (the peers-pane attribution).
func (n *Node) Bandwidth() *metrics.BandwidthCounter { return n.bwc }

// Bitswap exposes the boxo exchange for read-only stats (Stat / LedgerForPeer / WantlistForPeer)
// — the seed-monitor ledger view (who we've actually served, and what each peer still wants).
func (n *Node) Bitswap() *bitswap.Bitswap { return n.bswap }

// ProvideStats reports DHT-advertise health: cumulative successes/failures plus the bounded
// provide queue's current depth and capacity (a persistently-full queue = advertise backpressure).
func (n *Node) ProvideStats() (ok, fail int64, queue, capacity int) {
	q, c := 0, 0
	if n.provideQ != nil {
		q, c = len(n.provideQ), cap(n.provideQ)
	}
	return n.provOK.Load(), n.provFail.Load(), q, c
}

// Seeds returns the configured bootstrap/seed peer IDs (excluded from peer-provider dialing).
func (n *Node) Seeds() []peer.ID {
	out := make([]peer.ID, 0, len(n.seeds))
	for id := range n.seeds {
		out = append(out, id)
	}
	return out
}

// Uptime is how long this process has been assembled (seed/status header).
func (n *Node) Uptime() time.Duration { return time.Since(n.startedAt) }

// LastReprovide is the unix time of the last completed reprovide sweep (0 = none yet), and the
// configured interval — the monitor computes the next-sweep ETA from the two.
func (n *Node) LastReprovide() (unix int64, interval time.Duration) {
	return n.reprovideUnix.Load(), n.reprovideInterval
}

// Connect dials a peer (used for bootstrap, warm-set formation, the test harness).
func (n *Node) Connect(ctx context.Context, ai peer.AddrInfo) error {
	return n.host.Connect(ctx, ai)
}

// PutBlock stores a block we hold and notifies bitswap so it serves it.
func (n *Node) PutBlock(ctx context.Context, b blocks.Block) error {
	return n.bserv.AddBlock(ctx, b)
}

// PutBlocks stores many blocks in ONE leveldb batch — the blockservice routes to
// blockstore.PutMany, which commits via a single datastore.Batch (one fsync for the
// whole slice) instead of one fsync per block. On the sync-per-write leveldb (and
// especially a high-latency network volume) this is the difference between the bulk
// ingest floor and ~batch-size× that. Callers batch a whole module's DAG per call.
func (n *Node) PutBlocks(ctx context.Context, blks []blocks.Block) error {
	return n.bserv.AddBlocks(ctx, blks)
}

// GetBlock fetches a block: local blockstore, else Bitswap from connected peers / providers.
func (n *Node) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return n.bserv.GetBlock(ctx, c)
}

// Keystore exposes the named-key store (key/gen, key/list, IPNS publishing).
func (n *Node) Keystore() *Keystore { return n.keystore }

// Pins exposes the pinset (pin/add, pin/rm, reprovide, verify).
func (n *Node) Pins() *Pinset { return n.pins }

// Control exposes the control plane (reachability, relay stats, warm set) for node/status.
func (n *Node) Control() *control { return n.control }

// PubSub exposes the gossipsub wrapper (catalog topic).
func (n *Node) PubSub() *PubSub { return n.pubsub }

// ImportKey brings an `ipfs key export` blob into the keystore under name (the prod swarm +
// catalog identities), so the same PeerId / IPNS name is reproduced on tsnode.
func (n *Node) ImportKey(name string, marshaled []byte) error {
	return n.keystore.Import(name, marshaled)
}

// Pin recursively pins a root and immediately advertises it to the custom DHT (so a freshly
// ingested root is discoverable without waiting for the reprovide sweep).
func (n *Node) Pin(ctx context.Context, c cid.Cid) error {
	if err := n.pins.Add(ctx, c); err != nil {
		return err
	}
	n.provideNow(c)
	return nil
}

// ProvideTrackRoot records + advertises a track manifest root this node holds. Peers want the
// whole track, so advertising just the root suffices — bitswap pulls the interior DAG from the
// provider once connected. Interior track blocks are deliberately NOT advertised (DHT stays
// light). Call this when a client has streamed/cached a track it is willing to serve.
func (n *Node) ProvideTrackRoot(ctx context.Context, c cid.Cid) error {
	if err := n.pins.AddTrackRoot(ctx, c); err != nil {
		return err
	}
	n.provideNow(c)
	return nil
}

// ProvideCatalogPiece records + advertises a single catalog block this node fetched. Catalog
// access is random/partial, so peers advertising the pages they hold lets clients fetch catalog
// pieces from each other and offload the seed. These are cache entries — pair with Unpin to evict.
func (n *Node) ProvideCatalogPiece(ctx context.Context, c cid.Cid) error {
	if err := n.pins.AddCatalogPiece(ctx, c); err != nil {
		return err
	}
	n.provideNow(c)
	return nil
}

// DialProviders looks up providers of c on the DHT and connects to the NON-seed ones, so a
// subsequent bitswap fetch can pull from peers instead of only the always-connected seed. This is
// the "try other peers, fall back to the seed" hook: bitswap won't discover peer providers on its
// own here (the seed is connected and answers for roots, short-circuiting content routing; interior
// blocks aren't advertised at all), so we surface peer providers explicitly. The provider record
// carries each peer's seed-observed addresses — including its LAN address — so same-network peers
// can connect directly. Best-effort and bounded; returns how many non-seed providers we hold a
// connection to afterwards. The seed is never dialed here (it's already connected) and stays the
// fallback once these peers are in the swarm.
func (n *Node) DialProviders(ctx context.Context, c cid.Cid) int {
	if n.dht == nil {
		return 0
	}
	self := n.host.ID()
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	connected := 0
	for p := range n.dht.FindProvidersAsync(fctx, c, 16) {
		if p.ID == self {
			continue
		}
		// Discovery hint: a NAT'd provider advertises a /…/p2p/<B>/p2p-circuit/… addr — B is its
		// donor (every tsnode runs both the clamped relay AND the forwarder). Keep B warm so a later
		// FwdFetch for this provider's interior blocks (never on the DHT) has a live forwarder.
		for _, a := range p.Addrs {
			if b, ok := relayHopID(a); ok && b != self {
				if _, isSeed := n.seeds[b]; !isSeed {
					n.control.Warm(b)
				}
			}
		}
		if _, isSeed := n.seeds[p.ID]; isSeed {
			continue
		}
		// Remember this content provider so GetBlockAssisted can suppress forwarding once we hold a
		// direct (hole-punched) connection to it — letting direct take over the instant it forms.
		n.fwd.recordProvider(p.ID)
		if n.host.Network().Connectedness(p.ID) == network.Connected {
			mu.Lock()
			connected++
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func(ai peer.AddrInfo) {
			defer wg.Done()
			cctx, cc := context.WithTimeout(ctx, 8*time.Second)
			defer cc()
			if err := n.host.Connect(cctx, ai); err != nil {
				n.logf("dial provider %s failed: %v", ai.ID, err)
				return
			}
			n.control.Warm(ai.ID) // keep a useful peer source warm
			mu.Lock()
			connected++
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	return connected
}

// provideCatalogSource advertises the catalog root this node reads, marking us as a holder of
// catalog content so peers' dial-providers(catalogRoot) discover + connect to us. Deduped; the
// node holds the root block (the cat reader fetched it), so serving it is honest.
func (n *Node) provideCatalogSource(c cid.Cid) {
	if n.pins == nil || n.pins.Has(c) {
		return
	}
	// KindRoot — "I hold (some of) this catalog". Log a datastore-write failure: we'd otherwise
	// still advertise (below) a root we failed to record, so the pin index silently drifts.
	if err := n.pins.Add(context.Background(), c); err != nil {
		n.logf("provideCatalogSource: pin add %s: %v", c, err)
	}
	n.provideNow(c)
}

// provideCatalogPieces records + advertises freshly-fetched catalog page CIDs (the leaf blocks a
// CatCatalog read touched), skipping any already tracked so a re-read doesn't re-advertise.
func (n *Node) provideCatalogPieces(cids []cid.Cid) {
	if n.pins == nil {
		return
	}
	for _, c := range cids {
		if n.pins.Has(c) {
			continue
		}
		if err := n.pins.AddCatalogPiece(context.Background(), c); err != nil {
			n.logf("provideCatalogPieces: pin add %s: %v", c, err)
		}
		n.provideNow(c)
	}
}

// provideNow enqueues a single best-effort DHT advertisement onto the bounded provide queue
// (drained by provideWorker). Non-blocking: if the queue is full it drops the advertisement
// rather than spawn an unbounded goroutine — the reprovide loop re-advertises roots on its long
// interval, and catalog pieces are best-effort (a dropped one just means a peer fetches that page
// from another holder). This is what keeps a broad query from firing a provide storm.
func (n *Node) provideNow(c cid.Cid) {
	if n.dht == nil || n.provideQ == nil {
		return
	}
	select {
	case n.provideQ <- c:
	default: // queue full — drop (bounded by design)
	}
}

// provideWorker drains the provide queue with fixed concurrency (provideWorkers of these run).
// Deduped: a CID advertised recently is skipped, so re-reads of the same catalog pages don't
// re-walk the DHT. Only successful advertisements enter the dedup set, so a transient failure
// is retried when the page is fetched again.
func (n *Node) provideWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-n.provideQ:
			k := c.KeyString()
			if _, dup := n.provDedup.Get(k); dup {
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			err := n.dht.Provide(cctx, c, true)
			cancel()
			if err != nil {
				n.provFail.Add(1)
			} else {
				n.provDedup.Add(k, struct{}{})
				n.provOK.Add(1)
			}
		}
	}
}

// provideStatsLoop replaces the old per-CID "provide … failed" log line with a periodic
// summary — a NAT'd client can fail thousands of advertisements, and one line each buried the
// log. Silent when there was no advertise activity in the window.
func (n *Node) provideStatsLoop(ctx context.Context) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	var lastOK, lastFail int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ok, fail := n.provOK.Load(), n.provFail.Load()
			dOK, dFail := ok-lastOK, fail-lastFail
			if dOK+dFail > 0 {
				n.logf("provide: %d ok, %d failed (60s); queue=%d", dOK, dFail, len(n.provideQ))
				lastOK, lastFail = ok, fail
			}
		}
	}
}

// Unpin removes a root pin (idempotent).
func (n *Node) Unpin(ctx context.Context, c cid.Cid) error { return n.pins.Remove(ctx, c) }

// Close tears the node down.
func (n *Node) Close() error {
	if n.bswap != nil {
		_ = n.bswap.Close()
	}
	if n.host != nil {
		_ = n.host.Close()
	}
	if n.ds != nil {
		_ = n.ds.Close()
	}
	return nil
}

// --- helpers ---------------------------------------------------------------------------

func openDatastore(repo string) (ds.Batching, error) {
	if repo == "" {
		return dssync.MutexWrap(ds.NewMapDatastore()), nil
	}
	if err := os.MkdirAll(repo, 0o700); err != nil {
		return nil, err
	}
	return levelds.NewDatastore(filepath.Join(repo, "datastore"), nil)
}

// loadOrCreateKey reads (or mints + persists) the node identity as a libp2p
// protobuf-encoded private key — the SAME encoding the Rust side and `ipfs key export`
// use, so the server can later import kubo's swarm + catalog keys to preserve PeerIds.
func loadOrCreateKey(repo string) (crypto.PrivKey, error) {
	if repo == "" {
		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		return priv, err
	}
	path := filepath.Join(repo, "identity.key")
	if data, err := os.ReadFile(path); err == nil {
		return crypto.UnmarshalPrivateKey(data)
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	data, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(repo, 0o700); err != nil {
		return nil, err
	}
	return priv, os.WriteFile(path, data, 0o600)
}

// makeWebrtcCircuitAddrsFactory builds the AddrsFactory that makes a NATed client discoverable over
// private-to-private WebRTC. go-libp2p advertises the webrtcprivate listener as a bare, undialable
// "/webrtc" and never combines it with the "/p2p-circuit" reservation addresses. The factory (a)
// drops the bare "/webrtc" — nothing can dial it; (b) for every address ending in "/p2p-circuit",
// additionally advertises "…/p2p-circuit/webrtc", which is what a browser dials to hole-punch to us
// (webrtcprivate CanDial requires circuit + webrtc); and (c) while `held` (our standing master
// reservation is live, see reservationLoop) appends `extra` — the master circuit path — UNCONDITIONALLY.
// Part (c) is the fix: the address manager only surfaces its own "/p2p-circuit" addrs while
// reachability is Private, so a flap to Public would otherwise delete the browser's ONLY route to a
// NATed client. Advertising the relay path while Public is a pure superset (the same address shape,
// just kept alive); a genuinely-public client still advertises its direct webrtc-direct addr too, and
// a browser prefers that.
func makeWebrtcCircuitAddrsFactory(extra []multiaddr.Multiaddr, held *atomic.Bool) func([]multiaddr.Multiaddr) []multiaddr.Multiaddr {
	return func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		out := make([]multiaddr.Multiaddr, 0, len(addrs)+len(extra)+1)
		seen := make(map[string]struct{}, len(addrs)+len(extra)+1)
		add := func(a multiaddr.Multiaddr) {
			s := a.String()
			if _, ok := seen[s]; ok {
				return
			}
			seen[s] = struct{}{}
			out = append(out, a)
		}
		for _, a := range addrs {
			if isBareWebRTCAddr(a) {
				continue
			}
			add(a)
			if endsInCircuit(a) {
				add(a.Encapsulate(webrtcprivate.WebRTCAddr))
			}
		}
		if held.Load() {
			for _, a := range extra {
				add(a)
			}
		}
		return out
	}
}

// masterAddrInfo folds all bootstrap entries sharing the first entry's peer ID into one AddrInfo for
// the master relay — the box is listed once per transport. Zero value (empty ID) when no bootstrap.
func masterAddrInfo(bootstrap []peer.AddrInfo) peer.AddrInfo {
	if len(bootstrap) == 0 {
		return peer.AddrInfo{}
	}
	m := peer.AddrInfo{ID: bootstrap[0].ID}
	for _, ai := range bootstrap {
		if ai.ID == m.ID {
			m.Addrs = append(m.Addrs, ai.Addrs...)
		}
	}
	return m
}

// circuitWebRTCAddrs builds the advertise set for reaching THIS node through the master relay: for
// each master transport addr, "<addr>/p2p/<master>/p2p-circuit" and its "…/p2p-circuit/webrtc" variant
// (what a browser dials to hole-punch in). Mirrors exactly the addrs the address manager surfaces when
// Private, so advertising them while held is a superset — no new address shape, just kept alive.
func circuitWebRTCAddrs(master peer.AddrInfo) []multiaddr.Multiaddr {
	if master.ID == "" {
		return nil
	}
	suffix := multiaddr.StringCast("/p2p/" + master.ID.String() + "/p2p-circuit")
	out := make([]multiaddr.Multiaddr, 0, len(master.Addrs)*2)
	for _, a := range master.Addrs {
		base := a.Encapsulate(suffix)
		out = append(out, base, base.Encapsulate(webrtcprivate.WebRTCAddr))
	}
	return out
}

// reservationLoop keeps a live circuit-v2 reservation on the master relay regardless of AutoNAT
// reachability, flipping `held` so the AddrsFactory advertises the circuit path. go-libp2p's own
// AutoRelay only reserves while Private and tears the reservation — and the browser-dialable
// /p2p-circuit/webrtc addr — down on a flip to Public, which is fatal for a NATed client a browser
// can reach ONLY via the relay. This runs alongside AutoRelay: when Private both refresh the one
// per-peer reservation; when Public this is the sole keeper. Refreshes before the voucher expires;
// retries on error.
func (n *Node) reservationLoop(ctx context.Context, master peer.AddrInfo, held *atomic.Bool) {
	const (
		retryWait  = 30 * time.Second
		refreshPad = 5 * time.Minute // renew this long before the voucher expires
		minWait    = time.Minute
	)
	wait := func(d time.Duration) bool {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}
	for {
		if n.host.Network().Connectedness(master.ID) != network.Connected {
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := n.host.Connect(cctx, master)
			cancel()
			if err != nil {
				held.Store(false)
				if !wait(retryWait) {
					return
				}
				continue
			}
		}
		rctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		rsvp, err := relayclient.Reserve(rctx, n.host, master)
		cancel()
		if err != nil {
			held.Store(false)
			n.logf("master relay reservation failed: %v", err)
			if !wait(retryWait) {
				return
			}
			continue
		}
		held.Store(true)
		renew := time.Until(rsvp.Expiration) - refreshPad
		if renew < minWait {
			renew = minWait
		}
		if !wait(renew) {
			return
		}
	}
}

// isBareWebRTCAddr reports whether a is the lone "/webrtc" listener address (undialable without a
// relay prefix, so we never advertise it).
func isBareWebRTCAddr(a multiaddr.Multiaddr) bool {
	ps := a.Protocols()
	return len(ps) == 1 && ps[0].Code == multiaddr.P_WEBRTC
}

// endsInCircuit reports whether a's last component is "/p2p-circuit" (an AutoRelay reservation
// address, before any transport is encapsulated onto it).
func endsInCircuit(a multiaddr.Multiaddr) bool {
	ps := a.Protocols()
	return len(ps) > 0 && ps[len(ps)-1].Code == multiaddr.P_CIRCUIT
}

// hasCircuit reports whether a contains a "/p2p-circuit" component ANYWHERE — true for both a bare
// reservation "…/p2p-circuit" and a transport-encapsulated "…/p2p-circuit/webrtc". (endsInCircuit,
// which checks only the last component, misses the latter.)
func hasCircuit(a multiaddr.Multiaddr) bool {
	for _, p := range a.Protocols() {
		if p.Code == multiaddr.P_CIRCUIT {
			return true
		}
	}
	return false
}

// selfDialable reports whether another peer could actually reach us: a public direct address, or a
// live relay reservation (a "/p2p-circuit" address AutoRelay has surfaced). A client with neither is
// a ghost — advertising it to the donor rendezvous only makes finders waste a dial. This is the Go
// analog of the web client's getMultiaddrs()-based `dialable` gate (apps/web/src/lib/offload.ts).
func (n *Node) selfDialable() bool {
	if n.control.Reachable() == "public" {
		return true
	}
	for _, a := range n.host.Addrs() {
		if hasCircuit(a) {
			return true
		}
	}
	return false
}

// serverResourceManager builds a resource manager with raised System/Transient/Peer conn +
// stream ceilings (the master absorbs a large inbound swarm). Memory/FD are left at the scaled
// default (DefaultLimit) as the true backstop — the box's RAM bounds the real ceiling. Mirrors
// the per-scope override in deploy/install.sh.
func serverResourceManager() (network.ResourceManager, error) {
	partial := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Conns: 16384, ConnsInbound: 8192, ConnsOutbound: 16384,
			Streams: 65536, StreamsInbound: 32768, StreamsOutbound: 65536,
		},
		Transient: rcmgr.ResourceLimits{
			Conns: 4096, ConnsInbound: 2048, ConnsOutbound: 4096,
			Streams: 16384, StreamsInbound: 8192, StreamsOutbound: 16384,
		},
		PeerDefault: rcmgr.ResourceLimits{
			Conns: 64, ConnsInbound: 32, ConnsOutbound: 64,
			Streams: 4096, StreamsInbound: 2048, StreamsOutbound: 4096,
		},
	}
	limits := partial.Build(rcmgr.DefaultLimits.AutoScale())
	return rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits))
}

func parseAddrInfos(addrs []string) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, 0, len(addrs))
	for _, s := range addrs {
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			return nil, err
		}
		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			return nil, err
		}
		out = append(out, *ai)
	}
	return out, nil
}
