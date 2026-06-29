package tsnode

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

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
	"github.com/libp2p/go-libp2p/core/routing"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
)

// Node is the assembled trackerstream node: a go-libp2p host with a custom-prefix
// Kademlia DHT, plus boxo's bitswap + blockstore data plane. Higher layers (gossipsub
// IPNS, unixfs cat, the RPC, the control plane) build on this.
type Node struct {
	cfg      Config
	host     host.Host
	dht      *dht.IpfsDHT
	bswap    *bitswap.Bitswap
	bstore   blockstore.Blockstore
	bserv    blockservice.BlockService
	bwc      *metrics.BandwidthCounter
	ds       ds.Batching
	keystore *Keystore
	ipns     *ipnsStore
	pubsub   *PubSub
	pins     *Pinset
	control  *control
}

// logf is the node's structured-ish log sink (stderr). Kept trivial; the deploy captures
// stderr via journald.
func (n *Node) logf(format string, args ...any) {
	log.Printf("[tsnode] "+format, args...)
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

	bwc := metrics.NewBandwidthCounter()

	var idht *dht.IpfsDHT
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.BandwidthReporter(bwc),
		libp2p.EnableNATService(),    // AutoNAT (reachability)
		libp2p.EnableHolePunching(),  // DCUtR
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			mode := dht.ModeClient
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
	if cfg.RelayServer {
		// The clamped circuit-relay v2 service (coordination floor; bulk limits applied later).
		opts = append(opts, libp2p.EnableRelayService())
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

	bstore := blockstore.NewBlockstore(datastore)
	net := bsnet.NewFromIpfsHost(h)
	// Bitswap serves from the blockstore AND fetches; the DHT is the content router for
	// provider discovery, but wants also broadcast to connected peers (the offload path).
	bswap := bitswap.New(ctx, net, idht, bstore)
	bserv := blockservice.New(bstore, bswap)

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
		cfg:      cfg,
		host:     h,
		dht:      idht,
		bswap:    bswap,
		bstore:   bstore,
		bserv:    bserv,
		bwc:      bwc,
		ds:       datastore,
		keystore: keystore,
		ipns:     newIpnsStore(),
		pubsub:   ps,
		pins:     pins,
	}
	n.control = newControl(n)
	if err := n.control.start(ctx); err != nil {
		return nil, fmt.Errorf("control plane: %w", err)
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
	// Reprovide pinned roots to the custom DHT (Provide.Strategy=roots; 22h in prod).
	go n.reprovideLoop(ctx, 22*time.Hour)

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

// Connect dials a peer (used for bootstrap, warm-set formation, the test harness).
func (n *Node) Connect(ctx context.Context, ai peer.AddrInfo) error {
	return n.host.Connect(ctx, ai)
}

// PutBlock stores a block we hold and notifies bitswap so it serves it.
func (n *Node) PutBlock(ctx context.Context, b blocks.Block) error {
	return n.bserv.AddBlock(ctx, b)
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
	if n.dht != nil {
		go func() {
			cctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := n.dht.Provide(cctx, c, true); err != nil {
				n.logf("provide %s failed: %v", c, err)
			}
		}()
	}
	return nil
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
