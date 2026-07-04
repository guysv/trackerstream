package tsnode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

// RPCServer exposes a kubo-compatible `/api/v0` subset over localhost HTTP — the seam
// both the desktop client (Rust) and the ingest driver (Node `KuboRpc`) talk to, so the
// existing kubo-client code is reused unchanged. Bound to loopback only (same trust model
// as kubo's `127.0.0.1:5001`).
type RPCServer struct {
	node *Node
	mux  *http.ServeMux
}

// NewRPCServer wires the handlers onto a fresh mux.
func NewRPCServer(n *Node) *RPCServer {
	s := &RPCServer{node: n, mux: http.NewServeMux()}
	s.mux.HandleFunc("/api/v0/id", s.handleID)
	s.mux.HandleFunc("/api/v0/version", s.handleVersion)
	s.mux.HandleFunc("/api/v0/block/put", s.handleBlockPut)
	s.mux.HandleFunc("/api/v0/block/put-many", s.handleBlockPutMany)
	s.mux.HandleFunc("/api/v0/block/get", s.handleBlockGet)
	s.mux.HandleFunc("/api/v0/cat", s.handleCat)
	s.mux.HandleFunc("/api/v0/add", s.handleAdd)
	s.mux.HandleFunc("/api/v0/pin/add", s.handlePinAdd)
	s.mux.HandleFunc("/api/v0/pin/rm", s.handlePinRm)
	s.mux.HandleFunc("/api/v0/pin/ls", s.handlePinLs)
	s.mux.HandleFunc("/api/v0/key/gen", s.handleKeyGen)
	s.mux.HandleFunc("/api/v0/key/list", s.handleKeyList)
	s.mux.HandleFunc("/api/v0/name/publish", s.handleNamePublish)
	s.mux.HandleFunc("/api/v0/routing/get", s.handleRoutingGet)
	s.mux.HandleFunc("/api/v0/swarm/connect", s.handleSwarmConnect)
	s.mux.HandleFunc("/api/v0/swarm/disconnect", s.handleSwarmDisconnect)
	s.mux.HandleFunc("/api/v0/swarm/peers", s.handleSwarmPeers)
	// trackerstream extensions (no kubo equivalent) — the peers pane + reachability.
	s.mux.HandleFunc("/api/v0/bandwidth/by-peer", s.handleBandwidthByPeer)
	s.mux.HandleFunc("/api/v0/node/status", s.handleNodeStatus)
	s.mux.HandleFunc("/api/v0/warm", s.handleWarm)
	// Content-typed providing (clients advertise what they hold): a track manifest root at
	// whole-track granularity, or an individual catalog page at piece granularity.
	s.mux.HandleFunc("/api/v0/provide/track-root", s.handleProvideTrackRoot)
	s.mux.HandleFunc("/api/v0/provide/catalog-piece", s.handleProvideCatalogPiece)
	// Connect to non-seed providers of a CID so bitswap fetches from peers, not just the seed.
	s.mux.HandleFunc("/api/v0/dial-providers", s.handleDialProviders)
	// Playlists (PLAYLISTS.md): docs travel inline on the gossip topic; these are the
	// desktop's publish / drain / re-announce seams.
	s.mux.HandleFunc("/api/v0/playlist/publish", s.handlePlaylistPublish)
	s.mux.HandleFunc("/api/v0/playlist/records", s.handlePlaylistRecords)
	s.mux.HandleFunc("/api/v0/playlist/announce", s.handlePlaylistAnnounce)
	// Playlist-list (§10, shipped): Rust posts the disclosure set; peer-list dials a peer.
	s.mux.HandleFunc("/api/v0/playlist/manifest", s.handlePlaylistManifest)
	s.mux.HandleFunc("/api/v0/playlist/peer-list", s.handlePlaylistPeerList)
	// Hold beacons (§10, shipped): windowed backer counts from the beacon topic.
	s.mux.HandleFunc("/api/v0/playlist/backers", s.handlePlaylistBackers)
	return s
}

// Handler exposes the mux (for tests / embedding).
func (s *RPCServer) Handler() http.Handler { return s.mux }

// ListenAndServe blocks serving the RPC on addr (e.g. "127.0.0.1:5099").
func (s *RPCServer) ListenAndServe(addr string) error {
	return (&http.Server{Addr: addr, Handler: s.mux}).ListenAndServe()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func rpcErr(w http.ResponseWriter, code int, err error) {
	w.WriteHeader(code)
	writeJSON(w, map[string]any{"Message": err.Error(), "Code": 0, "Type": "error"})
}

func (s *RPCServer) handleID(w http.ResponseWriter, r *http.Request) {
	addrs := make([]string, 0)
	for _, a := range s.node.Addrs() {
		addrs = append(addrs, a.String())
	}
	writeJSON(w, map[string]any{
		"ID":        s.node.ID().String(),
		"Addresses": addrs,
	})
}

func (s *RPCServer) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"Version": "tsnode/0.1.0"})
}

// codecFromString maps the kubo `cid-codec` param to a multicodec value. Matches the
// scheme repack/the Rust client use (CIDv1, sha2-256, raw 0x55 / dag-cbor 0x71 / dag-pb 0x70).
func codecFromString(s string) (uint64, error) {
	switch s {
	case "raw", "":
		return cid.Raw, nil
	case "dag-cbor":
		return cid.DagCBOR, nil
	case "dag-pb":
		return cid.DagProtobuf, nil
	case "dag-json":
		return cid.DagJSON, nil
	}
	if n, err := strconv.ParseUint(s, 0, 64); err == nil {
		return n, nil
	}
	return 0, fmt.Errorf("unknown cid-codec %q", s)
}

// handleBlockPut mirrors kubo `block/put`: store a block under a CIDv1 we compute from the
// posted bytes + `cid-codec` (sha2-256). Returns {Key, Size}. This is the ingest primitive
// (repack `loadDagToKubo` block-puts precomputed-CID blocks).
func (s *RPCServer) handleBlockPut(w http.ResponseWriter, r *http.Request) {
	codec, err := codecFromString(r.URL.Query().Get("cid-codec"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	data, err := readUploadedFile(r)
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	hash, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	c := cid.NewCidV1(codec, hash)
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.node.PutBlock(r.Context(), blk); err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Key": c.String(), "Size": len(data)})
}

// handleBlockPutMany is the batched bulk-ingest primitive: store a whole DAG's worth of
// blocks in ONE leveldb batch (one fsync) instead of one block/put (one fsync) each. The
// body is a raw framed stream of [uint32-BE codec][uint32-BE len][payload] entries; we
// recompute each CID (sha2-256) exactly as handleBlockPut does, so CIDs stay self-verifying.
// Returns {Keys:[...], Count}. Repack's loadDagToKubo posts one module's blocks per call.
func (s *RPCServer) handleBlockPutMany(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	blks := make([]blocks.Block, 0, 64)
	keys := make([]string, 0, 64)
	for off := 0; off < len(body); {
		if off+8 > len(body) {
			rpcErr(w, http.StatusBadRequest, fmt.Errorf("block/put-many: truncated frame header at %d", off))
			return
		}
		codec := uint64(binary.BigEndian.Uint32(body[off:]))
		n := int(binary.BigEndian.Uint32(body[off+4:]))
		off += 8
		if off+n > len(body) {
			rpcErr(w, http.StatusBadRequest, fmt.Errorf("block/put-many: frame len %d overruns body", n))
			return
		}
		data := body[off : off+n]
		off += n
		hash, err := mh.Sum(data, mh.SHA2_256, -1)
		if err != nil {
			rpcErr(w, http.StatusInternalServerError, err)
			return
		}
		c := cid.NewCidV1(codec, hash)
		blk, err := blocks.NewBlockWithCid(data, c)
		if err != nil {
			rpcErr(w, http.StatusInternalServerError, err)
			return
		}
		blks = append(blks, blk)
		keys = append(keys, c.String())
	}
	if err := s.node.PutBlocks(r.Context(), blks); err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Keys": keys, "Count": len(keys)})
}

func (s *RPCServer) handleBlockGet(w http.ResponseWriter, r *http.Request) {
	c, err := cid.Decode(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	// n.GetBlock rides the forwarding-assisted exchange (R5) — bitswap with a block-forwarding
	// fallback when a peer holds it but isn't directly reachable. Same for the catalog cat path.
	blk, err := s.node.GetBlock(r.Context(), c)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(blk.RawData())
}

// handleCat mirrors kubo `cat?arg&offset&length` — the catalog VFS ranged read.
func (s *RPCServer) handleCat(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c, err := cid.Decode(q.Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	length := int64(-1)
	if l := q.Get("length"); l != "" {
		length, _ = strconv.ParseInt(l, 10, 64)
	}
	// The cat RPC is the catalog read path: advertise the page blocks we fetch (peer page-sharing).
	data, err := s.node.CatCatalog(r.Context(), c, offset, length)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
}

func (s *RPCServer) handleSwarmConnect(w http.ResponseWriter, r *http.Request) {
	ma, err := multiaddr.NewMultiaddr(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	ai, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.node.Connect(r.Context(), *ai); err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Strings": []string{"connect " + ai.ID.String() + " success"}})
}

func (s *RPCServer) handleSwarmPeers(w http.ResponseWriter, r *http.Request) {
	conns := s.node.Host().Network().Conns()
	peers := make([]map[string]any, 0, len(conns))
	for _, c := range conns {
		peers = append(peers, map[string]any{
			"Peer": c.RemotePeer().String(),
			"Addr": c.RemoteMultiaddr().String(),
		})
	}
	writeJSON(w, map[string]any{"Peers": peers})
}

// handleAdd mirrors kubo `add`: build a UnixFS DAG from the uploaded file and return the
// root as a newline-delimited `{Hash}` line (ingest reads the LAST line). The catalog publish
// uses chunker=size-16384, raw-leaves=false, cid-version=1, pin=true.
func (s *RPCServer) handleAdd(w http.ResponseWriter, r *http.Request) {
	data, err := readNamedFile(r, "file")
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	q := r.URL.Query()
	opts := AddOptions{
		ChunkSize: chunkSizeFromString(q.Get("chunker")),
		RawLeaves: q.Get("raw-leaves") == "true",
		Pin:       q.Get("pin") != "false",
	}
	root, err := s.node.AddUnixFS(r.Context(), bytes.NewReader(data), opts)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	// Newline-delimited JSON (kubo streams progress; we emit a single final object).
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"Name": root.String(), "Hash": root.String(), "Size": len(data)})
}

func (s *RPCServer) handlePinAdd(w http.ResponseWriter, r *http.Request) {
	c, err := cid.Decode(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.node.Pin(r.Context(), c); err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Pins": []string{c.String()}})
}

func (s *RPCServer) handlePinRm(w http.ResponseWriter, r *http.Request) {
	c, err := cid.Decode(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.node.Unpin(r.Context(), c); err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Pins": []string{c.String()}})
}

// handlePinLs lists recursively-pinned roots (the `verify-pinset` source).
func (s *RPCServer) handlePinLs(w http.ResponseWriter, r *http.Request) {
	keys := map[string]any{}
	for _, c := range s.node.Pins().Roots() {
		keys[c.String()] = map[string]any{"Type": "recursive"}
	}
	writeJSON(w, map[string]any{"Keys": keys})
}

// handleKeyGen mirrors kubo `key/gen?arg=<name>` — idempotent (returns the existing key's Id
// if present, matching kubo's "already exists" fallback).
func (s *RPCServer) handleKeyGen(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("arg")
	if name == "" {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("key name required"))
		return
	}
	k, err := s.node.Keystore().GetOrCreate(name)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	id, err := peer.IDFromPublicKey(k.GetPublic())
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Name": name, "Id": id.String()})
}

func (s *RPCServer) handleKeyList(w http.ResponseWriter, r *http.Request) {
	keys, err := s.node.Keystore().List()
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{"Name": k.Name, "Id": k.ID})
	}
	writeJSON(w, map[string]any{"Keys": out})
}

// handleNamePublish mirrors kubo `name/publish?arg=/ipfs/<cid>&key=<name>&lifetime=<dur>` —
// signs the IPNS record, stores it, puts to DHT, and pushes on the catalog topic.
func (s *RPCServer) handleNamePublish(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c, err := cid.Decode(stripIPFSPrefix(q.Get("arg")))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("name/publish arg: %w", err))
		return
	}
	keyName := q.Get("key")
	if keyName == "" {
		keyName = "self"
	}
	lifetime := parseDuration(q.Get("lifetime"), 48*time.Hour)
	seq := uint64(0)
	if v := q.Get("seq"); v != "" {
		seq, _ = strconv.ParseUint(v, 10, 64)
	}
	pid, _, err := s.node.PublishIPNS(r.Context(), keyName, c, lifetime, seq)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Name": pid.String(), "Value": "/ipfs/" + c.String()})
}

// handlePlaylistPublish signs + distributes a playlist doc:
// `playlist/publish?key=<name>&seq=<n>&lifetime=<dur>` with the raw doc bytes as the
// body. The node computes the doc's raw-sha256 CID (integrity anchor), signs the IPNS
// record, DHT-puts the record, and gossips {name, record, doc} inline. Seq is
// caller-owned (Rust does max(local, network)+1 — seq recovery).
func (s *RPCServer) handlePlaylistPublish(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	keyName := q.Get("key")
	if keyName == "" {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("playlist/publish: key required"))
		return
	}
	seq, err := strconv.ParseUint(q.Get("seq"), 10, 64)
	if err != nil {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("playlist/publish: seq required: %w", err))
		return
	}
	lifetime := parseDuration(q.Get("lifetime"), 168*time.Hour)
	doc, err := io.ReadAll(io.LimitReader(r.Body, playlistDocMax+1))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	if len(doc) > playlistDocMax {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("playlist doc exceeds %d bytes", playlistDocMax))
		return
	}
	pid, rec, err := s.node.PublishPlaylist(r.Context(), keyName, doc, lifetime, seq)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	// Record rides back base64 (JSON []byte) — Rust keeps it for the re-announce cycle.
	writeJSON(w, map[string]any{"Name": pid.String(), "Seq": seq, "Record": rec})
}

// handlePlaylistRecords drains the bounded relay buffer: `playlist/records?since=<v>`
// returns entries written after store-version v plus the current version (the next
// poll's cursor). Record/Doc are base64 (JSON []byte); Rust re-verifies both.
func (s *RPCServer) handlePlaylistRecords(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	ver, recs := s.node.playlists.since(since)
	if recs == nil {
		recs = []PlaylistRecord{}
	}
	writeJSON(w, map[string]any{"Version": ver, "Records": recs})
}

// handlePlaylistAnnounce re-gossips held {record, doc} pairs verbatim with last-seen
// suppression — the desktop's jittered re-announce cycle feeds playlists.db through
// here, so late joiners learn the working set without the node holding durable state.
func (s *RPCServer) handlePlaylistAnnounce(w http.ResponseWriter, r *http.Request) {
	var entries []PlaylistRecord
	if err := json.NewDecoder(io.LimitReader(r.Body, 96<<20)).Decode(&entries); err != nil {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("playlist/announce: %w", err))
		return
	}
	announced, suppressed, rejected := s.node.AnnouncePlaylists(r.Context(), entries)
	writeJSON(w, map[string]any{"Announced": announced, "Suppressed": suppressed, "Rejected": rejected})
}

// handlePlaylistManifest replaces the node's disclosure-set manifest — the exact set the
// playlist-list protocol answers with (and the hold-beacon source). Full replacement,
// idempotent; the desktop posts it on startup and whenever its library changes.
func (s *RPCServer) handlePlaylistManifest(w http.ResponseWriter, r *http.Request) {
	var entries []PlaylistListEntry
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&entries); err != nil {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("playlist/manifest: %w", err))
		return
	}
	count := s.node.plList.setManifest(entries)
	// A changed disclosure set beacons ahead of the hourly cycle (floor-limited in
	// KickBeacon; the desktop only posts manifests that actually changed).
	s.node.KickBeacon(r.Context())
	writeJSON(w, map[string]any{"Count": count})
}

// handlePlaylistBackers maps names → windowed distinct-origin holder counts from the
// beacon topic: `playlist/backers` body {"Names":[...]}. The hash scheme stays
// node-side (the node hashes; Rust only ever sees names). 0 = no beacon heard.
func (s *RPCServer) handlePlaylistBackers(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names []string `json:"Names"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 256<<10)).Decode(&req); err != nil {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("playlist/backers: %w", err))
		return
	}
	if len(req.Names) > 512 {
		req.Names = req.Names[:512]
	}
	writeJSON(w, map[string]any{"Counts": s.node.beacons.backers(req.Names)})
}

// handlePlaylistPeerList asks one connected peer for its disclosure set:
// `playlist/peer-list?peer=<id>&want=<a,b,c>`. A peer that doesn't speak the protocol
// (old build, the seed) is a normal answer — Supported=false — not an error.
func (s *RPCServer) handlePlaylistPeerList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p, err := peer.Decode(q.Get("peer"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, fmt.Errorf("playlist/peer-list: peer: %w", err))
		return
	}
	var want []string
	if v := q.Get("want"); v != "" {
		want = strings.Split(v, ",")
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	entries, reann, err := s.node.PeerPlaylists(ctx, p, want)
	if errors.Is(err, ErrPlaylistListUnsupported) {
		writeJSON(w, map[string]any{"Playlists": []PlaylistListEntry{}, "Reannounced": 0, "Supported": false})
		return
	}
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	if entries == nil {
		entries = []PlaylistListEntry{}
	}
	writeJSON(w, map[string]any{"Playlists": entries, "Reannounced": reann, "Supported": true})
}

// handleRoutingGet mirrors kubo `routing/get?arg=/ipns/<name>` — returns the signed record
// base64 in `Extra` on a Type-5 (Value) line. Ingest reads this and POSTs it to the /ipns store.
func (s *RPCServer) handleRoutingGet(w http.ResponseWriter, r *http.Request) {
	name := stripIPNSPrefix(r.URL.Query().Get("arg"))
	rec, err := s.node.ResolveIPNS(r.Context(), name)
	if err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// Type 5 == Value (libp2p routing event kind), matching what KuboRpc.routingGet filters on.
	_ = json.NewEncoder(w).Encode(map[string]any{"Type": 5, "Extra": recordBase64(rec)})
}

func (s *RPCServer) handleSwarmDisconnect(w http.ResponseWriter, r *http.Request) {
	ma, err := multiaddr.NewMultiaddr(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	ai, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.node.Host().Network().ClosePeer(ai.ID); err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Strings": []string{"disconnect " + ai.ID.String() + " success"}})
}

// handleBandwidthByPeer is the per-peer Bitswap-attribution endpoint (rust-ipfs patch 0001
// dissolved into BandwidthCounter) — the peers-pane down/up totals.
func (s *RPCServer) handleBandwidthByPeer(w http.ResponseWriter, r *http.Request) {
	byPeer := s.node.Bandwidth().GetBandwidthByPeer()
	out := make(map[string]map[string]any, len(byPeer))
	for p, st := range byPeer {
		out[p.String()] = map[string]any{"TotalIn": st.TotalIn, "TotalOut": st.TotalOut, "RateIn": st.RateIn, "RateOut": st.RateOut}
	}
	writeJSON(w, map[string]any{"ByPeer": out})
}

// handleNodeStatus is the consolidated client status: reachability verdict + relay-hop
// breakdown + peer/topic counts (node/status in the plan).
func (s *RPCServer) handleNodeStatus(w http.ResponseWriter, r *http.Request) {
	totals := s.node.Bandwidth().GetBandwidthTotals()
	writeJSON(w, map[string]any{
		"Reachability": s.node.Control().Reachable(),
		"RelayStats":   s.node.Control().Stats(),
		"Peers":        len(s.node.Host().Network().Peers()),
		"CatalogPeers": len(s.node.PubSub().CatalogPeers()),
		"TotalIn":      totals.TotalIn,
		"TotalOut":     totals.TotalOut,
		"Pins":         len(s.node.Pins().Roots()),
	})
}

// handleWarm marks a peer keepalive-worthy (warm holder) — the client's warm_root command.
func (s *RPCServer) handleWarm(w http.ResponseWriter, r *http.Request) {
	id, err := peer.Decode(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	s.node.Control().Warm(id)
	writeJSON(w, map[string]any{"Strings": []string{"warm " + id.String()}})
}

// handleProvideTrackRoot advertises a track manifest root (whole-track granularity).
func (s *RPCServer) handleProvideTrackRoot(w http.ResponseWriter, r *http.Request) {
	c, err := cid.Decode(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.node.ProvideTrackRoot(r.Context(), c); err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Provided": c.String(), "Kind": "track-root"})
}

// handleProvideCatalogPiece advertises an individual catalog block (page granularity).
func (s *RPCServer) handleProvideCatalogPiece(w http.ResponseWriter, r *http.Request) {
	c, err := cid.Decode(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.node.ProvideCatalogPiece(r.Context(), c); err != nil {
		rpcErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]any{"Provided": c.String(), "Kind": "catalog-piece"})
}

// handleDialProviders finds + connects the non-seed providers of a CID (peer-source priming).
func (s *RPCServer) handleDialProviders(w http.ResponseWriter, r *http.Request) {
	c, err := cid.Decode(r.URL.Query().Get("arg"))
	if err != nil {
		rpcErr(w, http.StatusBadRequest, err)
		return
	}
	n := s.node.DialProviders(r.Context(), c)
	writeJSON(w, map[string]any{"Connected": n})
}

// readUploadedFile reads the block bytes from a kubo-style multipart upload (the "data"
// part), falling back to the raw request body.
func readUploadedFile(r *http.Request) ([]byte, error) { return readNamedFile(r, "data") }

// readNamedFile reads the uploaded bytes for a kubo-style RPC. kubo clients send a multipart
// body with the payload in the named field ("data" for block/put, "file" for add); we also
// accept a raw body as a fallback. CRITICAL: dispatch on Content-Type BEFORE touching the
// multipart parser — r.FormFile/ParseMultipartForm drains r.Body even when it errors on a
// non-multipart request, which would leave the raw-body fallback reading nothing (an empty
// block whose CID is the hash of "", silently un-servable).
func readNamedFile(r *http.Request, field string) ([]byte, error) {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
		return io.ReadAll(r.Body) // raw body — don't let the form parser drain it first
	}
	if file, _, err := r.FormFile(field); err == nil {
		defer file.Close()
		return io.ReadAll(file)
	}
	// Field name didn't match (some clients vary it) — take the first part.
	if mr, err := r.MultipartReader(); err == nil {
		part, err := mr.NextPart()
		if err != nil {
			return nil, err
		}
		defer part.Close()
		return io.ReadAll(part)
	}
	return io.ReadAll(r.Body)
}

// chunkSizeFromString parses kubo's `chunker=size-<n>` form (default 16384). Only size-based
// chunking is supported (the catalog uses size-16384, page-aligned for the SQLite VFS).
func chunkSizeFromString(s string) int64 {
	if strings.HasPrefix(s, "size-") {
		if n, err := strconv.ParseInt(strings.TrimPrefix(s, "size-"), 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 16384
}

func stripIPFSPrefix(s string) string { return strings.TrimPrefix(s, "/ipfs/") }
func stripIPNSPrefix(s string) string { return strings.TrimPrefix(s, "/ipns/") }

// parseDuration accepts kubo-style durations ("48h"); empty/invalid → def.
func parseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return def
}
