package tsnode

// Playlists over IPNS (PLAYLISTS.md): playlist documents travel INLINE in the gossip
// message next to their signed IPNS record — no bitswap, no blockstore, no pinning.
// The node is a validating relay with bounded caches: the seed stores records only
// (never docs); a client keeps a byte-bounded {record, doc} ingest buffer that the
// desktop (Rust) drains via `playlist/records` and persists durably in its own DB.
// Rust re-verifies everything it drains; the node-side checks are the mesh's edge
// filter, not the trust anchor.

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

const (
	playlistDocMax  = 1 << 20 // 1 MiB — a bigger doc is adversarial by definition (validator drops it)
	playlistNameMax = 128     // base58 PeerId length bound (envelope sanity)
)

// Tunables kept as vars so tests can shrink them.
var (
	playlistStoreEnts  = 8192     // record-entry cap, all roles
	playlistStoreBytes = 16 << 20 // client ingest-buffer doc budget; the durable store is Rust's playlists.db
	// playlistAnnounceWindow is the re-announce suppression horizon: a playlist seen
	// announced on the topic within this window is NOT re-announced by us — whoever's
	// jitter fires first wins, so steady state converges to ~one announce per playlist
	// per window network-wide instead of holders × playlists.
	playlistAnnounceWindow = 10 * time.Minute
)

// ---- gossip envelope ----

// encodePlaylistMsg frames the playlist gossip envelope: uvarint-length-prefixed
// name ++ record ++ doc. Binary because the doc can be ~1 MiB and the envelope is
// Go↔Go only — JSON's base64 would add +33% to every mesh hop.
func encodePlaylistMsg(name string, rec, doc []byte) []byte {
	buf := make([]byte, 0, len(name)+len(rec)+len(doc)+3*binary.MaxVarintLen64)
	for _, part := range [][]byte{[]byte(name), rec, doc} {
		buf = binary.AppendUvarint(buf, uint64(len(part)))
		buf = append(buf, part...)
	}
	return buf
}

func readFrame(b []byte, max int) (part, rest []byte, err error) {
	l, n := binary.Uvarint(b)
	if n <= 0 || l > uint64(max) || uint64(len(b)-n) < l {
		return nil, nil, fmt.Errorf("malformed playlist envelope frame")
	}
	return b[n : n+int(l)], b[n+int(l):], nil
}

func decodePlaylistMsg(b []byte) (name string, rec, doc []byte, err error) {
	nb, b, err := readFrame(b, playlistNameMax)
	if err != nil {
		return "", nil, nil, err
	}
	rec, b, err = readFrame(b, ipns.MaxRecordSize)
	if err != nil {
		return "", nil, nil, err
	}
	doc, b, err = readFrame(b, playlistDocMax)
	if err != nil {
		return "", nil, nil, err
	}
	if len(b) != 0 {
		return "", nil, nil, fmt.Errorf("trailing bytes in playlist envelope")
	}
	return string(nb), rec, doc, nil
}

// validatePlaylistMsg fully verifies a playlist message: the name decodes, the record
// validates under the name's key (signature AND EOL — boxo's Validate checks both), the
// doc is within cap, and the doc hashes to the CID in the record's value. The message is
// therefore completely self-certifying: a forged, tampered, or expired one never rides
// the mesh. Returns the record's sequence number.
func validatePlaylistMsg(name string, rec, doc []byte) (uint64, error) {
	if len(doc) == 0 || len(doc) > playlistDocMax {
		return 0, fmt.Errorf("playlist doc size %d out of bounds", len(doc))
	}
	pid, err := peer.Decode(name)
	if err != nil {
		return 0, fmt.Errorf("bad playlist name %q: %w", name, err)
	}
	r, err := ipns.UnmarshalRecord(rec)
	if err != nil {
		return 0, fmt.Errorf("unmarshal record: %w", err)
	}
	if err := ipns.ValidateWithName(r, ipns.NameFromPeer(pid)); err != nil {
		return 0, fmt.Errorf("invalid record for %s: %w", name, err)
	}
	v, err := r.Value()
	if err != nil {
		return 0, fmt.Errorf("record value: %w", err)
	}
	cs := strings.TrimPrefix(v.String(), "/ipfs/")
	if cs == v.String() {
		return 0, fmt.Errorf("record value %q is not /ipfs/", v)
	}
	c, err := cid.Decode(cs)
	if err != nil {
		return 0, fmt.Errorf("record value cid: %w", err)
	}
	chk, err := c.Prefix().Sum(doc)
	if err != nil || !chk.Equals(c) {
		return 0, fmt.Errorf("doc does not hash to record cid %s", c)
	}
	seq, err := r.Sequence()
	if err != nil {
		return 0, fmt.Errorf("record sequence: %w", err)
	}
	return seq, nil
}

// ---- bounded store (ingest buffer + suppression state) ----

// PlaylistRecord is the `playlist/records` / `playlist/announce` wire shape. []byte
// fields marshal as base64 in JSON, matching what the Rust client expects to verify.
type PlaylistRecord struct {
	Name   string `json:"Name"`
	Seq    uint64 `json:"Seq"`
	Record []byte `json:"Record"`
	Doc    []byte `json:"Doc,omitempty"`
}

type plEntry struct {
	rec      []byte
	doc      []byte // nil on the seed (records only) and past the byte budget
	seq      uint64
	ver      uint64    // store version when (re)written — the records?since= cursor
	lastSeen time.Time // last time this name was seen announced on the topic
}

// playlistStore is the node's bounded playlist cache: newest-seq-wins records, docs only
// when storeDocs (clients), entry- and byte-capped with least-recently-seen eviction.
// It is a relay buffer + suppression ledger, NOT a durable store — Rust's playlists.db is.
type playlistStore struct {
	mu        sync.Mutex
	entries   map[string]*plEntry
	storeDocs bool
	docBytes  int
	version   uint64
}

func newPlaylistStore(storeDocs bool) *playlistStore {
	return &playlistStore{entries: map[string]*plEntry{}, storeDocs: storeDocs}
}

// ingest stores a VALIDATED message (newest-seq wins) and refreshes the name's
// last-seen (the suppression input) even when the record isn't newer — a duplicate
// announce from elsewhere means we need not re-announce it ourselves.
func (s *playlistStore) ingest(name string, rec, doc []byte, seq uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	e, ok := s.entries[name]
	if ok {
		e.lastSeen = now
		if e.seq >= seq {
			return false
		}
	} else {
		e = &plEntry{lastSeen: now}
		s.entries[name] = e
	}
	s.docBytes -= len(e.doc)
	e.rec, e.seq, e.doc = rec, seq, nil
	if s.storeDocs {
		e.doc = doc
		s.docBytes += len(doc)
	}
	s.version++
	e.ver = s.version
	s.evictLocked()
	return true
}

// evictLocked enforces the entry cap and doc-byte budget by dropping least-recently-seen
// entries — gossip-refreshed playlists survive, dead or spam ones age out.
func (s *playlistStore) evictLocked() {
	for len(s.entries) > 0 && (len(s.entries) > playlistStoreEnts || s.docBytes > playlistStoreBytes) {
		var oldName string
		var oldAt time.Time
		for name, e := range s.entries {
			if oldName == "" || e.lastSeen.Before(oldAt) {
				oldName, oldAt = name, e.lastSeen
			}
		}
		s.docBytes -= len(s.entries[oldName].doc)
		delete(s.entries, oldName)
	}
}

func (s *playlistStore) getRecord(name string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[name]; ok {
		return e.rec, true
	}
	return nil, false
}

// since snapshots entries written after version v (the poll cursor) plus the current
// store version. Entries evicted since a caller's last poll are simply absent — the
// durable ledger is the caller's.
func (s *playlistStore) since(v uint64) (uint64, []PlaylistRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []PlaylistRecord
	for name, e := range s.entries {
		if e.ver > v {
			out = append(out, PlaylistRecord{Name: name, Seq: e.seq, Record: e.rec, Doc: e.doc})
		}
	}
	return s.version, out
}

// shouldAnnounce reports whether name has gone a full suppression window without being
// seen on the topic (unknown names always announce).
func (s *playlistStore) shouldAnnounce(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[name]
	if !ok {
		return true
	}
	return time.Since(e.lastSeen) >= playlistAnnounceWindow
}

// ---- node wiring ----

// playlistValidator is the gossipsub topic validator: full self-certification at the
// mesh edge. Reject = dropped at the FIRST hop and never forwarded (and the sender is
// penalized by gossipsub's peer scoring) — unlike the catalog path, where validation
// happens after the mesh has already relayed the message.
func (n *Node) playlistValidator(_ context.Context, _ peer.ID, m *pubsub.Message) pubsub.ValidationResult {
	name, rec, doc, err := decodePlaylistMsg(m.Data)
	if err != nil {
		return pubsub.ValidationReject
	}
	if _, err := validatePlaylistMsg(name, rec, doc); err != nil {
		n.logf("playlist gossip rejected: %v", err)
		return pubsub.ValidationReject
	}
	return pubsub.ValidationAccept
}

// playlistSink ingests a validator-passed message into the bounded store (the validator
// already proved record+doc integrity; here we only need the seq for newest-wins).
func (n *Node) playlistSink(name string, rec, doc []byte) {
	r, err := ipns.UnmarshalRecord(rec)
	if err != nil {
		return
	}
	seq, err := r.Sequence()
	if err != nil {
		return
	}
	n.playlists.ingest(name, rec, doc, seq)
}

// PublishPlaylist signs an IPNS record for keyName whose value is the raw-sha256 CID of
// doc (an integrity anchor, not a fetchable address), stores it, and gossips the
// {name, record, doc} envelope. Playlists are pubsub+seq ONLY at v1 — deliberately no
// DHT writes (unlike PublishIPNS): distribution, discovery, and seq recovery all ride
// the gossip topic and its re-announce cycle. Returns the publisher PeerId and the
// marshaled record — the caller (Rust) keeps the record for its re-announce cycle.
func (n *Node) PublishPlaylist(ctx context.Context, keyName string, doc []byte, lifetime time.Duration, seq uint64) (peer.ID, []byte, error) {
	if len(doc) == 0 || len(doc) > playlistDocMax {
		return "", nil, fmt.Errorf("playlist doc size %d out of bounds (max %d)", len(doc), playlistDocMax)
	}
	key, err := n.keystore.GetOrCreate(keyName)
	if err != nil {
		return "", nil, err
	}
	pid, err := peer.IDFromPublicKey(key.GetPublic())
	if err != nil {
		return "", nil, err
	}
	h, err := mh.Sum(doc, mh.SHA2_256, -1)
	if err != nil {
		return "", nil, err
	}
	c := cid.NewCidV1(cid.Raw, h)
	rec, err := signIPNS(key, c, lifetime, seq)
	if err != nil {
		return "", nil, err
	}
	marshaled, err := ipns.MarshalRecord(rec)
	if err != nil {
		return "", nil, err
	}

	name := pid.String()
	n.playlists.ingest(name, marshaled, doc, seq)

	if n.pubsub != nil {
		if err := n.pubsub.PublishPlaylist(ctx, encodePlaylistMsg(name, marshaled, doc)); err != nil {
			n.logf("playlist: gossip push for %s failed (non-fatal): %v", name, err)
		}
	}
	return pid, marshaled, nil
}

// AnnouncePlaylists re-gossips held {record, doc} pairs verbatim (no re-signing) with
// last-seen suppression — the Rust side calls this on its jittered cycle with the
// contents of playlists.db, so durable data lives exactly once and the node stays a
// relay. Returns (announced, suppressed, rejected).
func (n *Node) AnnouncePlaylists(ctx context.Context, entries []PlaylistRecord) (announced, suppressed, rejected int) {
	for _, e := range entries {
		seq, err := validatePlaylistMsg(e.Name, e.Record, e.Doc)
		if err != nil {
			rejected++
			continue
		}
		if !n.playlists.shouldAnnounce(e.Name) {
			suppressed++
			continue
		}
		if n.pubsub != nil {
			if err := n.pubsub.PublishPlaylist(ctx, encodePlaylistMsg(e.Name, e.Record, e.Doc)); err != nil {
				n.logf("playlist: announce for %s failed: %v", e.Name, err)
			}
		}
		n.playlists.ingest(e.Name, e.Record, e.Doc, seq) // refreshes lastSeen (self-suppression)
		announced++
	}
	return announced, suppressed, rejected
}
