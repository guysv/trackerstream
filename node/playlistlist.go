package tsnode

// Playlist-list (PLAYLISTS.md §10, shipped): a direct request/response stream protocol —
// ask a connected peer "what playlists do you hold?" and get back its DISCLOSURE SET:
// held + published-mine, exactly the set it re-announces. Both are explicit public acts;
// private (unpublished) playlists and the seen tier are never listed, structurally — the
// node is stateless about the library, and only serves what the desktop (Rust) posted via
// `playlist/manifest`. The optional `want` names in a request trigger a targeted
// re-announce of those playlists on the gossip topic, BYPASSING last-seen suppression
// (the asker demonstrably missed the last announce — fresh mesh join, name-only deep
// link) but bounded by a per-name cooldown. Disclosure is pull, 1:1: the responder
// reveals its library to the specific peer who asked, not to the whole network.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multistream"
	"golang.org/x/time/rate"
)

const (
	plListMaxEntries  = 256       // response entry cap (a library bigger than this discloses its head)
	plListManifestMax = 1024      // manifest entry cap (what Rust may post)
	plListMaxFrame    = 256 << 10 // response frame reader bound (256 entries × name+title ≪ this)
	plListReqMax      = 8 << 10   // request frame reader bound
	plListWantMax     = 64        // want names per request
	plListTitleMax    = 300       // title byte clamp (mirrors the Rust schema clamp)
	plListPeerLims    = 4096      // bounded per-peer limiter table (fwd.go doctrine)
	plWantCoolEnts    = 1024      // bounded per-name forced-reannounce cooldown table
)

// Tunables kept as vars so tests can shrink them.
var (
	plListTimeout = 15 * time.Second
	// plWantCooldown bounds forced (suppression-bypassing) re-announces per name: a
	// `want` storm re-gossips a fat doc at most once per window, network-wide per node.
	plWantCooldown = 30 * time.Second
	// Per-peer request rate: listing a library is a human-scale action. Sybils dodge
	// per-peer buckets by minting IDs — the bounded tables and the want cooldown are
	// the real backstop (same doctrine as fwd.go / playlist.go).
	plListPeerRate  = rate.Limit(0.1)
	plListPeerBurst = 4
)

// PlaylistListEntry is one disclosed playlist: both the manifest RPC body and the
// stream response element (and the `playlist/peer-list` RPC reply to Rust).
type PlaylistListEntry struct {
	Name  string `json:"Name"`
	Seq   uint64 `json:"Seq"`
	Title string `json:"Title"`
}

// plListRequest is the single JSON request frame.
type plListRequest struct {
	Want []string `json:"want,omitempty"`
}

// plListResponse is the single JSON response frame.
type plListResponse struct {
	Playlists   []PlaylistListEntry `json:"playlists"`
	Reannounced int                 `json:"reannounced"`
}

// plListState holds the manifest (Rust-posted disclosure set) plus the serving-side
// rate/cooldown tables. Built in New().
type plListState struct {
	mu       sync.Mutex
	manifest []PlaylistListEntry
	byName   map[string]struct{}

	lims *lru.Cache[peer.ID, *rate.Limiter] // per-peer request buckets, bounded
	cool *lru.Cache[string, time.Time]      // per-name forced-reannounce cooldown
}

func newPlListState() *plListState {
	return &plListState{
		byName: map[string]struct{}{},
		lims:   mustLRU[peer.ID, *rate.Limiter](plListPeerLims),
		cool:   mustLRU[string, time.Time](plWantCoolEnts),
	}
}

// setManifest replaces the disclosure set (full replacement — the RPC is idempotent).
// Titles are clamped and the entry count capped here so a misbehaving caller can't
// bloat what we serve.
func (s *plListState) setManifest(entries []PlaylistListEntry) int {
	if len(entries) > plListManifestMax {
		entries = entries[:plListManifestMax]
	}
	byName := make(map[string]struct{}, len(entries))
	kept := make([]PlaylistListEntry, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || len(e.Name) > playlistNameMax {
			continue
		}
		if len(e.Title) > plListTitleMax {
			e.Title = e.Title[:plListTitleMax]
		}
		kept = append(kept, e)
		byName[e.Name] = struct{}{}
	}
	s.mu.Lock()
	s.manifest, s.byName = kept, byName
	s.mu.Unlock()
	return len(kept)
}

// snapshot copies up to max manifest entries (the response body).
func (s *plListState) snapshot(max int) []PlaylistListEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.manifest)
	if n > max {
		n = max
	}
	out := make([]PlaylistListEntry, n)
	copy(out, s.manifest[:n])
	return out
}

func (s *plListState) inManifest(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.byName[name]
	return ok
}

// allow charges one request against p's bucket.
func (s *plListState) allow(p peer.ID) bool {
	l, ok := s.lims.Get(p)
	if !ok {
		l = rate.NewLimiter(plListPeerRate, plListPeerBurst)
		s.lims.Add(p, l)
	}
	return l.Allow()
}

// wantAllowed reports (and records) whether name may be force-reannounced now.
func (s *plListState) wantAllowed(name string) bool {
	if at, ok := s.cool.Get(name); ok && time.Since(at) < plWantCooldown {
		return false
	}
	s.cool.Add(name, time.Now())
	return true
}

// handlePlaylistList serves one list request: read a single JSON frame, force-reannounce
// the allowed `want` names, respond with the manifest in a single JSON frame. Registered
// on CLIENTS only (the seed holds no library and never will).
func (n *Node) handlePlaylistList(s network.Stream) {
	defer s.Close()
	if !n.plList.allow(s.Conn().RemotePeer()) {
		s.Reset()
		return
	}
	_ = s.SetDeadline(time.Now().Add(plListTimeout))

	r := msgio.NewVarintReaderSize(s, plListReqMax)
	reqBytes, err := r.ReadMsg()
	if err != nil {
		s.Reset()
		return
	}
	var req plListRequest
	jerr := json.Unmarshal(reqBytes, &req)
	r.ReleaseMsg(reqBytes)
	if jerr != nil {
		s.Reset()
		return
	}
	if len(req.Want) > plListWantMax {
		req.Want = req.Want[:plListWantMax]
	}

	reann := 0
	for _, name := range req.Want {
		// Only manifest names are servable on request: `want` must not turn the node
		// into a general re-gossip oracle for everything its buffer happens to hold.
		if !n.plList.inManifest(name) {
			continue
		}
		rec, doc, seq, ok := n.playlists.getFull(name)
		if !ok {
			continue // doc evicted from the relay buffer — next announce cycle restores it
		}
		if !n.plList.wantAllowed(name) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), plListTimeout)
		err := n.pubsub.PublishPlaylist(ctx, encodePlaylistMsg(name, rec, doc))
		cancel()
		if err != nil {
			n.logf("playlist-list: want reannounce for %s failed: %v", name, err)
			continue
		}
		n.playlists.ingest(name, rec, doc, seq) // refresh lastSeen (self-suppression)
		reann++
	}

	resp, err := json.Marshal(plListResponse{Playlists: n.plList.snapshot(plListMaxEntries), Reannounced: reann})
	if err != nil {
		s.Reset()
		return
	}
	_ = msgio.NewVarintWriter(s).WriteMsg(resp)
}

// ErrPlaylistListUnsupported marks a peer that doesn't speak the protocol (old build,
// or the seed) — the RPC surfaces it as Supported=false rather than an error.
var ErrPlaylistListUnsupported = errors.New("peer does not support playlist-list")

// PeerPlaylists asks one connected peer for its disclosure set (optionally requesting a
// targeted re-announce of `want` names). One JSON frame each way.
func (n *Node) PeerPlaylists(ctx context.Context, p peer.ID, want []string) ([]PlaylistListEntry, int, error) {
	if len(want) > plListWantMax {
		want = want[:plListWantMax]
	}
	s, err := n.host.NewStream(ctx, p, PlaylistListProtocol)
	if err != nil {
		var ns multistream.ErrNotSupported[protocol.ID]
		if errors.As(err, &ns) {
			return nil, 0, ErrPlaylistListUnsupported
		}
		return nil, 0, err
	}
	defer s.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = s.SetDeadline(dl)
	} else {
		_ = s.SetDeadline(time.Now().Add(plListTimeout))
	}

	req, err := json.Marshal(plListRequest{Want: want})
	if err != nil {
		return nil, 0, err
	}
	if err := msgio.NewVarintWriter(s).WriteMsg(req); err != nil {
		return nil, 0, err
	}
	_ = s.CloseWrite()

	r := msgio.NewVarintReaderSize(s, plListMaxFrame)
	frame, err := r.ReadMsg()
	if err != nil {
		return nil, 0, err
	}
	var resp plListResponse
	jerr := json.Unmarshal(frame, &resp)
	r.ReleaseMsg(frame)
	if jerr != nil {
		return nil, 0, jerr
	}
	if len(resp.Playlists) > plListMaxEntries {
		resp.Playlists = resp.Playlists[:plListMaxEntries]
	}
	for i := range resp.Playlists {
		if len(resp.Playlists[i].Title) > plListTitleMax {
			resp.Playlists[i].Title = resp.Playlists[i].Title[:plListTitleMax]
		}
	}
	return resp.Playlists, resp.Reannounced, nil
}
