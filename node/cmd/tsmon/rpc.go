package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Client talks to a tsnode's loopback RPC (the same `/api/v0` surface the desktop client and
// deploy/metrics-export.sh use). Every call is a POST with no body, kubo-style.
type Client struct {
	base string
	http *http.Client
}

func NewClient(base string) *Client {
	base = strings.TrimRight(base, "/")
	return &Client{base: base, http: &http.Client{Timeout: 8 * time.Second}}
}

func (c *Client) post(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, "POST", c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s: %s: %s", path, resp.Status, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// --- response shapes (mirror node/rpc.go handlers) ---

type RelayStats struct {
	Direct      int `json:"direct"`
	PeerRelay   int `json:"peer_relay"`
	MasterRelay int `json:"master_relay"`
}

type SeedStatus struct {
	ID           string         `json:"ID"`
	Role         string         `json:"Role"`
	AgentVersion string         `json:"AgentVersion"`
	Version      string         `json:"Version"`
	UptimeSec    int64          `json:"UptimeSec"`
	Reachability string         `json:"Reachability"`
	RelayStats   RelayStats     `json:"RelayStats"`
	Peers        int            `json:"Peers"`
	CatalogPeers int            `json:"CatalogPeers"`
	TotalIn      int64          `json:"TotalIn"`
	TotalOut     int64          `json:"TotalOut"`
	Pins         int            `json:"Pins"`
	PinsByKind   map[string]int `json:"PinsByKind"`
	Provide      struct {
		OK    int64 `json:"OK"`
		Fail  int64 `json:"Fail"`
		Queue int   `json:"Queue"`
		Cap   int   `json:"Cap"`
	} `json:"Provide"`
	Reprovide struct {
		LastUnix    int64 `json:"LastUnix"`
		IntervalSec int64 `json:"IntervalSec"`
	} `json:"Reprovide"`
	Seeds []string `json:"Seeds"`

	// legacy is set when we had to fall back to /api/v0/node/status on an un-upgraded node
	// (no seed-health fields). The seed-only panels render "—" in that case.
	legacy bool
}

type swarmPeer struct {
	Peer string `json:"Peer"`
	Addr string `json:"Addr"`
}

type bwEntry struct {
	TotalIn  int64   `json:"TotalIn"`
	TotalOut int64   `json:"TotalOut"`
	RateIn   float64 `json:"RateIn"`
	RateOut  float64 `json:"RateOut"`
}

type ledgerEntry struct {
	Sent        uint64  `json:"Sent"`
	Recv        uint64  `json:"Recv"`
	Value       float64 `json:"Value"`
	Exchanged   uint64  `json:"Exchanged"`
	WantlistLen int     `json:"WantlistLen"`
}

type BitswapStat struct {
	BlocksSent       uint64 `json:"BlocksSent"`
	DataSent         uint64 `json:"DataSent"`
	BlocksReceived   uint64 `json:"BlocksReceived"`
	DataReceived     uint64 `json:"DataReceived"`
	DupBlksReceived  uint64 `json:"DupBlksReceived"`
	DupDataReceived  uint64 `json:"DupDataReceived"`
	MessagesReceived uint64 `json:"MessagesReceived"`
	WantlistLen      int    `json:"WantlistLen"`
	EngagedPeers     int    `json:"EngagedPeers"`
}

// Snapshot is one poll's worth of the whole seed picture. nil sub-fields (e.g. bitswap on an
// old node) render as "—" rather than crashing.
type Snapshot struct {
	At      time.Time
	Status  *SeedStatus
	Peers   []swarmPeer
	Bw      map[string]bwEntry
	Ledger  map[string]ledgerEntry
	Bitswap *BitswapStat
	Backers map[string]int
}

// Fetch pulls every monitoring endpoint concurrently. Partial failures degrade to nil sub-
// fields; a total failure to reach seed/status (and its node/status fallback) is returned as
// the error so the header can show "unreachable".
func (c *Client) Fetch(ctx context.Context) (*Snapshot, error) {
	snap := &Snapshot{At: time.Now()}
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Each sub-fetch runs concurrently and writes its slice of the snapshot under mu. Soft
	// failures (an old node missing an endpoint) just leave that field nil; only the status
	// fetch records a hard error (see statusErr below).
	run := func(f func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = f()
		}()
	}

	var statusErr error
	run(func() error {
		st, err := c.fetchStatus(ctx)
		if err != nil {
			mu.Lock()
			statusErr = err
			mu.Unlock()
			return err
		}
		mu.Lock()
		snap.Status = st
		mu.Unlock()
		return nil
	})
	run(func() error {
		var out struct{ Peers []swarmPeer }
		if err := c.post(ctx, "/api/v0/swarm/peers", &out); err != nil {
			return err
		}
		mu.Lock()
		snap.Peers = out.Peers
		mu.Unlock()
		return nil
	})
	run(func() error {
		var out struct{ ByPeer map[string]bwEntry }
		if err := c.post(ctx, "/api/v0/bandwidth/by-peer", &out); err != nil {
			return err
		}
		mu.Lock()
		snap.Bw = out.ByPeer
		mu.Unlock()
		return nil
	})
	run(func() error {
		var out struct{ ByPeer map[string]ledgerEntry }
		if err := c.post(ctx, "/api/v0/bitswap/ledger", &out); err != nil {
			return err // soft: old node has no ledger endpoint
		}
		mu.Lock()
		snap.Ledger = out.ByPeer
		mu.Unlock()
		return nil
	})
	run(func() error {
		var out BitswapStat
		if err := c.post(ctx, "/api/v0/bitswap/stat", &out); err != nil {
			return err
		}
		mu.Lock()
		snap.Bitswap = &out
		mu.Unlock()
		return nil
	})
	run(func() error {
		var out struct{ Counts map[string]int }
		if err := c.post(ctx, "/api/v0/playlist/backers", &out); err != nil {
			return err
		}
		mu.Lock()
		snap.Backers = out.Counts
		mu.Unlock()
		return nil
	})

	wg.Wait()
	if snap.Status == nil {
		return snap, statusErr
	}
	return snap, nil
}

// fetchStatus prefers the rich seed/status; on a 404 (un-upgraded node) it falls back to the
// old node/status and flags the result legacy so seed-only panels blank out.
func (c *Client) fetchStatus(ctx context.Context) (*SeedStatus, error) {
	var st SeedStatus
	err := c.post(ctx, "/api/v0/seed/status", &st)
	if err == nil {
		return &st, nil
	}
	if !strings.Contains(err.Error(), "404") {
		return nil, err
	}
	// Fallback: node/status (subset of fields).
	var legacy struct {
		Reachability string
		RelayStats   RelayStats
		Peers        int
		CatalogPeers int
		TotalIn      int64
		TotalOut     int64
		Pins         int
	}
	if err := c.post(ctx, "/api/v0/node/status", &legacy); err != nil {
		return nil, err
	}
	return &SeedStatus{
		legacy:       true,
		Reachability: legacy.Reachability,
		RelayStats:   legacy.RelayStats,
		Peers:        legacy.Peers,
		CatalogPeers: legacy.CatalogPeers,
		TotalIn:      legacy.TotalIn,
		TotalOut:     legacy.TotalOut,
		Pins:         legacy.Pins,
	}, nil
}

// Identify pulls agent version + protocols for a single peer (peers-pane detail).
func (c *Client) Identify(ctx context.Context, id string) (agent string, protocols []string) {
	var out struct {
		Agent     string
		Protocols []string
	}
	_ = c.post(ctx, "/api/v0/peer/identify?arg="+id, &out)
	return out.Agent, out.Protocols
}
