package main

import (
	"context"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type viewKind int

const (
	viewPeers viewKind = iota
	viewBackers
	numViews
)

type sortKind int

const (
	sortServedRate sortKind = iota
	sortServed
	sortDown
	sortUp
	sortID
	numSorts
)

func (s sortKind) String() string {
	switch s {
	case sortServedRate:
		return "serving↓"
	case sortServed:
		return "served"
	case sortDown:
		return "down"
	case sortUp:
		return "up"
	default:
		return "peer"
	}
}

// peerRow is the joined per-peer view: swarm connection ∪ bandwidth attribution ∪ bitswap
// ledger. Rates come from the node (bandwidth EWMA); the serve rate is diffed across polls.
type peerRow struct {
	id        string
	connected bool
	role      string // seed | other
	addr      string
	relayed   bool
	downTotal int64
	upTotal   int64
	downRate  float64
	upRate    float64
	served    uint64 // ledger bytes sent to this peer (cumulative)
	servedR   float64
	recv      uint64
	wantlist  int
}

type model struct {
	client   *Client
	interval time.Duration
	target   string // display label for the RPC target

	width, height int
	view          viewKind
	sortMode      sortKind
	paused        bool
	detail        bool
	selected      int

	snap     *Snapshot
	prevSnap *Snapshot
	err      error

	rows    []peerRow
	backers []backerRow
	sparks  map[string][]float64 // peer id → recent serve-rate samples (detail sparkline)

	// detail-only identify, fetched lazily when a row is opened
	detailID    string
	detailAgent string
	detailProto []string
}

type backerRow struct {
	name  string
	count int
}

type snapMsg struct {
	snap *Snapshot
	err  error
}
type tickMsg struct{}
type identifyMsg struct {
	id    string
	agent string
	proto []string
}

func newModel(client *Client, interval time.Duration, target string) model {
	return model{
		client:   client,
		interval: interval,
		target:   target,
		sparks:   map[string][]float64{},
	}
}

func (m model) Init() tea.Cmd { return m.fetch() }

func (m model) fetch() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		snap, err := m.client.Fetch(ctx)
		return snapMsg{snap: snap, err: err}
	}
}

func tick(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(time.Time) tea.Msg { return tickMsg{} })
}

func (m model) identify(id string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		agent, proto := m.client.Identify(ctx, id)
		return identifyMsg{id: id, agent: agent, proto: proto}
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case tickMsg:
		if m.paused {
			return m, tick(m.interval)
		}
		return m, m.fetch()

	case snapMsg:
		m.prevSnap = m.snap
		m.snap = msg.snap
		m.err = msg.err
		if msg.snap != nil {
			m.recompute()
		}
		return m, tick(m.interval)

	case identifyMsg:
		if msg.id == m.detailID {
			m.detailAgent, m.detailProto = msg.agent, msg.proto
		}
		return m, nil
	}
	return m, nil
}

func (m *model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return *m, tea.Quit
	case "p":
		m.paused = !m.paused
		return *m, nil
	case "r":
		return *m, m.fetch() // manual refresh
	case "tab":
		m.detail = false
		m.view = (m.view + 1) % numViews
		m.selected = 0
		return *m, nil
	case "s":
		m.sortMode = (m.sortMode + 1) % numSorts
		m.sortRows()
		return *m, nil
	case "up", "k":
		if m.selected > 0 {
			m.selected--
		}
		return *m, nil
	case "down", "j":
		if m.selected < m.rowCount()-1 {
			m.selected++
		}
		return *m, nil
	case "enter":
		if m.view == viewPeers && m.rowCount() > 0 {
			if m.detail {
				m.detail = false
				return *m, nil
			}
			m.detail = true
			m.detailID = m.rows[m.selected].id
			m.detailAgent, m.detailProto = "", nil
			return *m, m.identify(m.detailID)
		}
		return *m, nil
	case "esc":
		m.detail = false
		return *m, nil
	}
	return *m, nil
}

func (m *model) rowCount() int {
	if m.view == viewBackers {
		return len(m.backers)
	}
	return len(m.rows)
}

// recompute rebuilds the joined peer rows + backers from the current snapshot and updates the
// per-peer serve-rate sparklines by diffing cumulative ledger bytes against the previous poll.
func (m *model) recompute() {
	seeds := map[string]bool{}
	if m.snap.Status != nil {
		for _, s := range m.snap.Status.Seeds {
			seeds[s] = true
		}
	}

	// connected set + primary addr from swarm/peers
	connAddr := map[string]string{}
	for _, p := range m.snap.Peers {
		if _, ok := connAddr[p.Peer]; !ok {
			connAddr[p.Peer] = p.Addr
		}
	}

	// union of every peer id we know anything about
	ids := map[string]struct{}{}
	for id := range connAddr {
		ids[id] = struct{}{}
	}
	for id := range m.snap.Bw {
		ids[id] = struct{}{}
	}
	for id := range m.snap.Ledger {
		ids[id] = struct{}{}
	}

	dt := 0.0
	if m.prevSnap != nil {
		dt = m.snap.At.Sub(m.prevSnap.At).Seconds()
	}

	rows := make([]peerRow, 0, len(ids))
	for id := range ids {
		addr, connected := connAddr[id]
		row := peerRow{
			id:        id,
			connected: connected,
			addr:      addr,
			relayed:   strings.Contains(addr, "/p2p-circuit"),
			role:      "other",
		}
		if seeds[id] {
			row.role = "seed"
		}
		if bw, ok := m.snap.Bw[id]; ok {
			row.downTotal, row.upTotal = bw.TotalIn, bw.TotalOut
			row.downRate, row.upRate = bw.RateIn, bw.RateOut
		}
		if l, ok := m.snap.Ledger[id]; ok {
			row.served, row.recv, row.wantlist = l.Sent, l.Recv, l.WantlistLen
			if dt > 0 && m.prevSnap != nil {
				if pl, ok := m.prevSnap.Ledger[id]; ok && l.Sent >= pl.Sent {
					row.servedR = float64(l.Sent-pl.Sent) / dt
				}
			}
		}
		rows = append(rows, row)
		// sparkline history (serve rate), bounded to 60 samples
		h := append(m.sparks[id], row.servedR)
		if len(h) > 60 {
			h = h[len(h)-60:]
		}
		m.sparks[id] = h
	}
	m.rows = rows
	m.sortRows()

	// prune sparkline history for peers that vanished
	for id := range m.sparks {
		if _, ok := ids[id]; !ok {
			delete(m.sparks, id)
		}
	}

	// backers
	m.backers = m.backers[:0]
	for name, c := range m.snap.Backers {
		m.backers = append(m.backers, backerRow{name: name, count: c})
	}
	sort.Slice(m.backers, func(i, j int) bool {
		if m.backers[i].count != m.backers[j].count {
			return m.backers[i].count > m.backers[j].count
		}
		return m.backers[i].name < m.backers[j].name
	})

	if m.selected >= m.rowCount() {
		m.selected = max(0, m.rowCount()-1)
	}
}

func (m *model) sortRows() {
	less := func(i, j int) bool {
		a, b := m.rows[i], m.rows[j]
		switch m.sortMode {
		case sortServedRate:
			if a.servedR != b.servedR {
				return a.servedR > b.servedR
			}
			return a.served > b.served
		case sortServed:
			return a.served > b.served
		case sortDown:
			return a.downRate > b.downRate
		case sortUp:
			return a.upRate > b.upRate
		default:
			return a.id < b.id
		}
	}
	// connected peers always float above disconnected-but-remembered ones
	sort.SliceStable(m.rows, func(i, j int) bool {
		if m.rows[i].connected != m.rows[j].connected {
			return m.rows[i].connected
		}
		return less(i, j)
	})
}
