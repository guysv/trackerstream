package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

var (
	cAccent = lipgloss.Color("39")  // cyan-blue
	cGood   = lipgloss.Color("42")  // green
	cWarn   = lipgloss.Color("214") // amber
	cBad    = lipgloss.Color("203") // red
	cDim    = lipgloss.Color("245") // gray
	cSeed   = lipgloss.Color("214") // seed/master tag

	stTitle  = lipgloss.NewStyle().Bold(true).Foreground(cAccent)
	stDim    = lipgloss.NewStyle().Foreground(cDim)
	stGood   = lipgloss.NewStyle().Foreground(cGood)
	stWarn   = lipgloss.NewStyle().Foreground(cWarn)
	stBad    = lipgloss.NewStyle().Foreground(cBad)
	stHeader = lipgloss.NewStyle().Bold(true).Foreground(cDim)
	stSelRow = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("231")).Background(lipgloss.Color("24"))
	stBox    = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(cDim).Padding(0, 1)
)

func (m model) View() string {
	if m.width == 0 {
		return "starting tsmon…"
	}
	var b strings.Builder
	b.WriteString(m.renderHeader())
	b.WriteString("\n")
	b.WriteString(m.renderSeedStrip())
	b.WriteString("\n\n")
	if m.detail && m.view == viewPeers && m.selected < len(m.rows) {
		b.WriteString(m.renderDetail(m.rows[m.selected]))
	} else if m.view == viewBackers {
		b.WriteString(m.renderBackers())
	} else {
		b.WriteString(m.renderPeers())
	}
	b.WriteString("\n")
	b.WriteString(m.renderFooter())
	return b.String()
}

func (m model) renderHeader() string {
	s := m.snap.GetStatus()
	title := stTitle.Render("tsmon") + stDim.Render(" · "+m.target)
	if m.err != nil {
		return title + "  " + stBad.Render("● unreachable: "+truncate(m.err.Error(), 60))
	}
	if s == nil {
		return title + "  " + stDim.Render("connecting…")
	}
	reach := reachBadge(s.Reachability)
	role := s.Role
	if role == "" {
		role = "?"
	}
	id := shortID(s.ID)
	line1 := fmt.Sprintf("%s  %s  %s  %s  up %s",
		title, stDim.Render(role+" "+id), reach,
		stDim.Render(s.AgentVersion), fmtDuration(time.Duration(s.UptimeSec)*time.Second))

	var down, up float64
	if s != nil {
		// aggregate live rate from per-peer bandwidth
		for _, bw := range m.snap.Bw {
			down += bw.RateIn
			up += bw.RateOut
		}
	}
	line2 := fmt.Sprintf("  peers %s (catalog-mesh %s)   relay %s/%s/%s   %s %s (%s)   %s %s (%s)",
		stGood.Render(fmt.Sprint(s.Peers)), stDim.Render(fmt.Sprint(s.CatalogPeers)),
		fmt.Sprint(s.RelayStats.Direct), fmt.Sprint(s.RelayStats.PeerRelay), fmt.Sprint(s.RelayStats.MasterRelay),
		stGood.Render("↓"), fmtRate(down), fmtBytes(s.TotalIn),
		stWarn.Render("↑"), fmtRate(up), fmtBytes(s.TotalOut),
	)
	return line1 + "\n" + line2
}

func (m model) renderSeedStrip() string {
	s := m.snap.GetStatus()
	if s == nil {
		return ""
	}
	if s.legacy {
		return stBox.Render(stWarn.Render("seed-health unavailable — node predates the seed/status RPC (redeploy tsnode)"))
	}
	pk := s.PinsByKind
	pins := fmt.Sprintf("pins %s  (root %d · track %d · piece %d)",
		stTitle.Render(fmt.Sprint(s.Pins)), pk["root"], pk["track_root"], pk["catalog_piece"])

	prov := s.Provide
	total := prov.OK + prov.Fail
	pct := 100.0
	if total > 0 {
		pct = 100 * float64(prov.OK) / float64(total)
	}
	provStyle := stGood
	if prov.Fail > 0 && pct < 95 {
		provStyle = stWarn
	}
	provide := fmt.Sprintf("provide %s ok / %d fail (%s)  queue %d/%d",
		fmt.Sprint(prov.OK), prov.Fail, provStyle.Render(fmt.Sprintf("%.0f%%", pct)),
		prov.Queue, prov.Cap)

	repro := "reprovide —"
	if s.Reprovide.IntervalSec > 0 {
		if s.Reprovide.LastUnix == 0 {
			repro = "reprovide: pending first sweep"
		} else {
			last := time.Unix(s.Reprovide.LastUnix, 0)
			next := last.Add(time.Duration(s.Reprovide.IntervalSec) * time.Second)
			repro = fmt.Sprintf("reprovide %s ago · next in %s",
				fmtDuration(time.Since(last)), fmtDuration(time.Until(next)))
		}
	}

	bsw := "bitswap —"
	if m.snap.Bitswap != nil {
		bs := m.snap.Bitswap
		dup := 0.0
		if bs.BlocksReceived > 0 {
			dup = 100 * float64(bs.DupBlksReceived) / float64(bs.BlocksReceived)
		}
		bsw = fmt.Sprintf("bitswap served %s in %s blocks · recv %s (dup %.0f%%) · engaged %d",
			fmtBytes(int64(bs.DataSent)), fmt.Sprint(bs.BlocksSent),
			fmtBytes(int64(bs.DataReceived)), dup, bs.EngagedPeers)
	}

	body := strings.Join([]string{pins, provide, repro, bsw}, "\n")
	return stBox.Width(min(m.width-2, 110)).Render(body)
}

// peer table columns
func (m model) renderPeers() string {
	if len(m.rows) == 0 {
		return stDim.Render("  no peers")
	}
	header := stHeader.Render(fmt.Sprintf("  %-16s %-6s %-8s %12s %12s %12s %10s %5s",
		"PEER", "ROLE", "LINK", "↓ RATE", "↑ RATE", "SERVING", "SERVED", "WANT"))

	// vertical budget: total height minus header/seedstrip/footer chrome (~11 lines)
	visible := m.height - 13
	if visible < 3 {
		visible = 3
	}
	start := 0
	if m.selected >= visible {
		start = m.selected - visible + 1
	}
	end := min(len(m.rows), start+visible)

	var b strings.Builder
	b.WriteString(header + "\n")
	for i := start; i < end; i++ {
		r := m.rows[i]
		link := "direct"
		if r.relayed {
			link = "relay"
		}
		if !r.connected {
			link = "—"
		}
		roleStr := r.role
		line := fmt.Sprintf("  %-16s %-6s %-8s %12s %12s %12s %10s %5d",
			shortID(r.id), roleStr, link,
			fmtRate(r.downRate), fmtRate(r.upRate), fmtRate(r.servedR),
			fmtBytes(int64(r.served)), r.wantlist)
		switch {
		case i == m.selected:
			line = stSelRow.Render(fmt.Sprintf("%-*s", min(m.width, 92), line))
		case !r.connected:
			line = stDim.Render(line)
		case r.role == "seed":
			line = lipgloss.NewStyle().Foreground(cSeed).Render(line)
		}
		b.WriteString(line + "\n")
	}
	if len(m.rows) > visible {
		b.WriteString(stDim.Render(fmt.Sprintf("  … %d/%d peers (↑/↓ to scroll)", m.selected+1, len(m.rows))))
	}
	return b.String()
}

func (m model) renderDetail(r peerRow) string {
	var b strings.Builder
	b.WriteString(stTitle.Render("peer "+shortID(r.id)) + stDim.Render("  ("+r.id+")") + "\n\n")

	transport := "direct"
	if r.relayed {
		transport = stWarn.Render("⚠ relayed via circuit")
	}
	if !r.connected {
		transport = stDim.Render("disconnected (remembered)")
	}
	rows := [][2]string{
		{"role", r.role},
		{"transport", transport},
		{"addr", orDash(r.addr)},
		{"agent", orDash(m.detailAgent)},
		{"protocols", orDash(strings.Join(m.detailProto, " "))},
		{"served → them", fmt.Sprintf("%s  (%s now)", fmtBytes(int64(r.served)), fmtRate(r.servedR))},
		{"recv ← them", fmtBytes(int64(r.recv))},
		{"↓ from them", fmt.Sprintf("%s  (%s)", fmtBytes(r.downTotal), fmtRate(r.downRate))},
		{"↑ to them", fmt.Sprintf("%s  (%s)", fmtBytes(r.upTotal), fmtRate(r.upRate))},
		{"their wantlist", fmt.Sprintf("%d blocks", r.wantlist)},
	}
	for _, kv := range rows {
		b.WriteString(fmt.Sprintf("  %-16s %s\n", stDim.Render(kv[0]), kv[1]))
	}
	b.WriteString("\n  " + stDim.Render("serve rate") + "  " + sparkline(m.sparks[r.id]) + "\n")
	b.WriteString("\n" + stDim.Render("  enter/esc: back"))
	return b.String()
}

func (m model) renderBackers() string {
	if len(m.backers) == 0 {
		return stDim.Render("  no backer beacons in window")
	}
	var b strings.Builder
	b.WriteString(stHeader.Render(fmt.Sprintf("  %-52s %6s", "PLAYLIST", "HOLDERS")) + "\n")
	visible := m.height - 13
	for i, br := range m.backers {
		if i >= visible {
			b.WriteString(stDim.Render(fmt.Sprintf("  … %d more", len(m.backers)-visible)))
			break
		}
		b.WriteString(fmt.Sprintf("  %-52s %6d\n", truncate(br.name, 52), br.count))
	}
	return b.String()
}

func (m model) renderFooter() string {
	tabs := []string{"peers", "backers"}
	var parts []string
	for i, t := range tabs {
		if viewKind(i) == m.view {
			parts = append(parts, stTitle.Render("["+t+"]"))
		} else {
			parts = append(parts, stDim.Render(" "+t+" "))
		}
	}
	state := stGood.Render("● live")
	if m.paused {
		state = stWarn.Render("⏸ paused")
	}
	age := ""
	if m.snap != nil {
		age = fmt.Sprintf("· %s ago", fmtDuration(time.Since(m.snap.At)))
	}
	keys := stDim.Render("tab:view  s:sort(" + m.sortMode.String() + ")  enter:detail  p:pause  r:refresh  q:quit")
	return strings.Join(parts, " ") + "   " + state + " " + stDim.Render(age) + "\n" + keys
}

// GetStatus is a nil-safe accessor.
func (s *Snapshot) GetStatus() *SeedStatus {
	if s == nil {
		return nil
	}
	return s.Status
}

func reachBadge(r string) string {
	switch r {
	case "public":
		return stGood.Render("● public")
	case "private":
		return stWarn.Render("● private")
	default:
		return stDim.Render("● " + orDash(r))
	}
}
