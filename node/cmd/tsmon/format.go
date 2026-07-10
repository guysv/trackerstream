package main

import (
	"fmt"
	"time"
)

func fmtBytes(n int64) string {
	if n < 0 {
		n = 0
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}

func fmtRate(bps float64) string {
	if bps < 1 {
		return "·"
	}
	return fmtBytes(int64(bps)) + "/s"
}

func fmtDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

func shortID(id string) string {
	if len(id) <= 14 {
		return id
	}
	return id[:6] + "…" + id[len(id)-5:]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// sparkline renders a rolling series with block characters, scaled to the series max.
func sparkline(vals []float64) string {
	if len(vals) == 0 {
		return "—"
	}
	blocks := []rune("▁▂▃▄▅▆▇█")
	mx := 0.0
	for _, v := range vals {
		if v > mx {
			mx = v
		}
	}
	out := make([]rune, len(vals))
	for i, v := range vals {
		idx := 0
		if mx > 0 {
			idx = int(v / mx * float64(len(blocks)-1))
		}
		if idx < 0 {
			idx = 0
		}
		if idx >= len(blocks) {
			idx = len(blocks) - 1
		}
		out[i] = blocks[idx]
	}
	return string(out)
}
