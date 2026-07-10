// Command tsmon is a terminal dashboard for a running tsnode — the seed-operator's view of
// "how is seeding going": reachability, relay-hop breakdown, per-peer bandwidth AND the
// seed-only signals the desktop peers panel can't show (DHT provide success/fail + queue,
// reprovide timing, pin-kind breakdown, and the real bitswap ledger of bytes served per peer).
//
// It polls the node's loopback RPC — the same `/api/v0` surface the desktop client and
// deploy/metrics-export.sh use. The RPC is loopback-only, so to watch the prod seed either run
// tsmon on the box, or tunnel first:
//
//	ssh -N -L 5001:127.0.0.1:5001 trackerstream-server   # in another shell
//	tsmon --rpc http://127.0.0.1:5001
//
// or let tsmon open the tunnel for you:
//
//	tsmon --ssh trackerstream-server        # implies the seed's default 5001
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	var (
		rpc      = flag.String("rpc", "http://127.0.0.1:5001", "tsnode RPC base URL (seed default :5001, client sidecar :5099)")
		interval = flag.Duration("interval", time.Second, "poll interval")
		sshHost  = flag.String("ssh", "", "SSH host (e.g. trackerstream-server) to auto-tunnel the remote loopback RPC")
		remote   = flag.Int("remote-port", 5001, "remote RPC port to tunnel when --ssh is set")
	)
	flag.Parse()

	target := *rpc
	if *sshHost != "" {
		local, cleanup, err := sshTunnel(*sshHost, *remote)
		if err != nil {
			fmt.Fprintf(os.Stderr, "tsmon: ssh tunnel: %v\n", err)
			os.Exit(1)
		}
		defer cleanup()
		*rpc = fmt.Sprintf("http://127.0.0.1:%d", local)
		target = fmt.Sprintf("%s:%d (via %s)", "127.0.0.1", *remote, *sshHost)
	}

	client := NewClient(*rpc)
	p := tea.NewProgram(newModel(client, *interval, target), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "tsmon: %v\n", err)
		os.Exit(1)
	}
}

// sshTunnel opens `ssh -N -L <local>:127.0.0.1:<remote> <host>` on a free local port and waits
// until it accepts connections. Returns the local port + a cleanup that kills the tunnel.
func sshTunnel(host string, remotePort int) (int, func(), error) {
	local, err := freePort()
	if err != nil {
		return 0, nil, err
	}
	spec := fmt.Sprintf("%d:127.0.0.1:%d", local, remotePort)
	cmd := exec.Command("ssh", "-N",
		"-o", "ExitOnForwardFailure=yes",
		"-o", "ServerAliveInterval=15",
		"-L", spec, host)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, nil, err
	}
	cleanup := func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
	// wait up to 10s for the forwarded port to come up
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addr := fmt.Sprintf("127.0.0.1:%d", local)
	for {
		if c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond); err == nil {
			c.Close()
			return local, cleanup, nil
		}
		select {
		case <-ctx.Done():
			cleanup()
			return 0, nil, fmt.Errorf("tunnel to %s did not come up", host)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
