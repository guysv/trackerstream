// Command tsnode is the trackerstream custom IPFS node — one binary run as the server
// master (`--role server`) and the desktop client sidecar (`--role client`).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	tsnode "github.com/trackerstream/tsnode"
)

func main() {
	// Deploy-time subcommand: import a kubo `ipfs key export` blob so tsnode reproduces the
	// prod swarm identity ("self" → repo/identity.key) and catalog IPNS key ("catalog").
	if len(os.Args) > 1 && os.Args[1] == "import-key" {
		importKeyCmd(os.Args[2:])
		return
	}

	role := flag.String("role", envOr("TS_ROLE", "client"), "node role: server|client")
	repo := flag.String("repo", envOr("TS_REPO", os.Getenv("IPFS_PATH")), "data dir (blockstore + identity.key); empty = ephemeral")
	swarmPort := flag.Int("swarm-port", envOrInt("TS_SWARM_PORT", 0), "libp2p swarm port (0 = OS-assigned)")
	rpcAddr := flag.String("rpc", envOr("TS_RPC", "127.0.0.1:5099"), "kubo-compatible RPC listen addr")
	bootstrap := flag.String("bootstrap", os.Getenv("TS_BOOTSTRAP"), "comma-separated bootstrap multiaddrs")
	noNATPortMap := flag.Bool("no-natportmap", envOrBool("TS_NO_NATPORTMAP", false), "disable client UPnP/NAT-PMP port mapping (fall back to relay+DCUtR)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := tsnode.DefaultConfig(tsnode.Role(*role), *repo, *swarmPort)
	cfg.DisableNATPortMap = *noNATPortMap
	if *bootstrap != "" {
		for _, s := range strings.Split(*bootstrap, ",") {
			if s = strings.TrimSpace(s); s != "" {
				cfg.Bootstrap = append(cfg.Bootstrap, s)
			}
		}
	}

	n, err := tsnode.New(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[tsnode] start failed: %v\n", err)
		os.Exit(1)
	}
	defer n.Close()

	fmt.Printf("[tsnode] role=%s peer=%s\n", *role, n.ID())
	for _, a := range n.Addrs() {
		fmt.Printf("[tsnode] listen %s/p2p/%s\n", a, n.ID())
	}

	srv := tsnode.NewRPCServer(n)
	go func() {
		fmt.Printf("[tsnode] RPC on http://%s/api/v0/\n", *rpcAddr)
		if err := srv.ListenAndServe(*rpcAddr); err != nil {
			fmt.Fprintf(os.Stderr, "[tsnode] RPC server: %v\n", err)
			cancel()
		}
	}()

	<-ctx.Done()
	fmt.Println("[tsnode] shutting down")
}

// importKeyCmd implements `tsnode import-key --repo <dir> --name <self|catalog> --file <blob>`.
// `--name self` writes the swarm identity (repo/identity.key); any other name installs a
// keystore key. Prints the resulting PeerId so the deploy can assert it matches config.
func importKeyCmd(args []string) {
	fs := flag.NewFlagSet("import-key", flag.ExitOnError)
	repo := fs.String("repo", envOr("TS_REPO", os.Getenv("IPFS_PATH")), "tsnode data dir")
	name := fs.String("name", "", "key name: 'self' (swarm identity) or a keystore name (e.g. catalog)")
	file := fs.String("file", "", "path to an `ipfs key export` blob")
	expect := fs.String("expect", "", "optional: assert the resulting PeerId equals this")
	_ = fs.Parse(args)
	if *repo == "" || *name == "" || *file == "" {
		fmt.Fprintln(os.Stderr, "usage: tsnode import-key --repo <dir> --name <self|catalog> --file <blob> [--expect <peerid>]")
		os.Exit(2)
	}
	blob, err := os.ReadFile(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[tsnode] read key file: %v\n", err)
		os.Exit(1)
	}
	var id string
	if *name == "self" {
		pid, err := tsnode.ImportIdentity(*repo, blob)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[tsnode] import identity: %v\n", err)
			os.Exit(1)
		}
		id = pid.String()
	} else {
		pid, err := tsnode.ImportNamedKey(*repo, *name, blob)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[tsnode] import key %q: %v\n", *name, err)
			os.Exit(1)
		}
		id = pid.String()
	}
	if *expect != "" && *expect != id {
		fmt.Fprintf(os.Stderr, "[tsnode] PeerId mismatch: imported %s, expected %s\n", id, *expect)
		os.Exit(1)
	}
	fmt.Printf("[tsnode] imported %q → %s\n", *name, id)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

// envOrBool reads a truthy env var (1/true/yes/on, case-insensitive); anything else is false.
func envOrBool(key string, def bool) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}
