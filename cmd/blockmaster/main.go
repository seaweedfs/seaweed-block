// Command blockmaster is the block master daemon — the P15 beta
// product host for P14 authority components + master-volume
// control RPC. See sw-block/design/v3-phase-15-t0-sketch.md
// §4.1 for the product-daemon contract.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/master"
)

type flags struct {
	authorityStore string
	listen         string
	topology       string
	// printReadyLine: test-only flag that emits a single
	// structured JSON line to stdout after the gRPC listener is
	// bound, so L2 subprocess tests can parse the ready event.
	// Not operator-facing; documented in the T0 smoke scenario.
	printReadyLine bool
}

func parseFlags(args []string) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("blockmaster", flag.ContinueOnError)
	fs.StringVar(&f.authorityStore, "authority-store", "", "durable authority store directory (required)")
	fs.StringVar(&f.listen, "listen", "127.0.0.1:0", "gRPC listen address (e.g. 127.0.0.1:9180)")
	fs.StringVar(&f.topology, "topology", "", "path to accepted-topology YAML (required for assignment to mint)")
	fs.BoolVar(&f.printReadyLine, "t0-print-ready", false, "internal test-only: emit one structured JSON line on stdout after listener bound")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	if f.authorityStore == "" {
		return flags{}, fmt.Errorf("--authority-store is required")
	}
	return f, nil
}

func main() {
	f, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		os.Exit(2)
	}
	os.Exit(run(f))
}

type readyLine struct {
	Component string `json:"component"`
	Phase     string `json:"phase"`
	Addr      string `json:"addr"`
}

func run(f flags) int {
	topo, err := loadTopology(f.topology)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		return 2
	}
	if len(topo.Volumes) == 0 {
		// Without a topology the observation host classifies every
		// volume UnknownReplicaClaim; controller can never mint.
		// That is a valid "empty cluster" startup state and not an
		// error, but operators should know — log a prominent line.
		fmt.Fprintln(os.Stderr, "blockmaster: WARNING: --topology not supplied or empty; controller will not mint assignments until topology is provided")
	}
	cfg := master.Config{
		AuthorityStoreDir: f.authorityStore,
		Listen:            f.listen,
		Topology:          topo,
		Freshness:         authority.FreshnessConfig{FreshnessWindow: 30 * time.Second, PendingGrace: 1 * time.Second},
		ControllerConfig:  authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3},
	}
	h, err := master.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster:", err)
		return 1
	}

	if f.printReadyLine {
		_ = json.NewEncoder(os.Stdout).Encode(readyLine{
			Component: "blockmaster",
			Phase:     "listening",
			Addr:      h.Addr(),
		})
	}

	h.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "blockmaster: close:", err)
		return 1
	}
	return 0
}
