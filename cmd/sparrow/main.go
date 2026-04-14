// Command sparrow runs the minimal V3 block slice demonstrating
// healthy, catch-up, and rebuild paths through the semantic engine.
//
// Default invocation (Phase 04 accepted behavior, unchanged):
//
//	go run ./cmd/sparrow
//
// The sparrow starts a primary + replica in-process, writes blocks,
// and exercises all three recovery paths through the V3 adapter.
//
// Phase 05 extensions (opt-in flags, do not change default behavior):
//
//	--json          Emit machine-readable JSON instead of text
//	--runs N        Repeat the full demo N times (validation-style)
//	--http ADDR     Start a read-only HTTP ops surface; stays up after
//	                demos until SIGINT. /status /projection /trace.
//	--help          Print the authoritative supported/unsupported scope
//	--version       Print version information
//
// Exit codes:
//
//	0               All demos passed
//	1               One or more demos failed (validation use)
//	2               Usage / flag error
//
// This binary is a development and validation entry point only.
// The production operations surface is `weed shell` per the
// V3 operations design; this sparrow exists to make the runnable
// slice easy to start, inspect, and validate repeatedly during the
// standalone pre-integration stage of the repo.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// options captures parsed command-line flags.
type options struct {
	json        bool
	runs        int
	httpAddr    string
	calibrate   bool
	persistDemo bool
	persistDir  string
}

func main() {
	log.SetFlags(log.Lmicroseconds)

	// Handle --help / --version before normal flag parsing so users
	// can always discover capability.
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "-h", "--help", "help":
			fmt.Print(ScopeStatement)
			os.Exit(0)
		case "-v", "--version", "version":
			fmt.Printf("sparrow %s\n", Version)
			os.Exit(0)
		}
	}

	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if opts.calibrate {
		os.Exit(runCalibration(opts))
	}

	if opts.persistDemo {
		os.Exit(runPersistDemo(opts))
	}

	code := runSparrow(opts)
	os.Exit(code)
}

func parseFlags(args []string) (options, error) {
	fs := flag.NewFlagSet("sparrow", flag.ContinueOnError)
	var opts options
	fs.BoolVar(&opts.json, "json", false, "emit machine-readable JSON")
	fs.IntVar(&opts.runs, "runs", 1, "repeat the demo N times")
	fs.StringVar(&opts.httpAddr, "http", "", "optional HTTP ops listen address (e.g. :9090)")
	fs.BoolVar(&opts.calibrate, "calibrate", false, "run the Phase 06 calibration pass instead of the demo")
	fs.BoolVar(&opts.persistDemo, "persist-demo", false, "run the Phase 07 single-node persistence demo instead of the demo")
	fs.StringVar(&opts.persistDir, "persist-dir", "", "directory for the --persist-demo data file (required with --persist-demo)")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if opts.runs < 1 {
		return opts, fmt.Errorf("sparrow: --runs must be >= 1, got %d", opts.runs)
	}
	return opts, nil
}

// demoResult is one outcome record for JSON or text emission.
type demoResult struct {
	Run    int    `json:"run"`
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Mode   string `json:"mode"`
	R      uint64 `json:"R"`
	S      uint64 `json:"S"`
	H      uint64 `json:"H"`
	Reason string `json:"reason,omitempty"`
}

type runSummary struct {
	Version   string       `json:"version"`
	Runs      int          `json:"runs"`
	Total     int          `json:"total"`
	Passed    int          `json:"passed"`
	Failed    int          `json:"failed"`
	AllPassed bool         `json:"all_passed"`
	Results   []demoResult `json:"results"`
}

// lastAdapter is set after each demo so the HTTP ops server, if
// enabled, can surface the most recent projection/trace for inspection.
// Protected by its own mutex-free simple assignment: the ops server
// reads it via an accessor under lock.
var sparrowState = ops.NewState()

// startHTTPOps binds a listener synchronously and starts the ops
// server in a goroutine. Returns the running *http.Server, the
// ACTUAL bound address (useful when addr is ":0" for a random port),
// and an error if the bind fails.
//
// Binding must happen on the caller's goroutine so a port conflict
// returns a proper error rather than being swallowed into a log line
// after the caller has already claimed success. The goroutine only
// runs Serve() — which, given a pre-bound listener, cannot fail at
// bind time.
func startHTTPOps(addr string) (*http.Server, string, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}
	boundAddr := listener.Addr().String()
	srv := ops.NewServer(boundAddr, Version, ScopeStatement, sparrowState)
	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("sparrow: http ops serve: %v", err)
		}
	}()
	return srv, boundAddr, nil
}

func runSparrow(opts options) int {
	// Optional HTTP ops surface. Started BEFORE demos so testers can
	// probe /status even during long runs. Bind MUST be synchronous so
	// a port conflict surfaces as an exit code, not a log line — the
	// Phase 05 scope promises honest interfaces.
	var httpServer *http.Server
	if opts.httpAddr != "" {
		srv, boundAddr, err := startHTTPOps(opts.httpAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sparrow: failed to bind --http %s: %v\n", opts.httpAddr, err)
			return 3
		}
		httpServer = srv
		if !opts.json {
			fmt.Printf("HTTP ops surface listening on %s (/status /projection /trace)\n", boundAddr)
		}
	}

	summary := runSummary{
		Version: Version,
		Runs:    opts.runs,
		Results: []demoResult{},
	}

	for run := 1; run <= opts.runs; run++ {
		if !opts.json {
			fmt.Printf("=== V3 Block Sparrow (run %d/%d) ===\n\n", run, opts.runs)
		}
		results := runOne(!opts.json)
		for i := range results {
			results[i].Run = run
		}
		summary.Results = append(summary.Results, results...)
	}

	for _, r := range summary.Results {
		summary.Total++
		if r.Pass {
			summary.Passed++
		} else {
			summary.Failed++
		}
	}
	summary.AllPassed = summary.Failed == 0

	if opts.json {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(summary)
	} else {
		fmt.Println()
		fmt.Printf("=== Sparrow Complete: %d/%d passed ===\n", summary.Passed, summary.Total)
		fmt.Println("Three paths demonstrated per run:")
		fmt.Println("  1. Healthy (R >= H)")
		fmt.Println("  2. Catch-up (R >= S, R < H) → session close → healthy")
		fmt.Println("  3. Rebuild (R < S) → full base copy → session close → healthy")
	}

	// If HTTP ops is enabled, stay up until signal — testers need
	// time to inspect /status /projection /trace.
	if httpServer != nil {
		if !opts.json {
			fmt.Println()
			fmt.Println("HTTP ops surface is live. Press Ctrl+C to exit.")
		}
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}

	if summary.AllPassed {
		return 0
	}
	return 1
}

// runOne executes the three canonical demos once and returns per-demo results.
// verbose=true means human-readable progress output to stdout.
func runOne(verbose bool) []demoResult {
	results := make([]demoResult, 0, 3)

	primaryStore := storage.NewBlockStore(256, 4096)
	replicaStore := storage.NewBlockStore(256, 4096)

	replicaListener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		log.Fatalf("replica listener: %v", err)
	}
	replicaListener.Serve()
	defer replicaListener.Stop()
	replicaAddr := replicaListener.Addr()
	if verbose {
		fmt.Printf("Replica listening on %s\n", replicaAddr)
	}

	// --- Demo 1: Healthy ---
	if verbose {
		fmt.Println("\n--- Demo 1: Healthy (no recovery needed) ---")
	}
	for i := uint32(0); i < 10; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		primaryStore.Write(i, data)
	}
	_, _ = primaryStore.Sync()

	blocks := primaryStore.AllBlocks()
	_, _, pH := primaryStore.Boundaries()
	for lba, data := range blocks {
		replicaStore.ApplyEntry(lba, data, pH)
	}
	_, _ = replicaStore.Sync()

	exec := transport.NewBlockExecutor(primaryStore, replicaAddr, 1)
	adpt := adapter.NewVolumeReplicaAdapter(exec)
	adpt.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: replicaAddr, CtrlAddr: replicaAddr,
	})
	p1 := waitForMode(adpt, engine.ModeHealthy, 5*time.Second)
	sparrowState.Update("healthy", adpt)
	results = append(results, buildResult("healthy", p1))
	if verbose {
		fmt.Printf("Mode: %s  Decision: %s  R=%d S=%d H=%d\n",
			p1.Mode, p1.RecoveryDecision, p1.R, p1.S, p1.H)
		printResult("Demo 1", p1.Mode == engine.ModeHealthy)
	}

	// --- Demo 2: Catch-up ---
	if verbose {
		fmt.Println("\n--- Demo 2: Catch-up (short gap) ---")
	}
	for i := uint32(10); i < 20; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		primaryStore.Write(i, data)
	}
	_, _ = primaryStore.Sync()

	exec2 := transport.NewBlockExecutor(primaryStore, replicaAddr, 2)
	adpt2 := adapter.NewVolumeReplicaAdapter(exec2)
	adpt2.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: replicaAddr, CtrlAddr: replicaAddr,
	})
	p2 := waitForMode(adpt2, engine.ModeHealthy, 5*time.Second)
	sparrowState.Update("catch-up", adpt2)
	results = append(results, buildResult("catch-up", p2))
	if verbose {
		fmt.Printf("Mode: %s  Decision: %s  R=%d S=%d H=%d\n",
			p2.Mode, p2.RecoveryDecision, p2.R, p2.S, p2.H)
		printResult("Demo 2", p2.Mode == engine.ModeHealthy)
	}

	// --- Demo 3: Rebuild ---
	if verbose {
		fmt.Println("\n--- Demo 3: Rebuild (long gap) ---")
	}
	replicaStore2 := storage.NewBlockStore(256, 4096)
	replicaListener.Stop()
	replicaListener2, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore2)
	if err != nil {
		log.Fatalf("replica listener 2: %v", err)
	}
	replicaListener2.Serve()
	defer replicaListener2.Stop()
	replicaAddr2 := replicaListener2.Addr()

	primaryStore.AdvanceWALTail(primaryStore.NextLSN())

	exec3 := transport.NewBlockExecutor(primaryStore, replicaAddr2, 3)
	adpt3 := adapter.NewVolumeReplicaAdapter(exec3)
	adpt3.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 3, EndpointVersion: 3,
		DataAddr: replicaAddr2, CtrlAddr: replicaAddr2,
	})
	p3 := waitForMode(adpt3, engine.ModeHealthy, 5*time.Second)
	sparrowState.Update("rebuild", adpt3)
	results = append(results, buildResult("rebuild", p3))
	if verbose {
		fmt.Printf("Mode: %s  Decision: %s  R=%d S=%d H=%d\n",
			p3.Mode, p3.RecoveryDecision, p3.R, p3.S, p3.H)
		printResult("Demo 3", p3.Mode == engine.ModeHealthy)
	}

	return results
}

func buildResult(name string, p engine.ReplicaProjection) demoResult {
	r := demoResult{
		Name: name,
		Pass: p.Mode == engine.ModeHealthy,
		Mode: string(p.Mode),
		R:    p.R,
		S:    p.S,
		H:    p.H,
	}
	if !r.Pass {
		r.Reason = p.Reason
		if r.Reason == "" {
			r.Reason = fmt.Sprintf("did not reach healthy mode (final mode=%s decision=%s)", p.Mode, p.RecoveryDecision)
		}
	}
	return r
}

func waitForMode(adpt *adapter.VolumeReplicaAdapter, want engine.Mode, timeout time.Duration) engine.ReplicaProjection {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p := adpt.Projection()
		if p.Mode == want {
			return p
		}
		time.Sleep(50 * time.Millisecond)
	}
	return adpt.Projection()
}

func printResult(demo string, passed bool) {
	if passed {
		fmt.Printf("  %s: PASS\n", demo)
	} else {
		fmt.Printf("  %s: FAIL\n", demo)
	}
}
