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
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

type options struct {
	json           bool
	runs           int
	httpAddr       string
	calibrate      bool
	persistDemo    bool
	persistDir     string
	authorityStore  string
	s5Bootstrap     bool
	s7RestartSmoke  bool
}

type demoResult struct {
	Demo       string                 `json:"demo"`
	Pass       bool                   `json:"pass"`
	Reason     string                 `json:"reason,omitempty"`
	Projection engine.ReplicaProjection `json:"projection"`
}

func main() {
	log.SetFlags(log.Lmicroseconds)

	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "sparrow:", err)
		os.Exit(2)
	}

	switch {
	case opts.calibrate:
		os.Exit(runCalibration(opts))
	case opts.persistDemo:
		os.Exit(runPersistDemo(opts))
	case opts.s5Bootstrap:
		os.Exit(runS5Bootstrap(opts))
	case opts.s7RestartSmoke:
		os.Exit(runS7RestartSmoke(opts))
	default:
		os.Exit(runSparrow(opts))
	}
}

func parseFlags(args []string) (options, error) {
	var opts options
	fs := flag.NewFlagSet("sparrow", flag.ContinueOnError)
	fs.BoolVar(&opts.json, "json", false, "emit machine-readable JSON output")
	fs.IntVar(&opts.runs, "runs", 1, "repeat the validation path N times")
	fs.StringVar(&opts.httpAddr, "http", "", "start read-only HTTP ops server on ADDR")
	fs.BoolVar(&opts.calibrate, "calibrate", false, "run the calibration package")
	fs.BoolVar(&opts.persistDemo, "persist-demo", false, "run the single-node persistence demonstration")
	fs.StringVar(&opts.persistDir, "persist-dir", "", "directory for --persist-demo backing file")
	fs.StringVar(&opts.authorityStore, "authority-store", "", "directory for durable authority records")
	fs.BoolVar(&opts.s5Bootstrap, "s5-bootstrap", false, "run the durable authority bootstrap (requires --authority-store)")
	// --s7-restart-smoke is a test-only entry for the real-subprocess
	// restart smoke. It is intentionally undocumented in operator
	// tooling; the flag help string is terse so running sparrow with
	// --help doesn't advertise it as a workflow.
	fs.BoolVar(&opts.s7RestartSmoke, "s7-restart-smoke", false, "internal test-only: restart smoke (requires --authority-store)")
	fs.SetOutput(ioDiscard{})
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if opts.runs < 1 {
		return options{}, fmt.Errorf("--runs must be >= 1")
	}
	// --authority-store only makes sense under the durable
	// bootstrap mode. The default validation demos are SMOKE
	// tests by design — their Bind → Reassign → Reassign
	// sequence is not restart-clean against a persisted store
	// and forcing it to be would smuggle durability-aware
	// branches into demo code. Keep the durable flag strict:
	// only --s5-bootstrap consumes it. Future slices (S6 / S7)
	// can widen this.
	if opts.authorityStore != "" && !opts.s5Bootstrap && !opts.s7RestartSmoke {
		return options{}, fmt.Errorf("--authority-store requires --s5-bootstrap or --s7-restart-smoke; use one of those for the durable authority path")
	}
	if opts.s5Bootstrap && opts.authorityStore == "" {
		return options{}, fmt.Errorf("--s5-bootstrap requires --authority-store <dir>")
	}
	if opts.s7RestartSmoke && opts.authorityStore == "" {
		return options{}, fmt.Errorf("--s7-restart-smoke requires --authority-store <dir>")
	}
	if opts.s5Bootstrap && opts.s7RestartSmoke {
		return options{}, fmt.Errorf("--s5-bootstrap and --s7-restart-smoke are mutually exclusive")
	}
	return opts, nil
}

func runSparrow(opts options) int {
	state := ops.NewState()
	var srv *http.Server
	if opts.httpAddr != "" {
		boundSrv, boundAddr, err := startHTTPOpsWithState(opts.httpAddr, state)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sparrow: failed to bind --http %s: %v\n", opts.httpAddr, err)
			return 3
		}
		srv = boundSrv
		fmt.Printf("listening on %s\n", boundAddr)
		defer shutdownServer(srv)
	}

	// The default validation path is in-memory by design.
	// Durable authority (--authority-store) is only valid under
	// --s5-bootstrap; see parseFlags for why. Attempting to run
	// the demos against a durable store is rejected earlier.
	allPassed := true
	var allResults [][]demoResult
	for i := 0; i < opts.runs; i++ {
		results, err := runValidationPass(state, "vol1")
		if err != nil {
			fmt.Fprintf(os.Stderr, "sparrow: validation run %d: %v\n", i+1, err)
			return 3
		}
		allResults = append(allResults, results)
		for _, r := range results {
			if !r.Pass {
				allPassed = false
			}
		}
	}

	if opts.json {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(allResults)
	} else {
		fmt.Println("=== V3 Block Sparrow ===")
		for runIdx, results := range allResults {
			fmt.Printf("\nRun %d:\n", runIdx+1)
			for _, r := range results {
				status := "FAIL"
				if r.Pass {
					status = "PASS"
				}
				fmt.Printf("  %s: %s", r.Demo, status)
				if r.Reason != "" {
					fmt.Printf(" (%s)", r.Reason)
				}
				fmt.Println()
			}
		}
	}

	if allPassed {
		return 0
	}
	return 1
}

func runValidationPass(state *ops.State, volumeID string, pubOpts ...authority.PublisherOption) ([]demoResult, error) {
	var results []demoResult

	// All three demos route assignment truth through the S2 authority
	// publisher — the publisher mints Epoch/EndpointVersion, no demo
	// code constructs AssignmentInfo directly.
	//
	// pubOpts is an S5-era wiring seam kept for forward-compat,
	// but runSparrow ALWAYS passes the empty option set today:
	// --authority-store is rejected at parseFlags unless paired
	// with --s5-bootstrap, which is a separate mode and does not
	// reach this function. The default validation demos run
	// entirely in memory. Any LoadErrors() from an empty-option
	// publisher will be zero-length; the logging loop below is a
	// no-op in the default path and is kept for forward-compat
	// with future slices that may legitimately pass a store here.
	//
	// volumeID is caller-chosen so future slices (S6/S7) can
	// widen the flag surface without reworking this signature.
	// Today the only caller is runSparrow, which passes "vol1".
	dir := authority.NewStaticDirective(nil)
	pub := authority.NewPublisher(dir, pubOpts...)
	if errs := pub.LoadErrors(); len(errs) > 0 {
		for _, e := range errs {
			log.Printf("sparrow: durable authority reload skip: %v", e)
		}
	}
	pubCtx, pubCancel := context.WithCancel(context.Background())
	defer pubCancel()
	go func() { _ = pub.Run(pubCtx) }()

	primaryStore := storage.NewBlockStore(256, 4096)
	replicaStore := storage.NewBlockStore(256, 4096)

	replicaListener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		return nil, fmt.Errorf("replica listener: %w", err)
	}
	replicaListener.Serve()
	defer replicaListener.Stop()
	replicaAddr := replicaListener.Addr()

	exec := transport.NewBlockExecutor(primaryStore, replicaAddr)
	adpt := adapter.NewVolumeReplicaAdapter(exec)
	state.Update("healthy", adpt)

	for i := uint32(0); i < 10; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		_, _ = primaryStore.Write(i, data)
	}
	_, _ = primaryStore.Sync()
	blocks := primaryStore.AllBlocks()
	_, _, pH := primaryStore.Boundaries()
	for lba, data := range blocks {
		_ = replicaStore.ApplyEntry(lba, data, pH)
	}
	_, _ = replicaStore.Sync()

	bridge1Ctx, bridge1Cancel := context.WithCancel(pubCtx)
	go authority.Bridge(bridge1Ctx, pub, adpt, volumeID, "r1")
	dir.Append(authority.AssignmentAsk{
		VolumeID: volumeID, ReplicaID: "r1",
		DataAddr: replicaAddr, CtrlAddr: replicaAddr,
		Intent: authority.IntentBind,
	})
	time.Sleep(200 * time.Millisecond)
	results = append(results, buildResult("healthy", adpt.Projection()))
	bridge1Cancel()

	for i := uint32(10); i < 20; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		_, _ = primaryStore.Write(i, data)
	}
	_, _ = primaryStore.Sync()
	exec2 := transport.NewBlockExecutor(primaryStore, replicaAddr)
	adpt2 := adapter.NewVolumeReplicaAdapter(exec2)
	state.Update("catch_up", adpt2)

	bridge2Ctx, bridge2Cancel := context.WithCancel(pubCtx)
	go authority.Bridge(bridge2Ctx, pub, adpt2, volumeID, "r1")
	dir.Append(authority.AssignmentAsk{
		VolumeID: volumeID, ReplicaID: "r1",
		DataAddr: replicaAddr, CtrlAddr: replicaAddr,
		Intent: authority.IntentReassign,
	})
	results = append(results, buildResult("catch_up", waitForMode(adpt2, engine.ModeHealthy, 5*time.Second)))
	bridge2Cancel()

	replicaStore2 := storage.NewBlockStore(256, 4096)
	replicaListener2, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore2)
	if err != nil {
		return nil, fmt.Errorf("replica listener 2: %w", err)
	}
	replicaListener2.Serve()
	defer replicaListener2.Stop()
	replicaAddr2 := replicaListener2.Addr()
	primaryStore.AdvanceWALTail(primaryStore.NextLSN())
	exec3 := transport.NewBlockExecutor(primaryStore, replicaAddr2)
	adpt3 := adapter.NewVolumeReplicaAdapter(exec3)
	state.Update("rebuild", adpt3)

	bridge3Ctx, bridge3Cancel := context.WithCancel(pubCtx)
	defer bridge3Cancel()
	go authority.Bridge(bridge3Ctx, pub, adpt3, volumeID, "r1")
	dir.Append(authority.AssignmentAsk{
		VolumeID: volumeID, ReplicaID: "r1",
		DataAddr: replicaAddr2, CtrlAddr: replicaAddr2,
		Intent: authority.IntentReassign,
	})
	results = append(results, buildResult("rebuild", waitForMode(adpt3, engine.ModeHealthy, 5*time.Second)))

	return results, nil
}

func buildResult(name string, proj engine.ReplicaProjection) demoResult {
	r := demoResult{
		Demo:       name,
		Pass:       proj.Mode == engine.ModeHealthy,
		Projection: proj,
	}
	if !r.Pass {
		if proj.Reason != "" {
			r.Reason = proj.Reason
		} else {
			r.Reason = string(proj.Mode)
		}
	}
	return r
}

func startHTTPOps(addr string) (*http.Server, string, error) {
	return startHTTPOpsWithState(addr, ops.NewState())
}

func startHTTPOpsWithState(addr string, state *ops.State) (*http.Server, string, error) {
	srv := ops.NewServer(addr, Version, ScopeStatement, state)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	return srv, ln.Addr().String(), nil
}

func shutdownServer(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
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

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
