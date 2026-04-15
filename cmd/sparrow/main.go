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
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

type options struct {
	json        bool
	runs        int
	httpAddr    string
	calibrate   bool
	persistDemo bool
	persistDir  string
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
	fs.BoolVar(&opts.calibrate, "calibrate", false, "run the Phase 06 calibration package")
	fs.BoolVar(&opts.persistDemo, "persist-demo", false, "run the single-node persistence demonstration")
	fs.StringVar(&opts.persistDir, "persist-dir", "", "directory for --persist-demo backing file")
	fs.SetOutput(ioDiscard{})
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if opts.runs < 1 {
		return options{}, fmt.Errorf("--runs must be >= 1")
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

	allPassed := true
	var allResults [][]demoResult
	for i := 0; i < opts.runs; i++ {
		results, err := runValidationPass(state)
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

func runValidationPass(state *ops.State) ([]demoResult, error) {
	var results []demoResult

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
	adpt.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: replicaAddr, CtrlAddr: replicaAddr,
	})
	time.Sleep(200 * time.Millisecond)
	results = append(results, buildResult("healthy", adpt.Projection()))

	for i := uint32(10); i < 20; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		_, _ = primaryStore.Write(i, data)
	}
	_, _ = primaryStore.Sync()
	exec2 := transport.NewBlockExecutor(primaryStore, replicaAddr)
	adpt2 := adapter.NewVolumeReplicaAdapter(exec2)
	state.Update("catch_up", adpt2)
	adpt2.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: replicaAddr, CtrlAddr: replicaAddr,
	})
	results = append(results, buildResult("catch_up", waitForMode(adpt2, engine.ModeHealthy, 5*time.Second)))

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
	adpt3.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 3, EndpointVersion: 3,
		DataAddr: replicaAddr2, CtrlAddr: replicaAddr2,
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
