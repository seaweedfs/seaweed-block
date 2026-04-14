package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

func TestParseFlags_Defaults(t *testing.T) {
	opts, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if opts.json {
		t.Fatal("default --json should be false")
	}
	if opts.runs != 1 {
		t.Fatalf("default --runs = %d, want 1", opts.runs)
	}
	if opts.httpAddr != "" {
		t.Fatalf("default --http = %q, want empty", opts.httpAddr)
	}
}

func TestParseFlags_AllFlagsSet(t *testing.T) {
	opts, err := parseFlags([]string{"--json", "--runs", "5", "--http", ":9090"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.json {
		t.Fatal("--json not set")
	}
	if opts.runs != 5 {
		t.Fatalf("--runs = %d, want 5", opts.runs)
	}
	if opts.httpAddr != ":9090" {
		t.Fatalf("--http = %q, want :9090", opts.httpAddr)
	}
}

func TestParseFlags_RejectsBadRuns(t *testing.T) {
	_, err := parseFlags([]string{"--runs", "0"})
	if err == nil {
		t.Fatal("expected error for --runs 0")
	}
	_, err = parseFlags([]string{"--runs", "-3"})
	if err == nil {
		t.Fatal("expected error for --runs -3")
	}
}

func TestScopeStatement_HonestAboutUnsupported(t *testing.T) {
	// The scope statement is the authoritative capability claim shown
	// by --help and echoed by /status. Verify it lists every
	// unsupported item we care about.
	mustContain := []string{
		"Supported:",
		"Not supported",
		"persistence",
		"master service",
		"RF3",
		"iSCSI",
		"NVMe-oF",
		"concurrent writer",
		"real operator CLI",
	}
	for _, s := range mustContain {
		if !strings.Contains(ScopeStatement, s) {
			t.Fatalf("ScopeStatement missing %q", s)
		}
	}
}

func TestVersion_HasPhase05Tag(t *testing.T) {
	// The version string should make the Phase 05 pre-production
	// status obvious. Zero-dot prefix + "phase05" tag.
	if !strings.HasPrefix(Version, "0.") {
		t.Fatalf("Version %q should start with 0. (pre-production)", Version)
	}
	if !strings.Contains(Version, "phase05") {
		t.Fatalf("Version %q should tag the phase explicitly", Version)
	}
}

func TestRunSparrow_AllThreePathsPass(t *testing.T) {
	// Exercise the full validation path once in non-verbose mode.
	// All three demos should PASS; exit code 0.
	code := runSparrow(options{json: true, runs: 1})
	if code != 0 {
		t.Fatalf("runSparrow exit code = %d, want 0 (all pass)", code)
	}
}

func TestRunSparrow_RepeatableAcrossRuns(t *testing.T) {
	// Repeat runs must all pass. If any run flakes, the exit code
	// becomes non-zero. This is the deterministic-validation proof.
	code := runSparrow(options{json: true, runs: 3})
	if code != 0 {
		t.Fatalf("runSparrow --runs=3 exit code = %d, want 0", code)
	}
}

func TestBuildResult_PassWhenHealthy(t *testing.T) {
	healthy := engine.ReplicaProjection{
		Mode: engine.ModeHealthy, R: 100, S: 10, H: 100,
	}
	r := buildResult("x", healthy)
	if !r.Pass {
		t.Fatal("expected Pass=true for healthy mode")
	}
	if r.Reason != "" {
		t.Fatalf("healthy result should have no reason, got %q", r.Reason)
	}
}

func TestStartHTTPOps_BindsAndServes(t *testing.T) {
	// Happy path: bind succeeds on :0, returns a real address, the
	// server actually answers /status requests.
	srv, boundAddr, err := startHTTPOps("127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected bind error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	// Bound address must be concrete — not ":0" and not empty.
	if boundAddr == "" || strings.HasSuffix(boundAddr, ":0") {
		t.Fatalf("boundAddr should resolve to a real port, got %q", boundAddr)
	}

	// Server must actually answer. Retry briefly for goroutine schedule.
	var resp *http.Response
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = http.Get("http://" + boundAddr + "/status")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /status never succeeded: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/status: got %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "phase05") {
		t.Fatalf("/status body should contain version tag, got: %s", body)
	}
}

func TestStartHTTPOps_FailsOnPortConflict(t *testing.T) {
	// Hold a port so the ops server cannot bind it. This is the
	// regression test for the honesty bug: the old code printed
	// "listening" even when ListenAndServe() failed asynchronously.
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not reserve port: %v", err)
	}
	defer blocker.Close()
	busyAddr := blocker.Addr().String()

	_, _, err = startHTTPOps(busyAddr)
	if err == nil {
		t.Fatalf("expected bind failure on occupied port %s, got nil", busyAddr)
	}
	// Must be a real net error, not a wrapped log line.
	if !strings.Contains(err.Error(), "bind") && !strings.Contains(err.Error(), "address") {
		t.Logf("note: error is %v (platform-dependent message; this is not a failure)", err)
	}
}

func TestRunSparrow_HTTPBindFailure_ReturnsExit3(t *testing.T) {
	// End-to-end honesty check: when --http can't bind, runSparrow
	// must exit with code 3 (runtime error), NOT silently proceed
	// to run demos as if the server were live.
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not reserve port: %v", err)
	}
	defer blocker.Close()
	busyAddr := blocker.Addr().String()

	code := runSparrow(options{
		json:     true,
		runs:     1,
		httpAddr: busyAddr,
	})
	if code != 3 {
		t.Fatalf("exit code = %d, want 3 (http bind failure)", code)
	}
}

func TestBuildResult_FailWhenNotHealthy(t *testing.T) {
	degraded := engine.ReplicaProjection{
		Mode:             engine.ModeDegraded,
		RecoveryDecision: engine.DecisionUnknown,
		R:                0, S: 10, H: 100,
	}
	r := buildResult("x", degraded)
	if r.Pass {
		t.Fatal("expected Pass=false for degraded mode")
	}
	if r.Reason == "" {
		t.Fatal("failing result should carry a reason")
	}
}
