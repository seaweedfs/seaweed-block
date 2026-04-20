package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestS7Process_RealSubprocessRestartSmoke is the L2 process
// proof required by P14 S7 sketch §8.1 shape 2 and row 10b of
// the proof matrix.
//
// Shape:
//   1. Build the sparrow binary into a tempdir.
//   2. Spawn `sparrow --authority-store <dir> --s7-restart-smoke`.
//      First invocation sees an empty store, mints one Bind,
//      emits {"run":"first",...}, exits 0.
//   3. Spawn the same command again against the same dir.
//      Second invocation sees the reloaded line, mints nothing,
//      emits {"run":"second",...} whose Epoch matches the first
//      run's Epoch, exits 0.
//   4. Parent test asserts: both exit 0, both runs emitted the
//      pinned JSON pass-line, the lock was released between
//      runs (second run started cleanly), the line reloaded
//      matches what the first run wrote, and backwardMint is
//      false on both runs.
//
// Windows residual-risk scope (sketch §8.5): only teardown /
// cleanup-phase errors may be logged as residual. Any error
// from Bootstrap / the store itself must remain a hard
// failure. This test treats every stdout-parse failure and
// every exit!=0 as a hard failure.
func TestS7Process_RealSubprocessRestartSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess smoke under -short")
	}

	// Bound the whole test (build + two subprocess runs) to a
	// conservative deadline so a hung `go build` or subprocess
	// dies when `go test` times out instead of lingering.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tmp := t.TempDir()
	binName := "sparrow-s7-smoke"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	binPath := filepath.Join(tmp, binName)

	// Build the sparrow binary against the current module. Using
	// the Go toolchain the test runs under keeps versions aligned
	// with the test process.
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binPath, "github.com/seaweedfs/seaweed-block/cmd/sparrow")
	var buildErr bytes.Buffer
	buildCmd.Stderr = &buildErr
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("go build sparrow: %v\nstderr: %s", err, buildErr.String())
	}

	storeDir := filepath.Join(tmp, "store")

	// First run.
	firstReport := runS7Smoke(ctx, t, binPath, storeDir, "first")
	if firstReport.Error != "" {
		t.Fatalf("first run reported error: %s", firstReport.Error)
	}
	if firstReport.Pass != "s7-restart-smoke" {
		t.Fatalf("first run pass tag: got %q want %q", firstReport.Pass, "s7-restart-smoke")
	}
	if firstReport.Run != "first" {
		t.Fatalf("first run label: got %q want %q (empty store should produce a first-run)", firstReport.Run, "first")
	}
	if firstReport.Reloaded != 0 {
		t.Fatalf("first run reloaded: got %d want 0 (empty store)", firstReport.Reloaded)
	}
	if firstReport.Line == nil {
		t.Fatal("first run must report the minted line")
	}
	if firstReport.Line.Epoch != 1 || firstReport.Line.ReplicaID != "s7-smoke-r1" {
		t.Fatalf("first run line: got %+v, want Epoch=1 rid=s7-smoke-r1", firstReport.Line)
	}
	if firstReport.BackwardMint {
		t.Fatal("first run must not report backwardMint")
	}

	// Second run against the same store directory.
	secondReport := runS7Smoke(ctx, t, binPath, storeDir, "second")
	if secondReport.Error != "" {
		t.Fatalf("second run reported error: %s", secondReport.Error)
	}
	if secondReport.Pass != "s7-restart-smoke" {
		t.Fatalf("second run pass tag: got %q want %q", secondReport.Pass, "s7-restart-smoke")
	}
	if secondReport.Run != "second" {
		t.Fatalf("second run label: got %q want %q (non-empty store should produce a second-run)", secondReport.Run, "second")
	}
	if secondReport.Reloaded < 1 {
		t.Fatalf("second run reloaded: got %d, expected >= 1", secondReport.Reloaded)
	}
	if secondReport.Line == nil {
		t.Fatal("second run must report the reloaded line")
	}
	if secondReport.Line.Epoch != firstReport.Line.Epoch ||
		secondReport.Line.ReplicaID != firstReport.Line.ReplicaID ||
		secondReport.Line.VolumeID != firstReport.Line.VolumeID ||
		secondReport.Line.EndpointVersion != firstReport.Line.EndpointVersion {
		t.Fatalf("reloaded line mismatch: first=%+v second=%+v", firstReport.Line, secondReport.Line)
	}
	if secondReport.BackwardMint {
		t.Fatal("second run must not report backwardMint")
	}
}

// runS7Smoke spawns one invocation of the smoke binary,
// captures stdout, parses exactly one JSON line, and returns
// the decoded report. Asserts exit 0 and that the pass-line
// was emitted; anything else is a hard test failure.
func runS7Smoke(ctx context.Context, t *testing.T, binPath, storeDir, label string) s7RestartSmokeReport {
	t.Helper()

	cmd := exec.CommandContext(ctx, binPath, "--authority-store", storeDir, "--s7-restart-smoke")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
	} else if err != nil {
		t.Fatalf("%s run: unexpected error invoking subprocess: %v", label, err)
	}
	if exitCode != 0 {
		t.Fatalf("%s run: exit code %d\nstdout: %s\nstderr: %s", label, exitCode, stdout.String(), stderr.String())
	}

	// The schema is ONE JSON line. Decode the last non-empty
	// line in stdout so any incidental log output before the
	// pass-line would not break parsing. The child emits
	// exactly one encoded object via json.Encoder, so in the
	// happy path there is only one line.
	line := lastNonEmptyLine(stdout.String())
	if line == "" {
		t.Fatalf("%s run: no stdout line emitted\nstderr: %s", label, stderr.String())
	}
	var report s7RestartSmokeReport
	if decodeErr := json.Unmarshal([]byte(line), &report); decodeErr != nil {
		t.Fatalf("%s run: failed to decode stdout line: %v\nline: %s", label, decodeErr, line)
	}
	return report
}

func lastNonEmptyLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		ln := strings.TrimSpace(lines[i])
		if ln != "" {
			return ln
		}
	}
	return ""
}
