//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// plannedHandoffHarness owns real binaries, loopback endpoints and process
// lifetimes. The lifecycle test will use the commands and observables supplied
// with docs/volume-lifecycle.md; this scaffold makes no hand-over claim yet.
type plannedHandoffHarness struct {
	dir                                                  string
	master, volume, cli                                  string
	masterAddr, dataAddr, ctrlAddr, statusAddr, nvmeAddr string
}

func newPlannedHandoffHarness(t *testing.T) *plannedHandoffHarness {
	t.Helper()
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Fatal("CHUNKSTORE_WEED must name the pinned real SeaweedFS binary")
	}
	h := &plannedHandoffHarness{dir: t.TempDir()}
	suffix := ""
	if isWindows() {
		suffix = ".exe"
	}
	for _, b := range []struct {
		name string
		dst  *string
	}{
		{"blockmaster", &h.master}, {"blockvolume", &h.volume}, {"sw-block", &h.cli},
	} {
		*b.dst = filepath.Join(h.dir, b.name+suffix)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", *b.dst, "github.com/seaweedfs/seaweed-block/cmd/"+b.name)
		cmd.WaitDelay = 5 * time.Second
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("build %s: %v\n%s", b.name, err, out)
		}
	}
	h.masterAddr = leaseBinaryAddress(t)
	h.dataAddr = leaseBinaryAddress(t)
	h.ctrlAddr = leaseBinaryAddress(t)
	h.statusAddr = leaseBinaryAddress(t)
	h.nvmeAddr = leaseBinaryAddress(t)
	return h
}

// start registers cancellation and a bounded join before starting a daemon.
// Each child writes to its own file, and cleanup targets only that child.
func (h *plannedHandoffHarness) start(t *testing.T, name, binary string, args ...string) {
	t.Helper()
	log, err := os.Create(filepath.Join(h.dir, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = h.dir, log, log
	cmd.WaitDelay = 5 * time.Second
	done := make(chan error, 1)
	started := false
	t.Cleanup(func() {
		cancel()
		if started {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Errorf("%s did not exit after cancellation", name)
			}
		}
		_ = log.Close()
		if t.Failed() {
			data, _ := os.ReadFile(log.Name())
			if len(data) > 16000 {
				data = data[len(data)-16000:]
			}
			t.Logf("%s log:\n%s", name, data)
		}
	})
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	started = true
	go func() { done <- cmd.Wait() }()
}

// command keeps stdout separate from stderr: detach stdout remains JSON while
// its sealed-generation line is read from stderr by the documented sequence.
func (h *plannedHandoffHarness) command(t *testing.T, args ...string) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, h.cli, args...)
	cmd.Dir = h.dir
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("sw-block %q: %v\nstdout: %s\nstderr: %s", args, err, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}
