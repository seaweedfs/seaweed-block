package durable_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"
)

type stalledSyncProxy struct {
	release chan struct{}
	done    chan struct{}
	exited  atomic.Int32
}

func (p *stalledSyncProxy) Write(_ context.Context, _ int64, data []byte) (int, error) {
	return len(data), nil
}
func (p *stalledSyncProxy) Sync(context.Context) error {
	// Match contextless LogicalStorage.Sync: cancellation cannot release it.
	<-p.release
	if p.exited.Add(1) == 8 {
		close(p.done)
	}
	return nil
}

func TestConcurrentSyncProxyStallChild(t *testing.T) {
	if os.Getenv("SYNC_PROXY_STALL_CHILD") != "1" {
		t.Skip("subprocess failure control only")
	}
	backend := &stalledSyncProxy{release: make(chan struct{}), done: make(chan struct{})}
	t.Cleanup(func() {
		close(backend.release)
		select {
		case <-backend.done:
			fmt.Println("STALL_CONTROL_CLEANUP all8 Sync workers released")
		case <-time.After(time.Second):
			t.Error("stalled workers did not settle during cleanup")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = runConcurrentSyncProxy(t, ctx, backend, 8, 100000)
	t.Fatal("stalled real-style Sync unexpectedly completed")
}

func TestConcurrentSyncProxyStallFailsWithinDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestConcurrentSyncProxyStallChild$", "-test.v")
	cmd.Env = append(os.Environ(), "SYNC_PROXY_STALL_CHILD=1")
	cmd.WaitDelay = time.Second
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("local wait bound failed; subprocess deadline reached: %v\n%s", ctx.Err(), out)
	}
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 1 {
		t.Fatalf("expected deliberate test failure exit1: %v\n%s", err, out)
	}
	for _, marker := range []string{"timed out waiting for backend Sync workers", "goroutine ", "stalledSyncProxy).Sync", "STALL_CONTROL_CLEANUP all8 Sync workers released"} {
		if !bytes.Contains(out, []byte(marker)) {
			t.Fatalf("missing %q in bounded stall receipt\n%s", marker, out)
		}
	}
	t.Logf("context-ignoring Sync produced bounded failure, stack dump and cleanup: %d receipt bytes", len(out))
}
