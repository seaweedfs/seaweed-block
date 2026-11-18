package volume

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// lockedBuffer collects log output from the guard (log.Printf may be called
// from more than one goroutine in other tests of this package).
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func captureLog(t *testing.T) *lockedBuffer {
	t.Helper()
	buf := &lockedBuffer{}
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(prevOut); log.SetFlags(prevFlags) })
	return buf
}

// Issue #205: under sustained write load a volume fenced because its lease
// renewals kept failing until the lease expired, and nothing said so: the
// shutdown path logged nothing and the only trace was the raw RPC error at
// closeout. The fence is correct; it must name its cause.
func TestWorkspaceLeaseRenewalTimeoutFenceNamesItsCause(t *testing.T) {
	logs := captureLog(t)
	now := time.Unix(1000, 0)
	calls, stops := 0, 0
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		calls++
		if calls == 1 {
			return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second)}, nil
		}
		// A slow master: the renewal RPC runs past the lease deadline and times out.
		now = now.Add(3 * time.Second)
		return nil, context.DeadlineExceeded
	}}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { stops++; return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = g.Refresh(context.Background())
	if err == nil {
		t.Fatal("a renewal that failed past the deadline did not fence")
	}
	if stops != 1 {
		t.Fatalf("fence did not stop the volume: stops=%d", stops)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the renewal error was lost: %v", err)
	}
	if !strings.Contains(err.Error(), "workspace lease expired") {
		t.Fatalf("the returned error does not name the cause: %q", err)
	}
	out := logs.String()
	if !strings.Contains(out, "workspace lease: fenced") || !strings.Contains(out, "renewal failed") {
		t.Fatalf("the fence and its cause were not logged; log=%q", out)
	}
}

// A renewal that fails while the lease is still valid does not fence, but it
// must be visible with its latency: a run of these is the early warning.
func TestWorkspaceLeaseRenewalFailureIsLoggedWithLatency(t *testing.T) {
	logs := captureLog(t)
	now := time.Unix(1000, 0)
	calls := 0
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		calls++
		if calls == 1 {
			return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(10 * time.Second)}, nil
		}
		now = now.Add(2 * time.Second)
		return nil, context.DeadlineExceeded
	}}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err == nil {
		t.Fatal("the failed renewal was not reported to the caller")
	}
	if !g.Projection().Healthy {
		t.Fatal("a failed renewal inside the lease fenced the volume")
	}
	out := logs.String()
	if !strings.Contains(out, "renewal failed after 2s") {
		t.Fatalf("the failed renewal and its latency were not logged; log=%q", out)
	}
}
