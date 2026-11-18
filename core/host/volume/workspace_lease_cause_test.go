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
	"google.golang.org/grpc"
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

// Issue #205: under sustained write load a volume stopped and nothing said
// why: the shutdown path logged nothing and the only trace was a raw
// DeadlineExceeded at closeout. Renewals failing until the lease expired is a
// hypothesis that matches the code path, not something the logs proved. A
// fence must name its cause so the next occurrence settles it.
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
	// The lease (1s) ended before the 2s per-call timeout: the log must say the
	// call ran into the lease deadline, not that the RPC timed out on its own.
	if !strings.Contains(out, "reached the lease deadline") || strings.Contains(out, "2s renewal timeout") {
		t.Fatalf("the log does not say which limit the call reached; log=%q", out)
	}
	if !strings.Contains(g.FenceCause(), "lease expired after a failed renewal") {
		t.Fatalf("FenceCause = %q", g.FenceCause())
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
	// The lease (10s) outlives the call: this failure is the 2s per-call
	// timeout, a slow master, not a lease that ran out.
	if !strings.Contains(out, "reached the 2s renewal timeout") || strings.Contains(out, "lease deadline") {
		t.Fatalf("the log does not say the 2s call timeout was reached; log=%q", out)
	}
	if g.FenceCause() != "" {
		t.Fatalf("a failure inside the lease recorded a fence cause: %q", g.FenceCause())
	}
}

// The first cause wins and is logged once: a frontend that notices expiry
// latches the cause with the exact deadline; the later drain, and a second
// Refresh, keep it and do not log the fence again.
func TestWorkspaceLeaseFirstFenceCauseWinsAndIsLoggedOnce(t *testing.T) {
	logs := captureLog(t)
	now := time.Unix(1000, 0)
	renewals, stops := 0, 0
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		renewals++
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second)}, nil
	}}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { stops++; return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if g.Projection().Healthy {
		t.Fatal("expired lease served")
	}
	const first = "lease expired at 1970-01-01T00:16:41Z without a successful renewal"
	if got := g.FenceCause(); got != first {
		t.Fatalf("FenceCause = %q, want %q", got, first)
	}
	err = g.Refresh(context.Background())
	if err == nil || !strings.Contains(err.Error(), first) {
		t.Fatalf("drain error does not carry the first cause: %v", err)
	}
	if err := g.Refresh(context.Background()); err == nil {
		t.Fatal("fenced guard reopened")
	}
	if got := g.FenceCause(); got != first {
		t.Fatalf("the first cause was replaced: %q", got)
	}
	if renewals != 1 || stops != 1 {
		t.Fatalf("renewals=%d stops=%d, want 1 and 1", renewals, stops)
	}
	if n := strings.Count(logs.String(), "workspace lease: fenced"); n != 1 {
		t.Fatalf("fence logged %d times, want once; log=%q", n, logs.String())
	}
}

// A late reply that also carries a revocation keeps the first cause (the late
// reply), not the revocation that arrived with it.
func TestWorkspaceLeaseLateRevokingReplyKeepsTheFirstCause(t *testing.T) {
	captureLog(t)
	now := time.Unix(1000, 0)
	renewals := 0
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		renewals++
		if renewals > 1 {
			now = now.Add(3 * time.Second) // the reply arrives after the 1s lease ended
			return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second), Revoking: true}, nil
		}
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second)}, nil
	}, ack: func() error { t.Fatal("an expired lease must not ACK"); return nil }}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err == nil {
		t.Fatal("a late reply did not fence")
	}
	if got := g.FenceCause(); !strings.HasPrefix(got, "renewal reply arrived after the lease deadline") {
		t.Fatalf("FenceCause = %q, want the late reply", got)
	}
}

// A stop that fails after a fence must not hide the cause: the returned error
// names the cause and keeps both the stop error and the renewal error.
func TestWorkspaceLeaseFailedStopKeepsTheCause(t *testing.T) {
	captureLog(t)
	errStop := errors.New("frontend close failed")
	now := time.Unix(1000, 0)
	calls := 0
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		calls++
		if calls == 1 {
			return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second)}, nil
		}
		now = now.Add(3 * time.Second)
		return nil, context.DeadlineExceeded
	}}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 0, errStop }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = g.Refresh(context.Background())
	if !errors.Is(err, errStop) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the stop or renewal error was lost: %v", err)
	}
	if !strings.Contains(err.Error(), "stop failed") || !strings.Contains(err.Error(), "lease expired after a failed renewal") {
		t.Fatalf("the error does not name the cause and the failed stop: %q", err)
	}
	// The next Refresh retries the stop through the fenced path: same cause.
	err = g.Refresh(context.Background())
	if !errors.Is(err, errStop) || !strings.Contains(err.Error(), "lease expired after a failed renewal") {
		t.Fatalf("the retried stop lost the cause: %v", err)
	}
}

// ctxLeaseClient answers a renewal only once the call's context has ended,
// like a reply that lands just after the 2s per-call timeout.
type ctxLeaseClient struct{ validFor time.Duration }

func (c ctxLeaseClient) RenewWorkspaceLease(ctx context.Context, _ *control.WorkspaceIdentity, _ ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	if ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second): // bound: the 2s call timeout ends it first
		}
	}
	return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(c.validFor)}, nil
}

func (ctxLeaseClient) AckWorkspaceRevoked(context.Context, *control.WorkspaceRevocationAck, ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	return &control.WorkspaceEmpty{}, nil
}

// A valid reply that arrives after the renewal call's 2s context ended, while
// the old lease and the new grant are both still valid, is still refused, but
// its cause must say the call's context ended, not that the lease expired.
func TestWorkspaceLeaseReplyAfterCallTimeoutIsNotCalledAnExpiry(t *testing.T) {
	captureLog(t)
	first := true
	slow := ctxLeaseClient{validFor: 20 * time.Second}
	renew := func(ctx context.Context, id *control.WorkspaceIdentity, o ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
		if first {
			first = false
			return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(20 * time.Second)}, nil
		}
		return slow.RenewWorkspaceLease(ctx, id, o...)
	}
	stops := 0
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, funcLeaseClient(renew), &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { stops++; return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	if err := g.Refresh(context.Background()); err == nil {
		t.Fatal("a reply after the call's context ended was accepted")
	}
	if took := time.Since(began); took > 4*time.Second {
		t.Fatalf("the late call was not bounded by the 2s timeout: %s", took)
	}
	if stops != 1 {
		t.Fatalf("stops=%d, want 1 (the refusal is unchanged)", stops)
	}
	cause := g.FenceCause()
	if !strings.HasPrefix(cause, "renewal reply arrived after the renewal call's context ended") || strings.Contains(cause, "lease deadline") {
		t.Fatalf("FenceCause = %q, want the call-context cause, not an expiry", cause)
	}
}

// funcLeaseClient adapts a function to WorkspaceLeaseClient (renewals only).
type funcLeaseClient func(context.Context, *control.WorkspaceIdentity, ...grpc.CallOption) (*control.WorkspaceLeaseFact, error)

func (f funcLeaseClient) RenewWorkspaceLease(ctx context.Context, id *control.WorkspaceIdentity, o ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	return f(ctx, id, o...)
}

func (funcLeaseClient) AckWorkspaceRevoked(context.Context, *control.WorkspaceRevocationAck, ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	return &control.WorkspaceEmpty{}, nil
}
