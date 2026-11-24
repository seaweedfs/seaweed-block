package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func refusalWith(t *testing.T, text string, kind control.WorkspaceLeaseRefusal_Kind, retry time.Duration) error {
	t.Helper()
	st, err := status.New(codes.FailedPrecondition, text).WithDetails(&control.WorkspaceLeaseRefusal{Kind: kind, RetryAfterNs: int64(retry)})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

// The startup wait's bound starts at the first answered contact, never
// while the master gives no reply; a BARRIER moves it to T + 30 s; a
// transport failure ends the streak (the next contact restarts it);
// credentials refusals have no bound; MOVED and DELETED are final; a
// refusal with the old text and no detail is plain contact, as today.
func TestChunkAuthorityWaitStartsAtFirstContact(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	w := newMasterWait(func() time.Time { return now }, func(string, ...any) {})
	unreachable := status.Error(codes.Unavailable, "connection refused")

	w.call(unreachable)
	now = now.Add(5 * time.Minute)
	w.call(unreachable)
	if d, final := w.state(); !d.IsZero() || final != "" {
		t.Fatalf("5 min without a reply: bound %v final %q; want none", d, final)
	}
	if s := w.snapshot(false); s.Reason != "unreachable" || !s.Until.IsZero() || now.Sub(s.Since) != 5*time.Minute {
		t.Fatalf("unreachable status %+v", s)
	}

	contact := now
	w.call(nil) // a heartbeat answered
	now = now.Add(10 * time.Second)
	w.call(nil)
	if d, _ := w.state(); !d.Equal(contact.Add(MasterWaitLimit)) {
		t.Fatalf("bound %v; want first contact + 30 s (%v)", d, contact.Add(MasterWaitLimit))
	}
	if s := w.snapshot(false); s.Reason != "no_assignment" || !s.Until.Equal(contact.Add(MasterWaitLimit)) {
		t.Fatalf("no-assignment status %+v", s)
	}

	// Old text, no detail: contact, as today (no early exit, no barrier).
	w.renewal(now, status.Error(codes.FailedPrecondition, "workspace forced move lease expiry plus clock-skew margin has not elapsed"))
	if d, final := w.state(); !d.Equal(contact.Add(MasterWaitLimit)) || final != "" {
		t.Fatalf("detail-less refusal changed the wait: bound %v final %q", d, final)
	}

	// BARRIER: T = request start + retry_after; bound T + 30 s.
	start := now
	w.renewal(start, refusalWith(t, "barrier", control.WorkspaceLeaseRefusal_BARRIER, 45*time.Second))
	if d, _ := w.state(); !d.Equal(start.Add(45*time.Second + MasterWaitLimit)) {
		t.Fatalf("bound %v after BARRIER; want T + 30 s", d)
	}
	if s := w.snapshot(true); s.Reason != "barrier" || !s.Until.Equal(start.Add(45*time.Second)) {
		t.Fatalf("barrier status %+v; want until T", s)
	}

	// A transport failure ends the streak; the next contact restarts it.
	now = now.Add(time.Minute)
	w.call(status.Error(codes.DeadlineExceeded, "slow"))
	if d, _ := w.state(); !d.IsZero() {
		t.Fatalf("bound %v while unreachable again; want none", d)
	}
	now = now.Add(2 * time.Minute)
	again := now
	w.call(nil)
	if d, _ := w.state(); !d.Equal(again.Add(MasterWaitLimit)) {
		t.Fatalf("bound %v; want the new streak + 30 s", d)
	}

	// Credentials refused: no bound, visible.
	w.call(status.Error(codes.PermissionDenied, "node revoked"))
	now = now.Add(10 * time.Minute)
	if d, _ := w.state(); !d.IsZero() {
		t.Fatalf("bound %v under refused credentials; want none", d)
	}
	if s := w.snapshot(true); s.Reason != "credentials" {
		t.Fatalf("credentials status %+v", s)
	}

	// A plain local error is contact (it is not a transport code).
	w.call(errors.New("local"))
	for _, k := range []struct {
		kind control.WorkspaceLeaseRefusal_Kind
		want string
	}{{control.WorkspaceLeaseRefusal_MOVED, "moved"}, {control.WorkspaceLeaseRefusal_DELETED, "deleted"}} {
		m := newMasterWait(func() time.Time { return now }, func(string, ...any) {})
		m.renewal(now, refusalWith(t, "x", k.kind, 0))
		if _, final := m.state(); final != k.want {
			t.Fatalf("%v refusal: final %q; want %q", k.kind, final, k.want)
		}
	}
}

// Before its first grant the guard sleeps until the master's barrier time T
// (condition: not a fixed 1 s poll), a detail-less refusal keeps the 1 s
// poll, and a cancel during the long sleep ends Run promptly.
func TestLeaseGuardSleepsUntilTheBarrier(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var reply error
	client := funcLeaseClient(func(context.Context, *control.WorkspaceIdentity, ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
		return nil, reply
	})
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	w := newMasterWait(func() time.Time { return now }, func(string, ...any) {})
	var observed int
	g.observe = func(start time.Time, err error) { observed++; w.renewal(start, err) }
	g.barrierAt = w.barrierTime
	in := func(what string, d, lo, hi time.Duration) {
		t.Helper()
		if d < lo || d > hi {
			t.Fatalf("%s: delay %v; want within [%v, %v]", what, d, lo, hi)
		}
	}

	reply = status.Error(codes.FailedPrecondition, "workspace forced move lease expiry plus clock-skew margin has not elapsed")
	_ = g.Refresh(context.Background())
	for i := 0; i < 20; i++ {
		in("detail-less refusal: the 1 s poll, jittered", g.renewDelay(), 800*time.Millisecond, 1200*time.Millisecond)
	}
	reply = refusalWith(t, "barrier", control.WorkspaceLeaseRefusal_BARRIER, 40*time.Second)
	_ = g.Refresh(context.Background())
	for i := 0; i < 20; i++ {
		in("after BARRIER: until just past T", g.renewDelay(), 40*time.Second+100*time.Millisecond, 40*time.Second+600*time.Millisecond)
	}
	now = now.Add(39 * time.Second)
	in("1 s before T", g.renewDelay(), 1100*time.Millisecond, 1600*time.Millisecond)
	now = now.Add(2 * time.Second)
	in("past T: the 1 s poll", g.renewDelay(), 800*time.Millisecond, 1200*time.Millisecond)
	w.renewal(now, refusalWith(t, "barrier", control.WorkspaceLeaseRefusal_BARRIER, time.Hour))
	in("a far barrier: capped", g.renewDelay(), 2*time.Minute, 2*time.Minute)
	if observed != 2 {
		t.Fatalf("renewals observed %d; want 2", observed)
	}

	// Cancel during the long barrier sleep: Run returns at once.
	now = time.Unix(1_800_000_000, 0)
	g2, _ := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g2.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)
	stopped := time.Now()
	cancel()
	select {
	case <-done:
		if d := time.Since(stopped); d > 2*time.Second {
			t.Fatalf("Run took %v to stop during the barrier sleep", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop during the barrier sleep")
	}
}

// /status shows MasterWait (reason, since, until) only while a workspace
// primary holds no granted lease.
func TestStatusShowsTheMasterWait(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1}}
	view := NewAdapterProjectionView(p, "v1", "r1", nil)
	view.workspaceRequired = true
	s := NewStatusServer(view)
	s.SetDurableStatusSource(stubDurableStatusSource{vols: []durable.VolumeStatus{{VolumeID: "v1", Operational: true}}})
	since, until := time.Unix(1000, 0), time.Unix(1045, 0)
	s.SetMasterWait(func() *MasterWait { return &MasterWait{Reason: "barrier", Since: since, Until: until} })
	got := s.statusProjection()
	if got.NotReadyReason != NotReadyLeaseNotHeld || got.MasterWait == nil ||
		*got.MasterWait != (MasterWaitStatus{Reason: "barrier", SinceMs: since.UnixMilli(), UntilMs: until.UnixMilli()}) {
		t.Fatalf("status %q MasterWait %+v; want barrier since/until", got.NotReadyReason, got.MasterWait)
	}
	view.workspaceRequired = false
	if got := s.statusProjection(); got.MasterWait != nil {
		t.Fatalf("MasterWait %+v shown without a lease wait", got.MasterWait)
	}
}
