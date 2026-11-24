package volume

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
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
	w.begin(now) // the wait starts (after the local open and recovery)
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
	wakes := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		d := g.renewDelay()
		in("after BARRIER: until just past T", d, 40*time.Second+100*time.Millisecond, 40*time.Second+600*time.Millisecond)
		wakes[d] = true
	}
	if len(wakes) < 10 {
		t.Fatalf("%d distinct wake delays in 50; the wake past T must be jittered", len(wakes))
	}
	now = now.Add(39 * time.Second)
	in("1 s before T", g.renewDelay(), 1100*time.Millisecond, 1600*time.Millisecond)
	now = now.Add(2 * time.Second)
	in("just past T: the fast poll", g.renewDelay(), 150*time.Millisecond, 250*time.Millisecond)
	now = now.Add(5 * time.Second)
	in("5 s past T: the 1 s poll", g.renewDelay(), 800*time.Millisecond, 1200*time.Millisecond)
	w.renewal(now, refusalWith(t, "barrier", control.WorkspaceLeaseRefusal_BARRIER, time.Hour))
	in("a far barrier: capped", g.renewDelay(), 2*time.Minute, 2*time.Minute)
	if observed != 2 {
		t.Fatalf("renewals observed %d; want 2", observed)
	}

	// Cancel during the long barrier sleep: Run returns at once.
	now = time.Unix(1_800_000_000, 0)
	g2, _ := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	g2.barrierAt = func() time.Time { return now.Add(2 * time.Minute) } // a long sleep to T
	if d := g2.renewDelay(); d < time.Minute {
		t.Fatalf("fixture: delay %v; want the long sleep to the barrier", d)
	}
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

// Refused credentials for an hour: no bound, reason credentials, and the log
// line at the start and every 10 min, driven by the heartbeat cadence alone
// (no /status read) (F1).
func TestCredentialsRefusedForAnHourWaitsAndLogs(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var lines []string
	w := newMasterWait(func() time.Time { return now }, func(f string, a ...any) { lines = append(lines, fmt.Sprintf(f, a...)) })
	denied := status.Error(codes.PermissionDenied, "node certificate revoked")
	for i := 0; i < 1800; i++ { // one heartbeat every 2 s for 60 min
		w.call(denied)
		_ = w.snapshot(true)
		now = now.Add(2 * time.Second)
	}
	if d, final := w.state(); !d.IsZero() || final != "" {
		t.Fatalf("after an hour of refused credentials: bound %v final %q; want none", d, final)
	}
	if len(lines) != 6 {
		t.Fatalf("%d log lines in 60 min; want 6 (at 0 and every 10 min): %q", len(lines), lines)
	}
	for _, l := range lines {
		if !strings.Contains(l, "waiting for the master: credentials since") || !strings.Contains(l, "(no bound)") {
			t.Fatalf("log line %q; want the credentials reason without a bound", l)
		}
	}
}

// The heartbeat loop itself logs the wait's reason: a workspace host whose
// master is unreachable states it with no /status read (F1 wiring).
func TestHeartbeatLogsTheMasterWait(t *testing.T) {
	var buf safeBuffer
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "s1", VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", WorkspaceLease: true,
		HeartbeatInterval: 20 * time.Millisecond, Logger: log.New(&buf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() { _ = h.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(buf.String(), "waiting for the master: unreachable since") {
		if time.Now().After(deadline) {
			t.Fatalf("no wait reason logged from the heartbeat loop; log:\n%s", buf.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type safeBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *safeBuffer) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *safeBuffer) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// A flapping master (an answer, then no reply, every 10 s for 10 min) never
// ends the wait early: each answer starts a new streak, so the bound is never
// before the latest contact + 30 s. It postpones the bound for as long as the
// flapping lasts, also for an obsolete unit (documented: F7).
func TestFlappingMasterNeverExpiresEarly(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	w := newMasterWait(func() time.Time { return now }, func(string, ...any) {})
	var lastContact time.Time
	for i := 0; i < 60; i++ {
		if i%2 == 0 {
			w.call(nil)
			lastContact = now
		} else {
			w.call(status.Error(codes.Unavailable, "flap"))
		}
		d, _ := w.state()
		if !d.IsZero() && (d.Before(lastContact.Add(MasterWaitLimit)) || !now.Before(d)) {
			t.Fatalf("step %d: bound %v ends the wait early (latest contact %v, now %v)", i, d, lastContact, now)
		}
		now = now.Add(10 * time.Second)
	}
}

// An identity refusal (typed IDENTITY detail) waits like refused credentials:
// no bound, reason "identity refused: <kind>" with its age; the first
// accepted reply ends it.
func TestIdentityRefusalWaitsVisibly(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	w := newMasterWait(func() time.Time { return now }, func(string, ...any) {})
	st, err := status.New(codes.FailedPrecondition, `workspace lease: server id "s9" is not registered`).
		WithDetails(&control.WorkspaceLeaseRefusal{Kind: control.WorkspaceLeaseRefusal_IDENTITY, Identity: "unregistered"})
	if err != nil {
		t.Fatal(err)
	}
	w.call(nil) // the heartbeat is answered first
	since := now
	for i := 0; i < 600; i++ { // 20 min of refused renewals and streams
		w.renewal(now, st.Err())
		now = now.Add(2 * time.Second)
	}
	if d, final := w.state(); !d.IsZero() || final != "" {
		t.Fatalf("after 20 min of identity refusals: bound %v final %q; want none", d, final)
	}
	if s := w.snapshot(true); s.Reason != "identity refused: unregistered" || !s.Since.Equal(since) || !s.Until.IsZero() {
		t.Fatalf("status %+v; want identity refused: unregistered since the first refusal, no bound", s)
	}
	w.call(nil) // a heartbeat answer does not check identity: still refused
	if s := w.snapshot(true); s.Reason != "identity refused: unregistered" {
		t.Fatalf("status %+v after a heartbeat answer; want the identity wait to go on", s)
	}
	w.renewal(now, nil) // registered now: the renewal passes
	if s := w.snapshot(true); s.Reason == "identity refused: unregistered" {
		t.Fatalf("status %+v after an accepted call; want the identity wait over", s)
	}
}

// The window opens at the first reply received at or after the wait's start:
// a run of replies that began during a slow local open and recovery does not
// spend it (claude's red case: recovery slower than 30 s after first contact).
func TestChunkAuthorityWaitAfterASlowOpenGetsItsFullLimit(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	w := newMasterWait(func() time.Time { return now }, func(string, ...any) {})
	for i := 0; i < 30; i++ { // heartbeats answered for 60 s while the store opens
		w.call(nil)
		now = now.Add(2 * time.Second)
	}
	start := now // open and recovery done: the wait starts
	w.begin(start)
	if d, _ := w.state(); !d.IsZero() {
		t.Fatalf("bound %v before any reply since the wait started; want none", d)
	}
	now = now.Add(1500 * time.Millisecond)
	first := now
	w.call(nil)
	if d, _ := w.state(); !d.Equal(first.Add(MasterWaitLimit)) {
		t.Fatalf("bound %v; want the first reply after the start + 30 s (%v)", d, first.Add(MasterWaitLimit))
	}
	w.renewal(now, refusalWith(t, "barrier", control.WorkspaceLeaseRefusal_BARRIER, 45*time.Second))
	if d, _ := w.state(); !d.Equal(now.Add(45*time.Second + MasterWaitLimit)) {
		t.Fatalf("bound %v; want T + 30 s", d)
	}
}

// An identity refusal ends the run of replies (no bound, no window kept); an
// accepted identity-checked call starts a new one there (the window opens
// again), so a corrected registration gets a full 30 s (L1, a). A refusal
// that is not repeated for identityDecay while replies continue ends by
// itself, so a fixed registration with no assignment still reaches the
// bound (b).
func TestIdentityRefusalEndsAndDecays(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	refused, err := status.New(codes.FailedPrecondition, "assignment stream: server id is not registered").
		WithDetails(&control.WorkspaceLeaseRefusal{Kind: control.WorkspaceLeaseRefusal_IDENTITY, Identity: "unregistered"})
	if err != nil {
		t.Fatal(err)
	}
	w := newMasterWait(func() time.Time { return now }, func(string, ...any) {})
	w.begin(now)
	w.call(nil) // window opens
	now = now.Add(10 * time.Second)
	w.call(refused.Err())
	if d, _ := w.state(); !d.IsZero() {
		t.Fatalf("bound %v under an identity refusal; want none", d)
	}
	now = now.Add(10 * time.Minute)
	w.identityAccepted() // a delivered assignment: registered now
	if d, _ := w.state(); !d.Equal(now.Add(MasterWaitLimit)) {
		t.Fatalf("bound %v after the identity was accepted; want a new window there + 30 s (%v)", d, now.Add(MasterWaitLimit))
	}

	w2 := newMasterWait(func() time.Time { return now }, func(string, ...any) {})
	w2.begin(now)
	w2.call(refused.Err())
	for i := 0; i < 14; i++ { // 28 s of heartbeat answers: still refused
		now = now.Add(2 * time.Second)
		w2.call(nil)
	}
	if s := w2.snapshot(false); s.Reason != "identity refused: unregistered" {
		t.Fatalf("status %+v at 28 s; want the identity refusal", s)
	}
	now = now.Add(4 * time.Second)
	w2.call(nil) // 32 s without a new identity refusal: it decays
	if s := w2.snapshot(false); s.Reason != "no_assignment" || s.Until.IsZero() {
		t.Fatalf("status %+v after the decay; want no_assignment with the 30 s bound", s)
	}
}
