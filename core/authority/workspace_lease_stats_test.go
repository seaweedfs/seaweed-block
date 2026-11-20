package authority

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockedLeaseStore holds every armed workspace-lease write until release
// closes, standing in for an authority journal stuck behind other fsyncs.
type blockedLeaseStore struct {
	AuthorityStore
	armed   atomic.Bool
	entered chan struct{}
	once    sync.Once
	release chan struct{}
}

func (s *blockedLeaseStore) Put(r DurableRecord) error {
	if s.armed.Load() && r.Workspace != nil && r.Workspace.Lease != nil {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.AuthorityStore.Put(r)
}

type noteLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *noteLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, s)
}

func (l *noteLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

func indexOf(lines []string, sub string) int {
	for i, s := range lines {
		if strings.Contains(s, sub) {
			return i
		}
	}
	return -1
}

// With the authority store's lease write stuck, renewals at an advancing
// clock must raise the headroom alarm on a renewal strictly before the first
// clamped grant, then count clamps (total and last minute) and the minimum
// headroom, and log a per-minute summary once a minute with clamps ends.
func TestWorkspaceLeaseHeadroomAlarmPrecedesFirstClamp(t *testing.T) {
	disk, _ := newTempStore(t)
	store := &blockedLeaseStore{AuthorityStore: disk, entered: make(chan struct{}), release: make(chan struct{})}
	var nowNS atomic.Int64
	t0 := time.Unix(1000, 0)
	nowNS.Store(t0.UnixNano())
	at := func(d time.Duration) { nowNS.Store(t0.Add(d).UnixNano()) }
	const ttl = 30 * time.Second
	p := NewPublisher(nil, WithStore(store), WithWorkspaceLeaseConfig(ttl, func() time.Time { return time.Unix(0, nowNS.Load()) }))
	notes := &noteLog{}
	p.leaseLog = notes.add
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	t.Cleanup(p.CloseWorkspaceExtenders) // runs after release (LIFO)
	t.Cleanup(release)
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	// First grant: synchronous write, horizon t0+45s.
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	if s := p.WorkspaceLeaseStats(); s.HeadroomAlarms != 0 || s.ClampsTotal != 0 || s.MinHeadroomObserved {
		t.Fatalf("stats after the first grant: %+v", s)
	}
	store.armed.Store(true)
	renew := func(d time.Duration) (WorkspaceLease, LeaseStats) {
		t.Helper()
		at(d)
		l, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1)
		if err != nil {
			t.Fatalf("renewal at +%s: %v", d, err)
		}
		return l, p.WorkspaceLeaseStats()
	}

	// +10s: headroom 35s < 1.25 TTL starts the extension, which blocks.
	_, s := renew(10 * time.Second)
	if s.HeadroomAlarms != 0 || s.ClampsTotal != 0 {
		t.Fatalf("+10s (headroom 35s, extension just started): %+v", s)
	}
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the extension write never reached the store")
	}
	// +12s: headroom 33s < 33.75s (1.125 TTL), extension pending: alarm, no clamp.
	l, s := renew(12 * time.Second)
	if s.HeadroomAlarms != 1 || s.ClampsTotal != 0 {
		t.Fatalf("+12s (headroom 33s, extension pending): want 1 alarm and 0 clamps, got %+v", s)
	}
	if want := t0.Add(12*time.Second + ttl).UnixNano(); l.ExpiresAtNS != want {
		t.Fatalf("+12s grant %d, want unclamped %d", l.ExpiresAtNS, want)
	}
	// +13s: same streak, no second alarm.
	if _, s = renew(13 * time.Second); s.HeadroomAlarms != 1 {
		t.Fatalf("+13s: alarm repeated within one streak: %+v", s)
	}
	// +20s, +25s: headroom 25s, 20s < TTL: clamped grants.
	l, s = renew(20 * time.Second)
	if want := t0.Add(45 * time.Second).UnixNano(); l.ExpiresAtNS != want {
		t.Fatalf("+20s grant %d, want clamped to horizon %d", l.ExpiresAtNS, want)
	}
	if s.ClampsTotal != 1 || s.ClampsLastMinute != 1 || s.HeadroomAlarms != 1 {
		t.Fatalf("+20s: %+v", s)
	}
	_, s = renew(25 * time.Second)
	if s.ClampsTotal != 2 || s.ClampsLastMinute != 2 || !s.MinHeadroomObserved || s.MinHeadroom != 20*time.Second {
		t.Fatalf("+25s: want 2 clamps, min headroom 20s; got %+v", s)
	}
	lines := notes.snapshot()
	alarmAt, clampAt := indexOf(lines, "below 1.125 TTL while the extension write is pending"), indexOf(lines, "are clamped to the durable horizon")
	if alarmAt < 0 || clampAt < 0 || alarmAt >= clampAt {
		t.Fatalf("alarm line must precede the first clamp line: alarm=%d clamp=%d\n%s", alarmAt, clampAt, strings.Join(lines, "\n"))
	}
	if !strings.Contains(lines[alarmAt], "volume=v headroom 33s") {
		t.Fatalf("alarm line: %q", lines[alarmAt])
	}

	// The store recovers: the extension lands (horizon +10s+45s = +55s).
	release()
	waitExtenders(t, p)
	// +26s: horizon +55s, headroom 29s < TTL: one more clamp; a new extension
	// starts (trigger +26s, horizon +71s) and lands at once.
	_, s = renew(26 * time.Second)
	waitExtenders(t, p)
	if s.ClampsTotal != 3 {
		t.Fatalf("+26s: %+v", s)
	}
	// +27s: headroom 44s >= 1.25 TTL: unclamped, the alarm streak ends.
	if _, s = renew(27 * time.Second); s.ClampsTotal != 3 || s.HeadroomAlarms != 1 {
		t.Fatalf("+27s: %+v", s)
	}
	// Healthy renewals keep the lease ahead: no clamps, no alarms.
	for _, d := range []time.Duration{40 * time.Second, 55 * time.Second} {
		if _, s = renew(d); s.ClampsTotal != 3 || s.HeadroomAlarms != 1 {
			t.Fatalf("+%s: %+v", d, s)
		}
		waitExtenders(t, p)
	}
	if indexOf(notes.snapshot(), "clamped grants in the last") >= 0 {
		t.Fatal("summary logged before the minute ended")
	}
	// +70s: a minute after the window's first renewal (+10s): the window with
	// clamps ends and its summary is logged by this renewal.
	if _, s = renew(70 * time.Second); s.ClampsTotal != 3 {
		t.Fatalf("+70s: %+v", s)
	}
	waitExtenders(t, p)
	lines = notes.snapshot()
	if i := indexOf(lines, "workspace lease: 3 clamped grants in the last 1m0s (total 3, min headroom 20s)"); i < 0 {
		t.Fatalf("no per-minute summary:\n%s", strings.Join(lines, "\n"))
	}
	// The +20s/+25s/+26s clamps are older than a minute at +87s.
	at(87 * time.Second)
	if s = p.WorkspaceLeaseStats(); s.ClampsLastMinute != 0 || s.ClampsTotal != 3 || s.MinHeadroom != 20*time.Second {
		t.Fatalf("+87s: clamps should age out of the last minute: %+v", s)
	}
}
