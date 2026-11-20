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

// stalledLeaseRig is a publisher whose lease writes, once armed, block until
// release, with a settable clock (t0 = first grant, TTL 30s, horizon +45s).
type stalledLeaseRig struct {
	p       *Publisher
	store   *blockedLeaseStore
	notes   *noteLog
	t0      time.Time
	nowNS   atomic.Int64
	release func()
}

func newStalledLeaseRig(t *testing.T) *stalledLeaseRig {
	t.Helper()
	disk, _ := newTempStore(t)
	r := &stalledLeaseRig{store: &blockedLeaseStore{AuthorityStore: disk, entered: make(chan struct{}), release: make(chan struct{})}, notes: &noteLog{}, t0: time.Unix(1000, 0)}
	r.nowNS.Store(r.t0.UnixNano())
	r.p = NewPublisher(nil, WithStore(r.store), WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return time.Unix(0, r.nowNS.Load()) }))
	r.p.leaseLog = r.notes.add
	var once sync.Once
	r.release = func() { once.Do(func() { close(r.store.release) }) }
	t.Cleanup(r.p.CloseWorkspaceExtenders) // runs after release (LIFO)
	t.Cleanup(r.release)
	if err := r.p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	r.store.armed.Store(true)
	return r
}

func (r *stalledLeaseRig) renew(d time.Duration) (WorkspaceLease, LeaseStats, error) {
	r.nowNS.Store(r.t0.Add(d).UnixNano())
	l, err := r.p.RenewWorkspaceLease(context.Background(), "v", "r", 1)
	return l, r.p.WorkspaceLeaseStats(), err
}

func (r *stalledLeaseRig) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-r.store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the extension write never reached the store")
	}
}

// assertAlarmBeforeClamp checks exactly one alarm line containing want, logged
// before the first clamp line.
func assertAlarmBeforeClamp(t *testing.T, lines []string, want string) {
	t.Helper()
	alarms := 0
	for _, s := range lines {
		if strings.Contains(s, "the authority store is lagging") && strings.Contains(s, "headroom") && strings.Contains(s, "volume=v") {
			alarms++
		}
	}
	alarmAt, clampAt := indexOf(lines, want), indexOf(lines, "are clamped to the durable horizon")
	if alarms != 1 || alarmAt < 0 || clampAt < 0 || alarmAt >= clampAt {
		t.Fatalf("want one alarm %q before the first clamp line: alarms=%d alarm=%d clamp=%d\n%s", want, alarms, alarmAt, clampAt, strings.Join(lines, "\n"))
	}
}

// Skipped band: the extension starts and stalls at +10s, and the next renewal
// (+20s) lands already clamped, never in the 1-1.125 TTL band. The alarm fires
// on that same renewal, logged before the clamp line, and counts once.
func TestWorkspaceLeaseAlarmWhenTheBandIsSkipped(t *testing.T) {
	r := newStalledLeaseRig(t)
	if _, s, err := r.renew(10 * time.Second); err != nil || s.HeadroomAlarms != 0 {
		t.Fatalf("+10s: %+v %v", s, err)
	}
	r.waitEntered(t)
	l, s, err := r.renew(20 * time.Second)
	if err != nil || l.ExpiresAtNS != r.t0.Add(45*time.Second).UnixNano() {
		t.Fatalf("+20s: want a grant clamped to the horizon, got %+v %v", l, err)
	}
	if s.HeadroomAlarms != 1 || s.ClampsTotal != 1 {
		t.Fatalf("+20s: want 1 alarm and 1 clamp, got %+v", s)
	}
	if _, s, err = r.renew(25 * time.Second); err != nil || s.HeadroomAlarms != 1 || s.ClampsTotal != 2 {
		t.Fatalf("+25s: alarm repeated or clamp missed: %+v %v", s, err)
	}
	assertAlarmBeforeClamp(t, r.notes.snapshot(), "volume=v headroom 25s below 1.125 TTL while the extension write is pending")
}

// First renewal below one TTL: no extension has started yet (the renewal
// itself starts it), and the grant clamps. The alarm still fires with that
// first clamp, before the clamp line, and does not repeat in the streak.
func TestWorkspaceLeaseAlarmWithAFirstRenewalBelowTTL(t *testing.T) {
	r := newStalledLeaseRig(t)
	l, s, err := r.renew(16 * time.Second)
	if err != nil || l.ExpiresAtNS != r.t0.Add(45*time.Second).UnixNano() {
		t.Fatalf("+16s: want a grant clamped to the horizon, got %+v %v", l, err)
	}
	if s.HeadroomAlarms != 1 || s.ClampsTotal != 1 {
		t.Fatalf("+16s: want 1 alarm with the first clamp, got %+v", s)
	}
	r.waitEntered(t) // the renewal started the extension; it stalls
	if _, s, err = r.renew(18 * time.Second); err != nil || s.HeadroomAlarms != 1 || s.ClampsTotal != 2 {
		t.Fatalf("+18s: %+v %v", s, err)
	}
	assertAlarmBeforeClamp(t, r.notes.snapshot(), "volume=v headroom 29s below 1 TTL: this grant is the first clamped to the durable horizon")
}

// Stall through expiry: after the clamps, every renewal is refused (no host
// validity, then expired) and none is granted again. The per-minute summary
// is still logged, by the first refused renewal after the minute, once.
func TestWorkspaceLeaseSummaryWithoutALaterGrant(t *testing.T) {
	r := newStalledLeaseRig(t)
	if _, _, err := r.renew(10 * time.Second); err != nil { // opens the window, starts the extension
		t.Fatal(err)
	}
	r.waitEntered(t)
	for _, d := range []time.Duration{20 * time.Second, 25 * time.Second} {
		if _, _, err := r.renew(d); err != nil {
			t.Fatalf("+%s: %v", d, err)
		}
	}
	for _, d := range []time.Duration{36 * time.Second, 46 * time.Second, 60 * time.Second} {
		if l, _, err := r.renew(d); err == nil {
			t.Fatalf("+%s: granted %+v through a stalled store past the horizon", d, l)
		}
	}
	if indexOf(r.notes.snapshot(), "clamped grants in the last") >= 0 {
		t.Fatal("summary logged before the minute ended")
	}
	if _, s, err := r.renew(71 * time.Second); err == nil || s.ClampsTotal != 2 {
		t.Fatalf("+71s: %+v %v, want a refusal", s, err)
	}
	if _, _, err := r.renew(72 * time.Second); err == nil {
		t.Fatal("+72s granted")
	}
	lines := r.notes.snapshot()
	want := "workspace lease: 2 clamped grants in the last 1m1s (total 2, min headroom 20s)"
	n := 0
	for _, s := range lines {
		if strings.Contains(s, "clamped grants in the last") {
			n++
		}
	}
	if indexOf(lines, want) < 0 || n != 1 {
		t.Fatalf("want the summary %q once from refused renewals, got %d summaries:\n%s", want, n, strings.Join(lines, "\n"))
	}
}
