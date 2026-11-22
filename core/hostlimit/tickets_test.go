package hostlimit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// admitWith tries one Normal admission carrying t.
func admitWith(l *Limiter, t *Ticket) (uint64, error) {
	return l.Admit(WithTicket(WithClass(context.Background(), Normal), t), 1, nil)
}

// With one slot, tickets are admitted in due order whatever order their
// holders poll in.
func TestTicketsAdmitInDueOrder(t *testing.T) {
	c := testConfig(t)
	a, b, d := newTest(t, c), newTest(t, c), newTest(t, c)
	holder := newTest(t, c)
	hold, err := holder.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := HostNow()
	t1, _ := a.TakeTicket(now - 3e9)
	t2, _ := b.TakeTicket(now - 2e9)
	t3, _ := d.TakeTicket(now - 1e9)
	defer t1.Close()
	defer t2.Close()
	defer t3.Close()
	holder.Release(hold)
	settled(t, holder)
	order := []struct {
		l *Limiter
		t *Ticket
	}{{d, t3}, {b, t2}, {a, t1}}
	var admitted []*Ticket
	for round := 0; round < 3; round++ {
		for _, w := range order {
			if containsTicket(admitted, w.t) {
				continue
			}
			id, err := admitWith(w.l, w.t)
			if err != nil {
				if !errors.Is(err, ErrSlotsHeld) {
					t.Fatalf("refusal that is not a wait: %v", err)
				}
				continue
			}
			admitted = append(admitted, w.t)
			w.l.Release(id)
			settled(t, w.l)
			break
		}
	}
	if len(admitted) != 3 || admitted[0] != t1 || admitted[1] != t2 || admitted[2] != t3 {
		t.Fatalf("admission order %v, want t1, t2, t3", dues(admitted))
	}
}

func containsTicket(ts []*Ticket, t *Ticket) bool {
	for _, x := range ts {
		if x == t {
			return true
		}
	}
	return false
}

func dues(ts []*Ticket) []int64 {
	var d []int64
	for _, x := range ts {
		d = append(d, x.Due())
	}
	return d
}

// An earlier ticket whose holder exited (its lock dropped, its file left)
// is removed by the next waiter, which is then admitted.
func TestDeadEarlierTicketIsRemoved(t *testing.T) {
	c := testConfig(t)
	a, b := newTest(t, c), newTest(t, c)
	now := HostNow()
	dead, _ := a.TakeTicket(now - 2e9)
	late, _ := b.TakeTicket(now - 1e9)
	defer late.Close()
	_ = dead.f.Close() // the kernel drops an exited process's lock; the file stays
	id, err := admitWith(b, late)
	if err != nil {
		t.Fatalf("waiter behind a dead ticket: %v", err)
	}
	b.Release(id)
	if _, err := os.Stat(dead.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead ticket left in place: %v", err)
	}
}

// A live earlier ticket whose holder never polls (stopped) blocks a later
// waiter only until the later one has seen it ahead, with the slot free and
// no urgent work, for the skip time.
func TestStalledEarlierTicketIsSkipped(t *testing.T) {
	c := testConfig(t)
	a, b := newTest(t, c), newTest(t, c)
	b.testSkipAfter = 300 * time.Millisecond
	now := HostNow()
	stalled, _ := a.TakeTicket(now - 2e9)
	defer stalled.Close()
	late, _ := b.TakeTicket(now - 1e9)
	defer late.Close()
	start := time.Now()
	var id uint64
	var err error
	for {
		if id, err = admitWith(b, late); err == nil {
			break
		}
		if !errors.Is(err, ErrNotHead) {
			t.Fatalf("refusal: %v", err)
		}
		if time.Since(start) > 3*time.Second {
			t.Fatal("never passed the stalled ticket")
		}
		time.Sleep(20 * time.Millisecond)
	}
	el := time.Since(start)
	b.Release(id)
	if el < 300*time.Millisecond || el > 700*time.Millisecond {
		t.Fatalf("passed the stalled ticket after %s; want the skip time plus a poll", el)
	}
	if late.Skipped() != 1 {
		t.Fatalf("skipped %d tickets", late.Skipped())
	}
	if _, err := os.Stat(stalled.path); err != nil {
		t.Fatalf("the skipped ticket must keep its file: %v", err)
	}
}

// While the slot is held the skip observation does not run: a stalled head
// keeps its place as long as there is nothing to hand over.
func TestNoSkipWhileTheSlotIsHeld(t *testing.T) {
	c := testConfig(t)
	a, b, holder := newTest(t, c), newTest(t, c), newTest(t, c)
	b.testSkipAfter = 100 * time.Millisecond
	hold, err := holder.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := HostNow()
	head, _ := a.TakeTicket(now - 2e9)
	defer head.Close()
	late, _ := b.TakeTicket(now - 1e9)
	defer late.Close()
	for i := 0; i < 15; i++ {
		if _, err := admitWith(b, late); !errors.Is(err, ErrSlotsHeld) {
			t.Fatalf("poll %d with the slot held: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	holder.Release(hold)
	settled(t, holder)
	if _, err := admitWith(b, late); !errors.Is(err, ErrNotHead) {
		t.Fatalf("first poll after the slot freed must still respect the head: %v", err)
	}
}

// A prober that removes a ticket between its create and its lock does not
// lose the creator's place: the creator re-checks the path and creates a
// new ticket.
func TestTicketRemovedBeforeItsLockIsRetaken(t *testing.T) {
	c := testConfig(t)
	l := newTest(t, c)
	raced := false
	l.testAfterTicketCreate = func(path string) {
		if !raced {
			raced = true
			if !reap(path) {
				t.Error("the unlocked new ticket was not removable")
			}
		}
	}
	tk, err := l.TakeTicket(HostNow())
	if err != nil {
		t.Fatal(err)
	}
	defer tk.Close()
	if !raced || !samePath(tk.path, tk.f) {
		t.Fatalf("raced=%t; the kept ticket must be the one the path names", raced)
	}
	ents, _ := os.ReadDir(filepath.Join(c.Directory, ticketsDir))
	if len(ents) != 1 {
		t.Fatalf("%d ticket files, want 1", len(ents))
	}
}

// Urgent work waits at most a bounded time for its shared hold: a waiter
// stalled while holding its exclusive probe neither refuses nor blocks it.
func TestUrgentProceedsPastAStalledProbe(t *testing.T) {
	c := testConfig(t)
	l := newTest(t, c)
	probe, err := os.OpenFile(filepath.Join(c.Directory, urgentPendingFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err := tryLock(probe); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	id, err := l.Acquire(context.Background(), Urgent, nil)
	if err != nil {
		t.Fatalf("urgent admission behind a stalled probe: %v", err)
	}
	el := time.Since(start)
	l.Release(id)
	if el < urgentHoldWait || el > urgentHoldWait+300*time.Millisecond {
		t.Fatalf("urgent admission took %s; want about %s", el, urgentHoldWait)
	}
	if st := l.Snapshot().(Stats); st.UrgentPendingUnheld != 1 {
		t.Fatalf("UrgentPendingUnheld = %d", st.UrgentPendingUnheld)
	}
	release, held, err := HoldUrgentPending(c.Directory)
	if err != nil || held {
		t.Fatalf("HoldUrgentPending behind a stalled probe: held=%t err=%v", held, err)
	}
	release()
}

// Pacing divides the budget by the normal and urgent slots held on the
// host: a lone transfer gets all of it; with a second slot held, half.
func TestPacingUsesTheBudgetOverHeldSlots(t *testing.T) {
	c := testConfig(t)
	c.BytesPerSecond = 1 << 20
	c.PayloadBytes = 256 << 10
	a, b := newTest(t, c), newTest(t, c)
	id, err := a.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release(id)
	ctx := context.Background()
	if err := a.WaitUpload(ctx, 256<<10); err != nil { // reserves 0.25 s at the full budget
		t.Fatal(err)
	}
	start := time.Now()
	if err := a.WaitUpload(ctx, 256<<10); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 200*time.Millisecond || el > 450*time.Millisecond {
		t.Fatalf("lone transfer waited %s; want about 0.25 s (the whole budget)", el)
	}
	other, err := b.Acquire(ctx, Urgent, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release(other)
	if err := a.WaitUpload(ctx, 256<<10); err != nil { // re-counts: two held
		t.Fatal(err)
	}
	start = time.Now()
	if err := a.WaitUpload(ctx, 256<<10); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 400*time.Millisecond || el > 800*time.Millisecond {
		t.Fatalf("with two slots held waited %s; want about 0.5 s (half the budget)", el)
	}
}

// Status shows since when each slot file has been seen held, whoever holds
// it, so a stopped holder can be found.
func TestSnapshotShowsSlotsHeldSince(t *testing.T) {
	c := testConfig(t)
	a, b := newTest(t, c), newTest(t, c)
	before := time.Now().UnixNano()
	id, err := a.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	st := b.Snapshot().(Stats)
	since, ok := st.SlotsHeldSinceNs["normal-0"]
	if !ok || since < before {
		t.Fatalf("held slots %v; want normal-0 since the admission", st.SlotsHeldSinceNs)
	}
	time.Sleep(20 * time.Millisecond)
	if again := b.Snapshot().(Stats).SlotsHeldSinceNs["normal-0"]; again != since {
		t.Fatalf("hold start moved from %d to %d", since, again)
	}
	a.Release(id)
	settled(t, a)
	if st := b.Snapshot().(Stats); len(st.SlotsHeldSinceNs) != 0 {
		t.Fatalf("released slot still reported: %v", st.SlotsHeldSinceNs)
	}
}

// Status shows since when urgent-pending has been held (a hung stop keeps
// Normal admissions refused while it lives), and clears once it is free.
func TestSnapshotShowsUrgentPendingHeldSince(t *testing.T) {
	c := testConfig(t)
	l := newTest(t, c)
	if st := l.Snapshot().(Stats); st.UrgentPendingHeldSinceNs != 0 {
		t.Fatalf("free urgent-pending reported held: %d", st.UrgentPendingHeldSinceNs)
	}
	before := time.Now().UnixNano()
	release, held, err := HoldUrgentPending(c.Directory)
	if err != nil || !held {
		t.Fatalf("hold: %v %v", held, err)
	}
	since := l.Snapshot().(Stats).UrgentPendingHeldSinceNs
	if since < before {
		t.Fatalf("held since %d, want >= %d", since, before)
	}
	time.Sleep(20 * time.Millisecond)
	if again := l.Snapshot().(Stats).UrgentPendingHeldSinceNs; again != since {
		t.Fatalf("hold start moved from %d to %d", since, again)
	}
	release()
	if st := l.Snapshot().(Stats); st.UrgentPendingHeldSinceNs != 0 {
		t.Fatalf("released urgent-pending still reported: %d", st.UrgentPendingHeldSinceNs)
	}
}

// A pool error is reported until the next admission through the pool, which
// clears it: status then shows no stale error.
func TestPoolErrorClearsOnTheNextGoodAdmission(t *testing.T) {
	c := testConfig(t)
	missing := c
	missing.Directory = filepath.Join(c.Directory, "missing")
	l := newTest(t, missing)
	if _, err := l.Acquire(context.Background(), Normal, nil); err == nil {
		t.Fatal("admission through a missing pool")
	}
	if st := l.Snapshot().(Stats); st.LastPoolError == "" {
		t.Fatal("the pool error is not reported")
	}
	if err := os.MkdirAll(missing.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	id, err := l.Acquire(context.Background(), Normal, nil)
	if err != nil {
		t.Fatalf("admission once the pool exists: %v", err)
	}
	l.Release(id)
	if st := l.Snapshot().(Stats); st.LastPoolError != "" {
		t.Fatalf("stale pool error after a good admission: %q", st.LastPoolError)
	}
}

// holdGuard takes configuration.lock from another descriptor, as another
// admission does while it checks the configuration, until release.
func holdGuard(t *testing.T, dir string) (release func()) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, "configuration.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := tryLock(f); err != nil {
		t.Fatal(err)
	}
	var once bool
	release = func() {
		if !once {
			once = true
			_ = f.Close()
		}
	}
	t.Cleanup(release)
	return release
}

// An urgent admission that meets another admission's configuration lock
// waits for it briefly and is admitted once it is released; it is never
// refused for it.
func TestUrgentWaitsBrieflyForTheConfigurationLock(t *testing.T) {
	c := testConfig(t)
	l := newTest(t, c)
	release := holdGuard(t, c.Directory)
	go func() { time.Sleep(20 * time.Millisecond); release() }()
	id, err := l.Acquire(context.Background(), Urgent, nil)
	if err != nil {
		t.Fatalf("urgent admission behind a briefly held configuration lock: %v", err)
	}
	l.Release(id)
	if st := l.Snapshot().(Stats); st.Fallback != 0 || st.LastPoolError != "" {
		t.Fatalf("a held configuration lock was taken for a pool fault: %+v", st)
	}
}

// Held past the bound, the configuration lock is a wait (ErrSlotsHeld, the
// typed retryable answer), for urgent work and for a ticketed timer cut,
// which keeps its ticket.
func TestConfigurationLockHeldPastTheBoundIsAWait(t *testing.T) {
	c := testConfig(t)
	u, n := newTest(t, c), newTest(t, c)
	holdGuard(t, c.Directory)
	start := time.Now()
	if _, err := u.Acquire(context.Background(), Urgent, nil); !errors.Is(err, ErrSlotsHeld) {
		t.Fatalf("urgent admission behind a held configuration lock: %v; want ErrSlotsHeld", err)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("the urgent admission waited %s; want about %s", el, guardWait)
	}
	tk, err := n.TakeTicket(HostNow())
	if err != nil {
		t.Fatal(err)
	}
	defer tk.Close()
	if _, err := admitWith(n, tk); !errors.Is(err, ErrSlotsHeld) {
		t.Fatalf("ticketed admission behind a held configuration lock: %v; want ErrSlotsHeld", err)
	}
	if _, err := os.Stat(tk.path); err != nil {
		t.Fatalf("the waiting ticket lost its file: %v", err)
	}
	for _, l := range []*Limiter{u, n} {
		if st := l.Snapshot().(Stats); st.Fallback != 0 || st.LastPoolError != "" {
			t.Fatalf("a held configuration lock was taken for a pool fault: %+v", st)
		}
	}
}

// One stalled ticket costs one skip, not one per hand-off. Three live
// waiters behind it poll concurrently, as volumes on one host do, and keep
// polling while another holds the slot. After the first skip each hand-off
// takes at most a few polls, never another skip time, and the waiters are
// admitted in due order.
func TestOneStalledTicketCostsOneSkipNotEveryHandoff(t *testing.T) {
	c := testConfig(t)
	stall := newTest(t, c)
	now := HostNow()
	stalled, err := stall.TakeTicket(now - 10e9)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	const skip, cut, poll = 500 * time.Millisecond, 50 * time.Millisecond, 20 * time.Millisecond
	type admission struct {
		due      int64
		at, done time.Time
	}
	var mu sync.Mutex
	var log []admission
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		l := newTest(t, c)
		l.testSkipAfter = skip
		tk, err := l.TakeTicket(now - int64(9-i)*1e9)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { tk.Close() }()
			for {
				select {
				case <-stop:
					return
				case <-time.After(poll):
				}
				id, err := admitWith(l, tk)
				if err != nil {
					if !errors.Is(err, ErrSlotsHeld) {
						t.Errorf("refusal that is not a wait: %v", err)
						return
					}
					continue
				}
				at := time.Now()
				time.Sleep(cut)
				l.Release(id)
				mu.Lock()
				log = append(log, admission{tk.Due(), at, time.Now()})
				mu.Unlock()
				if tk, err = l.TakeTicket(HostNow()); err != nil { // the next cycle's cut
					t.Error(err)
					return
				}
			}
		}()
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(log)
		mu.Unlock()
		if n >= 6 || time.Now().After(deadline) {
			break
		}
		time.Sleep(poll)
	}
	close(stop)
	wg.Wait()
	if len(log) < 6 {
		t.Fatalf("only %d admissions in 10 s", len(log))
	}
	for i := 1; i < len(log); i++ {
		if log[i].due <= log[i-1].due {
			t.Fatalf("admissions out of due order at %d", i)
		}
		if gap := log[i].at.Sub(log[i-1].done); gap > 200*time.Millisecond {
			t.Fatalf("hand-off %d idled %s after the release; one stalled ticket must not cost every hand-off a skip (%s)", i, gap.Round(time.Millisecond), skip)
		}
	}
}

// A ticket never recreates a pool directory that was removed: that would
// start a second set of slots beside the holders of the old one.
func TestTakeTicketDoesNotRecreateARemovedPool(t *testing.T) {
	c := DefaultConfig(filepath.Join(t.TempDir(), "pool"))
	c.NormalSlots, c.UrgentSlots = 1, 1
	if err := os.Mkdir(c.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	l := newTest(t, c)
	tk, err := l.TakeTicket(HostNow())
	if err != nil {
		t.Fatal(err)
	}
	tk.Close()
	if err := os.RemoveAll(c.Directory); err != nil {
		t.Fatal(err)
	}
	if _, err := l.TakeTicket(HostNow()); err == nil {
		t.Fatal("a ticket was taken in a removed pool directory")
	}
	if _, err := os.Stat(c.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the removed pool directory came back: %v", err)
	}
}
