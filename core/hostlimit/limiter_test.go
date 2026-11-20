package hostlimit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	c := DefaultConfig(t.TempDir())
	c.NormalSlots = 1
	c.UrgentSlots = 1
	return c
}
func newTest(t *testing.T, c Config) *Limiter {
	t.Helper()
	l, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		l.mu.Lock()
		ids := make([]uint64, 0, len(l.live))
		for id := range l.live {
			ids = append(ids, id)
		}
		l.mu.Unlock()
		for _, id := range ids {
			l.Release(id)
		}
		l.Close()
	})
	return l
}
func settled(t *testing.T, l *Limiter) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if l.Snapshot().(Stats).Active == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("release did not settle")
		case <-tick.C:
		}
	}
}
func TestSlotsUrgentAndJoinedRelease(t *testing.T) {
	c := testConfig(t)
	a, b, d := newTest(t, c), newTest(t, c), newTest(t, c)
	x, e := a.Admit(context.Background(), c.PayloadBytes, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Release(x)
	if _, e = b.Admit(context.Background(), 1, nil); !errors.Is(e, ErrBusy) {
		t.Fatalf("normal bypass: %v", e)
	}
	y, e := b.Admit(WithClass(context.Background(), Urgent), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Release(y)
	if _, e = d.Admit(WithClass(context.Background(), Urgent), 1, nil); !errors.Is(e, ErrBusy) {
		t.Fatalf("urgent bypass: %v", e)
	}
	a.Release(x)
	a.Release(x)
	settled(t, a)
	z, e := d.Admit(context.Background(), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	d.Release(z)
	settled(t, d)
}
func TestCOWAbortHoldsSlotUntilWorkerSettlement(t *testing.T) {
	c := testConfig(t)
	a, b := newTest(t, c), newTest(t, c)
	revoked := make(chan struct{}, 1)
	x, e := a.Admit(context.Background(), 1, func(error) { revoked <- struct{}{} })
	if e != nil {
		t.Fatal(e)
	}
	defer a.Release(x)
	if e = a.ReserveCOW(x, c.COWBytes); e != nil {
		t.Fatal(e)
	}
	if e = a.ReserveCOW(x, 1); !errors.Is(e, ErrRevoked) {
		t.Fatal(e)
	}
	select {
	case <-revoked:
	case <-time.After(time.Second):
		t.Fatal("no abort")
	}
	if _, e = b.Admit(context.Background(), 1, nil); !errors.Is(e, ErrBusy) {
		t.Fatalf("revocation released live I/O: %v", e)
	}
	a.Release(x)
	settled(t, a)
}
func TestHoldExpiryAndCloseDoNotSteal(t *testing.T) {
	c := testConfig(t)
	c.MaxHold = 30 * time.Millisecond
	a, b := newTest(t, c), newTest(t, c)
	revoked := make(chan struct{}, 4)
	x, e := a.Admit(context.Background(), 1, func(error) { revoked <- struct{}{} })
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-revoked:
	case <-time.After(time.Second):
		t.Fatal("no expiry")
	}
	done := make(chan struct{})
	go func() { a.Close(); close(done) }()
	select {
	case <-revoked:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel")
	}
	select {
	case <-done:
		t.Fatal("Close abandoned worker")
	default:
	}
	if _, e = b.Admit(context.Background(), 1, nil); !errors.Is(e, ErrBusy) {
		t.Fatal("lease stolen", e)
	}
	a.Release(x)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close not joined")
	}
}
func TestFallbackAndConfigMismatch(t *testing.T) {
	c := testConfig(t)
	a := newTest(t, c)
	x, e := a.Admit(context.Background(), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	a.Release(x)
	settled(t, a)
	a.Close()
	bad := c
	bad.NormalSlots++
	b := newTest(t, bad)
	if _, e = b.Admit(context.Background(), 1, nil); !errors.Is(e, ErrConfig) {
		t.Fatalf("config mismatch bypassed cap: %v", e)
	}
	c.Directory += "-missing"
	f := newTest(t, c)
	x, e = f.Admit(context.Background(), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	f.Release(x)
	settled(t, f)
	if !f.Snapshot().(Stats).FallbackActive {
		t.Fatal("degraded cap hidden")
	}
}
func TestRateCancellationAndPhase(t *testing.T) {
	c := testConfig(t)
	c.BytesPerSecond = 1024
	l := newTest(t, c)
	if e := l.WaitUpload(context.Background(), 2048); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e := l.WaitUpload(ctx, 2048); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		v := fmt.Sprint(i)
		p := Phase(v, time.Minute)
		if p < 0 || p >= time.Minute || p != Phase(v, time.Minute) {
			t.Fatal("bad phase")
		}
		seen[p] = true
	}
	if len(seen) < 190 {
		t.Fatal("phase collision", len(seen))
	}
}
func TestProcessSlotHelper(t *testing.T) {
	dir := os.Getenv("SWBLOCK_TEST_LIMITER_DIR")
	if dir == "" {
		return
	}
	c := DefaultConfig(dir)
	c.NormalSlots = 1
	c.UrgentSlots = 1
	l, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	_, e = l.Admit(context.Background(), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	fmt.Println("HELD")
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
}
func TestKilledProcessReleasesButLiveHolderDoesNot(t *testing.T) {
	c := testConfig(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessSlotHelper$")
	cmd.Env = append(os.Environ(), "SWBLOCK_TEST_LIMITER_DIR="+c.Directory)
	out, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	in, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	defer in.Close()
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	ready := make(chan bool, 1)
	go func() {
		s := bufio.NewScanner(out)
		for s.Scan() {
			if s.Text() == "HELD" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("child failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("child not ready")
	}
	l := newTest(t, c)
	if _, e = l.Admit(context.Background(), 1, nil); !errors.Is(e, ErrBusy) {
		t.Fatalf("live child not counted: %v", e)
	}
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = cmd.Wait()
	x, e := l.Admit(context.Background(), 1, nil)
	if e != nil {
		t.Fatal("dead child retained slot", e)
	}
	l.Release(x)
	settled(t, l)
}

func TestBackgroundPoolsDoNotBorrowCutReservations(t *testing.T) {
	c := testConfig(t)
	c.FillSlots = 1
	c.IdleSlots = 1
	l := newTest(t, c)
	x, e := l.Admit(context.Background(), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Release(x)
	y, e := l.Acquire(context.Background(), BackgroundFill, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Release(y)
	if _, e = l.Acquire(context.Background(), BackgroundFill, nil); !errors.Is(e, ErrBusy) {
		t.Fatal("fill exceeded independent pool", e)
	}
	z, e := l.Acquire(context.Background(), Idle, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Release(z)
	if e = l.ReserveCOW(y, 1); !errors.Is(e, ErrRevoked) {
		t.Fatal("fill consumed cut spill", e)
	}
}
