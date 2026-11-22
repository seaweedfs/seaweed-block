package hostlimit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
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
	// A free normal slot is not taken while the urgent cut runs.
	if _, e = d.Admit(context.Background(), 1, nil); !errors.Is(e, ErrUrgentPending) {
		t.Fatalf("normal admission during an urgent cut: %v", e)
	}
	b.Release(y)
	settled(t, b)
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
	for _, class := range []Class{Normal, Urgent} {
		if _, e = b.Admit(WithClass(context.Background(), class), 1, nil); !errors.Is(e, ErrConfig) {
			t.Fatalf("config mismatch bypassed cap: class=%d err=%v", class, e)
		}
	}
	if st := b.Snapshot().(Stats); st.LastPoolError == "" || st.Skipped != 2 || st.Fallback != 0 {
		t.Fatal("configuration refusal was silent", st)
	}
	c.Directory += "-missing"
	f := newTest(t, c)
	x, e = f.Admit(WithClass(context.Background(), Urgent), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	if !f.Snapshot().(Stats).FallbackActive {
		t.Fatal("degraded cap hidden")
	}
	f.Release(x)
	settled(t, f)
	if st := f.Snapshot().(Stats); st.FallbackActive || st.Fallback != 1 {
		t.Fatalf("settled fallback status: %+v", st)
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
	if os.Getenv("SWBLOCK_TEST_LIMITER_INIT_GATE") == "1" {
		l.testBeforeConfigWrite = func() { fmt.Println("CONFIG-WRITE"); _, _ = bufio.NewReader(os.Stdin).ReadByte() }
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
	case <-time.After(15 * time.Second):
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

type heldLog struct {
	entered, release chan struct{}
	once             sync.Once
}

func (w *heldLog) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}
func TestFallbackLogDoesNotHoldCOWAccountingLock(t *testing.T) {
	c := testConfig(t)
	c.Directory += "-missing"
	l := newTest(t, c)
	gate := &heldLog{entered: make(chan struct{}), release: make(chan struct{})}
	before := log.Writer()
	log.SetOutput(gate)
	defer log.SetOutput(before)
	defer close(gate.release)
	done := make(chan struct{})
	go func() { _, _ = l.Admit(WithClass(context.Background(), Urgent), 1, nil); close(done) }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fallback log not reached")
	}
	if !l.mu.TryLock() {
		t.Fatal("blocked log holds guest COW accounting lock")
	}
	l.mu.Unlock()
}

func TestLastFallbackReleaseClearsActiveStatus(t *testing.T) {
	c := testConfig(t)
	c.Directory += "-missing"
	l := newTest(t, c)
	id, err := l.Acquire(context.Background(), Urgent, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !l.Snapshot().(Stats).FallbackActive {
		t.Fatal("fallback not reported while held")
	}
	l.Release(id)
	settled(t, l)
	st := l.Snapshot().(Stats)
	if st.FallbackActive || st.PayloadReserved != 0 || st.COWReserved != 0 || st.Released != 1 {
		t.Fatalf("last fallback release left stale status: %+v", st)
	}
}

func TestConfigurationLockFailurePreservesPoolPolicy(t *testing.T) {
	for _, class := range []Class{Normal, Urgent} {
		t.Run(fmt.Sprint(class), func(t *testing.T) {
			l := newTest(t, testConfig(t))
			var lockErr error
			l.testBeforeConfigLock = func(f *os.File) {
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				lockErr = tryLock(f)
			}
			id, err := l.Acquire(context.Background(), class, nil)
			if lockErr == nil || errors.Is(lockErr, ErrBusy) {
				t.Fatalf("fault did not cause non-contention lock error: %v", lockErr)
			}
			st := l.Snapshot().(Stats)
			if class == Normal {
				if id != 0 || !errors.Is(err, ErrBusy) || !errors.Is(err, lockErr) || st.SkippedByClass[Normal] != 1 || st.Fallback != 0 {
					t.Fatalf("configuration lock fault lost cause: id=%d err=%v stats=%+v", id, err, st)
				}
			} else {
				if err != nil || id == 0 || st.Fallback != 1 || !st.FallbackActive {
					t.Fatalf("configuration lock fault refused urgent fallback: id=%d err=%v stats=%+v", id, err, st)
				}
				l.Release(id)
			}
			if st.LastPoolError != lockErr.Error() {
				t.Fatalf("configuration lock alarm lost cause: got %q want %q", st.LastPoolError, lockErr)
			}
		})
	}
}

func TestConfigurationLockContentionRefusesAllClasses(t *testing.T) {
	c := testConfig(t)
	f, err := os.OpenFile(filepath.Join(c.Directory, "configuration.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = tryLock(f); err != nil {
		t.Fatal(err)
	}
	for _, class := range []Class{Normal, Urgent} {
		l := newTest(t, c)
		id, err := l.Acquire(context.Background(), class, nil)
		st := l.Snapshot().(Stats)
		if id != 0 || !errors.Is(err, ErrBusy) || st.Fallback != 0 || st.FallbackActive || st.LastPoolError != "" || st.SkippedByClass[class] != 1 {
			t.Fatalf("configuration contention bypassed cap: class=%d id=%d err=%v stats=%+v", class, id, err, st)
		}
	}
}

func TestHealthyAdmissionDoesNotHideOlderFallbackLease(t *testing.T) {
	c := testConfig(t)
	c.Directory = filepath.Join(c.Directory, "late-pool")
	l := newTest(t, c)
	x, e := l.Admit(WithClass(context.Background(), Urgent), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(c.Directory, 0700); e != nil {
		t.Fatal(e)
	}
	y, e := l.Acquire(context.Background(), BackgroundFill, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Release(y)
	if !l.Snapshot().(Stats).FallbackActive {
		t.Fatal("healthy admission hid an active uncoordinated lease")
	}
	l.Release(x)
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for l.Snapshot().(Stats).Active != 1 {
		select {
		case <-deadline:
			t.Fatal("fallback lease not released")
		case <-tick.C:
		}
	}
	if l.Snapshot().(Stats).FallbackActive {
		t.Fatal("settled fallback lease still degrades healthy owner")
	}
}

func TestKilledInitializerDoesNotPublishPartialConfiguration(t *testing.T) {
	c := testConfig(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessSlotHelper$")
	cmd.Env = append(os.Environ(), "SWBLOCK_TEST_LIMITER_DIR="+c.Directory, "SWBLOCK_TEST_LIMITER_INIT_GATE=1")
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
			if s.Text() == "CONFIG-WRITE" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("initializer did not reach write boundary")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("initializer boundary not reached")
	}
	if _, e = os.Stat(filepath.Join(c.Directory, "configuration.json")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("unpublished configuration became visible", e)
	}
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = cmd.Wait()
	l := newTest(t, c)
	x, e := l.Admit(context.Background(), 1, nil)
	if e != nil {
		t.Fatal("successor did not recover interrupted initialization", e)
	}
	defer l.Release(x)
	if l.Snapshot().(Stats).FallbackActive {
		t.Fatal("successor silently bypassed shared cap")
	}
}

func TestUnusablePoolClassPolicy(t *testing.T) {
	for _, class := range []Class{Normal, Urgent, BackgroundFill, Idle} {
		t.Run(fmt.Sprint(class), func(t *testing.T) {
			c := testConfig(t)
			c.Directory += "-missing"
			l := newTest(t, c)
			id, err := l.Acquire(context.Background(), class, nil)
			st := l.Snapshot().(Stats)
			if class == Urgent {
				if err != nil || st.Fallback != 1 || !st.FallbackActive || st.LastPoolError == "" {
					t.Fatalf("urgent fallback: id=%d err=%v stats=%+v", id, err, st)
				}
				l.Release(id)
			} else if !errors.Is(err, ErrBusy) || id != 0 || st.Admitted != 0 || st.SkippedByClass[class] != 1 || st.Fallback != 0 || st.LastPoolError == "" {
				t.Fatalf("background bypassed unavailable pool: class=%d id=%d err=%v stats=%+v", class, id, err, st)
			}
		})
	}
}

func TestPacingDebtSurvivesOwnLeaseTurnover(t *testing.T) {
	c := testConfig(t)
	c.BytesPerSecond = 2 // one byte/second per coordinated cut slot
	l := newTest(t, c)
	first, err := l.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.WaitUpload(context.Background(), 4<<20); err != nil {
		t.Fatal(err)
	}
	l.Release(first)
	settled(t, l)
	next, err := l.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release(next)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = l.WaitUpload(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same-process debt reset at lease turnover: %v", err)
	}
}

func TestPacingProcessHelper(t *testing.T) {
	dir := os.Getenv("SWBLOCK_TEST_PACING_DIR")
	if dir == "" {
		return
	}
	c := DefaultConfig(dir)
	c.NormalSlots, c.UrgentSlots, c.BytesPerSecond = 1, 1, 2
	l, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	id, err := l.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { l.Release(id); l.Close() }()
	// Start the pacing budget after process startup and slot admission.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err = l.WaitUpload(ctx, 4<<20); err != nil {
		t.Fatalf("fresh process inherited another process's pacing debt: %v", err)
	}
	fmt.Println("FRESH-PACING 4194304")
}

func TestPacingFreshProcessesDoNotShareDebt(t *testing.T) {
	dir := t.TempDir()
	const starts = 6
	start := time.Now()
	for i := 0; i < starts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPacingProcessHelper$")
		cmd.Env = append(os.Environ(), "SWBLOCK_TEST_PACING_DIR="+dir)
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil || !strings.Contains(string(out), "FRESH-PACING 4194304") {
			t.Fatalf("fresh holder %d: %v\n%s", i, err, out)
		}
	}
	// Each new process gets an immediate first reservation. Slot reuse does
	// not preserve pacing debt, so these bytes cannot certify a host-rate cap.
	t.Logf("fresh_processes=%d attempted_payload_bytes=%d elapsed=%s", starts, starts*(4<<20), time.Since(start))
}

// A limiter closed while an admission is acquiring its pool slot refuses
// it as revoked: closing is terminal, not a held-slot wait.
func TestAdmitClosedDuringPoolAcquisitionIsRevoked(t *testing.T) {
	l, err := New(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Error("Close did not return within 5 s")
		}
	})
	l.testBeforeConfigLock = func(*os.File) {
		go func() { l.Close(); close(closed) }()
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
			l.mu.Lock()
			c := l.closed
			l.mu.Unlock()
			if c {
				return
			}
			if time.Now().After(deadline) {
				t.Error("Close did not mark the limiter closed within 5 s")
				return
			}
		}
	}
	id, err := l.Acquire(context.Background(), Normal, nil)
	if id != 0 || !errors.Is(err, ErrRevoked) || errors.Is(err, ErrSlotsHeld) {
		t.Fatalf("admission racing Close: id=%d err=%v; want ErrRevoked", id, err)
	}
}
