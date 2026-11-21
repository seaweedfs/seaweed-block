package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/hostlimit"
)

func countGets(r *fakeChunkRemote) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for _, op := range r.ops {
		if key, ok := strings.CutPrefix(op, "get:"); ok {
			out[key]++
		}
	}
	return out
}

func fastWalker(t *testing.T) {
	t.Helper()
	minB, maxB := hydrateAllMinBackoff, hydrateAllMaxBackoff
	hydrateAllMinBackoff, hydrateAllMaxBackoff = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { hydrateAllMinBackoff, hydrateAllMaxBackoff = minB, maxB })
}

// walkRun is one HydrateAll call on its own goroutine. Its ctx is cancelled
// and its return joined (bounded) at cleanup, registered before launch.
type walkRun struct {
	done   chan struct{}
	cancel context.CancelFunc
	p      HydrateProgress
	err    error
}

func startWalker(t *testing.T, layer *chunkLayerImpl, parent context.Context) *walkRun {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	w := &walkRun{done: make(chan struct{}), cancel: cancel}
	t.Cleanup(func() {
		cancel()
		select {
		case <-w.done:
		case <-time.After(ctLocalBound):
			t.Error("cleanup: walker did not return")
		}
	})
	go func() {
		w.p, w.err = layer.HydrateAll(ctx)
		close(w.done)
	}()
	return w
}

func (w *walkRun) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(ctLocalBound):
		t.Fatalf("%s: walker did not return", what)
	}
}

func walkerSignal[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(ctLocalBound):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitUntil polls cond (bounded by ctLocalBound); permits are released
// asynchronously, when each flight settles.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(ctLocalBound)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// fillLimiter is a real host limiter on pool dir with fillSlots fill permits.
func fillLimiter(t *testing.T, dir string, fillSlots int) *hostlimit.Limiter {
	t.Helper()
	c := hostlimit.DefaultConfig(dir)
	c.FillSlots = fillSlots
	l, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	return l
}

// payloadRecorder passes admissions to a real Budget and records each payload.
type payloadRecorder struct {
	Budget
	mu       sync.Mutex
	payloads []int64
}

func (r *payloadRecorder) Admit(ctx context.Context, payload int64, abort func(error)) (uint64, error) {
	r.mu.Lock()
	r.payloads = append(r.payloads, payload)
	r.mu.Unlock()
	return r.Budget.Admit(ctx, payload, abort)
}

func bgFill() context.Context {
	return hostlimit.WithClass(context.Background(), hostlimit.BackgroundFill)
}

func TestHydrateAll_FillsEveryChunkOnceAndReadsTheHeadImage(t *testing.T) {
	fastWalker(t)
	s, layer, remote, _ := hydrationFixture(t, 2, 64)
	ctx := context.Background()
	p, err := layer.HydrateAll(ctx)
	if err != nil || p.Remaining != 0 || p.Filled != ctNumBlocks/ctChunkBlocks {
		t.Fatalf("hydrate all: %+v %v, want %d filled, 0 remaining", p, err, ctNumBlocks/ctChunkBlocks)
	}
	for key, n := range countGets(remote) {
		if n != 1 {
			t.Fatalf("chunk %s fetched %d times, want exactly once", key, n)
		}
	}
	for b := uint32(0); b < ctNumBlocks; b++ {
		got, err := s.Read(b)
		if err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, byte(0x40+b))) {
			t.Fatalf("block %d after hydrate: %v", b, err)
		}
	}
	before := len(countGets(remote))
	if p, err := layer.HydrateAll(ctx); err != nil || p.Filled != 0 || p.Remaining != 0 || len(countGets(remote)) != before {
		t.Fatalf("a second walk of a full volume must do nothing: %+v %v", p, err)
	}
}

// A guest write that is still only in the WAL is not visible to the walker as
// local (localBlocks is set when the extent gets a local version), so the chunk
// may be fetched; the fill must never overwrite the local write (reads resolve
// the WAL first and the fill installs only blocks without a newer local version).
func TestHydrateAll_NeverOverwritesALocalWrite(t *testing.T) {
	fastWalker(t)
	s, layer, _, _ := hydrationFixture(t, 2, 64)
	for i := uint32(0); i < ctChunkBlocks; i++ { // chunk 0 fully overwritten
		if _, err := s.Write(i, makeBlock(ctBlockSize, 0xEE)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Write(ctChunkBlocks+1, makeBlock(ctBlockSize, 0xEF)); err != nil { // one block of chunk 1
		t.Fatal(err)
	}
	p, err := layer.HydrateAll(context.Background())
	if err != nil || p.Remaining != 0 {
		t.Fatalf("hydrate all: %+v %v", p, err)
	}
	for i := uint32(0); i < ctChunkBlocks; i++ {
		if got, _ := s.Read(i); !bytes.Equal(got, makeBlock(ctBlockSize, 0xEE)) {
			t.Fatalf("block %d: the local write was overwritten by the walker", i)
		}
	}
	if got, _ := s.Read(ctChunkBlocks + 1); !bytes.Equal(got, makeBlock(ctBlockSize, 0xEF)) {
		t.Fatal("partial local write in chunk 1 overwritten")
	}
	if got, _ := s.Read(ctChunkBlocks); !bytes.Equal(got, makeBlock(ctBlockSize, byte(0x40+ctChunkBlocks))) {
		t.Fatal("the unwritten blocks of chunk 1 must read HEAD's bytes after the walk")
	}
}

// The walker's deferral is witnessed (its pause hook fires) while the guest's
// fetch is held, before anything is allowed to progress.
func TestHydrateAll_YieldsToGuestReadsAndNeverFetchesTwice(t *testing.T) {
	fastWalker(t)
	s, layer, remote, registered := hydrationFixture(t, 2, 64)
	paused := make(chan struct{}, 1)
	layer.testHydrateAllPause = func() {
		select {
		case paused <- struct{}{}:
		default:
		}
	}
	entered, release := holdNextGet(t, remote)
	guest := startHydrationRead(t, s, context.Background(), 5*ctChunkBlocks)
	waitHydrationRegistration(t, registered, 1)
	walkerSignal(t, entered, "the guest's fetch of chunk 5")
	w := startWalker(t, layer, context.Background())
	walkerSignal(t, paused, "the walker to yield to the held guest fetch")
	if n := len(countGets(remote)); n != 1 {
		t.Fatalf("the walker fetched while a guest was waiting: %d distinct gets", n)
	}
	release()
	if r := hydrationOutcome(t, guest); r.err != nil {
		t.Fatalf("guest read: %v", r.err)
	}
	w.wait(t, "after the guest fill")
	if w.err != nil || w.p.Remaining != 0 || w.p.Paused == 0 || w.p.Filled != ctNumBlocks/ctChunkBlocks-1 {
		t.Fatalf("walker: %+v %v, want paused for the guest and the 7 other chunks filled", w.p, w.err)
	}
	for key, n := range countGets(remote) {
		if n != 1 {
			t.Fatalf("chunk %s fetched %d times", key, n)
		}
	}
}

type walkerClassKey struct{}

type walkerBudget struct {
	mu       sync.Mutex
	refuse   int
	admitted int
	released int
	classes  []any
	abort    func(error)
}

func (b *walkerBudget) Admit(ctx context.Context, _ int64, abort func(error)) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.classes = append(b.classes, ctx.Value(walkerClassKey{}))
	b.abort = abort
	if b.refuse > 0 {
		b.refuse--
		return 0, errors.New("fill class over budget")
	}
	b.admitted++
	return uint64(b.admitted), nil
}
func (b *walkerBudget) ReserveCOW(uint64, int64) error { return nil }
func (b *walkerBudget) Release(uint64) {
	b.mu.Lock()
	b.released++
	b.mu.Unlock()
}

func (b *walkerBudget) counts() (admitted, released int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.admitted, b.released
}

func TestHydrateAll_AdmitsEachFillThroughTheLimiterWithTheCallersClass(t *testing.T) {
	fastWalker(t)
	_, layer, _, _ := hydrationFixture(t, 2, 64)
	budget := &walkerBudget{refuse: 2}
	layer.cfg.Budget = budget
	ctx := context.WithValue(context.Background(), walkerClassKey{}, "fill")
	p, err := layer.HydrateAll(ctx)
	if err != nil || p.Remaining != 0 || p.Deferred != 2 {
		t.Fatalf("hydrate all: %+v %v, want 2 deferred then everything filled", p, err)
	}
	waitUntil(t, "every permit is released", func() bool { a, r := budget.counts(); return a == r })
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.admitted != p.Filled {
		t.Fatalf("budget admitted %d, filled %d: every fill admitted once", budget.admitted, p.Filled)
	}
	for _, c := range budget.classes {
		if c != "fill" {
			t.Fatalf("the caller's limiter class did not reach Admit: %v", c)
		}
	}
}

func TestHydrateAll_CancelReturnsWhileTheFetchIsHeld(t *testing.T) {
	fastWalker(t)
	_, layer, remote, _ := hydrationFixture(t, 2, 64)
	entered, _ := holdNextGet(t, remote)
	w := startWalker(t, layer, context.Background())
	walkerSignal(t, entered, "the walker's first fetch")
	w.cancel()
	w.wait(t, "cancel with the fetch still held")
	if !errors.Is(w.err, context.Canceled) || w.p.Remaining == 0 {
		t.Fatalf("cancelled walk: %+v %v, want context.Canceled and chunks remaining", w.p, w.err)
	}
}

// The real limiter refuses a background permit that carries a cut payload:
// every fill must be admitted as a fill permit (no payload) under
// BackgroundFill, fill the HEAD image exactly, and give every permit back.
func TestHydrateAll_RealBackgroundFillLimiterAdmitsEveryFill(t *testing.T) {
	fastWalker(t)
	s, layer, remote, _ := hydrationFixture(t, 2, 64)
	lim := fillLimiter(t, t.TempDir(), 2)
	rec := &payloadRecorder{Budget: lim}
	layer.cfg.Budget = rec
	ctx, cancel := context.WithTimeout(bgFill(), ctLocalBound)
	defer cancel()
	p, err := layer.HydrateAll(ctx)
	// A refusal is possible while an earlier permit is still being closed by
	// the limiter; it is a deferral, and the limiter counts it as a skip.
	if err != nil || p.Remaining != 0 || p.Filled != ctNumBlocks/ctChunkBlocks {
		t.Fatalf("hydrate all under the real limiter: %+v %v", p, err)
	}
	for b := uint32(0); b < ctNumBlocks; b++ {
		if got, err := s.Read(b); err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, byte(0x40+b))) {
			t.Fatalf("block %d after hydrate: %v", b, err)
		}
	}
	for key, n := range countGets(remote) {
		if n != 1 {
			t.Fatalf("chunk %s fetched %d times", key, n)
		}
	}
	rec.mu.Lock()
	for _, n := range rec.payloads {
		if n != 0 {
			t.Fatalf("fill admitted with a %d-byte cut payload", n)
		}
	}
	rec.mu.Unlock()
	waitUntil(t, "every fill permit is admitted once and released", func() bool {
		st := lim.Snapshot().(hostlimit.Stats)
		return st.Admitted == uint64(p.Filled) && st.Released == st.Admitted && st.Active == 0 && st.Skipped == p.Deferred
	})
}

// The permit follows the owned flight, not the walker's wait: a cancelled
// walker returns at once, but its fill permit stays taken until the held
// physical Get settles.
func TestHydrateAll_PermitIsHeldUntilTheOwnedFetchSettles(t *testing.T) {
	fastWalker(t)
	_, layer, remote, _ := hydrationFixture(t, 2, 64)
	dir := t.TempDir()
	layer.cfg.Budget = fillLimiter(t, dir, 1)
	contender := fillLimiter(t, dir, 1)
	entered, release := holdNextGet(t, remote)
	w := startWalker(t, layer, bgFill())
	walkerSignal(t, entered, "the walker's fetch")
	w.cancel()
	w.wait(t, "cancel with the fetch still held")
	if !errors.Is(w.err, context.Canceled) {
		t.Fatalf("cancelled walk: %v", w.err)
	}
	if tk, err := contender.Acquire(context.Background(), hostlimit.BackgroundFill, nil); err == nil {
		contender.Release(tk)
		t.Fatal("the fill permit was free while the walker's Get was still running")
	} else if !errors.Is(err, hostlimit.ErrBusy) {
		t.Fatalf("contender: %v", err)
	}
	release()
	waitUntil(t, "the permit is free after the Get settled", func() bool {
		tk, err := contender.Acquire(context.Background(), hostlimit.BackgroundFill, nil)
		if err == nil {
			contender.Release(tk)
		}
		return err == nil
	})
}

// A limiter revocation (the abort callback) stops the walker with the cause;
// the permit is still released only when the owned fetch settles.
func TestHydrateAll_RevocationStopsTheWalker(t *testing.T) {
	fastWalker(t)
	_, layer, remote, _ := hydrationFixture(t, 2, 64)
	budget := &walkerBudget{}
	layer.cfg.Budget = budget
	entered, release := holdNextGet(t, remote)
	w := startWalker(t, layer, context.Background())
	walkerSignal(t, entered, "the walker's fetch")
	budget.mu.Lock()
	abort := budget.abort
	budget.mu.Unlock()
	abort(hostlimit.ErrRevoked)
	w.wait(t, "revocation with the fetch still held")
	if !errors.Is(w.err, hostlimit.ErrRevoked) {
		t.Fatalf("revoked walk: %v, want ErrRevoked", w.err)
	}
	if _, r := budget.counts(); r != 0 {
		t.Fatal("permit released before the owned fetch settled")
	}
	release()
	waitUntil(t, "the permit is released after the fetch settled", func() bool { _, r := budget.counts(); return r == 1 })
}

func TestHydrateAll_StatsReportAbsentChunks(t *testing.T) {
	fastWalker(t)
	_, layer, _, _ := hydrationFixture(t, 2, 64)
	if n := layer.Stats().AbsentChunks; n != ctNumBlocks/ctChunkBlocks {
		t.Fatalf("absent before the walk: %d, want %d", n, ctNumBlocks/ctChunkBlocks)
	}
	if _, err := layer.HydrateAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := layer.Stats().AbsentChunks; n != 0 {
		t.Fatalf("absent after the walk: %d, want 0", n)
	}
}
