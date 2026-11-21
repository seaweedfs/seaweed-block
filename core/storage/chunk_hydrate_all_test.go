package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestHydrateAll_YieldsToGuestReadsAndNeverFetchesTwice(t *testing.T) {
	fastWalker(t)
	s, layer, remote, registered := hydrationFixture(t, 2, 64)
	entered, release := holdNextGet(t, remote)
	guest := startHydrationRead(t, s, context.Background(), 5*ctChunkBlocks)
	waitHydrationRegistration(t, registered, 1)
	<-entered // the guest's fetch of chunk 5 is held
	type res struct {
		p   HydrateProgress
		err error
	}
	walked := make(chan res, 1)
	go func() {
		p, err := layer.HydrateAll(context.Background())
		walked <- res{p, err}
	}()
	time.Sleep(30 * time.Millisecond)
	if n := len(countGets(remote)); n != 1 {
		t.Fatalf("the walker fetched while a guest was waiting: %d distinct gets", n)
	}
	release()
	if r := hydrationOutcome(t, guest); r.err != nil {
		t.Fatalf("guest read: %v", r.err)
	}
	var r res
	select {
	case r = <-walked:
	case <-time.After(ctLocalBound):
		t.Fatal("walker did not finish")
	}
	if r.err != nil || r.p.Remaining != 0 || r.p.Paused == 0 || r.p.Filled != ctNumBlocks/ctChunkBlocks-1 {
		t.Fatalf("walker: %+v %v, want paused for the guest and the 7 other chunks filled", r.p, r.err)
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
}

func (b *walkerBudget) Admit(ctx context.Context, _ int64, _ func(error)) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.classes = append(b.classes, ctx.Value(walkerClassKey{}))
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
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.admitted != p.Filled || budget.released != budget.admitted {
		t.Fatalf("budget admitted %d released %d, filled %d: every fill admitted once and released", budget.admitted, budget.released, p.Filled)
	}
	for _, c := range budget.classes {
		if c != "fill" {
			t.Fatalf("the caller's limiter class did not reach Admit: %v", c)
		}
	}
}

func TestHydrateAll_CancelReturnsTheRemainingCount(t *testing.T) {
	fastWalker(t)
	_, layer, remote, _ := hydrationFixture(t, 2, 64)
	entered, release := holdNextGet(t, remote)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan HydrateProgress, 1)
	go func() {
		p, _ := layer.HydrateAll(ctx)
		done <- p
	}()
	<-entered
	cancel()
	release()
	select {
	case p := <-done:
		if p.Remaining == 0 {
			t.Fatalf("cancelled walk reports nothing remaining: %+v", p)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("cancelled walker did not return")
	}
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
