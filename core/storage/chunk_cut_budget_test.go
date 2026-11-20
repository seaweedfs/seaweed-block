package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
)

var errTestCutBudget = errors.New("test cut budget refusal")

type testCutBudget struct {
	mu                         sync.Mutex
	rejectAdmission, rejectCOW bool
	admitted, released         int
	payload, saved             int64
	abort                      func(error)
	probe                      func()
}

func (b *testCutBudget) Admit(_ context.Context, payload int64, abort func(error)) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rejectAdmission {
		return 0, errTestCutBudget
	}
	b.admitted++
	b.payload = payload
	b.abort = abort
	return 17, nil
}
func (b *testCutBudget) ReserveCOW(ticket uint64, bytes int64) error {
	if b.probe != nil {
		b.probe()
	}
	b.mu.Lock()
	b.saved += bytes
	reject, abort := b.rejectCOW, b.abort
	b.mu.Unlock()
	if reject {
		abort(errTestCutBudget)
		return errTestCutBudget
	}
	return nil
}
func (b *testCutBudget) Release(ticket uint64) { b.mu.Lock(); b.released++; b.mu.Unlock() }

func TestCutBudgetRefusesBeforeBarrier(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	gate.unblock()
	b := &testCutBudget{rejectAdmission: true}
	l.cfg.Budget = b
	before := f.stats().head.Gen
	barriers := 0
	l.testAfterBarrierSyncHook = func(uint64) { barriers++ }
	if _, err := l.Cut(ctTimeout(t)); !errors.Is(err, errTestCutBudget) {
		t.Fatalf("admission: %v", err)
	}
	if barriers != 0 || f.stats().head.Gen != before {
		t.Fatal("refused budget reached barrier or published")
	}
	if _, err := s.Write(5, makeBlock(ctBlockSize, 0xee)); err != nil {
		t.Fatal("budget refused guest write:", err)
	}
}

func TestCutBudgetCOWRefusalAbortsOnlyCut(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	b := &testCutBudget{rejectCOW: true}
	l.cfg.Budget = b
	b.probe = func() {
		w := l.cowActive.Load()
		if !w.mu.TryLock() {
			t.Fatal("budget called holding COW mutex")
		}
		w.mu.Unlock()
	}
	before := f.stats().head.Gen
	done, errs := cowStartCut(t, l, gate)
	if _, err := s.Write(5, makeBlock(ctBlockSize, 0xee)); err != nil {
		t.Fatal("guest write:", err)
	}
	gate.unblock()
	cowWaitSignal(t, done, "budget-aborted cut")
	if err := <-errs; !errors.Is(err, errTestCutBudget) {
		t.Fatalf("cut: %v", err)
	}
	if f.stats().head.Gen != before {
		t.Fatal("budget-aborted point published")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.admitted != 1 || b.released != 1 || b.payload != l.cfg.PayloadMemoryBudget || b.saved != ctChunkBytes {
		t.Fatalf("budget lifecycle: %+v", b)
	}
}
