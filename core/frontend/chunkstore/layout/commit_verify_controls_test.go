package layout

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The contract these controls pin is the literal 32: a change of the
// production constant must fail them, so it is not read from the code.
const wantCap = 32

const controlBound = 5 * time.Second

// scriptedObjects runs a per-object script around each Stat and counts the
// lookups in flight. Stat honours its context, as the production store does.
// returned is told after a lookup has fully returned (its count released).
type scriptedObjects struct {
	*memObjects
	active, peak, started atomic.Int64
	script                func(ctx context.Context, rel string) error // nil: answer from the store
	returned              func(rel string)
}

func (s *scriptedObjects) Stat(ctx context.Context, rel string) (ObjectMeta, bool, error) {
	s.started.Add(1)
	n := s.active.Add(1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	meta, ok, err := s.stat(ctx, rel)
	s.active.Add(-1)
	if s.returned != nil {
		s.returned(rel)
	}
	return meta, ok, err
}

func (s *scriptedObjects) stat(ctx context.Context, rel string) (ObjectMeta, bool, error) {
	if s.script != nil {
		if err := s.script(ctx, rel); err != nil {
			return ObjectMeta{}, false, err
		}
	}
	return s.memObjects.Stat(ctx, rel)
}

// gate is a one-shot release that a test always opens at cleanup, so no
// held lookup outlives a failing control.
type gate struct {
	once sync.Once
	ch   chan struct{}
}

func newGate(t *testing.T) *gate {
	g := &gate{ch: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *gate) open() { g.once.Do(func() { close(g.ch) }) }

// wait blocks until the gate opens, failing the lookup at controlBound so a
// broken schedule ends instead of hanging.
func (g *gate) wait() error {
	select {
	case <-g.ch:
		return nil
	case <-time.After(controlBound):
		return errors.New("control gate never opened")
	}
}

// startCommit runs RunCommit in the background with a bounded context; its
// cleanup opens the given gates, cancels the commit and joins it, so a
// failing control returns promptly and strands nothing. The result arrives
// once on the returned channel.
func startCommit(t *testing.T, parent context.Context, st HeadStore, os ObjectStore, req CommitRequest, gates ...*gate) <-chan error {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*controlBound)
	finished := make(chan struct{})
	out := make(chan error, 1)
	go func() {
		_, err := RunCommit(ctx, st, os, "v1", me, req)
		out <- err
		close(finished)
	}()
	t.Cleanup(func() {
		for _, g := range gates {
			g.open()
		}
		cancel()
		select {
		case <-finished:
		case <-time.After(2 * controlBound):
			t.Errorf("the commit did not return after cleanup")
		}
	})
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(controlBound)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func resultWithin(t *testing.T, done <-chan error, d time.Duration, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("%s did not return within %v", what, d)
		return nil
	}
}

func headUpdates(st *memHead) (int, uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.updates, st.h.Gen
}

// The check never has more than 32 lookups in flight: every lookup is held
// until 32 are in flight, and no further one may start while they are held.
func TestCommitLookupsNeverExceedTheCap(t *testing.T) {
	f, st, req := firstCommitOf(t, 500, "s1")
	release := newGate(t)
	so := &scriptedObjects{memObjects: f, script: func(context.Context, string) error { return release.wait() }}
	done := startCommit(t, context.Background(), st, so, req, release)
	waitFor(t, "32 lookups in flight", func() bool { return so.active.Load() >= wantCap })
	time.Sleep(50 * time.Millisecond) // a 33rd lookup would start here if admission did not wait
	if a, s := so.active.Load(), so.started.Load(); a != wantCap || s != wantCap {
		t.Fatalf("while lookups are held: %d in flight, %d started; want exactly %d of each", a, s, wantCap)
	}
	release.open()
	if err := resultWithin(t, done, controlBound, "the commit"); err != nil {
		t.Fatal(err)
	}
	if p := so.peak.Load(); p > wantCap {
		t.Fatalf("peak %d lookups in flight; the cap is %d", p, wantCap)
	}
}

// A cancelled commit joins every admitted lookup before it returns, starts
// no further lookups, and writes nothing. The admitted lookups are held past
// the cancellation until the test opens their exit, so a commit that
// returned without joining them is caught while they are still running.
func TestACancelledCommitStopsJoinsAndWritesNothing(t *testing.T) {
	for _, mid := range []bool{false, true} {
		name := map[bool]string{false: "before any lookup", true: "mid-flight"}[mid]
		t.Run(name, func(t *testing.T) {
			f, st, req := firstCommitOf(t, 500, "s1")
			before := *st.h
			exit := newGate(t)
			so := &scriptedObjects{memObjects: f, script: func(ctx context.Context, _ string) error {
				select { // a lookup that answers only when cancelled
				case <-ctx.Done():
				case <-time.After(controlBound):
					return errors.New("never cancelled")
				}
				if err := exit.wait(); err != nil {
					return err
				}
				return ctx.Err()
			}}
			parent, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			if !mid {
				cancelParent()
			}
			done := startCommit(t, parent, st, so, req, exit)
			if mid {
				waitFor(t, "32 lookups in flight", func() bool { return so.active.Load() >= wantCap })
				cancelParent()
			} else {
				waitFor(t, "a lookup in flight", func() bool { return so.active.Load() > 0 })
			}
			select {
			case err := <-done:
				t.Fatalf("the commit returned (%v) while %d admitted lookups were still running: they were not joined", err, so.active.Load())
			case <-time.After(100 * time.Millisecond):
			}
			exit.open()
			err := resultWithin(t, done, controlBound, "the cancelled commit")
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled commit: %v; want context.Canceled", err)
			}
			if a := so.active.Load(); a != 0 {
				t.Fatalf("%d lookups still running after the commit returned", a)
			}
			if s := so.started.Load(); s > 2*wantCap {
				t.Fatalf("%d lookups started after the cancellation; admission must stop (at most about one cap's worth)", s)
			}
			if updates, gen := headUpdates(st); updates != 0 || gen != before.Gen {
				t.Fatalf("HEAD updated by a cancelled commit: %d updates, gen %d (was %d)", updates, gen, before.Gen)
			}
		})
	}
}

// Of two failures, the one at the earlier position decides the error. The
// earlier lookup is held until the later failure has fully returned and no
// other lookup is in flight, so the later failure is the first to finish.
func TestTheEarliestFailureByPositionDecidesTheError(t *testing.T) {
	errFiler := errors.New("filer error")
	for _, early := range []string{"lookup error", "not prepared"} {
		t.Run(early+" first", func(t *testing.T) {
			f, st, req := firstCommitOf(t, 400, "s1")
			earlyRel, lateRel := req.Next[40].Chunk.Path(), req.Next[300].Chunk.Path()
			lateBack := newGate(t)
			so := &scriptedObjects{memObjects: f}
			so.returned = func(rel string) {
				if rel == lateRel {
					lateBack.open()
				}
			}
			so.script = func(_ context.Context, rel string) error {
				switch rel {
				case earlyRel:
					if err := lateBack.wait(); err != nil {
						return err // the later lookup never ran while this one was held
					}
					deadline := time.Now().Add(controlBound)
					for so.active.Load() > 1 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					time.Sleep(20 * time.Millisecond) // let the later result be recorded
					if early == "lookup error" {
						return errFiler
					}
				case lateRel:
					if early == "not prepared" {
						return errFiler
					}
				}
				return nil
			}
			if early == "lookup error" {
				delete(f.objs, f.full(lateRel))
			} else {
				delete(f.objs, f.full(earlyRel))
			}
			done := startCommit(t, context.Background(), st, so, req, lateBack)
			err := resultWithin(t, done, 2*controlBound, "the commit")
			switch early {
			case "lookup error":
				if !errors.Is(err, errFiler) {
					t.Fatalf("error %v; want the earlier lookup error (index 40), not the later not-prepared", err)
				}
			case "not prepared":
				if !errors.Is(err, ErrNotPrepared) || errors.Is(err, errFiler) {
					t.Fatalf("error %v; want the earlier not-prepared (index 40), not the later lookup error", err)
				}
			}
			if updates, _ := headUpdates(st); updates != 0 {
				t.Fatalf("HEAD updated %d times by a refused commit", updates)
			}
		})
	}
}
