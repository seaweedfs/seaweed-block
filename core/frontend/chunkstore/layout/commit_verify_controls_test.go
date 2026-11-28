package layout

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedObjects runs a per-object script around each Stat and counts the
// lookups in flight. Stat honours its context, as the production store does.
type scriptedObjects struct {
	*memObjects
	active, peak, started atomic.Int64
	script                func(ctx context.Context, rel string) error // nil: answer from the store
}

func (s *scriptedObjects) Stat(ctx context.Context, rel string) (ObjectMeta, bool, error) {
	s.started.Add(1)
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	if s.script != nil {
		if err := s.script(ctx, rel); err != nil {
			return ObjectMeta{}, false, err
		}
	}
	return s.memObjects.Stat(ctx, rel)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// The check never has more than commitVerifyParallel lookups in flight:
// every lookup is held until the test sees the cap reached, and no further
// lookup may start while they are held.
func TestCommitLookupsNeverExceedTheCap(t *testing.T) {
	f, st, req := firstCommitOf(t, 500, "s1")
	release := make(chan struct{})
	so := &scriptedObjects{memObjects: f, script: func(ctx context.Context, _ string) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	done := make(chan error, 1)
	go func() { _, err := RunCommit(context.Background(), st, so, "v1", me, req); done <- err }()
	waitFor(t, "the cap to be reached", func() bool { return so.active.Load() >= commitVerifyParallel })
	time.Sleep(50 * time.Millisecond) // a 33rd lookup would start here if admission did not wait
	if a, s := so.active.Load(), so.started.Load(); a != commitVerifyParallel || s != commitVerifyParallel {
		t.Fatalf("while %d lookups are held: %d in flight, %d started; want exactly %d of each", commitVerifyParallel, a, s, commitVerifyParallel)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if p := so.peak.Load(); p > commitVerifyParallel {
		t.Fatalf("peak %d lookups in flight; the cap is %d", p, commitVerifyParallel)
	}
}

// A cancelled commit returns within a bound, with every admitted lookup
// joined (none still running), no further lookup started, and HEAD not
// written: before any lookup, and with the cap's worth of lookups in flight.
func TestACancelledCommitStopsJoinsAndWritesNothing(t *testing.T) {
	for _, mid := range []bool{false, true} {
		name := map[bool]string{false: "before any lookup", true: "mid-flight"}[mid]
		t.Run(name, func(t *testing.T) {
			f, st, req := firstCommitOf(t, 500, "s1")
			before := *st.h
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			so := &scriptedObjects{memObjects: f, script: func(ctx context.Context, _ string) error {
				<-ctx.Done() // a lookup that answers only when cancelled
				return ctx.Err()
			}}
			if !mid {
				cancel()
			}
			done := make(chan error, 1)
			start := time.Now()
			go func() { _, err := RunCommit(ctx, st, so, "v1", me, req); done <- err }()
			if mid {
				waitFor(t, "lookups in flight", func() bool { return so.active.Load() == commitVerifyParallel })
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled commit: %v; want context.Canceled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a cancelled commit did not return within 5 s")
			}
			if a := so.active.Load(); a != 0 {
				t.Fatalf("%d lookups still running after the commit returned; every admitted one must be joined", a)
			}
			if s := so.started.Load(); s > 2*commitVerifyParallel {
				t.Fatalf("%d lookups started after the cancellation; admission must stop (at most about one cap's worth)", s)
			}
			if st.h.Gen != before.Gen || st.h.Current != nil {
				t.Fatalf("HEAD written by a cancelled commit: %+v", *st.h)
			}
			t.Logf("returned after %v, %d lookups started", time.Since(start).Round(time.Millisecond), so.started.Load())
		})
	}
}

// Of two failures, the one at the earlier position decides the error, even
// when the later one finishes first: a slow lookup error at an early chunk
// against a fast "not prepared" at a later one, and the reverse.
func TestTheEarliestFailureByPositionDecidesTheError(t *testing.T) {
	errSlow := errors.New("slow filer error")
	for _, early := range []string{"lookup error", "not prepared"} {
		t.Run(early+" first", func(t *testing.T) {
			f, st, req := firstCommitOf(t, 400, "s1")
			earlyRel, lateRel := req.Next[40].Chunk.Path(), req.Next[300].Chunk.Path()
			var lateDone sync.WaitGroup
			lateDone.Add(1)
			so := &scriptedObjects{memObjects: f, script: func(ctx context.Context, rel string) error {
				switch rel {
				case earlyRel:
					lateDone.Wait() // the later failure has already completed
					if early == "lookup error" {
						return errSlow
					}
				case lateRel:
					defer lateDone.Done()
					if early == "lookup error" {
						return nil // reported "not prepared" below: it was removed
					}
					return errSlow
				}
				return nil
			}}
			if early == "lookup error" {
				delete(f.objs, f.full(lateRel))
			} else {
				delete(f.objs, f.full(earlyRel))
			}
			before := *st.h
			_, err := RunCommit(context.Background(), st, so, "v1", me, req)
			switch early {
			case "lookup error":
				if !errors.Is(err, errSlow) {
					t.Fatalf("error %v; want the earlier lookup error (index 40), not the later not-prepared", err)
				}
			case "not prepared":
				if !errors.Is(err, ErrNotPrepared) || errors.Is(err, errSlow) {
					t.Fatalf("error %v; want the earlier not-prepared (index 40), not the later lookup error", err)
				}
			}
			if st.h.Gen != before.Gen {
				t.Fatal("HEAD written by a refused commit")
			}
		})
	}
}
