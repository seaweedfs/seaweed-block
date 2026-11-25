package chunkstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The holder's cleaning and a cut meet in one remote: a cut cancels the
// cleaning through the remote's state and waits for it to end. Both waits
// are bounded by the caller's context, so a stop on a hung filer never
// waits unbounded inside the guest freeze.

// A cleaning whose removal step ignores cancellation (a filer call that
// hangs): the cut's Begin cancels it and waits only until its own context
// ends, then reports the remote unavailable; it does not hang.
func TestLayoutRemote_ACutWaitsForAHungCleaningOnlyUntilItsContextEnds(t *testing.T) {
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	cleaned := make(chan error, 1)
	go func() {
		cleaned <- r.Clean(context.Background(), func(context.Context, layout.HolderID, string) error {
			close(started)
			<-release // ignores the cancellation, like a hung filer call
			return nil
		})
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := r.Begin(ctx); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, storage.ErrChunkRemoteUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Begin over a hung cleaning: %v; want remote unavailable (deadline exceeded)", err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Begin waited for a hung cleaning past its own context")
	}
	close(release)
	if err := <-cleaned; err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("the released cleaning ended with %v", err)
	}
}

// hungEndStore lets every HEAD update through except the cleaning's close,
// which hangs until the caller's context ends (a filer that stops answering).
type hungEndStore struct{ sessionStore }

func (s hungEndStore) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	cur, _, err := s.sessionStore.ReadHead(ctx)
	if err == nil && cur.Activity.Kind == layout.ActClean && next.Activity.Kind == layout.ActIdle {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.sessionStore.UpdateHead(ctx, gen, next)
}

// The cleaning's close (EndClean) on a store that stops answering ends with
// the caller's context instead of hanging; the cleaning left open on HEAD is
// ended by the next cut.
func TestLayoutRemote_TheCleaningsCloseIsBoundedByTheCallersContext(t *testing.T) {
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	orig := r.st
	r.st = hungEndStore{orig}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- r.Clean(ctx, func(context.Context, layout.HolderID, string) error { return nil })
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cleaning whose close never landed reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cleaning's close waited past the caller's context")
	}
	if h := lh.head(); h.Activity.Kind != layout.ActClean {
		t.Fatalf("HEAD after the unanswered close: %+v; want the cleaning still open", h.Activity)
	}
	// the next cut ends the cleaning left open and opens its own session
	r.st = orig
	if _, err := r.Begin(context.Background()); err != nil {
		t.Fatalf("the next cut after an unended cleaning: %v", err)
	}
	if h := lh.head(); h.Activity.Kind != layout.ActSnap {
		t.Fatalf("HEAD after the next cut's Begin: %+v; want its session open", h.Activity)
	}
}
