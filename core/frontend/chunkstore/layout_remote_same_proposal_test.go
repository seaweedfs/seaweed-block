package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A commit whose reply is lost is settled by reading HEAD back: the engine
// asks "is HEAD the image I proposed?". The layout keeps no chunk birth, so
// the answer must come from identity and image only (epoch, holder,
// position, detached state; per chunk key, hash, encoding). A chunk whose
// birth is below the cut position, or one inherited from an earlier cut,
// is the same image whatever birth the adapter reports: no fence.

// lostThenDownStore loses the reply of the next commit (it applies) and,
// when down is set, then fails every HEAD read until it is brought up: the
// lost commit stays unresolved in this process.
type lostThenDownStore struct {
	sessionStore
	armed, down, lost bool
}

func (s *lostThenDownStore) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	err := s.sessionStore.UpdateHead(ctx, gen, next)
	if err == nil && s.armed && next.Activity.Kind == layout.ActIdle {
		s.armed, s.lost = false, true
		return errors.New("reply lost")
	}
	return err
}

func (s *lostThenDownStore) ReadHead(ctx context.Context) (layout.Head, bool, error) {
	if s.lost && s.down {
		return layout.Head{}, false, errors.New("store unreachable")
	}
	return s.sessionStore.ReadHead(ctx)
}

func requireNotFenced(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v (fenced=%t); want the lost commit settled as this proposal", what, err, errors.Is(err, storage.ErrChunkLayerFenced))
	}
}

// Cut 0: two chunks written at different LSNs, so the first chunk's birth
// is below the cut position.
func TestLayoutRemote_LostReplyOfTheFirstCutIsNotAFence(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	lost := &lostThenDownStore{sessionStore: r.st, armed: true}
	r.st = lost
	want := map[uint32][]byte{3: block(3)}
	writeSync(t, s, want)
	want[30] = block(30)
	writeSync(t, s, map[uint32][]byte{30: want[30]})
	_, err := l.Capture(ctx)
	if !lost.lost {
		t.Fatal("the fault never fired")
	}
	requireNotFenced(t, err, "cut 0 with its reply lost")
	want[50] = block(50)
	writeSync(t, s, map[uint32][]byte{50: want[50]})
	_, err = l.Capture(ctx)
	requireNotFenced(t, err, "the cut after")
	_ = s.Close()
	lh.readBack("b", want)
}

// Cut 1: one chunk changes, the other is inherited from cut 0.
func TestLayoutRemote_LostReplyOfALaterCutWithAnInheritedChunkIsNotAFence(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	want := map[uint32][]byte{3: block(3)}
	writeSync(t, s, want)
	if _, err := l.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	lost := &lostThenDownStore{sessionStore: r.st, armed: true}
	r.st = lost
	want[30] = block(30)
	writeSync(t, s, map[uint32][]byte{30: want[30]})
	_, err := l.Capture(ctx)
	if !lost.lost {
		t.Fatal("the fault never fired")
	}
	requireNotFenced(t, err, "cut 1 with its reply lost")
	_ = s.Close()
	lh.readBack("b", want)
}

// A cut whose image is unchanged (a write that restores the same bytes
// moves only the position): the same proposal, no fence.
func TestLayoutRemote_LostReplyOfANoOpCutIsNotAFence(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	want := map[uint32][]byte{3: block(3), 30: block(30)}
	writeSync(t, s, map[uint32][]byte{3: want[3]})
	writeSync(t, s, map[uint32][]byte{30: want[30]})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	lost := &lostThenDownStore{sessionStore: r.st, armed: true}
	r.st = lost
	writeSync(t, s, map[uint32][]byte{3: want[3]}) // same bytes again
	_, err := l.Capture(ctx)
	if !lost.lost {
		t.Fatal("the fault never fired: the no-op cut committed nothing")
	}
	requireNotFenced(t, err, "a no-op cut with its reply lost")
	_ = s.Close()
	lh.readBack("b", want)
}

// The same across a process restart: the commit applied, its reply was
// lost and HEAD could not be read back, so the process ends with the
// commit unresolved. The restarted process starts on that HEAD and cuts.
func TestLayoutRemote_LostReplyResolvedAcrossAProcessRestart(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	want := map[uint32][]byte{3: block(3)}
	writeSync(t, s, want)
	if _, err := l.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	lost := &lostThenDownStore{sessionStore: r.st, armed: true, down: true}
	r.st = lost
	want[30] = block(30)
	writeSync(t, s, map[uint32][]byte{30: want[30]})
	if _, err := l.Capture(ctx); err == nil {
		t.Fatal("fixture: the cut reported success though its reply was lost and HEAD unreadable")
	}
	if !lost.lost {
		t.Fatal("the fault never fired")
	}
	_ = s.Close() // the process ends with the commit unresolved

	s2, l2, _ := lh.attach("a", false, false)
	defer s2.Close()
	want[50] = block(50)
	writeSync(t, s2, map[uint32][]byte{50: want[50]})
	_, err := l2.Capture(ctx)
	requireNotFenced(t, err, "the restarted process's first cut")
	_ = s2.Close()
	lh.readBack("b", want)
}
