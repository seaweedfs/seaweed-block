package chunkstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// commitThenChangeHEAD places an operator write after the holder's commit but
// before the engine's confirmation read, which is the ordering that used to
// make a healthy holder fence itself.
type commitThenChangeHEAD struct {
	*LayoutRemote
	armed  bool
	change func(context.Context) error
}

// conflictBeforeCommitStore places a second HEAD writer after Commit read its
// base but before its conditional write. The underlying store gives the
// commit a definite generation refusal.
type conflictBeforeCommitStore struct {
	sessionStore
	armed  bool
	change func(context.Context) error
}

func (s *conflictBeforeCommitStore) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	if s.armed && next.Activity.Kind == layout.ActIdle && next.Current != nil {
		s.armed = false
		if err := s.change(ctx); err != nil {
			return err
		}
	}
	return s.sessionStore.UpdateHead(ctx, gen, next)
}

func TestLayoutRemote_OperatorHEADWriteDuringCommitRetries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(context.Context, *layoutHost, sessionStore) error
	}{
		{"delete snapshot", func(ctx context.Context, _ *layoutHost, st sessionStore) error {
			_, err := layout.RunDeleteSnapshot(ctx, st, "old")
			return err
		}},
		{"retire snapshot", func(ctx context.Context, _ *layoutHost, st sessionStore) error {
			_, err := layout.RunRetireSnapshot(ctx, st, "old", 10)
			return err
		}},
		{"add dependent", func(ctx context.Context, lh *layoutHost, st sessionStore) error {
			_, reader, err := lh.cat.open(lh.v)
			if err != nil {
				return err
			}
			_, err = layout.RunAddDependent(ctx, st, reader, "old", layout.Dependent{Target: "target", TargetID: layout.VolumeID{7}})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			lh := newLayoutHost(t)
			s, layer, remote := lh.attach("active", true, true)
			defer s.Close()
			writeSync(t, s, map[uint32][]byte{2: block(2)})
			remote.SetSnapshot("old", layout.SnapshotMeta{CreatedNs: 1})
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatal(err)
			}
			st := &conflictBeforeCommitStore{sessionStore: remote.st, armed: true}
			st.change = func(ctx context.Context) error { return tc.change(ctx, lh, st.sessionStore) }
			remote.st = st
			writeSync(t, s, map[uint32][]byte{9: block(9)})
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatalf("same-holder commit after operator write: %v", err)
			}
			if st.armed {
				t.Fatal("operator write was not injected before the conditional commit")
			}
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatalf("next cut after operator write: %v", err)
			}
			lh.readBack("reader", map[uint32][]byte{2: block(2), 9: block(9)})
		})
	}
}

func TestLayoutRemote_ForeignClaimDuringCommitFences(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, layer, remote := lh.attach("active", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{2: block(2)})
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	st := &conflictBeforeCommitStore{sessionStore: remote.st, armed: true}
	st.change = func(ctx context.Context) error {
		_, err := layout.RunClaim(ctx, st.sessionStore, okClaimLease{}, layout.HolderID{Epoch: lh.me.Epoch + 1, Holder: "foreign", ClaimID: "foreign-claim"})
		return err
	}
	remote.st = st
	writeSync(t, s, map[uint32][]byte{9: block(9)})
	if _, err := layer.Capture(ctx); !errors.Is(err, storage.ErrChunkLayerFenced) {
		t.Fatalf("foreign claim during commit: %v; want final fence", err)
	}
	if st.armed {
		t.Fatal("foreign claim was not injected before the conditional commit")
	}
}

func (r *commitThenChangeHEAD) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	gen, err := r.LayoutRemote.CASHead(ctx, expected, next)
	if err == nil && r.armed {
		r.armed = false
		if err := r.change(ctx); err != nil {
			return 0, err
		}
	}
	return gen, err
}

func TestLayoutRemote_OperatorHEADWriteBetweenCommitAndReadback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(context.Context, *layoutHost) error
	}{
		{"delete snapshot", func(ctx context.Context, lh *layoutHost) error {
			_, err := lh.cat.DeleteSnapshot(ctx, lh.v, "old")
			return err
		}},
		{"retire snapshot", func(ctx context.Context, lh *layoutHost) error {
			st, _ := lh.cat.Store(lh.v)
			_, err := layout.RunRetireSnapshot(ctx, st, "old", 10)
			return err
		}},
		{"add dependent", func(ctx context.Context, lh *layoutHost) error {
			st, reader, err := lh.cat.open(lh.v)
			if err != nil {
				return err
			}
			_, err = layout.RunAddDependent(ctx, st, reader, "old", layout.Dependent{Target: "target", TargetID: layout.VolumeID{7}})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			lh := newLayoutHost(t)
			s, err := storage.CreateWALStoreWithOptions(lh.dir+"/active", lrBlocks, 4096, storage.WALStoreOptions{ChunkBlocks: lrChunkBlocks})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err := s.Recover(); err != nil {
				t.Fatal(err)
			}
			inner, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
			if err != nil {
				t.Fatal(err)
			}
			r := &commitThenChangeHEAD{LayoutRemote: inner}
			layer, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: lrChunkBlocks, Epoch: lh.me.Epoch, Holder: lh.me.Holder, FreshFromHead: true})
			if err != nil {
				t.Fatal(err)
			}
			writeSync(t, s, map[uint32][]byte{2: block(2)})
			inner.SetSnapshot("old", layout.SnapshotMeta{CreatedNs: 1})
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatal(err)
			}
			r.change = func(ctx context.Context) error { return tc.change(ctx, lh) }
			r.armed = true
			if _, err := s.Write(3, block(3)); err != nil {
				t.Fatal(err)
			}
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatalf("cut after operator HEAD write: %v", err)
			}
			if r.armed {
				t.Fatal("operator write was not injected after the commit")
			}
			writeSync(t, s, map[uint32][]byte{4: block(4)})
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatalf("next cut after operator HEAD write: %v", err)
			}
			lh.readBack("reader", map[uint32][]byte{2: block(2), 3: block(3), 4: block(4)})
		})
	}
}

// The layout records chunk names, not each chunk's birth LSN. A later cut
// that advances HEAD while reusing an older chunk must still permit a no-op
// cut: the same object and holder cannot become a competing publication.
func TestLayoutRemote_ReusedChunkBirthDoesNotFenceNoopCut(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, layer, _ := lh.attach("active", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{1: block(1)})
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	writeSync(t, s, map[uint32][]byte{9: block(9)})
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatalf("no-op cut fenced the same holder after reusing a chunk: %v", err)
	}
	lh.readBack("reader", map[uint32][]byte{1: block(1), 9: block(9)})
}

// A successful Commit may lose its reply. The next HEAD read must use the
// pending engine refs for that published image, including a reused old chunk.
func TestLayoutRemote_ReusedChunkBirthAfterLostCommitReply(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, layer, remote := lh.attach("active", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{1: block(1)})
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	lost := &lostReplyStore{sessionStore: remote.st, armed: true}
	remote.st = lost
	writeSync(t, s, map[uint32][]byte{9: block(9)})
	_, _ = layer.Capture(ctx)
	if lost.armed {
		t.Fatal("the commit reply was not lost")
	}
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatalf("no-op cut after a lost commit reply: %v", err)
	}
	lh.readBack("reader", map[uint32][]byte{1: block(1), 9: block(9)})
}

type pauseCleanEndStore struct {
	sessionStore
	persisted chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (s *pauseCleanEndStore) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	if err := s.sessionStore.UpdateHead(ctx, gen, next); err != nil {
		return err
	}
	if next.Activity.Kind == layout.ActIdle {
		var waitErr error
		s.once.Do(func() {
			close(s.persisted)
			select {
			case <-s.release:
			case <-ctx.Done():
				waitErr = ctx.Err()
			}
		})
		return waitErr
	}
	return nil
}

// The clean-end HEAD write can become visible before Clean has reacquired
// the adapter mutex to record its generation. A read in that gap must keep
// the holder's engine view, and the next cut must still succeed.
func TestLayoutRemote_CleanEndBetweenCommitAndReadback(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, layer, remote := lh.attach("active", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{2: block(2)})
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st := &pauseCleanEndStore{sessionStore: remote.st, persisted: make(chan struct{}), release: make(chan struct{})}
	remote.st = st
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(st.release) }) }
	defer release()
	done := make(chan error, 1)
	go func() { done <- remote.Clean(ctx, func(context.Context, layout.HolderID, string) error { return nil }) }()
	select {
	case <-st.persisted:
	case err := <-done:
		t.Fatalf("clean ended before its HEAD write was observed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("clean end did not reach the persisted HEAD write")
	}
	mid, err := remote.GetHead(ctx)
	if err != nil || mid.Gen != before.Gen {
		t.Fatalf("clean end changed the engine view: gen %d -> %d, err=%v", before.Gen, mid.Gen, err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	writeSync(t, s, map[uint32][]byte{9: block(9)})
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatalf("next cut after clean end: %v", err)
	}
	lh.readBack("reader", map[uint32][]byte{2: block(2), 9: block(9)})
}
