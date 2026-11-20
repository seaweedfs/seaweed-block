package storage

// F1(b): a layer whose dirty set is unknown (bitmap lost or mismatched, or a
// BASE install just finished) reconciles it against HEAD in the background,
// outside the capture barrier. No HEAD moves until it is done.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type reconcileFixture struct {
	s     *WALStore
	f     *fakeChunkRemote
	bm    *fakeBitmapStore
	cfg   chunkLayerConfig
	layer chunkLayer
	hold  chan struct{}
	path  string
}

// newReconcileFixture publishes chunks 0 and 5, leaves chunk 6 flushed and
// chunk 7 synced only, crashes, loses the bitmap and reattaches. With hold
// the reconciler is held before its first slice.
func newReconcileFixture(t *testing.T, hold bool) *reconcileFixture {
	return newReconcileFixtureWith(t, hold, func(bm *fakeBitmapStore) { bm.lostNext = true })
}

// newReconcileFixtureWith reattaches after mutate damaged the bitmap.
func newReconcileFixtureWith(t *testing.T, hold bool, mutate func(bm *fakeBitmapStore)) *reconcileFixture {
	t.Helper()
	return newReconcileFixtureOpts(t, hold, mutate, nil)
}

// newReconcileFixtureOpts also lets mutateCfg adjust the reattach config.
func newReconcileFixtureOpts(t *testing.T, hold bool, mutate func(bm *fakeBitmapStore), mutateCfg func(cfg *chunkLayerConfig)) *reconcileFixture {
	t.Helper()
	s, path, crash := legacyCrashableStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	bm := cfg.Bitmap.(*fakeBitmapStore)
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 1, 0x01)  // chunk 0
	w.write(t, s, 21, 0x21) // chunk 5
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 24, 0x24) // chunk 6: flushed
	mustSyncFlush(t, s)
	w.write(t, s, 28, 0x28) // chunk 7: synced only
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crash()
	s2, _ := legacyReopen(t, path)
	bm.mu.Lock()
	mutate(bm)
	bm.mu.Unlock()
	fx := &reconcileFixture{s: s2, f: f, bm: bm, cfg: cfg, path: path}
	if hold {
		fx.hold = make(chan struct{})
		fx.cfg.testReconcileHold = fx.hold
	}
	if mutateCfg != nil {
		mutateCfg(&fx.cfg)
	}
	l, err := attachChunkLayer(s2, f, fx.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if fx.hold != nil {
			select {
			case <-fx.hold:
			default:
				close(fx.hold)
			}
		}
		_ = l.Close()
	})
	fx.layer = l
	return fx
}

func (fx *reconcileFixture) release() { close(fx.hold) }

func TestChunkReconcile_NoHeadMovesWhileReconciling(t *testing.T) {
	fx := newReconcileFixture(t, true)
	st := fx.layer.Stats()
	if !st.Reconciling || st.ReconcileTotal == 0 || st.ReconcileDone != 0 {
		t.Fatalf("stats while held: reconciling=%v done=%d/%d", st.Reconciling, st.ReconcileDone, st.ReconcileTotal)
	}
	if st.DirtyChunks != 0 {
		t.Fatalf("the reconcile obligation counted as %d dirty chunks", st.DirtyChunks)
	}
	before := fx.f.stats()
	if _, err := fx.layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkReconciling) {
		t.Fatalf("Cut while reconciling: %v", err)
	}
	if err := fx.layer.MakeRemoteDurable(ctTimeout(t), 1); !errors.Is(err, errChunkReconciling) {
		t.Fatalf("MakeRemoteDurable while reconciling: %v", err)
	}
	if after := fx.f.stats(); after.puts != before.puts || after.cas != before.cas || after.getHeads != before.getHeads {
		t.Fatalf("refused cuts made remote calls: %+v -> %+v", before, after)
	}
	if got := fx.layer.Stats().CutsRefused["reconciling"]; got != 2 {
		t.Fatalf("reconciling refusals %d, want 2", got)
	}
	fx.release()
	waitReconciledForTest(t, fx.layer)
	if d := fx.layer.Stats().DirtyChunks; d != 2 {
		t.Fatalf("dirty chunks after reconciliation: %d, want 2 (chunks 6 and 7)", d)
	}
	before = fx.f.stats()
	r, err := fx.layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatalf("Cut after reconciliation: %v", err)
	}
	if n := fx.f.stats().puts - before.puts; n != 2 || r.FullHash {
		t.Fatalf("first cut after reconciliation stored %d chunks (fullHash=%v), want exactly 2 and no full hash", n, r.FullHash)
	}
}

func TestChunkReconcile_WritesProceedWhileReconciling(t *testing.T) {
	fx := newReconcileFixture(t, true)
	done := make(chan error, 1)
	go func() { _, err := fx.s.Write(9, makeBlock(ctBlockSize, 0x09)); done <- err }() // chunk 2
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("a write blocked behind the reconciliation")
	}
	if _, err := fx.s.Sync(); err != nil {
		t.Fatal(err)
	}
	fx.release()
	waitReconciledForTest(t, fx.layer)
	r, err := fx.layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Head.Chunks[2]; !ok {
		t.Fatal("the write made during reconciliation is missing from the next HEAD")
	}
}

func TestChunkReconcile_DetachWaitsForReconciliation(t *testing.T) {
	fx := newReconcileFixture(t, true)
	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := fx.layer.Detach(ctx)
		errc <- err
	}()
	select {
	case err := <-errc:
		t.Fatalf("Detach returned while reconciling: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	fx.release()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Detach after reconciliation: %v", err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("Detach never returned after reconciliation")
	}
	if !chunkHeadIsDetached(fx.f.stats().head) {
		t.Fatal("HEAD not detached after the waited Detach")
	}
}

func TestChunkReconcile_DetachDeadlineWhileReconciling(t *testing.T) {
	fx := newReconcileFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	before := fx.f.stats()
	errc := make(chan error, 1)
	go func() { _, err := fx.layer.Detach(ctx); errc <- err }()
	select {
	case err := <-errc:
		if !errors.Is(err, errChunkReconciling) {
			t.Fatalf("Detach past its deadline while reconciling: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Detach ignored its deadline while reconciling")
	}
	if after := fx.f.stats(); after.cas != before.cas {
		t.Fatal("a refused Detach issued a CAS")
	}
}

// The reconciled dirty set is persisted with a header naming HEAD: a
// restart trusts it and does not reconcile again.
func TestChunkReconcile_ResultPersisted(t *testing.T) {
	// A header naming another HEAD generation: the reconciled result must
	// rewrite it, or the next attach mismatches and reconciles again.
	fx := newReconcileFixtureWith(t, false, func(bm *fakeBitmapStore) {
		bm.hdr.HeadGen += 7
		bm.dirty = map[uint64]bool{}
	})
	waitReconciledForTest(t, fx.layer)
	head := fx.f.stats().head
	fx.bm.mu.Lock()
	hdr, dirty := fx.bm.hdr, len(fx.bm.dirty)
	fx.bm.mu.Unlock()
	if hdr.HeadGen != head.Gen || hdr.Epoch != head.Epoch || hdr.BaseLSN != head.RemoteLSN || hdr.Base != chunkBaseNone || dirty != 2 {
		t.Fatalf("persisted header %+v with %d dirty chunks; want HEAD gen %d epoch %d lsn %d and 2 dirty", hdr, dirty, head.Gen, head.Epoch, head.RemoteLSN)
	}
	if err := fx.s.Close(); err != nil { // closes the layer too
		t.Fatal(err)
	}
	s3, _ := legacyReopen(t, fx.path)
	l2, err := attachChunkLayer(s3, fx.f, fx.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if st := l2.Stats(); st.Reconciling || st.DirtyChunks != 2 {
		t.Fatalf("reattach after a persisted reconciliation: reconciling=%v dirty=%d", st.Reconciling, st.DirtyChunks)
	}
}

// A failed bitmap save keeps the layer reconciling (and cuts refused) until
// the result is persisted.
func TestChunkReconcile_UnpersistedResultKeepsReconciling(t *testing.T) {
	fx := newReconcileFixture(t, true)
	fx.bm.mu.Lock()
	fx.bm.failSave = errors.New("injected save failure")
	fx.bm.mu.Unlock()
	fx.release()
	time.Sleep(300 * time.Millisecond)
	if st := fx.layer.Stats(); !st.Reconciling || st.ReconcileDone != st.ReconcileTotal {
		t.Fatalf("with the save failing: reconciling=%v done=%d/%d; want every chunk compared and still reconciling", st.Reconciling, st.ReconcileDone, st.ReconcileTotal)
	}
	if _, err := fx.layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkReconciling) {
		t.Fatalf("Cut with an unpersisted reconciliation: %v", err)
	}
	fx.bm.mu.Lock()
	fx.bm.failSave = nil
	fx.bm.mu.Unlock()
	waitReconciledForTest(t, fx.layer)
}

// refusedOver asserts check() holds at every poll for d: a negative
// invariant is checked over an interval, not once.
func refusedOver(t *testing.T, d time.Duration, what string, check func() error) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if err := check(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A worker reconciling a lost bitmap when a BASE install begins must not
// finish over it: the install guard (Base=Installing) stays in force, in
// memory and on disk, until the install itself completes.
func TestChunkReconcile_BaseInstallSupersedesRunningReconciliation(t *testing.T) {
	fx := newReconcileFixture(t, true) // the worker is held before its first slice
	if err := fx.layer.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	fx.release() // the old worker runs on
	refusedOver(t, 300*time.Millisecond, "after BeginBaseInstall", func() error {
		if _, err := fx.layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkBaseInstalling) {
			return fmt.Errorf("Cut admitted or refused for another cause during an incomplete BASE install: %v", err)
		}
		fx.bm.mu.Lock()
		base := fx.bm.hdr.Base
		fx.bm.mu.Unlock()
		if base != chunkBaseInstalling {
			return fmt.Errorf("persisted Base=%v, want the install guard (installing)", base)
		}
		return nil
	})
	// A restart still sees the incomplete install and refuses cuts.
	if err := fx.s.Close(); err != nil { // closes the layer too
		t.Fatal(err)
	}
	s3, _ := legacyReopen(t, fx.path)
	defer s3.Close()
	cfg := fx.cfg
	cfg.testReconcileHold = nil
	l2, err := attachChunkLayer(s3, fx.f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if _, err := l2.Cut(ctTimeout(t)); !errors.Is(err, errChunkBaseInstalling) {
		t.Fatalf("Cut after restart during an incomplete BASE install: %v", err)
	}
}

// An old worker that saw its set empty and paused before certifying must
// not certify (or touch) the replacement reconciliation that a BASE
// install started meanwhile: cuts stay refused until the replacement is
// done.
func TestChunkReconcile_OldCompletionCannotCertifyReplacement(t *testing.T) {
	var oldGen atomic.Uint64
	atFinish, releaseFinish, replHold := make(chan struct{}), make(chan struct{}), make(chan struct{})
	hook := func(stage string, gen uint64) {
		switch {
		case stage == "finish" && oldGen.CompareAndSwap(0, gen):
			close(atFinish)
			<-releaseFinish
		case stage == "batch" && oldGen.Load() != 0 && gen != oldGen.Load():
			<-replHold
		}
	}
	fx := newReconcileFixtureOpts(t, false, func(bm *fakeBitmapStore) { bm.lostNext = true },
		func(cfg *chunkLayerConfig) { cfg.testReconcileHook = hook })
	// Registered after the fixture: cleanups run last-in first-out, so the
	// workers are released before the layer closes and waits for them.
	t.Cleanup(func() {
		for _, ch := range []chan struct{}{releaseFinish, replHold} {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
	select {
	case <-atFinish:
	case <-time.After(ctLocalBound):
		t.Fatal("the first reconciliation never reached certification")
	}
	if err := fx.layer.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	if err := fx.layer.EndBaseInstall(fx.s.CheckpointLSN()); err != nil {
		t.Fatal(err)
	}
	total := fx.layer.Stats().ReconcileTotal
	if total == 0 {
		t.Fatal("the replacement reconciliation has nothing to compare; the schedule needs local chunks")
	}
	close(releaseFinish) // the old worker resumes into certification
	refusedOver(t, 300*time.Millisecond, "after the old worker resumed", func() error {
		st := fx.layer.Stats()
		if !st.Reconciling || st.ReconcileTotal != total || st.ReconcileDone != 0 {
			return fmt.Errorf("replacement certified or modified by the old worker: reconciling=%v done=%d/%d (total was %d)", st.Reconciling, st.ReconcileDone, st.ReconcileTotal, total)
		}
		if _, err := fx.layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkReconciling) {
			return fmt.Errorf("Cut admitted while the replacement reconciles: %v", err)
		}
		return nil
	})
	close(replHold)
	waitReconciledForTest(t, fx.layer)
	if _, err := fx.layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("Cut after the replacement reconciled: %v", err)
	}
}

// An old worker paused between comparing a chunk and recording it must not
// record into (or delete from) the replacement reconciliation's set.
func TestChunkReconcile_OldWorkerCannotModifyReplacementSet(t *testing.T) {
	var oldGen atomic.Uint64
	atCompared, releaseOld, replHold := make(chan struct{}), make(chan struct{}), make(chan struct{})
	hook := func(stage string, gen uint64) {
		switch {
		case stage == "compared" && oldGen.CompareAndSwap(0, gen):
			close(atCompared)
			<-releaseOld
		case stage == "batch" && oldGen.Load() != 0 && gen != oldGen.Load():
			<-replHold
		}
	}
	fx := newReconcileFixtureOpts(t, false, func(bm *fakeBitmapStore) { bm.lostNext = true },
		func(cfg *chunkLayerConfig) { cfg.testReconcileHook = hook })
	// Registered after the fixture: cleanups run last-in first-out, so the
	// workers are released before the layer closes and waits for them.
	t.Cleanup(func() {
		for _, ch := range []chan struct{}{releaseOld, replHold} {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
	select {
	case <-atCompared:
	case <-time.After(ctLocalBound):
		t.Fatal("the first reconciliation never compared a chunk")
	}
	if err := fx.layer.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	if err := fx.layer.EndBaseInstall(fx.s.CheckpointLSN()); err != nil {
		t.Fatal(err)
	}
	total := fx.layer.Stats().ReconcileTotal
	close(releaseOld)
	refusedOver(t, 300*time.Millisecond, "after the paused old worker resumed", func() error {
		if st := fx.layer.Stats(); st.ReconcileTotal != total || st.ReconcileDone != 0 {
			return fmt.Errorf("old worker modified the replacement set: done=%d/%d (total was %d)", st.ReconcileDone, st.ReconcileTotal, total)
		}
		return nil
	})
	close(replHold)
	waitReconciledForTest(t, fx.layer)
}
