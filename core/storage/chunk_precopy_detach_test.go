package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// Exercise the owner-facing optional seam without changing ChunkLayer's
// existing interface or the remote protocol.
func TestPrecopyDetachConvergesBeforeStoppingIO(t *testing.T) {
	s, f, l, _ := precopyLayer(t, 1024, 2)
	type precopy interface {
		PrecopyDetach(context.Context, func() bool) (ChunkCutResult, error)
	}
	owner, ok := any(l).(precopy)
	if !ok {
		t.Fatal("chunk layer cannot converge rolling cuts before final detach")
	}
	frozen := false
	r, err := owner.PrecopyDetach(ctTimeout(t), func() bool { frozen = true; return true })
	if err != nil {
		t.Fatal(err)
	}
	if !frozen || r.Head.State != ChunkHeadDetached {
		t.Fatalf("frozen=%v state=%s", frozen, r.Head.State)
	}
	got := f.imageOf(t, r.Head)
	want := make([]byte, len(got))
	for i := uint32(0); i < ctNumBlocks; i++ {
		b, err := s.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		copy(want[int(i)*ctBlockSize:], b)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("detached HEAD differs from local image")
	}
	assertNoIntentsLeft(t, f)
}

func TestPrecopyDetachUnsettledRoundsKeepDirtyBytes(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 1)
	// Fail before publication, after a candidate image has been collected.
	f.failPutIntent = errFakeRemoteDown
	stopped := 0
	_, err := l.PrecopyDetach(ctTimeout(t), func() bool { stopped++; return true })
	if !errors.Is(err, ErrPrecopyUnknown) || stopped != 0 {
		t.Fatalf("err=%v stopped=%d", err, stopped)
	}
	if f.stats().head.State == ChunkHeadDetached {
		t.Fatal("failed rounds detached")
	}
	f.failPutIntent = nil
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.CandidateChunks == 0 {
		t.Fatal("aborted rounds marked dirty chunks clean")
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	if !bytes.Equal(f.imageOf(t, r.Head)[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 0x10)) {
		t.Fatal("failed rounds lost acknowledged bytes")
	}
	if _, err := s.Write(2, makeBlock(ctBlockSize, 0x99)); err != nil {
		t.Fatal("failed migration stopped old owner:", err)
	}
}

func TestPrecopyDetachBusyOwnerExhaustsWithoutStopping(t *testing.T) {
	precopyBusyOwnerControl(t, false)
}

func TestPrecopyDetachBusyOwnerRetainsSlowFixedCost(t *testing.T) {
	precopyBusyOwnerControl(t, true)
}

func precopyBusyOwnerControl(t *testing.T, slow bool) {
	t.Helper()
	s, f, l, _ := precopyLayer(t, 1024, 1)
	if slow {
		f.beforeCAS = func(*fakeChunkRemote) { time.Sleep(1300 * time.Millisecond) }
	}
	calls := 0
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { calls++; return false })
	if !errors.Is(err, ErrPrecopyNotConverged) || len(r.Precopy.Rounds) != 5 || r.Precopy.Frozen || calls == 0 {
		t.Fatalf("report=%+v calls=%d err=%v", r.Precopy, calls, err)
	}
	if f.stats().head.State == ChunkHeadDetached {
		t.Fatal("busy frontend detached")
	}
	if _, err := s.Write(2, makeBlock(ctBlockSize, 0x99)); err != nil {
		t.Fatal(err)
	}
}

func TestPrecopyDetachCountsBurstAtNextBarrier(t *testing.T) {
	s, _, l, _ := precopyLayer(t, 1024, 0)
	writes := 0
	l.testBeforeBarrierLockHook = func() {
		writes++
		if _, err := s.Write(1, makeBlock(ctBlockSize, byte(writes))); err != nil {
			t.Fatal(err)
		}
	}
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { return false })
	if !errors.Is(err, ErrPrecopyNotConverged) || writes != 5 || r.Precopy.TailBytes != uint64(ctChunkBytes) {
		t.Fatalf("writes=%d report=%+v err=%v", writes, r.Precopy, err)
	}
}

func TestPrecopyDetachRefusedFinalCutKeepsChangedBytes(t *testing.T) {
	precopyRefusedFinalCutControl(t, false)
}

func TestPrecopyDetachRefusedFinalCutRetainsSlowFixedCost(t *testing.T) {
	precopyRefusedFinalCutControl(t, true)
}

func precopyRefusedFinalCutControl(t *testing.T, slow bool) {
	t.Helper()
	s, f, l, w := precopyLayer(t, 1024, 0)
	if slow {
		f.beforeCAS = func(*fakeChunkRemote) { time.Sleep(1300 * time.Millisecond) }
	}
	for i := 0; i < 3; i++ {
		w.write(t, s, 1, byte(0x40+i))
		if _, err := l.Cut(ctTimeout(t)); err != nil {
			t.Fatal(err)
		}
	}
	w.write(t, s, 1, 0xbb)
	f.beforeCAS = func(f *fakeChunkRemote) { f.installLocked(copyHead(f.head)) }
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { return true })
	if !r.Precopy.Frozen || !errors.Is(err, ErrChunkHeadPrecondition) {
		t.Fatalf("report=%+v err=%v", r.Precopy, err)
	}
	how, _, err := l.SettleDetach(ctTimeout(t))
	if err != nil || how != DetachNotPublished {
		t.Fatalf("settlement=%v err=%v", how, err)
	}
	r, err = l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, r.Head)[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 0xbb)) {
		t.Fatal("refused final cut marked changed bytes clean")
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

func TestPrecopyDetachSustainedWrapFlushAndFreshRestore(t *testing.T) {
	makeStore := func(name string) *WALStore {
		s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), name), ctNumBlocks, ctBlockSize, v3TestWAL, WModeStrict, ctChunkBlocks)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		s.SetRecoveryRetentionLSNs(0)
		t.Cleanup(func() { closeBounded(t, s) })
		return s
	}
	s := makeStore("old")
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.FreshFromHead = true
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, ctNumBlocks*ctBlockSize)
	written := 0
	for i := 0; written <= 2*int(s.sb.WALSize); i++ {
		lba := uint32(i % ctNumBlocks)
		data := makeBlock(ctBlockSize, byte(i%250+1))
		if _, err := s.Write(lba, data); err != nil {
			t.Fatal(err)
		}
		copy(want[int(lba)*ctBlockSize:], data)
		written += len(data)
		if i%4 == 3 {
			mustSyncFlush(t, s)
		}
		if i%16 == 15 {
			if _, err := l.Cut(ctTimeout(t)); err != nil {
				t.Fatal(err)
			}
		}
	}
	mustSyncFlush(t, s)
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, r.Head), want) {
		t.Fatal("sustained writes/Flush detached the wrong image")
	}
	fresh := makeStore("new")
	next := ctConfig(2)
	next.Holder = "host-b"
	next.FreshFromHead = true
	next.RestoreChunkBytes = ctChunkBytes
	next.PrepareRestore = func(h ChunkHead) error { return fresh.SeedChunkFrontier(h.RemoteLSN) }
	h, err := ClaimChunkHead(ctTimeout(t), f, next)
	if err != nil {
		t.Fatal(err)
	}
	next.RestoreGuard = &ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}
	mustAttach(t, fresh, f, next)
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		got, err := fresh.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want[int(lba)*ctBlockSize:int(lba+1)*ctBlockSize]) {
			t.Fatalf("fresh restore LBA %d differs", lba)
		}
	}
	t.Logf("wrote %d bytes > WAL %d with periodic Flush; fresh host exact readback", written, s.sb.WALSize)
}

func TestPrecopyDetachCanceledBarrierNeverStopsFrontend(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 1024, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.testAfterBarrierSyncHook = func(uint64) { cancel() }
	stopped := false
	_, err := l.PrecopyDetach(ctx, func() bool { stopped = true; return true })
	if stopped {
		t.Fatal("cancellation at the tentative barrier still stopped the frontend")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled attempt: %v", err)
	}
}

func TestPrecopyDetachFreezeIncludesBarrierPause(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 1024, 0)
	l.testBeforeBarrierLockHook = func() { time.Sleep(120 * time.Millisecond) }
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if r.Precopy.FreezeElapsed < 120*time.Millisecond {
		t.Fatalf("freeze %s omitted tentative barrier pause", r.Precopy.FreezeElapsed)
	}
}

func TestPrecopyDetachQueuedCancellationReturnsWhileOtherCutOwnsPipeline(t *testing.T) {
	_, f, l, _ := precopyLayer(t, 1024, 1)
	entered, release := holdPuts(t, f)
	cutDone, preDone := make(chan struct{}), make(chan struct{})
	var ctx context.Context
	cancel := func() {}
	var precopyErr error
	t.Cleanup(func() {
		release()
		cancel()
		for _, done := range []chan struct{}{cutDone, preDone} {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("cut owner did not join")
			}
		}
	})
	go func() { defer close(cutDone); _, _ = l.Cut(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(preDone)
		t.Fatal("other cut did not reach held upload")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	go func() {
		defer close(preDone)
		_, precopyErr = l.PrecopyDetach(ctx, func() bool { t.Error("queued canceled caller stopped I/O"); return false })
	}()
	select {
	case <-preDone:
		if !errors.Is(precopyErr, context.DeadlineExceeded) {
			t.Fatalf("queued error: %v", precopyErr)
		}
	case <-time.After(time.Second):
		t.Fatal("queued pre-copy ignored its caller deadline")
	}
}

func TestPrecopyDetachExcludesServingPreparationFromFreezeCost(t *testing.T) {
	store, _, l, _ := precopyLayer(t, 1024, 0)
	if _, err := store.Write(1, makeBlock(ctBlockSize, 0x6e)); err != nil {
		t.Fatal(err)
	}
	l.testAfterAdmitHook = func() { l.testAfterAdmitHook = nil; time.Sleep(1300 * time.Millisecond) }
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { return true })
	if err != nil {
		t.Fatalf("preparation while serving was charged as frozen time: %v", err)
	}
	if !r.Precopy.Frozen {
		t.Fatal("empty final tail never froze")
	}
}

func TestPrecopyDetachRetryResolvesPreviouslyAppliedDetachedCut(t *testing.T) {
	_, f, l, _ := precopyLayer(t, 1024, 1)
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	f.casModes = []fakeCASMode{casAppliedLost}
	f.afterCAS = func(f *fakeChunkRemote) { f.failGet = errFakeRemoteDown }
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { return true })
	if !r.Precopy.Frozen || !errors.Is(err, ErrChunkCASOutcomeUnknown) {
		t.Fatalf("first report=%+v err=%v", r.Precopy, err)
	}
	f.failGet = nil
	before := f.stats()
	r, err = l.PrecopyDetach(ctTimeout(t), func() bool { t.Fatal("retry stopped I/O again instead of resolving the applied detach"); return false })
	if err != nil || r.Head.State != ChunkHeadDetached || r.Head.Gen != before.head.Gen {
		t.Fatalf("resolved result=%+v err=%v", r, err)
	}
	if !bytes.Equal(f.imageOf(t, r.Head)[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 0x10)) {
		t.Fatal("resolved detach lost bytes")
	}
}

func TestPrecopyDetachColdNoopRequiresMeasuredCASCost(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	f.seedHead(t, make([]byte, ctNumBlocks*ctBlockSize), 0, 1)
	cfg := ctConfig(1)
	cfg.Holder = "seed"
	cfg.FreshFromHead = true
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	stopped := 0
	_, err := l.PrecopyDetach(ctTimeout(t), func() bool { stopped++; return true })
	if !errors.Is(err, ErrPrecopyUnknown) || stopped != 0 {
		t.Fatalf("no-op read treated as measured CAS cost: stopped=%d err=%v", stopped, err)
	}
	if f.stats().head.Gen != 1 {
		t.Fatal("unknown cost published a final detached HEAD")
	}
	if _, err := s.Write(1, makeBlock(ctBlockSize, 0x61)); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { stopped++; return true })
	if err != nil || !r.Precopy.Frozen || stopped != 1 {
		t.Fatalf("measured CAS did not enable empty-tail freeze: report=%+v stopped=%d err=%v", r.Precopy, stopped, err)
	}
}

func TestPrecopyDrainReceives100MillisecondBound(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 1024, 1)
	calls := 0
	_, err := l.PrecopyDetachWithAdmission(context.Background(), func(ctx context.Context) (func(), error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 100*time.Millisecond {
			t.Fatalf("unbounded pre-copy drain: deadline=%v present=%t", deadline, ok)
		}
		return nil, errors.New("controlled drain refusal")
	}, func() bool { t.Error("refused drain committed freeze"); return true })
	if calls == 0 || !errors.Is(err, ErrPrecopyNotConverged) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestPrecopyDrainLetsAnAdmittedWriterReachTheBarrier(t *testing.T) {
	s, f, l, _ := precopyLayer(t, 1024, 3)
	written := make(chan error, 1)
	started, timedOut := false, false
	t.Cleanup(func() {
		if !started {
			return
		}
		select {
		case <-written:
		case <-time.After(3 * time.Second):
			t.Error("admitted writer did not join")
		}
	})
	r, err := l.PrecopyDetachWithAdmission(ctTimeout(t), func(ctx context.Context) (func(), error) {
		if started {
			return func() {}, nil
		}
		started = true
		go func() {
			_, err := s.Write(2, makeBlock(ctBlockSize, 0xa7))
			written <- err
		}()
		select {
		case err := <-written:
			written <- err // retain the completion for the cleanup join
			return func() {}, err
		case <-ctx.Done():
			timedOut = true
			return nil, ctx.Err()
		}
	}, func() bool { return true })
	if timedOut || err != nil || !r.Precopy.Frozen {
		t.Fatalf("drain blocked admitted writer: timed_out=%t report=%+v err=%v", timedOut, r.Precopy, err)
	}
	if !bytes.Equal(f.imageOf(t, r.Head)[2*ctBlockSize:3*ctBlockSize], makeBlock(ctBlockSize, 0xa7)) {
		t.Fatal("authoritative barrier omitted the drained writer")
	}
}

func TestPrecopyAttemptDiagnosticsSeparateTimeoutFromFit(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 1024, 0)
	calls := 0
	r, err := l.PrecopyDetachWithAdmission(ctTimeout(t), func(ctx context.Context) (func(), error) {
		calls++
		if calls == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return func() {}, nil
	}, func() bool { return true })
	if err != nil || len(r.Precopy.Rounds) != 2 {
		t.Fatalf("rounds=%+v err=%v", r.Precopy.Rounds, err)
	}
	first, second := r.Precopy.Rounds[0], r.Precopy.Rounds[1]
	if first.Attempt != 1 || first.Reason != "drain_timeout" || first.Frozen || second.Attempt != 2 || second.Reason != "fit" || !second.Frozen {
		t.Fatalf("attempt outcomes mixed: first=%+v second=%+v", first, second)
	}
	for _, a := range r.Precopy.Rounds {
		if a.Drain <= 0 || a.Pause < a.Drain || a.Barrier != a.Pause-a.Drain {
			t.Fatalf("pause accounting: %+v", a)
		}
	}
}

func TestPrecopyCountsWarmAttemptsSeparatelyFromMeasurementRounds(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "warm"}[warm], func(t *testing.T) {
			_, _, l, _ := precopyLayer(t, 1024, 0)
			l.precopySamples = nil
			if warm {
				for i := 0; i < 3; i++ {
					l.precopySamples = append(l.precopySamples, precopySample{at: time.Now(), fixed: time.Millisecond, rate: 1 << 30})
				}
			}
			r, _ := l.PrecopyDetachWithAdmission(ctTimeout(t), func(context.Context) (func(), error) {
				return nil, errors.New("controlled admission refusal")
			}, func() bool { t.Fatal("refused admission froze"); return false })
			data, err := json.Marshal(r.Precopy)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			wantSamples, wantAttempts, wantMeasurement := float64(0), float64(0), float64(5)
			if warm {
				wantSamples, wantAttempts, wantMeasurement = 3, 5, 0
			}
			if fields["samplesAtStart"] != wantSamples || fields["attempts"] != wantAttempts || fields["measurementRounds"] != wantMeasurement || len(r.Precopy.Rounds) != 5 {
				t.Fatalf("warm=%t report=%s", warm, data)
			}
		})
	}
}
