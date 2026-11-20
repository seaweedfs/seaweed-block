package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestPrecopyFixedCostSmallTailCommitsBySecondAttempt(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 0)
	fixed := 1300 * time.Millisecond
	rounds := 0
	l.testBeforeBarrierLockHook = func() {
		rounds++
		w.write(t, s, 1, byte(rounds))
		f.beforeCAS = func(*fakeChunkRemote) { time.Sleep(fixed) }
		// Keep the same known small tail and fixed envelope in each attempt.
		l.precopySamples = nil
		for i := 0; i < 3; i++ {
			l.precopySamples = append(l.precopySamples, precopySample{at: time.Now(), fixed: fixed, parts: PrecopyFixedParts{HeadCAS: fixed}, rate: 20 * float64(ctChunkBytes)})
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := l.PrecopyDetach(ctx, func() bool { return true })
	if err != nil || !r.Precopy.Frozen || len(r.Precopy.Rounds) > 2 || r.Precopy.Reason != "fixed_cost_over_allowance" || r.Precopy.Fixed < fixed || r.Precopy.DeclaredWorst < fixed {
		t.Fatalf("fixed cost cannot converge by recopying the same small tail: report=%+v err=%v", r.Precopy, err)
	}
	if r.Precopy.FreezeElapsed < fixed || r.Precopy.LatencyGateFailed || time.Duration(r.Stats.CASWriteNs) < fixed {
		t.Fatalf("final fixed-cost operation was not witnessed within its declaration: %+v", r)
	}
	if r.Head.State != ChunkHeadDetached || !bytes.Equal(f.imageOf(t, r.Head)[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, byte(rounds))) {
		t.Fatal("amended commit lost the exact point")
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

func TestPrecopyLargeTailStillRefusesFixedCostException(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 0)
	rounds := 0
	l.testBeforeBarrierLockHook = func() {
		rounds++
		w.write(t, s, 1, byte(rounds))
		l.precopySamples = nil
		for i := 0; i < 3; i++ {
			l.precopySamples = append(l.precopySamples, precopySample{at: time.Now(), fixed: time.Millisecond, rate: float64(ctChunkBytes)})
		}
	}
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { t.Fatal("large tail froze"); return true })
	if !errors.Is(err, ErrPrecopyNotConverged) || r.Precopy.Frozen || len(r.Precopy.Rounds) != 5 || r.Precopy.Reason != "tail_too_large" {
		t.Fatalf("large tail was not refused: %+v %v", r.Precopy, err)
	}
	if f.stats().head.State != ChunkHeadActive {
		t.Fatal("refusal lost active owner")
	}
	w.write(t, s, 2, 0x77)
	r, err = l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, r.Head)[2*ctBlockSize:3*ctBlockSize], makeBlock(ctBlockSize, 0x77)) {
		t.Fatal("refusal stopped serving")
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

func TestPrecopyFixedCommitRuleKeepsProgressAndFreezeBounds(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		tail, estimate, previous time.Duration
		known, want              bool
	}{
		{"first", 100 * time.Millisecond, 1500 * time.Millisecond, 0, true, false},
		{"improving", 100 * time.Millisecond, 1500 * time.Millisecond, 1600 * time.Millisecond, true, false},
		{"stalled", 100 * time.Millisecond, 1500 * time.Millisecond, 1500 * time.Millisecond, true, true},
		{"worse-fixed", 100 * time.Millisecond, 1600 * time.Millisecond, 1500 * time.Millisecond, true, true},
		{"quarter", 300 * time.Millisecond, 1500 * time.Millisecond, 1500 * time.Millisecond, true, true},
		{"large-tail", 300*time.Millisecond + 1, 1500 * time.Millisecond, 1500 * time.Millisecond, true, false},
		{"unknown", 0, 1500 * time.Millisecond, 1500 * time.Millisecond, false, false},
		{"freeze-ceiling", 0, 5*time.Second + 1, 5*time.Second + 1, true, false},
		{"first-freeze-ceiling", 0, 5*time.Second + 1, 0, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason := "tail_too_large"
			if !tc.known {
				reason = "samples_unknown"
			}
			r := PrecopyReport{Fixed: 1300 * time.Millisecond, TailEstimate: tc.tail, Estimate: tc.estimate, Reason: reason}
			var previous *PrecopyAttempt
			if tc.previous > 0 {
				previous = &PrecopyAttempt{Estimate: tc.previous}
			}
			if got := r.admitPrecopy(previous); got != tc.want {
				t.Fatalf("admit=%t report=%+v", got, r)
			}
			if tc.known && tc.estimate > 5*time.Second && r.Reason != "fixed_cost_exceeds_freeze_budget" {
				t.Fatalf("mislabelled fixed ceiling: %+v", r)
			}
			if tc.want && r.Reason != "fixed_cost_over_allowance" {
				t.Fatalf("missing exception reason: %+v", r)
			}
		})
	}
}

func TestPrecopyFixedPartsStayWithLargestObservation(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 1024, 0)
	l.precopySamples = nil
	st := chunkCutStats{CASNs: 1, DrainNs: int64(10 * time.Millisecond), LockWaitNs: int64(30 * time.Millisecond), SyncNs: int64(40 * time.Millisecond), CASWriteNs: int64(50 * time.Millisecond), HeadReadbackNs: int64(60 * time.Millisecond), MapPersistNs: int64(10 * time.Millisecond)}
	l.samplePrecopy(st, int64(200*time.Millisecond))
	l.samplePrecopy(chunkCutStats{CASNs: 1, SyncNs: int64(100 * time.Millisecond)}, int64(150*time.Millisecond))
	r := l.precopyEstimate(0, 0)
	p := r.FixedParts
	if r.Fixed != 200*time.Millisecond || p.Drain != 10*time.Millisecond || p.StoreLock != 20*time.Millisecond || p.DataSync != 40*time.Millisecond || p.HeadCAS != 50*time.Millisecond || p.HeadReadback != 60*time.Millisecond || p.MapPersist != 10*time.Millisecond || p.Other != 10*time.Millisecond {
		t.Fatalf("mixed cost samples or overlapping terms: %+v", r)
	}
}

func TestPrecopyFixedExceptionFinishUsesDeclaredFreeze(t *testing.T) {
	r := PrecopyReport{Reason: "fixed_cost_over_allowance", Estimate: 1500 * time.Millisecond, DeclaredFreeze: 1500 * time.Millisecond}
	r.Finish(1400*time.Millisecond, 0, false)
	if r.LatencyGateFailed || r.DeclaredWorst != 1500*time.Millisecond {
		t.Fatalf("new declaration ignored: %+v", r)
	}
	r.Finish(1600*time.Millisecond, 0, false)
	if !r.LatencyGateFailed {
		t.Fatal("missed declared freeze overrun")
	}
	r.Finish(time.Second, 0, true)
	if !r.LatencyGateFailed {
		t.Fatal("hid physical failure")
	}
	r = PrecopyReport{Reason: "fit", Estimate: time.Second}
	r.Finish(1300*time.Millisecond, 0, false)
	if !r.LatencyGateFailed {
		t.Fatal("relaxed ordinary fit bound")
	}
}

type precopyTimedHeadRemote struct {
	*fakeChunkRemote
	casDelay, readDelay time.Duration
	beforeRead          func()
}

func (r *precopyTimedHeadRemote) CASHead(ctx context.Context, v chunkHeadVersion, h chunkHead) (uint64, error) {
	timer := time.NewTimer(r.casDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
	}
	return r.fakeChunkRemote.CASHead(ctx, v, h)
}
func (r *precopyTimedHeadRemote) GetHead(ctx context.Context) (chunkHead, error) {
	if r.beforeRead != nil {
		r.beforeRead()
	}
	timer := time.NewTimer(r.readDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return chunkHead{}, ctx.Err()
	case <-timer.C:
	}
	return r.fakeChunkRemote.GetHead(ctx)
}
func TestPrecopyFixedPartsMeasureCASAndReadbackSeparately(t *testing.T) {
	// The bubble's virtual clock advances only during the two blocked phases.
	// Exact phase attribution is independent of host scheduling or elapsed limits.
	synctest.Test(t, func(t *testing.T) {
		r := &precopyTimedHeadRemote{fakeChunkRemote: newFakeChunkRemote(), casDelay: 15 * time.Millisecond, readDelay: 25 * time.Millisecond}
		var st chunkCutStats
		readEntered := false
		r.beforeRead = func() {
			readEntered = true
			if time.Duration(st.CASWriteNs) != r.casDelay || st.HeadReadbackNs != 0 {
				t.Fatalf("CAS must be settled before readback starts: %+v", st)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := casAndConfirmChunkHeadMeasured(ctx, r, chunkHeadVersion{}, chunkHead{Epoch: 1, Holder: "owner", Chunks: map[uint64]chunkRef{}}, &st)
		if err != nil {
			t.Fatal(err)
		}
		if !readEntered || time.Duration(st.CASWriteNs) != r.casDelay || time.Duration(st.HeadReadbackNs) != r.readDelay {
			t.Fatalf("overlapping or missing phase attribution: %+v readEntered=%t", st, readEntered)
		}
	})
}
