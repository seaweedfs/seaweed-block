package storage

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func frozenContext(t *testing.T, elapsed time.Duration, force bool) context.Context {
	t.Helper()
	ctx, err := WithFrozenDetach(ctTimeout(t), FrozenDetachOptions{StartedAt: time.Now().Add(-elapsed), AllowOverBudget: force})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}
func seedFrozenSamples(l *chunkLayerImpl, fixed time.Duration) {
	l.precopySamples = nil
	for i := 0; i < 3; i++ {
		l.precopySamples = append(l.precopySamples, precopySample{at: time.Now(), fixed: fixed, rate: 20 * float64(ctChunkBytes)})
	}
}
func TestFrozenDetachRejectsBeforeStagingOrUpload(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 0)
	w.write(t, s, 1, 0x12)
	seedFrozenSamples(l, 1300*time.Millisecond)
	before := f.stats()
	l.testBeforeBarrierLockHook = func() { t.Fatal("preflight rejection reached capture") }
	r, err := l.PrecopyDetach(frozenContext(t, 4*time.Second, false), func() bool { t.Fatal("over-budget attempt stopped backend"); return true })
	after := f.stats()
	if !errors.Is(err, ErrFrozenTailTooLarge) || r.Precopy.Frozen || r.Precopy.Reason != "frozen_tail_too_large" || after.puts != before.puts || after.cas != before.cas || after.head.Gen != before.head.Gen {
		t.Fatalf("over-budget frozen cut did work: %+v %v before=%+v after=%+v", r.Precopy, err, before, after)
	}
	l.testBeforeBarrierLockHook = nil
	r, err = l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, r.Head)[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 0x12)) {
		t.Fatal("refusal dropped dirty bytes")
	}
}
func TestFrozenDetachRechecksBudgetAtBarrierWithoutUpload(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 3)
	w.write(t, s, 1, 0x24)
	seedFrozenSamples(l, time.Millisecond)
	before := f.stats()
	reached := false
	l.testBeforeBarrierLockHook = func() { reached = true; seedFrozenSamples(l, 6*time.Second) }
	r, err := l.PrecopyDetach(frozenContext(t, 0, false), func() bool { t.Fatal("authoritative refusal stopped backend"); return true })
	after := f.stats()
	if !reached || !errors.Is(err, ErrFrozenTailTooLarge) || r.Precopy.Frozen || len(r.Precopy.Rounds) != 1 || after.puts != before.puts || after.cas != before.cas || after.head.Gen != before.head.Gen {
		t.Fatalf("barrier rejection uploaded or rolled: %+v %v", r.Precopy, err)
	}
	l.testBeforeBarrierLockHook = nil
	r, err = l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}
func TestFrozenDetachForcedFinalCommitsSamePoint(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 0)
	w.write(t, s, 1, 0x36)
	seedFrozenSamples(l, 6*time.Second)
	stops := 0
	r, err := l.PrecopyDetach(frozenContext(t, 6*time.Second, true), func() bool { stops++; return true })
	if err != nil || !r.Precopy.OverBudget || !r.Precopy.GuestFrozen || !r.Precopy.Frozen || r.Precopy.Reason != "over_budget" || len(r.Precopy.Rounds) != 1 || stops != 1 || r.Head.State != ChunkHeadDetached {
		t.Fatalf("explicit final did not commit once: %+v %v", r, err)
	}
	if !r.Precopy.LatencyGateFailed || r.Precopy.FreezeElapsed < 6*time.Second {
		t.Fatalf("external elapsed hidden: %+v", r.Precopy)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	r2, err := l.PrecopyDetach(frozenContext(t, 7*time.Second, false), func() bool { t.Fatal("applied retry stopped again"); return true })
	want := r.Precopy
	want.replayed = true
	if err != nil || r2.Head.Gen != r.Head.Gen || !reflect.DeepEqual(r2.Precopy, want) {
		t.Fatalf("applied retry budget-refused: %+v %v", r2, err)
	}
}
func TestFrozenBudgetInputValidation(t *testing.T) {
	for _, at := range []time.Time{{}, time.Now().Add(time.Hour)} {
		if _, err := WithFrozenDetach(context.Background(), FrozenDetachOptions{StartedAt: at}); !errors.Is(err, ErrInvalidFreezeBudget) {
			t.Fatalf("invalid origin accepted: %v", err)
		}
	}
}

func TestFrozenFinalOverrideIsVisibleEvenWhenEstimateFits(t *testing.T) {
	r := PrecopyReport{Estimate: time.Second, Reason: "fit"}
	if !r.admitFrozen(FrozenDetachOptions{StartedAt: time.Now(), AllowOverBudget: true}) || !r.OverBudget || r.Reason != "over_budget" {
		t.Fatalf("explicit override hidden: %+v", r)
	}
}

func TestFrozenFinalOverrideKeepsAuthorityAndCloseGuards(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "fenced", true: "closed"}[closed], func(t *testing.T) {
			s, f, l, w := precopyLayer(t, 1, 0)
			w.write(t, s, 1, 0x4a)
			before := f.stats()
			want := ErrChunkLayerFenced
			if closed {
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				want = ErrChunkLayerClosed
			} else {
				l.cfg.CaptureAllowed = func() error { return ErrChunkLayerFenced }
			}
			_, err := l.PrecopyDetach(frozenContext(t, time.Minute, true), func() bool { t.Fatal("override bypassed gate"); return true })
			after := f.stats()
			if !errors.Is(err, want) || after.puts != before.puts || after.cas != before.cas {
				t.Fatalf("override bypassed %v: %v", want, err)
			}
		})
	}
}
func TestFrozenRetryDoesNotHideUnknownPublication(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 0)
	w.write(t, s, 1, 0x5b)
	seedFrozenSamples(l, time.Millisecond)
	f.casModes = []fakeCASMode{casAppliedLost}
	f.afterCAS = func(f *fakeChunkRemote) { f.failGet = errFakeRemoteDown }
	r, err := l.PrecopyDetach(frozenContext(t, 0, true), func() bool { return true })
	if !r.Precopy.Frozen || !errors.Is(err, ErrChunkCASOutcomeUnknown) {
		t.Fatalf("missing unknown publication: %+v %v", r.Precopy, err)
	}
	before := f.stats()
	_, err = l.PrecopyDetach(frozenContext(t, time.Minute, false), func() bool { t.Fatal("unknown retry stopped again"); return true })
	if !errors.Is(err, ErrChunkCASOutcomeUnknown) || errors.Is(err, ErrFrozenTailTooLarge) || f.stats().puts != before.puts {
		t.Fatalf("budget hid uncertainty: %v", err)
	}
	f.failGet = nil
	r, err = l.PrecopyDetach(frozenContext(t, time.Minute, false), func() bool { t.Fatal("applied retry stopped again"); return true })
	if err != nil || r.Head.Gen != before.head.Gen || r.Head.State != ChunkHeadDetached {
		t.Fatalf("applied same-point result budget-refused: %+v %v", r, err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

func TestFrozenBarrierConsumesElapsedWithoutNewAllowance(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 3)
	w.write(t, s, 1, 0x61)
	seedFrozenSamples(l, 1300*time.Millisecond)
	origin := time.Now()
	now := origin
	l.testFrozenNow = func() time.Time { return now }
	reached := false
	l.testBeforeBarrierLockHook = func() { reached = true; now = origin.Add(4 * time.Second) }
	before := f.stats()
	ctx, err := WithFrozenDetach(ctTimeout(t), FrozenDetachOptions{StartedAt: origin})
	if err != nil {
		t.Fatal(err)
	}
	stops := 0
	result, err := l.PrecopyDetach(ctx, func() bool { stops++; return true })
	after := f.stats()
	if !reached || stops != 0 || !errors.Is(err, ErrFrozenTailTooLarge) || result.Precopy.Estimate >= 5*time.Second || result.Precopy.RemainingFreeze > 1100*time.Millisecond || result.Precopy.Estimate <= result.Precopy.RemainingFreeze || after.puts != before.puts || after.cas != before.cas {
		t.Fatalf("elapsed barrier budget not enforced without upload: %+v %v", result.Precopy, err)
	}
	l.testBeforeBarrierLockHook = nil
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}
