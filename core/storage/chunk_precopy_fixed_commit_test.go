package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestPrecopyFixedCostSmallTailCommitsBySecondAttempt(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 0)
	fixed := 1300 * time.Millisecond
	f.beforeCAS = func(*fakeChunkRemote) { time.Sleep(fixed) }
	rounds := 0
	l.testBeforeBarrierLockHook = func() {
		rounds++
		w.write(t, s, 1, byte(rounds))
		// Keep the same known small tail and fixed envelope in each attempt.
		l.precopySamples = nil
		for i := 0; i < 3; i++ {
			l.precopySamples = append(l.precopySamples, precopySample{at: time.Now(), fixed: fixed, rate: 20 * float64(ctChunkBytes)})
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := l.PrecopyDetach(ctx, func() bool { return true })
	if err != nil || !r.Precopy.Frozen || r.Precopy.Attempts > 2 || r.Precopy.Reason != "fixed_cost_over_allowance" || r.Precopy.Fixed < fixed || r.Precopy.DeclaredWorst < fixed {
		t.Fatalf("fixed cost cannot converge by recopying the same small tail: report=%+v err=%v", r.Precopy, err)
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
