package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// The spill budget, rather than a fixed chunk count, determines when local
// reads must be uploaded before fixing the cut frontier.
func TestChunkStageV3_SpillBudgetTriggersPrecopy(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 5)
	l.cfg.COWSpillLimit = 2 * int64(ctChunkBlocks*ctBlockSize)
	rounds := 0
	l.testAfterStageHook = func(int) {
		rounds++
		// A hot subset smaller than the cap remains writable while staging.
		w.write(t, s, 1, 0xb1)
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if rounds == 0 || rounds > 2 {
		t.Fatalf("precopy rounds=%d, want progress before the barrier", rounds)
	}
	if r.Stats.StagedChunksUsed < 4 {
		t.Fatalf("reused %d unchanged staged chunks", r.Stats.StagedChunksUsed)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// A known-aborted point retains immutable uploaded chunks and their intents.
// Retrying the current frontier must not send unchanged payloads again.
func TestChunkStageV3_AbortedPointReusesUploads(t *testing.T) {
	_, f, l, _ := precopyLayer(t, 1, 5)
	l.cfg.COWSpillLimit = int64(ctChunkBlocks * ctBlockSize)
	ctx, cancel := context.WithCancel(context.Background())
	l.testBeforeBarrierLockHook = cancel
	before := f.stats().head
	_, err := l.Cut(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted cut: %v", err)
	}
	if f.stats().head.Gen != before.Gen {
		t.Fatal("aborted point moved HEAD")
	}
	f.mu.Lock()
	sent := f.putCalls
	f.mu.Unlock()
	if sent == 0 {
		t.Fatal("no uploads before abort")
	}
	l.testBeforeBarrierLockHook = nil
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	resent := f.putCalls - sent
	f.mu.Unlock()
	if resent != 0 {
		t.Fatalf("retried unchanged point uploaded %d chunks", resent)
	}
	if bytes.Equal(f.imageOf(t, r.Head), f.imageOf(t, before)) {
		t.Fatal("retry did not publish the submitted changes")
	}
	assertNoIntentsLeft(t, f)
}
