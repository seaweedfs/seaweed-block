package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

// zeroStagedLayer is a layer past its zero baseline cut with n chunks
// rewritten with zeros: candidates that staging reads and hashes but never
// uploads. The spill cap of two chunks makes the cut stage them.
func zeroStagedLayer(t *testing.T, n int) (*fakeChunkRemote, *chunkLayerImpl) {
	t.Helper()
	s, f, l, w := precopyLayer(t, 1, 0)
	l.cfg.COWSpillLimit = 2 * ctChunkBytes
	for c := 0; c < n; c++ {
		w.write(t, s, uint32(c*ctChunkBlocks+2), 0)
	}
	return f, l
}

// The payload time of a cut counts its staging rounds even when they
// upload nothing (every staged chunk was zero), so a first cut that spent
// its time reading and hashing shows it.
func TestChunkStage_PayloadTimeCountsStagingWithoutUploads(t *testing.T) {
	_, l := zeroStagedLayer(t, 6)
	// Staging takes measurable time on any clock resolution.
	l.testAfterStageHook = func(int) { time.Sleep(20 * time.Millisecond) }
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	st := r.Stats
	if st.StageRounds == 0 || st.StagedChunks == 0 {
		t.Fatalf("cut stats %+v; want staging rounds", st)
	}
	if st.UploadedBytes != 0 {
		t.Fatalf("uploaded %d bytes; want none (every staged chunk is zero)", st.UploadedBytes)
	}
	if st.PayloadNs < int64(20*time.Millisecond) {
		t.Fatalf("payload time %d ns with %d chunks staged; want the staging time", st.PayloadNs, st.StagedChunks)
	}
}

// The layer counts every chunk staged by any cut attempt, a failed one
// included, so a first cut retried after its deadline shows the progress
// its earlier attempts made; the last successful cut's stats do not move.
// An attempt that staged all 6 candidates counts exactly 6.
func TestChunkStage_StagedChunksCountsFailedAttempts(t *testing.T) {
	_, l := zeroStagedLayer(t, 6)
	before := l.Stats()
	ctx, cancel := context.WithCancel(context.Background())
	l.testBeforeBarrierLockHook = cancel
	if _, err := l.Cut(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cut: %v; want it cancelled before the barrier", err)
	}
	l.testBeforeBarrierLockHook = nil
	after := l.Stats()
	if got := after.StagedChunks - before.StagedChunks; got != 6 {
		t.Fatalf("a failed attempt that staged all 6 candidates counted %d", got)
	}
	if after.CutSeq != before.CutSeq || after.LastCut.StagedChunks != before.LastCut.StagedChunks {
		t.Fatal("a failed attempt changed the last successful cut")
	}
	// A retry stages nothing again (the chunks are reused) and counts nothing.
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if got := l.Stats().StagedChunks - after.StagedChunks; got != 0 {
		t.Fatalf("the retry counted %d staged chunks; want 0 (every chunk reused)", got)
	}
}

// A round that fails partway counts the chunks it finished, each remembered
// for reuse, and not the ones it did not: one upload worker stages the five
// zero chunks (the most recent writes go first), then the non-zero chunk's
// upload fails at its intent.
func TestChunkStage_StagedChunksCountsAnInterruptedRound(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 0)
	l.cfg.COWSpillLimit = 2 * ctChunkBytes
	l.cfg.UploadConcurrency = 1
	w.write(t, s, uint32(0*ctChunkBlocks+2), 0x5a) // oldest write: staged last
	for c := 1; c <= 5; c++ {
		w.write(t, s, uint32(c*ctChunkBlocks+2), 0)
	}
	f.mu.Lock()
	f.failPutIntent = errors.New("intent refused")
	f.mu.Unlock()
	before := l.Stats().StagedChunks
	l.stMu.Lock()
	reusedBefore := len(l.reusable)
	l.stMu.Unlock()
	if _, err := l.Cut(ctTimeout(t)); err == nil {
		t.Fatal("cut succeeded; want the staging round to fail at the intent")
	}
	l.stMu.Lock()
	remembered := len(l.reusable) - reusedBefore
	l.stMu.Unlock()
	got := l.Stats().StagedChunks - before
	if got != 5 || uint64(remembered) != got {
		t.Fatalf("interrupted round counted %d staged chunks, remembered %d; want 5 and 5", got, remembered)
	}
}
