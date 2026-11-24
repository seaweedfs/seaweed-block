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
	if after.StagedChunks <= before.StagedChunks {
		t.Fatalf("staged chunks %d before, %d after a failed attempt that staged; want more", before.StagedChunks, after.StagedChunks)
	}
	if after.CutSeq != before.CutSeq || after.LastCut.StagedChunks != before.LastCut.StagedChunks {
		t.Fatal("a failed attempt changed the last successful cut")
	}
}
