package storage

import (
	"strings"
	"testing"
)

func TestCutPayloadBudgetBoundsSlots(t *testing.T) {
	const chunkBytes = int64(4 << 20)
	for _, candidates := range []int{256, 4096} {
		l := &chunkLayerImpl{blockSize: 4096, chunkBlocks: 1024, cfg: chunkLayerConfig{UploadConcurrency: 64, PayloadMemoryBudget: 6 * chunkBytes}}
		pending := make(map[uint64]bool, candidates)
		for i := 0; i < candidates; i++ {
			pending[uint64(i)] = true
		}
		w, err := l.installCOWWindow(pending, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		workers, _, limit := l.cutPayloadWorkers()
		if workers != 1 || cap(w.saverSlots) != 1 || limit != 6*chunkBytes || w.memoryLimit != limit || w.memory > limit {
			t.Errorf("candidates=%d workers=%d savers=%d memory=%d limit=%d windowLimit=%d; want one uploader+saver within configured %d", candidates, workers, cap(w.saverSlots), w.memory, limit, w.memoryLimit, 6*chunkBytes)
		}
		w.abort(errCutCOWAborted)
		if err := w.join(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestChunkLayerPayloadBudgetRejectsUndersized(t *testing.T) {
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(1)
	for _, budget := range []int64{-1, 1} {
		cfg.PayloadMemoryBudget = budget
		if _, err := attachChunkLayer(s, newFakeChunkRemote(), cfg); err == nil || !strings.Contains(err.Error(), "payload memory budget") {
			t.Fatalf("payload memory budget %d: got %v, want refusal", budget, err)
		}
	}
}
