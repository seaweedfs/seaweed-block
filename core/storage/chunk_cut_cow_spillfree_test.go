package storage

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The spill cap is min(COWSpillLimit, free/2 - reserve), at least 0, read
// from COWSpillFree on every call; an error counts as no free space, and
// without COWSpillFree the configured cap applies.
func TestCOWSpillLimit_FromFreeSpace(t *testing.T) {
	l := &chunkLayerImpl{cfg: ChunkLayerConfig{COWSpillLimit: 4 << 30}}
	if got := l.cowSpillLimit(); got != 4<<30 {
		t.Fatalf("no free-space reader: cap %d, want the configured 4 GiB", got)
	}
	var free atomic.Int64
	var reads atomic.Int64
	var fail atomic.Bool
	l.cfg.COWSpillFree = func() (int64, error) {
		reads.Add(1)
		if fail.Load() {
			return 1 << 40, errors.New("statfs failed")
		}
		return free.Load(), nil
	}
	for _, c := range []struct {
		free, want int64
	}{
		{100 << 30, 4 << 30},             // plenty: the configured cap
		{6 << 30, 2 << 30},               // 6 GiB free: half is 3, minus the 1 GiB reserve
		{2 << 30, 0},                     // half the free space is all reserve
		{1 << 30, 0},                     // below the reserve: never negative
		{10<<30 + 2, 4 << 30},            // the configured cap still bounds it
		{3 << 30, (3<<30)/2 - (1 << 30)}, // 1.5 GiB - 1 GiB
	} {
		free.Store(c.free)
		if got := l.cowSpillLimit(); got != c.want {
			t.Fatalf("free %d: cap %d, want %d", c.free, got, c.want)
		}
	}
	if reads.Load() != 6 {
		t.Fatalf("free space read %d times for 6 calls; want once per call", reads.Load())
	}
	fail.Store(true)
	if got := l.cowSpillLimit(); got != 0 {
		t.Fatalf("free-space error: cap %d, want 0 (no free space)", got)
	}
}

// The copy window reads the free space when it opens, not only when the
// cut starts staging: free space that shrinks below the reserve between
// the two gives the window a cap of 0, so once the in-memory budget is used
// a save aborts that cut (the guest write completes); after space is freed
// the cap reads the new value.
func TestCutCOWV3_SpillCapFollowsFreeSpaceEachCut(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	var free, reads atomic.Int64
	free.Store(100 << 30)
	l.cfg.COWSpillFree = func() (int64, error) {
		if reads.Add(1) == 2 { // the window opens: the filesystem has filled up
			free.Store(1 << 30)
		}
		return free.Load(), nil
	}
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit // the in-memory budget is used
	limit := w.spillLimit
	w.mu.Unlock()
	if limit != 0 {
		t.Fatalf("window cap %d with 1 GiB free; want 0", limit)
	}
	writeDone := make(chan error, 1)
	go func() { _, err := s.Write(5, makeBlock(ctBlockSize, 0xd6)); writeDone <- err }()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal("guest write refused at the free-space cap:", err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("guest write did not complete at the free-space cap")
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "cut at the free-space cap")
	if err := <-cutErr; !errors.Is(err, errCutCOWAborted) || !strings.Contains(err.Error(), "cap of 0 bytes") {
		t.Fatalf("cut at a 0 cap: %v, want the cap abort", err)
	}
	free.Store(100 << 30)
	if got := l.cowSpillLimit(); got != l.cfg.COWSpillLimit {
		t.Fatalf("after freeing space the cap is %d, want the configured %d", got, l.cfg.COWSpillLimit)
	}
}
