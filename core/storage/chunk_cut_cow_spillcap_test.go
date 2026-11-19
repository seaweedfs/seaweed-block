package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// P3: once the in-memory budget is used, a save that would take the side file
// past its byte cap aborts the cut (retried) instead of growing the file; the
// guest write still completes and no side file is left.
func TestCutCOWV3_SpillCapAbortsTheCut(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit    // the in-memory budget is used
	w.spillLimit = w.chunkBytes - 1 // the cap leaves no room for one chunk
	w.mu.Unlock()
	writeDone := make(chan error, 1)
	go func() { _, err := s.Write(5, makeBlock(ctBlockSize, 0xd5)); writeDone <- err }()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal("guest write refused by the spill cap:", err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("guest write did not complete when the spill cap aborted the cut")
	}
	w.mu.Lock()
	side, spilled := w.side, w.spillBytes
	w.mu.Unlock()
	if side != nil || spilled != 0 {
		t.Fatalf("side file %v / %d spilled bytes past the cap", side != nil, spilled)
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "capped cut")
	if err := <-cutErr; !errors.Is(err, errCutCOWAborted) || !strings.Contains(err.Error(), "spill would exceed its cap") {
		t.Fatalf("cut past the spill cap: %v, want the cap abort", err)
	}
	if _, err := l.Cut(context.Background()); err != nil {
		t.Fatal("retried cut:", err)
	}
}

// The default cap applies at attach (0), and a negative cap is refused.
func TestChunkLayer_COWSpillLimitDefaultAndRefusal(t *testing.T) {
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(1)
	cfg.COWSpillLimit = -1
	if _, err := attachChunkLayer(s, newFakeChunkRemote(), cfg); err == nil || !strings.Contains(err.Error(), "spill limit") {
		t.Fatalf("negative COW spill limit: %v, want a refusal", err)
	}
	cfg.COWSpillLimit = 0
	l := mustAttach(t, s, newFakeChunkRemote(), cfg).(*chunkLayerImpl)
	if l.cfg.COWSpillLimit != defaultCOWSpillLimit {
		t.Fatalf("COW spill limit %d, want the default %d", l.cfg.COWSpillLimit, defaultCOWSpillLimit)
	}
}

// A spill within the cap is accounted: with room for exactly one chunk the
// save spills, the window counts that chunk, and the cut completes.
func TestCutCOWV3_SpillWithinCapIsAccounted(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit
	w.spillLimit = w.chunkBytes
	w.mu.Unlock()
	if _, err := s.Write(5, makeBlock(ctBlockSize, 0xd5)); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	side, spilled := w.side, w.spillBytes
	w.mu.Unlock()
	if side == nil || spilled != w.chunkBytes {
		t.Fatalf("spill within the cap: side file %v, %d bytes accounted, want one chunk (%d)", side != nil, spilled, w.chunkBytes)
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "cut within the spill cap")
	if err := <-cutErr; err != nil {
		t.Fatal(err)
	}
}
