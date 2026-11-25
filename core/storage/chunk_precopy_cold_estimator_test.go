package storage

import (
	"bytes"
	"testing"
	"time"
)

// A published image with no remaining local tail needs no upload-rate history.
// The sample window is cleared to model a holder restart after the first cut.
func TestPrecopyColdPublishedWriteRetryConverges(t *testing.T) {
	s, f, layer, _ := precopyLayer(t, 1024, 0)
	if _, err := s.Write(1, makeBlock(ctBlockSize, 0x71)); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	layer.precopySamples = nil
	for attempt := 0; attempt < 3; attempt++ {
		stopped := 0
		r, err := layer.PrecopyDetach(ctTimeout(t), func() bool { stopped++; return true })
		if err == nil {
			if stopped != 1 || !r.Precopy.Frozen || r.Precopy.TailBytes != 0 || r.Precopy.RateSamples != 0 ||
				!bytes.Equal(f.imageOf(t, r.Head)[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 0x71)) {
				t.Fatalf("cold empty-tail detach: stopped=%d report=%+v", stopped, r.Precopy)
			}
			return
		}
		if stopped != 0 {
			t.Fatalf("refused attempt stopped I/O: %v", err)
		}
	}
	t.Fatal("empty-tail detach did not converge in three retries")
}

// The first rolling upload is an observation. A later non-empty tail may use
// that measured rate; it must not require three pre-existing history entries.
func TestPrecopyColdNonemptyTailUsesFirstMeasuredRound(t *testing.T) {
	s, f, layer, _ := precopyLayer(t, 1024, 0)
	if _, err := s.Write(1, makeBlock(ctBlockSize, 0x72)); err != nil {
		t.Fatal(err)
	}
	rounds := 0
	layer.testBeforeBarrierLockHook = func() {
		rounds++
		if rounds != 2 {
			return
		}
		if len(layer.precopySamples) != 1 || f.stats().head.Gen == 0 {
			t.Fatalf("first upload did not leave one measured observation: samples=%d", len(layer.precopySamples))
		}
		// Inject the measured sample values; the transition from zero to one
		// observation is the fact under test, independent of scheduler speed.
		layer.precopySamples[0].rate = float64(ctChunkBytes) / (10 * time.Millisecond).Seconds()
		layer.precopySamples[0].fixed = 10 * time.Millisecond
		if _, err := s.Write(1, makeBlock(ctBlockSize, 0x73)); err != nil {
			t.Fatal(err)
		}
	}
	stopped := 0
	r, err := layer.PrecopyDetach(ctTimeout(t), func() bool { stopped++; return true })
	if err != nil || stopped != 1 || !r.Precopy.Frozen || r.Precopy.RateSamples != 1 || rounds != 2 ||
		!bytes.Equal(f.imageOf(t, r.Head)[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 0x73)) {
		t.Fatalf("first measured round did not admit remaining tail: rounds=%d stopped=%d report=%+v err=%v", rounds, stopped, r.Precopy, err)
	}
}
