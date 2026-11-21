package storage

import (
	"testing"
	"time"
)

// TestPrecopySampleHistoryKeepsNewest32 pins that the pre-copy cost sample
// history is bounded at 32 entries and keeps the newest ones.
func TestPrecopySampleHistoryKeepsNewest32(t *testing.T) {
	l := &chunkLayerImpl{}
	const taken = 40
	for i := 1; i <= taken; i++ {
		// The fixed cost carries the sample's order.
		l.samplePrecopy(chunkCutStats{CASNs: 1}, int64(i))
	}
	if len(l.precopySamples) != 32 {
		t.Fatalf("sample history holds %d entries, want 32", len(l.precopySamples))
	}
	for j, s := range l.precopySamples {
		if want := time.Duration(taken - 32 + 1 + j); s.fixed != want {
			t.Fatalf("sample %d has fixed=%s, want %s (newest 32 kept in order)", j, s.fixed, want)
		}
	}
}
