package storage

import (
	"testing"
	"time"
)

// Metadata-only maximum-volume/default-chunk shape; no volume-sized image or
// device is allocated. This measures the added sampler, not a complete cut.
func BenchmarkCaptureBirthsMaxVolumeDefaultChunk(b *testing.B) {
	const count = 1 << 22 // ceil((16 TiB - 4 KiB) / 4 MiB)
	candidates := make(map[uint64]bool, count)
	s := &WALStore{v3: &walStoreV3State{mapEntries: make([]chunkLSNEntryV3, count), mapProof: newMapProofEpochV3()}}
	for i := uint64(0); i < count; i++ {
		candidates[i] = true
		s.v3.mapEntries[i].LSN = i + 1
	}
	l := &chunkLayerImpl{s: s}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.captureMu.Lock()
		out := l.captureBirths(candidates, count)
		s.captureMu.Unlock()
		if len(out) != count || out[0] != 1 || out[count-1] != count {
			b.Fatal("worst-shape birth mismatch")
		}
	}
}
func TestCaptureBirthsIncludesStoreLockWait(t *testing.T) {
	s := &WALStore{v3: &walStoreV3State{mapEntries: []chunkLSNEntryV3{{LSN: 7}}, mapProof: newMapProofEpochV3()}}
	l := &chunkLayerImpl{s: s}
	candidates := map[uint64]bool{0: true}
	s.mu.Lock()
	locked := true
	done := make(chan map[uint64]uint64, 1)
	t.Cleanup(func() {
		if locked {
			s.mu.Unlock()
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("sampler did not join")
		}
	})
	go func() { s.captureMu.Lock(); out := l.captureBirths(candidates, 7); s.captureMu.Unlock(); done <- out }()
	select {
	case out := <-done:
		done <- out
		t.Fatal("sampler bypassed store lock")
	case <-time.After(25 * time.Millisecond):
	}
	s.mu.Unlock()
	locked = false
	select {
	case out := <-done:
		done <- out
		if out[0] != 7 {
			t.Fatal("native image frontier changed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sampler did not finish after lock release")
	}
}
