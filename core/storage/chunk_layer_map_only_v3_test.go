package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"
)

func TestChunkLayerV3_MapOnlyCaptureNeedsNoLegacyBitmap(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 5, 5)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	cfg.BarrierBound = 1
	layer := mustAttach(t, s, f, cfg)
	first, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, first.Head)[5*ctBlockSize:6*ctBlockSize], blk(5)) {
		t.Fatal("map-only capture omitted checkpointed bytes")
	}
	if _, err = s.Write(5, blk(8)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	second, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, second.Head)[5*ctBlockSize:6*ctBlockSize], blk(8)) {
		t.Fatal("map-only checkpoint/capture omitted later bytes")
	}
	if _, err = os.Stat(path + ".chunkbitmap"); !os.IsNotExist(err) {
		t.Fatalf("impl3 created or used a legacy bitmap: %v", err)
	}
	if layer.Stats().DirtyChunks != 0 {
		t.Error("verified map-only capture remains dirty")
	}
}

func TestChunkLayerV3_IgnoresProvidedLegacyBitmap(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	b := newFakeBitmap()
	b.failSave = errors.New("legacy bitmap must not be used")
	cfg.Bitmap = b
	layer := mustAttach(t, s, f, cfg)
	if layer.Stats().DirtyChunks == 0 {
		t.Error("owned-map UNKNOWN missing from status")
	}
	writeSynced(t, s, 5, 5)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	calls := b.calls
	b.mu.Unlock()
	if calls != 0 {
		t.Errorf("impl3 invoked legacy bitmap %d times", calls)
	}
}
func TestChunkLayerV3_MapOnlyFreshZeroHEADPreservesLocalWrites(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 5, 5)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, f, cfg)
	result, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, result.Head)[5*ctBlockSize:6*ctBlockSize], blk(5)) {
		t.Fatal("fresh zero HEAD discarded checkpointed local bytes")
	}
}
func TestChunkLayerV3_MapOnlyRefusesLegacyBASEHeaderMutation(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	layer := mustAttach(t, s, newFakeChunkRemote(), cfg).(*chunkLayerImpl)
	before := s.v3.pub.current()
	if err := layer.BeginBaseInstall(); !errors.Is(err, errV3NotYet) {
		t.Fatalf("legacy begin mutated impl3 BASE: %v", err)
	}
	if err := layer.EndBaseInstall(0); !errors.Is(err, errV3NotYet) {
		t.Fatalf("legacy end mutated impl3 BASE: %v", err)
	}
	if s.v3.pub.current() != before {
		t.Error("legacy BASE callback published a descriptor")
	}
	if _, err := os.Stat(path + ".chunkbitmap"); !os.IsNotExist(err) {
		t.Fatalf("BASE refusal created a legacy bitmap: %v", err)
	}
}
func TestChunkLayerV3_MapStatusConcurrentClose(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	layer := mustAttach(t, s, newFakeChunkRemote(), cfg)
	begun, done := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(ctLocalBound):
			t.Error("status worker not joined")
		}
	})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			layer.Stats()
			if i == 0 {
				close(begun)
			}
		}
	}()
	select {
	case <-begun:
	case <-time.After(ctLocalBound):
		t.Fatal("status worker did not begin")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(ctLocalBound):
		t.Fatal("status during Close did not settle")
	}
}
