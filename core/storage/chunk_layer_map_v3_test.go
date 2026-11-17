package storage

import (
	"bytes"
	"testing"
)

func TestChunkLayerV3_CaptureUsesOwnedMapAfterCheckpoint(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 5, 5)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.dm.len() != 0 {
		t.Fatal("setup requires checkpointed data with no WAL dirty entry")
	}
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	// The legacy bitmap deliberately reports clean in the current HEAD period.
	// The owned impl3 map, not this stale fixture, must still select local data.
	if err := cfg.Bitmap.WriteHeader(chunkBitmapHeader{}); err != nil {
		t.Fatal(err)
	}
	layer := mustAttach(t, s, f, cfg)
	cut, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	image := f.imageOf(t, cut.Head)
	if !bytes.Equal(image[5*ctBlockSize:6*ctBlockSize], blk(5)) {
		t.Error("capture omitted checkpointed data named by the owned map")
	}
	es, _, err := readChunkLSNMapV3(s.v3.ownedMap.fd, s.v3.pub.current(), 4)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range es {
		if e.Unknown {
			t.Errorf("verified captured chunk%d still UNKNOWN", i)
		}
	}
	again, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Noop || again.Stats.CandidateChunks != 0 {
		t.Errorf("verified clean cut reread chunks: noop=%v candidates=%d", again.Noop, again.Stats.CandidateChunks)
	}
	assertNoRemoteMisuse(t, f)
}
