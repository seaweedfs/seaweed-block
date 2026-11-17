package storage

import (
	"bytes"
	"errors"
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

func attachCleanMapFixtureV3(t *testing.T, s *WALStore, f *fakeChunkRemote, head chunkHead) chunkLayer {
	t.Helper()
	cfg := ctConfig(1)
	if err := cfg.Bitmap.WriteHeader(chunkBitmapHeader{BaseLSN: head.RemoteLSN, HeadGen: head.Gen, Epoch: head.Epoch}); err != nil {
		t.Fatal(err)
	}
	return mustAttach(t, s, f, cfg)
}
func TestChunkLayerV3_MaxLSNSelectsAfterReopenWithCleanLegacyBitmap(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 5, 5)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	layer := attachCleanMapFixtureV3(t, s, f, chunkHead{})
	first, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { r.Close() })
	if _, err = r.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Write(5, blk(7)); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = r.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	es, _, err := readChunkLSNMapV3(r.v3.ownedMap.fd, r.v3.pub.current(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if es[1].Unknown || es[1].LSN != 2 || r.dm.len() != 0 {
		t.Fatalf("setup needs known map maxLSN2 and no WAL dirt: %+v", es[1])
	}
	secondLayer := attachCleanMapFixtureV3(t, r, f, first.Head)
	second, err := secondLayer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if second.Stats.CandidateChunks != 1 {
		t.Errorf("max-LSN incremental selection candidates=%d want1", second.Stats.CandidateChunks)
	}
	image := f.imageOf(t, second.Head)
	if !bytes.Equal(image[5*ctBlockSize:6*ctBlockSize], blk(7)) {
		t.Error("max-LSN map entry omitted checkpointed update")
	}
}
func TestChunkLayerV3_ChangedCaptureTokenKeepsUNKNOWN(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 5, 5)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	layer := attachCleanMapFixtureV3(t, s, f, chunkHead{})
	entered, release := holdPuts(t, f)
	done := startCapture(t, layer)
	waitKey(t, entered, "map capture upload")
	if _, err := s.Write(5, blk(9)); err != nil {
		t.Fatal(err)
	}
	release()
	if err := waitErr(t, done, "map capture after concurrent write"); err != nil {
		t.Fatal(err)
	}
	head := f.stats().head
	if head.RemoteLSN != 1 || !bytes.Equal(f.imageOf(t, head)[5*ctBlockSize:6*ctBlockSize], blk(5)) {
		t.Error("published capture is not its barrier image")
	}
	es, _, err := readChunkLSNMapV3(s.v3.ownedMap.fd, s.v3.pub.current(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if !es[1].Unknown || es[1].LSN != 2 {
		t.Fatalf("changed chunk token was cleared: %+v", es[1])
	}
	next, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, next.Head)[5*ctBlockSize:6*ctBlockSize], blk(9)) {
		t.Error("next cut missed post-barrier write")
	}
	es, _, err = readChunkLSNMapV3(s.v3.ownedMap.fd, s.v3.pub.current(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if es[1].Unknown {
		t.Error("fresh verified version did not clear UNKNOWN")
	}
}
func TestChunkLayerV3_UnsettledCaptureNeverClearsUNKNOWN(t *testing.T) {
	for _, mode := range []string{"intent_failure", "cas_unknown"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			writeSynced(t, s, 5, 5)
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			f := newFakeChunkRemote()
			layer := attachCleanMapFixtureV3(t, s, f, chunkHead{})
			if mode == "intent_failure" {
				f.failPutIntent = errors.New("intent unavailable")
			} else {
				f.casModes = []fakeCASMode{casUnknownNotApplied}
			}
			before := s.v3.pub.current()
			if _, err := layer.Cut(ctTimeout(t)); err == nil {
				t.Fatal("unsettled cut returned success")
			}
			after := s.v3.pub.current()
			if after.MapRootHash != before.MapRootHash || after.MapGeneration != before.MapGeneration {
				t.Error("unsettled capture published a clean map root")
			}
			es, _, err := readChunkLSNMapV3(s.v3.ownedMap.fd, after, 4)
			if err != nil {
				t.Fatal(err)
			}
			for i, e := range es {
				if !e.Unknown {
					t.Errorf("unsettled chunk%d lost UNKNOWN", i)
				}
			}
		})
	}
}
func TestChunkLayerV3_RefusesMapGeometryBeforeRemote(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.ChunkBlocks = 8
	if _, err := attachChunkLayer(s, f, cfg); !errors.Is(err, ErrMapInvalid) {
		t.Fatalf("mismatched map geometry=%v", err)
	}
	if f.stats().getHeads != 0 {
		t.Error("geometry refusal contacted the remote")
	}
}
