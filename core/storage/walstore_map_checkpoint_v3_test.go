package storage

import (
	"bytes"
	"errors"
	"testing"
)

func newMappedCheckpointStoreV3(t *testing.T) (*WALStore, string) {
	t.Helper()
	path := t.TempDir() + "/mapped"
	s, err := createMappedWALStoreV3ForTest(path, 64, 4096, v3TestWAL, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { s.Close() })
	return s, path
}
func TestWALStoreV3_AutomaticMapCheckpoint(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 2)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	if d.CheckpointLSN != 2 || d.MapCoveredCheckpoint != 2 || d.W < 2 {
		t.Fatalf("automatic map checkpoint stayed at cp=%d C=%d W=%d", d.CheckpointLSN, d.MapCoveredCheckpoint, d.W)
	}
	es, _, err := readChunkLSNMapV3(s.v3.ownedMap.fd, d, 4)
	if err != nil {
		t.Fatal(err)
	}
	if es[0].LSN != 2 || !es[0].Unknown {
		t.Errorf("map lost effective write coverage: %+v", es[0])
	}
	if s.dm.len() != 0 {
		t.Error("covered dirty entries not retired")
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
	for lba := uint32(1); lba <= 2; lba++ {
		if !bytes.Equal(readV3(t, r, lba), blk(byte(lba))) {
			t.Errorf("LBA%d lost after checkpoint reopen", lba)
		}
	}
}
func TestWALStoreV3_AutomaticMapPersistenceFailureLatchesStore(t *testing.T) {
	for _, stage := range []string{"file", "directory"} {
		t.Run(stage, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			writeSynced(t, s, 1, 1)
			before := s.v3.pub.current()
			injected := errors.New("injected map persistence failure")
			if stage == "file" {
				s.v3.ownedMap.syncFile = func() error { return injected }
			} else {
				s.v3.ownedMap.syncDirectory = func() error { return injected }
			}
			err := s.flusher.flushOnce()
			if !errors.Is(err, injected) {
				t.Errorf("automatic flusher did not surface map persistence failure: %v", err)
			}
			if s.stickyIOFault() == nil {
				t.Error("map persistence failure did not latch the whole store")
			}
			if s.v3.pub.current() != before {
				t.Error("failed map persistence published a descriptor")
			}
			if s.dm.len() != 1 {
				t.Error("failed map persistence retired dirty retry ownership")
			}
			if _, err = s.Sync(); err == nil {
				t.Error("Sync acknowledged after map persistence failure")
			}
			if _, err = s.Write(2, blk(2)); err == nil {
				t.Error("Write accepted after map persistence failure")
			}
			if _, err = s.Recover(); err == nil {
				t.Error("Recover accepted after map persistence failure")
			}
			// Close is expected to surface the latched failure and still owns cleanup.
		})
	}
}
