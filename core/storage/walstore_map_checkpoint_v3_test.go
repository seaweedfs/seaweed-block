package storage

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
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
	crashRecoveryHardeningStore(t, s) // no Close/final flush or descriptor refresh
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock() // cleanup must not touch the crashed file
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
	for _, stage := range []string{"file", "root", "directory"} {
		t.Run(stage, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			writeSynced(t, s, 1, 1)
			before := s.v3.pub.current()
			injected := errors.New("injected map persistence failure")
			if stage == "root" {
				realSync := s.v3.ownedMap.syncFile
				calls := 0
				s.v3.ownedMap.syncFile = func() error {
					calls++
					if calls == 2 {
						return injected
					}
					return realSync()
				}
			} else if stage == "file" {
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

func TestWALStoreV3_PreparedCheckpointRefusesChangedProof(t *testing.T) {
	for _, event := range []string{"write", "recover"} {
		t.Run(event, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			writeSynced(t, s, 1, 1)
			entered, resume, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			s.v3.testMapPrepared = func() { close(entered); <-resume }
			t.Cleanup(func() {
				release()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("flusher did not join")
				}
			})
			go func() { defer close(joined); done <- s.flusher.flushOnce() }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("actual flusher did not persist a map candidate")
			}
			if event == "write" {
				if _, err := s.Write(2, blk(2)); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := s.Recover(); err != nil {
					t.Fatal(err)
				}
			}
			before := s.v3.pub.current()
			release()
			select {
			case err := <-done:
				if event == "recover" && !errors.Is(err, ErrMapCoverageStale) {
					t.Errorf("stale prepared checkpoint=%v", err)
				} else if event == "write" && err != nil {
					t.Errorf("newer retained append invalidated prefix coverage: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("flusher did not settle")
			}
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Fatal("flusher result without join")
			}
			s.v3.testMapPrepared = nil
			if event == "recover" && s.v3.pub.current() != before {
				t.Error("stale proof published a checkpoint")
			} else if event == "write" && s.CheckpointLSN() != 1 {
				t.Error("newer retained append blocked the older checkpoint")
			}
			if s.stickyIOFault() != nil {
				t.Error("proof refusal incorrectly latched a persistence fault")
			}
			if s.dm.len() == 0 {
				t.Error("proof refusal lost retry ownership")
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatalf("fresh candidate retry: %v", err)
			}
			want := uint64(1)
			if event == "write" {
				want = 2
			}
			d := s.v3.pub.current()
			if d.CheckpointLSN != want || d.MapCoveredCheckpoint != want {
				t.Fatalf("fresh retry cp=%d C=%d want%d", d.CheckpointLSN, d.MapCoveredCheckpoint, want)
			}
		})
	}
}
func TestWALStoreV3_AutomaticMapClampedProgressRetries(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	if err := s.SetWMode(WModeLagged); err != nil {
		t.Fatal(err)
	}
	writeSynced(t, s, 1, 1)
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	if d.CheckpointLSN != 1 || d.MapCoveredCheckpoint != 1 || s.dm.len() != 1 {
		t.Fatal("clamped automatic checkpoint lost retry ownership")
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	d = s.v3.pub.current()
	if d.CheckpointLSN != 2 || d.MapCoveredCheckpoint != 2 || s.dm.len() != 0 {
		t.Fatal("later Sync did not advance automatic checkpoint and coverage")
	}
}
func TestWALStoreV3_UncertainMapCheckpointPinsDependencies(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	before := s.v3.pub.current()
	injected := errors.New("descriptor response lost after full pwrite")
	realWrite := s.v3.pub.io.writeSlot
	s.v3.pub.io.writeSlot = func(off int64, b []byte) error {
		if err := realWrite(off, b); err != nil {
			return err
		}
		return injected
	}
	if err := s.flusher.flushOnce(); !errors.Is(err, ErrDescriptorPublishUncertain) {
		t.Fatalf("uncertain checkpoint=%v", err)
	}
	if s.v3.pub.current() != before || s.dm.len() != 1 {
		t.Fatal("unknown publication advanced in-memory checkpoint or retired dirty entries")
	}
	uncertain, ok := s.v3.pub.uncertainCandidate()
	if !ok {
		t.Fatal("unknown publication lost candidate pin")
	}
	if _, _, err := readChunkLSNMapV3(s.v3.ownedMap.fd, uncertain, 4); err != nil {
		t.Fatalf("unknown slot references missing map: %v", err)
	}
	if _, err := s.Sync(); err == nil {
		t.Fatal("unknown publication allowed Sync")
	}
	crashRecoveryHardeningStore(t, s)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { r.Close() })
	if _, err = r.Recover(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readV3(t, r, 1), blk(1)) {
		t.Fatal("resolved checkpoint lost extent data")
	}
}

type checkpointOrderMetadataV3 struct{ before func(uint64) error }

func (*checkpointOrderMetadataV3) OnExtentWrite(uint32, []byte) error        { return nil }
func (*checkpointOrderMetadataV3) VerifyExtent(uint32, []byte) error         { return nil }
func (h *checkpointOrderMetadataV3) BeforeCheckpointAdvance(cp uint64) error { return h.before(cp) }
func TestWALStoreV3_AutomaticCheckpointMetadataOrderAndRetry(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	dataDurable, metadataDone, mapDurable := false, false, false
	failBarrier := true
	injected := errors.New("metadata barrier refused")
	s.metadata = &checkpointOrderMetadataV3{before: func(cp uint64) error {
		if !dataDurable || cp != 1 || s.CheckpointLSN() != 0 {
			t.Error("metadata callback not between data sync and checkpoint")
		}
		if failBarrier {
			return injected
		}
		metadataDone = true
		return nil
	}}
	realExtentSync := s.syncExtentFile
	s.syncExtentFile = func() error {
		err := realExtentSync()
		if err == nil {
			dataDurable = true
		}
		return err
	}
	m := s.v3.ownedMap
	realMapSync, realDirectory := m.syncFile, m.syncDirectory
	m.syncFile = func() error {
		if !metadataDone {
			t.Error("map persisted before metadata barrier")
		}
		return realMapSync()
	}
	m.syncDirectory = func() error {
		err := realDirectory()
		if err == nil {
			mapDurable = true
		}
		return err
	}
	s.v3.testBeforeSlotWrite = func() error {
		if !metadataDone || !mapDurable {
			t.Error("descriptor published before map/metadata durability")
		}
		return nil
	}
	initial := s.v3.pub.current()
	if err := s.flusher.flushOnce(); !errors.Is(err, injected) {
		t.Fatalf("metadata failure=%v", err)
	}
	if s.v3.pub.current() != initial || m.lastGeneration != initial.MapGeneration || s.dm.len() != 1 {
		t.Fatal("metadata refusal published/retired progress or prepared a map")
	}
	failBarrier = false
	dataDurable = false
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if !metadataDone || !mapDurable || s.CheckpointLSN() != 1 {
		t.Fatal("retry did not complete ordered checkpoint")
	}
}
func TestWALStoreV3_AutomaticMapRecoverRebuildsCoverage(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 5, 5)
	crashRecoveryHardeningStore(t, s)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { r.Close() })
	if _, err = r.Recover(); err != nil {
		t.Fatal(err)
	}
	if err = r.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	es, _, err := readChunkLSNMapV3(r.v3.ownedMap.fd, r.v3.pub.current(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if es[1].LSN != 1 {
		t.Fatalf("replayed write omitted from map: %+v", es[1])
	}
}

func TestWALStoreV3_MapFailureRefusesQueuedSync(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	waiting, joined := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	s.v3.syncMu.Lock()
	release := func() { once.Do(func() { s.v3.syncMu.Unlock() }) }
	s.v3.testLeaderWaiting = func() { close(waiting) }
	t.Cleanup(func() {
		release()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("queued Sync did not join")
		}
	})
	go func() { defer close(joined); _, err := s.Sync(); done <- err }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("Sync did not queue for leader role")
	}
	injected := errors.New("map fsync failed while Sync queued")
	s.v3.ownedMap.syncFile = func() error { return injected }
	// Preparation is outside syncMu, so this failure must surface while the
	// later Sync remains queued behind the held leader role.
	flushed := make(chan error, 1)
	go func() { flushed <- s.flusher.flushOnce() }()
	select {
	case err := <-flushed:
		if !errors.Is(err, injected) {
			t.Fatalf("map failure=%v", err)
		}
	case <-time.After(5 * time.Second):
		// Release and join before failing; never abandon the flusher worker.
		release()
		<-flushed
		t.Fatal("idle pressure check blocked map preparation behind Sync")
	}
	if s.stickyIOFault() == nil {
		t.Error("map failure not globally latched before releasing Sync")
	}
	release()
	select {
	case err := <-done:
		if err == nil {
			t.Error("queued Sync acknowledged after map failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued Sync did not return")
	}
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("Sync result without join")
	}
	s.v3.testLeaderWaiting = nil
}
