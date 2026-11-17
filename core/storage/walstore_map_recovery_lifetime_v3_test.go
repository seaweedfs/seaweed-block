package storage

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWALStoreV3_FlushMaterializationBoundToRecoveryLifetime(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1) // W1, still in the WAL.
	if lsn, err := s.Write(5, blk(0xA7)); err != nil || lsn != 2 {
		t.Fatalf("write A: %d %v", lsn, err)
	}
	oldOff, _, _, _, ok := s.dm.get(5)
	if !ok {
		t.Fatal("missing A")
	}
	entered, resume, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	s.flusher.afterExtentSyncForTest = func() { close(entered); <-resume }
	t.Cleanup(func() {
		release()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("old flusher not joined")
		}
	})
	go func() { defer close(joined); done <- s.flusher.flushOnce() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old extent materialization not reached")
	}
	// Damage only the unacknowledged A record after its bytes reached extent.
	// Recover keeps W1, drops/fences A, and reuses its LSN and physical offset.
	pos := int64(s.sb.WALOffset + oldOff + walEntryPrefixSizeV3)
	if _, err := s.fd.WriteAt([]byte{0xA6}, pos); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Recover(); err != nil || r != 1 {
		t.Fatalf("recover drop: %d %v", r, err)
	}
	if lsn, err := s.Write(5, blk(0xB8)); err != nil || lsn != 2 {
		t.Fatalf("write B: %d %v", lsn, err)
	}
	off, _, _, _, ok := s.dm.get(5)
	if !ok || off != oldOff {
		t.Fatal("control did not reuse A offset")
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	before := s.v3.pub.current()
	release()
	select {
	case err := <-done:
		if !errors.Is(err, ErrMapCoverageStale) {
			t.Errorf("cross-Recover cycle accepted: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old cycle did not finish")
	}
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("old flusher result without join")
	}
	s.flusher.afterExtentSyncForTest = nil
	if s.v3.pub.current() != before {
		t.Error("old materialization published a new checkpoint/map")
	}
	if _, _, lsn, _, ok := s.dm.get(5); !ok || lsn != 2 {
		t.Error("old cycle retired reused acknowledged B")
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	if d.CheckpointLSN != 2 || d.MapCoveredCheckpoint != 2 {
		t.Fatalf("fresh retry cp/C=%d/%d", d.CheckpointLSN, d.MapCoveredCheckpoint)
	}
	extent := make([]byte, 4096)
	if _, err := s.fd.ReadAt(extent, int64(s.extentBase+5*4096)); err != nil || !bytes.Equal(extent, blk(0xB8)) {
		t.Fatalf("fresh retry did not materialize B: %v", err)
	}
	crashRecoveryHardeningStore(t, s) // no Close or final flush
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err = r.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := r.Read(5)
	if err != nil || !bytes.Equal(got, blk(0xB8)) {
		t.Fatalf("reopen lost acknowledged B: %v", err)
	}
}
