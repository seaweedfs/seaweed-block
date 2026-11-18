package storage

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Row7 lifecycle slice: bind a durable map to the actual store publisher.
// This fixture does not claim automatic provider/chunk-layer map attachment.
func bindRecoveryMapV3(t *testing.T, s *WALStore) (*chunkLSNMapV3, *mapProofEpochV3, []chunkLSNEntryV3, *chunkMapCandidateV3) {
	t.Helper()
	d := s.v3.pub.current()
	m, err := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	shape, err := mapShapeForV3(d, 4)
	if err != nil {
		t.Fatal(err)
	}
	es := make([]chunkLSNEntryV3, int(shape.chunks))
	for i := range es {
		es[i].Unknown = true
	}
	c, err := m.Prepare(d.MapGeneration+1, d.CheckpointLSN, es, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.v3.pub.publish(&s.mu, nil, func(n *descriptorV3) error { *n = c.descriptor(*n); return nil }); err != nil {
		t.Fatal(err)
	}
	// Bootstrap both slots to the durable initial map before binding. The
	// older dark row-5 placeholder has no sidecar dependency and cannot remain
	// a valid fallback once the real map binding is installed.
	if _, err = s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	proof := newMapProofEpochV3()
	if err = s.v3.pub.bindMapV3(c, proof); err != nil {
		t.Fatal(err)
	}
	return m, proof, es, c
}

func TestWALStoreV3_RealRecoverInvalidatesMapProofAtStartAndEnd(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	m, proof, es, current := bindRecoveryMapV3(t, s)
	before, err := prepareMapPublicationV3(m, proof, current.generation+1, 0, es, current)
	if err != nil {
		t.Fatal(err)
	}
	startEpoch := proof.epoch.Load()
	entered, resume, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	done := make(chan error, 1)
	// The existing slot-write hook parks the real Recover after START and before END.
	s.v3.testBeforeSlotWrite = func() error { close(entered); <-resume; return nil }
	t.Cleanup(func() {
		release()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("Recover did not join")
		}
	})
	go func() { defer close(joined); _, e := s.Recover(); done <- e }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Recover did not reach its slot publication")
	}
	if before.proof.validV3(proof) || proof.epoch.Load() != startEpoch+1 {
		t.Error("actual Recover START did not invalidate the previous map proof")
	}
	during, err := prepareMapPublicationV3(m, proof, current.generation+2, 0, es, current)
	if err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Recover did not finish")
	}
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("Recover result arrived but worker did not join")
	}
	s.v3.testBeforeSlotWrite = nil
	if err != nil {
		t.Fatal(err)
	}
	if proof.epoch.Load() != startEpoch+2 {
		t.Error("actual Recover END did not advance the map proof epoch")
	}
	for label, prepared := range map[string]*preparedMapPublicationV3{"before": before, "during": during} {
		if _, err = s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); !errors.Is(err, ErrMapCoverageStale) {
			t.Errorf("%s real Recover candidate was not refused as stale: %v", label, err)
		}
	}
	fresh, err := prepareMapPublicationV3(m, proof, current.generation+3, 0, es, current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.v3.pub.publishMapV3(&s.mu, nil, fresh, nil); err != nil {
		t.Fatalf("fresh post-Recover candidate refused: %v", err)
	}
}

func TestWALStoreV3_FailedRecoverInvalidatesMapProofAtBothBoundaries(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.Write(0, blk(0x51)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	m, proof, es, current := bindRecoveryMapV3(t, s)
	before, err := prepareMapPublicationV3(m, proof, current.generation+1, 0, es, current)
	if err != nil {
		t.Fatal(err)
	}
	epoch := proof.epoch.Load()
	off, _, _, _, ok := s.dm.get(0)
	if !ok {
		t.Fatal("synced write missing from WAL dirty map")
	}
	// Corrupt an acknowledged WAL payload: R<W is a real recovery failure.
	if _, err = s.fd.WriteAt([]byte{0x52}, int64(s.sb.WALOffset+off+walEntryPrefixSizeV3)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Recover(); !isWALCorrupt(err) {
		t.Fatalf("damaged acknowledged WAL recovery verdict = %v, want WALIntegrity", err)
	}
	if proof.epoch.Load() != epoch+2 {
		t.Error("failed actual Recover must invalidate START and END")
	}
	if _, err = s.v3.pub.publishMapV3(&s.mu, nil, before, nil); !errors.Is(err, ErrMapCoverageStale) {
		t.Fatalf("pre-failure map candidate was not refused as stale: %v", err)
	}
}
