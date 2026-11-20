package storage

import (
	"bytes"
	"testing"
)

// DroppedSuffixBirthStoreForTest exposes only the recovery fixture to the
// external-package regression, which uses chunkstore's real manifest builder.
func DroppedSuffixBirthStoreForTest(t *testing.T, remote ChunkRemote) *WALStore {
	t.Helper()
	s, path := newMappedCheckpointStoreV3(t)
	layer := mustAttach(t, s, remote, ctConfig(1))
	if _, err := s.Write(1, blk(0xA1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, blk(0xA2)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, blk(0xA3)); err != nil {
		t.Fatal(err)
	}
	prepared, err := s.prepareOwnedMapCheckpointV3(1)
	if err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	if e := prepared.candidate.entries[0]; e.LSN != 3 || d.CheckpointLSN != 1 || d.W != 1 {
		t.Fatalf("setup entry%+v cp%d W%d", e, d.CheckpointLSN, d.W)
	}
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err != nil {
		t.Fatal(err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	damageRecord(t, path, 2)
	reopen := func() *WALStore {
		r, err := openWALStoreV3ForTest(path)
		if err != nil {
			t.Fatal(err)
		}
		r.DisableAutoFlushForRecoveryTest()
		t.Cleanup(func() { _ = r.Close() })
		if _, err := r.Recover(); err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := reopen()
	if r.NextLSN() != 2 || r.v3.mapEntries[0].LSN != 3 {
		t.Fatal("missing conservative maximum after dropped suffix")
	}
	if n, err := r.Write(1, blk(0xB2)); err != nil || n != 2 {
		t.Fatalf("reused LSN %d: %v", n, err)
	}
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, r)
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r2 := reopen()
	if e := r2.v3.mapEntries[0]; e.LSN != 3 || !e.Unknown || r2.NextLSN() != 3 || !bytes.Equal(readV3(t, r2, 1), blk(0xB2)) {
		t.Fatalf("recovered reused image: entry%+v next%d", e, r2.NextLSN())
	}
	return r2
}
