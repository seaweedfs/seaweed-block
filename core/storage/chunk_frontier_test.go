package storage

import (
	"errors"
	"testing"
)

func TestSeedChunkFrontierMetadataFailureRetry(t *testing.T) {
	s, path := newChunkTestStore(t)
	s.DisableAutoFlushForRecoveryTest()
	syncMetadata := s.syncDirectFrontierMetadata
	s.syncDirectFrontierMetadata = func([]byte) error { return errors.New("injected metadata fsync failure") }
	if err := s.SeedChunkFrontier(7); err == nil {
		t.Fatal("failed metadata sync accepted")
	}
	if next := s.NextLSN(); next != 1 {
		t.Fatalf("failed seed advanced NextLSN to %d", next)
	}
	if r, _, h := s.Boundaries(); r != 0 || h != 0 {
		t.Fatal("failed seed published frontier", r, h)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	s.syncDirectFrontierMetadata = syncMetadata
	if err := s.SeedChunkFrontier(7); err != nil {
		t.Fatal(err)
	}
	if s.NextLSN() != 8 {
		t.Fatal("retry not seeded")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	if reopened.NextLSN() != 8 {
		t.Fatal("durable seed missing after reopen")
	}
}
