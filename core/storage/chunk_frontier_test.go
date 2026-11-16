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
	// The failed fsync is sticky for this open store (ErrFsyncFailed):
	// neither a same-store Recover nor a seed retry may run on it.
	s.syncDirectFrontierMetadata = syncMetadata
	if _, err := s.Recover(); !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("same-store Recover after a failed fsync: %v; want ErrFsyncFailed", err)
	}
	if err := s.SeedChunkFrontier(7); !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("same-store seed retry after a failed fsync: %v; want ErrFsyncFailed", err)
	}
	_ = s.Close()
	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopenedOpen := true
	t.Cleanup(func() {
		if reopenedOpen {
			_ = reopened.Close()
		}
	})
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.SeedChunkFrontier(7); err != nil {
		t.Fatalf("seed after a reopen: %v", err)
	}
	if reopened.NextLSN() != 8 {
		t.Fatal("seed after a reopen not applied")
	}
	reopened.committer.Stop()
	_ = reopened.fd.Close() // crash
	reopenedOpen = false
	again, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if _, err := again.Recover(); err != nil {
		t.Fatal(err)
	}
	if r, _, h := again.Boundaries(); again.CheckpointLSN() != 7 || r != 7 || h != 7 || again.NextLSN() != 8 {
		t.Fatalf("seed after a crash: checkpoint=%d R=%d H=%d next=%d want 7/7/7/8", again.CheckpointLSN(), r, h, again.NextLSN())
	}
}
