package storage

import (
	"errors"
	"testing"
)

func TestDurabilityLatencyCountsSharedPhysicalSyncAndDedup(t *testing.T) {
	s := newGenerationStore(t)
	if _, err := s.Write(0, makeBlock(4096, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	first := s.DurabilityLatency()
	if first.WALAppend.Count == 0 || first.GroupCommitWait.Count != 1 || first.FileSync.Count != 1 {
		t.Fatal(first)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	next := s.DurabilityLatency()
	if next.FileSync.Count != 1 || next.GroupCommitWait.Count != 1 {
		t.Fatal("dedup fabricated physical or queued sample", next)
	}
	if _, err := s.Write(1, makeBlock(4096, 2)); err != nil {
		t.Fatal(err)
	}
	s.syncWALFile = func() error { return errors.New("injected fsync") }
	if _, err := s.Sync(); err == nil {
		t.Fatal("failed sync acknowledged")
	}
	next = s.DurabilityLatency()
	if next.FileSync.Count != 2 || next.GroupCommitWait.Count != 2 {
		t.Fatal("failed physical attempt missing", next)
	}
}
