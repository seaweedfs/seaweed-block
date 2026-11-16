package storage

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
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

func TestDurabilityLatencyGroupWaitIsPerRequest(t *testing.T) {
	var calls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{MaxBatch: 2, MaxDelay: time.Second, SyncFunc: func() error { calls.Add(1); return nil }})
	go gc.Run()
	defer gc.Stop()
	result := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { result <- gc.SyncCache() }()
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("group did not settle")
		}
	}
	if calls.Load() != 1 || gc.waitLatency.Snapshot().Count != 2 {
		t.Fatalf("calls %d queue %+v", calls.Load(), gc.waitLatency.Snapshot())
	}
}
