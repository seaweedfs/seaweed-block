package transport

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func retainedBaseStore(t *testing.T, name string) *storage.WALStore {
	t.Helper()
	s, err := storage.CreateWALStoreWithOptions(filepath.Join(t.TempDir(), name), 64, 4096, storage.WALStoreOptions{ChunkBlocks: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func retainedBaseRebuild(t *testing.T, primary storage.LogicalStorage, l *ReplicaListener, session uint64) bool {
	t.Helper()
	_, _, pH := primary.Boundaries()
	exec := NewBlockExecutor(primary, l.Addr())
	done := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { done <- r })
	if err := exec.StartRebuild("r2", session, 1, 1, pH); err != nil {
		return false
	}
	select {
	case r := <-done:
		return r.Success
	case <-time.After(30 * time.Second):
		t.Fatal("rebuild did not close")
		return false
	}
}

// A replica removed and added back with its older store (its BASE still
// installed) cannot be rebuilt on v1: the listener reports the typed reason
// rebuild_retained_base, so the operator sees which replica to wipe and
// re-add. A brand-new replica's rebuild reports nothing.
func TestReplicaReportsARetainedBaseRebuildRefusal(t *testing.T) {
	primary := retainedBaseStore(t, "primary")
	for lba := uint32(0); lba < 8; lba++ {
		if _, err := primary.Write(lba, bytes.Repeat([]byte{0x40 + byte(lba)}, 4096)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	l, err := NewReplicaListener("127.0.0.1:0", retainedBaseStore(t, "replica"))
	if err != nil {
		t.Fatal(err)
	}
	l.Serve()
	t.Cleanup(l.Stop)

	if !retainedBaseRebuild(t, primary, l, 7) {
		t.Fatal("the brand-new replica's rebuild failed")
	}
	if got := l.RebuildRetainedBase(); got != "" {
		t.Fatalf("after a completed rebuild: %q; want none", got)
	}
	if _, err := primary.Write(0, bytes.Repeat([]byte{0x77}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	if retainedBaseRebuild(t, primary, l, 8) {
		t.Fatal("the re-added replica with its older store was rebuilt: the v1 limit is gone; update this test and the operator note")
	}
	if got := l.RebuildRetainedBase(); got != RebuildRetainedBaseReason {
		t.Fatalf("after the refused rebuild: %q; want %s", got, RebuildRetainedBaseReason)
	}
}
