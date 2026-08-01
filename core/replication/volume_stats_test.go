package replication

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestReplicationVolume_StatsCountWriteAndSyncContention(t *testing.T) {
	store := storage.NewBlockStore(8, 4096)
	v := NewReplicationVolume("stats", store)
	t.Cleanup(func() { _ = v.Close() })

	data := make([]byte, 4096)
	lsn, err := store.Write(0, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.OnLocalWrite(context.Background(), LocalWrite{LBA: 0, LSN: lsn, Data: data}); err != nil {
		t.Fatal(err)
	}
	if err := v.Sync(context.Background(), lsn); err != nil {
		t.Fatal(err)
	}

	got := v.Stats()
	if got.WriteOps != 1 || got.SyncOps != 1 {
		t.Fatalf("stats counts=%+v want one write and one sync", got)
	}
	if got.WriteLockWaitNanos == 0 || got.WriteFanoutNanos == 0 ||
		got.SyncLockWaitNanos == 0 || got.SyncDurationNanos == 0 {
		t.Fatalf("stats missing duration evidence: %+v", got)
	}
}
