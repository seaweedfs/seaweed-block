package replication

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// A primary process restarted at the same epoch and address (its line kept
// through the boot grace) starts a new session order at 1, while each
// replica still holds the dead process's latest session. The master
// re-issues the line for the new process at a new endpoint version, so the
// new process's live ship and its catch-up both outrank the held session
// and are admitted. At the old endpoint version both are refused as older.
func TestRestartedPrimaryAtTheSameEpochIsAdmitted(t *testing.T) {
	dir := t.TempDir()
	replica := storage.NewBlockStore(64, 4096)
	floor := filepath.Join(dir, "v1.floor")
	if err := transport.InitReplicaFloor(floor, "v1"); err != nil {
		t.Fatal(err)
	}
	listener, err := transport.NewReplicaListenerWithFloorAndApplyHook("127.0.0.1:0", replica, "v1", floor, NewReplicaApplyGate(replica))
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	target := targetFor("r1", listener.Addr(), 1, 1)
	primary := storage.NewBlockStore(64, 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	write := func(v *ReplicationVolume, lba uint32, fill byte) {
		t.Helper()
		data := bytes.Repeat([]byte{fill}, 4096)
		lsn, err := primary.Write(lba, data)
		if err != nil {
			t.Fatal(err)
		}
		if err := v.Observe(ctx, lba, lsn, data); err != nil {
			t.Fatalf("ship LSN %d: %v", lsn, err)
		}
		if err := v.Sync(ctx, lsn); err != nil {
			t.Fatalf("barrier at LSN %d: %v", lsn, err)
		}
	}

	// The old process: its peer re-added a few times (each a live
	// session), then it dies.
	old := NewReplicationVolume("v1", primary)
	for gen := uint64(1); gen <= 6; gen += 2 {
		if err := old.UpdateReplicaSet(gen, []ReplicaTarget{target}); err != nil {
			t.Fatal(err)
		}
		write(old, uint32(gen), byte(gen))
		if err := old.UpdateReplicaSet(gen+1, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := old.UpdateReplicaSet(7, []ReplicaTarget{target}); err != nil {
		t.Fatal(err)
	}
	write(old, 7, 7)
	_ = old.Close()

	// The restarted process, on its re-issued line: same epoch and
	// address, the next endpoint version.
	restarted := NewReplicationVolume("v1", primary)
	t.Cleanup(func() { _ = restarted.Close() })
	reissued := targetFor("r1", listener.Addr(), 1, 2)
	if err := restarted.UpdateReplicaSet(1, []ReplicaTarget{reissued}); err != nil {
		t.Fatal(err)
	}
	write(restarted, 8, 8)
	if got, err := replica.Read(8); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{8}, 4096)) {
		t.Fatalf("the restarted primary's live ship did not land (err %v): refused as an older session", err)
	}

	exec := restarted.peers["r1"].Executor()
	closed := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(adapter.SessionStartResult) {})
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closed <- r })
	end, _ := replica.Sync()
	_, _, head := primary.Boundaries()
	if err := exec.StartCatchUp("r1", exec.SessionIDs().Next(), 1, 2, end+1, head); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-closed:
		if !r.Success {
			t.Fatalf("the restarted primary's catch-up was refused: %+v", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the catch-up never closed")
	}
}
