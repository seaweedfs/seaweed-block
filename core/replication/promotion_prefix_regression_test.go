package replication

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// Promotion must establish a common prefix, not infer one from equal lengths.
// The old tail at position 2 was never acknowledged to the guest. The new
// primary independently wrote different bytes at that position in epoch 2.
func TestPromotionRejoinNeverAcknowledgesConflictingSameLengthTail(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		name := "common_prefix"
		if conflict {
			name = "old_epoch_tail"
		}
		t.Run(name, func(t *testing.T) {
			open := func(name string) storage.LogicalStorage {
				s, err := storage.CreateWALStore(filepath.Join(t.TempDir(), name), 64, 4096)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			}
			primary, replica := open("new-primary.wal"), open("rejoining.wal")
			common := bytes.Repeat([]byte{0x11}, 4096)
			newer := bytes.Repeat([]byte{0x22}, 4096)
			older := bytes.Repeat([]byte{0x33}, 4096)
			for _, s := range []storage.LogicalStorage{primary, replica} {
				if _, err := s.Write(0, common); err != nil {
					t.Fatal(err)
				}
			}
			target, err := primary.Write(1, newer)
			if err != nil {
				t.Fatal(err)
			}
			tail := newer
			if conflict {
				tail = older
			}
			oldTarget, err := replica.Write(1, tail)
			if err != nil || oldTarget != target {
				t.Fatalf("fixture positions: old=%d new=%d err=%v", oldTarget, target, err)
			}
			if _, err := replica.Sync(); err != nil {
				t.Fatal(err)
			}
			listener, err := transport.NewReplicaListener("127.0.0.1:0", replica)
			if err != nil {
				t.Fatal(err)
			}
			// The epoch tables production keeps beside each store: both wrote
			// LSN 1 at epoch 1; the new primary wrote LSN 2 at epoch 2. The
			// rejoining replica's LSN 2 is the old primary's epoch-1 tail, or
			// (control) the new primary's epoch-2 entry.
			own, _ := transport.LoadEpochTable("", "promotion-prefix")
			_ = own.Replace([]transport.EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: target}})
			theirs, _ := transport.LoadEpochTable("", "promotion-prefix")
			if conflict {
				_ = theirs.Replace([]transport.EpochRow{{Epoch: 1, FirstLSN: 1}})
			} else {
				_ = theirs.Replace([]transport.EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: target}})
			}
			listener.SetEpochTable(theirs)
			// Re-admission raises the epoch floor; it must also prevent a divergent
			// old tail from satisfying the new primary's durability acknowledgement.
			if err := listener.AdmitAsReplica(2); err != nil {
				t.Fatal(err)
			}
			listener.Serve()
			t.Cleanup(listener.StopHard)
			v := NewReplicationVolume("promotion-prefix", primary)
			v.SetDurabilityMode(DurabilitySyncAll)
			v.SetEpochTable(own)
			t.Cleanup(func() { _ = v.Close() })
			if err := v.UpdateReplicaSet(2, []ReplicaTarget{targetFor("rejoining", listener.Addr(), 2, 1)}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err = v.Sync(ctx, target)
			if err != nil {
				if !conflict {
					t.Fatalf("identical-prefix control could not acknowledge: %v", err)
				}
				if ctx.Err() != nil {
					t.Fatalf("timeout is not a prefix refusal: %v", err)
				}
				t.Logf("conflicting tail refused: %v", err)
				return
			}
			got, err := replica.Read(1)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, newer) {
				t.Fatalf("ACK by length accepted old-epoch bytes at LSN %d: replica=%x new-primary=%x", target, got[:8], newer[:8])
			}
		})
	}
}
