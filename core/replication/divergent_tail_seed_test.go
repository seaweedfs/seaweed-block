package replication

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// A survivor that holds an entry above the promoted primary's frontier holds
// a tail written at the old epoch that the new primary never had: it was never
// acknowledged (an acknowledged write is on the promotion target). The seed
// barrier must not accept it as caught up on length: the peer is degraded,
// its live cursor is not seeded, and it does not satisfy sync-quorum.
func TestReplicationVolume_UpdateReplicaSet_DegradesPeerAboveLocalFrontier(t *testing.T) {
	const blockSize = 4096
	primary := storage.NewBlockStore(64, blockSize)
	replica := storage.NewBlockStore(64, blockSize)

	for i := uint32(0); i < 3; i++ {
		data := make([]byte, blockSize)
		data[0] = byte(i + 1)
		data[1] = 0xC0
		lsn, err := primary.Write(i, data)
		if err != nil {
			t.Fatalf("primary seed write %d: %v", i, err)
		}
		if err := replica.ApplyEntry(i, data, lsn); err != nil {
			t.Fatalf("replica seed apply %d: %v", i, err)
		}
	}
	frontier, err := primary.Sync()
	if err != nil {
		t.Fatalf("primary seed sync: %v", err)
	}
	// The old primary's unacknowledged entry, shipped to this survivor only.
	tail := make([]byte, blockSize)
	tail[0], tail[1] = 0xEE, 0xEE
	if err := replica.ApplyEntry(5, tail, frontier+1); err != nil {
		t.Fatalf("replica old-epoch tail apply: %v", err)
	}
	if got, err := replica.Sync(); err != nil || got <= frontier {
		t.Fatalf("replica sync=(%d,%v), want above frontier %d", got, err, frontier)
	}

	listener, err := transport.NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(func() { listener.Stop() })

	v := NewReplicationVolume("vol-promoted", primary)
	v.SetDurabilityMode(DurabilitySyncQuorum)
	t.Cleanup(func() { _ = v.Close() })
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("r3", listener.Addr(), 2, 1)}); err != nil {
		t.Fatalf("UpdateReplicaSet: %v", err)
	}
	v.mu.Lock()
	state := v.peers["r3"].State()
	v.mu.Unlock()
	if state != ReplicaDegraded {
		t.Fatalf("peer state=%s want degraded for a survivor holding a divergent tail above the frontier", state)
	}
	// A flush must not count it either: its length covers the target, but it
	// is not in sync.
	if err := v.Sync(context.Background(), frontier); err == nil || !strings.Contains(err.Error(), ErrDurabilityQuorumLost.Error()) {
		t.Fatalf("Sync err=%v, want quorum loss: a degraded divergent survivor's barrier must not count", err)
	}

	next := make([]byte, blockSize)
	next[0], next[1] = 0x12, 0x34
	lsn, err := primary.Write(9, next)
	if err != nil {
		t.Fatalf("primary post-promotion write: %v", err)
	}
	err = v.OnLocalWrite(context.Background(), LocalWrite{LBA: 9, Data: next, LSN: lsn})
	if err == nil || !strings.Contains(err.Error(), ErrDurabilityQuorumLost.Error()) {
		t.Fatalf("OnLocalWrite err=%v, want quorum loss: a divergent survivor's ack must not count", err)
	}
}
