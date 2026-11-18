//go:build swblock_format_v3_test

package replication

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// The real ReplicaApplyGate over a dark v3 store: the recovery lane applies
// fresh entries and stale-skips without touching the store; the live lane
// applies the next LSNs. Mutant: ApplyEntry refused on v3 fails here.
func TestApplyGateOverV3Store(t *testing.T) {
	s, err := storage.CreateWALStoreV3Test(filepath.Join(t.TempDir(), "gate.v3"), 64, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	gate := NewReplicaApplyGate(s)
	rec := transport.RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	live := transport.RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 100}
	blk := func(b byte) []byte { return bytes.Repeat([]byte{b}, blockSize) }
	for lsn := uint64(1); lsn <= 2; lsn++ {
		if err := gate.ApplyRecovery(rec, uint32(lsn), blk(byte(lsn)), lsn); err != nil {
			t.Fatalf("recovery apply %d: %v", lsn, err)
		}
	}
	if err := gate.ApplyRecovery(rec, 2, blk(0xEE), 1); err != nil {
		t.Fatalf("recovery stale-skip: %v", err)
	}
	if err := gate.ApplyLive(live, 3, blk(3), 3); err != nil {
		t.Fatalf("live apply: %v", err)
	}
	for lba := uint32(1); lba <= 3; lba++ {
		got, err := s.Read(lba)
		if err != nil || !bytes.Equal(got, blk(byte(lba))) {
			t.Fatalf("LBA %d: err %v", lba, err)
		}
	}
}
