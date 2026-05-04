package component_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/replication/component"
)

// G8-B component prelude pins the data-continuity prerequisite for failover: after
// acknowledged primary writes have been replicated to a candidate,
// the candidate's local durable bytes are sufficient for serving the
// same data once authority moves to it. Authority movement itself is
// covered in core/authority G8-A; stale old-primary fencing is covered
// in core/host/volume G8-B.
func TestG8B0_NewPrimaryCandidate_ReadsAcknowledgedWritesAfterConvergence(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithLiveShip().
			WithApplyGate().
			Start()

		const writes = 24
		for i := 0; i < writes; i++ {
			data := make([]byte, component.DefaultBlockSize)
			data[0] = byte(0x80 + i)
			data[len(data)-1] = byte(0x40 ^ i)
			c.PrimaryWriteViaBackend(uint32(i), data)
		}
		c.PrimarySync()
		c.WaitForConverge(3 * time.Second)

		for lba := uint32(0); lba < writes; lba++ {
			primaryData, err := c.Primary().Store.Read(lba)
			if err != nil {
				t.Fatalf("primary read lba=%d: %v", lba, err)
			}
			newPrimaryData, err := c.Replica(0).Store.Read(lba)
			if err != nil {
				t.Fatalf("candidate read lba=%d: %v", lba, err)
			}
			if !bytes.Equal(primaryData, newPrimaryData) {
				t.Fatalf("candidate data mismatch at lba=%d: primary[0]=%02x candidate[0]=%02x",
					lba, primaryData[0], newPrimaryData[0])
			}
		}
	})
}

func TestG8B0_FailoverCandidate_NotAcceptedBeforeByteConvergence(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(8)
		c.PrimarySync()

		if candidateMatchesPrimary(t, c, 0, 8) {
			t.Fatal("empty candidate unexpectedly matches primary before recovery/convergence")
		}

		result := c.CatchUpReplica(0)
		if !result.Success {
			t.Fatalf("catch-up: %s", result.FailReason)
		}
		if !candidateMatchesPrimary(t, c, 0, 8) {
			t.Fatal("candidate must match primary after successful recovery/convergence")
		}
	})
}

func TestG8_FailoverCannotCloseOnAuthorityMoveOnly(t *testing.T) {
	component.RunSubstrate(t, "memorywal", component.MemoryWAL, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(8)
		c.PrimarySync()

		line := authority.AuthorityBasis{
			Assigned: true, ReplicaID: "r2", Epoch: 2, EndpointVersion: 1,
			DataAddr: "r2-data", CtrlAddr: "r2-ctrl",
		}
		if !line.Assigned || line.ReplicaID != "r2" {
			t.Fatalf("authority move setup failed: line=%+v", line)
		}
		if candidateMatchesPrimary(t, c, 0, 8) {
			t.Fatal("authority move alone must not imply G8 data-continuity close")
		}
	})
}

func candidateMatchesPrimary(t *testing.T, c *component.Cluster, replicaIdx int, n uint32) bool {
	t.Helper()
	for lba := uint32(0); lba < n; lba++ {
		primaryData, err := c.Primary().Store.Read(lba)
		if err != nil {
			t.Fatalf("primary read lba=%d: %v", lba, err)
		}
		replicaData, err := c.Replica(replicaIdx).Store.Read(lba)
		if err != nil {
			return false
		}
		if !bytes.Equal(primaryData, replicaData) {
			return false
		}
	}
	return true
}
