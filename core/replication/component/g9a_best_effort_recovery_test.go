package component

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
)

func TestG9A_BestEffortWriteStillFeedsLaggingReplicaRecovery(t *testing.T) {
	c := NewCluster(t, Smartwal).
		WithReplicas(1).
		WithBlockGeometry(64, 4096).
		WithLiveShip().
		WithEngineDrivenRecovery().
		Start()
	c.primary.RepVol.SetDurabilityMode(replication.DurabilityBestEffort)

	drivePeerAdmit(t, c)
	for i := 0; i < 3; i++ {
		writeOneBlock(t, c, uint32(i), byte(0xA0+i))
	}
	c.WaitForConverge(2 * time.Second)

	c.KillReplicaListener(0)
	for i := 3; i < 5; i++ {
		// Best-effort foreground writes must succeed even while the
		// replica listener is down. The recovery/probe layer remains
		// responsible for catching the peer up later.
		writeOneBlock(t, c, uint32(i), byte(0xD0+i))
	}
	if c.replicaConverged(c.Replica(0)) {
		t.Fatal("replica unexpectedly converged while listener was down; test did not create lag")
	}

	rebindReplicaListener(t, c, 0)
	configureProbeLoopOnCluster(t, c)
	c.WaitForConverge(8 * time.Second)
	c.AssertReplicaConverged(0)
}
