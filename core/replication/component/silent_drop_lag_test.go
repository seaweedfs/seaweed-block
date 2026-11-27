package component

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
)

// A healthy replica whose connection dies silently (no FIN, no RST: a
// partition) must still be caught up. The primary's ships into the dead
// connection "succeed" into its socket buffer, best-effort runs no barrier
// per write, and no later write fails, so only a timer-driven check of the
// healthy peer can find the gap. The check's barrier gets no answer at all on
// that connection, so it must fail at its own deadline and take the existing
// invalidate, probe and catch-up path over a new connection.
func TestASilentlyDroppedHealthyReplicaIsCaughtUp(t *testing.T) {
	c := NewCluster(t, Smartwal).
		WithReplicas(1).
		WithBlockGeometry(64, 4096).
		WithLiveShip().
		WithEngineDrivenRecovery().
		WithSilentProxy().
		Start()
	logRecoveryStateOnFailure(t, c)
	c.primary.RepVol.SetDurabilityMode(replication.DurabilityBestEffort)

	drivePeerAdmit(t, c)
	for i := 0; i < 3; i++ {
		writeOneBlock(t, c, uint32(i), byte(0xA0+i))
	}
	c.WaitForConverge(2 * time.Second)
	c.AssertReplicaConverged(0)

	// The path dies silently; the primary is not told.
	c.Replica(0).Proxy.GoSilent()
	for i := 3; i < 5; i++ {
		writeOneBlock(t, c, uint32(i), byte(0xD0+i))
	}
	if c.replicaConverged(c.Replica(0)) {
		t.Fatal("replica converged through a silent connection; the proxy did not drop")
	}
	if st := c.primary.RepVol.PeerStatuses()[0].State; st != replication.ReplicaHealthy.String() {
		t.Fatalf("peer %s right after the silent drop; this test needs it still healthy (the gap is that nothing notices)", st)
	}

	// No further write. The barrier deadline on the dead connection is 5 s
	// (transport recoveryConnTimeout); one check interval plus that deadline,
	// the probe and the catch-up must fit in the bound below.
	configureProbeLoopOnCluster(t, c)
	c.WaitForConverge(15 * time.Second)
	c.AssertReplicaConverged(0)
}
