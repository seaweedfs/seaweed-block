package component

import "testing"

// RunMatrix runs the given scenario against each substrate factory.
// Both rows are gating; smartwal is sign-bearing per round-22.
//
// Authoring shape:
//
//	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
//	    c.WithReplicas(1).Start()
//	    c.PrimaryWriteN(20)
//	    c.PrimarySync()
//	    c.CatchUpReplica(0)
//	    c.AssertReplicaConverged(0)
//	})
//
// The cluster is fresh per substrate row; t.Cleanup handles
// teardown automatically.
func RunMatrix(t *testing.T, scenario func(t *testing.T, c *Cluster)) {
	t.Helper()
	t.Run("smartwal", func(t *testing.T) {
		scenario(t, NewCluster(t, Smartwal))
	})
	t.Run("walstore", func(t *testing.T) {
		scenario(t, NewCluster(t, Walstore))
	})
}

// RunSubstrate runs the scenario against one named substrate. Useful
// when a scenario only makes sense against a specific substrate
// (e.g., a wal_replay-specific behavior pin) — most scenarios
// should use RunMatrix instead.
func RunSubstrate(t *testing.T, name string, factory SubstrateFactory, scenario func(t *testing.T, c *Cluster)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		scenario(t, NewCluster(t, factory))
	})
}
