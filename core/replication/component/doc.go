// Package component provides a reusable cluster-style harness for
// authoring V3 replication scenario tests. Sits between unit tests
// (engine/transport-only, in-process types) and L3 m01 hardware
// runs (real binaries over real network).
//
// What it gives you:
//
//   - A Cluster type that owns one Primary + N Replicas wired over
//     real localhost TCP using the actual production code paths
//     (BlockStore + ReplicaListener + BlockExecutor).
//   - A substrate matrix runner — every scenario runs against
//     smartwal AND walstore, both gating per round-22.
//   - Network fault primitives: KillReplicaConnection / SeverTransport /
//     RestoreTransport — for catch-up + degraded scenarios that L1
//     unit tests can't reach.
//   - Convergence + mode-label assertion helpers — the standard
//     "did the replica catch up byte-exact + did the recovery_mode
//     label surface" checks every T4c scenario needs.
//   - Log capture so scenarios can assert on observability surfaces
//     (recovery_mode label, failure reasons, etc.) without parsing
//     stderr.
//
// Authoring shape (target: ~20-30 lines per scenario):
//
//	func TestT4c_MyScenario(t *testing.T) {
//	    component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
//	        c.WithReplicas(1).Start()
//	        c.PrimaryWriteN(20)
//	        c.PrimarySync()
//	        c.CatchUpReplica(0)
//	        c.AssertReplicaConverged(0)
//	        c.AssertSawRecoveryMode(0, component.ExpectAnyMode)
//	    })
//	}
//
// What it does NOT do:
//
//   - Spin up subprocess binaries (cmd/sparrow). For full-binary
//     subprocess testing, write a wrapper around os/exec; the
//     component framework's API can be retargeted by swapping the
//     Cluster impl.
//   - Drive engine→adapter→executor recovery flows. Today
//     ReplicationVolume bypasses the adapter; component scenarios
//     drive catch-up directly via executor.StartCatchUp until the
//     engine→adapter→executor wiring lands (forward-carry to T4d).
//
// Substrate matrix: smartwal is sign-bearing; walstore is the
// second matrix row. Both are gating in this harness — there is no
// "non-gating" demote; if walstore regresses, the test fails
// honestly. See `core/replication/l2_integration_test.go` for the
// QA round on this discipline.
package component
