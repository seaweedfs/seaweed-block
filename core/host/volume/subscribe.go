// Package volume implements the block volume host: the volume-side
// product daemon composition that connects to a block master, ingests
// local slot facts into periodic heartbeat RPCs, subscribes to
// the master's assignment stream, and feeds delivered assignments
// into a VolumeReplicaAdapter.
//
// T0 scope (v3-phase-15-t0-sketch.md):
//
//   - Drives control-plane route only (G0 + G1).
//   - NO data path, NO real runtime executor: uses a noop executor
//     stub (noop_executor.go). Adapter projection reflects the
//     assigned lineage; ModeHealthy is NOT claimed.
//   - Single volume per process for T0 simplicity.
//
// Boundary (sketch §3 + §3.1 + §6.4):
//
//   - volume host imports core/rpc (wire types) + core/adapter
//     (consumer API). It MUST NOT import core/authority mint
//     symbols (Publisher.apply, SubmitObservedState, AssignmentAsk).
//   - The ONLY adapter.AssignmentInfo composite literal allowed
//     in this package sits in decodeAssignmentFact below.
//     TestNoOtherAssignmentInfoConstruction (L0) enforces this
//     as a static guard.
package volume

import (
	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// decodeAssignmentFact is the SOLE permitted translator from a
// master-minted wire fact into the adapter's ingress type. It is
// a strict field-copy: every field in the output comes directly
// from the input's corresponding field. No mutation, no local
// defaults, no fields from any other source.
//
// This is the round-3 architect-pinned boundary (T0 sketch §3.1).
// If a second AssignmentInfo composite literal appears anywhere
// in core/host/volume/ the AST guard in
// TestNoOtherAssignmentInfoConstruction fails.
func decodeAssignmentFact(f *control.AssignmentFact) adapter.AssignmentInfo {
	return adapter.AssignmentInfo{
		VolumeID:        f.VolumeId,
		ReplicaID:       f.ReplicaId,
		Epoch:           f.Epoch,
		EndpointVersion: f.EndpointVersion,
		DataAddr:        f.DataAddr,
		CtrlAddr:        f.CtrlAddr,
	}
}

// decodeReplicaTargets is the T4a-5 peer-set decode path. Parallel
// to decodeAssignmentFact but produces replication.ReplicaTarget
// values + the monotonic peer_set_generation — NOT adapter.Assignment
// Info. This preserves the T0 boundary (decodeAssignmentFact
// remains the sole AssignmentInfo constructor; AST fence
// TestNoOtherAssignmentInfoConstruction continues to pass) while
// unlocking primary-side replication fan-out via
// ReplicationVolume.UpdateReplicaSet(generation, targets).
//
// Field-copy only, same discipline as decodeAssignmentFact: no
// derivation, no accumulation, no cache — the peer-set authority
// is master-minted (T4a-5.0 discovery §9.1 guardrail #3).
//
// Master-side contract (v3-phase-15-t4a-5-0-host-callback-discovery
// doc §9.1): f.Peers already excludes the self-replica; no
// further filter is needed here. Callers feed the returned
// targets slice + generation directly into UpdateReplicaSet.
func decodeReplicaTargets(f *control.AssignmentFact) ([]replication.ReplicaTarget, uint64) {
	if len(f.Peers) == 0 {
		return nil, f.PeerSetGeneration
	}
	out := make([]replication.ReplicaTarget, 0, len(f.Peers))
	for _, p := range f.Peers {
		out = append(out, replication.ReplicaTarget{
			ReplicaID:       p.ReplicaId,
			DataAddr:        p.DataAddr,
			ControlAddr:     p.CtrlAddr,
			Epoch:           p.Epoch,
			EndpointVersion: p.EndpointVersion,
		})
	}
	return out, f.PeerSetGeneration
}
