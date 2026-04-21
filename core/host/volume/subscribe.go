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
