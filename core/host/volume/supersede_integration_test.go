// Ownership: sw regression test (architect finding 2026-04-21,
// round 3: the supersede fail-closed was covered via stubProbe
// but not the real Host.recordOtherLine → Host.IsSuperseded →
// AdapterProjectionView composition). This test drives the full
// composition so the closure claim "supersede cross-replica
// fail-closed" lands on integration-level evidence, not only
// on a stub seam.
package volume

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestHost_RecordOtherLine_FlipsProjectionHealthyToFalse(t *testing.T) {
	// Build a minimal Host: no gRPC wiring required for this
	// test because we exercise only the projection/supersede
	// path. The adapter is replaced with a stub projector so we
	// can deterministically control local Healthy.
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1,
	}}
	h := &Host{
		cfg: Config{VolumeID: "v1", ReplicaID: "r1"},
	}
	h.view = NewAdapterProjectionView(proj, "v1", "r1", h)

	// Baseline: no lastOther → healthy.
	if p := h.view.Projection(); !p.Healthy {
		t.Fatalf("baseline projection: want Healthy, got %+v", p)
	}

	// Drive the real write path: recordOtherLine is what the
	// subscribe loop calls when the master's volume-scoped
	// subscription emits a fact for a DIFFERENT replica.
	h.recordOtherLine(&control.AssignmentFact{
		VolumeId:        "v1",
		ReplicaId:       "r2", // different replica
		Epoch:           2,    // strictly newer
		EndpointVersion: 1,
	})

	p := h.view.Projection()
	if p.Healthy {
		t.Fatalf("after cross-replica supersede: want !Healthy, got %+v", p)
	}
	// Identity fields must still reflect r1's own lineage so
	// the backend can compute the drift itself (VolumeID/
	// ReplicaID stay r1/v1; Epoch/EV come from the local
	// engine projection).
	if p.ReplicaID != "r1" || p.VolumeID != "v1" {
		t.Fatalf("identity mutated by supersede: %+v", p)
	}
}

func TestHost_RecordOtherLine_SameReplica_NotSuperseded(t *testing.T) {
	// lastOther naming the SAME replica (refresh endpoint for
	// self) must NOT trigger supersede — that's normal lineage
	// advance, handled by the adapter projection directly.
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode: engine.ModeHealthy, Epoch: 2, EndpointVersion: 2,
	}}
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r1"}}
	h.view = NewAdapterProjectionView(proj, "v1", "r1", h)
	h.recordOtherLine(&control.AssignmentFact{
		VolumeId: "v1", ReplicaId: "r1", Epoch: 2, EndpointVersion: 2,
	})
	if !h.view.Projection().Healthy {
		t.Fatal("same-replica lastOther must not trigger supersede")
	}
}

func TestHost_RecordOtherLine_OlderLineage_NotSuperseded(t *testing.T) {
	// A stale (lower epoch) cross-replica fact must NOT flip
	// this host to fail-closed. Only strictly newer lineages
	// count; otherwise an out-of-order delivery could DoS the
	// current primary.
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 3,
	}}
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r1"}}
	h.view = NewAdapterProjectionView(proj, "v1", "r1", h)
	h.recordOtherLine(&control.AssignmentFact{
		VolumeId: "v1", ReplicaId: "r2", Epoch: 3, EndpointVersion: 1,
	})
	if !h.view.Projection().Healthy {
		t.Fatal("older cross-replica fact must not trigger supersede")
	}
}

type mutableProjector struct{ p engine.ReplicaProjection }

func (m *mutableProjector) Projection() engine.ReplicaProjection { return m.p }
