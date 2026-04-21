// Ownership: sw regression test (architect finding 2026-04-21:
// supersede-after-cross-replica-move was not closing the
// frontend fence). Pins the Healthy = localHealthy && !superseded
// rule in AdapterProjectionView.
package volume

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
)

type stubProjector struct{ p engine.ReplicaProjection }

func (s stubProjector) Projection() engine.ReplicaProjection { return s.p }

type stubProbe struct{ yes bool }

func (s stubProbe) IsSuperseded(string, uint64, uint64) bool { return s.yes }

func TestAdapterProjectionView_LocalHealthyAndNotSuperseded_IsHealthy(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1}}
	v := NewAdapterProjectionView(p, "v1", "r1", stubProbe{yes: false})
	got := v.Projection()
	if !got.Healthy {
		t.Fatalf("want Healthy, got %+v", got)
	}
}

func TestAdapterProjectionView_SupersededOverridesLocalHealthy(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1}}
	v := NewAdapterProjectionView(p, "v1", "r1", stubProbe{yes: true})
	got := v.Projection()
	if got.Healthy {
		t.Fatalf("want !Healthy (superseded), got %+v", got)
	}
	if got.Epoch != 1 || got.EndpointVersion != 1 {
		t.Fatalf("lineage lost under supersede: %+v", got)
	}
}

func TestAdapterProjectionView_LocalDegradedStaysDegraded(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeDegraded, Epoch: 1}}
	v := NewAdapterProjectionView(p, "v1", "r1", stubProbe{yes: false})
	if v.Projection().Healthy {
		t.Fatal("local Degraded must not be Healthy")
	}
}

func TestAdapterProjectionView_NilProbe_DoesNotPanic(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy}}
	v := NewAdapterProjectionView(p, "v1", "r1", nil)
	_ = v.Projection() // just must not panic
}

// Sanity: the bridge returns exactly the frontend.Projection
// the frontend contract requires (nothing else). Compile-time
// check so a future field addition is flagged immediately.
var _ frontend.ProjectionView = (*AdapterProjectionView)(nil)
