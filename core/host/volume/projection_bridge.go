package volume

import (
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// adapterProjector is the narrow seam this bridge needs. Keeps
// the bridge testable and unaware of the adapter's full surface.
// (*adapter.VolumeReplicaAdapter).Projection satisfies this.
type adapterProjector interface {
	Projection() engine.ReplicaProjection
}

// SupersedeProbe reports whether this replica's locally-reported
// Healthy should be overridden to fail-closed because the master
// has published an authoritative line naming a DIFFERENT replica
// at a newer (Epoch, EndpointVersion).
//
// Why this seam exists:
//
//	An adapter's engine projection only reflects its own local
//	state. When master reassigns v1 from r1 → r2, r1's adapter
//	engine doesn't automatically leave ModeHealthy — it only
//	changes when r1 itself hears an authority line that either
//	demotes it or advances its own lineage. If r1 simply stops
//	being the primary (and the new line is for r2, not r1),
//	r1's frontend projection must STILL fail-closed so any
//	backend still holding an r1 Identity can't continue to read
//	or write. The volume host provides this probe via its
//	lastOther supersede record (core/host/volume/host.go:recordOtherLine).
//
// Implementations must be non-blocking and safe for concurrent
// callers.
type SupersedeProbe interface {
	IsSuperseded(selfReplicaID string, selfEpoch, selfEndpointVersion uint64) bool
}

// AdapterProjectionView is the T1 readiness bridge: it reads the
// adapter's operator-facing engine projection, consults the
// supersede probe, and surfaces the combined result as a
// frontend.ProjectionView.
//
// Identity fields (VolumeID, ReplicaID) are captured from the
// volume host at construction — they are NOT minted by the
// frontend and are NOT derived from engine.ReplicaProjection
// (which deliberately doesn't carry them). Epoch and
// EndpointVersion come from the engine projection on every call,
// so lineage drift shows up on the very next Read/Write.
//
// Healthy rule (fail-closed):
//
//	Healthy = (engine.Mode == ModeHealthy) AND NOT superseded
//
// If the engine is locally Healthy but master has named another
// replica as primary at a newer lineage, Healthy flips to false.
// This closes the cross-replica authority-move gap architect
// review flagged on 2026-04-21.
type AdapterProjectionView struct {
	projector adapterProjector
	volumeID  string
	replicaID string
	probe     SupersedeProbe
}

// NewAdapterProjectionView wires the bridge. probe may be nil
// for tests that do not exercise supersede flow; production
// callers (Host.ProjectionView) must pass a non-nil probe.
func NewAdapterProjectionView(a adapterProjector, volumeID, replicaID string, probe SupersedeProbe) *AdapterProjectionView {
	return &AdapterProjectionView{
		projector: a,
		volumeID:  volumeID,
		replicaID: replicaID,
		probe:     probe,
	}
}

// Projection satisfies frontend.ProjectionView.
func (v *AdapterProjectionView) Projection() frontend.Projection {
	p := v.projector.Projection()
	healthy := p.Mode == engine.ModeHealthy
	if healthy && v.probe != nil && v.probe.IsSuperseded(v.replicaID, p.Epoch, p.EndpointVersion) {
		healthy = false
	}
	return frontend.Projection{
		VolumeID:        v.volumeID,
		ReplicaID:       v.replicaID,
		Epoch:           p.Epoch,
		EndpointVersion: p.EndpointVersion,
		Healthy:         healthy,
	}
}
