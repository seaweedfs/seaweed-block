package volume

import (
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// adapterProjector is the narrow seam this bridge needs. Keeps
// the bridge testable and unaware of the adapter's full surface.
// (*adapter.VolumeReplicaAdapter).Projection satisfies this.
type adapterProjector interface {
	Projection() engine.ReplicaProjection
}

type promotionReadyProjector interface {
	PromotionReadyFact() engine.PromotionReadyFact
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
//	AND NOT supporting-replica-ready
//
// If the engine is locally Healthy but master has named another
// replica as primary at a newer lineage, Healthy flips to false.
// This closes the cross-replica authority-move gap architect
// review flagged on 2026-04-21.
//
// If the engine is locally Healthy and master has named another
// replica as primary at the SAME lineage, this replica may be a
// recovered supporting replica, but it still must not serve frontend
// I/O. Status surfaces that as replication_role=replica_ready; the
// frontend projection fails closed by reporting Healthy=false.
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
	p, _ := v.projectionWithSupersede()
	return p
}

// projectionWithSupersede returns the frontend projection plus the
// reason bit that /status needs for G9A vocabulary. The frontend
// still consumes Projection() above; this helper is package-local so
// control-plane diagnostics can explain why Healthy is false without
// changing the frontend contract.
func (v *AdapterProjectionView) projectionWithSupersede() (frontend.Projection, bool) {
	p := v.projector.Projection()
	healthy := p.Mode == engine.ModeHealthy
	superseded := healthy && v.isSupersededLine(p.Epoch, p.EndpointVersion)
	if superseded || (healthy && v.supportingReplicaReady(p.Epoch, p.EndpointVersion)) {
		healthy = false
	}
	return frontend.Projection{
		VolumeID:        v.volumeID,
		ReplicaID:       v.replicaID,
		Epoch:           p.Epoch,
		EndpointVersion: p.EndpointVersion,
		Healthy:         healthy,
	}, superseded
}

func (v *AdapterProjectionView) supportingReplicaReady(selfEpoch, selfEV uint64) bool {
	if v == nil || v.probe == nil {
		return false
	}
	probe, ok := v.probe.(interface {
		LastOtherLine() *control.AssignmentFact
	})
	if !ok {
		return false
	}
	other := probe.LastOtherLine()
	if other == nil {
		return false
	}
	return other.GetReplicaId() != v.replicaID &&
		other.GetEpoch() == selfEpoch &&
		other.GetEndpointVersion() == selfEV
}

func (v *AdapterProjectionView) isSupersededLine(epoch, endpointVersion uint64) bool {
	return v != nil &&
		v.probe != nil &&
		v.probe.IsSuperseded(v.replicaID, epoch, endpointVersion)
}

// PromotionReadyFact returns a control-facing readiness fact when the wrapped
// adapter exposes one. Test-only projectors that only implement Projection()
// fail closed by returning Ready=false.
func (v *AdapterProjectionView) PromotionReadyFact() engine.PromotionReadyFact {
	if v == nil || v.projector == nil {
		return engine.PromotionReadyFact{Reason: engine.PromotionReadyReasonNotMember}
	}
	projector, ok := v.projector.(promotionReadyProjector)
	if !ok {
		return engine.PromotionReadyFact{
			Reason:          engine.PromotionReadyReasonNotCaughtUp,
			ReplicaID:       v.replicaID,
			Epoch:           0,
			EndpointVersion: 0,
		}
	}
	return projector.PromotionReadyFact()
}

// EngineProjection returns the underlying engine.ReplicaProjection
// (Mode, R/S/H, RecoveryDecision, SessionKind/Phase, Reason). Used
// by the G5-5 opt-in `/status/recovery` endpoint to surface recovery
// boundaries for hardware-test catch-up verification. NOT consumed
// by the frontend (which uses Projection() above for Healthy gating).
//
// This accessor reads engine state without modifying it; supersede
// is NOT applied here (callers needing the same fail-closed semantic
// the frontend gets should also call Projection()).
func (v *AdapterProjectionView) EngineProjection() engine.ReplicaProjection {
	return v.projector.Projection()
}
