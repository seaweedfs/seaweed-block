package volume

import (
	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// PeerCommandExecutor adapts a *replication.ReplicaPeer's transport
// BlockExecutor + ReplicaState mutators to satisfy
// adapter.CommandExecutor for a per-peer VolumeReplicaAdapter.
//
// Most methods are pure delegation to the peer's transport executor
// (Probe / StartCatchUp / StartRebuild / StartRecoverySession /
// InvalidateSession / Fence). The two value-adds:
//
//  1. SetOnSessionStart / SetOnSessionClose intercept the underlying
//     callbacks to ALSO drive peer.SetState transitions
//     (Degraded → CatchingUp on SessionStart; CatchingUp → Healthy
//     on SessionClose with Success=true). Without this, the peer
//     would stay Degraded after a successful catch-up and the probe
//     loop would keep probing a healthy peer forever.
//
//  2. PublishHealthy / PublishDegraded translate the engine's
//     "operator-visible publication" verdict into runtime peer state
//     mutations (peer.SetState(Healthy) / peer.Invalidate(reason)).
//     The transport executor logs them only; the runtime peer state
//     machine is what gates Ship + ProbeIfDegraded.
//
// Lifecycle: PeerCommandExecutor borrows both the *ReplicaPeer and
// the underlying *transport.BlockExecutor. It does NOT close
// either — caller (PeerAdapterRegistry) owns lifecycle.
type PeerCommandExecutor struct {
	peer  *replication.ReplicaPeer
	inner *transport.BlockExecutor // == peer.Executor(); cached for clarity
}

// NewPeerCommandExecutor wraps a peer's transport executor with peer
// state-mutation interceptors. Returns an adapter.CommandExecutor
// suitable for adapter.NewVolumeReplicaAdapter.
//
// Called by: PeerAdapterRegistry.OnPeerAdded.
func NewPeerCommandExecutor(peer *replication.ReplicaPeer) *PeerCommandExecutor {
	return &PeerCommandExecutor{
		peer:  peer,
		inner: peer.Executor(),
	}
}

// --- Session-lifecycle callback interceptors (peer state translation) ---

// SetOnSessionStart wraps the adapter-supplied callback so a session
// start ALSO transitions peer.state Degraded → CatchingUp. The
// adapter's own bookkeeping still runs after.
func (p *PeerCommandExecutor) SetOnSessionStart(fn adapter.OnSessionStart) {
	p.inner.SetOnSessionStart(func(r adapter.SessionStartResult) {
		// Best-effort transition: SetState rejects illegal transitions
		// (e.g., NeedsRebuild → CatchingUp is not allowed; this is fine
		// because the engine wouldn't emit StartCatchUp from that state
		// anyway). A rejected transition logs but does not panic.
		p.peer.SetState(replication.ReplicaCatchingUp)
		if fn != nil {
			fn(r)
		}
	})
}

// SetOnSessionClose wraps the adapter-supplied callback so a
// successful recovery session first rotates the peer's steady live
// session, then transitions peer.state CatchingUp → Healthy. The
// rotation is the EndSession → steady-capable handoff: post-recovery
// live traffic must not reuse a pre-recovery broken TCP conn or stale
// session ID.
func (p *PeerCommandExecutor) SetOnSessionClose(fn adapter.OnSessionClose) {
	p.inner.SetOnSessionClose(func(r adapter.SessionCloseResult) {
		if r.Success {
			if err := p.peer.RefreshLiveShipSessionAfter(r.SessionID, r.AchievedLSN, "recovery session completed"); err != nil {
				r.Success = false
				r.FailReason = err.Error()
				p.peer.Invalidate("live session refresh failed: " + err.Error())
			} else {
				p.peer.SetState(replication.ReplicaHealthy)
			}
		}
		// Failure path: the adapter feeds engine
		// SessionClosedFailed which (per FailureKind) may emit
		// PublishDegraded; that lands at our PublishDegraded
		// override and calls peer.Invalidate. We do NOT
		// pre-mutate peer state on session close failure here
		// to avoid double-mutation.
		if fn != nil {
			fn(r)
		}
	})
}

// SetOnFenceComplete delegates verbatim to the inner executor.
// Fence completion does not directly drive peer state; it advances
// engine Reachability.FencedEpoch which is the ack-gate for Healthy.
func (p *PeerCommandExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete) {
	p.inner.SetOnFenceComplete(fn)
}

// --- Wire-level command delegation (verbatim to inner) ---

func (p *PeerCommandExecutor) Probe(replicaID, dataAddr, ctrlAddr string, sessionID, epoch, endpointVersion uint64) adapter.ProbeResult {
	return p.inner.Probe(replicaID, dataAddr, ctrlAddr, sessionID, epoch, endpointVersion)
}

func (p *PeerCommandExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, fromLSN, targetLSN uint64) error {
	return p.inner.StartCatchUp(replicaID, sessionID, epoch, endpointVersion, fromLSN, targetLSN)
}

func (p *PeerCommandExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	return p.inner.StartRebuild(replicaID, sessionID, epoch, endpointVersion, targetLSN)
}

func (p *PeerCommandExecutor) StartRecoverySession(
	replicaID string,
	sessionID, epoch, endpointVersion, targetLSN uint64,
	contentKind engine.RecoveryContentKind,
	policy engine.RecoveryRuntimePolicy,
) error {
	return p.inner.StartRecoverySession(replicaID, sessionID, epoch, endpointVersion, targetLSN, contentKind, policy)
}

func (p *PeerCommandExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {
	p.inner.InvalidateSession(replicaID, sessionID, reason)
}

func (p *PeerCommandExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	return p.inner.Fence(replicaID, sessionID, epoch, endpointVersion)
}

// --- Operator-visible publication (translate to peer state) ---

// PublishHealthy translates the engine's "this peer is operator-
// visible healthy" verdict into peer.SetState(Healthy). The
// underlying transport executor's PublishHealthy is a log-only stub;
// without this translation, the runtime peer state stays Degraded
// after a successful catch-up and the probe loop keeps polling a
// peer that's already caught up.
func (p *PeerCommandExecutor) PublishHealthy(replicaID string) {
	p.peer.SetState(replication.ReplicaHealthy)
	p.inner.PublishHealthy(replicaID)
}

// PublishDegraded translates the engine's degraded-publication
// verdict into peer.Invalidate(reason). Same rationale as
// PublishHealthy: the transport executor logs only.
func (p *PeerCommandExecutor) PublishDegraded(replicaID string, reason string) {
	p.peer.Invalidate(reason)
	p.inner.PublishDegraded(replicaID, reason)
}
