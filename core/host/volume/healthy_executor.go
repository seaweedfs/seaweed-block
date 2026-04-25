package volume

import (
	"sync"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// HealthyPathExecutor is the T1 minimum-readiness executor. It
// replaces the T0 noopExecutor's deliberate probe failure with a
// synchronous happy-path: Probe returns Success=true and Fence
// fires onFence with Success=true. Combined with a new
// AssignmentInfo, those two results drive the engine through
// ReachabilityReachable + FencedEpoch advance, and
// engine.DeriveProjection flips Mode to ModeHealthy.
//
// Scope (sketch §11.L2):
//
//	This is the "minimum product-route readiness bridge" T1 owns.
//	It is NOT a real data-plane executor. It runs in-process on
//	the volume host so that frontend.ProjectionView over a real
//	adapter reaches Healthy. It carries out no data transfer, no
//	catch-up, no rebuild — StartCatchUp/StartRebuild still return
//	nil without firing any completion event, because the T0/T1
//	scope has no session lifecycle.
//
// Real catch-up / rebuild / fence execution is T3+ territory.
type HealthyPathExecutor struct {
	mu sync.Mutex

	onFence adapter.OnFenceComplete
	onStart adapter.OnSessionStart
	onClose adapter.OnSessionClose

	commandLog []string
}

// NewHealthyPathExecutor constructs a fresh executor. Its
// lifetime is tied to the volume host that owns it.
func NewHealthyPathExecutor() *HealthyPathExecutor {
	return &HealthyPathExecutor{}
}

func (e *HealthyPathExecutor) SetOnSessionStart(fn adapter.OnSessionStart)   { e.onStart = fn }
func (e *HealthyPathExecutor) SetOnSessionClose(fn adapter.OnSessionClose)   { e.onClose = fn }
func (e *HealthyPathExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete) { e.onFence = fn }

// Probe returns a successful probe. The adapter normalizes this
// into a ProbeSucceeded event → Reachability.Status=Reachable.
func (e *HealthyPathExecutor) Probe(replicaID, dataAddr, ctrlAddr string, epoch, endpointVersion uint64) adapter.ProbeResult {
	e.record("Probe:" + replicaID)
	// Return Success=true plus caught-up boundaries (R=H=1, S=0).
	// decide() sees R>=H and emits DecisionNone + FenceAtEpoch
	// (because Identity.Epoch > FencedEpoch=0 on first probe).
	// On FenceCompleted(success), FencedEpoch advances, the next
	// decide() emits PublishHealthy, and Mode becomes Healthy.
	return adapter.ProbeResult{
		ReplicaID:         replicaID,
		Success:           true,
		EndpointVersion:   endpointVersion,
		TransportEpoch:    epoch,
		ReplicaFlushedLSN: 1,
		PrimaryTailLSN:    0,
		PrimaryHeadLSN:    1,
	}
}

// StartCatchUp / StartRebuild / InvalidateSession / PublishHealthy /
// PublishDegraded — all no-op + record. T1 does not exercise session
// lifecycle; engine currently doesn't demand session completion for
// Healthy because Recovery.Decision stays None under a successful
// probe with no contact gap.
func (e *HealthyPathExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	e.record("StartCatchUp:" + replicaID)
	return nil
}

func (e *HealthyPathExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	e.record("StartRebuild:" + replicaID)
	return nil
}

func (e *HealthyPathExecutor) StartRecoverySession(
	replicaID string,
	sessionID, epoch, endpointVersion, targetLSN uint64,
	contentKind engine.RecoveryContentKind,
	policy engine.RecoveryRuntimePolicy,
) error {
	e.record("StartRecoverySession:" + replicaID + ":" + string(contentKind))
	return nil
}

func (e *HealthyPathExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {
	e.record("InvalidateSession:" + replicaID)
}

func (e *HealthyPathExecutor) PublishHealthy(replicaID string) {
	e.record("PublishHealthy:" + replicaID)
}

func (e *HealthyPathExecutor) PublishDegraded(replicaID string, reason string) {
	e.record("PublishDegraded:" + replicaID + ":" + reason)
}

// Fence fires the success callback synchronously (next tick via
// the adapter's own apply loop — the adapter took care of
// routing the normalized event back into engine state). The
// synchronous callback is what advances Reachability.FencedEpoch
// to Identity.Epoch, which is the final gate before Mode becomes
// Healthy.
func (e *HealthyPathExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	e.record("Fence:" + replicaID)
	fn := e.onFence
	if fn != nil {
		go fn(adapter.FenceResult{
			ReplicaID:       replicaID,
			SessionID:       sessionID,
			Epoch:           epoch,
			EndpointVersion: endpointVersion,
			Success:         true,
		})
	}
	return nil
}

func (e *HealthyPathExecutor) record(s string) {
	e.mu.Lock()
	e.commandLog = append(e.commandLog, s)
	e.mu.Unlock()
}

// Commands returns a copy of the recorded command log. Used by
// tests to assert what reached the executor.
func (e *HealthyPathExecutor) Commands() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make([]string, len(e.commandLog))
	copy(cp, e.commandLog)
	return cp
}
