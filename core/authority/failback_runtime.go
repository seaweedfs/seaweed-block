package authority

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// FailbackRuntimeRequest is the narrow authority-owned failback seam.
// It carries only pre-validated target facts and expected-current guards; the
// Publisher still mints the authority epoch through IntentReassign.
type FailbackRuntimeRequest struct {
	VolumeID                     string   `json:"volumeID"`
	ReplicaID                    string   `json:"replicaID"`
	TargetDataAddr               string   `json:"targetDataAddr"`
	TargetCtrlAddr               string   `json:"targetCtrlAddr"`
	ExpectedCurrentReplicaID     string   `json:"expectedCurrentReplicaID"`
	ExpectedCurrentEpoch         uint64   `json:"expectedCurrentEpoch"`
	AckEligible                  bool     `json:"ackEligible"`
	FrontendFencedBeforeFailback bool     `json:"frontendFencedBeforeFailback"`
	DurableFrontierCovered       bool     `json:"durableFrontierCovered"`
	NoCrossVolumeIdentityChange  bool     `json:"noCrossVolumeIdentityChange"`
	EvidenceRefs                 []string `json:"evidenceRefs,omitempty"`
}

type FailbackRuntimeResult struct {
	FailbackStarted                   bool     `json:"failbackStarted"`
	AuthorityEpochAdvanced            bool     `json:"authorityEpochAdvanced"`
	SinglePrimaryAfterFailback        bool     `json:"singlePrimaryAfterFailback"`
	PublishTargetSwappedAfterFailback bool     `json:"publishTargetSwappedAfterFailback"`
	NoStorageMutation                 bool     `json:"noStorageMutation"`
	NoCrossVolumeIdentityChange       bool     `json:"noCrossVolumeIdentityChange"`
	EvidenceRefs                      []string `json:"evidenceRefs,omitempty"`
}

type FailbackAuthorityRuntime struct {
	Publisher *Publisher
	// Gate is the promotion gate's decision for this planned move: the
	// target's probed evidence, or why the move is refused. A failback is a
	// primary change: without a gate it publishes nothing.
	Gate func(volumeID, targetReplicaID string) (adapter.PrimaryEvidence, error)
}

func (r FailbackAuthorityRuntime) ExecuteFailback(ctx context.Context, req FailbackRuntimeRequest) (FailbackRuntimeResult, error) {
	if err := validateFailbackRuntimeRequest(req); err != nil {
		return FailbackRuntimeResult{}, err
	}
	if r.Publisher == nil {
		return FailbackRuntimeResult{}, fmt.Errorf("authority failback runtime publisher is required")
	}
	select {
	case <-ctx.Done():
		return FailbackRuntimeResult{}, ctx.Err()
	default:
	}
	before, ok := r.Publisher.VolumeAuthorityLine(req.VolumeID)
	if !ok {
		return FailbackRuntimeResult{}, fmt.Errorf("authority failback runtime: missing current authority line for volume %s", req.VolumeID)
	}
	if before.ReplicaID != req.ExpectedCurrentReplicaID || before.Epoch != req.ExpectedCurrentEpoch {
		return FailbackRuntimeResult{}, fmt.Errorf("authority failback runtime: stale expected current line got current %s@%d want expected %s@%d",
			before.ReplicaID, before.Epoch, req.ExpectedCurrentReplicaID, req.ExpectedCurrentEpoch)
	}
	if before.ReplicaID == req.ReplicaID {
		return FailbackRuntimeResult{}, fmt.Errorf("authority failback runtime: target replica %s is already current", req.ReplicaID)
	}
	if r.Gate == nil {
		return FailbackRuntimeResult{}, fmt.Errorf("authority failback runtime: %w (no promotion gate)", ErrReassignWithoutKind)
	}
	evidence, err := r.Gate(req.VolumeID, req.ReplicaID)
	if err != nil {
		return FailbackRuntimeResult{}, fmt.Errorf("authority failback runtime: %w", err)
	}
	if err := r.Publisher.apply(AssignmentAsk{
		VolumeID:  req.VolumeID,
		ReplicaID: req.ReplicaID,
		DataAddr:  req.TargetDataAddr,
		CtrlAddr:  req.TargetCtrlAddr,
		Intent:    IntentReassign,
		Evidence:  evidence,
	}); err != nil {
		return FailbackRuntimeResult{}, err
	}
	after, ok := r.Publisher.VolumeAuthorityLine(req.VolumeID)
	if !ok {
		return FailbackRuntimeResult{}, fmt.Errorf("authority failback runtime: missing authority line after failback")
	}
	return FailbackRuntimeResult{
		FailbackStarted:            true,
		AuthorityEpochAdvanced:     after.Epoch > before.Epoch,
		SinglePrimaryAfterFailback: after.ReplicaID == req.ReplicaID,
		PublishTargetSwappedAfterFailback: after.ReplicaID == req.ReplicaID &&
			after.DataAddr == req.TargetDataAddr &&
			after.CtrlAddr == req.TargetCtrlAddr &&
			(before.ReplicaID != after.ReplicaID || before.DataAddr != after.DataAddr || before.CtrlAddr != after.CtrlAddr),
		NoStorageMutation:           true,
		NoCrossVolumeIdentityChange: req.NoCrossVolumeIdentityChange,
		EvidenceRefs: append([]string{
			"authority_failback_reassign_minted",
		}, req.EvidenceRefs...),
	}, nil
}

func validateFailbackRuntimeRequest(req FailbackRuntimeRequest) error {
	if req.VolumeID == "" || req.ReplicaID == "" || req.TargetDataAddr == "" || req.TargetCtrlAddr == "" {
		return fmt.Errorf("authority failback runtime: volumeID, replicaID, targetDataAddr, and targetCtrlAddr are required")
	}
	if req.ExpectedCurrentReplicaID == "" || req.ExpectedCurrentEpoch == 0 {
		return fmt.Errorf("authority failback runtime: expected current replica and epoch are required")
	}
	if !req.AckEligible || !req.FrontendFencedBeforeFailback || !req.DurableFrontierCovered || !req.NoCrossVolumeIdentityChange {
		return fmt.Errorf("authority failback runtime: terminal failback preconditions are not satisfied")
	}
	return nil
}
