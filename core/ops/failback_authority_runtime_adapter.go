package ops

import (
	"context"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
)

// AuthorityFailbackRuntime adapts the ops failback executor contract to the
// authority package seam. The executor remains policy-gated; this adapter only
// performs the already-authorized authority reassignment.
type AuthorityFailbackRuntime struct {
	Runtime authority.FailbackAuthorityRuntime
}

// NewAuthorityFailbackRuntime adapts the authority failback runtime; gate is
// the promotion gate's planned-move decision and endpoint the master's record
// of the target's addresses (nil either: every failback refuses).
func NewAuthorityFailbackRuntime(publisher *authority.Publisher, gate func(volumeID, targetReplicaID string, current authority.AuthorityBasis) (adapter.PrimaryEvidence, error), endpoint func(volumeID, targetReplicaID string) (string, string, bool)) *AuthorityFailbackRuntime {
	return &AuthorityFailbackRuntime{
		Runtime: authority.FailbackAuthorityRuntime{Publisher: publisher, Gate: gate, TargetEndpoint: endpoint},
	}
}

func (r *AuthorityFailbackRuntime) ExecuteFailback(ctx context.Context, req FailbackRuntimeRequest) (FailbackRuntimeResult, error) {
	result, err := r.Runtime.ExecuteFailback(ctx, authority.FailbackRuntimeRequest{
		VolumeID:                     req.VolumeID,
		ReplicaID:                    req.ReplicaID,
		TargetDataAddr:               req.TargetDataAddr,
		TargetCtrlAddr:               req.TargetCtrlAddr,
		ExpectedCurrentReplicaID:     req.ExpectedCurrentReplicaID,
		ExpectedCurrentEpoch:         req.ExpectedCurrentEpoch,
		AckEligible:                  req.AckEligible,
		FrontendFencedBeforeFailback: req.FrontendFencedBeforeFailback,
		DurableFrontierCovered:       req.DurableFrontierCovered,
		NoCrossVolumeIdentityChange:  req.NoCrossVolumeIdentityChange,
		EvidenceRefs:                 append([]string(nil), req.EvidenceRefs...),
	})
	if err != nil {
		return FailbackRuntimeResult{}, err
	}
	return FailbackRuntimeResult{
		FailbackStarted:                   result.FailbackStarted,
		AuthorityEpochAdvanced:            result.AuthorityEpochAdvanced,
		SinglePrimaryAfterFailback:        result.SinglePrimaryAfterFailback,
		PublishTargetSwappedAfterFailback: result.PublishTargetSwappedAfterFailback,
		NoStorageMutation:                 result.NoStorageMutation,
		NoCrossVolumeIdentityChange:       result.NoCrossVolumeIdentityChange,
		EvidenceRefs:                      append([]string(nil), result.EvidenceRefs...),
	}, nil
}
