package ops

import (
	"context"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// AuthorityFailbackRuntime adapts the ops failback executor contract to the
// authority package seam. The executor remains policy-gated; this adapter only
// performs the already-authorized authority reassignment.
type AuthorityFailbackRuntime struct {
	Runtime authority.FailbackAuthorityRuntime
}

func NewAuthorityFailbackRuntime(publisher *authority.Publisher) *AuthorityFailbackRuntime {
	return &AuthorityFailbackRuntime{
		Runtime: authority.FailbackAuthorityRuntime{Publisher: publisher},
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
