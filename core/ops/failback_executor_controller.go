package ops

import (
	"context"
	"fmt"
	"time"
)

const (
	FailbackStateBlocked = "blocked"

	AuthorityExecutorFailbackReasonDisabled     = "failback_policy_disabled"
	AuthorityExecutorFailbackReasonMissingFacts = "missing_required_facts"
)

type FailbackExecutorClient interface {
	ListSwBlockReplicaFailbacks(ctx context.Context, namespace string) ([]SwBlockReplicaFailbackObject, error)
	WriteReplicaFailbackStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockReplicaFailbackCRDStatus) error
}

type FailbackExecutorReconciler struct {
	Namespace string
	Client    FailbackExecutorClient
	DryRun    bool
	Now       func() time.Time
}

type FailbackExecutorReconcileResult struct {
	TargetCount                int  `json:"targetCount"`
	StatusWriteCount           int  `json:"statusWriteCount"`
	InvalidTargetCount         int  `json:"invalidTargetCount"`
	FailbackAttempts           int  `json:"failbackAttempts"`
	AuthorityMutationAllowed   bool `json:"authorityMutationAllowed"`
	FrontendPublicationAllowed bool `json:"frontendPublicationAllowed"`
	StorageMutationAllowed     bool `json:"storageMutationAllowed"`
}

func (r FailbackExecutorReconciler) Reconcile(ctx context.Context) (FailbackExecutorReconcileResult, error) {
	if r.Client == nil {
		return FailbackExecutorReconcileResult{}, fmt.Errorf("failback executor client is required")
	}
	namespace := defaultString(r.Namespace, "default")
	targets, err := r.Client.ListSwBlockReplicaFailbacks(ctx, namespace)
	if err != nil {
		return FailbackExecutorReconcileResult{}, err
	}
	result := FailbackExecutorReconcileResult{TargetCount: len(targets)}
	for _, target := range targets {
		status := failbackExecutorDisabledStatus(r.now()(), target)
		if !failbackExecutorTargetValid(target) {
			result.InvalidTargetCount++
			status.ReasonCode = AuthorityExecutorFailbackReasonMissingFacts
			status.Conditions[0].Reason = AuthorityExecutorFailbackReasonMissingFacts
			status.Conditions[0].Message = "failback target is missing required terminal evidence"
		}
		if r.DryRun {
			continue
		}
		if err := r.Client.WriteReplicaFailbackStatus(ctx, target.Ref, status); err != nil {
			return result, err
		}
		result.StatusWriteCount++
	}
	return result, nil
}

func failbackExecutorTargetValid(target SwBlockReplicaFailbackObject) bool {
	spec := target.Spec
	return spec.VolumeName != "" &&
		spec.ReplicaID != "" &&
		spec.AckEligible &&
		spec.FrontendFencedBeforeFailback &&
		spec.DurableFrontierCovered &&
		spec.NoCrossVolumeIdentityChange
}

func failbackExecutorDisabledStatus(now time.Time, target SwBlockReplicaFailbackObject) SwBlockReplicaFailbackCRDStatus {
	return SwBlockReplicaFailbackCRDStatus{
		ObservedAt:                        now,
		Executor:                          "failback-executor",
		State:                             FailbackStateBlocked,
		ReasonCode:                        AuthorityExecutorFailbackReasonDisabled,
		FailbackMutationAllowed:           false,
		FailbackStarted:                   false,
		AuthorityEpochAdvanced:            false,
		SinglePrimaryAfterFailback:        false,
		PublishTargetSwappedAfterFailback: false,
		NoCrossVolumeIdentityChange:       target.Spec.NoCrossVolumeIdentityChange,
		Conditions: []ObservationCondition{{
			Type:     ConditionBlocked,
			Status:   "True",
			Reason:   AuthorityExecutorFailbackReasonDisabled,
			Severity: "warning",
			Message:  "failback executor is disabled by product policy",
		}},
		NonClaims: []string{
			"no_failback",
			"no_authority_epoch_advance",
			"no_primary_reassignment",
			"no_publish_target_swap",
			"no_frontend_publication",
			"no_storage_mutation",
		},
	}
}

func (r FailbackExecutorReconciler) now() func() time.Time {
	if r.Now != nil {
		return r.Now
	}
	return time.Now
}
