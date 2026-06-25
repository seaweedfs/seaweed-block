package ops

import (
	"context"
	"fmt"
	"time"
)

const (
	FrontendPublicationStateBlocked = "blocked"
)

type FrontendPublicationExecutorClient interface {
	ListSwBlockFrontendPublications(ctx context.Context, namespace string) ([]SwBlockFrontendPublicationObject, error)
	WriteFrontendPublicationStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockFrontendPublicationCRDStatus) error
}

type FrontendPublicationExecutorReconciler struct {
	Namespace string
	Client    FrontendPublicationExecutorClient
	DryRun    bool
	Now       func() time.Time
}

type FrontendPublicationExecutorReconcileResult struct {
	TargetCount                 int  `json:"targetCount"`
	StatusWriteCount            int  `json:"statusWriteCount"`
	InvalidTargetCount          int  `json:"invalidTargetCount"`
	FrontendPublicationAttempts int  `json:"frontendPublicationAttempts"`
	FailbackAttempts            int  `json:"failbackAttempts"`
	StorageMutationAllowed      bool `json:"storageMutationAllowed"`
}

func (r FrontendPublicationExecutorReconciler) Reconcile(ctx context.Context) (FrontendPublicationExecutorReconcileResult, error) {
	if r.Client == nil {
		return FrontendPublicationExecutorReconcileResult{}, fmt.Errorf("frontend publication executor client is required")
	}
	namespace := defaultString(r.Namespace, "default")
	targets, err := r.Client.ListSwBlockFrontendPublications(ctx, namespace)
	if err != nil {
		return FrontendPublicationExecutorReconcileResult{}, err
	}
	result := FrontendPublicationExecutorReconcileResult{TargetCount: len(targets)}
	for _, target := range targets {
		status := frontendPublicationExecutorDisabledStatus(r.now()(), target)
		if !frontendPublicationExecutorTargetValid(target) {
			result.InvalidTargetCount++
			status.ReasonCode = "missing_required_facts"
		}
		if r.DryRun {
			continue
		}
		if err := r.Client.WriteFrontendPublicationStatus(ctx, target.Ref, status); err != nil {
			return result, err
		}
		result.StatusWriteCount++
	}
	return result, nil
}

func frontendPublicationExecutorTargetValid(target SwBlockFrontendPublicationObject) bool {
	spec := target.Spec
	return spec.VolumeName != "" &&
		spec.ReplicaID != "" &&
		spec.AckEligibilityKnown &&
		spec.AckEligible &&
		spec.FrontendFencedAfterExecution &&
		spec.PrimaryUnchanged &&
		spec.DurableFrontierCovered &&
		spec.NoCrossVolumeIdentityChange &&
		spec.FrontendPublicationDecision == AuthorityExecutorPublicationDecisionDisabled &&
		spec.FrontendPublicationReason == AuthorityExecutorFrontendPublicationReasonDisabled &&
		!spec.FrontendPublicationMutationAllowed
}

func frontendPublicationExecutorDisabledStatus(now time.Time, target SwBlockFrontendPublicationObject) SwBlockFrontendPublicationCRDStatus {
	return SwBlockFrontendPublicationCRDStatus{
		ObservedAt:                  now,
		Executor:                    "frontend-publication-executor",
		State:                       FrontendPublicationStateBlocked,
		ReasonCode:                  AuthorityExecutorFrontendPublicationReasonDisabled,
		PublicationMutationAllowed:  false,
		FrontendPublished:           false,
		FailbackStarted:             false,
		NoStorageMutation:           true,
		NoCrossVolumeIdentityChange: target.Spec.NoCrossVolumeIdentityChange,
		Conditions: []ObservationCondition{{
			Type:     ConditionBlocked,
			Status:   "True",
			Reason:   AuthorityExecutorFrontendPublicationReasonDisabled,
			Severity: "warning",
			Message:  "frontend publication executor is disabled by product policy",
		}},
		NonClaims: []string{
			"no_frontend_publication",
			"no_failback",
			"no_storage_mutation",
		},
	}
}

func (r FrontendPublicationExecutorReconciler) now() func() time.Time {
	if r.Now != nil {
		return r.Now
	}
	return time.Now
}
