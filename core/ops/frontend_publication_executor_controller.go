package ops

import (
	"context"
	"fmt"
	"time"
)

const (
	FrontendPublicationStateBlocked   = "blocked"
	FrontendPublicationStatePublished = "published"
)

type FrontendPublicationExecutorClient interface {
	ListSwBlockFrontendPublications(ctx context.Context, namespace string) ([]SwBlockFrontendPublicationObject, error)
	WriteFrontendPublicationStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockFrontendPublicationCRDStatus) error
}

type FrontendPublicationExecutorReconciler struct {
	Namespace              string
	Client                 FrontendPublicationExecutorClient
	Runtime                FrontendPublicationRuntime
	DryRun                 bool
	ExecutionRequested     bool
	ExecutionPolicyEnabled bool
	Now                    func() time.Time
}

type FrontendPublicationRuntime interface {
	ExecuteFrontendPublication(ctx context.Context, req FrontendPublicationRuntimeRequest) (FrontendPublicationRuntimeResult, error)
}

type FrontendPublicationRuntimeRequest struct {
	VolumeName                   string   `json:"volumeName"`
	VolumeID                     string   `json:"volumeID"`
	PVCName                      string   `json:"pvcName"`
	ReplicaID                    string   `json:"replicaID"`
	RuntimeEndpoint              string   `json:"runtimeEndpoint,omitempty"`
	SourceEligibilityName        string   `json:"sourceEligibilityName,omitempty"`
	AckEligibilityKnown          bool     `json:"ackEligibilityKnown"`
	AckEligible                  bool     `json:"ackEligible"`
	FrontendFencedAfterExecution bool     `json:"frontendFencedAfterExecution"`
	PrimaryUnchanged             bool     `json:"primaryUnchanged"`
	DurableFrontierCovered       bool     `json:"durableFrontierCovered"`
	NoCrossVolumeIdentityChange  bool     `json:"noCrossVolumeIdentityChange"`
	EvidenceRefs                 []string `json:"evidenceRefs,omitempty"`
}

type FrontendPublicationRuntimeResult struct {
	FrontendPublished           bool     `json:"frontendPublished"`
	FailbackStarted             bool     `json:"failbackStarted"`
	NoStorageMutation           bool     `json:"noStorageMutation"`
	NoCrossVolumeIdentityChange bool     `json:"noCrossVolumeIdentityChange"`
	EvidenceRefs                []string `json:"evidenceRefs,omitempty"`
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
	if r.ExecutionRequested && !r.ExecutionPolicyEnabled {
		return FrontendPublicationExecutorReconcileResult{}, fmt.Errorf("frontend publication executor execution is disabled by product policy")
	}
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
		if r.ExecutionRequested {
			if err := r.executeTarget(ctx, &result, target); err != nil {
				return result, err
			}
			continue
		}
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

func (r FrontendPublicationExecutorReconciler) executeTarget(ctx context.Context, result *FrontendPublicationExecutorReconcileResult, target SwBlockFrontendPublicationObject) error {
	status := frontendPublicationExecutorDisabledStatus(r.now()(), target)
	if frontendPublicationExecutorRequiresAuthorityOwner(target) {
		result.InvalidTargetCount++
		status.ReasonCode = AuthorityExecutorFrontendPublicationReasonAuthorityOwnerRequired
		status.Conditions[0].Reason = AuthorityExecutorFrontendPublicationReasonAuthorityOwnerRequired
		status.Conditions[0].Message = "frontend publication requires an authority/failback owner; primary-unchanged runtime publication is not a valid product side effect"
		if !r.DryRun {
			if err := r.Client.WriteFrontendPublicationStatus(ctx, target.Ref, status); err != nil {
				return err
			}
			result.StatusWriteCount++
		}
		return nil
	}
	if !frontendPublicationExecutorExecutableTarget(target) {
		result.InvalidTargetCount++
		status.ReasonCode = "missing_required_facts"
		if !r.DryRun {
			if err := r.Client.WriteFrontendPublicationStatus(ctx, target.Ref, status); err != nil {
				return err
			}
			result.StatusWriteCount++
		}
		return nil
	}
	runtime := r.Runtime
	if runtime == nil && target.Spec.RuntimeEndpoint != "" {
		runtime = NewHTTPFrontendPublicationRuntime(target.Spec.RuntimeEndpoint, nil)
	}
	if runtime == nil {
		status.ReasonCode = "frontend_publication_runtime_target_missing"
		if !r.DryRun {
			if err := r.Client.WriteFrontendPublicationStatus(ctx, target.Ref, status); err != nil {
				return err
			}
			result.StatusWriteCount++
		}
		return nil
	}
	result.FrontendPublicationAttempts++
	if r.DryRun {
		return nil
	}
	runtimeResult, err := runtime.ExecuteFrontendPublication(ctx, frontendPublicationRuntimeRequest(target))
	if err != nil {
		status.ReasonCode = "frontend_publication_runtime_failed"
		if writeErr := r.Client.WriteFrontendPublicationStatus(ctx, target.Ref, status); writeErr != nil {
			return fmt.Errorf("write failed frontend publication status after runtime error %v: %w", err, writeErr)
		}
		result.StatusWriteCount++
		return fmt.Errorf("execute frontend publication runtime: %w", err)
	}
	if runtimeResult.FailbackStarted {
		result.FailbackAttempts++
	}
	if !frontendPublicationRuntimeTerminalEvidenceValid(runtimeResult) {
		status.ReasonCode = "frontend_publication_runtime_invalid_terminal_evidence"
		if err := r.Client.WriteFrontendPublicationStatus(ctx, target.Ref, status); err != nil {
			return err
		}
		result.StatusWriteCount++
		return nil
	}
	if err := r.Client.WriteFrontendPublicationStatus(ctx, target.Ref, frontendPublicationExecutorPublishedStatus(r.now()(), target, runtimeResult)); err != nil {
		return err
	}
	result.StatusWriteCount++
	return nil
}

func frontendPublicationExecutorRequiresAuthorityOwner(target SwBlockFrontendPublicationObject) bool {
	spec := target.Spec
	return spec.FrontendPublicationDecision == AuthorityExecutorPublicationDecisionEnabled &&
		spec.FrontendPublicationMutationAllowed &&
		spec.SourceEligibilityName != "" &&
		spec.PrimaryUnchanged
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

func frontendPublicationExecutorExecutableTarget(target SwBlockFrontendPublicationObject) bool {
	spec := target.Spec
	return spec.VolumeName != "" &&
		spec.ReplicaID != "" &&
		spec.RuntimeEndpoint != "" &&
		spec.AckEligibilityKnown &&
		spec.AckEligible &&
		spec.FrontendFencedAfterExecution &&
		spec.PrimaryUnchanged &&
		spec.DurableFrontierCovered &&
		spec.NoCrossVolumeIdentityChange &&
		spec.FrontendPublicationDecision == AuthorityExecutorPublicationDecisionEnabled &&
		spec.FrontendPublicationMutationAllowed
}

func frontendPublicationRuntimeTerminalEvidenceValid(result FrontendPublicationRuntimeResult) bool {
	return result.FrontendPublished &&
		!result.FailbackStarted &&
		result.NoStorageMutation &&
		result.NoCrossVolumeIdentityChange
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

func frontendPublicationExecutorPublishedStatus(now time.Time, target SwBlockFrontendPublicationObject, result FrontendPublicationRuntimeResult) SwBlockFrontendPublicationCRDStatus {
	return SwBlockFrontendPublicationCRDStatus{
		ObservedAt:                  now,
		Executor:                    "frontend-publication-executor",
		State:                       FrontendPublicationStatePublished,
		ReasonCode:                  AuthorityExecutorFrontendPublicationReasonPublished,
		PublicationMutationAllowed:  false,
		FrontendPublished:           result.FrontendPublished,
		FailbackStarted:             result.FailbackStarted,
		NoStorageMutation:           result.NoStorageMutation,
		NoCrossVolumeIdentityChange: target.Spec.NoCrossVolumeIdentityChange && result.NoCrossVolumeIdentityChange,
		Conditions: []ObservationCondition{{
			Type:     ConditionReady,
			Status:   "True",
			Reason:   AuthorityExecutorFrontendPublicationReasonPublished,
			Severity: "info",
			Message:  "frontend publication runtime reported publication complete",
		}},
		EvidenceRefs: append([]string(nil), result.EvidenceRefs...),
		NonClaims: []string{
			"no_failback",
			"no_storage_mutation",
		},
	}
}

func frontendPublicationRuntimeRequest(target SwBlockFrontendPublicationObject) FrontendPublicationRuntimeRequest {
	spec := target.Spec
	return FrontendPublicationRuntimeRequest{
		VolumeName:                   spec.VolumeName,
		VolumeID:                     spec.VolumeID,
		PVCName:                      spec.PVCName,
		ReplicaID:                    spec.ReplicaID,
		RuntimeEndpoint:              spec.RuntimeEndpoint,
		SourceEligibilityName:        spec.SourceEligibilityName,
		AckEligibilityKnown:          spec.AckEligibilityKnown,
		AckEligible:                  spec.AckEligible,
		FrontendFencedAfterExecution: spec.FrontendFencedAfterExecution,
		PrimaryUnchanged:             spec.PrimaryUnchanged,
		DurableFrontierCovered:       spec.DurableFrontierCovered,
		NoCrossVolumeIdentityChange:  spec.NoCrossVolumeIdentityChange,
	}
}

func (r FrontendPublicationExecutorReconciler) now() func() time.Time {
	if r.Now != nil {
		return r.Now
	}
	return time.Now
}
