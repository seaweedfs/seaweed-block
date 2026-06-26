package ops

import (
	"context"
	"fmt"
	"time"
)

const (
	FailbackStateBlocked    = "blocked"
	FailbackStateFailedBack = "failed_back"

	AuthorityExecutorFailbackReasonDisabled                = "failback_policy_disabled"
	AuthorityExecutorFailbackReasonMissingFacts            = "missing_required_facts"
	AuthorityExecutorFailbackReasonRuntimeTargetMissing    = "failback_runtime_target_missing"
	AuthorityExecutorFailbackReasonRuntimeFailed           = "failback_runtime_failed"
	AuthorityExecutorFailbackReasonInvalidTerminalEvidence = "failback_runtime_invalid_terminal_evidence"
	AuthorityExecutorFailbackReasonCompleted               = "failback_completed"
	AuthorityExecutorFailbackDecisionDisabled              = "disabled"
	AuthorityExecutorFailbackDecisionEnabled               = "enabled"
)

type FailbackExecutorClient interface {
	ListSwBlockReplicaFailbacks(ctx context.Context, namespace string) ([]SwBlockReplicaFailbackObject, error)
	WriteReplicaFailbackStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockReplicaFailbackCRDStatus) error
}

type FailbackExecutorReconciler struct {
	Namespace              string
	Client                 FailbackExecutorClient
	Runtime                FailbackRuntime
	DryRun                 bool
	ExecutionRequested     bool
	ExecutionPolicyEnabled bool
	Now                    func() time.Time
}

type FailbackRuntime interface {
	ExecuteFailback(ctx context.Context, req FailbackRuntimeRequest) (FailbackRuntimeResult, error)
}

type FailbackRuntimeRequest struct {
	VolumeName                   string   `json:"volumeName"`
	VolumeID                     string   `json:"volumeID"`
	PVCName                      string   `json:"pvcName"`
	ReplicaID                    string   `json:"replicaID"`
	RuntimeEndpoint              string   `json:"runtimeEndpoint,omitempty"`
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
	if r.ExecutionRequested && !r.ExecutionPolicyEnabled {
		return FailbackExecutorReconcileResult{}, fmt.Errorf("failback executor execution is disabled by product policy")
	}
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
		if r.ExecutionRequested {
			if err := r.executeTarget(ctx, &result, target); err != nil {
				return result, err
			}
			continue
		}
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

func (r FailbackExecutorReconciler) executeTarget(ctx context.Context, result *FailbackExecutorReconcileResult, target SwBlockReplicaFailbackObject) error {
	status := failbackExecutorDisabledStatus(r.now()(), target)
	if !failbackExecutorExecutableTarget(target) {
		result.InvalidTargetCount++
		status.ReasonCode = AuthorityExecutorFailbackReasonRuntimeTargetMissing
		status.Conditions[0].Reason = AuthorityExecutorFailbackReasonRuntimeTargetMissing
		status.Conditions[0].Message = "failback runtime target is missing required execution facts"
		if !r.DryRun {
			if err := r.Client.WriteReplicaFailbackStatus(ctx, target.Ref, status); err != nil {
				return err
			}
			result.StatusWriteCount++
		}
		return nil
	}
	runtime := r.Runtime
	if runtime == nil && target.Spec.RuntimeEndpoint != "" {
		runtime = NewHTTPFailbackRuntime(target.Spec.RuntimeEndpoint, nil)
	}
	if runtime == nil {
		status.ReasonCode = AuthorityExecutorFailbackReasonRuntimeTargetMissing
		if !r.DryRun {
			if err := r.Client.WriteReplicaFailbackStatus(ctx, target.Ref, status); err != nil {
				return err
			}
			result.StatusWriteCount++
		}
		return nil
	}
	result.FailbackAttempts++
	if r.DryRun {
		return nil
	}
	runtimeResult, err := runtime.ExecuteFailback(ctx, failbackRuntimeRequest(target))
	if err != nil {
		status.ReasonCode = AuthorityExecutorFailbackReasonRuntimeFailed
		if writeErr := r.Client.WriteReplicaFailbackStatus(ctx, target.Ref, status); writeErr != nil {
			return fmt.Errorf("write failed failback status after runtime error %v: %w", err, writeErr)
		}
		result.StatusWriteCount++
		return fmt.Errorf("execute failback runtime: %w", err)
	}
	if !failbackRuntimeTerminalEvidenceValid(runtimeResult) {
		status.ReasonCode = AuthorityExecutorFailbackReasonInvalidTerminalEvidence
		if err := r.Client.WriteReplicaFailbackStatus(ctx, target.Ref, status); err != nil {
			return err
		}
		result.StatusWriteCount++
		return nil
	}
	if err := r.Client.WriteReplicaFailbackStatus(ctx, target.Ref, failbackExecutorFailedBackStatus(r.now()(), target, runtimeResult)); err != nil {
		return err
	}
	result.StatusWriteCount++
	return nil
}

func failbackExecutorTargetValid(target SwBlockReplicaFailbackObject) bool {
	spec := target.Spec
	return spec.VolumeName != "" &&
		spec.ReplicaID != "" &&
		spec.AckEligible &&
		spec.FrontendFencedBeforeFailback &&
		spec.DurableFrontierCovered &&
		spec.NoCrossVolumeIdentityChange &&
		spec.FailbackDecision == AuthorityExecutorFailbackDecisionDisabled &&
		spec.FailbackReason == AuthorityExecutorFailbackReasonDisabled &&
		!spec.FailbackMutationAllowed
}

func failbackExecutorExecutableTarget(target SwBlockReplicaFailbackObject) bool {
	spec := target.Spec
	return spec.VolumeName != "" &&
		spec.ReplicaID != "" &&
		spec.RuntimeEndpoint != "" &&
		spec.AckEligible &&
		spec.FrontendFencedBeforeFailback &&
		spec.DurableFrontierCovered &&
		spec.NoCrossVolumeIdentityChange &&
		spec.FailbackDecision == AuthorityExecutorFailbackDecisionEnabled &&
		spec.FailbackMutationAllowed
}

func failbackRuntimeTerminalEvidenceValid(result FailbackRuntimeResult) bool {
	return result.FailbackStarted &&
		result.AuthorityEpochAdvanced &&
		result.SinglePrimaryAfterFailback &&
		result.PublishTargetSwappedAfterFailback &&
		result.NoStorageMutation &&
		result.NoCrossVolumeIdentityChange
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

func failbackExecutorFailedBackStatus(now time.Time, target SwBlockReplicaFailbackObject, result FailbackRuntimeResult) SwBlockReplicaFailbackCRDStatus {
	return SwBlockReplicaFailbackCRDStatus{
		ObservedAt:                        now,
		Executor:                          "failback-executor",
		State:                             FailbackStateFailedBack,
		ReasonCode:                        AuthorityExecutorFailbackReasonCompleted,
		FailbackMutationAllowed:           false,
		FailbackStarted:                   result.FailbackStarted,
		AuthorityEpochAdvanced:            result.AuthorityEpochAdvanced,
		SinglePrimaryAfterFailback:        result.SinglePrimaryAfterFailback,
		PublishTargetSwappedAfterFailback: result.PublishTargetSwappedAfterFailback,
		NoCrossVolumeIdentityChange:       target.Spec.NoCrossVolumeIdentityChange && result.NoCrossVolumeIdentityChange,
		Conditions: []ObservationCondition{{
			Type:     ConditionRecovered,
			Status:   "True",
			Reason:   AuthorityExecutorFailbackReasonCompleted,
			Severity: "info",
			Message:  "failback runtime reported terminal authority evidence",
		}},
		EvidenceRefs: append([]string(nil), result.EvidenceRefs...),
		NonClaims: []string{
			"no_storage_mutation",
		},
	}
}

func failbackRuntimeRequest(target SwBlockReplicaFailbackObject) FailbackRuntimeRequest {
	spec := target.Spec
	return FailbackRuntimeRequest{
		VolumeName:                   spec.VolumeName,
		VolumeID:                     spec.VolumeID,
		PVCName:                      spec.PVCName,
		ReplicaID:                    spec.ReplicaID,
		RuntimeEndpoint:              spec.RuntimeEndpoint,
		AckEligible:                  spec.AckEligible,
		FrontendFencedBeforeFailback: spec.FrontendFencedBeforeFailback,
		DurableFrontierCovered:       spec.DurableFrontierCovered,
		NoCrossVolumeIdentityChange:  spec.NoCrossVolumeIdentityChange,
	}
}

func (r FailbackExecutorReconciler) now() func() time.Time {
	if r.Now != nil {
		return r.Now
	}
	return time.Now
}
