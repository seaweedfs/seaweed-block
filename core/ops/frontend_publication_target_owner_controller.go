package ops

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type FrontendPublicationTargetOwnerClient interface {
	ListSwBlockReplicaEligibilities(ctx context.Context, namespace string) ([]SwBlockReplicaEligibilityObject, error)
	ListSwBlockFrontendPublications(ctx context.Context, namespace string) ([]SwBlockFrontendPublicationObject, error)
	CreateSwBlockFrontendPublication(ctx context.Context, namespace string, obj SwBlockFrontendPublicationObject) error
}

type FrontendPublicationTargetOwnerReconciler struct {
	Namespace string
	Client    FrontendPublicationTargetOwnerClient
	DryRun    bool
	Now       func() time.Time
}

type FrontendPublicationTargetOwnerReconcileResult struct {
	EligibilityCount            int  `json:"eligibilityCount"`
	ReadyEligibilityCount       int  `json:"readyEligibilityCount"`
	TargetPlannedCount          int  `json:"targetPlannedCount"`
	TargetExistingCount         int  `json:"targetExistingCount"`
	TargetCreateCount           int  `json:"targetCreateCount"`
	InvalidEligibilityCount     int  `json:"invalidEligibilityCount"`
	FrontendPublicationAttempts int  `json:"frontendPublicationAttempts"`
	FailbackAttempts            int  `json:"failbackAttempts"`
	StorageMutationAllowed      bool `json:"storageMutationAllowed"`
}

func (r FrontendPublicationTargetOwnerReconciler) Reconcile(ctx context.Context) (FrontendPublicationTargetOwnerReconcileResult, error) {
	if r.Client == nil {
		return FrontendPublicationTargetOwnerReconcileResult{}, fmt.Errorf("frontend publication target owner client is required")
	}
	namespace := defaultString(r.Namespace, "default")
	eligibilities, err := r.Client.ListSwBlockReplicaEligibilities(ctx, namespace)
	if err != nil {
		return FrontendPublicationTargetOwnerReconcileResult{}, err
	}
	targets, err := r.Client.ListSwBlockFrontendPublications(ctx, namespace)
	if err != nil {
		return FrontendPublicationTargetOwnerReconcileResult{}, err
	}
	result := FrontendPublicationTargetOwnerReconcileResult{EligibilityCount: len(eligibilities)}
	for _, eligibility := range eligibilities {
		if !frontendPublicationTargetOwnerEligibilityReady(eligibility) {
			result.InvalidEligibilityCount++
			continue
		}
		result.ReadyEligibilityCount++
		result.TargetPlannedCount++
		if frontendPublicationTargetOwnerHasTarget(eligibility, targets) {
			result.TargetExistingCount++
			continue
		}
		obj := frontendPublicationTargetOwnerObject(namespace, eligibility)
		if !r.DryRun {
			if err := r.Client.CreateSwBlockFrontendPublication(ctx, namespace, obj); err != nil {
				return result, err
			}
			result.TargetCreateCount++
		}
	}
	return result, nil
}

func frontendPublicationTargetOwnerEligibilityReady(eligibility SwBlockReplicaEligibilityObject) bool {
	status := eligibility.Status
	return eligibility.Spec.VolumeName != "" &&
		eligibility.Spec.ReplicaID != "" &&
		status.AckEligibilityKnown &&
		status.AckEligible &&
		status.FrontendFencedAfterExecution &&
		status.PrimaryUnchanged &&
		status.DurableFrontierCovered &&
		status.NoCrossVolumeIdentityChange &&
		status.FrontendPublicationDecision == AuthorityExecutorPublicationDecisionDisabled &&
		status.FrontendPublicationReason == AuthorityExecutorFrontendPublicationReasonDisabled &&
		!status.FrontendPublicationMutationAllowed
}

func frontendPublicationTargetOwnerHasTarget(eligibility SwBlockReplicaEligibilityObject, targets []SwBlockFrontendPublicationObject) bool {
	for _, target := range targets {
		if target.Spec.ReplicaID != eligibility.Spec.ReplicaID {
			continue
		}
		if target.Spec.VolumeName != "" && target.Spec.VolumeName == eligibility.Spec.VolumeName {
			return true
		}
		if target.Spec.VolumeID != "" && target.Spec.VolumeID == eligibility.Spec.VolumeID {
			return true
		}
		if target.Spec.PVCName != "" && target.Spec.PVCName == eligibility.Spec.PVCName {
			return true
		}
	}
	return false
}

func frontendPublicationTargetOwnerObject(namespace string, eligibility SwBlockReplicaEligibilityObject) SwBlockFrontendPublicationObject {
	status := eligibility.Status
	return SwBlockFrontendPublicationObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockFrontendPublicationKind,
			Namespace:  namespace,
			Name:       frontendPublicationTargetOwnerName(eligibility.Spec.VolumeName, eligibility.Spec.ReplicaID),
		},
		Spec: SwBlockFrontendPublicationSpec{
			VolumeName:                         eligibility.Spec.VolumeName,
			VolumeID:                           eligibility.Spec.VolumeID,
			PVCName:                            eligibility.Spec.PVCName,
			ReplicaID:                          eligibility.Spec.ReplicaID,
			SourceEligibilityName:              eligibility.Ref.Name,
			AckEligibilityKnown:                status.AckEligibilityKnown,
			AckEligible:                        status.AckEligible,
			FrontendFencedAfterExecution:       status.FrontendFencedAfterExecution,
			PrimaryUnchanged:                   status.PrimaryUnchanged,
			DurableFrontierCovered:             status.DurableFrontierCovered,
			NoCrossVolumeIdentityChange:        status.NoCrossVolumeIdentityChange,
			FrontendPublicationDecision:        status.FrontendPublicationDecision,
			FrontendPublicationReason:          status.FrontendPublicationReason,
			FrontendPublicationMutationAllowed: status.FrontendPublicationMutationAllowed,
		},
	}
}

func frontendPublicationTargetOwnerName(volumeName, replicaID string) string {
	name := strings.ToLower(strings.TrimSpace(volumeName + "-" + replicaID + "-frontend-publication"))
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "frontend-publication-target"
	}
	if len(out) > 63 {
		out = strings.TrimRight(out[:63], "-")
	}
	return out
}
