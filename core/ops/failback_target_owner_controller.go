package ops

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type FailbackTargetOwnerClient interface {
	ListSwBlockVolumes(ctx context.Context, namespace string) ([]SwBlockVolumeObject, error)
	ListSwBlockReplicaFailbacks(ctx context.Context, namespace string) ([]SwBlockReplicaFailbackObject, error)
	CreateSwBlockReplicaFailback(ctx context.Context, namespace string, obj SwBlockReplicaFailbackObject) error
}

type FailbackTargetOwnerReconciler struct {
	Namespace string
	Client    FailbackTargetOwnerClient
	DryRun    bool
	Now       func() time.Time
}

type FailbackTargetOwnerReconcileResult struct {
	VolumeCount                int  `json:"volumeCount"`
	ContractCount              int  `json:"contractCount"`
	TargetPlannedCount         int  `json:"targetPlannedCount"`
	TargetExistingCount        int  `json:"targetExistingCount"`
	TargetCreateCount          int  `json:"targetCreateCount"`
	InvalidContractCount       int  `json:"invalidContractCount"`
	TerminalEvidenceReady      int  `json:"terminalEvidenceReady"`
	TerminalEvidenceMissing    int  `json:"terminalEvidenceMissing"`
	FailbackAttempts           int  `json:"failbackAttempts"`
	StorageMutationAllowed     bool `json:"storageMutationAllowed"`
	FrontendPublicationAllowed bool `json:"frontendPublicationAllowed"`
}

func (r FailbackTargetOwnerReconciler) Reconcile(ctx context.Context) (FailbackTargetOwnerReconcileResult, error) {
	if r.Client == nil {
		return FailbackTargetOwnerReconcileResult{}, fmt.Errorf("failback target owner client is required")
	}
	namespace := defaultString(r.Namespace, "default")
	volumes, err := r.Client.ListSwBlockVolumes(ctx, namespace)
	if err != nil {
		return FailbackTargetOwnerReconcileResult{}, err
	}
	targets, err := r.Client.ListSwBlockReplicaFailbacks(ctx, namespace)
	if err != nil {
		return FailbackTargetOwnerReconcileResult{}, err
	}
	result := FailbackTargetOwnerReconcileResult{VolumeCount: len(volumes)}
	for _, volume := range volumes {
		for _, contract := range volume.Status.ExecutorContracts {
			if contract.ActionType != ManagedVolumeActionFailbackReturned {
				continue
			}
			result.ContractCount++
			if !failbackTargetOwnerContractReady(contract) {
				result.InvalidContractCount++
				continue
			}
			returned, ok := failbackTargetOwnerReturnedReplica(volume, contract)
			if !ok || !failbackTargetOwnerTerminalEvidenceReady(returned) {
				result.TerminalEvidenceMissing++
				continue
			}
			result.TerminalEvidenceReady++
			result.TargetPlannedCount++
			if failbackTargetOwnerHasTarget(volume, contract, targets) {
				result.TargetExistingCount++
				continue
			}
			obj := failbackTargetOwnerObject(namespace, volume, contract, returned)
			if !r.DryRun {
				if err := r.Client.CreateSwBlockReplicaFailback(ctx, namespace, obj); err != nil {
					return result, err
				}
				result.TargetCreateCount++
			}
		}
	}
	return result, nil
}

func failbackTargetOwnerContractReady(contract SwBlockVolumeCRDExecutorContract) bool {
	return contract.Decision == ReturnedReplicaExecutorContractDisabled &&
		contract.Reason == ReturnedReplicaExecutorContractReasonExecutorDisabled &&
		contract.PreflightDecision == ReturnedReplicaExecutorPreflightReady &&
		contract.PreflightReason == ReturnedReplicaExecutorPreflightReasonSatisfied &&
		authorityExecutorStringSliceContains(contract.AllowedMutationClass, "failback") &&
		contract.ReplicaID != ""
}

func failbackTargetOwnerReturnedReplica(volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract) (SwBlockVolumeCRDReturnedReplica, bool) {
	for _, returned := range volume.Status.ReplicaReintegrations {
		if returned.ReplicaID == contract.ReplicaID {
			return returned, true
		}
	}
	return SwBlockVolumeCRDReturnedReplica{}, false
}

func failbackTargetOwnerTerminalEvidenceReady(returned SwBlockVolumeCRDReturnedReplica) bool {
	return returned.FrontendFenced &&
		!returned.FrontendPrimaryReady &&
		returned.AckEligibilityKnown &&
		returned.AckEligible &&
		returned.DurableFrontierKnown &&
		returned.RequiredFrontierKnown &&
		returned.DurableFrontierLSN >= returned.RequiredFrontierLSN
}

func failbackTargetOwnerHasTarget(volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, targets []SwBlockReplicaFailbackObject) bool {
	for _, target := range targets {
		if target.Spec.ReplicaID != contract.ReplicaID {
			continue
		}
		if target.Spec.VolumeName != "" && target.Spec.VolumeName == volume.Ref.Name {
			return true
		}
		if target.Spec.VolumeID != "" && target.Spec.VolumeID == volume.Status.VolumeID {
			return true
		}
		if target.Spec.PVCName != "" && target.Spec.PVCName == volume.Status.PVCName {
			return true
		}
	}
	return false
}

func failbackTargetOwnerObject(namespace string, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica) SwBlockReplicaFailbackObject {
	return SwBlockReplicaFailbackObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockReplicaFailbackKind,
			Namespace:  namespace,
			Name:       failbackTargetOwnerName(volume.Ref.Name, contract.ReplicaID),
		},
		Spec: SwBlockReplicaFailbackSpec{
			VolumeName:                   volume.Ref.Name,
			VolumeID:                     volume.Status.VolumeID,
			PVCName:                      volume.Status.PVCName,
			ReplicaID:                    contract.ReplicaID,
			AckEligible:                  returned.AckEligibilityKnown && returned.AckEligible,
			FrontendFencedBeforeFailback: returned.FrontendFenced && !returned.FrontendPrimaryReady,
			DurableFrontierCovered:       returned.DurableFrontierKnown && returned.RequiredFrontierKnown && returned.DurableFrontierLSN >= returned.RequiredFrontierLSN,
			NoCrossVolumeIdentityChange:  true,
		},
	}
}

func failbackTargetOwnerName(volumeName, replicaID string) string {
	name := strings.ToLower(strings.TrimSpace(volumeName + "-" + replicaID + "-failback"))
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
		return "failback-target"
	}
	if len(out) > 63 {
		out = strings.TrimRight(out[:63], "-")
	}
	return out
}
