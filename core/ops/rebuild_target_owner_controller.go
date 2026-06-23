package ops

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type RebuildTargetOwnerClient interface {
	ListSwBlockVolumes(ctx context.Context, namespace string) ([]SwBlockVolumeObject, error)
	ListSwBlockReplicaRebuilds(ctx context.Context, namespace string) ([]SwBlockReplicaRebuildObject, error)
	CreateSwBlockReplicaRebuild(ctx context.Context, namespace string, obj SwBlockReplicaRebuildObject) error
}

type RebuildTargetOwnerReconciler struct {
	Namespace string
	Client    RebuildTargetOwnerClient
	DryRun    bool
	Now       func() time.Time
}

type RebuildTargetOwnerReconcileResult struct {
	VolumeCount          int `json:"volumeCount"`
	ContractCount        int `json:"contractCount"`
	TargetPlannedCount   int `json:"targetPlannedCount"`
	TargetExistingCount  int `json:"targetExistingCount"`
	TargetCreateCount    int `json:"targetCreateCount"`
	InvalidContractCount int `json:"invalidContractCount"`
}

func (r RebuildTargetOwnerReconciler) Reconcile(ctx context.Context) (RebuildTargetOwnerReconcileResult, error) {
	if r.Client == nil {
		return RebuildTargetOwnerReconcileResult{}, fmt.Errorf("rebuild target owner client is required")
	}
	namespace := defaultString(r.Namespace, "default")
	volumes, err := r.Client.ListSwBlockVolumes(ctx, namespace)
	if err != nil {
		return RebuildTargetOwnerReconcileResult{}, err
	}
	targets, err := r.Client.ListSwBlockReplicaRebuilds(ctx, namespace)
	if err != nil {
		return RebuildTargetOwnerReconcileResult{}, err
	}
	result := RebuildTargetOwnerReconcileResult{VolumeCount: len(volumes)}
	for _, volume := range volumes {
		for _, contract := range volume.Status.ExecutorContracts {
			if contract.ActionType != ManagedVolumeActionRebuildReturned {
				continue
			}
			result.ContractCount++
			if !rebuildTargetOwnerContractReady(contract) {
				result.InvalidContractCount++
				continue
			}
			result.TargetPlannedCount++
			if rebuildTargetOwnerHasTarget(volume, contract, targets) {
				result.TargetExistingCount++
				continue
			}
			obj := rebuildTargetOwnerObject(namespace, volume, contract)
			if !r.DryRun {
				if err := r.Client.CreateSwBlockReplicaRebuild(ctx, namespace, obj); err != nil {
					return result, err
				}
				result.TargetCreateCount++
			}
		}
	}
	return result, nil
}

func rebuildTargetOwnerContractReady(contract SwBlockVolumeCRDExecutorContract) bool {
	return contract.Decision == ReturnedReplicaExecutorContractDisabled &&
		contract.Reason == ReturnedReplicaExecutorContractReasonExecutorDisabled &&
		contract.PreflightDecision == ReturnedReplicaExecutorPreflightReady &&
		contract.PreflightReason == ReturnedReplicaExecutorPreflightReasonSatisfied &&
		authorityExecutorStringSliceContains(contract.AllowedMutationClass, AuthorityExecutorAllowedMutationRebuildTraffic) &&
		contract.ReplicaID != ""
}

func rebuildTargetOwnerHasTarget(volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, targets []SwBlockReplicaRebuildObject) bool {
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

func rebuildTargetOwnerObject(namespace string, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract) SwBlockReplicaRebuildObject {
	return SwBlockReplicaRebuildObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockReplicaRebuildKind,
			Namespace:  namespace,
			Name:       rebuildTargetOwnerName(volume.Ref.Name, contract.ReplicaID),
		},
		Spec: SwBlockReplicaRebuildSpec{
			VolumeName: volume.Ref.Name,
			VolumeID:   volume.Status.VolumeID,
			PVCName:    volume.Status.PVCName,
			ReplicaID:  contract.ReplicaID,
		},
	}
}

func rebuildTargetOwnerName(volumeName, replicaID string) string {
	name := strings.ToLower(strings.TrimSpace(volumeName + "-" + replicaID + "-rebuild"))
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
		return "rebuild-target"
	}
	if len(out) > 63 {
		out = strings.TrimRight(out[:63], "-")
	}
	return out
}
