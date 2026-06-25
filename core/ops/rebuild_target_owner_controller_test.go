package ops

import (
	"context"
	"testing"
)

func TestRebuildTargetOwnerDryRunPlansTargetWithoutCreate(t *testing.T) {
	client := &fakeRebuildTargetOwnerClient{
		volumes: []SwBlockVolumeObject{rebuildTargetOwnerTestVolume()},
	}
	result, err := (RebuildTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
		DryRun:    true,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.VolumeCount != 1 ||
		result.ContractCount != 1 ||
		result.TargetPlannedCount != 1 ||
		result.TargetCreateCount != 0 ||
		result.InvalidContractCount != 0 ||
		result.RuntimeTargetReadyCount != 1 ||
		result.RuntimeTargetMissingCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 0 {
		t.Fatalf("dry-run created targets: %+v", client.creates)
	}
}

func TestRebuildTargetOwnerCreatesMissingTarget(t *testing.T) {
	client := &fakeRebuildTargetOwnerClient{
		volumes: []SwBlockVolumeObject{rebuildTargetOwnerTestVolume()},
	}
	result, err := (RebuildTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetCreateCount != 1 || result.TargetExistingCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 1 {
		t.Fatalf("creates=%+v", client.creates)
	}
	created := client.creates[0]
	if created.Ref.Name != "demo-pvc-r2-rebuild" ||
		created.Ref.Namespace != "kube-system" ||
		created.Spec.VolumeName != "demo-pvc" ||
		created.Spec.VolumeID != "pvc-demo" ||
		created.Spec.PVCName != "demo-pvc" ||
		created.Spec.ReplicaID != "r2" ||
		created.Spec.RuntimeEndpoint != "http://127.0.0.1:23260/rebuild/runtime" ||
		created.Spec.TargetDataAddr != "127.0.0.1:19103" ||
		created.Spec.SessionID != 1001 ||
		created.Spec.Epoch != 7 ||
		created.Spec.EndpointVersion != 3 ||
		created.Spec.FromLSN != 52 ||
		created.Spec.FrontierHintLSN != 53 ||
		created.Spec.BasePinLSN != 60 {
		t.Fatalf("created=%+v", created)
	}
	if created.Status.State != "" {
		t.Fatalf("target owner must not pre-populate status: %+v", created.Status)
	}
}

func TestRebuildTargetOwnerSkipsExistingTarget(t *testing.T) {
	client := &fakeRebuildTargetOwnerClient{
		volumes: []SwBlockVolumeObject{rebuildTargetOwnerTestVolume()},
		rebuilds: []SwBlockReplicaRebuildObject{{
			Ref: OperatorObjectRef{
				Namespace: "kube-system",
				Name:      "existing",
			},
			Spec: SwBlockReplicaRebuildSpec{
				VolumeName: "demo-pvc",
				ReplicaID:  "r2",
			},
		}},
	}
	result, err := (RebuildTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetExistingCount != 1 || result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 0 {
		t.Fatalf("unexpected creates=%+v", client.creates)
	}
}

func TestRebuildTargetOwnerDoesNotCreateTargetWithoutRuntimeFacts(t *testing.T) {
	volume := rebuildTargetOwnerTestVolume()
	volume.Status.ReplicaReintegrations[0].RuntimeEndpoint = ""
	client := &fakeRebuildTargetOwnerClient{
		volumes: []SwBlockVolumeObject{volume},
	}
	result, err := (RebuildTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.ContractCount != 1 ||
		result.RuntimeTargetMissingCount != 1 ||
		result.RuntimeTargetReadyCount != 0 ||
		result.TargetPlannedCount != 0 ||
		result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 0 {
		t.Fatalf("unexpected creates=%+v", client.creates)
	}
}

func TestRebuildTargetOwnerRejectsInvalidContract(t *testing.T) {
	volume := rebuildTargetOwnerTestVolume()
	volume.Status.ExecutorContracts[0].AllowedMutationClass = []string{AuthorityExecutorAllowedMutationAckEligibility}
	client := &fakeRebuildTargetOwnerClient{
		volumes: []SwBlockVolumeObject{volume},
	}
	result, err := (RebuildTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.InvalidContractCount != 1 ||
		result.TargetPlannedCount != 0 ||
		result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func rebuildTargetOwnerTestVolume() SwBlockVolumeObject {
	return SwBlockVolumeObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockVolumeKind,
			Namespace:  "kube-system",
			Name:       "demo-pvc",
		},
		Status: SwBlockVolumeCRDStatus{
			VolumeID: "pvc-demo",
			PVCName:  "demo-pvc",
			ReplicaReintegrations: []SwBlockVolumeCRDReturnedReplica{{
				ReplicaID:             "r2",
				State:                 ReturnedReplicaStateRecovering,
				ReasonCode:            ReasonCandidateFrontierBehind,
				FrontendFenced:        true,
				FrontendPrimaryReady:  false,
				DurableFrontierKnown:  true,
				DurableFrontierLSN:    51,
				RequiredFrontierKnown: true,
				RequiredFrontierLSN:   53,
				RuntimeEndpoint:       "http://127.0.0.1:23260/rebuild/runtime",
				TargetDataAddr:        "127.0.0.1:19103",
				SessionID:             1001,
				Epoch:                 7,
				EndpointVersion:       3,
				FromLSN:               52,
				FrontierHintLSN:       53,
				BasePinLSN:            60,
			}},
			ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
				ActionType:           ManagedVolumeActionRebuildReturned,
				ReplicaID:            "r2",
				Decision:             ReturnedReplicaExecutorContractDisabled,
				Reason:               ReturnedReplicaExecutorContractReasonExecutorDisabled,
				PreflightDecision:    ReturnedReplicaExecutorPreflightReady,
				PreflightReason:      ReturnedReplicaExecutorPreflightReasonSatisfied,
				AllowedMutationClass: []string{AuthorityExecutorAllowedMutationRebuildTraffic},
			}},
		},
	}
}

type fakeRebuildTargetOwnerClient struct {
	volumes  []SwBlockVolumeObject
	rebuilds []SwBlockReplicaRebuildObject
	creates  []SwBlockReplicaRebuildObject
}

func (f *fakeRebuildTargetOwnerClient) ListSwBlockVolumes(context.Context, string) ([]SwBlockVolumeObject, error) {
	return append([]SwBlockVolumeObject(nil), f.volumes...), nil
}

func (f *fakeRebuildTargetOwnerClient) ListSwBlockReplicaRebuilds(context.Context, string) ([]SwBlockReplicaRebuildObject, error) {
	return append([]SwBlockReplicaRebuildObject(nil), f.rebuilds...), nil
}

func (f *fakeRebuildTargetOwnerClient) CreateSwBlockReplicaRebuild(_ context.Context, _ string, obj SwBlockReplicaRebuildObject) error {
	f.creates = append(f.creates, obj)
	return nil
}
