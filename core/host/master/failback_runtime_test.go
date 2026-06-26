package master

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/ops"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHostFailbackAuthorityRuntimeUsesLivePublisher(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	current := waitAuthorityLine(t, h.Publisher(), "vol-a")
	if current.ReplicaID != "r2" {
		t.Fatalf("current line=%+v want r2", current)
	}

	result, err := h.FailbackAuthorityRuntime().ExecuteFailback(context.Background(), authority.FailbackRuntimeRequest{
		VolumeID:                     "vol-a",
		ReplicaID:                    "r1",
		TargetDataAddr:               "127.0.0.1:9201",
		TargetCtrlAddr:               "127.0.0.1:9101",
		ExpectedCurrentReplicaID:     current.ReplicaID,
		ExpectedCurrentEpoch:         current.Epoch,
		AckEligible:                  true,
		FrontendFencedBeforeFailback: true,
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
	})
	if err != nil {
		t.Fatalf("failback runtime: %v", err)
	}
	if !result.FailbackStarted ||
		!result.AuthorityEpochAdvanced ||
		!result.SinglePrimaryAfterFailback ||
		!result.PublishTargetSwappedAfterFailback ||
		!result.NoStorageMutation ||
		!result.NoCrossVolumeIdentityChange {
		t.Fatalf("result=%+v", result)
	}
	line, ok := h.Publisher().VolumeAuthorityLine("vol-a")
	if !ok {
		t.Fatalf("missing authority line after failback")
	}
	if line.ReplicaID != "r1" ||
		line.Epoch != current.Epoch+1 ||
		line.DataAddr != "127.0.0.1:9201" ||
		line.CtrlAddr != "127.0.0.1:9101" {
		t.Fatalf("line=%+v current=%+v", line, current)
	}
}

func TestFailbackServiceDefaultDisabled(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)

	_, err := newServices(h).ExecuteFailback(context.Background(), &control.FailbackRequest{
		VolumeId:                     "vol-a",
		ReplicaId:                    "r1",
		TargetDataAddr:               "127.0.0.1:9201",
		TargetCtrlAddr:               "127.0.0.1:9101",
		ExpectedCurrentReplicaId:     "r2",
		ExpectedCurrentEpoch:         1,
		AckEligible:                  true,
		FrontendFencedBeforeFailback: true,
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err=%v code=%s", err, status.Code(err))
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("vol-a"); ok {
		t.Fatalf("disabled failback RPC mutated authority")
	}
}

func TestFailbackServiceEnabledUsesHostRuntime(t *testing.T) {
	h := newTestMasterWithFailbackRuntimeRPC(t)
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	current := waitAuthorityLine(t, h.Publisher(), "vol-a")

	resp, err := newServices(h).ExecuteFailback(context.Background(), &control.FailbackRequest{
		VolumeId:                     "vol-a",
		ReplicaId:                    "r1",
		TargetDataAddr:               "127.0.0.1:9201",
		TargetCtrlAddr:               "127.0.0.1:9101",
		ExpectedCurrentReplicaId:     current.ReplicaID,
		ExpectedCurrentEpoch:         current.Epoch,
		AckEligible:                  true,
		FrontendFencedBeforeFailback: true,
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
		EvidenceRefs:                 []string{"phase81-service-test"},
	})
	if err != nil {
		t.Fatalf("ExecuteFailback: %v", err)
	}
	if !resp.GetFailbackStarted() ||
		!resp.GetAuthorityEpochAdvanced() ||
		!resp.GetSinglePrimaryAfterFailback() ||
		!resp.GetPublishTargetSwappedAfterFailback() ||
		!resp.GetNoStorageMutation() ||
		!resp.GetNoCrossVolumeIdentityChange() {
		t.Fatalf("resp=%+v", resp)
	}
	line, ok := h.Publisher().VolumeAuthorityLine("vol-a")
	if !ok {
		t.Fatalf("missing authority line after service failback")
	}
	if line.ReplicaID != "r1" || line.Epoch != current.Epoch+1 {
		t.Fatalf("line=%+v current=%+v", line, current)
	}
}

func TestFailbackExecutorGRPCRuntimeUsesRealMasterService(t *testing.T) {
	h := newTestMasterWithFailbackRuntimeRPC(t)
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	current := waitAuthorityLine(t, h.Publisher(), "vol-a")
	if current.ReplicaID != "r2" {
		t.Fatalf("current line=%+v want r2", current)
	}

	client := &failbackExecutorMasterServiceClient{
		targets: []ops.SwBlockReplicaFailbackObject{{
			Ref: ops.OperatorObjectRef{
				APIVersion: "block.seaweedfs.com/v1alpha1",
				Kind:       "SwBlockReplicaFailback",
				Namespace:  "kube-system",
				Name:       "vol-a-r1-failback",
			},
			Spec: ops.SwBlockReplicaFailbackSpec{
				VolumeName:                   "vol-a",
				VolumeID:                     "vol-a",
				PVCName:                      "pvc-a",
				ReplicaID:                    "r1",
				TargetDataAddr:               "127.0.0.1:9201",
				TargetCtrlAddr:               "127.0.0.1:9101",
				ExpectedCurrentReplicaID:     current.ReplicaID,
				ExpectedCurrentEpoch:         current.Epoch,
				AckEligible:                  true,
				FrontendFencedBeforeFailback: true,
				DurableFrontierCovered:       true,
				NoCrossVolumeIdentityChange:  true,
				FailbackDecision:             ops.AuthorityExecutorFailbackDecisionEnabled,
				FailbackReason:               "failback_requested",
				FailbackMutationAllowed:      true,
				RuntimeEndpoint:              "grpc://blockmaster",
			},
		}},
	}

	result, err := (ops.FailbackExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                ops.NewGRPCFailbackRuntime(h.Addr()),
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetCount != 1 ||
		result.FailbackAttempts != 1 ||
		result.StatusWriteCount != 1 ||
		!result.AuthorityMutationAllowed ||
		result.FrontendPublicationAllowed ||
		result.StorageMutationAllowed {
		t.Fatalf("result=%+v", result)
	}
	if len(client.writes) != 1 {
		t.Fatalf("writes=%d", len(client.writes))
	}
	status := client.writes[0].status
	if status.State != ops.FailbackStateFailedBack ||
		status.ReasonCode != ops.AuthorityExecutorFailbackReasonCompleted ||
		!status.FailbackStarted ||
		!status.AuthorityEpochAdvanced ||
		!status.SinglePrimaryAfterFailback ||
		!status.PublishTargetSwappedAfterFailback ||
		!status.NoCrossVolumeIdentityChange {
		t.Fatalf("status=%+v", status)
	}
	line, ok := h.Publisher().VolumeAuthorityLine("vol-a")
	if !ok {
		t.Fatalf("missing authority line after integrated failback")
	}
	if line.ReplicaID != "r1" ||
		line.Epoch != current.Epoch+1 ||
		line.DataAddr != "127.0.0.1:9201" ||
		line.CtrlAddr != "127.0.0.1:9101" {
		t.Fatalf("line=%+v current=%+v", line, current)
	}
}

func newTestMasterWithFailbackRuntimeRPC(t *testing.T) *Host {
	t.Helper()
	h, err := New(Config{
		AuthorityStoreDir:  t.TempDir(),
		LifecycleStoreDir:  t.TempDir(),
		Listen:             "127.0.0.1:0",
		FailbackRuntimeRPC: true,
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	return h
}

type failbackExecutorMasterServiceClient struct {
	targets []ops.SwBlockReplicaFailbackObject
	writes  []failbackExecutorMasterServiceWrite
}

type failbackExecutorMasterServiceWrite struct {
	ref    ops.OperatorObjectRef
	status ops.SwBlockReplicaFailbackCRDStatus
}

func (c *failbackExecutorMasterServiceClient) ListSwBlockReplicaFailbacks(context.Context, string) ([]ops.SwBlockReplicaFailbackObject, error) {
	return append([]ops.SwBlockReplicaFailbackObject(nil), c.targets...), nil
}

func (c *failbackExecutorMasterServiceClient) WriteReplicaFailbackStatus(_ context.Context, ref ops.OperatorObjectRef, status ops.SwBlockReplicaFailbackCRDStatus) error {
	c.writes = append(c.writes, failbackExecutorMasterServiceWrite{ref: ref, status: status})
	return nil
}
