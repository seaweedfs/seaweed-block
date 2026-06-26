package master

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
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
