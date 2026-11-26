package master

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// The operator's failover publishes the new primary at the addresses the
// master knows for that replica (its heartbeat), never at the caller's: a
// request naming other addresses is refused and the line does not move; the
// same request at the recorded addresses moves it.
func TestFailbackPublishesOnlyTheMastersRecordedTargetAddresses(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 2})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{result: PromotionProbeResult{
		AckProfile: "sync_all",
		Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 1}},
	}})
	seedRF2Placement(t, h)
	ingestRF2Observation(t, h, true, true) // r2 heartbeats at 127.0.0.1:19201 / :19202
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf2")
	if line.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", line)
	}
	req := func(data, ctrl string) authority.FailbackRuntimeRequest {
		return authority.FailbackRuntimeRequest{VolumeID: "vol-rf2", ReplicaID: "r2", TargetDataAddr: data, TargetCtrlAddr: ctrl,
			ExpectedCurrentReplicaID: line.ReplicaID, ExpectedCurrentEpoch: line.Epoch,
			AckEligible: true, FrontendFencedBeforeFailback: true, DurableFrontierCovered: true, NoCrossVolumeIdentityChange: true}
	}
	for name, r := range map[string]authority.FailbackRuntimeRequest{
		"wrong data address":    req("127.0.0.1:29201", "127.0.0.1:19202"),
		"wrong control address": req("127.0.0.1:19201", "127.0.0.1:29202"),
	} {
		if _, err := h.FailbackAuthorityRuntime().ExecuteFailback(context.Background(), r); err == nil {
			t.Fatalf("%s: the failover published the caller's addresses", name)
		}
		if after, _ := h.Publisher().VolumeAuthorityLine("vol-rf2"); after != line {
			t.Fatalf("%s: line=%+v; want %+v unchanged", name, after, line)
		}
	}
	if _, err := h.FailbackAuthorityRuntime().ExecuteFailback(context.Background(), req("127.0.0.1:19201", "127.0.0.1:19202")); err != nil {
		t.Fatalf("at the recorded addresses: %v", err)
	}
	after, _ := h.Publisher().VolumeAuthorityLine("vol-rf2")
	if after.ReplicaID != "r2" || after.DataAddr != "127.0.0.1:19201" || after.CtrlAddr != "127.0.0.1:19202" {
		t.Fatalf("line=%+v; want r2 at its recorded addresses", after)
	}
}
