package authority

import (
	"context"
	"strings"
	"testing"
)

func TestFailbackAuthorityRuntime_ReassignsThroughPublisher(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID:  "v1",
		ReplicaID: "r2",
		DataAddr:  "data-r2",
		CtrlAddr:  "ctrl-r2",
		Intent:    IntentBind,
	}); err != nil {
		t.Fatalf("bind current: %v", err)
	}
	result, err := (FailbackAuthorityRuntime{Publisher: pub}).ExecuteFailback(context.Background(), validAuthorityFailbackRequest())
	if err != nil {
		t.Fatalf("execute failback: %v", err)
	}
	if !result.FailbackStarted ||
		!result.AuthorityEpochAdvanced ||
		!result.SinglePrimaryAfterFailback ||
		!result.PublishTargetSwappedAfterFailback ||
		!result.NoStorageMutation ||
		!result.NoCrossVolumeIdentityChange {
		t.Fatalf("result=%+v", result)
	}
	line, ok := pub.VolumeAuthorityLine("v1")
	if !ok {
		t.Fatal("missing authority line")
	}
	if line.ReplicaID != "r1" || line.Epoch != 2 || line.EndpointVersion != 1 ||
		line.DataAddr != "data-r1" || line.CtrlAddr != "ctrl-r1" {
		t.Fatalf("line=%+v", line)
	}
}

func TestFailbackAuthorityRuntime_RejectsStaleExpectedCurrent(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r2", DataAddr: "data-r2", CtrlAddr: "ctrl-r2", Intent: IntentBind}); err != nil {
		t.Fatalf("bind current: %v", err)
	}
	req := validAuthorityFailbackRequest()
	req.ExpectedCurrentEpoch = 99
	_, err := (FailbackAuthorityRuntime{Publisher: pub}).ExecuteFailback(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "stale expected current line") {
		t.Fatalf("err=%v", err)
	}
	line, _ := pub.VolumeAuthorityLine("v1")
	if line.ReplicaID != "r2" || line.Epoch != 1 {
		t.Fatalf("stale request must not move authority: %+v", line)
	}
}

func TestFailbackAuthorityRuntime_RequiresTerminalPreconditions(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	req := validAuthorityFailbackRequest()
	req.AckEligible = false
	_, err := (FailbackAuthorityRuntime{Publisher: pub}).ExecuteFailback(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "terminal failback preconditions") {
		t.Fatalf("err=%v", err)
	}
	if _, ok := pub.VolumeAuthorityLine("v1"); ok {
		t.Fatal("precondition failure must not mint authority")
	}
}

func validAuthorityFailbackRequest() FailbackRuntimeRequest {
	return FailbackRuntimeRequest{
		VolumeID:                     "v1",
		ReplicaID:                    "r1",
		TargetDataAddr:               "data-r1",
		TargetCtrlAddr:               "ctrl-r1",
		ExpectedCurrentReplicaID:     "r2",
		ExpectedCurrentEpoch:         1,
		AckEligible:                  true,
		FrontendFencedBeforeFailback: true,
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
		EvidenceRefs:                 []string{"failback-target"},
	}
}
