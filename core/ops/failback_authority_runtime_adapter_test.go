package ops

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

func TestFailbackExecutorUsesAuthorityRuntimeAdapter(t *testing.T) {
	publisher := seededFailbackPublisher(t)
	target := failbackExecutorExecutableTargetFixture()
	target.Spec.ReplicaID = "r1"
	target.Spec.ExpectedCurrentEpoch = 2
	client := &fakeFailbackExecutorClient{targets: []SwBlockReplicaFailbackObject{target}}

	result, err := (FailbackExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                NewAuthorityFailbackRuntime(publisher),
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		Now:                    func() time.Time { return time.Date(2026, 6, 26, 15, 0, 0, 0, time.UTC) },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FailbackAttempts != 1 ||
		result.StatusWriteCount != 1 ||
		!result.AuthorityMutationAllowed ||
		result.FrontendPublicationAllowed ||
		result.StorageMutationAllowed {
		t.Fatalf("result=%+v", result)
	}
	line, ok := publisher.VolumeAuthorityLine(target.Spec.VolumeID)
	if !ok {
		t.Fatalf("missing authority line for %s", target.Spec.VolumeID)
	}
	if line.ReplicaID != target.Spec.ReplicaID ||
		line.Epoch != 3 ||
		line.DataAddr != target.Spec.TargetDataAddr ||
		line.CtrlAddr != target.Spec.TargetCtrlAddr {
		t.Fatalf("authority line=%+v", line)
	}
	status := client.writes[0].status
	if status.State != FailbackStateFailedBack ||
		status.ReasonCode != AuthorityExecutorFailbackReasonCompleted ||
		!status.FailbackStarted ||
		!status.AuthorityEpochAdvanced ||
		!status.SinglePrimaryAfterFailback ||
		!status.PublishTargetSwappedAfterFailback ||
		status.FailbackMutationAllowed {
		t.Fatalf("status=%+v", status)
	}
	if !failbackExecutorStringSliceContains(status.EvidenceRefs, "authority_failback_reassign_minted") {
		t.Fatalf("evidenceRefs=%+v", status.EvidenceRefs)
	}
}

func TestFailbackAuthorityRuntimeAdapterRejectsStaleExpectedCurrent(t *testing.T) {
	publisher := seededFailbackPublisher(t)
	target := failbackExecutorExecutableTargetFixture()
	target.Spec.ReplicaID = "r1"
	target.Spec.ExpectedCurrentEpoch = 99
	client := &fakeFailbackExecutorClient{targets: []SwBlockReplicaFailbackObject{target}}

	result, err := (FailbackExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                NewAuthorityFailbackRuntime(publisher),
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
	}).Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "stale expected current line") {
		t.Fatalf("err=%v", err)
	}
	if result.FailbackAttempts != 1 || result.StatusWriteCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	status := client.writes[0].status
	if status.State != FailbackStateBlocked ||
		status.ReasonCode != AuthorityExecutorFailbackReasonRuntimeFailed ||
		status.FailbackStarted ||
		status.AuthorityEpochAdvanced ||
		status.SinglePrimaryAfterFailback ||
		status.PublishTargetSwappedAfterFailback {
		t.Fatalf("status=%+v", status)
	}
	line, ok := publisher.VolumeAuthorityLine(target.Spec.VolumeID)
	if !ok {
		t.Fatalf("missing authority line for %s", target.Spec.VolumeID)
	}
	if line.ReplicaID != "r2" || line.Epoch != 2 {
		t.Fatalf("stale expected-current failure changed authority line: %+v", line)
	}
}

func seededFailbackPublisher(t *testing.T) *authority.Publisher {
	t.Helper()
	directive := authority.NewStaticDirective([]authority.AssignmentAsk{
		{
			VolumeID:  "pvc-demo",
			ReplicaID: "r1",
			DataAddr:  "data-old-r1",
			CtrlAddr:  "ctrl-old-r1",
			Intent:    authority.IntentBind,
		},
		{
			VolumeID:  "pvc-demo",
			ReplicaID: "r2",
			DataAddr:  "data-r2",
			CtrlAddr:  "ctrl-r2",
			Intent:    authority.IntentReassign,
		},
	})
	publisher := authority.NewPublisher(directive)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- publisher.Run(ctx)
	}()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		line, ok := publisher.VolumeAuthorityLine("pvc-demo")
		if ok && line.ReplicaID == "r2" && line.Epoch == 2 {
			cancel()
			if err := <-done; err != context.Canceled {
				t.Fatalf("publisher run err=%v", err)
			}
			return publisher
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("publisher did not seed r2@2")
		case <-ticker.C:
		}
	}
}
