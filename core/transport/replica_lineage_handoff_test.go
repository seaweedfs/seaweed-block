package transport

import "testing"

func TestReplicaListener_PostRecoveryLiveLineageRequiresNewerSession(t *testing.T) {
	r := &ReplicaListener{}
	recoveryLineage := RecoveryLineage{
		SessionID:       5,
		Epoch:           1,
		EndpointVersion: 1,
		TargetLSN:       6001,
	}
	if !r.acceptMutationLineage(recoveryLineage) {
		t.Fatal("recovery lineage should establish active lineage")
	}

	oldLiveLineage := RecoveryLineage{
		SessionID:       1,
		Epoch:           1,
		EndpointVersion: 1,
		TargetLSN:       liveShipTargetLSNSentinel,
	}
	if r.acceptMutationLineage(oldLiveLineage) {
		t.Fatal("old pre-recovery live session must not be accepted after recovery lineage")
	}

	newLiveLineage := RecoveryLineage{
		SessionID:       6,
		Epoch:           1,
		EndpointVersion: 1,
		TargetLSN:       liveShipTargetLSNSentinel,
	}
	if !r.acceptMutationLineage(newLiveLineage) {
		t.Fatal("post-recovery live session with newer sessionID should be accepted")
	}
}
