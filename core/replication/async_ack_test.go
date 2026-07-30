package replication

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestWaitForPeerAcks_BestEffortPreservesTerminalQueueError(t *testing.T) {
	results := make(chan peerWorkResult, 2)
	results <- peerWorkResult{peerID: "ordinary", err: errors.New("transport unavailable")}
	results <- peerWorkResult{peerID: "terminal", err: ErrPeerQueueSaturated}

	err := waitForPeerAcks(
		context.Background(),
		DurabilityBestEffort,
		3,
		2,
		results,
		"write",
	)
	if !errors.Is(err, ErrPeerQueueSaturated) {
		t.Fatalf("error=%v want ErrPeerQueueSaturated", err)
	}
}

func TestWaitForPeerAcks_SyncAllReportsSuccessfulAcknowledgements(t *testing.T) {
	results := make(chan peerWorkResult, 2)
	results <- peerWorkResult{peerID: "r1", eligible: true}
	results <- peerWorkResult{peerID: "r2", err: errors.New("write failed")}

	err := waitForPeerAcks(
		context.Background(),
		DurabilitySyncAll,
		3,
		2,
		results,
		"write",
	)
	if !errors.Is(err, ErrDurabilityBarrierFailed) {
		t.Fatalf("error=%v want ErrDurabilityBarrierFailed", err)
	}
	if !strings.Contains(err.Error(), "acknowledgements 2/3") {
		t.Fatalf("error=%q want actual successful acknowledgement count 2/3", err)
	}
}

func TestWaitForSyncAcks_SyncAllReportsSuccessfulAcknowledgements(t *testing.T) {
	local := make(chan localSyncResult, 1)
	local <- localSyncResult{}
	results := make(chan peerWorkResult, 2)
	results <- peerWorkResult{peerID: "r1"}
	results <- peerWorkResult{peerID: "r2", err: errors.New("barrier failed")}

	err := waitForSyncAcks(
		context.Background(),
		DurabilitySyncAll,
		7,
		2,
		local,
		results,
	)
	if !errors.Is(err, ErrDurabilityBarrierFailed) {
		t.Fatalf("error=%v want ErrDurabilityBarrierFailed", err)
	}
	if !strings.Contains(err.Error(), "acknowledgements 2/3") {
		t.Fatalf("error=%q want actual successful acknowledgement count 2/3", err)
	}
}
