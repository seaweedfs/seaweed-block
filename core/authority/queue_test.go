package authority

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAssignmentQueue_ReplacesLatestPerVolume(t *testing.T) {
	q := newAssignmentQueue()
	q.enqueue(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind,
	})
	q.enqueue(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2", DataAddr: "d2", CtrlAddr: "c2", Intent: IntentReassign,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ask, err := q.next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if ask.ReplicaID != "r2" || ask.Intent != IntentReassign {
		t.Fatalf("queue must keep latest ask for volume, got %+v", ask)
	}
}

func TestAssignmentQueue_DiscardRemovesPendingVolume(t *testing.T) {
	q := newAssignmentQueue()
	q.enqueue(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind,
	})
	q.discard("v1")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := q.next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline after discard, got %v", err)
	}
}
