package authority

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// An issuing write that lands in the store but reports an error (the
// directory sync after the rename) leaves the move's outcome unknown:
// MoveWorkspace fails with ErrWorkspaceOutcomeUncertain while memory shows
// it unissued, a retry before a reload fails the same way, and a reload finds
// it issued and replays it. Ordinary refusals (admission before the barrier,
// a failed lost-range write before the issue) are not marked uncertain.
func TestWorkspaceUncertainIssueWriteIsTyped(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	p, now, barrier := forcedLostRangeFixture(t, disk, false)
	req := forcedLostRangeReq
	req.RequestID = "uncertain-1"

	*now = barrier.Add(-time.Nanosecond)
	if _, err := p.MoveWorkspace(ctx, req, forcedLostRangeHead); err == nil || errors.Is(err, ErrWorkspaceOutcomeUncertain) {
		t.Fatalf("admission refusal before the barrier: %v; want a plain refusal", err)
	}
	*now = barrier.Add(time.Second)
	testLostRangeFaultHook = func(string) error { return errInjected }
	t.Cleanup(func() { testLostRangeFaultHook = nil })
	if _, err := p.MoveWorkspace(ctx, req, forcedLostRangeHead); !errors.Is(err, errInjected) || errors.Is(err, ErrWorkspaceOutcomeUncertain) {
		t.Fatalf("lost-range write failure before the issue: %v; want a plain refusal", err)
	}
	testLostRangeFaultHook = nil

	faults := 0
	SetPutAfterRenameFaultForTest(func(r DurableRecord) error {
		if r.Workspace != nil {
			for _, m := range r.Workspace.Moves {
				if m.RequestID == req.RequestID && m.Issued {
					faults++
					return errInjected
				}
			}
		}
		return nil
	})
	t.Cleanup(func() { SetPutAfterRenameFaultForTest(nil) })
	if _, err := p.MoveWorkspace(ctx, req, forcedLostRangeHead); !errors.Is(err, ErrWorkspaceOutcomeUncertain) {
		t.Fatalf("issuing write landed then failed: %v; want ErrWorkspaceOutcomeUncertain", err)
	}
	SetPutAfterRenameFaultForTest(nil)
	if faults != 1 || p.IssuedWorkspaceMove(req) {
		t.Fatalf("fixture: faults=%d issued in memory=%v; want one fault and memory unissued", faults, p.IssuedWorkspaceMove(req))
	}
	if _, err := p.MoveWorkspace(ctx, req, forcedLostRangeHead); !errors.Is(err, ErrWorkspaceOutcomeUncertain) {
		t.Fatalf("retry before a reload: %v; want ErrWorkspaceOutcomeUncertain", err)
	}
	reloaded := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return *now }))
	if !reloaded.IssuedWorkspaceMove(req) {
		t.Fatal("the reloaded store does not hold the issued move")
	}
	r, err := reloaded.MoveWorkspace(ctx, req, WorkspaceHead{})
	if err != nil || !r.Issued || r.RequestID != req.RequestID {
		t.Fatalf("replay after reload: %+v %v", r, err)
	}
	if got, err := reloaded.WorkspaceLostRangeReadouts("v", req.RequestID); err != nil || len(got) != 1 || got[0].LostAfterLSN != 42 {
		t.Fatalf("readout after reload: %+v %v", got, err)
	}
}

func TestWorkspaceUncertaintyPrecedesRequestRefusal(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	p, _, _ := forcedLostRangeFixture(t, disk, false)
	t.Cleanup(p.CloseWorkspaceExtenders)
	bad := forcedLostRangeReq
	bad.TargetReplicaID = "other-slot"
	if _, err := p.MoveWorkspace(ctx, bad, forcedLostRangeHead); err == nil || errors.Is(err, ErrWorkspaceOutcomeUncertain) {
		t.Fatalf("healthy refusal changed: %v", err)
	}
	faults := 0
	SetPutAfterRenameFaultForTest(func(r DurableRecord) error {
		if r.Workspace != nil && r.Workspace.Lease != nil && r.Workspace.Lease.Revoking {
			faults++
			return errInjected
		}
		return nil
	})
	t.Cleanup(func() { SetPutAfterRenameFaultForTest(nil) })
	if err := p.BeginWorkspaceRevocation(ctx, "v", "old", 1); !errors.Is(err, errInjected) {
		t.Fatalf("failed persistence not witnessed: %v", err)
	}
	SetPutAfterRenameFaultForTest(nil)
	if faults != 1 || p.WorkspaceState("v").Lease.Revoking {
		t.Fatal("fixture did not retain pre-write memory")
	}
	records, err := disk.Load()
	if err != nil {
		t.Fatal(err)
	}
	landed := false
	for _, r := range records {
		if r.Workspace != nil && r.Workspace.Lease != nil && r.Workspace.Lease.Revoking {
			landed = true
		}
	}
	if !landed {
		t.Fatal("faulted write did not land on disk")
	}
	before := p.WorkspaceState("v")
	for _, req := range []WorkspaceMoveRequest{bad, forcedLostRangeReq} {
		if _, err := p.MoveWorkspace(ctx, req, forcedLostRangeHead); !errors.Is(err, ErrWorkspaceOutcomeUncertain) {
			t.Errorf("uncertain volume answered a definite admission refusal: %v", err)
		}
	}
	for _, dependencyErr := range []error{context.Canceled, context.DeadlineExceeded} {
		if ctx.Err() != nil {
			t.Fatal("request parent unexpectedly ended")
		}
		if err := p.WorkspaceMoveOutcomeError("v", dependencyErr); !errors.Is(err, ErrWorkspaceOutcomeUncertain) {
			t.Errorf("live caller dependency error bypassed UNKNOWN: %v", err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := p.MoveWorkspace(canceled, bad, forcedLostRangeHead); err != context.Canceled {
		t.Errorf("caller cancellation changed: %v", err)
	}
	other := bad
	other.VolumeID = "other"
	if _, err := p.MoveWorkspace(ctx, other, forcedLostRangeHead); err == nil || errors.Is(err, ErrWorkspaceOutcomeUncertain) {
		t.Errorf("uncertainty leaked across volumes: %v", err)
	}
	if !reflect.DeepEqual(before, p.WorkspaceState("v")) {
		t.Fatal("refused requests changed authority memory")
	}
}
