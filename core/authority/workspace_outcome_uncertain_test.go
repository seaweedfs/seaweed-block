package authority

import (
	"context"
	"errors"
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
