package authority

import (
	"context"
	"testing"
	"time"
)

// A lost local image can rejoin on the same host only through a new epoch
// with the same lease barrier and durable loss record as a cross-host move.
func TestWorkspaceSameHostForcedMoveRecordsLostRange(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "running", true: "reloaded"}[restart], func(t *testing.T) {
			disk, _ := newTempStore(t)
			p, now, barrier := forcedLostRangeFixture(t, disk, restart)
			req := forcedLostRangeReq
			req.TargetHost = req.ExpectedHost
			*now = barrier.Add(-time.Nanosecond)
			if p.PreviewWorkspaceForcedMove(req).Admitted {
				t.Fatal("same-host preview bypassed the live lease barrier")
			}
			if _, err := p.MoveWorkspace(context.Background(), req, forcedLostRangeHead); err == nil {
				t.Fatal("same-host forced move bypassed the live lease barrier")
			}
			if len(p.WorkspaceLostRanges("v")) != 0 || len(p.WorkspaceState("v").Moves) != 0 {
				t.Fatal("refused move wrote a loss record or reservation")
			}
			*now = barrier.Add(time.Nanosecond)
			if preview := p.PreviewWorkspaceForcedMove(req); !preview.Admitted {
				t.Fatalf("same-host recovery preview refused after barrier: %s", preview.Reason)
			}
			grant, err := p.MoveWorkspace(context.Background(), req, forcedLostRangeHead)
			if err != nil {
				t.Fatal(err)
			}
			if !grant.Issued || grant.NewEpoch != 2 || grant.OldHost != grant.NewHost {
				t.Fatalf("wrong same-host successor: %+v", grant)
			}
			want := WorkspaceLostRange{RequestID: req.RequestID, NewEpoch: 2, OldReplicaID: "old", OldEpoch: 1, LostAfterLSN: 42, LostUntilNS: barrier.UnixNano()}
			for _, checked := range []*Publisher{p, NewPublisher(nil, WithStore(disk))} {
				if got := checked.WorkspaceLostRanges("v"); len(got) != 1 || got[0] != want {
					t.Fatalf("same-host recovery lost the durable loss report: %+v", got)
				}
			}
			if replay, err := p.MoveWorkspace(context.Background(), req, WorkspaceHead{}); err != nil || replay != grant {
				t.Fatalf("same-host replay changed grant: %+v %v", replay, err)
			}
		})
	}
}
