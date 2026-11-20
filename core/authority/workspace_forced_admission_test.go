package authority

import (
	"context"
	"strings"
	"testing"
	"time"
)

// WorkspaceForcedMoveAdmitted gives the move's own answer: refused before
// the lease expiry plus skew, admitted at it; a journaled request id is left
// to MoveWorkspace (its replay uses no new HEAD).
func TestWorkspaceForcedMoveAdmittedMatchesMoveAdmission(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	now := time.Unix(1000, 0)
	p, issued := cleanMoveToGrantee(t, disk, &now)
	barrier := issued.Add(grantTTL + grantTTL/2 + p.workspaceSkew(p.WorkspaceState("v").Lease))
	now = barrier.Add(-time.Nanosecond)
	if err := p.WorkspaceForcedMoveAdmitted(ctx, "v", "forced", "r", 2); err == nil || !strings.Contains(err.Error(), "expiry plus clock-skew margin has not elapsed") {
		t.Fatalf("admitted before expiry+skew: %v", err)
	}
	if err := p.WorkspaceForcedMoveAdmitted(ctx, "v", "forced", "r", 1); err == nil || !strings.Contains(err.Error(), "missing or changed authority/lease") {
		t.Fatalf("admitted a stale epoch: %v", err)
	}
	if err := p.WorkspaceForcedMoveAdmitted(ctx, "v", "clean", "r", 1); err != nil {
		t.Fatalf("a journaled request id must be left to MoveWorkspace: %v", err)
	}
	now = barrier
	if err := p.WorkspaceForcedMoveAdmitted(ctx, "v", "forced", "r", 2); err != nil {
		t.Fatal("refused after expiry+skew:", err)
	}
	if _, err := p.MoveWorkspace(ctx, forcedFromGrantee("forced"), predecessorHead); err != nil {
		t.Fatal(err)
	}
}
