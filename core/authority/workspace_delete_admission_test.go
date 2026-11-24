package authority

import (
	"context"
	"strings"
	"testing"
	"time"
)

// INV-WS-DELETE-NO-LIVE-HOLDER-001 at the admission itself
// (RecordWorkspaceDeletion), on a line at epoch 2 whose ACK is for epoch 1: a
// HEAD below the line is never started (admitted); an ACK of an older epoch
// does not drain the line, so a HEAD claimed at epoch 2 is refused; a HEAD at
// a higher epoch, another holder at the line's epoch, an unread HEAD and a
// pending move are refused; a HEAD past the fence (deleting) is admitted.
func TestDeleteAdmissionVerdicts(t *testing.T) {
	now := time.Unix(1000, 0)
	store, _ := newTempStore(t)
	p, _ := cleanMoveToGrantee(t, store, &now)
	const owner = "new-node/r"
	persisted := 0
	admit := func(head *WorkspaceHead) error {
		return p.RecordWorkspaceDeletion(context.Background(), "v", false, head, owner, func() error { persisted++; return nil })
	}
	refused := func(what string, head *WorkspaceHead) {
		t.Helper()
		before := persisted
		err := admit(head)
		if err == nil || !strings.Contains(err.Error(), "live authority requires clean revocation ACK") || persisted != before {
			t.Fatalf("%s: %v (persisted %d -> %d); want refused before the reservation", what, err, before, persisted)
		}
	}
	refused("an ACK of epoch 1 with HEAD claimed at epoch 2", &WorkspaceHead{Gen: 8, Epoch: 2, Holder: owner})
	refused("HEAD at a higher epoch", &WorkspaceHead{Gen: 9, Epoch: 3, Holder: owner})
	refused("another holder at the line's epoch", &WorkspaceHead{Gen: 8, Epoch: 2, Holder: "third-node/r"})
	refused("HEAD not read", nil)
	head := predecessorHead // epoch 1: the grantee never claimed epoch 2
	if err := admit(&head); err != nil || persisted != 1 {
		t.Fatalf("HEAD below the line: %v (persisted %d); want admitted", err, persisted)
	}
	if err := admit(&WorkspaceHead{Gen: 9, Deleting: true}); err != nil || persisted != 2 {
		t.Fatalf("HEAD deleting (a rerun): %v (persisted %d); want admitted", err, persisted)
	}
	p.mu.Lock()
	s := cloneWorkspace(p.workspace["v"])
	s.Moves = append(s.Moves, WorkspaceMoveRecord{RequestID: "pending", VolumeID: "v", Kind: "clean"})
	p.workspace["v"] = s
	p.mu.Unlock()
	if err := admit(&head); err == nil || !strings.Contains(err.Error(), "move pending") {
		t.Fatalf("a pending move: %v; want refused", err)
	}
}
