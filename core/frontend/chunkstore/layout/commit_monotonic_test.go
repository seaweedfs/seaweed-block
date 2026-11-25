package layout

import (
	"errors"
	"testing"
)

// HEAD's position only rises: a commit at 10, then a commit at 9 is
// refused (ErrPositionBehind) and HEAD is unchanged; a commit at 10 again
// (an unchanged image) is legal. A HEAD naming a snapshot past its own
// position is malformed.
func TestCommit_PositionIsMonotonic(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("new"))}
	base := snapHead("s1").Position // positions below are relative to the fixture's
	at10, _, err := snapHead("s1").Commit(me, "s1", rn, base+10, "", SnapshotMeta{}, StateLive)
	if err != nil || at10.Position != base+10 {
		t.Fatalf("commit at 10: %+v %v", at10, err)
	}
	reopened := at10.clone()
	reopened.Activity = Activity{Kind: ActSnap, Session: "s2"}
	other := RootName{Hash: Sum([]byte("older"))}
	if got, _, err := reopened.Commit(me, "s2", other, base+9, "", SnapshotMeta{}, StateLive); !errors.Is(err, ErrPositionBehind) {
		t.Fatalf("commit at 9 over HEAD at 10: %+v %v; want ErrPositionBehind", got, err)
	}
	if reopened.Position != base+10 || *reopened.Current != rn {
		t.Fatalf("HEAD changed by the refused commit: %+v", reopened)
	}
	if got, changed, err := reopened.Commit(me, "s2", rn, base+10, "same", SnapshotMeta{}, StateLive); err != nil || !changed || got.Snapshots["same"].Position != base+10 {
		t.Fatalf("a named snapshot of the unchanged image at 10: %+v %t %v", got, changed, err)
	}

	bad := at10.clone()
	bad.Snapshots["future"] = Snapshot{Root: rn, Position: base + 11}
	if _, err := bad.Encode(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("HEAD at 10 naming a snapshot at 11: %v; want ErrMalformed", err)
	}
}
