package layout

import (
	"errors"
	"testing"
)

// A snapshot is named by one commit: its root and its position. A commit
// repeating the name at the same root but another position is not that
// commit's replay; it is refused, and HEAD keeps the recorded position.
func TestCommit_SnapshotNameAtAnotherPositionIsAnotherCommit(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("new"))}
	next, _, err := snapHead("s1").Commit(me, "s1", rn, 50, "hourly", SnapshotMeta{}, StateLive)
	if err != nil || next.Snapshots["hourly"].Position != 50 {
		t.Fatalf("Commit: %+v %v", next.Snapshots["hourly"], err)
	}
	if _, changed, err := next.Commit(me, "s1", rn, 50, "hourly", SnapshotMeta{}, StateLive); err != nil || changed {
		t.Fatalf("same name, root and position: %t %v; want a replay", changed, err)
	}
	reopened := next.clone()
	reopened.Activity = Activity{Kind: ActSnap, Session: "s2"}
	if _, _, err := reopened.Commit(me, "s2", rn, 60, "hourly", SnapshotMeta{}, StateLive); !errors.Is(err, ErrSnapshotExists) {
		t.Fatalf("same name and root at position 60: %v; want ErrSnapshotExists", err)
	}
	if _, changed, err := next.Commit(me, "s1", rn, 50, "hourly", SnapshotMeta{}, StateLive); err != nil || changed {
		t.Fatalf("after the refusal: %t %v", changed, err)
	}
}
