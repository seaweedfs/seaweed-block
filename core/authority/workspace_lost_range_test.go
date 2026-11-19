package authority

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func forcedLostRangeFixture(t *testing.T, store AuthorityStore, restart bool) (p *Publisher, now *time.Time, barrier time.Time) {
	t.Helper()
	ctx := context.Background()
	at := time.Unix(1000, 0)
	now = &at
	clock := func() time.Time { return *now }
	p = NewPublisher(nil, WithStore(store), WithWorkspaceLeaseConfig(time.Second, clock))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	if restart {
		*now = now.Add(time.Second)
		p = NewPublisher(nil, WithStore(store), WithWorkspaceLeaseConfig(time.Second, clock))
	}
	lease := p.WorkspaceState("v").Lease
	if restart && !p.leaseDeadlineLockedForTest("v").After(time.Unix(0, lease.ExpiresAtNS)) {
		t.Fatal("fixture: the restart deadline is not the later barrier term")
	}
	barrier = time.Unix(0, lease.ExpiresAtNS)
	if d := p.leaseDeadlineLockedForTest("v"); d.After(barrier) {
		barrier = d
	}
	return p, now, barrier.Add(p.workspaceSkew(lease))
}

var forcedLostRangeReq = WorkspaceMoveRequest{RequestID: "f", VolumeID: "v", ExpectedReplicaID: "old", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1,
	TargetReplicaID: "old", TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
var forcedLostRangeHead = WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42}

// A forced move records the abandoned range beside its record: the kept HEAD
// frontier and the forced barrier (lease horizon, or after a restart the
// later restart deadline, plus skew) as the latest master time an old-holder
// write can have been acknowledged. A reload returns it; the move record's
// own encoding is unchanged, so older binaries still verify and load it.
func TestWorkspaceForcedMoveRecordsTheLostRange(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "running", true: "after restart"}[restart], func(t *testing.T) {
			disk, _ := newTempStore(t)
			p, now, barrier := forcedLostRangeFixture(t, disk, restart)
			*now = barrier.Add(-time.Nanosecond)
			if _, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead); err == nil {
				t.Fatal("forced move admitted before its barrier")
			}
			if got := p.WorkspaceLostRanges("v"); len(got) != 0 {
				t.Fatalf("a refused move recorded a lost range: %+v", got)
			}
			*now = barrier.Add(5 * time.Second) // the bound is the barrier, not the time of the move
			r, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead)
			if err != nil {
				t.Fatal(err)
			}
			want := WorkspaceLostRange{RequestID: "f", NewEpoch: r.NewEpoch, OldReplicaID: "old", OldEpoch: 1, LostAfterLSN: 42, LostUntilNS: barrier.UnixNano()}
			if got := p.WorkspaceLostRanges("v"); len(got) != 1 || got[0] != want {
				t.Fatalf("lost range %+v; want [%+v]", got, want)
			}
			reloaded := NewPublisher(nil, WithStore(disk))
			if got := reloaded.WorkspaceLostRanges("v"); len(got) != 1 || got[0] != want {
				t.Fatalf("reloaded lost range %+v; want [%+v]", got, want)
			}
			raw, err := json.Marshal(reloaded.WorkspaceState("v").Moves[0])
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "Lost") {
				t.Fatalf("the move record's encoding changed: %s", raw)
			}
		})
	}
}

// A clean move abandons nothing and records no lost range.
func TestWorkspaceCleanMoveRecordsNoLostRange(t *testing.T) {
	ctx := context.Background()
	disk, dir := newTempStore(t)
	p := NewPublisher(nil, WithStore(disk))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.BeginWorkspaceRevocation(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "old", Epoch: 1, HeadGen: 7}); err != nil {
		t.Fatal(err)
	}
	req := WorkspaceMoveRequest{RequestID: "c", VolumeID: "v", ExpectedReplicaID: "old", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1,
		TargetReplicaID: "old", TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "clean"}
	if _, err := p.MoveWorkspace(ctx, req, WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42, Detached: true}); err != nil {
		t.Fatal(err)
	}
	if got := NewPublisher(nil, WithStore(disk)).WorkspaceLostRanges("v"); len(got) != 0 {
		t.Fatalf("clean move recorded a lost range: %+v", got)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "*"+lostRangeFileExtension)); len(m) != 0 {
		t.Fatalf("clean move wrote a lost range file: %v", m)
	}
}

// lostRangeFaultStore is the file store with injectable failures.
type lostRangeFaultStore struct {
	*FileAuthorityStore
	failPut, failLostRange bool
}

var errInjected = errors.New("injected store failure")

func (s *lostRangeFaultStore) Put(r DurableRecord) error {
	if s.failPut && r.Workspace != nil && len(r.Workspace.Moves) > 0 {
		return errInjected
	}
	return s.FileAuthorityStore.Put(r)
}

func (s *lostRangeFaultStore) PutWorkspaceLostRanges(v string, rs []WorkspaceLostRange) error {
	if s.failLostRange {
		return errInjected
	}
	return s.FileAuthorityStore.PutWorkspaceLostRanges(v, rs)
}

// The lost range is written before the move record: a failed range write
// refuses the move (no epoch issued); a range whose move record then fails
// is an orphan, on disk but never reported.
func TestWorkspaceForcedMoveLostRangeOrdering(t *testing.T) {
	t.Run("range write fails", func(t *testing.T) {
		disk, _ := newTempStore(t)
		store := &lostRangeFaultStore{FileAuthorityStore: disk, failLostRange: true}
		p, now, barrier := forcedLostRangeFixture(t, store, false)
		*now = barrier.Add(time.Second)
		if _, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead); !errors.Is(err, errInjected) {
			t.Fatalf("move with a failed range write: %v; want refused", err)
		}
		if line, _ := NewPublisher(nil, WithStore(disk)).VolumeAuthorityLine("v"); line.Epoch != 1 {
			t.Fatalf("epoch issued without its lost range: %+v", line)
		}
	})
	t.Run("move record fails", func(t *testing.T) {
		disk, dir := newTempStore(t)
		store := &lostRangeFaultStore{FileAuthorityStore: disk, failPut: true}
		p, now, barrier := forcedLostRangeFixture(t, store, false)
		*now = barrier.Add(time.Second)
		if _, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead); err == nil {
			t.Fatal("move issued although its record failed")
		}
		raw, err := os.ReadFile(filepath.Join(dir, "v"+lostRangeFileExtension))
		if err != nil || !strings.Contains(string(raw), `"RequestID":"f"`) {
			t.Fatalf("fixture: no orphan range on disk: %v %s", err, raw)
		}
		if got := p.WorkspaceLostRanges("v"); len(got) != 0 {
			t.Fatalf("orphan reported: %+v", got)
		}
		if got := NewPublisher(nil, WithStore(disk)).WorkspaceLostRanges("v"); len(got) != 0 {
			t.Fatalf("orphan reported after reload: %+v", got)
		}
	})
}
