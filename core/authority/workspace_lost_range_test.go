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
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "old", Epoch: 1, HeadGen: 7, Holder: "old-node/old", RemoteLSN: 42}, WorkspaceHead{Detached: true, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42}); err != nil {
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

// issueFailStore fails the authority write that would issue a move, leaving
// its reservation pending on disk (a crash between reservation and issue).
type issueFailStore struct {
	*FileAuthorityStore
	failIssue bool
}

func (s *issueFailStore) Put(r DurableRecord) error {
	if s.failIssue && r.Workspace != nil {
		for _, m := range r.Workspace.Moves {
			if m.Issued {
				return errInjected
			}
		}
	}
	return s.FileAuthorityStore.Put(r)
}

// A pending forced reservation resumes only with the exact range recorded
// before it: after a reload the retry issues it with that range (not one
// recomputed at the retry); without it the retry is refused, and a new
// request records a fresh range.
func TestWorkspaceForcedReservationResumeNeedsItsRange(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "range kept", true: "range missing"}[missing], func(t *testing.T) {
			disk, dir := newTempStore(t)
			store := &issueFailStore{FileAuthorityStore: disk, failIssue: true}
			p, now, barrier := forcedLostRangeFixture(t, store, false)
			*now = barrier.Add(time.Second)
			if _, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead); err == nil {
				t.Fatal("fixture: the issue write did not fail")
			}
			st := NewPublisher(nil, WithStore(disk)).WorkspaceState("v")
			if st == nil || len(st.Moves) != 1 || st.Moves[0].Issued {
				t.Fatalf("fixture: no pending reservation on disk: %+v", st)
			}
			want := WorkspaceLostRange{RequestID: "f", NewEpoch: st.Moves[0].NewEpoch, OldReplicaID: "old", OldEpoch: 1, LostAfterLSN: 42, LostUntilNS: barrier.UnixNano()}
			if missing {
				if err := os.Remove(filepath.Join(dir, "v"+lostRangeFileExtension)); err != nil {
					t.Fatal(err)
				}
			}
			later := barrier.Add(time.Hour) // a recomputed bound would differ
			q := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return later }))
			later = later.Add(time.Minute) // past the reload's own restart deadline
			r, err := q.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead)
			if missing {
				if err == nil || !strings.Contains(err.Error(), "no recorded lost range") {
					t.Fatalf("resume without its range: %v; want refused", err)
				}
				if line, _ := q.VolumeAuthorityLine("v"); line.Epoch != 1 {
					t.Fatalf("epoch issued without a range: %+v", line)
				}
				fresh := forcedLostRangeReq
				fresh.RequestID = "f2"
				r2, err := q.MoveWorkspace(context.Background(), fresh, forcedLostRangeHead)
				if err != nil {
					t.Fatalf("a new request after the refused resume: %v", err)
				}
				if got := q.WorkspaceLostRanges("v"); len(got) != 1 || got[0].RequestID != "f2" || got[0].NewEpoch != r2.NewEpoch {
					t.Fatalf("new request range %+v", got)
				}
				return
			}
			if err != nil || !r.Issued || r.NewEpoch != want.NewEpoch {
				t.Fatalf("resume: %+v %v", r, err)
			}
			if got := q.WorkspaceLostRanges("v"); len(got) != 1 || got[0] != want {
				t.Fatalf("resumed range %+v; want the recorded [%+v]", got, want)
			}
		})
	}
}

// Failures inside the file store: before the rename nothing is published
// and no temp file is left; after the rename (directory sync) the range may
// be on disk as an orphan. Either way the move is refused with no epoch, the
// orphan is not reported, and a retry issues with exactly one range.
func TestWorkspaceLostRangeFileFaults(t *testing.T) {
	for _, step := range []string{"before-rename", "directory-sync"} {
		t.Run(step, func(t *testing.T) {
			disk, dir := newTempStore(t)
			p, now, barrier := forcedLostRangeFixture(t, disk, false)
			*now = barrier.Add(time.Second)
			testLostRangeFaultHook = func(s string) error {
				if s == step {
					return errInjected
				}
				return nil
			}
			t.Cleanup(func() { testLostRangeFaultHook = nil })
			if _, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead); !errors.Is(err, errInjected) {
				t.Fatalf("move with a failed range write: %v; want refused", err)
			}
			if line, _ := p.VolumeAuthorityLine("v"); line.Epoch != 1 {
				t.Fatalf("epoch issued: %+v", line)
			}
			_, statErr := os.Stat(filepath.Join(dir, "v"+lostRangeFileExtension))
			temps, _ := filepath.Glob(filepath.Join(dir, "*"+lostRangeFileExtension+tempSuffix))
			if len(temps) != 0 {
				t.Fatalf("temp file left: %v", temps)
			}
			if step == "before-rename" && statErr == nil {
				t.Fatal("range published although the write failed before the rename")
			}
			if step == "directory-sync" && statErr != nil {
				t.Fatalf("fixture: the rename did not land: %v", statErr)
			}
			if got := NewPublisher(nil, WithStore(disk)).WorkspaceLostRanges("v"); len(got) != 0 {
				t.Fatalf("orphan reported: %+v", got)
			}
			testLostRangeFaultHook = nil
			r, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead)
			if err != nil {
				t.Fatal("retry:", err)
			}
			if got := NewPublisher(nil, WithStore(disk)).WorkspaceLostRanges("v"); len(got) != 1 || got[0].NewEpoch != r.NewEpoch {
				t.Fatalf("after the retry: %+v; want one range", got)
			}
		})
	}
}

// A store wrapper that embeds AuthorityStore forwards the lost ranges: a
// forced move through it records its range.
func TestWorkspaceLostRangeThroughAStoreWrapper(t *testing.T) {
	disk, _ := newTempStore(t)
	wrapped := &workspaceFailStore{AuthorityStore: disk}
	p, now, barrier := forcedLostRangeFixture(t, wrapped, false)
	*now = barrier.Add(time.Second)
	if _, err := p.MoveWorkspace(context.Background(), forcedLostRangeReq, forcedLostRangeHead); err != nil {
		t.Fatal(err)
	}
	if got := NewPublisher(nil, WithStore(disk)).WorkspaceLostRanges("v"); len(got) != 1 {
		t.Fatalf("range through a wrapper: %+v", got)
	}
}

// Budget evidence (logged, not asserted): the added critical-section write,
// one file of the volume's whole range history plus two fsyncs, for a short
// and a long history.
func TestWorkspaceLostRangeWriteCost(t *testing.T) {
	disk, _ := newTempStore(t)
	for _, n := range []int{1, 1000} {
		ranges := make([]WorkspaceLostRange, n)
		for i := range ranges {
			ranges[i] = WorkspaceLostRange{RequestID: "request-" + strings.Repeat("x", 24), NewEpoch: uint64(i + 2), OldReplicaID: "r1", OldEpoch: uint64(i + 1), LostAfterLSN: 1 << 40, LostUntilNS: 1 << 60}
		}
		var worst time.Duration
		for i := 0; i < 5; i++ {
			start := time.Now()
			if err := disk.PutWorkspaceLostRanges("v", ranges); err != nil {
				t.Fatal(err)
			}
			if d := time.Since(start); d > worst {
				worst = d
			}
		}
		t.Logf("lost range write: %d entries, worst of 5 = %v", n, worst)
	}
}
