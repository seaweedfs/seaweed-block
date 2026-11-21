package wsclient

import (
	"context"
	"errors"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeRPC answers QueryWorkspace and MoveWorkspace and counts every call.
type fakeRPC struct {
	control.WorkspaceServiceClient // nil: other methods panic
	calls                          int
	status                         *control.WorkspaceStatus
	moves                          []*control.WorkspaceMove
	grant                          *control.WorkspaceGrant
	moveErr                        error
}

func (f *fakeRPC) QueryWorkspace(context.Context, *control.WorkspaceVolume, ...grpc.CallOption) (*control.WorkspaceStatus, error) {
	f.calls++
	return f.status, nil
}

func (f *fakeRPC) MoveWorkspace(_ context.Context, q *control.WorkspaceMove, _ ...grpc.CallOption) (*control.WorkspaceGrant, error) {
	f.calls++
	f.moves = append(f.moves, q)
	return f.grant, f.moveErr
}

// INV-WS-FORCED-CONFIRM-001: the confirmation is exactly <volume>@<epoch>.
func TestParseConfirmation(t *testing.T) {
	for _, tc := range []struct {
		confirm, volume string
		epoch           uint64
	}{
		{"vol@3", "vol", 3},
		{"a@b@7", "a@b", 7},
		{"", "vol", 0},
		{"vol", "vol", 0},
		{"vol@", "vol", 0},
		{"other@3", "vol", 0},
		{"vol@0", "vol", 0},
		{"vol@03", "vol", 0},
		{"vol@+3", "vol", 0},
		{"vol@-3", "vol", 0},
		{"vol@3 ", "vol", 0},
		{" vol@3", "vol", 0},
		{"VOL@3", "vol", 0},
	} {
		got, err := ParseConfirmation(tc.confirm, tc.volume)
		if got != tc.epoch || (err == nil) != (tc.epoch != 0) || (err != nil && !errors.Is(err, ErrConfirmation)) {
			t.Errorf("ParseConfirmation(%q, %q) = %d, %v; want %d", tc.confirm, tc.volume, got, err, tc.epoch)
		}
	}
}

var testMove = ForcedMove{RequestID: "req-1", VolumeID: "vol", TargetHost: "node-b", TargetDataAddr: "10.0.0.2:1", TargetCtrlAddr: "10.0.0.2:2", Confirm: "vol@3"}

// INV-WS-FORCED-CONFIRM-001: force:true is sent only with a matching
// confirmation, and then always with kind forced and the confirmed epoch.
func TestForceMoveSendsForceOnlyWithTheConfirmation(t *testing.T) {
	for _, bad := range []string{"", "vol@x", "other@3", "vol@0"} {
		f := &fakeRPC{}
		m := testMove
		m.Confirm = bad
		if _, err := New(f).ForceMove(context.Background(), m); OutcomeOf(err) != OutcomeRefusedLocally || f.calls != 0 {
			t.Fatalf("confirmation %q: %v after %d RPCs; want a local refusal and none", bad, err, f.calls)
		}
	}
	f := &fakeRPC{status: &control.WorkspaceStatus{Lease: &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3}}, moveErr: status.Error(codes.FailedPrecondition, "not yet")}
	if _, err := New(f).ForceMove(context.Background(), testMove); OutcomeOf(err) != OutcomeRefused {
		t.Fatalf("master refusal: %v", err)
	}
	q := f.moves[0]
	if len(f.moves) != 1 || q.Kind != "forced" || !q.Force || q.ExpectedEpoch != 3 || q.ExpectedReplicaId != "r1" || q.TargetReplicaId != "r1" || q.RequestId != "req-1" || q.TargetServerId != "node-b" {
		t.Fatalf("sent %+v", f.moves)
	}
}

// The holder's replica id at the confirmed epoch comes from the lease, or
// on a rerun after the issue from the journal; an epoch the master never
// had is refused without sending a move.
func TestForceMoveResolvesTheHolder(t *testing.T) {
	rerun := &fakeRPC{status: &control.WorkspaceStatus{Lease: &control.WorkspaceLeaseFact{ReplicaId: "r1", Epoch: 4},
		Moves: []*control.WorkspaceGrant{{RequestId: "req-1", OldReplicaId: "r1", OldEpoch: 3, NewReplicaId: "r1", NewEpoch: 4, Issued: true, Kind: "forced"}}}, moveErr: status.Error(codes.Unavailable, "down")}
	if _, err := New(rerun).ForceMove(context.Background(), testMove); OutcomeOf(err) != OutcomeUnknown || len(rerun.moves) != 1 || rerun.moves[0].ExpectedReplicaId != "r1" {
		t.Fatalf("rerun: %v %+v", err, rerun.moves)
	}
	stale := &fakeRPC{status: &control.WorkspaceStatus{Lease: &control.WorkspaceLeaseFact{ReplicaId: "r1", Epoch: 9}}}
	if _, err := New(stale).ForceMove(context.Background(), testMove); OutcomeOf(err) != OutcomeRefused || len(stale.moves) != 0 {
		t.Fatalf("stale epoch: %v, %d moves sent", err, len(stale.moves))
	}
}

// A forced grant without its own stored range is issued but unproven.
func TestForceMoveGrantWithoutRangeIsUnproven(t *testing.T) {
	g := &control.WorkspaceGrant{RequestId: "req-1", VolumeId: "vol", Kind: "forced", Issued: true, OldReplicaId: "r1", OldEpoch: 3, NewReplicaId: "r1", NewEpoch: 4}
	f := &fakeRPC{status: &control.WorkspaceStatus{Lease: &control.WorkspaceLeaseFact{ReplicaId: "r1", Epoch: 3}}, grant: g}
	got, err := New(f).ForceMove(context.Background(), testMove)
	if OutcomeOf(err) != OutcomeReadoutUnproven || got != g {
		t.Fatalf("no range: %v", err)
	}
	g.LostRange = &control.WorkspaceLostRange{RequestId: "other", NewEpoch: 4, OldEpoch: 3, OldReplicaId: "r1"}
	if _, err := New(f).ForceMove(context.Background(), testMove); OutcomeOf(err) != OutcomeReadoutUnproven {
		t.Fatalf("another move's range: %v", err)
	}
	g.LostRange.RequestId = "req-1"
	if _, err := New(f).ForceMove(context.Background(), testMove); err != nil {
		t.Fatalf("own range: %v", err)
	}
}

func TestClassify(t *testing.T) {
	for code, want := range map[codes.Code]Outcome{
		codes.FailedPrecondition: OutcomeRefused, codes.NotFound: OutcomeRefused, codes.DataLoss: OutcomeReadoutUnproven,
		codes.Unavailable: OutcomeUnknown, codes.DeadlineExceeded: OutcomeUnknown, codes.Canceled: OutcomeUnknown, codes.Internal: OutcomeUnknown,
	} {
		if got := OutcomeOf(classify("op", status.Error(code, "x"))); got != want {
			t.Errorf("%v: %v; want %v", code, got, want)
		}
	}
	if OutcomeOf(classify("op", context.DeadlineExceeded)) != OutcomeUnknown {
		t.Error("a bare deadline is not unknown")
	}
}

// INV-WS-LOST-RANGE-READOUT-001: the readout wording (ruling A: HEAD time is
// the filer time; the upper bound is master time).
func TestReadoutWording(t *testing.T) {
	r := &control.WorkspaceLostRange{OldReplicaId: "r1", OldEpoch: 3, LostAfterLsn: 42, LostUntilNs: time.Date(2026, 11, 21, 1, 5, 0, 5, time.UTC).UnixNano(),
		HeadGen: 9, HeadTimeNs: time.Date(2026, 11, 21, 1, 2, 3, 0, time.UTC).UnixNano(), HeadTimeKnown: true}
	want := "volume vol restored to the last published image at 2026-11-21T01:02:03Z, filer clock (gen 9, LSN 42): writes with LSN <= 42 are preserved; " +
		"writes acknowledged by r1@3 after that and before 2026-11-21T01:05:00.000000005Z, master clock, may be lost; the upper bound is a time, not an LSN."
	if got := Readout("vol", r); got != want {
		t.Fatalf("readout\n got %s\nwant %s", got, want)
	}
	r.HeadTimeKnown = false
	want = "volume vol restored to the last published image at an unknown time (no filer time was recorded) (gen 9, LSN 42): writes with LSN <= 42 are preserved; " +
		"writes acknowledged by r1@3 after that and before 2026-11-21T01:05:00.000000005Z, master clock, may be lost; the upper bound is a time, not an LSN."
	if got := Readout("vol", r); got != want {
		t.Fatalf("readout, time unknown\n got %s\nwant %s", got, want)
	}
	r.HeadGen = 0
	if got := Readout("vol", r); got != "no remote image exists: everything written by r1@3 is lost." {
		t.Fatalf("no-HEAD readout: %s", got)
	}
}
