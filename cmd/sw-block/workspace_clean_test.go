package main

import (
	"strings"
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func cleanMoveArgs(addr string, extra ...string) []string {
	return append([]string{"ops", "workspace", "move", "--clean", "--master", addr, "--volume", "vol", "--replica", "r1", "--epoch", "3", "--request-id", "clean-1", "--target-host", "node-b", "--target-data", "10.0.0.2:3260", "--target-ctrl", "10.0.0.2:9100"}, extra...)
}

func cleanGrant(q *control.WorkspaceMove) *control.WorkspaceGrant {
	g := wsGrant(q)
	g.Kind, g.LostRange = "clean", nil
	return g
}

// Existing M2 Acked -> Moved (clean), INV-WS-CLEAN-MOVE-001.
func TestWorkspaceCleanMoveSendsOnlyTheExplicitCleanRequest(t *testing.T) {
	f := &fakeWorkspace{move: func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
		if q.GetKind() != "clean" || q.GetForce() || q.GetVolumeId() != "vol" || q.GetRequestId() != "clean-1" || q.GetExpectedReplicaId() != "r1" || q.GetExpectedEpoch() != 3 || q.GetTargetReplicaId() != "r1" || q.GetTargetServerId() != "node-b" || q.GetTargetDataAddr() != "10.0.0.2:3260" || q.GetTargetCtrlAddr() != "10.0.0.2:9100" {
			t.Errorf("clean request=%+v", q)
		}
		return cleanGrant(q), nil
	}}
	addr, _ := startFakeWorkspace(t, f)
	for i := 0; i < 2; i++ {
		code, out, errOut := runCLI(cleanMoveArgs(addr))
		if code != wsExitOK || !strings.Contains(out, "clean move issued") || !strings.Contains(out, "clean-1") || !strings.Contains(out, "start") || strings.Contains(out, "no loss") {
			t.Fatalf("exit=%d out=%q err=%q", code, out, errOut)
		}
	}
	if f.total() != 2 || f.n("MoveWorkspace") != 2 {
		t.Fatalf("unexpected orchestration calls=%d", f.total())
	}
}

func TestWorkspaceCleanMoveRefusesBadInputBeforeDial(t *testing.T) {
	f := &fakeWorkspace{}
	addr, dials := startFakeWorkspace(t, f)
	for _, extra := range [][]string{{"--clean=false"}, {"--master", ""}, {"--volume", ""}, {"--replica", ""}, {"--epoch", "0"}, {"--request-id", ""}, {"--target-host", ""}, {"--target-data", ""}, {"--target-ctrl", ""}, {"--timeout", "0s"}, {"--force"}, {"unexpected"}} {
		if code, _, _ := runCLI(cleanMoveArgs(addr, extra...)); code != wsExitRefusedLocally || *dials != 0 || f.total() != 0 {
			t.Fatalf("%v: exit=%d dials=%d calls=%d", extra, code, *dials, f.total())
		}
	}
}

func TestWorkspaceCleanMoveRejectsMismatchedGrant(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*control.WorkspaceGrant)
	}{
		{"unissued", func(g *control.WorkspaceGrant) { g.Issued = false }},
		{"forced", func(g *control.WorkspaceGrant) { g.Kind = "forced" }},
		{"request", func(g *control.WorkspaceGrant) { g.RequestId = "other" }},
		{"volume", func(g *control.WorkspaceGrant) { g.VolumeId = "other" }},
		{"replica", func(g *control.WorkspaceGrant) { g.OldReplicaId = "other" }},
		{"old_epoch", func(g *control.WorkspaceGrant) { g.OldEpoch++ }},
		{"target_replica", func(g *control.WorkspaceGrant) { g.NewReplicaId = "other" }},
		{"target_host", func(g *control.WorkspaceGrant) { g.NewHost = "other" }},
		{"new_epoch", func(g *control.WorkspaceGrant) { g.NewEpoch = g.OldEpoch }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeWorkspace{move: func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
				g := cleanGrant(q)
				tc.edit(g)
				return g, nil
			}}
			addr, _ := startFakeWorkspace(t, f)
			code, out, errOut := runCLI(cleanMoveArgs(addr))
			if code != wsExitUnknown || out != "" || !strings.Contains(errOut, "SAME --request-id clean-1") {
				t.Fatalf("exit=%d out=%q err=%q", code, out, errOut)
			}
		})
	}
}

func TestWorkspaceCleanMovePreservesRefusalAndUnknown(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"unsealed_or_no_ack", status.Error(codes.FailedPrecondition, "HEAD not sealed or acknowledgement missing"), wsExitRefused},
		{"lost_reply", status.Error(codes.Unavailable, "reply lost"), wsExitUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeWorkspace{move: func(*control.WorkspaceMove) (*control.WorkspaceGrant, error) { return nil, tc.err }}
			addr, _ := startFakeWorkspace(t, f)
			code, out, errOut := runCLI(cleanMoveArgs(addr))
			if code != tc.want || out != "" || (tc.want == wsExitUnknown && !strings.Contains(errOut, "SAME --request-id clean-1")) {
				t.Fatalf("exit=%d out=%q err=%q", code, out, errOut)
			}
		})
	}
}
