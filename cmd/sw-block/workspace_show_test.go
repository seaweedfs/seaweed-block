package main

import (
	"context"
	"strings"
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/wsclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workspaceShowRecorder struct {
	control.WorkspaceServiceClient
	requests []*control.WorkspaceVolume
	reply    *control.WorkspaceStatus
	err      error
}

func (r *workspaceShowRecorder) QueryWorkspace(_ context.Context, q *control.WorkspaceVolume, _ ...grpc.CallOption) (*control.WorkspaceStatus, error) {
	r.requests = append(r.requests, q)
	return r.reply, r.err
}

func installShowRecorder(t *testing.T, r *workspaceShowRecorder) *int {
	t.Helper()
	dials := 0
	old := workspaceDial
	workspaceDial = func(string) (*wsclient.Client, error) { dials++; return wsclient.New(r), nil }
	t.Cleanup(func() { workspaceDial = old })
	return &dials
}

// Query-only view of M2 Granted -> Revoking -> Acked -> Moved (clean),
// INV-WS-CLEAN-MOVE-001. A recorded ACK must remain distinct from a request.
func TestWorkspaceShowPrintsTheRecordedLeaseACKAndMoves(t *testing.T) {
	r := &workspaceShowRecorder{reply: &control.WorkspaceStatus{
		Lease:         &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3, ValidForNs: 1000000000, Revoking: true},
		RevocationAck: &control.WorkspaceRevocationAck{Identity: &control.WorkspaceIdentity{VolumeId: "vol", ReplicaId: "r1", Epoch: 3}, HeadGen: 9},
		Moves:         []*control.WorkspaceGrant{{VolumeId: "vol", RequestId: "old-move", Kind: "clean", OldEpoch: 2, NewEpoch: 3, OldReplicaId: "r1", NewReplicaId: "r1", OldHost: "node-a", NewHost: "node-b", HeadGen: 8, Issued: true}},
	}}
	dials := installShowRecorder(t, r)
	code, out, errOut := runCLI([]string{"ops", "workspace", "show", "--master", "master:19090", "--volume", "vol"})
	if code != 0 || *dials != 1 || len(r.requests) != 1 || r.requests[0].GetVolumeId() != "vol" {
		t.Fatalf("exit=%d dials=%d requests=%v out=%q err=%q", code, *dials, r.requests, out, errOut)
	}
	for _, want := range []string{"holder: r1@3", "lease state: revoking", "recorded ACK: r1@3 HEAD generation 9", "old-move", "node-a", "node-b", "issued=true"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %q", want, out)
		}
	}
}

func TestWorkspaceShowUsesTheMastersLeaseValidity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lease *control.WorkspaceLeaseFact
		want  string
	}{
		{"granted", &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3, ValidForNs: 1}, "lease state: granted"},
		{"expired", &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3, ExpiresAtNs: 9223372036854775807}, "lease state: expired"},
		{"absent", nil, "holder: none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &workspaceShowRecorder{reply: &control.WorkspaceStatus{Lease: tc.lease}}
			installShowRecorder(t, r)
			code, out, errOut := runCLI([]string{"ops", "workspace", "show", "--master", "master:19090", "--volume", "vol"})
			if code != 0 || !strings.Contains(out, tc.want) || !strings.Contains(out, "recorded ACK: none") {
				t.Fatalf("exit=%d out=%q err=%q", code, out, errOut)
			}
		})
	}
}

func TestWorkspaceShowOrdersOperatorIdentityWithoutInventingAHost(t *testing.T) {
	r := &workspaceShowRecorder{reply: &control.WorkspaceStatus{
		Lease: &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3, ValidForNs: 1},
	}}
	installShowRecorder(t, r)
	code, out, errOut := runCLI([]string{"ops", "workspace", "show", "--master", "master:19090", "--volume", "vol"})
	if code != 0 {
		t.Fatalf("exit=%d err=%q", code, errOut)
	}
	last := -1
	for _, field := range []string{"volume: vol", "replica: r1\n", "epoch: 3\n", "lease state: granted", "recorded ACK: none"} {
		at := strings.Index(out, field)
		if at <= last {
			t.Fatalf("missing or out-of-order %q in %q", field, out)
		}
		last = at
	}
	if !strings.Contains(out, "host: unknown (not recorded by the master)") {
		t.Fatalf("physical host must remain unknown: %q", out)
	}
}

func TestWorkspaceShowRefusesBadUsageAndClassifiesRPCFailure(t *testing.T) {
	r := &workspaceShowRecorder{err: status.Error(codes.Unavailable, "query unavailable")}
	dials := installShowRecorder(t, r)
	for _, args := range [][]string{
		{"--volume", "vol"}, {"--master", "master:19090"},
		{"--master", "master:19090", "--volume", "vol", "--timeout", "0s"},
	} {
		if code, _, _ := runCLI(append([]string{"ops", "workspace", "show"}, args...)); code != wsExitRefusedLocally || *dials != 0 {
			t.Fatalf("%v: exit=%d dials=%d", args, code, *dials)
		}
	}
	code, out, errOut := runCLI([]string{"ops", "workspace", "show", "--master", "master:19090", "--volume", "vol"})
	if code != wsExitUnknown || out != "" || !strings.Contains(errOut, "query unavailable") || len(r.requests) != 1 {
		t.Fatalf("exit=%d out=%q err=%q requests=%d", code, out, errOut, len(r.requests))
	}
}
