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

// Other RPC methods are deliberately absent: revoke must not orchestrate a
// query, detach, acknowledgement or move on the operator's behalf.
type revokeCLIRecorder struct {
	control.WorkspaceServiceClient
	requests []*control.WorkspaceIdentity
	reply    *control.WorkspaceLeaseFact
	err      error
}

func (r *revokeCLIRecorder) RevokeWorkspace(_ context.Context, q *control.WorkspaceIdentity, _ ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	r.requests = append(r.requests, q)
	return r.reply, r.err
}

func installRevokeRecorder(t *testing.T, r *revokeCLIRecorder) *int {
	t.Helper()
	dials := 0
	old := workspaceDial
	workspaceDial = func(string) (*wsclient.Client, error) {
		dials++
		return wsclient.New(r), nil
	}
	t.Cleanup(func() { workspaceDial = old })
	return &dials
}

func revokeArgs(extra ...string) []string {
	return append([]string{"ops", "workspace", "revoke", "--master", "127.0.0.1:19090", "--volume", "vol", "--replica", "r1", "--epoch", "3", "--sealed-gen", "9"}, extra...)
}

// G1 / existing M2 Granted -> Revoking, INV-WS-CLEAN-MOVE-001.
// A successful request is not a host ACK or proof of a detached HEAD.
func TestWorkspaceRevokeSendsOnlyTheExplicitIdentity(t *testing.T) {
	r := &revokeCLIRecorder{reply: &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3, ExpiresAtNs: 12345, Revoking: true}}
	dials := installRevokeRecorder(t, r)
	code, out, errOut := runCLI(revokeArgs())
	if code != wsExitOK || *dials != 1 || len(r.requests) != 1 {
		t.Fatalf("exit=%d dials=%d requests=%d stdout=%q stderr=%q", code, *dials, len(r.requests), out, errOut)
	}
	q := r.requests[0]
	if q.GetVolumeId() != "vol" || q.GetReplicaId() != "r1" || q.GetEpoch() != 3 {
		t.Fatalf("identity=%+v", q)
	}
	if !strings.Contains(out, "revocation requested") || !strings.Contains(out, "vol") || !strings.Contains(out, "r1@3") || !strings.Contains(out, "9") {
		t.Fatalf("request outcome must name its identity and remain distinct from ACK: %q", out)
	}
}

func TestWorkspaceRevokeRejectsInvalidInputsBeforeDial(t *testing.T) {
	r := &revokeCLIRecorder{}
	dials := installRevokeRecorder(t, r)
	for _, extra := range [][]string{
		{"--master", ""}, {"--volume", ""}, {"--replica", ""}, {"--epoch", "0"}, {"--epoch", "-1"},
		{"--timeout", "0s"}, {"--timeout", "-1s"}, {"unexpected"},
		{"--sealed-gen", "0"}, {"--without-seal"},
	} {
		code, _, _ := runCLI(revokeArgs(extra...))
		if code != wsExitRefusedLocally || *dials != 0 || len(r.requests) != 0 {
			t.Fatalf("%v: exit=%d dials=%d requests=%d", extra, code, *dials, len(r.requests))
		}
	}
}

func TestWorkspaceRevokeRequiresAnExplicitSealChoice(t *testing.T) {
	r := &revokeCLIRecorder{reply: &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3, Revoking: true}}
	dials := installRevokeRecorder(t, r)
	args := revokeArgs()
	args = args[:len(args)-2] // omit --sealed-gen entirely
	if code, _, _ := runCLI(args); code != wsExitRefusedLocally || *dials != 0 {
		t.Fatalf("missing seal choice: exit=%d dials=%d", code, *dials)
	}
	code, out, errOut := runCLI(append(args, "--without-seal"))
	if code != wsExitOK || !strings.Contains(out, "abandoning unpublished tail") || len(r.requests) != 1 {
		t.Fatalf("explicit lossy choice: exit=%d requests=%d out=%q err=%q", code, len(r.requests), out, errOut)
	}
}

func TestWorkspaceRevokeDoesNotTrustAnUnmatchedReply(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply *control.WorkspaceLeaseFact
	}{
		{"missing", nil},
		{"volume", &control.WorkspaceLeaseFact{VolumeId: "other", ReplicaId: "r1", Epoch: 3, Revoking: true}},
		{"replica", &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r2", Epoch: 3, Revoking: true}},
		{"epoch", &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 4, Revoking: true}},
		{"not_revoking", &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &revokeCLIRecorder{reply: tc.reply}
			installRevokeRecorder(t, r)
			code, out, errOut := runCLI(revokeArgs())
			if code != wsExitUnknown || out != "" || len(r.requests) != 1 || !strings.Contains(errOut, "unknown") {
				t.Fatalf("exit=%d requests=%d stdout=%q stderr=%q", code, len(r.requests), out, errOut)
			}
		})
	}
}

func TestWorkspaceRevokePreservesRPCOutcome(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want int
	}{{codes.FailedPrecondition, wsExitRefused}, {codes.Unavailable, wsExitUnknown}, {codes.DeadlineExceeded, wsExitUnknown}} {
		t.Run(tc.code.String(), func(t *testing.T) {
			r := &revokeCLIRecorder{err: status.Error(tc.code, "recorded revoke result")}
			installRevokeRecorder(t, r)
			code, out, errOut := runCLI(revokeArgs())
			if code != tc.want || out != "" || len(r.requests) != 1 || !strings.Contains(errOut, "recorded revoke result") {
				t.Fatalf("exit=%d want=%d requests=%d stdout=%q stderr=%q", code, tc.want, len(r.requests), out, errOut)
			}
		})
	}
}
