package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type workspaceCLIServer struct {
	control.UnimplementedWorkspaceServiceServer
	calls   chan proto.Message
	failure error
}

func (s *workspaceCLIServer) RegisterWorkspaceBinding(_ context.Context, q *control.WorkspaceBinding) (*control.WorkspaceBinding, error) {
	s.calls <- proto.Clone(q)
	return q, s.failure
}
func (s *workspaceCLIServer) PlaceWorkspace(_ context.Context, q *control.WorkspacePlacement) (*control.WorkspaceEmpty, error) {
	s.calls <- proto.Clone(q)
	return &control.WorkspaceEmpty{}, s.failure
}
func (s *workspaceCLIServer) MoveWorkspace(_ context.Context, q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
	s.calls <- proto.Clone(q)
	return &control.WorkspaceGrant{RequestId: q.RequestId, Kind: q.Kind, Issued: true, NewEpoch: 9, HeadGen: 17, RemoteLsn: 9007199254740993}, s.failure
}
func (s *workspaceCLIServer) QueryWorkspace(_ context.Context, q *control.WorkspaceVolume) (*control.WorkspaceStatus, error) {
	s.calls <- proto.Clone(q)
	return &control.WorkspaceStatus{Moves: []*control.WorkspaceGrant{{Kind: "forced", NewEpoch: 9, HeadGen: 17, RemoteLsn: 9007199254740993, Issued: true}}}, s.failure
}
func workspaceServer(t *testing.T, failure error) (string, *workspaceCLIServer) {
	t.Helper()
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := &workspaceCLIServer{calls: make(chan proto.Message, 20), failure: failure}
	g := grpc.NewServer()
	control.RegisterWorkspaceServiceServer(g, s)
	go g.Serve(ln)
	t.Cleanup(g.Stop)
	return ln.Addr().String(), s
}
func workspaceRun(t *testing.T, addr, op, body string, extra ...string) (int, string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "request.json")
	if e := os.WriteFile(path, []byte(body), 0600); e != nil {
		t.Fatal(e)
	}
	args := []string{"ops", "workspace", op, "--master", addr, "--input", path, "--timeout", "2s"}
	args = append(args, extra...)
	var out, errs bytes.Buffer
	rc := run(args, &out, &errs)
	return rc, out.String(), errs.String()
}
func TestOpsWorkspaceJSONRequestsReachRPCs(t *testing.T) {
	addr, s := workspaceServer(t, nil)
	cases := []struct {
		op, body string
		extra    []string
		want     proto.Message
		reply    proto.Message
	}{
		{"bind", `{"volumeId":"v","serverId":"node-a","dataAddr":"127.0.0.1:9200","ctrlAddr":"127.0.0.1:9100"}`, nil, &control.WorkspaceBinding{VolumeId: "v", ServerId: "node-a", DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}, &control.WorkspaceBinding{}},
		{"place", `{"volume_id":"v","server_id":"node-a","replica_id":"r1"}`, nil, &control.WorkspacePlacement{VolumeId: "v", ServerId: "node-a", ReplicaId: "r1"}, &control.WorkspaceEmpty{}},
		{"move", `{"requestId":"clean-1","volumeId":"v","expectedReplicaId":"r1","expectedEpoch":"1","targetReplicaId":"r1","targetServerId":"node-b","targetDataAddr":"127.0.0.1:9201","targetCtrlAddr":"127.0.0.1:9101"}`, nil, &control.WorkspaceMove{RequestId: "clean-1", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:9201", TargetCtrlAddr: "127.0.0.1:9101", Kind: "clean"}, &control.WorkspaceGrant{}},
		{"move", `{"requestId":"forced-1","volumeId":"v","expectedReplicaId":"r1","expectedEpoch":"1","targetReplicaId":"r1","targetServerId":"node-b","targetDataAddr":"127.0.0.1:9201","targetCtrlAddr":"127.0.0.1:9101"}`, []string{"--force"}, &control.WorkspaceMove{RequestId: "forced-1", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:9201", TargetCtrlAddr: "127.0.0.1:9101", Kind: "forced", Force: true}, &control.WorkspaceGrant{}},
		{"journal", `{"volumeId":"v"}`, nil, &control.WorkspaceVolume{VolumeId: "v"}, &control.WorkspaceStatus{}},
	}
	for _, c := range cases {
		rc, out, errs := workspaceRun(t, addr, c.op, c.body, c.extra...)
		if rc != 0 {
			t.Fatalf("%s rc=%d out=%s err=%s", c.op, rc, out, errs)
		}
		select {
		case got := <-s.calls:
			if !proto.Equal(got, c.want) {
				t.Fatalf("RPC got %v want %v", got, c.want)
			}
		default:
			t.Fatal("CLI did not call RPC")
		}
		if e := protojson.Unmarshal([]byte(out), c.reply); e != nil {
			t.Fatal("invalid response JSON", out, e)
		}
		if c.op == "move" && c.reply.(*control.WorkspaceGrant).RemoteLsn != 9007199254740993 {
			t.Fatal("lost uint64 precision", out)
		}
	}
}
func TestOpsWorkspaceInvalidInputNeverMutates(t *testing.T) {
	addr, s := workspaceServer(t, nil)
	for _, tc := range []struct {
		op, body string
		extra    []string
	}{
		{"bind", `{`, nil}, {"bind", `null`, nil}, {"bind", `{"volumeId":"v","unexpected":1}`, nil}, {"bind", `{"volumeId":"v","retired":true}`, nil},
		{"place", `{}`, nil}, {"move", `{"kind":"forced","force":true}`, nil}, {"move", `{"kind":"clean"}`, []string{"--force"}}, {"journal", `{}`, nil},
		{"journal", `{"volumeId":"v"}`, []string{"--timeout", "0s"}}, {"journal", `{"volumeId":"v"}`, []string{"--force"}},
		{"move", `{"requestId":"forced-json","volumeId":"v","expectedReplicaId":"r1","expectedEpoch":"1","targetReplicaId":"r1","targetServerId":"node-b","targetDataAddr":"127.0.0.1:9201","targetCtrlAddr":"127.0.0.1:9101","kind":"forced","force":true}`, nil},
	} {
		rc, out, _ := workspaceRun(t, addr, tc.op, tc.body, tc.extra...)
		var e chunkErrBody
		if rc != 2 || json.Unmarshal([]byte(out), &e) != nil || e.Code != "bad_request" || e.Retryable {
			t.Fatalf("invalid input: rc=%d out=%s", rc, out)
		}
	}
	if len(s.calls) != 0 {
		t.Fatal("invalid input sent RPC", len(s.calls))
	}
}
func TestOpsWorkspaceTypedErrorsAndSingleAttempt(t *testing.T) {
	for _, tc := range []struct {
		grpc  codes.Code
		code  string
		retry bool
	}{{codes.FailedPrecondition, "failed_precondition", false}, {codes.Unavailable, "remote_unavailable", true}, {codes.DeadlineExceeded, "outcome_unknown", true}} {
		addr, s := workspaceServer(t, status.Error(tc.grpc, "caller must inspect journal"))
		rc, out, errs := workspaceRun(t, addr, "bind", `{"volumeId":"v","serverId":"a","dataAddr":"127.0.0.1:9200","ctrlAddr":"127.0.0.1:9100"}`)
		var e chunkErrBody
		if rc != 1 || json.Unmarshal([]byte(out), &e) != nil || e.Code != tc.code || e.Retryable != tc.retry || !strings.Contains(errs, "ops workspace") {
			t.Fatalf("typed error: %d %s %s", rc, out, errs)
		}
		if len(s.calls) != 1 {
			t.Fatal("ambiguous mutation replayed", len(s.calls))
		}
	}
}
