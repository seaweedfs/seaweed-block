package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func plannedMoveArgs(master string) []string {
	return []string{"ops", "workspace", "planned-move", "--master", master, "--volume", "vol", "--replica", "r1", "--epoch", "3", "--source-host", "node-a", "--source-status", "127.0.0.1:1", "--target-host", "node-b", "--target-status", "127.0.0.1:2", "--request-id", "plan-1", "--timeout", "1s"}
}

func TestPlannedMoveMissingTargetBindingChangesNothing(t *testing.T) {
	f := &fakeWorkspace{status: wsHolder()}
	master, _ := startFakeWorkspace(t, f)
	code, out, errout := runCLI(plannedMoveArgs(master))
	if code != wsExitRefused || !strings.Contains(out+errout, "REFUSED") || !strings.Contains(out+errout, "binding") {
		t.Fatalf("want binding refusal, got code=%d out=%q err=%q", code, out, errout)
	}
	if f.n("QueryWorkspace") != 1 || f.total() != 1 {
		t.Fatalf("missing binding caused a write: calls=%+v", f.calls)
	}
}

type plannedMaster struct {
	*fakeWorkspace
	rf int32
	control.UnimplementedLifecycleServiceServer
}

func (f *plannedMaster) GetVolume(context.Context, *control.GetLifecycleVolumeRequest) (*control.CreateVolumeResponse, error) {
	rf := f.rf
	if rf == 0 {
		rf = 1
	}
	return &control.CreateVolumeResponse{VolumeId: "vol", ReplicationFactor: rf, StoreReady: true}, nil
}
func (f *plannedMaster) RevokeWorkspace(_ context.Context, q *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	f.count("RevokeWorkspace")
	f.status.Lease.Revoking = true
	f.status.RevocationAck = &control.WorkspaceRevocationAck{Identity: &control.WorkspaceIdentity{VolumeId: q.VolumeId, ReplicaId: q.ReplicaId, Epoch: q.Epoch, ServerId: "node-a"}, HeadGen: 9}
	return f.status.Lease, nil
}
func startPlannedMaster(t *testing.T, f *plannedMaster) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := grpc.NewServer()
	control.RegisterWorkspaceServiceServer(s, f)
	control.RegisterLifecycleServiceServer(s, f)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
	return l.Addr().String()
}
func TestPlannedMoveCompletesOnlyAtTargetAndResumesSameID(t *testing.T) {
	f := &plannedMaster{fakeWorkspace: &fakeWorkspace{status: wsHolder()}}
	f.status.Bindings = []*control.WorkspaceBinding{{VolumeId: "vol", ServerId: "node-b", DataAddr: "127.0.0.1:3260", CtrlAddr: "127.0.0.1:9100"}}
	f.move = func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
		g := wsGrant(q)
		g.Kind = "clean"
		g.LostRange = nil
		f.status.Moves = append(f.status.Moves, g)
		return g, nil
	}
	seals := 0
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chunk/detach" {
			t.Errorf("unexpected source request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(400)
			return
		}
		var b struct {
			Volume   string `json:"volumeID"`
			Plain    bool   `json:"plain"`
			Expected struct {
				Replica string `json:"replicaID"`
				Epoch   uint64 `json:"epoch"`
			} `json:"expectedHolder"`
		}
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil || b.Volume != "vol" || !b.Plain || b.Expected.Replica != "r1" || b.Expected.Epoch != 3 {
			t.Errorf("unconditional/wrong seal: %+v %v", b, err)
			w.WriteHeader(400)
			return
		}
		seals++
		fmt.Fprint(w, `{"volumeID":"vol","headGen":9,"headEpoch":3,"holder":"node-a/r1","cutLSN":42,"lastWrittenLSN":42}`)
	}))
	defer source.Close()
	ready := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" || r.URL.Query().Get("volume") != "vol" {
			t.Errorf("wrong target observation %s", r.URL)
			w.WriteHeader(400)
			return
		}
		if len(f.status.Moves) == 0 {
			t.Error("target used as proof before a move receipt")
		}
		json.NewEncoder(w).Encode(map[string]any{"VolumeID": "vol", "ReplicaID": "r1", "Epoch": 4, "Healthy": true, "FrontendPrimaryReady": ready})
	}))
	defer target.Close()
	args := plannedMoveArgs(startPlannedMaster(t, f))
	args = append(args, "--source-status", source.URL, "--target-status", target.URL, "--timeout", "1s")
	code, out, errout := runCLI(args)
	if code != 6 || !strings.Contains(out, "MOVED-TARGET-NOT-SERVING") {
		t.Fatalf("move receipt alone completed: code=%d out=%q err=%q", code, out, errout)
	}
	if seals != 1 || f.n("MoveWorkspace") != 1 {
		t.Fatalf("wrong steps: seals=%d moves=%d", seals, f.n("MoveWorkspace"))
	}
	ready = true
	code, out, errout = runCLI(args)
	if code != 0 || !strings.Contains(out, "COMPLETE") {
		t.Fatalf("same-id resume did not complete: code=%d out=%q err=%q", code, out, errout)
	}
	if seals != 1 || f.n("MoveWorkspace") != 1 || len(f.status.Moves) != 1 {
		t.Fatalf("same-id retry repeated a move/seal: seals=%d moves=%d receipts=%d", seals, f.n("MoveWorkspace"), len(f.status.Moves))
	}
}

func TestPlannedMoveSealRefusalsNeverRevoke(t *testing.T) {
	for _, tc := range []struct {
		name, reply  string
		status, want int
	}{
		{"holder changed", `{"code":"holder_changed","retryable":false}`, 409, 1},
		{"wrong returned epoch", `{"volumeID":"vol","headGen":9,"headEpoch":4,"holder":"node-a/r1","cutLSN":42,"lastWrittenLSN":42}`, 200, 1},
		{"wrong returned holder", `{"volumeID":"vol","headGen":9,"headEpoch":3,"holder":"node-c/r1","cutLSN":42,"lastWrittenLSN":42}`, 200, 1},
		{"unsynced frontier", `{"volumeID":"vol","headGen":9,"headEpoch":3,"holder":"node-a/r1","cutLSN":41,"lastWrittenLSN":42}`, 200, 1},
		{"lost reply", `outcome unknown`, 503, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &plannedMaster{fakeWorkspace: &fakeWorkspace{status: wsHolder()}}
			f.status.Bindings = []*control.WorkspaceBinding{{VolumeId: "vol", ServerId: "node-b", DataAddr: "127.0.0.1:3260", CtrlAddr: "127.0.0.1:9100"}}
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.reply) }))
			defer source.Close()
			args := append(plannedMoveArgs(startPlannedMaster(t, f)), "--source-status", source.URL)
			code, out, errout := runCLI(args)
			if code != tc.want || f.n("RevokeWorkspace") != 0 || f.n("MoveWorkspace") != 0 {
				t.Fatalf("seal failure continued: exit=%d out=%q err=%q revokes=%d moves=%d", code, out, errout, f.n("RevokeWorkspace"), f.n("MoveWorkspace"))
			}
		})
	}
}

func TestPlannedMoveResumesAnIssuedMoveAfterLostReply(t *testing.T) {
	f := &plannedMaster{fakeWorkspace: &fakeWorkspace{status: wsHolder()}}
	f.status.Bindings = []*control.WorkspaceBinding{{VolumeId: "vol", ServerId: "node-b", DataAddr: "127.0.0.1:3260", CtrlAddr: "127.0.0.1:9100"}}
	f.status.Lease.Revoking = true
	f.status.RevocationAck = &control.WorkspaceRevocationAck{Identity: &control.WorkspaceIdentity{VolumeId: "vol", ReplicaId: "r1", Epoch: 3, ServerId: "node-a"}, HeadGen: 9}
	f.move = func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
		g := wsGrant(q)
		g.Kind = "clean"
		g.LostRange = nil
		f.status.Moves = append(f.status.Moves, g)
		return nil, status.Error(codes.Unavailable, "reply lost after issue")
	}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"VolumeID":"vol","ReplicaID":"r1","Epoch":4,"FrontendPrimaryReady":true}`)
	}))
	defer target.Close()
	args := append(plannedMoveArgs(startPlannedMaster(t, f)), "--target-status", target.URL)
	code, out, errout := runCLI(args)
	if code != 3 || !strings.Contains(out, "UNKNOWN") {
		t.Fatalf("lost move reply: %d %q %q", code, out, errout)
	}
	code, out, errout = runCLI(args)
	if code != 0 || !strings.Contains(out, "COMPLETE") || f.n("MoveWorkspace") != 1 || len(f.status.Moves) != 1 {
		t.Fatalf("lost reply replay: %d %q %q moves=%d receipts=%d", code, out, errout, f.n("MoveWorkspace"), len(f.status.Moves))
	}
}

func TestPlannedMoveDoesNotMoveWithoutRecordedACK(t *testing.T) {
	f := &plannedMaster{fakeWorkspace: &fakeWorkspace{status: wsHolder()}}
	f.status.Lease.Revoking = true
	f.status.Bindings = []*control.WorkspaceBinding{{VolumeId: "vol", ServerId: "node-b", DataAddr: "127.0.0.1:3260", CtrlAddr: "127.0.0.1:9100"}}
	seals := 0
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seals++; w.WriteHeader(500) }))
	defer source.Close()
	args := append(plannedMoveArgs(startPlannedMaster(t, f)), "--source-status", source.URL, "--timeout", "150ms")
	code, out, errout := runCLI(args)
	if code != 3 || !strings.Contains(out, "recorded ACK") || f.n("MoveWorkspace") != 0 || f.n("RevokeWorkspace") != 0 || seals != 0 {
		t.Fatalf("pending ACK advanced: %d out=%q err=%q moves=%d revokes=%d seals=%d", code, out, errout, f.n("MoveWorkspace"), f.n("RevokeWorkspace"), seals)
	}
}

func TestPlannedMoveRefusesARequestIDBoundToAnotherTarget(t *testing.T) {
	f := &plannedMaster{fakeWorkspace: &fakeWorkspace{status: wsHolder()}}
	f.status.Bindings = []*control.WorkspaceBinding{{VolumeId: "vol", ServerId: "node-b", DataAddr: "127.0.0.1:3260", CtrlAddr: "127.0.0.1:9100"}}
	f.status.Moves = []*control.WorkspaceGrant{{RequestId: "plan-1", VolumeId: "vol", Kind: "clean", Issued: true, OldReplicaId: "r1", OldEpoch: 3, OldHost: "node-a", NewReplicaId: "r1", NewEpoch: 4, NewHost: "node-c"}}
	code, out, errout := runCLI(plannedMoveArgs(startPlannedMaster(t, f)))
	if code != 1 || !strings.Contains(out, "bound to another move") || f.n("MoveWorkspace") != 0 {
		t.Fatalf("rebound request id: %d %q %q", code, out, errout)
	}
}

func TestPlannedMoveRefusesNETBeforeAnySeal(t *testing.T) {
	f := &plannedMaster{fakeWorkspace: &fakeWorkspace{status: wsHolder()}, rf: 2}
	f.status.Bindings = []*control.WorkspaceBinding{{VolumeId: "vol", ServerId: "node-b", DataAddr: "127.0.0.1:3260", CtrlAddr: "127.0.0.1:9100"}}
	code, out, errout := runCLI(plannedMoveArgs(startPlannedMaster(t, f)))
	if code != 1 || !strings.Contains(out, "RF1") || f.n("RevokeWorkspace") != 0 || f.n("MoveWorkspace") != 0 {
		t.Fatalf("NET not refused: %d %q %q", code, out, errout)
	}
}
