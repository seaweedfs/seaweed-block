package authority

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// A stopped image is stable while operator metadata may still change HEAD.
// Neither a snapshot deletion nor a restore dependency invalidates its ACK.
func TestWorkspaceCleanMoveAfterStoppedSnapshotMetadataChanges(t *testing.T) {
	for _, op := range []string{"DeleteSnapshot", "AddDependent"} {
		t.Run(op, func(t *testing.T) {
			p := newLeasePublisher(t, &failingStore{}, time.Second, newTestClock())
			bindAndGrant(t, p, "v", "r")
			ctx := context.Background()
			if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
				t.Fatal(err)
			}
			if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7, Holder: "host/r", RemoteLSN: 42}, WorkspaceHead{Detached: true, Epoch: 1, Holder: "host/r", RemoteLSN: 42}); err != nil {
				t.Fatal(err)
			}
			root := layout.RootName{Hash: layout.Hash{1}}
			h := layout.Head{Gen: 7, Epoch: 1, Holder: "host/r", Position: 42, State: layout.StateDetached, Current: &root, Activity: layout.Activity{Kind: layout.ActIdle}, Snapshots: map[string]layout.Snapshot{"s": {Root: root, Position: 42}}}
			var next layout.Head
			var changed bool
			var err error
			if op == "DeleteSnapshot" {
				next, changed, err = h.DeleteSnapshot("s")
			} else {
				id, e := layout.NewVolumeID()
				if e != nil {
					t.Fatal(e)
				}
				next, changed, err = h.AddDependent("s", layout.Dependent{Target: "restore", TargetID: id}, root)
			}
			if err != nil || !changed {
				t.Fatalf("operator transition: changed=%t err=%v", changed, err)
			}
			next.Gen++ // the filer's successful conditional metadata write
			req := WorkspaceMoveRequest{RequestID: "clean", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "host", TargetHost: "next", ExpectedEpoch: 1, TargetDataAddr: "d", TargetCtrlAddr: "c", Kind: "clean"}
			grant, err := p.MoveWorkspace(ctx, req, WorkspaceHead{Gen: next.Gen, Epoch: next.Epoch, Holder: next.Holder, RemoteLSN: next.Position, Detached: next.State == layout.StateDetached})
			if err != nil || !grant.Issued {
				t.Fatalf("clean move after %s: grant=%+v err=%v; want admitted", op, grant, err)
			}
			if grant.HeadGen != next.Gen || grant.RemoteLSN != h.Position {
				t.Fatalf("readout lost: %+v", grant)
			}
		})
	}
}

// A generation is readout only on both ACK receipt and move admission.
func TestWorkspaceStopProofNeverBranchesOnGeneration(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "workspace.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != "AckWorkspaceRevocation" && fn.Name.Name != "prepareWorkspaceMoveLocked") {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			cond, ok := n.(*ast.IfStmt)
			if !ok {
				return true
			}
			ast.Inspect(cond.Cond, func(n ast.Node) bool {
				if s, ok := n.(*ast.SelectorExpr); ok && (s.Sel.Name == "HeadGen" || s.Sel.Name == "Gen") {
					t.Errorf("%s branches on generation instead of stop identity", fn.Name.Name)
				}
				return true
			})
			return true
		})
	}
}

func TestWorkspaceAckReplayKeepsOriginalGenerationReadout(t *testing.T) {
	p := newLeasePublisher(t, &failingStore{}, time.Second, newTestClock())
	bindAndGrant(t, p, "v", "r")
	ctx := context.Background()
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	ack := WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7, Holder: "host/r", RemoteLSN: 42}
	if err := p.AckWorkspaceRevocation(ctx, "v", ack, WorkspaceHead{Detached: true, Epoch: ack.Epoch, Holder: ack.Holder, RemoteLSN: ack.RemoteLSN}); err != nil {
		t.Fatal(err)
	}
	ack.HeadGen = 99
	if err := p.AckWorkspaceRevocation(ctx, "v", ack, WorkspaceHead{Detached: true, Epoch: ack.Epoch, Holder: ack.Holder, RemoteLSN: ack.RemoteLSN}); err != nil {
		t.Fatal("metadata-only ACK replay refused", err)
	}
	if p.WorkspaceState("v").Ack.HeadGen != 7 {
		t.Fatal("replay replaced the original generation readout")
	}
	ack.RemoteLSN++
	if err := p.AckWorkspaceRevocation(ctx, "v", ack, WorkspaceHead{Detached: true, Epoch: ack.Epoch, Holder: ack.Holder, RemoteLSN: ack.RemoteLSN}); err == nil {
		t.Fatal("different stopped image replay accepted")
	}
}

func TestWorkspaceAckValidatesTheObservedStoppedHead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		head  WorkspaceHead
		admit bool
	}{
		{"same stop, changed generation", WorkspaceHead{Gen: 99, Epoch: 1, Holder: "host/r", RemoteLSN: 42, Detached: true}, true},
		{"still live", WorkspaceHead{Epoch: 1, Holder: "host/r", RemoteLSN: 42}, false},
		{"foreign epoch", WorkspaceHead{Epoch: 2, Holder: "host/r", Detached: true}, false},
		{"foreign holder", WorkspaceHead{Epoch: 1, Holder: "other/r", Detached: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newLeasePublisher(t, &failingStore{}, time.Second, newTestClock())
			bindAndGrant(t, p, "v", "r")
			ctx := context.Background()
			if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
				t.Fatal(err)
			}
			err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, Holder: "host/r", HeadGen: 7, RemoteLSN: 999}, tc.head)
			if (err == nil) != tc.admit {
				t.Fatalf("admit=%t: %v", tc.admit, err)
			}
			ack := p.WorkspaceState("v").Ack
			if !tc.admit && ack != nil {
				t.Fatal("refusal persisted an ACK")
			}
			if tc.admit && (ack == nil || ack.RemoteLSN != tc.head.RemoteLSN) {
				t.Fatalf("ACK did not persist the authoritative position: %+v", ack)
			}
		})
	}
}

func TestWorkspaceCleanMoveRequiresTheAckedStopIdentity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		head  WorkspaceHead
		admit bool
	}{
		{"same stop, changed generation", WorkspaceHead{Gen: 99, Epoch: 1, Holder: "host/r", RemoteLSN: 42, Detached: true}, true},
		{"not detached", WorkspaceHead{Gen: 7, Epoch: 1, Holder: "host/r", RemoteLSN: 42}, false},
		{"other epoch", WorkspaceHead{Gen: 7, Epoch: 2, Holder: "host/r", RemoteLSN: 42, Detached: true}, false},
		{"other holder", WorkspaceHead{Gen: 7, Epoch: 1, Holder: "other/r", RemoteLSN: 42, Detached: true}, false},
		{"other position", WorkspaceHead{Gen: 7, Epoch: 1, Holder: "host/r", RemoteLSN: 43, Detached: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newLeasePublisher(t, &failingStore{}, time.Second, newTestClock())
			bindAndGrant(t, p, "v", "r")
			ctx := context.Background()
			if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
				t.Fatal(err)
			}
			if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7, Holder: "host/r", RemoteLSN: 42}, WorkspaceHead{Detached: true, Epoch: 1, Holder: "host/r", RemoteLSN: 42}); err != nil {
				t.Fatal(err)
			}
			req := WorkspaceMoveRequest{RequestID: "clean", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "host", TargetHost: "next", ExpectedEpoch: 1, TargetDataAddr: "d", TargetCtrlAddr: "c", Kind: "clean"}
			got, err := p.MoveWorkspace(ctx, req, tc.head)
			if (err == nil) != tc.admit || got.Issued != tc.admit {
				t.Fatalf("admit=%t: %+v %v", tc.admit, got, err)
			}
			if !tc.admit && len(p.WorkspaceState("v").Moves) != 0 {
				t.Fatal("refusal wrote a reservation")
			}
		})
	}
}
