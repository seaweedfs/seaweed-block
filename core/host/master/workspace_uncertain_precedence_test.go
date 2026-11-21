package master

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"sync/atomic"
	"testing"
)

func TestWorkspaceMoveUnknownPrecedesAdmissionRefusals(t *testing.T) {
	for _, duringRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "already_uncertain", true: "uncertain_during_HEAD"}[duringRead], func(t *testing.T) {
			var inject, failRead atomic.Bool
			var latch func() error
			h, c, ctx := reviewBindingHostWithReader(t, func(context.Context, string) (authority.WorkspaceHead, error) {
				if inject.Swap(false) {
					if err := latch(); err != nil {
						return authority.WorkspaceHead{}, err
					}
				}
				if failRead.Load() {
					return authority.WorkspaceHead{}, errors.New("test HEAD unavailable")
				}
				return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}, nil
			})
			reviewLegacyBind(t, h, c, ctx, "v")
			if _, err := h.Publisher().RenewWorkspaceLease(ctx, "v", "r1", 1); err != nil {
				t.Fatal(err)
			}
			client := control.NewWorkspaceServiceClient(c)
			request := func(kind string) *control.WorkspaceMove {
				return &control.WorkspaceMove{VolumeId: "v", RequestId: "precedence-" + kind, ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "missing-node", TargetDataAddr: "127.0.0.1:29901", TargetCtrlAddr: "127.0.0.1:29902", Kind: kind, Force: kind == "forced"}
			}
			for _, kind := range []string{"clean", "forced"} {
				if _, err := client.MoveWorkspace(ctx, request(kind)); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("healthy admission control: %v", err)
				}
			}
			faults := atomic.Int64{}
			injected := errors.New("test persistence reply lost after rename")
			authority.SetPutAfterRenameFaultForTest(func(r authority.DurableRecord) error {
				if r.Workspace != nil && r.Workspace.Lease != nil && r.Workspace.Lease.Revoking {
					faults.Add(1)
					return injected
				}
				return nil
			})
			t.Cleanup(func() { authority.SetPutAfterRenameFaultForTest(nil) })
			latch = func() error {
				err := h.Publisher().BeginWorkspaceRevocation(ctx, "v", "r1", 1)
				if !errors.Is(err, injected) {
					return errors.New("test failed to latch uncertain persistence")
				}
				return nil
			}
			if duringRead {
				inject.Store(true)
			} else if err := latch(); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"clean", "forced"} {
				q := request(kind)
				if _, err := client.MoveWorkspace(ctx, q); status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "UNKNOWN") || !strings.Contains(status.Convert(err).Message(), q.RequestId) {
					t.Errorf("uncertain %s move answered definite refusal: %v", kind, err)
				}
			}
			failRead.Store(true)
			if _, err := client.MoveWorkspace(ctx, request("clean")); status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "UNKNOWN") {
				t.Errorf("early HEAD error hid uncertainty: %v", err)
			}
			authority.SetPutAfterRenameFaultForTest(nil)
			state := h.Publisher().WorkspaceState("v")
			line, ok := h.Publisher().VolumeAuthorityLine("v")
			if faults.Load() != 1 || state.Lease.Revoking || len(state.Moves) != 0 || !ok || line.Epoch != 1 || line.ReplicaID != "r1" {
				t.Fatalf("unknown requests changed authority: faults=%d state=%+v line=%+v", faults.Load(), state, line)
			}
		})
	}
}
