package master

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkspaceMoveUnknownPrecedesAdmissionRefusals(t *testing.T) {
	for _, duringRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "already_uncertain", true: "uncertain_during_HEAD"}[duringRead], func(t *testing.T) {
			var inject atomic.Bool
			var headFailure atomic.Int32
			var childDeadlineObserved atomic.Bool
			var latch func() error
			var cancelRequest context.CancelFunc
			h, c, ctx := reviewBindingHostWithReader(t, func(readCtx context.Context, _ string) (authority.WorkspaceHead, error) {
				if inject.Swap(false) {
					if err := latch(); err != nil {
						return authority.WorkspaceHead{}, err
					}
				}
				switch headFailure.Load() {
				case 1:
					return authority.WorkspaceHead{}, errors.New("test HEAD unavailable")
				case 2:
					// Expire the service's actual child read context. The
					// request parent below stays live indefinitely.
					<-readCtx.Done()
					childDeadlineObserved.Store(readCtx.Err() == context.DeadlineExceeded)
					return authority.WorkspaceHead{}, readCtx.Err()
				case 3:
					dependency, cancel := context.WithCancel(readCtx)
					cancel()
					return authority.WorkspaceHead{}, dependency.Err()
				case 4:
					dependency, cancel := context.WithDeadline(readCtx, time.Unix(0, 1))
					defer cancel()
					cancelRequest()
					return authority.WorkspaceHead{}, dependency.Err()
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
			headFailure.Store(1)
			if _, err := client.MoveWorkspace(ctx, request("clean")); status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "UNKNOWN") {
				t.Errorf("early HEAD error hid uncertainty: %v", err)
			}
			liveParent := context.Background()
			for _, mode := range []int32{2, 3} {
				headFailure.Store(mode)
				_, err := (&services{host: h}).MoveWorkspace(auth.DevCall(liveParent), request("clean"))
				if liveParent.Err() != nil || (mode == 2 && !childDeadlineObserved.Load()) {
					t.Fatal("dependency-end witness did not preserve live request")
				}
				if status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "UNKNOWN") || !strings.Contains(status.Convert(err).Message(), "same request id") {
					t.Errorf("live-parent HEAD dependency end bypassed UNKNOWN: mode=%d err=%v", mode, err)
				}
			}
			caller, cancelCaller := context.WithCancel(context.Background())
			cancelRequest = cancelCaller
			headFailure.Store(4)
			_, err := (&services{host: h}).MoveWorkspace(auth.DevCall(caller), request("clean"))
			cancelCaller()
			if caller.Err() != context.Canceled || status.Code(err) != codes.Canceled {
				t.Errorf("actual caller end lost to dependency status: %v", err)
			}
			// These direct calls inspect the server result, not the client's own
			// deadline override, so reply classification must preserve both.
			for _, deadline := range []bool{false, true} {
				var ended context.Context
				var end context.CancelFunc
				want := codes.Canceled
				if deadline {
					ended, end = context.WithDeadline(ctx, time.Unix(0, 1))
					want = codes.DeadlineExceeded
				} else {
					ended, end = context.WithCancel(ctx)
					end()
				}
				_, err := (&services{host: h}).MoveWorkspace(auth.DevCall(ended), request("forced"))
				end()
				if status.Code(err) != want {
					t.Errorf("server caller-end classification changed: %v want %v", err, want)
				}
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
