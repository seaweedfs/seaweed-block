package master

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// lostFirstAckReply loses only the first ACK's answer, after the real master
// committed it; later ACKs pass through.
type lostFirstAckReply struct {
	control.WorkspaceServiceClient
	calls *atomic.Int32
}

func (c lostFirstAckReply) AckWorkspaceRevoked(ctx context.Context, q *control.WorkspaceRevocationAck, opts ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	reply, err := c.WorkspaceServiceClient.AckWorkspaceRevoked(ctx, q, opts...)
	if c.calls.Add(1) == 1 && err == nil {
		return nil, status.Error(codes.DeadlineExceeded, "injected: the first ACK's answer lost after the master committed it")
	}
	return reply, err
}

// A real guard against the real master: the first revocation ACK gets no
// answer, (a) because its answer is lost after the master committed it, or
// (b) because the master's first HEAD read is slower than the guard's 5 s
// ACK bound. The guard asks again; the master answers the repeat (a: as
// already recorded; b: by recording it), so the guard's drain ends without
// error (the process exits 0, not 75) and the master holds exactly one ACK,
// with the holder's identity, the stopped generation and the HEAD position.
// On the parent commit (one attempt) both cases end with DeadlineExceeded.
func TestWorkspaceRevocationAckRetryAgainstTheRealMaster(t *testing.T) {
	for _, c := range []string{"answer lost after commit", "slow first HEAD read"} {
		t.Run(c, func(t *testing.T) {
			head := authority.WorkspaceHead{Gen: 99, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}
			var reads atomic.Int32
			reader := func(ctx context.Context, _ string) (authority.WorkspaceHead, error) {
				if reads.Add(1) == 1 && c == "slow first HEAD read" {
					time.Sleep(volume.WorkspaceAckTimeout + time.Second) // past the guard's bound
				}
				return head, nil
			}
			h, conn, ctx := reviewBindingHostBeforeStart(t, reader, nil)
			reviewLegacyBind(t, h, conn, ctx, "v")
			ws := control.NewWorkspaceServiceClient(conn)
			var acks atomic.Int32
			var client interface {
				RenewWorkspaceLease(context.Context, *control.WorkspaceIdentity, ...grpc.CallOption) (*control.WorkspaceLeaseFact, error)
				AckWorkspaceRevoked(context.Context, *control.WorkspaceRevocationAck, ...grpc.CallOption) (*control.WorkspaceEmpty, error)
			} = countingAck{ws, &acks}
			if c == "answer lost after commit" {
				client = lostFirstAckReply{ws, &acks}
			}
			id := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "v", ReplicaId: "r1", Epoch: 1}
			g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "v", ReplicaID: "r1", Epoch: 1, Healthy: true}}, client, id,
				func(context.Context) (uint64, error) { return 7, nil }, nil)
			if err != nil {
				t.Fatal(err)
			}
			runCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := g.Refresh(runCtx); err != nil {
				t.Fatal(err)
			}
			if _, err := ws.RevokeWorkspace(ctx, id); err != nil {
				t.Fatal(err)
			}
			if err := g.Refresh(runCtx); err != nil {
				t.Fatalf("the revocation's drain ended with %v; want a confirmed ACK after asking again", err)
			}
			if n := acks.Load(); n != 2 {
				t.Fatalf("%d ACK calls; want 2 (the unanswered one and its repeat)", n)
			}
			ack := h.Publisher().WorkspaceState("v").Ack
			if ack == nil || ack.ReplicaID != "r1" || ack.Epoch != 1 || ack.Holder != head.Holder || ack.RemoteLSN != head.RemoteLSN || ack.HeadGen != 7 {
				t.Fatalf("recorded ACK %+v; want r1@1 by %s at LSN %d, gen 7", ack, head.Holder, head.RemoteLSN)
			}
		})
	}
}

// countingAck counts the guard's ACK calls to the real master.
type countingAck struct {
	control.WorkspaceServiceClient
	calls *atomic.Int32
}

func (c countingAck) AckWorkspaceRevoked(ctx context.Context, q *control.WorkspaceRevocationAck, opts ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	c.calls.Add(1)
	return c.WorkspaceServiceClient.AckWorkspaceRevoked(ctx, q, opts...)
}
