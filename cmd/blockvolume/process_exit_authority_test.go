package main

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type exitAssignmentStream struct {
	control.UnimplementedAssignmentServiceServer
	control.UnimplementedObservationServiceServer
	control.UnimplementedWorkspaceServiceServer
	revoke   bool
	ackError error
	acks     atomic.Int32
	facts    chan *control.AssignmentFact
}

func (s *exitAssignmentStream) RenewWorkspaceLease(_ context.Context, id *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	return &control.WorkspaceLeaseFact{VolumeId: id.VolumeId, ReplicaId: id.ReplicaId, Epoch: id.Epoch, ValidForNs: int64(30 * time.Second), Revoking: s.revoke}, nil
}
func (s *exitAssignmentStream) AckWorkspaceRevoked(_ context.Context, a *control.WorkspaceRevocationAck) (*control.WorkspaceEmpty, error) {
	if a.GetHeadGen() != 1 {
		return nil, errors.New("ACK lacks the stored detached proof")
	}
	s.acks.Add(1)
	return &control.WorkspaceEmpty{}, s.ackError
}
func (s *exitAssignmentStream) ReportHeartbeat(context.Context, *control.HeartbeatReport) (*control.HeartbeatAck, error) {
	return &control.HeartbeatAck{}, nil
}
func (s *exitAssignmentStream) SubscribeAssignments(_ *control.SubscribeRequest, stream control.AssignmentService_SubscribeAssignmentsServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case f := <-s.facts:
			if err := stream.Send(f); err != nil {
				return err
			}
		}
	}
}
func exitAuthorityHost(t *testing.T, configure ...func(*exitAssignmentStream)) (*volume.Host, func(string, uint64)) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	stream := &exitAssignmentStream{facts: make(chan *control.AssignmentFact, 4)}
	for _, f := range configure {
		f(stream)
	}
	control.RegisterWorkspaceServiceServer(srv, stream)
	control.RegisterAssignmentServiceServer(srv, stream)
	control.RegisterObservationServiceServer(srv, stream)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	h, err := volume.New(volume.Config{WorkspaceLease: true, MasterAddr: ln.Addr().String(), ServerID: "s", VolumeID: "v", ReplicaID: "r", DataAddr: "127.0.0.1:1", CtrlAddr: "127.0.0.1:2"})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	})
	return h, func(replica string, epoch uint64) {
		t.Helper()
		stream.facts <- &control.AssignmentFact{VolumeId: "v", ReplicaId: replica, Epoch: epoch, EndpointVersion: 1}
		deadline := time.NewTimer(3 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			ready := false
			if replica == "r" {
				ready = h.Adapter().Projection().Epoch == epoch
			} else {
				f := h.LastOtherLine()
				ready = f != nil && f.ReplicaId == replica && f.Epoch == epoch
			}
			if ready {
				return
			}
			select {
			case <-deadline.C:
				t.Fatal("assignment observation did not arrive")
			case <-tick.C:
			}
		}
	}
}

// Exercise the real Host subscription and the process stop seam, including a
// historical other-replica record retained after the master assigns us again.
func TestWorkspaceRetryUsesNewestOwnAssignment(t *testing.T) {
	for _, tc := range []struct {
		name  string
		facts []struct {
			replica string
			epoch   uint64
		}
		want int
	}{
		{"old other then own", []struct {
			replica string
			epoch   uint64
		}{{"other", 6}, {"r", 7}}, 75},
		{"own then newer other", []struct {
			replica string
			epoch   uint64
		}{{"r", 5}, {"other", 6}}, 78},
		{"never assigned", []struct {
			replica string
			epoch   uint64
		}{{"other", 6}}, 78},
		{"same replica higher epoch", []struct {
			replica string
			epoch   uint64
		}{{"r", 5}, {"r", 7}}, 75},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, send := exitAuthorityHost(t)
			for _, f := range tc.facts {
				send(f.replica, f.epoch)
			}
			e := newProcessExit(context.Background())
			done := make(chan error, 1)
			done <- errors.New("lease ended")
			waitWorkspaceStop(e, flags{volumeID: "v", replicaID: "r"}, h, nil, done)
			if out := <-e.decision; out.code != tc.want {
				t.Fatalf("lease end classified %+v, want%d", out, tc.want)
			}
		})
	}
}
func TestWorkspaceUnconfirmedRevokeHasOwnReason(t *testing.T) {
	h, _ := exitAuthorityHost(t)
	e := newProcessExit(context.Background())
	done := make(chan error, 1)
	done <- errors.New("ACK answer lost")
	finishWorkspaceLease(e, flags{volumeID: "v", replicaID: "r"}, h, done)
	if out := <-e.decision; out.code != 75 || out.reason != "revoke_ack_unconfirmed" {
		t.Fatalf("unconfirmed ACK classified %+v", out)
	}
}

// Controlled provider outcome at its public generation/selection seam. The
// real provider deadline/retained-state boundary is tested in durable.
type exitStartupOutcome struct{ expired bool }

func (s *exitStartupOutcome) ChunkStartupOutcome(string) (uint64, bool) { return 1, s.expired }
func (s *exitStartupOutcome) ChooseChunkStartupExit(_ string, gen uint64, choose func()) bool {
	if gen != 1 || !s.expired {
		return false
	}
	choose()
	return true
}

func TestWorkspacePlannedDrainOwnsStartupExpiryUntilAck(t *testing.T) {
	for _, failAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "ACK succeeds", true: "ACK lost"}[failAck], func(t *testing.T) {
			var stream *exitAssignmentStream
			h, send := exitAuthorityHost(t, func(s *exitAssignmentStream) {
				stream = s
				s.revoke = true
				if failAck {
					s.ackError = errors.New("ACK answer lost")
				}
			})
			send("r", 1)
			ctx, cancel := context.WithCancel(context.Background())
			doneConsumed := false
			requests, done := startWorkspaceLease(ctx, h)
			t.Cleanup(func() {
				cancel()
				if !doneConsumed {
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("lease worker did not join")
					}
				}
			})
			e := newProcessExit(ctx)
			f := flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v", replicaID: "r"}
			// The real guard sets revoked, sends its stop request, and waits for the
			// resource owner's drain reply. No success is selected yet.
			request, leaseDone := waitWorkspaceStop(e, f, h, requests, done)
			if request == nil || !h.WorkspaceLeaseRevoking() {
				t.Fatal("real planned drain not acquired")
			}
			// Deliver expiry only after the real guard's planned drain owns
			// shutdown, while its resource-owner reply is deliberately held.
			p := &exitStartupOutcome{expired: true}
			chooseChunkStartupExit(e, f, h, p)
			select {
			case out := <-e.decision:
				t.Fatalf("startup overrode the pending real drain: %+v", out)
			default:
			}
			remote := storage.NewMemChunkRemote()
			_, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "s/r", State: storage.ChunkHeadDetached, DetachedBy: &storage.ChunkDetachedBy{Epoch: 1, Holder: "s/r", Gen: 1}})
			if err != nil {
				t.Fatal(err)
			}
			gen, err := workspaceDetachedGeneration(ctx, func(string) (storage.ChunkRemote, error) { return remote, nil }, request.id, "s/r")
			if err != nil {
				t.Fatal(err)
			}
			request.reply <- workspaceStopResult{gen: gen}
			finishWorkspaceLease(e, f, h, leaseDone)
			doneConsumed = true
			out := <-e.decision
			if stream.acks.Load() != 1 {
				t.Fatal("real guard did not send proof ACK")
			}
			if !failAck && out.code != 0 {
				t.Fatalf("ACKed stop restarted: %+v", out)
			}
			if failAck && (out.code != 75 || out.reason != "revoke_ack_unconfirmed") {
				t.Fatalf("unconfirmed ACK: %+v", out)
			}
		})
	}
}
