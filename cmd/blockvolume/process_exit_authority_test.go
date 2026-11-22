package main

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"net"
	"testing"
	"time"
)

type exitAssignmentStream struct {
	control.UnimplementedAssignmentServiceServer
	control.UnimplementedObservationServiceServer
	facts chan *control.AssignmentFact
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
func exitAuthorityHost(t *testing.T) (*volume.Host, func(string, uint64)) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	stream := &exitAssignmentStream{facts: make(chan *control.AssignmentFact, 4)}
	control.RegisterAssignmentServiceServer(srv, stream)
	control.RegisterObservationServiceServer(srv, stream)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	h, err := volume.New(volume.Config{MasterAddr: ln.Addr().String(), ServerID: "s", VolumeID: "v", ReplicaID: "r", DataAddr: "127.0.0.1:1", CtrlAddr: "127.0.0.1:2"})
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
