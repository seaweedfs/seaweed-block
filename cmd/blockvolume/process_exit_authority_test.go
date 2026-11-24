package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	refuse   error // when set, every renewal is refused with it
	ackError error
	acks     atomic.Int32
	facts    chan *control.AssignmentFact
}

func (s *exitAssignmentStream) RenewWorkspaceLease(_ context.Context, id *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	if s.refuse != nil {
		return nil, s.refuse
	}
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
		stream.facts <- &control.AssignmentFact{VolumeId: "v", ReplicaId: replica, ServerId: "s", Epoch: epoch, EndpointVersion: 1}
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
		}{{"other", 6}}, 75},
		{"empty replica newer epoch", []struct {
			replica string
			epoch   uint64
		}{{"r", 5}, {"", 7}}, 75},
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

func (s *exitStartupOutcome) ChunkStartupSuperseded(string, uint64) bool { return false }
func (s *exitStartupOutcome) ChunkStartupOutcome(string) (uint64, bool)  { return 1, s.expired }
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

// The real provider must carry the claim's filer proof to the process owner,
// even when the master stream has never reported another replica.
type claimExitView struct{ assigned atomic.Bool }

func (v *claimExitView) Projection() frontend.Projection {
	if !v.assigned.Load() {
		return frontend.Projection{VolumeID: "v"}
	}
	return frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}
}
func TestWorkspaceForeignHeadClaimChoosesNoRetry(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		for _, tc := range []struct {
			name, holder string
			want         int
		}{
			{"foreign holder", "other", 78}, {"same holder higher epoch", "r", 75},
		} {
			t.Run(tc.name+map[bool]string{false: "/immediate", true: "/waiting"}[deferred], func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				h, _ := exitAuthorityHost(t)
				remote := storage.NewMemChunkRemote()
				if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 2, Holder: tc.holder, State: storage.ChunkHeadActive}); err != nil {
					t.Fatal(err)
				}
				view := &claimExitView{}
				view.assigned.Store(!deferred)
				p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, view)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := p.Close(); err != nil {
						t.Error(err)
					}
				})
				if _, err = p.EnsureStorage("v"); err != nil {
					t.Fatal(err)
				}
				_, err = p.RecoverVolume(ctx, "v")
				if deferred {
					if err != nil {
						t.Fatal(err)
					}
					view.assigned.Store(true)
				} else if !errors.Is(err, storage.ErrChunkLayerFenced) {
					t.Fatalf("claim refusal=%v", err)
				}
				select {
				case <-p.ChunkStartupUpdates():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if h.LastOtherLine() != nil {
					t.Fatal("fixture unexpectedly supplied master supersede evidence")
				}
				e := newProcessExit(ctx)
				if !chooseChunkStartupExit(e, flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v"}, h, p) {
					t.Fatal("claim refusal did not select an exit")
				}
				select {
				case out := <-e.decision:
					if out.code != tc.want {
						t.Fatalf("claim exit=%+v want %d", out, tc.want)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				head, err := remote.GetHead(ctx)
				if err != nil || head.Gen != 1 || head.Holder != tc.holder || remote.Puts() != 0 {
					t.Fatalf("refused claim mutated remote: %+v puts=%d err=%v", head, remote.Puts(), err)
				}
				for _, status := range p.DurableStatuses() {
					if status.Operational {
						t.Fatal("refused claim became operational")
					}
				}
			})
		}
	}
}

// Unlike a move BACK to an older foreign publication, this local root's
// assignment is older than the foreign HEAD: another holder has superseded it.
func TestWorkspaceOldLocalHolderForeignHeadChoosesNoRetry(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "immediate", true: "waiting"}[deferred], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			h, _ := exitAuthorityHost(t)
			root := t.TempDir()
			remote := storage.NewMemChunkRemote()
			view := &claimExitView{}
			view.assigned.Store(true)
			open := func() *durable.DurableProvider {
				t.Helper()
				p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, view)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Close() })
				if _, err := p.EnsureStorage("v"); err != nil {
					t.Fatal(err)
				}
				return p
			}
			first := open()
			if _, err := first.RecoverVolume(ctx, "v"); err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte{0x73}, 4096)
			if _, err := first.LogicalStorage("v").Write(0, want); err != nil {
				t.Fatal(err)
			}
			if _, err := first.LogicalStorage("v").Sync(); err != nil {
				t.Fatal(err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := remote.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{Gen: before.Gen, Epoch: before.Epoch}, storage.ChunkHead{Epoch: 2, Holder: "other", State: storage.ChunkHeadActive}); err != nil {
				t.Fatal(err)
			}
			view.assigned.Store(!deferred)
			second := open()
			_, err = second.RecoverVolume(ctx, "v")
			if deferred {
				if err != nil {
					t.Fatal(err)
				}
				view.assigned.Store(true)
				select {
				case <-second.ChunkStartupUpdates():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			} else if !errors.Is(err, storage.ErrChunkLayerFenced) {
				t.Fatalf("old root refusal=%v", err)
			}
			e := newProcessExit(ctx)
			if !chooseChunkStartupExit(e, flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v"}, h, second) {
				t.Fatal("foreign HEAD attach refusal retained the superseded process")
			}
			if out := <-e.decision; out.code != 78 {
				t.Fatalf("old holder exit=%+v", out)
			}
			if got, err := second.LogicalStorage("v").Read(0); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("refusal lost local acknowledged bytes: %v", err)
			}
			after, err := remote.GetHead(ctx)
			if err != nil || after.Gen != before.Gen+1 || after.Holder != "other" || remote.Puts() != 0 {
				t.Fatalf("refusal mutated HEAD/payload: %+v puts=%d err=%v", after, remote.Puts(), err)
			}

		})
	}
}

// The real claim reads E1/A; a late E2/C wins before E3/B's CAS.
// The displaced HEAD is a fence for this attempt, not proof E3 was superseded.
type displacedClaimExitRemote struct {
	storage.ChunkRemote
	epoch uint64
	raced bool
}

func (r *displacedClaimExitRemote) CASHead(ctx context.Context, prev storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	if !r.raced {
		r.raced = true
		late := storage.ChunkHead{Epoch: r.epoch, Holder: "C", State: storage.ChunkHeadActive}
		if _, err := r.ChunkRemote.CASHead(ctx, prev, late); err != nil {
			return 0, err
		}
	}
	return r.ChunkRemote.CASHead(ctx, prev, next)
}

type displacedClaimExitView struct{}

func (displacedClaimExitView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v", ReplicaID: "B", Epoch: 3, Healthy: true}
}
func TestWorkspaceDisplacedClaimUsesAssignedEpoch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		epoch uint64
		want  int
	}{
		{"late lower epoch", 2, 75}, {"equal epoch foreign holder", 3, 78}, {"higher epoch foreign holder", 4, 78},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			h, _ := exitAuthorityHost(t)
			base := storage.NewMemChunkRemote()
			if _, err := base.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "A", State: storage.ChunkHeadActive}); err != nil {
				t.Fatal(err)
			}
			remote := &displacedClaimExitRemote{ChunkRemote: base, epoch: tc.epoch}
			p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, displacedClaimExitView{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := p.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err = p.EnsureStorage("v"); err != nil {
				t.Fatal(err)
			}
			if _, err = p.RecoverVolume(ctx, "v"); !errors.Is(err, storage.ErrChunkLayerFenced) {
				t.Fatalf("claim error=%v", err)
			}
			if !remote.raced {
				t.Fatal("controlled CAS did not run")
			}
			e := newProcessExit(ctx)
			if !chooseChunkStartupExit(e, flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v"}, h, p) {
				t.Fatal("fenced claim did not choose an exit")
			}
			select {
			case out := <-e.decision:
				if out.code != tc.want {
					t.Fatalf("displaced E%d/C at assigned E3/B chose %+v, want %d", tc.epoch, out, tc.want)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			head, err := base.GetHead(ctx)
			if err != nil || head.Gen != 2 || head.Epoch != tc.epoch || head.Holder != "C" || base.Puts() != 0 {
				t.Fatalf("refused claim mutated HEAD/payload: %+v puts=%d err=%v", head, base.Puts(), err)
			}
			for _, status := range p.DurableStatuses() {
				if status.Operational {
					t.Fatal("displaced claim served I/O")
				}
			}
		})
	}
}

// A typed MOVED or DELETED renewal refusal, seen by the real host's guard,
// turns the startup outcome into 78 with that reason; a refusal without the
// detail stays 75 assignment_timeout.
func TestChunkStartupExitFinalGives78(t *testing.T) {
	refusal := func(kind control.WorkspaceLeaseRefusal_Kind) error {
		st, err := status.New(codes.FailedPrecondition, "workspace lease: server identity does not match the current holder").
			WithDetails(&control.WorkspaceLeaseRefusal{Kind: kind})
		if err != nil {
			t.Fatal(err)
		}
		return st.Err()
	}
	for _, tc := range []struct {
		name    string
		refuse  error
		foreign bool // the stream also delivered a newer line naming another replica
		code    int
		reason  string
	}{
		{"moved", refusal(control.WorkspaceLeaseRefusal_MOVED), false, exitWorkspaceSuperseded, "moved"},
		{"moved, newer foreign line also seen", refusal(control.WorkspaceLeaseRefusal_MOVED), true, exitWorkspaceSuperseded, "moved"},
		{"deleted", refusal(control.WorkspaceLeaseRefusal_DELETED), false, exitWorkspaceSuperseded, "deleted"},
		{"no detail", status.Error(codes.FailedPrecondition, "workspace lease: server identity does not match the current holder"), false, exitWorkspaceRetry, "assignment_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			h, assign := exitAuthorityHost(t, func(s *exitAssignmentStream) { s.refuse = tc.refuse })
			assign("r", 1)
			leaseCtx, stopLease := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() {
				done <- h.RunWorkspaceLease(leaseCtx, func(context.Context, *control.WorkspaceIdentity) (uint64, error) { return 1, nil })
			}()
			t.Cleanup(func() { stopLease(); <-done })
			if tc.reason == "assignment_timeout" {
				time.Sleep(2 * time.Second) // a renewal or two, refused without a detail
				if _, final := h.MasterWaitState(); final != "" {
					t.Fatalf("a refusal without a detail was taken as final %q", final)
				}
			}
			for tc.reason != "assignment_timeout" {
				if _, final := h.MasterWaitState(); final != "" {
					if tc.foreign {
						assign("r2", 2)
						if !h.IsCurrentReplicaSuperseded() {
							t.Fatal("fixture: the newer foreign line does not supersede")
						}
					}
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("the guard's refusal never reached the host")
				case <-time.After(20 * time.Millisecond):
				}
			}
			e := newProcessExit(ctx)
			if !chooseChunkStartupExit(e, flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v"}, h, &exitStartupOutcome{expired: true}) {
				t.Fatal("the startup outcome selected no exit")
			}
			out := <-e.decision
			if out.code != tc.code || out.reason != tc.reason {
				t.Fatalf("exit %+v; want %d %s", out, tc.code, tc.reason)
			}
		})
	}
}
