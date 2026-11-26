package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
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
	revoke       bool
	refuse       error // when set, every renewal is refused with it
	streamRefuse error // the master may refuse a restarted source before an assignment
	// identityRefusals: the first this many stream calls are refused with
	// the typed IDENTITY detail.
	identityRefusals atomic.Int32
	ackError         error
	// ackFirstError, when set, answers only the first ACK (counted, not
	// recorded) with it; later ones get ackError.
	ackFirstError error
	acks          atomic.Int32
	facts            chan *control.AssignmentFact
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
	if s.acks.Add(1) == 1 && s.ackFirstError != nil {
		return nil, s.ackFirstError
	}
	return &control.WorkspaceEmpty{}, s.ackError
}
func (s *exitAssignmentStream) ReportHeartbeat(context.Context, *control.HeartbeatReport) (*control.HeartbeatAck, error) {
	return &control.HeartbeatAck{}, nil
}
func (s *exitAssignmentStream) SubscribeAssignments(_ *control.SubscribeRequest, stream control.AssignmentService_SubscribeAssignmentsServer) error {
	if s.streamRefuse != nil {
		return s.streamRefuse
	}
	if s.identityRefusals.Add(-1) >= 0 {
		st, _ := status.New(codes.FailedPrecondition, `assignment stream: server id "s" is not registered`).
			WithDetails(&control.WorkspaceLeaseRefusal{Kind: control.WorkspaceLeaseRefusal_IDENTITY, Identity: "unregistered"})
		return st.Err()
	}
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

func TestRestartedMovedSourceExits78WithoutAnOwnLine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	refusal, err := status.New(codes.FailedPrecondition, "workspace moved to holder node-b at epoch 8").
		WithDetails(&control.WorkspaceLeaseRefusal{Kind: control.WorkspaceLeaseRefusal_MOVED})
	if err != nil {
		t.Fatal(err)
	}
	h, _ := exitAuthorityHost(t, func(s *exitAssignmentStream) { s.streamRefuse = refusal.Err() })
	for {
		if _, final := h.MasterWaitState(); final == "moved" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the master's typed MOVED never reached the restarted source")
		case <-time.After(time.Millisecond):
		}
	}
	if h.Adapter().Projection().Epoch != 0 || h.IsCurrentReplicaSuperseded() {
		t.Fatal("fixture acquired an own line or inferred supersession")
	}
	e := newProcessExit(ctx)
	if !chooseChunkStartupExit(e, flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v"}, h, &exitStartupOutcome{expired: true}) {
		t.Fatal("no startup decision")
	}
	if out := <-e.decision; out.code != 78 || out.reason != "moved" || out.detail != refusal.Message() {
		t.Fatalf("moved restart chose %+v; want 78 moved, never retry 75", out)
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
			// The real proof reader supplies the generation to the real lease
			// worker; a constant here would not join the two safety checks.
			me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
			st := layoutProofStore{h: layout.Head{Gen: 1, Epoch: 1, Holder: "s/r", ClaimID: "k", State: layout.StateDetached, Activity: layout.Activity{Kind: layout.ActIdle}}, found: true}
			gen, proofErr := workspaceLayoutDetachedGeneration(request.ctx, st, request.id, me)
			if proofErr != nil {
				t.Fatal(proofErr)
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
type claimExitView struct {
	assigned atomic.Bool
	epoch    uint64
}

func (v *claimExitView) Projection() frontend.Projection {
	if !v.assigned.Load() {
		return frontend.Projection{VolumeID: "v"}
	}
	epoch := v.epoch
	if epoch == 0 {
		epoch = 1
	}
	return frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: epoch, Healthy: true}
}

// A layout HEAD held by another server at the granted epoch is a final
// refusal. The master has not delivered a newer foreign assignment, so the
// process must derive its exit from the provider's observed HEAD.
func TestWorkspaceForeignHeadClaimChoosesNoRetry(t *testing.T) {
	for _, tc := range []struct {
		name       string
		headEpoch  uint64
		grantEpoch uint64
		deferred   bool
		wantExit   bool
	}{
		{"foreign holder at granted epoch", 1, 1, false, true},
		{"foreign holder above granted epoch", 2, 1, false, true},
		{"foreign holder after assignment", 1, 1, true, true},
		{"foreign holder below granted epoch", 1, 2, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testWorkspaceLayoutForeignClaimExit(t, tc.headEpoch, tc.grantEpoch, tc.deferred, tc.wantExit)
		})
	}
}

func testWorkspaceLayoutForeignClaimExit(t *testing.T, headEpoch, grantEpoch uint64, deferred, wantExit bool) {
	t.Helper()
	ctx := context.Background()
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v", ID: id}
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), &maintenanceBlobs{d: map[string][]byte{}}, "/exit-layout")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	st, err := cat.Store(ref)
	if err != nil {
		t.Fatal(err)
	}
	foreign, found, err := st.ReadHead(ctx)
	if err != nil || !found {
		t.Fatalf("initial HEAD: found=%v err=%v", found, err)
	}
	foreign.Epoch, foreign.Holder, foreign.ClaimID = headEpoch, "other/r", "other-claim"
	if err := st.UpdateHead(ctx, foreign.Gen, foreign); err != nil {
		t.Fatal(err)
	}
	foreign, _, err = st.ReadHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	view := &claimExitView{epoch: grantEpoch}
	view.assigned.Store(!deferred)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r",
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return st, nil },
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			return chunkstore.NewLayoutRemote(cat, ref, me)
		},
	}
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.EnsureStorage("v"); err != nil {
		t.Fatal(err)
	}
	_, err = p.RecoverVolume(ctx, "v")
	if deferred {
		if err != nil {
			t.Fatalf("recovery before assignment: %v", err)
		}
		view.assigned.Store(true)
		select {
		case <-p.ChunkStartupUpdates():
		case <-time.After(3 * time.Second):
			t.Fatal("foreign layout HEAD did not end deferred startup")
		}
	} else if wantExit && !errors.Is(err, layout.ErrNotHolder) && !errors.Is(err, layout.ErrStaleEpoch) {
		t.Fatalf("layout Start refusal: %v; want not-holder or stale-epoch", err)
	}
	if !wantExit {
		if err != nil {
			t.Fatalf("newer grant did not claim: %v", err)
		}
		if _, ended := p.ChunkStartupOutcome("v"); ended {
			t.Fatal("newer grant ended startup")
		}
		claimed, _, err := st.ReadHead(ctx)
		if err != nil || claimed.Epoch != grantEpoch || claimed.Holder != "s/r" || claimed.Gen <= foreign.Gen {
			t.Fatalf("newer grant did not claim HEAD: %+v err=%v", claimed, err)
		}
		return
	}
	if _, ended := p.ChunkStartupOutcome("v"); !ended {
		t.Fatal("foreign layout HEAD did not end startup")
	}
	h, _ := exitAuthorityHost(t)
	e := newProcessExit(ctx)
	if !chooseChunkStartupExit(e, flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v"}, h, p) {
		t.Fatal("foreign layout HEAD did not select an exit")
	}
	if out := <-e.decision; out.code != 78 {
		t.Fatalf("foreign layout HEAD chose %+v; want 78", out)
	}
	after, _, err := st.ReadHead(ctx)
	if err != nil || after.Gen != foreign.Gen || after.Holder != foreign.Holder || after.ClaimID != foreign.ClaimID {
		t.Fatalf("refused Start changed HEAD: before=%+v after=%+v err=%v", foreign, after, err)
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
			id, err := layout.NewVolumeID()
			if err != nil {
				t.Fatal(err)
			}
			ref := chunkstore.VolumeRef{Name: "v", ID: id}
			blobs := &maintenanceBlobs{d: map[string][]byte{}}
			cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), blobs, "/exit-layout")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
				t.Fatal(err)
			}
			st, err := cat.Store(ref)
			if err != nil {
				t.Fatal(err)
			}
			view := &claimExitView{}
			view.assigned.Store(true)
			open := func() *durable.DurableProvider {
				t.Helper()
				cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r",
					ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return st, nil },
					ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
						return chunkstore.NewLayoutRemote(cat, ref, me)
					},
				}
				p, err := durable.NewDurableProvider(cfg, view)
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
			before, found, err := st.ReadHead(ctx)
			if err != nil || !found {
				t.Fatalf("claimed layout HEAD: found=%v err=%v", found, err)
			}
			foreign := before
			foreign.Epoch, foreign.Holder, foreign.ClaimID = 2, "other/r", "other-claim"
			if err := st.UpdateHead(ctx, before.Gen, foreign); err != nil {
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
			} else if !errors.Is(err, layout.ErrStaleEpoch) {
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
			after, found, err := st.ReadHead(ctx)
			if err != nil || !found || after.Gen != before.Gen+1 || after.Holder != "other/r" || after.ClaimID != "other-claim" || blobs.n != 0 {
				t.Fatalf("refusal mutated HEAD/payload: %+v found=%v puts=%d err=%v", after, found, blobs.n, err)
			}

		})
	}
}

type displacedClaimExitView struct{}

func (displacedClaimExitView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v", ReplicaID: "B", Epoch: 3, Healthy: true}
}

type displacedLayoutClaimStore struct {
	layout.StartStore
	epoch uint64
	raced bool
}

func (s *displacedLayoutClaimStore) UpdateHead(ctx context.Context, expected uint64, next layout.Head) error {
	if !s.raced {
		s.raced = true
		current, found, err := s.StartStore.ReadHead(ctx)
		if err != nil || !found {
			return errors.New("racing layout HEAD unavailable")
		}
		late := current
		late.Epoch, late.Holder, late.ClaimID = s.epoch, "C", "late-claim"
		if err := s.StartStore.UpdateHead(ctx, current.Gen, late); err != nil {
			return err
		}
	}
	return s.StartStore.UpdateHead(ctx, expected, next)
}

// The claim's conditional update loses to another holder. A re-read at or
// above the grant proves a final exit; below it, a new Start can claim.
func TestWorkspaceDisplacedClaimUsesAssignedEpoch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		epoch uint64
		final bool
	}{
		{"late lower epoch", 2, false},
		{"equal epoch foreign holder", 3, true},
		{"higher epoch foreign holder", 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			id, err := layout.NewVolumeID()
			if err != nil {
				t.Fatal(err)
			}
			ref := chunkstore.VolumeRef{Name: "v", ID: id}
			cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), &maintenanceBlobs{d: map[string][]byte{}}, "/exit-layout")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
				t.Fatal(err)
			}
			base, err := cat.Store(ref)
			if err != nil {
				t.Fatal(err)
			}
			initial, found, err := base.ReadHead(ctx)
			if err != nil || !found {
				t.Fatalf("initial HEAD: found=%v err=%v", found, err)
			}
			initial.Epoch, initial.Holder, initial.ClaimID = 1, "A", "first-claim"
			if err := base.UpdateHead(ctx, initial.Gen, initial); err != nil {
				t.Fatal(err)
			}
			store := &displacedLayoutClaimStore{StartStore: base, epoch: tc.epoch}
			cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "B",
				ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return store, nil },
				ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
					return chunkstore.NewLayoutRemote(cat, ref, me)
				},
			}
			p, err := durable.NewDurableProvider(cfg, displacedClaimExitView{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Close() })
			if _, err := p.EnsureStorage("v"); err != nil {
				t.Fatal(err)
			}
			if _, err := p.RecoverVolume(ctx, "v"); !errors.Is(err, layout.ErrPrecondition) {
				t.Fatalf("claim race: %v; want HEAD precondition", err)
			}
			if !store.raced {
				t.Fatal("controlled HEAD race did not run")
			}
			if tc.final {
				h, _ := exitAuthorityHost(t)
				e := newProcessExit(ctx)
				if !chooseChunkStartupExit(e, flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v"}, h, p) {
					t.Fatal("displaced layout claim did not choose an exit")
				}
				if out := <-e.decision; out.code != 78 {
					t.Fatalf("displaced claim chose %+v; want 78", out)
				}
			} else if _, ended := p.ChunkStartupOutcome("v"); ended {
				t.Fatal("a lower racing epoch ended startup")
			}
			after, found, err := base.ReadHead(ctx)
			if err != nil || !found || after.Gen != initial.Gen+2 || after.Epoch != tc.epoch || after.Holder != "C" {
				t.Fatalf("refused claim changed racing HEAD: %+v found=%v err=%v", after, found, err)
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

// An identity refusal on the assignment stream ends when the stream (which
// checks the holder's identity) delivers an assignment after the
// registration is fixed, not 30 s later by decay.
func TestIdentityRefusalEndsOnADeliveredAssignment(t *testing.T) {
	h, assign := exitAuthorityHost(t, func(s *exitAssignmentStream) { s.identityRefusals.Store(1) })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if w := h.MasterWaitStatus(); w != nil && w.Reason == "identity refused: unregistered" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stream's identity refusal never showed: %+v", h.MasterWaitStatus())
		}
		time.Sleep(10 * time.Millisecond)
	}
	assign("r", 1) // the next stream is accepted and delivers
	deadline = time.Now().Add(5 * time.Second)
	for {
		if w := h.MasterWaitStatus(); w == nil || w.Reason != "identity refused: unregistered" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("a delivered assignment did not end the identity refusal")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
