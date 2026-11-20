package master

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func reviewBindingHost(t *testing.T) (*Host, *grpc.ClientConn, context.Context) {
	t.Helper()
	return reviewBindingHostWithReader(t, func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}, nil
	})
}

func reviewBindingHostWithReader(t *testing.T, reader func(context.Context, string) (authority.WorkspaceHead, error)) (*Host, *grpc.ClientConn, context.Context) {
	t.Helper()
	return reviewBindingHostBeforeStart(t, reader, nil)
}

func reviewBindingHostBeforeStart(t *testing.T, reader func(context.Context, string) (authority.WorkspaceHead, error), before func(*Host)) (*Host, *grpc.ClientConn, context.Context) {
	t.Helper()
	h, e := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, WorkspaceHeadReader: reader})
	if e != nil {
		t.Fatal(e)
	}
	if before != nil {
		before(h)
	}
	h.Start()
	t.Cleanup(func() { closeTestMaster(t, h) })
	c, e := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	for _, server := range []string{"node-a", "node-b"} {
		if _, e = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); e != nil {
			t.Fatal(e)
		}
	}
	return h, c, ctx
}

func reviewLegacyBind(t *testing.T, h *Host, c *grpc.ClientConn, ctx context.Context, vol string) {
	t.Helper()
	reviewCreate(t, c, ctx, vol)
	if _, e := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: vol, DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{VolumeID: vol, ServerID: "node-a", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); e != nil {
		t.Fatal(e)
	}
	reviewHeartbeat(t, c, ctx, vol, "node-a", "127.0.0.1:10000", "127.0.0.1:10001")
	if _, e := h.RunLifecycleProductTick(); e != nil {
		t.Fatal(e)
	}
	if line := waitAuthorityLine(t, h.Publisher(), vol); line.Epoch != 1 {
		t.Fatal(line)
	}
}

func TestWorkspaceBindingLegacyUpgradeCannotTakeOwnLiveSource(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, c, ctx, "legacy")
	client := control.NewWorkspaceServiceClient(c)
	if _, e := client.RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: "legacy", ServerId: "node-b", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}); e == nil {
		t.Fatal("first target registration took own live source pair")
	}
	if _, e := client.RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: "legacy", ServerId: "node-a", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}); e != nil {
		t.Fatal("exact source-owner registration refused", e)
	}
}

func TestWorkspaceBindingLegacyMoveCannotTakePreparedReservation(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, c, ctx, "legacy")
	reviewCreate(t, c, ctx, "prepared")
	client := control.NewWorkspaceServiceClient(c)
	if _, e := client.RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: "prepared", ServerId: "node-b", DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); e != nil {
		t.Fatal(e)
	}
	id := &control.WorkspaceIdentity{VolumeId: "legacy", ReplicaId: "r1", Epoch: 1}
	if _, e := client.RenewWorkspaceLease(ctx, id); e != nil {
		t.Fatal(e)
	}
	if _, e := client.RevokeWorkspace(ctx, id); e != nil {
		t.Fatal(e)
	}
	if _, e := client.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); e != nil {
		t.Fatal(e)
	}
	if _, e := client.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: "legacy-move", VolumeId: "legacy", ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:9200", TargetCtrlAddr: "127.0.0.1:9100", Kind: "clean"}); e == nil {
		t.Fatal("legacy move took another volume's prepared reservation")
	}
	if line, _ := h.Publisher().VolumeAuthorityLine("legacy"); line.Epoch != 1 {
		t.Fatal("refused move changed authority", line)
	}
}

func TestWorkspaceBindingDeleteCannotReleaseLiveAuthority(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, c, ctx, "live")
	client := control.NewWorkspaceServiceClient(c)
	b := &control.WorkspaceBinding{VolumeId: "live", ServerId: "node-a", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
	if _, e := client.RegisterWorkspaceBinding(ctx, b); e != nil {
		t.Fatal(e)
	}
	if _, e := client.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "live", ReplicaId: "r1", Epoch: 1}); e != nil {
		t.Fatal(e)
	}
	if _, e := control.NewLifecycleServiceClient(c).DeleteVolume(ctx, &control.DeleteVolumeRequest{VolumeId: "live"}); e == nil {
		t.Fatal("delete released a live authority's reservation")
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("live"); !ok {
		t.Fatal("refused deletion removed lifecycle state")
	}
	if _, bound, e := h.Lifecycle().WorkspaceBindings.Get("live"); e != nil || !bound {
		t.Fatal("refused deletion removed binding", bound, e)
	}
	id := &control.WorkspaceIdentity{VolumeId: "live", ReplicaId: "r1", Epoch: 1}
	if _, e := client.RevokeWorkspace(ctx, id); e != nil {
		t.Fatal(e)
	}
	if _, e := client.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); e != nil {
		t.Fatal(e)
	}
	if _, e := control.NewLifecycleServiceClient(c).DeleteVolume(ctx, &control.DeleteVolumeRequest{VolumeId: "live"}); e != nil {
		t.Fatal("clean retired deletion positive", e)
	}
	reviewCreate(t, c, ctx, "reuse")
	b.VolumeId = "reuse"
	b.ServerId = "node-b"
	if _, e := client.RegisterWorkspaceBinding(ctx, b); e != nil {
		t.Fatal("retired pair not reusable after clean delete", e)
	}

}

func TestWorkspaceBindingCanceledQueuedDeleteDoesNotMutate(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	reviewCreate(t, c, ctx, "queued")
	if _, e := control.NewWorkspaceServiceClient(c).RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: "queued", ServerId: "node-a", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}); e != nil {
		t.Fatal(e)
	}
	canceled, cancel := context.WithCancel(ctx)
	h.workspaceMu.Lock()
	arrived := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(arrived)
		_, e := newServices(h).DeleteVolume(canceled, &control.DeleteVolumeRequest{VolumeId: "queued"})
		done <- e
	}()
	<-arrived
	cancel()
	h.workspaceMu.Unlock()
	select {
	case e := <-done:
		if e == nil {
			t.Error("canceled queued delete succeeded")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("queued"); !ok {
		t.Fatal("canceled queued delete removed lifecycle state")
	}
	if _, bound, e := h.Lifecycle().WorkspaceBindings.Get("queued"); e != nil || !bound {
		t.Fatal("canceled queued delete removed binding", bound, e)
	}
}

func reviewCreate(t *testing.T, c *grpc.ClientConn, ctx context.Context, vol string) {
	t.Helper()
	if _, e := control.NewLifecycleServiceClient(c).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: vol, SizeBytes: 1 << 20, ReplicationFactor: 1}); e != nil {
		t.Fatal(e)
	}
}

func reviewHeartbeat(t *testing.T, c *grpc.ClientConn, ctx context.Context, vol, server, data, ctrl string) {
	t.Helper()
	if _, e := control.NewObservationServiceClient(c).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: server, SentAt: timestamppb.Now(), Reachable: true, Eligible: true, Slots: []*control.HeartbeatSlot{{VolumeId: vol, ReplicaId: "r1", DataAddr: data, CtrlAddr: ctrl}}}); e != nil {
		t.Fatal(e)
	}
}

func TestWorkspaceBindingChosenObservationMustMatchServerAndPair(t *testing.T) {
	for _, wrong := range []string{"server", "pair"} {
		t.Run(wrong, func(t *testing.T) {
			h, c, ctx := reviewBindingHost(t)
			reviewCreate(t, c, ctx, "v")
			client := control.NewWorkspaceServiceClient(c)
			b := &control.WorkspaceBinding{VolumeId: "v", ServerId: "node-a", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
			if _, e := client.RegisterWorkspaceBinding(ctx, b); e != nil {
				t.Fatal(e)
			}
			if _, e := client.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "v", ServerId: "node-a", ReplicaId: "r1"}); e != nil {
				t.Fatal(e)
			}
			server, data, ctrl := "node-a", b.DataAddr, b.CtrlAddr
			if wrong == "server" {
				server = "node-b"
			} else {
				data = "127.0.0.1:10002"
			}
			reviewHeartbeat(t, c, ctx, "v", server, data, ctrl)
			result, e := h.RunLifecycleProductTick()
			if e != nil {
				t.Fatal(e)
			}
			if result.PublishedAsks != 0 {
				t.Fatalf("wrong %s observation submitted assignment: %+v", wrong, result)
			}
			if _, ok := h.Publisher().VolumeAuthorityLine("v"); ok {
				t.Fatal("wrong observation minted authority")
			}
			reviewHeartbeat(t, c, ctx, "v", "node-a", b.DataAddr, b.CtrlAddr)
			if _, e = h.RunLifecycleProductTick(); e != nil {
				t.Fatal(e)
			}
			line := waitAuthorityLine(t, h.Publisher(), "v")
			if line.DataAddr != b.DataAddr || line.CtrlAddr != b.CtrlAddr {
				t.Fatal("correct observation published wrong pair", line)
			}
		})
	}
}

// The ACK must complete while the unrelated remote reader/prober remains held.
// errHeldNotJoined: the held remote operation's worker did not return within
// its own bound after being released.
var errHeldNotJoined = errors.New("held remote operation not joined")

func TestWorkspaceBindingRemoteReadDoesNotBlockAck(t *testing.T) {
	for _, mode := range []string{"head", "promotion"} {
		t.Run(mode, func(t *testing.T) {
			arrived, release := make(chan struct{}), make(chan struct{})
			h, c, ctx := reviewBindingHostWithReader(t, func(ctx context.Context, vol string) (authority.WorkspaceHead, error) {
				close(arrived)
				select {
				case <-release:
				case <-ctx.Done():
					return authority.WorkspaceHead{}, ctx.Err()
				}
				return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", Detached: true}, nil
			})
			reviewLegacyBind(t, h, c, ctx, "ack")
			client := control.NewWorkspaceServiceClient(c)
			id := &control.WorkspaceIdentity{VolumeId: "ack", ReplicaId: "r1", Epoch: 1}
			if _, e := client.RenewWorkspaceLease(ctx, id); e != nil {
				t.Fatal(e)
			}
			if _, e := client.RevokeWorkspace(ctx, id); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			var releaseOnce sync.Once
			releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
			// Registered before either worker launches: every exit releases the
			// held call and joins its worker under its own bound, not under the
			// fixture context a blocked ACK has already used up.
			var joinOnce sync.Once
			var joined error
			join := func() error {
				joinOnce.Do(func() {
					select {
					case joined = <-done:
					case <-time.After(10 * time.Second):
						joined = errHeldNotJoined
					}
				})
				return joined
			}
			t.Cleanup(func() {
				releaseHeld()
				if err := join(); errors.Is(err, errHeldNotJoined) {
					t.Error(err)
				}
			})
			if mode == "head" {
				reviewLegacyBind(t, h, c, ctx, "probe")
				go func() {
					_, e := client.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: "held", VolumeId: "probe", ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:9200", TargetCtrlAddr: "127.0.0.1:9100", Kind: "clean"})
					done <- e
				}()
			} else {
				if _, e := h.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "vol-rf3", SizeBytes: 1 << 20, ReplicationFactor: 3}); e != nil {
					t.Fatal(e)
				}
				seedRF3Placement(t, h)
				ingestRF3Observation(t, h, true, true, true)
				if _, e := h.RunLifecycleProductTick(); e != nil {
					t.Fatal(e)
				}
				waitAuthorityLine(t, h.Publisher(), "vol-rf3")
				ingestRF3Observation(t, h, false, true, true)
				h.SetPromotionEvidenceProvider(reviewHeldPromotion{arrived, release})
				go func() { _, e := h.RunLifecycleProductTick(); done <- e }()
			}
			select {
			case <-arrived:
			case <-ctx.Done():
				t.Fatal("remote call not reached", ctx.Err())
			}
			// The held remote call is released only after the ACK returns, so an
			// ACK that waited for it would run into the test's own deadline. No
			// separate 1 s bound: the ACK includes a durable persist, which a
			// loaded host can slow past that without anything being blocked.
			_, e := client.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9})
			releaseHeld()
			if err := join(); errors.Is(err, errHeldNotJoined) {
				t.Fatal(err)
			}
			if e != nil {
				t.Fatal("held remote operation blocked ACK", e)
			}
		})
	}
}

type reviewHeldPromotion struct{ arrived, release chan struct{} }

func (p reviewHeldPromotion) ProbePromotionCandidates(string, authority.AuthorityBasis, []authority.ReplicaCandidate) (PromotionProbeResult, error) {
	close(p.arrived)
	<-p.release
	return PromotionProbeResult{}, nil
}

// The real controller dequeues the admitted ask, then its Publisher is held
// before the production admission/mint boundary. Queue discard cannot pass this.
func TestWorkspaceBindingInFlightPublicationRechecksOwnershipAndLifetime(t *testing.T) {
	for _, action := range []string{"register", "delete"} {
		t.Run(action, func(t *testing.T) {
			arrived, release := make(chan struct{}), make(chan struct{})
			finished := make(chan error, 1)
			h, c, ctx := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }, func(h *Host) {
				authority.WithPublicationAdmission(func(ask authority.AssignmentAsk, publish func() error) error {
					if ask.VolumeID == "inflight" {
						close(arrived)
						select {
						case <-release:
						case <-time.After(5 * time.Second):
							return context.DeadlineExceeded
						}
					}
					err := h.admitWorkspacePublication(ask, publish)
					if ask.VolumeID == "inflight" {
						finished <- err
					}
					return err
				})(h.Publisher())
			})
			reviewCreate(t, c, ctx, "inflight")
			if _, e := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "inflight", DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{VolumeID: "inflight", ServerID: "node-a", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); e != nil {
				t.Fatal(e)
			}
			reviewHeartbeat(t, c, ctx, "inflight", "node-a", "127.0.0.1:10000", "127.0.0.1:10001")
			if _, e := h.RunLifecycleProductTick(); e != nil {
				t.Fatal(e)
			}
			select {
			case <-arrived:
			case <-ctx.Done():
				t.Fatal("publisher did not dequeue ask", ctx.Err())
			}
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			if action == "delete" {
				if _, e := control.NewLifecycleServiceClient(c).DeleteVolume(ctx, &control.DeleteVolumeRequest{VolumeId: "inflight"}); e != nil {
					t.Fatal("unassigned legacy delete", e)
				}
				if _, ok := h.Lifecycle().Volumes.GetVolume("inflight"); ok {
					t.Fatal("delete did not remove volume")
				}
			}
			reviewCreate(t, c, ctx, "owner")
			wc := control.NewWorkspaceServiceClient(c)
			reserve := func() {
				if _, e := wc.RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: "owner", ServerId: "node-b", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}); e != nil {
					t.Fatal("reservation", e)
				}
			}
			if action == "register" {
				reserve()
			}
			close(release)
			select {
			case e := <-finished:
				if e == nil {
					t.Error("held stale ask was published after competing " + action)
				}
			case <-ctx.Done():
				t.Fatal("publisher did not settle", ctx.Err())
			}
			if line, ok := h.Publisher().VolumeAuthorityLine("inflight"); ok {
				t.Fatal("stale or deleted ask minted authority", line)
			}
			if action == "delete" {
				reserve()
			}
			st, e := wc.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "owner"})
			if e != nil || len(st.GetBindings()) != 1 {
				t.Fatal("owner reservation lost", st, e)
			}
			// An unrelated, matching explicit owner can still reach the actual publisher.
			if _, e = wc.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "owner", ServerId: "node-b", ReplicaId: "r1"}); e != nil {
				t.Fatal(e)
			}
			reviewHeartbeat(t, c, ctx, "owner", "node-b", "127.0.0.1:10000", "127.0.0.1:10001")
			if _, e = h.RunLifecycleProductTick(); e != nil {
				t.Fatal(e)
			}
			waitAuthorityLine(t, h.Publisher(), "owner")
		})
	}
}
