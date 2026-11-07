package volume

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// G1 admission latch (codex01 R1): a role call on the host's own replica
// listener that fails (its floor could not be made durable) must not leave a
// previously healthy frontend serving: the projection turns unhealthy and the
// durable backend refuses writes, until a later role call succeeds.

// switchFencer fails both role calls while fail is set.
type switchFencer struct {
	fail atomic.Bool
}

var errFloorNotDurable = errors.New("injected: replica floor record not durable")

func (s *switchFencer) FenceAsPrimary(uint64) error {
	if s.fail.Load() {
		return errFloorNotDurable
	}
	return nil
}

func (s *switchFencer) AdmitAsReplica(uint64) error {
	if s.fail.Load() {
		return errFloorNotDurable
	}
	return nil
}

func selfFact(epoch uint64) *control.AssignmentFact {
	return &control.AssignmentFact{
		VolumeId: "vol1", ReplicaId: "rB", Epoch: epoch, EndpointVersion: 1,
		DataAddr: "127.0.0.1:1", CtrlAddr: "127.0.0.1:2", PeerSetGeneration: epoch,
	}
}

// A host healthy and serving at epoch 2 gets its epoch-3 self fact, and the
// floor persist fails: writes through the real StorageBackend must refuse
// (the engine projection is unchanged at epoch 2, which IsSuperseded and
// lineageCheck alone would still accept). Redelivery with a working persist
// clears the latch.
func TestG1_FailedRoleAdmissionStopsAPreviouslyHealthyFrontend(t *testing.T) {
	h, a, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	proj := &mutableProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 2, EndpointVersion: 1}}
	h.view = NewAdapterProjectionView(proj, "vol1", "rB", h)
	fencer := &switchFencer{}
	if err := h.SetSelfFence(fencer); err != nil {
		t.Fatal(err)
	}
	h.applyFact(selfFact(2))
	if a.callCount.Load() != 1 {
		t.Fatal("fixture: primary at epoch 2 not adopted")
	}

	store := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = store.Close() })
	backend := durable.NewStorageBackend(store, h.view, frontend.Identity{VolumeID: "vol1", ReplicaID: "rB", Epoch: 2, EndpointVersion: 1})
	backend.SetOperational(true, "g1 latch baseline")
	ctx := context.Background()
	if _, err := backend.Write(ctx, 0, []byte("epoch-2")); err != nil {
		t.Fatalf("baseline write at epoch 2: %v", err)
	}

	fencer.fail.Store(true)
	h.applyFact(selfFact(3)) // the epoch advance whose floor persist fails
	if a.callCount.Load() != 1 {
		t.Fatal("the epoch-3 role was adopted though its floor persist failed")
	}
	if failed, reason := h.RoleAdmissionFailure(); !failed || reason == "" {
		t.Fatal("no admission latch after a failed role call")
	}
	if h.view.Projection().Healthy {
		t.Fatal("projection still Healthy after a failed role admission")
	}
	if _, err := backend.Write(ctx, 0, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("write on the prior-healthy host after a failed role admission: got %v, want ErrStalePrimary", err)
	}
	if s := NewStatusServer(h.view).statusProjection(); s.RoleAdmissionFailure == "" || s.FrontendPrimaryReady {
		t.Fatalf("status does not report the latch: %+v", s)
	}

	fencer.fail.Store(false)
	h.applyFact(selfFact(3)) // master's stream replay redelivers
	if failed, _ := h.RoleAdmissionFailure(); failed {
		t.Fatal("latch not cleared by a successful redelivery")
	}
	if a.callCount.Load() != 2 || a.lastInfo.Epoch != 3 {
		t.Fatalf("the epoch-3 role was not adopted after reconciliation (calls=%d epoch=%d)", a.callCount.Load(), a.lastInfo.Epoch)
	}
	proj.p.Epoch = 3 // the adapter's projection after OnAssignment(3)
	at3 := durable.NewStorageBackend(store, h.view, frontend.Identity{VolumeID: "vol1", ReplicaID: "rB", Epoch: 3, EndpointVersion: 1})
	at3.SetOperational(true, "g1 latch reconciled")
	if _, err := at3.Write(ctx, 0, []byte("epoch-3")); err != nil {
		t.Fatalf("write at epoch 3 after reconciliation: %v", err)
	}
}

// The supporting-replica path fails closed too: no OnAssignment, latch set.
func TestG1_SupportingAdmissionFailsClosed(t *testing.T) {
	h, a, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	fencer := &switchFencer{}
	fencer.fail.Store(true)
	if err := h.SetSelfFence(fencer); err != nil {
		t.Fatal(err)
	}
	h.applyFact(&control.AssignmentFact{
		VolumeId: "vol1", ReplicaId: "rA", Epoch: 2, EndpointVersion: 1,
		DataAddr: "127.0.0.1:3", CtrlAddr: "127.0.0.1:4", PeerSetGeneration: 2,
		Peers: []*control.ReplicaDescriptor{{ReplicaId: "rB", Epoch: 2, EndpointVersion: 1, DataAddr: "127.0.0.1:5", CtrlAddr: "127.0.0.1:6"}},
	})
	if a.callCount.Load() != 0 {
		t.Fatal("supporting replica admitted though its listener could not persist the floor")
	}
	if failed, _ := h.RoleAdmissionFailure(); !failed {
		t.Fatal("no admission latch on the supporting path")
	}
}

// A role adopted before the listener exists is replayed at install; a replay
// failure is returned (the binary then does not serve) and latched.
func TestG1_SetSelfFenceReturnsTheReplayError(t *testing.T) {
	h, _, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.applyFact(selfFact(2)) // no listener yet: the role is recorded
	fencer := &switchFencer{}
	fencer.fail.Store(true)
	if err := h.SetSelfFence(fencer); err == nil {
		t.Fatal("SetSelfFence hid the replay error")
	}
	if failed, _ := h.RoleAdmissionFailure(); !failed {
		t.Fatal("no admission latch after a failed replay")
	}
}

// epochFencer fails every role call at the given epoch only.
type epochFencer struct{ failAt atomic.Uint64 }

func (e *epochFencer) FenceAsPrimary(epoch uint64) error {
	if epoch == e.failAt.Load() {
		return errFloorNotDurable
	}
	return nil
}

func (e *epochFencer) AdmitAsReplica(epoch uint64) error { return e.FenceAsPrimary(epoch) }

func supportingFact(epoch uint64) *control.AssignmentFact {
	return &control.AssignmentFact{
		VolumeId: "vol1", ReplicaId: "rA", Epoch: epoch, EndpointVersion: 1,
		DataAddr: "127.0.0.1:3", CtrlAddr: "127.0.0.1:4", PeerSetGeneration: epoch,
		Peers: []*control.ReplicaDescriptor{{ReplicaId: "rB", Epoch: epoch, EndpointVersion: 1, DataAddr: "127.0.0.1:5", CtrlAddr: "127.0.0.1:6"}},
	}
}

// codex01 R1 on #83: after the epoch-3 role call fails, a stale epoch-2 replay
// whose role call would succeed must not clear the latch or reopen writes on
// the real StorageBackend; only the current epoch's success reconciles.
func TestG1_StaleReplayDoesNotClearANewerAdmissionFailure(t *testing.T) {
	h, _, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	proj := &mutableProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 2, EndpointVersion: 1}}
	h.view = NewAdapterProjectionView(proj, "vol1", "rB", h)
	fencer := &epochFencer{}
	fencer.failAt.Store(3)
	if err := h.SetSelfFence(fencer); err != nil {
		t.Fatal(err)
	}
	h.applyFact(selfFact(2))
	store := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = store.Close() })
	backend := durable.NewStorageBackend(store, h.view, frontend.Identity{VolumeID: "vol1", ReplicaID: "rB", Epoch: 2, EndpointVersion: 1})
	backend.SetOperational(true, "g1 stale replay baseline")
	ctx := context.Background()
	if _, err := backend.Write(ctx, 0, []byte("baseline")); err != nil {
		t.Fatal(err)
	}
	h.applyFact(selfFact(3)) // fails: latched
	h.applyFact(selfFact(2)) // stale replay; its role call would succeed
	if failed, _ := h.RoleAdmissionFailure(); !failed {
		t.Fatal("a stale epoch-2 replay cleared the unresolved epoch-3 admission failure")
	}
	if _, err := backend.Write(ctx, 0, []byte("stale")); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("stale replay reopened writes: %v", err)
	}
	fencer.failAt.Store(0)
	h.applyFact(selfFact(3)) // the current epoch, now durable
	if failed, _ := h.RoleAdmissionFailure(); failed {
		t.Fatal("the current epoch's success did not clear the latch")
	}
}

// The same on the supporting-replica path.
func TestG1_StaleSupportingReplayDoesNotClearTheLatch(t *testing.T) {
	h, a, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	fencer := &epochFencer{}
	fencer.failAt.Store(3)
	if err := h.SetSelfFence(fencer); err != nil {
		t.Fatal(err)
	}
	h.applyFact(supportingFact(2))
	h.applyFact(supportingFact(3)) // fails
	calls := a.callCount.Load()
	h.applyFact(supportingFact(2)) // stale
	if failed, _ := h.RoleAdmissionFailure(); !failed {
		t.Fatal("a stale supporting replay cleared the latch")
	}
	if a.callCount.Load() != calls {
		t.Fatal("a stale supporting replay was admitted")
	}
	fencer.failAt.Store(0)
	h.applyFact(supportingFact(3))
	if failed, _ := h.RoleAdmissionFailure(); failed {
		t.Fatal("the current supporting admission did not clear the latch")
	}
}

// Install replay targets the retained current role: a failure at epoch 3, a
// stale epoch-2 fact, then installing a working listener replays epoch 3 and
// only that clears the latch.
func TestG1_InstallReplayTargetsTheCurrentRole(t *testing.T) {
	h, _, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	failing := &epochFencer{}
	failing.failAt.Store(3)
	if err := h.SetSelfFence(failing); err != nil {
		t.Fatal(err)
	}
	h.applyFact(selfFact(3)) // fails
	h.applyFact(selfFact(2)) // stale
	recorder := &recordingFencer{}
	if err := h.SetSelfFence(recorder); err != nil {
		t.Fatalf("install replay: %v", err)
	}
	if recorder.last.Load() != 3 {
		t.Fatalf("install replayed epoch %d, want the current desired epoch 3", recorder.last.Load())
	}
	if failed, _ := h.RoleAdmissionFailure(); failed {
		t.Fatal("a successful replay of the current role did not clear the latch")
	}
}

type recordingFencer struct{ last atomic.Uint64 }

func (r *recordingFencer) FenceAsPrimary(epoch uint64) error { r.last.Store(epoch); return nil }
func (r *recordingFencer) AdmitAsReplica(epoch uint64) error { r.last.Store(epoch); return nil }
