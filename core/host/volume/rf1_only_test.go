package volume

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

func rf2PrimaryFact(epoch uint64) *control.AssignmentFact {
	return &control.AssignmentFact{
		VolumeId: "vol1", ReplicaId: "r1", Epoch: epoch, EndpointVersion: 1,
		DataAddr: "127.0.0.1:1000", CtrlAddr: "127.0.0.1:1001", PeerSetGeneration: epoch,
		Peers: []*control.ReplicaDescriptor{{ReplicaId: "r2", Epoch: epoch, EndpointVersion: 1, DataAddr: "127.0.0.1:2000", CtrlAddr: "127.0.0.1:2001"}},
	}
}

// The production start schedule of an RF1-only (walstore-chunk) volume: the
// host starts with local readiness blocked, a retained RF2 fact arrives and is
// stored, and installing the replication volume replays it with no self fence
// installed yet. The host must refuse it before any peer set, adoption or
// readiness marker, and latch the refusal; a later RF1 fact is adopted.
func TestRF1Only_RetainedReplicatedFactReplayedBeforeSelfFence(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r1", rec)
	ready := make(chan adapter.AssignmentInfo, 4)
	h.cfg.ReadyMarker, h.cfg.RF1Only = ready, "chunk snapshots need RF1"
	rv := h.replication
	h.replication = nil
	h.BlockLocalReadiness("awaiting durable recovery")

	h.applyFact(rf2PrimaryFact(3)) // stored for replay
	h.ClearLocalReadinessBlock()
	h.replicationMu.Lock()
	h.setReplicationLocked(rv) // what SetReplicationVolume does, then it replays
	h.replicationMu.Unlock()
	h.replayLocalReadinessFactIfReady()

	if h.selfFence != nil {
		t.Fatal("fixture: a self fence is installed")
	}
	if r.callCount.Load() != 0 || a.callCount.Load() != 0 || len(ready) != 0 {
		t.Fatalf("replayed RF2 fact installed something: peers=%d adopt=%d ready=%d %v", r.callCount.Load(), a.callCount.Load(), len(ready), rec.snapshot())
	}
	failed, reason := h.RoleAdmissionFailure()
	if !failed || !strings.Contains(reason, "refuses a primary role with 1 replica peers at epoch 3") || !strings.Contains(reason, "chunk snapshots need RF1") {
		t.Fatalf("no refusal latch: %v %q", failed, reason)
	}

	// A supporting-replica fact (another primary lists this replica) is refused too.
	sup := rf2PrimaryFact(4)
	sup.ReplicaId, sup.Peers[0].ReplicaId = "r2", "r1"
	h.applyFact(sup)
	if r.callCount.Load() != 0 || a.callCount.Load() != 0 || len(ready) != 0 {
		t.Fatal("supporting replica admitted on an RF1-only volume")
	}
	if _, reason := h.RoleAdmissionFailure(); !strings.Contains(reason, "refuses a supporting replica role at epoch 4") {
		t.Fatalf("supporting refusal not latched: %q", reason)
	}

	// The master moves the volume to RF1: adopted, latch cleared, ready emitted.
	rf1 := rf2PrimaryFact(5)
	rf1.Peers = nil
	h.applyFact(rf1)
	if a.callCount.Load() != 1 || len(ready) != 1 {
		t.Fatalf("RF1 primary not adopted: adopt=%d ready=%d", a.callCount.Load(), len(ready))
	}
	if failed, reason := h.RoleAdmissionFailure(); failed {
		t.Fatalf("latch not cleared by an RF1 fact: %q", reason)
	}
}

// Control: without the rule the same replayed RF2 fact installs its peer set
// and is adopted (today's behaviour for replicated volumes).
func TestRF1Only_UnsetKeepsReplicatedAdoption(t *testing.T) {
	h, a, r := newStubbedHost(t, "vol1", "r1", &orderingRecorder{})
	h.applyFact(rf2PrimaryFact(3))
	if r.callCount.Load() != 1 || a.callCount.Load() != 1 {
		t.Fatalf("replicated fact not applied: peers=%d adopt=%d", r.callCount.Load(), a.callCount.Load())
	}
	if failed, _ := h.RoleAdmissionFailure(); failed {
		t.Fatal("latched without the rule")
	}
}

// A refusal is bound to its epoch: RF1@2 healthy and writing, RF2@3 refused
// (writes stop), then a stale RF1@2 replay must neither adopt nor clear the
// refusal (writes stay refused, no peers installed); only RF1 at the refused
// epoch or newer, once its role is admitted, clears it.
func TestRF1Only_StaleRF1FactDoesNotClearANewerRefusal(t *testing.T) {
	f := newRF1BackendHost(t)
	f.h.applyFact(rf1Fact(2))
	if f.a.callCount.Load() != 1 {
		t.Fatal("fixture: RF1@2 not adopted")
	}
	f.mustWrite(t, 2, 0xD2)
	f.h.applyFact(rf2PrimaryFact(3))
	f.assertRefusedAt(t, 3)
	f.h.applyFact(rf1Fact(2)) // stale replay below the refused epoch
	f.assertRefusedAt(t, 3)
	if f.a.callCount.Load() != 1 || f.r.callCount.Load() != 1 {
		t.Fatalf("stale RF1@2 adopted or RF2 peers installed: adopt=%d peerSets=%d", f.a.callCount.Load(), f.r.callCount.Load())
	}
	f.fencer.fail.Store(true) // RF1@3 whose role fence fails: a failed attempt does not clear the refusal
	f.h.applyFact(rf1Fact(3))
	if _, reason := f.h.RoleAdmissionFailure(); !strings.Contains(reason, "refuses a primary role") || f.a.callCount.Load() != 1 {
		t.Fatalf("failed RF1@3 attempt cleared the refusal: %q adopt=%d", reason, f.a.callCount.Load())
	}
	f.fencer.fail.Store(false)
	f.h.applyFact(rf1Fact(3))
	if failed, reason := f.h.RoleAdmissionFailure(); failed || f.a.callCount.Load() != 2 {
		t.Fatalf("RF1@3 did not reconcile: %v %q adopt=%d", failed, reason, f.a.callCount.Load())
	}
	f.mustWrite(t, 3, 0xD3)
}

type rf1Fixture struct {
	fencer *switchFencer
	h      *Host
	a      *stubAdapter
	r      *stubReplication
	proj   *mutableProjector
	store  *memorywal.Store
}

func newRF1BackendHost(t *testing.T) *rf1Fixture {
	t.Helper()
	h, a, r := newStubbedHost(t, "vol1", "r1", &orderingRecorder{})
	h.cfg.RF1Only = "chunk snapshots need RF1"
	proj := &mutableProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 2, EndpointVersion: 1}}
	h.view = NewAdapterProjectionView(proj, "vol1", "r1", h)
	fencer := &switchFencer{}
	if err := h.SetSelfFence(fencer); err != nil {
		t.Fatal(err)
	}
	store := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = store.Close() })
	return &rf1Fixture{fencer: fencer, h: h, a: a, r: r, proj: proj, store: store}
}

// backend is a frontend at this epoch's identity, with the engine projection
// at that epoch (only called while no applyFact runs).
func (f *rf1Fixture) backend(epoch uint64) *durable.StorageBackend {
	f.proj.p = engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: epoch, EndpointVersion: 1}
	b := durable.NewStorageBackend(f.store, f.h.view, frontend.Identity{VolumeID: "vol1", ReplicaID: "r1", Epoch: epoch, EndpointVersion: 1})
	b.SetOperational(true, "rf1 fixture")
	return b
}

// mustWrite writes one block at the epoch's identity and reads it back.
func (f *rf1Fixture) mustWrite(t *testing.T, epoch uint64, fill byte) {
	t.Helper()
	ctx := context.Background()
	b := f.backend(epoch)
	data := bytes.Repeat([]byte{fill}, 4096)
	if n, err := b.Write(ctx, 0, data); n != len(data) || err != nil {
		t.Fatalf("write at epoch %d: n=%d err=%v", epoch, n, err)
	}
	got := make([]byte, len(data))
	if n, err := b.Read(ctx, 0, got); n != len(got) || err != nil || !bytes.Equal(got, data) {
		t.Fatalf("readback at epoch %d: n=%d err=%v equal=%v", epoch, n, err, bytes.Equal(got, data))
	}
}

func rf1Fact(epoch uint64) *control.AssignmentFact {
	f := rf2PrimaryFact(epoch)
	f.Peers = nil
	return f
}

func (f *rf1Fixture) assertRefusedAt(t *testing.T, epoch uint64) {
	t.Helper()
	if failed, reason := f.h.RoleAdmissionFailure(); !failed || !strings.Contains(reason, fmt.Sprintf("at epoch %d", epoch)) {
		t.Fatalf("want the refusal at epoch %d latched: %v %q", epoch, failed, reason)
	}
	if _, err := f.backend(epoch-1).Write(context.Background(), 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("write at the old identity after the refusal: %v", err)
	}
	if len(f.r.lastTargetIDs) != 0 {
		t.Fatalf("forbidden peers installed: %v", f.r.lastTargetIDs)
	}
}

// Serial interleaving: an allowed RF1@2 replay passes the stale check, and
// before it admits, the stream latches an RF2@3 refusal. The replay must not
// clear that newer refusal nor adopt. RF1@3 then admits and serves at epoch 3.
func TestRF1Only_HeldRF1ReplayDoesNotClearANewerRefusal(t *testing.T) {
	f := newRF1BackendHost(t)
	f.h.applyFact(rf1Fact(2))
	f.mustWrite(t, 2, 0xA2)
	fired := false
	f.h.rf1BeforeAdmit = func() {
		if !fired {
			fired = true
			f.h.applyFact(rf2PrimaryFact(3))
		}
	}
	f.h.applyFact(rf1Fact(2))
	f.h.rf1BeforeAdmit = nil
	if !fired || f.a.callCount.Load() != 1 {
		t.Fatalf("fixture fired=%v; held RF1@2 adopted=%d", fired, f.a.callCount.Load())
	}
	f.assertRefusedAt(t, 3)
	f.h.applyFact(rf1Fact(3))
	if failed, reason := f.h.RoleAdmissionFailure(); failed || f.a.callCount.Load() != 2 || f.a.lastInfo.Epoch != 3 {
		t.Fatalf("current RF1@3 not admitted: %v %q adopt=%d", failed, reason, f.a.callCount.Load())
	}
	f.mustWrite(t, 3, 0xA3)
}

// holdingUpdater parks the first armed UpdateReplicaSet until released.
type holdingUpdater struct {
	inner            replicaSetUpdater
	armed            atomic.Bool
	entered, release chan struct{}
}

func (u *holdingUpdater) UpdateReplicaSet(gen uint64, targets []replication.ReplicaTarget) error {
	if u.armed.CompareAndSwap(true, false) {
		close(u.entered)
		<-u.release
	}
	return u.inner.UpdateReplicaSet(gen, targets)
}

// Two workers: the replay worker's RF1@2 passes the stale check and parks in
// the peer-set update; the stream worker's RF2@3 is refused meanwhile; the
// replay then resumes. Bounded by timeouts.
func TestRF1Only_ParallelHeldReplayAndNewerRefusal(t *testing.T) {
	f := newRF1BackendHost(t)
	f.h.applyFact(rf1Fact(2))
	f.mustWrite(t, 2, 0xB2)
	u := &holdingUpdater{inner: f.r, entered: make(chan struct{}), release: make(chan struct{})}
	u.armed.Store(true)
	f.h.replicationMu.Lock()
	f.h.setReplicationLocked(u)
	f.h.replicationMu.Unlock()

	replay := make(chan struct{})
	go func() { defer close(replay); f.h.applyFact(rf1Fact(2)) }()
	select {
	case <-u.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("replay worker did not reach the peer-set update")
	}
	stream := make(chan struct{})
	go func() { defer close(stream); f.h.applyFact(rf2PrimaryFact(3)) }()
	select {
	case <-stream:
	case <-time.After(5 * time.Second):
		t.Fatal("stream worker blocked behind the parked replay")
	}
	close(u.release)
	select {
	case <-replay:
	case <-time.After(5 * time.Second):
		t.Fatal("replay worker did not finish")
	}
	if f.a.callCount.Load() != 1 {
		t.Fatalf("parked RF1@2 adopted after the newer refusal: adopt=%d", f.a.callCount.Load())
	}
	f.assertRefusedAt(t, 3)
	f.h.applyFact(rf1Fact(3))
	if failed, reason := f.h.RoleAdmissionFailure(); failed || f.a.callCount.Load() != 2 {
		t.Fatalf("current RF1@3 not admitted: %v %q", failed, reason)
	}
	f.mustWrite(t, 3, 0xB3)
}

// A stale RF2 below an admitted RF1 epoch does not re-latch the refusal.
func TestRF1Only_StaleRF2AfterNewerRF1DoesNotRelatch(t *testing.T) {
	f := newRF1BackendHost(t)
	f.h.applyFact(rf1Fact(2))
	f.h.applyFact(rf2PrimaryFact(3))
	f.h.applyFact(rf1Fact(4))
	if failed, reason := f.h.RoleAdmissionFailure(); failed || f.a.callCount.Load() != 2 {
		t.Fatalf("RF1@4 not admitted: %v %q adopt=%d", failed, reason, f.a.callCount.Load())
	}
	f.h.applyFact(rf2PrimaryFact(3)) // stale replay
	if failed, reason := f.h.RoleAdmissionFailure(); failed {
		t.Fatalf("stale RF2@3 re-latched after RF1@4: %q", reason)
	}
	f.mustWrite(t, 4, 0xC4)
	f.h.applyFact(rf2PrimaryFact(4)) // same epoch, peers added: refused
	if failed, _ := f.h.RoleAdmissionFailure(); !failed {
		t.Fatal("RF2 at the admitted epoch not refused")
	}
}
