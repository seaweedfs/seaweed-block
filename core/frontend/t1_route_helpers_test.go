// Ownership: sw test-support (per test-spec §9: non-test helper
// files *_test_support.go are sw-owned). Holds the in-process
// route builder used by t1_route_test.go.
//
// Route wired here:
//   StaticDirective ->
//   authority.Publisher ->
//   authority.Bridge (per-replica subscription) ->
//   adapter.VolumeReplicaAdapter (real, with HealthyPathExecutor) ->
//   volume.AdapterProjectionView ->
//   frontend.Provider.
//
// Readiness source: volume.AdapterProjectionView reads
// (*VolumeReplicaAdapter).Projection() -> engine.ReplicaProjection
// -> Mode. Healthy flips only when engine.DeriveProjection says
// ModeHealthy, which requires Identity.Epoch <= FencedEpoch after
// a successful probe. That path is the exact one the architect-
// review finding required L1 to exercise.
package frontend_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
)

// t1Route bundles one in-process route for one (volumeID,
// replicaID). Each replica gets its own adapter; authority
// movement is simulated by the Publisher directing the ask to a
// new (volumeID, replicaID) slot.
type t1Route struct {
	volumeID string

	pub *authority.Publisher
	dir *authority.StaticDirective

	mu       sync.Mutex
	slots    map[string]*replicaSlot // replicaID -> slot
	current  string                  // current primary replicaID
	cancel   context.CancelFunc
	runDone  chan struct{}
	volCtx   context.Context
}

// replicaSlot holds one replica's real adapter + AdapterProjectionView.
type replicaSlot struct {
	replicaID string
	adapter   *adapter.VolumeReplicaAdapter
	executor  *volume.HealthyPathExecutor
	view      *volume.AdapterProjectionView
	cancel    context.CancelFunc
	done      chan struct{}
}

// newT1Route wires Publisher + Directive + per-replica adapter
// route. Replicas are started lazy — newT1Route returns before
// any AssignmentInfo flows. The caller uses bindReplica to
// drive the first Bind that brings a replica slot online.
func newT1Route(t *testing.T, volumeID string) *t1Route {
	t.Helper()
	dir := authority.NewStaticDirective(nil)
	pub := authority.NewPublisher(dir)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = pub.Run(ctx)
	}()

	r := &t1Route{
		volumeID: volumeID,
		pub:      pub,
		dir:      dir,
		slots:    map[string]*replicaSlot{},
		cancel:   cancel,
		runDone:  runDone,
		volCtx:   ctx,
	}
	t.Cleanup(r.Close)
	return r
}

// bindReplica starts a subscription + real adapter + bridge view
// for one replica, then appends the first IntentBind ask to the
// directive. Returns the slot for test bookkeeping.
func (r *t1Route) bindReplica(t *testing.T, replicaID, dataAddr, ctrlAddr string) *replicaSlot {
	t.Helper()

	exec := volume.NewHealthyPathExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)
	// L1 exercises same-replica Epoch drift, not cross-replica
	// supersede — no probe needed. L2 wires a real probe via
	// volume.Host.IsSuperseded.
	view := volume.NewAdapterProjectionView(a, r.volumeID, replicaID, nil)

	slotCtx, slotCancel := context.WithCancel(r.volCtx)
	done := make(chan struct{})

	// Bridge (per-replica subscription) forwards AssignmentInfo
	// from publisher to this replica's adapter.
	go func() {
		defer close(done)
		authority.Bridge(slotCtx, r.pub, a, r.volumeID, replicaID)
	}()

	slot := &replicaSlot{
		replicaID: replicaID,
		adapter:   a,
		executor:  exec,
		view:      view,
		cancel:    slotCancel,
		done:      done,
	}
	r.mu.Lock()
	r.slots[replicaID] = slot
	if r.current == "" {
		r.current = replicaID
	}
	r.mu.Unlock()

	r.dir.Append(authority.AssignmentAsk{
		VolumeID:  r.volumeID,
		ReplicaID: replicaID,
		DataAddr:  dataAddr,
		CtrlAddr:  ctrlAddr,
		Intent:    authority.IntentBind,
	})
	return slot
}

// reassignReplica bumps the Epoch on the current replica via
// IntentReassign. This is the L1 "authority move" arm — it
// drives the frontend fence against Epoch drift using the real
// publication path (Publisher authors a new Epoch, Bridge
// delivers it to the adapter, engine applies it, projection
// advances).
func (r *t1Route) reassignReplica(replicaID, dataAddr, ctrlAddr string) {
	r.dir.Append(authority.AssignmentAsk{
		VolumeID:  r.volumeID,
		ReplicaID: replicaID,
		DataAddr:  dataAddr,
		CtrlAddr:  ctrlAddr,
		Intent:    authority.IntentReassign,
	})
}

// view returns the frontend ProjectionView for a replica slot.
func (r *t1Route) view(replicaID string) frontend.ProjectionView {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.slots[replicaID]; ok {
		return s.view
	}
	return nil
}

// waitUntilHealthy polls the given replica's adapter projection
// until Healthy=true, then returns the frontend.Projection. On
// deadline expiry, fails the test.
func (r *t1Route) waitUntilHealthy(t *testing.T, replicaID string, deadline time.Duration) frontend.Projection {
	t.Helper()
	v := r.view(replicaID)
	if v == nil {
		t.Fatalf("no slot for replica %q", replicaID)
	}
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		p := v.Projection()
		if p.Healthy {
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replica %q did not reach Healthy within %v (last projection=%+v)", replicaID, deadline, v.Projection())
	return frontend.Projection{}
}

// waitUntilEpochAdvances polls until the replica's view reports
// a higher Epoch than `from`, then returns the new projection.
func (r *t1Route) waitUntilEpochAdvances(t *testing.T, replicaID string, from uint64, deadline time.Duration) frontend.Projection {
	t.Helper()
	v := r.view(replicaID)
	if v == nil {
		t.Fatalf("no slot for replica %q", replicaID)
	}
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		p := v.Projection()
		if p.Healthy && p.Epoch > from {
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replica %q Epoch did not advance past %d within %v (last=%+v)", replicaID, from, deadline, v.Projection())
	return frontend.Projection{}
}

// Close tears the route down: cancel context, wait for bridge
// and publisher goroutines to exit.
func (r *t1Route) Close() {
	r.mu.Lock()
	slots := make([]*replicaSlot, 0, len(r.slots))
	for _, s := range r.slots {
		slots = append(slots, s)
	}
	r.mu.Unlock()
	r.cancel()
	for _, s := range slots {
		<-s.done
	}
	<-r.runDone
}
