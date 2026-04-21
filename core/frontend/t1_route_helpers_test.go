// Ownership: sw test-support (per test-spec §9). Builds the
// in-process P14 publication route used by t1_route_test.go.
//
// Route: StaticDirective -> Publisher -> VolumeBridge ->
//        latestLineageConsumer -> ProjectionView -> Provider.
//
// Scope deviation (Discovery Bridge candidate, see sw review
// notes in commit message): the QA spec's T1.L1.1 requires a
// ReplicaID change across the "authority move". The current
// Directive API exposes only Bind / RefreshEndpoint / Reassign,
// where Reassign bumps Epoch for the same (Volume, Replica) —
// no ReplicaID change. Cross-replica authority movement is
// authored by TopologyController, not by StaticDirective.
//
// The route-helper below supports BOTH move modes:
//   - EpochBumpMove (Reassign on same replica): exercises the
//     per-op lineage fence against Epoch drift — the fence
//     invariant the test really needs to prove at L1.
//   - ReplicaMove   (cross-replica via a second Bind on a
//     different key): exercises the bridge fan-in picking the
//     new replica as primary.
// T1.L1.1 uses the epoch-bump mode; the ReplicaID-change arm is
// left for L2 over the real product route (per sketch §11.L2).
package frontend_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// latestLineageView captures the most-recent AssignmentInfo the
// bridge has delivered for one volume, and exposes it as a
// frontend.ProjectionView. Healthy=true once any AssignmentInfo
// has landed — that's the T1 minimum readiness bridge sketch
// §11.L2 refers to.
type latestLineageView struct {
	volumeID string

	mu   sync.Mutex
	seen bool
	info adapter.AssignmentInfo
}

func newLatestLineageView(volumeID string) *latestLineageView {
	return &latestLineageView{volumeID: volumeID}
}

// OnAssignment satisfies authority.AssignmentConsumer. Returns an
// empty ApplyLog — the view only observes lineage, it does not
// drive engine events.
func (v *latestLineageView) OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog {
	if info.VolumeID != v.volumeID {
		return adapter.ApplyLog{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	// Latest-wins by (Epoch, EndpointVersion). Lower-epoch stale
	// deliveries are discarded; same lineage is idempotent.
	if v.seen {
		if info.Epoch < v.info.Epoch {
			return adapter.ApplyLog{}
		}
		if info.Epoch == v.info.Epoch && info.EndpointVersion < v.info.EndpointVersion {
			return adapter.ApplyLog{}
		}
	}
	v.info = info
	v.seen = true
	return adapter.ApplyLog{}
}

// Projection implements frontend.ProjectionView.
func (v *latestLineageView) Projection() frontend.Projection {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.seen {
		return frontend.Projection{VolumeID: v.volumeID, Healthy: false}
	}
	return frontend.Projection{
		VolumeID:        v.info.VolumeID,
		ReplicaID:       v.info.ReplicaID,
		Epoch:           v.info.Epoch,
		EndpointVersion: v.info.EndpointVersion,
		Healthy:         true,
	}
}

// currentLineage returns the latest seen info for test assertions.
func (v *latestLineageView) currentLineage() (adapter.AssignmentInfo, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.info, v.seen
}

// t1Route bundles the in-process route components so the test
// can close them together.
type t1Route struct {
	pub    *authority.Publisher
	dir    *authority.StaticDirective
	view   *latestLineageView
	cancel context.CancelFunc
	done   chan struct{}
}

func (r *t1Route) Close() {
	r.cancel()
	<-r.done
}

// newT1Route wires Publisher + StaticDirective + VolumeBridge
// against a single volume and returns the route handle.
func newT1Route(t *testing.T, volumeID string, replicaIDs []string) *t1Route {
	t.Helper()
	dir := authority.NewStaticDirective(nil)
	pub := authority.NewPublisher(dir)
	view := newLatestLineageView(volumeID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = pub.Run(ctx)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			authority.VolumeBridge(ctx, pub, view, volumeID, replicaIDs...)
		}()
		wg.Wait()
	}()

	return &t1Route{
		pub:    pub,
		dir:    dir,
		view:   view,
		cancel: cancel,
		done:   done,
	}
}

// waitUntilHealthy polls the view until Healthy=true or deadline.
func (r *t1Route) waitUntilHealthy(t *testing.T, deadline time.Duration) frontend.Projection {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		p := r.view.Projection()
		if p.Healthy {
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("route did not reach Healthy within %v", deadline)
	return frontend.Projection{}
}

// waitUntilEpochAdvances polls the view until the published
// Epoch > from, or deadline.
func (r *t1Route) waitUntilEpochAdvances(t *testing.T, from uint64, deadline time.Duration) frontend.Projection {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		p := r.view.Projection()
		if p.Healthy && p.Epoch > from {
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("projection Epoch did not advance past %d within %v", from, deadline)
	return frontend.Projection{}
}
