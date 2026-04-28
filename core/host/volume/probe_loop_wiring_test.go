package volume

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// G5-5C #5 wiring tests for ProductionProbeFn: verify the production
// probeFn closure forwards adapter.ProbeResult into the
// VolumeReplicaAdapter and translates transport outcome into the
// (nil err / non-nil err) signal the probe loop needs for backoff.
//
// We don't stand up a real ReplicaListener — the focus here is on
// the wiring's contract, not on transport behavior. peer.Executor()
// returns whatever was injected at NewReplicaPeer; we stub that with
// a fake executor that records dispatch and returns canned results.

// fakeExecutorAdapter reuses the package-internal noopExecutor as
// the adapter.CommandExecutor seam for these tests — it satisfies
// the full interface without us re-implementing it.
func fakeExecutorAdapter() adapter.CommandExecutor {
	return newNoopExecutor()
}

// staticRouter returns a closure that returns the supplied adapter
// regardless of replicaID. Test seam.
func staticRouter(a *adapter.VolumeReplicaAdapter) AdapterRouter {
	return func(_ string) *adapter.VolumeReplicaAdapter { return a }
}

// keyedRouter returns a closure backed by a map keyed by replicaID.
// Test seam.
func keyedRouter(m map[string]*adapter.VolumeReplicaAdapter) AdapterRouter {
	return func(id string) *adapter.VolumeReplicaAdapter { return m[id] }
}

// TestProductionProbeFn_NilRouter_ReturnsErrAndDoesNotPanic verifies
// the fail-closed path when the wiring is misconfigured. probeFn
// must not panic on a nil router; it returns an error so the loop's
// backoff advances and the misconfiguration is loud in logs.
func TestProductionProbeFn_NilRouter_ReturnsErrAndDoesNotPanic(t *testing.T) {
	fn := ProductionProbeFn(nil)
	peer := newPeerForWiringTest(t, "r1", "127.0.0.1:9001", "127.0.0.1:9001", 1, 1)
	err := fn(context.Background(), peer)
	if err == nil {
		t.Fatal("nil router should produce a non-nil err return")
	}
}

// TestProductionProbeFn_RouterReturnsNil_ReturnsErr verifies that a
// router returning nil for a peer (e.g., registry not synced yet)
// produces a non-nil err so backoff advances and the log is loud.
// G5-5C Batch #7: this is the new fail-closed path under the router
// signature.
func TestProductionProbeFn_RouterReturnsNil_ReturnsErr(t *testing.T) {
	router := func(_ string) *adapter.VolumeReplicaAdapter { return nil }
	fn := ProductionProbeFn(router)
	peer := newPeerForWiringTest(t, "r1", "127.0.0.1:9001", "127.0.0.1:9001", 1, 1)

	err := fn(context.Background(), peer)
	if err == nil {
		t.Fatal("router returning nil for peer should produce non-nil err")
	}
}

// TestProductionProbeFn_RouterPicksByReplicaID verifies that the
// router selects the right per-peer adapter based on the peer's
// ReplicaID. G5-5C Batch #7: pins the routing contract — without
// this, the per-peer-adapter wiring could regress to "always pick
// the same adapter" and engine.checkReplicaID would drop events.
func TestProductionProbeFn_RouterPicksByReplicaID(t *testing.T) {
	r1Adpt := adapter.NewVolumeReplicaAdapter(fakeExecutorAdapter())
	r1Adpt.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "127.0.0.1:1", CtrlAddr: "127.0.0.1:1",
	})
	r2Adpt := adapter.NewVolumeReplicaAdapter(fakeExecutorAdapter())
	r2Adpt.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r2",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "127.0.0.1:1", CtrlAddr: "127.0.0.1:1",
	})

	router := keyedRouter(map[string]*adapter.VolumeReplicaAdapter{
		"r1": r1Adpt,
		"r2": r2Adpt,
	})
	fn := ProductionProbeFn(router)

	// Probe transport will fail for both (unreachable :1) — that's
	// fine; we're verifying routing happened, not transport success.
	peer1 := newPeerForWiringTest(t, "r1", "127.0.0.1:1", "127.0.0.1:1", 1, 1)
	peer2 := newPeerForWiringTest(t, "r2", "127.0.0.1:1", "127.0.0.1:1", 1, 1)

	if err := fn(context.Background(), peer1); err == nil {
		t.Fatal("expected probe transport failure")
	}
	if err := fn(context.Background(), peer2); err == nil {
		t.Fatal("expected probe transport failure")
	}

	// Both adapters should have received their respective failure
	// facts (NormalizeProbe → ProbeFailed event) — proven by the
	// adapter's CommandLog containing batch:ProbeFailed entries.
	r1Log := r1Adpt.CommandLog()
	r2Log := r2Adpt.CommandLog()
	if len(r1Log) == 0 {
		t.Errorf("r1 adapter: expected commands logged after probe, got none")
	}
	if len(r2Log) == 0 {
		t.Errorf("r2 adapter: expected commands logged after probe, got none")
	}
}

// newPeerForWiringTest builds a real *ReplicaPeer with valid lineage
// pointing at the given (data, ctrl) addresses. The probeFn will dial
// those addresses; tests that don't want real wire activity should
// use addresses that immediately fail (e.g. ":1" or unreachable).
func newPeerForWiringTest(t *testing.T, id, dataAddr, ctrlAddr string, epoch, ev uint64) *replication.ReplicaPeer {
	t.Helper()
	target := replication.ReplicaTarget{
		ReplicaID:       id,
		DataAddr:        dataAddr,
		ControlAddr:     ctrlAddr,
		Epoch:           epoch,
		EndpointVersion: ev,
	}
	// Probe needs a non-nil primaryStore for Boundaries(); use a
	// minimal in-memory store. We never write through it in these
	// wiring tests.
	primary := storage.NewBlockStore(8, 4096)
	exec := transport.NewBlockExecutor(primary, dataAddr)
	peer, err := replication.NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatalf("NewReplicaPeer: %v", err)
	}
	return peer
}

// TestProductionProbeFn_TransportFailure_ReturnsErr verifies architect
// Batch #5 self-check #3: a wire-level probe failure (peer
// unreachable) translates to a non-nil err so the probe loop's
// ResultFn (DefaultProbeResultFn) calls OnProbeAttempt(false) and
// advances per-peer backoff.
//
// Mechanism: dial to an unreachable port returns a failed
// adapter.ProbeResult (Success=false). ProductionProbeFn forwards
// the failure to OnProbeResult AND returns a non-nil err.
func TestProductionProbeFn_TransportFailure_ReturnsErr(t *testing.T) {
	adpt := adapter.NewVolumeReplicaAdapter(fakeExecutorAdapter())
	// Prime the adapter with an assignment so OnProbeResult has a
	// known peer to associate with.
	adpt.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "127.0.0.1:1", CtrlAddr: "127.0.0.1:1",
	})
	fn := ProductionProbeFn(staticRouter(adpt))

	// Use a definitely-unreachable address. 127.0.0.1:1 is reserved
	// (TCPMUX) and almost always closed; the dial will fail fast.
	peer := newPeerForWiringTest(t, "r1", "127.0.0.1:1", "127.0.0.1:1", 1, 1)

	err := fn(context.Background(), peer)
	if err == nil {
		t.Fatal("transport failure should produce a non-nil err return (backoff advance)")
	}
}

// TestProductionProbeFn_ContextCancelled_ReturnsErr verifies the
// guard: an already-cancelled context short-circuits before any
// wire dispatch, returning ctx.Err.
func TestProductionProbeFn_ContextCancelled_ReturnsErr(t *testing.T) {
	adpt := adapter.NewVolumeReplicaAdapter(fakeExecutorAdapter())
	fn := ProductionProbeFn(staticRouter(adpt))
	peer := newPeerForWiringTest(t, "r1", "127.0.0.1:1", "127.0.0.1:1", 1, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := fn(ctx, peer)
	if err == nil {
		t.Fatal("cancelled context should produce a non-nil err")
	}
}
