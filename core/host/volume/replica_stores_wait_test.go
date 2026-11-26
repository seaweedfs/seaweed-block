package volume

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// A new volume is not exposed before its replicas' stores are attested:
// while the master's fact awaits any initial replica's store, the primary's
// projection is not Healthy, writes through the real backend refuse, and
// status says "creating: waiting for replica attestations (n of N)". The
// same-generation re-send with nothing awaited opens it.
// requiringReplication is a replication slot that counts peers only with
// recorded stores (as replication.ReplicationVolume does on a volume host).
type requiringReplication struct{ *stubReplication }

func (requiringReplication) RequireAttestedStores() {}

func TestNewVolumePrimaryServesNothingBeforeReplicaAttestations(t *testing.T) {
	h, _, rep := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.replicationMu.Lock()
	h.setReplicationLocked(requiringReplication{rep})
	h.replicationMu.Unlock()
	proj := &mutableProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 2, EndpointVersion: 1}}
	h.view = NewAdapterProjectionView(proj, "vol1", "rB", h)
	if err := h.SetSelfFence(&switchFencer{}); err != nil {
		t.Fatal(err)
	}
	store := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = store.Close() })
	backend := durable.NewStorageBackend(store, h.view, frontend.Identity{VolumeID: "vol1", ReplicaID: "rB", Epoch: 2, EndpointVersion: 1})
	backend.SetOperational(true, "replica stores wait")
	ctx := context.Background()

	fact := selfFact(2)
	fact.ReplicaStoresAwaited, fact.ReplicaStoresInitial = 2, 3
	h.applyFixtureFact(fact)
	if h.view.Projection().Healthy {
		t.Fatal("a new volume's primary is Healthy before its replicas' stores are recorded")
	}
	if _, err := backend.Write(ctx, 0, []byte("early")); err == nil {
		t.Fatal("a write was served before the replicas' stores were recorded")
	}
	s := NewStatusServer(h.view).statusProjection()
	if s.FrontendPrimaryReady || !strings.Contains(s.NotReadyReason, "creating: waiting for replica attestations (1 of 3)") {
		t.Fatalf("status: ready=%t reason=%q", s.FrontendPrimaryReady, s.NotReadyReason)
	}

	fact = selfFact(2) // the master's re-send once every store is recorded
	h.applyFixtureFact(fact)
	if !h.view.Projection().Healthy {
		t.Fatal("not Healthy once nothing is awaited")
	}
	if _, err := backend.Write(ctx, 0, []byte("open")); err != nil {
		t.Fatalf("write once nothing is awaited: %v", err)
	}
}

// A host whose replication slot does not require recorded stores counts no
// peer ack by store, so the wait does not apply to it: the fact's count
// does not close its frontend.
func TestReplicaStoresWaitOnlyWhereStoresAreRequired(t *testing.T) {
	h, _, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	fact := selfFact(2)
	fact.ReplicaStoresAwaited, fact.ReplicaStoresInitial = 2, 2
	h.noteReplicaStores(fact)
	if reason := h.ReplicaStoresAwaited(); reason != "" {
		t.Fatalf("a host without a store-requiring slot waits: %q", reason)
	}
}

// The probe answer says the primary awaits replica stores with the epoch
// of the line whose assignment awaits them; a new line (another epoch) that
// never waited says nothing.
func TestProbeAnswerNamesTheLineAwaitingReplicaStores(t *testing.T) {
	h, _, rep := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.replicationMu.Lock()
	h.setReplicationLocked(requiringReplication{rep})
	h.replicationMu.Unlock()
	fact := selfFact(3)
	fact.ReplicaStoresAwaited, fact.ReplicaStoresInitial = 2, 3
	h.noteReplicaStores(fact)
	if got := h.probeAnswer(1).GetAwaitingReplicaStoresEpoch(); got != 3 {
		t.Fatalf("awaiting epoch %d inside the wait; want 3 (the line's epoch)", got)
	}
	h.noteReplicaStores(selfFact(4)) // a new line, which never waited
	if got := h.probeAnswer(2).GetAwaitingReplicaStoresEpoch(); got != 0 {
		t.Fatalf("awaiting epoch %d on a line that never waited; want 0", got)
	}
}

// The probe answer reads the wait before readiness: a stores-complete
// assignment that lands between the two reads still leaves the not-ready
// answer naming the wait it was given in (the frontend gate read the same
// wait), never "not ready, not awaiting".
func TestProbeAnswerReadsTheWaitBeforeReadiness(t *testing.T) {
	h, _, rep := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.replicationMu.Lock()
	h.setReplicationLocked(requiringReplication{rep})
	h.replicationMu.Unlock()
	fact := selfFact(3)
	fact.ReplicaStoresAwaited, fact.ReplicaStoresInitial = 1, 3
	h.noteReplicaStores(fact)
	h.SetProbeRoleSource(func() (bool, bool) {
		h.noteReplicaStores(selfFact(3)) // the stores-complete assignment lands mid-probe
		return false, false            // readiness was read while the wait still held
	})
	if got := h.probeAnswer(1).GetAwaitingReplicaStoresEpoch(); got != 3 {
		t.Fatalf("not-ready answer with the wait released mid-probe: awaiting epoch %d; want 3", got)
	}
}

// No sample is needed to end the excuse: once the assignment carries every
// store, a not-ready answer (a real failure, whether or not any probe or
// heartbeat saw the frontend open in between) names no wait.
func TestProbeAnswerAfterTheWaitNamesNothing(t *testing.T) {
	h, _, rep := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.replicationMu.Lock()
	h.setReplicationLocked(requiringReplication{rep})
	h.replicationMu.Unlock()
	fact := selfFact(3)
	fact.ReplicaStoresAwaited, fact.ReplicaStoresInitial = 1, 3
	h.noteReplicaStores(fact)
	h.noteReplicaStores(selfFact(3)) // stores complete; no probe or heartbeat in between
	h.SetProbeRoleSource(func() (bool, bool) { return false, false }) // then a real failure
	if got := h.probeAnswer(1).GetAwaitingReplicaStoresEpoch(); got != 0 {
		t.Fatalf("not ready after the wait ended: awaiting epoch %d; want 0 (a real not-ready)", got)
	}
}

// orderedReplication records whether the frontend gate was still closed
// when the peers' expected stores were installed.
type orderedReplication struct {
	*stubReplication
	h            *Host
	gateWasShut  bool
	installCalls int
}

func (r *orderedReplication) RequireAttestedStores() {}
func (r *orderedReplication) UpdateReplicaSet(gen uint64, targets []replication.ReplicaTarget) error {
	r.installCalls++
	r.gateWasShut = r.h.ReplicaStoresAwaited() != ""
	return r.stubReplication.UpdateReplicaSet(gen, targets)
}

// A stores-complete assignment opens the frontend only after the peers'
// expected stores are installed: no write is served before its peers can
// count.
func TestStoresCompleteAssignmentOpensAfterThePeersAreInstalled(t *testing.T) {
	h, _, rep := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	r := &orderedReplication{stubReplication: rep, h: h}
	h.replicationMu.Lock()
	h.setReplicationLocked(r)
	h.replicationMu.Unlock()
	if err := h.SetSelfFence(&switchFencer{}); err != nil {
		t.Fatal(err)
	}
	waiting := selfFact(2)
	waiting.ReplicaStoresAwaited, waiting.ReplicaStoresInitial = 1, 3
	h.applyFixtureFact(waiting)
	h.applyFixtureFact(selfFact(2)) // stores complete
	if r.installCalls < 2 || !r.gateWasShut {
		t.Fatalf("install calls %d, gate shut at the stores-complete install %t; want the gate still shut", r.installCalls, r.gateWasShut)
	}
	if reason := h.ReplicaStoresAwaited(); reason != "" {
		t.Fatalf("after the install: %q; want the frontend gate open", reason)
	}
}
