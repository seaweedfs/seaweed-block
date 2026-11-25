package volume

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// fenceListener is the replica listener as a fencing round sees it: its
// durable floor, raised by FenceFloor (or failing).
type fenceListener struct {
	floor   uint64
	fences  []uint64
	failErr error
}

func (f *fenceListener) FenceAsPrimary(uint64) error { return nil }
func (f *fenceListener) AdmitAsReplica(uint64) error { return nil }
func (f *fenceListener) FenceFloor(e uint64) error {
	f.fences = append(f.fences, e)
	if f.failErr != nil {
		return f.failErr
	}
	if e > f.floor {
		f.floor = e
	}
	return nil
}
func (f *fenceListener) DurableFloor() uint64 { return f.floor }

// fenceEvidence is the replication slot: its store and its evidence, which
// records the floor it was read under (the read must follow the fence).
type fenceEvidence struct {
	id      storage.StoreID
	ln      *fenceListener
	end     uint64
	readsAt []uint64 // the listener's floor at each evidence read
}

func (e *fenceEvidence) UpdateReplicaSet(uint64, []replication.ReplicaTarget) error { return nil }
func (e *fenceEvidence) LocalStoreID() storage.StoreID                              { return e.id }
func (e *fenceEvidence) LocalPrimaryEvidence() (replication.EpochTableStatus, uint64, error) {
	e.readsAt = append(e.readsAt, e.ln.floor)
	return replication.EpochTableStatus{}, e.end, nil
}

func fenceHost(t *testing.T) (*Host, *fenceListener, *fenceEvidence) {
	t.Helper()
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "s2", VolumeID: "v1", ReplicaID: "r2", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304", EnableT1Readiness: true})
	if err != nil {
		t.Fatal(err)
	}
	ln := &fenceListener{floor: 3}
	if err := h.SetSelfFence(ln); err != nil {
		t.Fatal(err)
	}
	ev := &fenceEvidence{id: storage.StoreID{7, 7, 7}, ln: ln, end: 42}
	h.replicationMu.Lock()
	h.replication = ev
	h.replicationMu.Unlock()
	return h, ln, ev
}

// A fencing round naming this store: the floor is raised and durable
// BEFORE the evidence is read, and the answer carries that floor and the end
// read under it (the model's fence-before shape; reading first is the
// violating fence-after shape).
func TestFencingRoundRaisesTheFloorBeforeTheEvidence(t *testing.T) {
	h, ln, ev := fenceHost(t)
	a, ok := h.roundAnswer(&control.ProbeRound{VolumeId: "v1", ReplicaId: "r2", RoundId: 9, FenceEpoch: 4, ExpectedStoreId: ev.id[:]})
	if !ok {
		t.Fatal("a completed fence was not answered")
	}
	if len(ln.fences) != 1 || ln.fences[0] != 4 {
		t.Fatalf("fences %v, want one at 4", ln.fences)
	}
	if len(ev.readsAt) != 1 || ev.readsAt[0] != 4 {
		t.Fatalf("evidence read under floor %v; want read once, after the floor reached 4", ev.readsAt)
	}
	if a.GetFloor() != 4 || a.GetRoundId() != 9 || a.GetDurableEnd() != 42 || string(a.GetStoreId()) != string(ev.id[:]) {
		t.Fatalf("answer %+v; want round 9, floor 4, end 42, this store", a)
	}
}

// A fencing round naming another store raises nothing; its answer carries
// the unchanged floor, which the master does not count.
func TestFencingRoundForAnotherStoreRaisesNothing(t *testing.T) {
	h, ln, _ := fenceHost(t)
	other := storage.StoreID{9}
	a, ok := h.roundAnswer(&control.ProbeRound{VolumeId: "v1", ReplicaId: "r2", RoundId: 10, FenceEpoch: 4, ExpectedStoreId: other[:]})
	if !ok || len(ln.fences) != 0 || a.GetFloor() != 3 {
		t.Fatalf("answered=%t fences=%v floor=%d; want an answer at the unchanged floor 3 and no fence", ok, ln.fences, a.GetFloor())
	}
	if _, ok := h.roundAnswer(&control.ProbeRound{VolumeId: "v1", ReplicaId: "r2", RoundId: 11, FenceEpoch: 4}); !ok || len(ln.fences) != 0 {
		t.Fatalf("a fencing round without an expected store fenced: %v", ln.fences)
	}
}

// A fence that fails (drain timeout, floor not durable) is not answered: no
// promise was made, so none may be counted.
func TestFailedFenceIsNotAnswered(t *testing.T) {
	h, ln, ev := fenceHost(t)
	ln.failErr = errors.New("floor record not written")
	if a, ok := h.roundAnswer(&control.ProbeRound{VolumeId: "v1", ReplicaId: "r2", RoundId: 12, FenceEpoch: 4, ExpectedStoreId: ev.id[:]}); ok {
		t.Fatalf("a failed fence was answered: %+v", a)
	}
	if len(ev.readsAt) != 0 {
		t.Fatal("the evidence was read for a round whose fence failed")
	}
}

// A plain round (fence_epoch 0) raises nothing and reports the floor.
func TestPlainRoundRaisesNothing(t *testing.T) {
	h, ln, ev := fenceHost(t)
	a, ok := h.roundAnswer(&control.ProbeRound{VolumeId: "v1", ReplicaId: "r2", RoundId: 13, ExpectedStoreId: ev.id[:]})
	if !ok || len(ln.fences) != 0 || a.GetFloor() != 3 || a.GetDurableEnd() != 42 {
		t.Fatalf("plain round: answered=%t fences=%v answer=%+v", ok, ln.fences, a)
	}
}

// A replica that fenced itself still answers ready: readiness comes from its
// assignment lines and engine projection (StatusServer.ProbeRoles), never
// from the floor, so a fencing round cannot make every target unqualified.
func TestFencedReplicaStillAnswersReady(t *testing.T) {
	h, _, ev := fenceHost(t)
	h.SetProbeRoleSource(func() (bool, bool) { return true, false })
	a, ok := h.roundAnswer(&control.ProbeRound{VolumeId: "v1", ReplicaId: "r2", RoundId: 11, FenceEpoch: 4, ExpectedStoreId: ev.id[:]})
	if !ok || a.GetFloor() != 4 {
		t.Fatalf("fence not answered at floor 4: %+v ok=%t", a, ok)
	}
	if !a.GetReplicaReady() {
		t.Fatalf("a fenced replica answered not ready: %+v", a)
	}
}
