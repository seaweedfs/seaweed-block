package transport

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The replica's half of a fencing round: FenceFloor(e+1) is a durable
// promise. From its return nothing below e+1 is applied or ACKed, across a
// restart; a failed round heals by the same primary's line re-issued at e+1.
func TestFenceFloor_PromiseRefusesBelowAndSurvivesRestart(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	floor := g1Fresh(t)
	ln := g1Listener(t, store, floor)
	if err := ln.AdmitAsReplica(3); err != nil {
		t.Fatal(err)
	}
	if !g1Ship(t, ln, store, g1Lineage(1, 3), 1, 'a') || !g1Barrier(t, ln, g1Lineage(1, 3)) {
		t.Fatal("fixture: the epoch-3 primary's ship and barrier were refused before the fence")
	}
	if err := ln.FenceFloor(4); err != nil {
		t.Fatal(err)
	}
	if got := ln.DurableFloor(); got != 4 || g1Persisted(t, floor) != 4 {
		t.Fatalf("after the fence: durable floor %d, record %d; want 4", got, g1Persisted(t, floor))
	}
	// the model's violating row: the old primary acknowledged after the promise
	if g1Barrier(t, ln, g1Lineage(2, 3)) {
		t.Fatal("an epoch-3 barrier was ACKed after the replica promised floor 4")
	}
	if g1Ship(t, ln, store, g1Lineage(3, 3), 2, 'b') || g1Rebuild(t, ln, store, g1Lineage(4, 3), 'r') {
		t.Fatal("an epoch-3 mutation was applied after the replica promised floor 4")
	}
	ln.StopHard()

	restarted := g1Listener(t, store, floor)
	if err := restarted.AdmitAsReplica(3); err != nil { // the stale line, again
		t.Fatal(err)
	}
	if g1Barrier(t, restarted, g1Lineage(5, 3)) || g1Ship(t, restarted, store, g1Lineage(6, 3), 3, 'c') {
		t.Fatal("after a restart the replica took the epoch-3 primary below its promised floor 4")
	}
	// closure: the round failed, the master re-issues the same primary at 4
	if err := restarted.AdmitAsReplica(4); err != nil {
		t.Fatal(err)
	}
	if !g1Ship(t, restarted, store, g1Lineage(7, 4), 4, 'd') || !g1Barrier(t, restarted, g1Lineage(7, 4)) {
		t.Fatal("the line re-issued at the fenced epoch 4 was refused: a failed round would degrade the volume for ever")
	}
}

// A ship admitted before the fence is still applying when FenceFloor is
// called: the promise is not made until it has applied, so it can never
// apply (or ACK) after the replica answered.
func TestFenceFloor_WaitsForAMutationAlreadyPastTheGate(t *testing.T) {
	listener, store := g2PausedShip(t)
	type result struct {
		at  time.Time
		err error
	}
	fenced := make(chan result, 1)
	go func() {
		err := listener.FenceFloor(2)
		fenced <- result{time.Now(), err}
	}()
	select {
	case r := <-fenced:
		close(store.release)
		t.Fatalf("FenceFloor returned (err=%v) while an admitted epoch-1 ship was still applying", r.err)
	case <-time.After(300 * time.Millisecond):
	}
	// a new epoch-1 frame is refused at once, while the fence drains
	old := RecoveryLineage{SessionID: 5, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(old, 2)) {
		close(store.release)
		t.Fatal("a new epoch-1 ship was admitted while the floor-2 fence was draining")
	}
	close(store.release)
	appliedAt := <-store.applied
	select {
	case r := <-fenced:
		if r.err != nil {
			t.Fatalf("fence: %v", r.err)
		}
		if r.at.Before(appliedAt) {
			t.Fatalf("FenceFloor returned %v before the in-flight apply finished", appliedAt.Sub(r.at))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FenceFloor never returned after the in-flight apply finished")
	}
}

// A fence whose floor cannot be made durable is an error: no promise, the
// record keeps the old floor (a restart is not bound), while this process
// keeps refusing below the fence epoch. A fence never lowers the floor.
func TestFenceFloor_PersistFailureMakesNoPromise(t *testing.T) {
	other := g1Listener(t, storage.NewBlockStore(16, 4096), g1Fresh(t))
	if err := other.AdmitAsReplica(5); err != nil {
		t.Fatal(err)
	}
	if err := other.FenceFloor(4); err != nil {
		t.Fatal(err)
	}
	if got := other.DurableFloor(); got != 5 {
		t.Fatalf("a fence at 4 over floor 5: durable floor %d, want 5 (never lowered)", got)
	}

	store := storage.NewBlockStore(16, 4096)
	floor := g1Fresh(t)
	ln := g1Listener(t, store, floor)
	if err := ln.AdmitAsReplica(3); err != nil {
		t.Fatal(err)
	}
	failAt(t, ln, "write")
	if err := ln.FenceFloor(4); err == nil {
		t.Fatal("FenceFloor returned nil though the floor record could not be written")
	}
	if got := ln.DurableFloor(); got != 3 || g1Persisted(t, floor) != 3 {
		t.Fatalf("after a failed fence: durable floor %d, record %d; want 3", got, g1Persisted(t, floor))
	}
	if g1Barrier(t, ln, g1Lineage(1, 3)) {
		t.Fatal("after a failed fence the epoch-3 barrier was ACKed")
	}

}
