package transport

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A fencing round's promise is FenceFloor returning nil, and it is made only
// with the floor durable. Interrupt the persist at each step and restart:
// the record holds the old floor or the fence epoch, never anything else,
// and no step that failed returned a promise. The rename that landed before
// a failed directory sync binds the restart (floor e+1) without a promise
// having been made: conservative, never the reverse.
func TestFenceFloor_InterruptedAtEachPersistStepThenRestart(t *testing.T) {
	for _, tc := range []struct {
		step       string
		afterCrash uint64 // the record a restart loads
	}{
		{"write", 3},
		{"rename", 3},
		{"dirsync", 4},
	} {
		t.Run(tc.step, func(t *testing.T) {
			store := storage.NewBlockStore(16, 4096)
			floor := g1Fresh(t)
			ln := g1Listener(t, store, floor)
			if err := ln.AdmitAsReplica(3); err != nil {
				t.Fatal(err)
			}
			failAt(t, ln, tc.step)
			if err := ln.FenceFloor(4); err == nil {
				t.Fatalf("FenceFloor returned a promise though the %s step of the persist failed", tc.step)
			}
			if got := ln.DurableFloor(); got != 3 {
				t.Fatalf("durable floor after the interrupted fence: %d, want 3 (no promise)", got)
			}
			// the crash: the process dies here; the disk is whatever the step left
			ln.StopHard()
			floorFS = osFloorOps
			if got := g1Persisted(t, floor); got != tc.afterCrash {
				t.Fatalf("record after a crash at %s: %d, want %d", tc.step, got, tc.afterCrash)
			}
			restarted := g1Listener(t, store, floor)
			if err := restarted.AdmitAsReplica(3); err != nil {
				t.Fatal(err)
			}
			if got := restarted.DurableFloor(); got != tc.afterCrash {
				t.Fatalf("restarted durable floor %d, want %d", got, tc.afterCrash)
			}
			below := g1Ship(t, restarted, store, g1Lineage(1, 3), 1, 'a')
			if tc.afterCrash == 4 && below {
				t.Fatal("restarted on a floor-4 record, the replica applied an epoch-3 ship")
			}
			if tc.afterCrash == 3 && !below {
				t.Fatal("control: restarted on a floor-3 record (no promise made), the epoch-3 ship was refused")
			}
			// the round is asked again and now completes: the promise is durable
			if err := restarted.FenceFloor(4); err != nil {
				t.Fatal(err)
			}
			if got := restarted.DurableFloor(); got != 4 || g1Persisted(t, floor) != 4 {
				t.Fatalf("re-asked fence: durable floor %d, record %d; want 4", got, g1Persisted(t, floor))
			}
			if g1Ship(t, restarted, store, g1Lineage(2, 3), 2, 'b') {
				t.Fatal("after the re-asked fence an epoch-3 ship was applied")
			}
		})
	}
}

// A drain that does not finish in time makes no promise: FenceFloor fails,
// the durable floor is unchanged, and a new frame below the fence epoch is
// still refused in this process.
func TestFenceFloor_DrainTimeoutMakesNoPromise(t *testing.T) {
	listener, store := g2PausedShip(t)
	defer close(store.release)
	listener.fenceDrainTimeout = 200 * time.Millisecond
	before := listener.DurableFloor()
	if err := listener.FenceFloor(2); err == nil {
		t.Fatal("FenceFloor returned a promise while an admitted epoch-1 ship was still applying")
	}
	if got := listener.DurableFloor(); got != before {
		t.Fatalf("durable floor after a drain timeout: %d, want %d (no promise)", got, before)
	}
	old := RecoveryLineage{SessionID: 6, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(old, 3)) {
		t.Fatal("after a timed-out fence a new epoch-1 ship was admitted")
	}
}

// Two fencing rounds race (a superseded round and its successor): both
// promises hold, the floor ends at the higher epoch and is never lowered,
// whatever order they finish in.
func TestFenceFloor_ConcurrentRoundsNeverLowerTheFloor(t *testing.T) {
	for i := 0; i < 20; i++ {
		store := storage.NewBlockStore(16, 4096)
		floor := g1Fresh(t)
		ln := g1Listener(t, store, floor)
		if err := ln.AdmitAsReplica(3); err != nil {
			t.Fatal(err)
		}
		errs := make(chan error, 2)
		go func() { errs <- ln.FenceFloor(4) }()
		go func() { errs <- ln.FenceFloor(5) }()
		for j := 0; j < 2; j++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		if got := ln.DurableFloor(); got != 5 || g1Persisted(t, floor) != 5 {
			t.Fatalf("iteration %d: durable floor %d, record %d; want 5", i, got, g1Persisted(t, floor))
		}
		ln.StopHard()
	}
}
