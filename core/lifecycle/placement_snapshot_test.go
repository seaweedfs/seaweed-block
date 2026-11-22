package lifecycle

import (
	"sync"
	"testing"
	"time"
)

// INV-WS-RENEW-NONBLOCKING-001: holder lookup cannot wait behind placement
// store I/O. A committed snapshot is also isolated from caller mutation.
func TestPlacementSnapshotDoesNotWaitForWriter(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenPlacementIntentStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ApplyPlan(PlacementPlan{VolumeID: "v", DesiredRF: 1, Candidates: []PlacementCandidate{{VolumeID: "v", ServerID: "node-a", ReplicaID: "r1", Source: PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock() // the mutex a writer owns across filesystem I/O
	var once sync.Once
	release := func() { once.Do(s.mu.Unlock) }
	result := make(chan PlacementIntent, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		release()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("snapshot reader did not join")
		}
	})
	go func() { defer close(joined); r, _ := s.GetPlacementSnapshot("v"); result <- r }()
	var got PlacementIntent
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("holder snapshot waited for placement writer")
	}
	release()
	if len(got.Slots) != 1 || got.Slots[0].ServerID != "node-a" {
		t.Fatalf("wrong committed holder: %+v", got)
	}
	got.Slots[0].ServerID = "changed-by-reader"
	again, _ := s.GetPlacementSnapshot("v")
	if again.Slots[0].ServerID != "node-a" {
		t.Fatal("caller mutated published holder")
	}
	reopened, err := OpenPlacementIntentStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := reopened.GetPlacementSnapshot("v"); !ok || got.Slots[0].ServerID != "node-a" {
		t.Fatal("reopened snapshot lost holder")
	}
	if err = s.DeletePlacement("v"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetPlacementSnapshot("v"); ok {
		t.Fatal("deleted placement remained published")
	}
}
