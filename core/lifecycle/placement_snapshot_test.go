package lifecycle

import (
	"os"
	"path/filepath"
	"reflect"
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

// INV-WS-RENEW-NONBLOCKING-001: publication is a committed view, not a
// speculative plan. Force a real replace/remove failure, without a hook.
func TestPlacementSnapshotFailedCommitStaysCommitted(t *testing.T) {
	for _, op := range []string{"create", "replace", "delete"} {
		t.Run(op, func(t *testing.T) {
			dir := t.TempDir()
			s, err := OpenPlacementIntentStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			plan := PlacementPlan{VolumeID: "v", DesiredRF: 1, Candidates: []PlacementCandidate{{VolumeID: "v", ServerID: "node-a", ReplicaID: "r1", Source: PlacementSourceExistingReplica}}}
			path := filepath.Join(dir, "v.json")
			var saved []byte
			if op != "create" {
				if _, err = s.ApplyPlan(plan); err != nil {
					t.Fatal(err)
				}
				saved, err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			before, existed := s.GetPlacementSnapshot("v")
			// A nonempty directory at the record path makes both replace and delete
			// fail after validation, on Windows and Linux, even when run as root.
			if err = os.Mkdir(path, 0755); err != nil {
				t.Fatal(err)
			}
			blocker := filepath.Join(path, "blocker")
			if err = os.WriteFile(blocker, []byte("owned test fixture"), 0600); err != nil {
				t.Fatal(err)
			}
			plan.Candidates[0].ServerID = "node-b"
			if op == "delete" {
				err = s.DeletePlacement("v")
			} else {
				_, err = s.ApplyPlan(plan)
			}
			if err == nil {
				t.Fatal("fixture did not fail commit")
			}
			got, ok := s.GetPlacementSnapshot("v")
			if ok != existed || !reflect.DeepEqual(got, before) {
				t.Fatalf("failed %s published %+v/%t instead of %+v/%t", op, got, ok, before, existed)
			}
			current, present := s.GetPlacement("v")
			if present != existed || !reflect.DeepEqual(current, before) {
				t.Fatal("failed commit changed the store's current placement")
			}
			if err = os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
			if err = os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if saved != nil {
				if err = os.WriteFile(path, saved, 0600); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := OpenPlacementIntentStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			got, ok = reopened.GetPlacementSnapshot("v")
			if ok != existed || !reflect.DeepEqual(got, before) {
				t.Fatal("reopen did not retain the committed placement")
			}
			if _, err = s.ApplyPlan(plan); err != nil {
				t.Fatal("unblocked commit", err)
			}
			got, ok = s.GetPlacementSnapshot("v")
			if !ok || len(got.Slots) != 1 || got.Slots[0].ServerID != "node-b" {
				t.Fatal("committed replacement not published", got, ok)
			}
		})
	}
}
