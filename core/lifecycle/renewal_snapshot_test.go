package lifecycle

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// The reservation snapshot mirrors records at every assignment: after a
// durable write, on a protective change kept in memory on an unknown outcome
// (fail closed), never on a known no-write, and at load. Snapshots are
// immutable, token slices included (issue #205).
func TestDeletionReservationSnapshotFollowsRecords(t *testing.T) {
	fail := errors.New("injected")
	dir := t.TempDir()
	s := reopen(t, dir)
	same := func(what string, st *DeletionReservationStore, vols ...string) {
		t.Helper()
		for _, v := range vols {
			if st.ReservedSnapshot(v) != st.Reserved(v) {
				t.Fatalf("%s: snapshot of %q = %t, records = %t", what, v, st.ReservedSnapshot(v), st.Reserved(v))
			}
		}
	}
	if s.ReservedSnapshot("v") {
		t.Fatal("empty store reports a reservation")
	}
	if _, err := s.Admit("v", "d1"); err != nil {
		t.Fatal(err)
	}
	if !s.ReservedSnapshot("v") {
		t.Fatal("durable admission not visible to the snapshot")
	}
	s.failWrite = func() error { return fail }
	if _, err := s.Admit("w", "d1"); !errors.Is(err, fail) || s.ReservedSnapshot("w") {
		t.Fatalf("known no-write: %v; snapshot must not show it", err)
	}
	s.failWrite = nil
	s.failRename = func() error { return fail }
	if _, err := s.Admit("r", "d1"); !errors.Is(err, fail) || !s.ReservedSnapshot("r") {
		t.Fatalf("unknown outcome of a protective change: %v; the snapshot must keep the exclusion (fail closed)", err)
	}
	s.failRename = nil
	s.failDirSync = func() error { return fail }
	if _, err := s.Admit("y", "d1"); !errors.Is(err, fail) || !s.ReservedSnapshot("y") {
		t.Fatalf("unknown outcome (directory sync): %v; the snapshot must keep the exclusion", err)
	}
	s.failDirSync = nil
	same("after faults", s, "v", "w", "r", "y")

	// Immutability: a published snapshot does not change when records do.
	before := s.snap.Load()
	tokens := append([]string(nil), (*before)["v"].Tokens...)
	if _, err := s.Admit("v", "d2"); err != nil {
		t.Fatal(err)
	}
	if got := (*before)["v"].Tokens; fmt.Sprint(got) != fmt.Sprint(tokens) {
		t.Fatalf("an earlier snapshot changed: %v, was %v", got, tokens)
	}
	after := s.snap.Load()
	(*after)["v"].Tokens[0] = "mutated"
	if got, _ := s.Get("v"); got.Tokens[0] == "mutated" {
		t.Fatal("snapshot token slice aliases the records")
	}
	if _, err := s.Abort("v", "d1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Abort("v", "d2"); err != nil || s.ReservedSnapshot("v") {
		t.Fatalf("release of the last token: %v; snapshot must drop the reservation", err)
	}
	same("after release", s, "v", "w", "r", "y")
	r := reopen(t, dir) // load publishes too
	same("after reopen", r, "v", "w", "r", "y")
}

// The Volumes snapshot follows every change of records, including load and
// the memory-first delete.
func TestFileStoreSnapshotFollowsRecords(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetVolumeSnapshot("a"); ok {
		t.Fatal("empty store reports a volume")
	}
	if _, err := s.CreateVolume(VolumeSpec{VolumeID: "a", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	if rec, ok := s.GetVolumeSnapshot("a"); !ok || rec.Spec.ReplicationFactor != 1 {
		t.Fatalf("create not visible: %+v %t", rec, ok)
	}
	if _, err := s.AttachVolume("a", "n1"); err != nil {
		t.Fatal(err)
	}
	if rec, _ := s.GetVolumeSnapshot("a"); rec.AttachedTo != "n1" {
		t.Fatalf("attach not visible: %+v", rec)
	}
	if _, err := s.DetachVolume("a", "n1"); err != nil {
		t.Fatal(err)
	}
	if rec, _ := s.GetVolumeSnapshot("a"); rec.AttachedTo != "" {
		t.Fatalf("detach not visible: %+v", rec)
	}
	r, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.GetVolumeSnapshot("a"); !ok {
		t.Fatal("load did not publish")
	}
	if err := s.DeleteVolume("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetVolumeSnapshot("a"); ok {
		t.Fatal("delete not visible")
	}
}

// Snapshot reads never take the store mutex that writers hold across file
// I/O: they return while the mutex is held.
func TestLifecycleSnapshotReadsDoNotTakeStoreMutex(t *testing.T) {
	vols, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vols.CreateVolume(VolumeSpec{VolumeID: "a", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	res := reopen(t, t.TempDir())
	if _, err := res.Admit("x", "d"); err != nil {
		t.Fatal(err)
	}
	vols.mu.Lock()
	res.mu.Lock()
	done := make(chan bool, 1)
	go func() {
		_, okV := vols.GetVolumeSnapshot("a")
		done <- okV && res.ReservedSnapshot("x")
	}()
	select {
	case ok := <-done:
		res.mu.Unlock()
		vols.mu.Unlock()
		if !ok {
			t.Fatal("snapshot read returned the wrong state")
		}
	case <-time.After(2 * time.Second):
		res.mu.Unlock()
		vols.mu.Unlock()
		<-done
		t.Fatal("snapshot read waited on the store mutex")
	}
}

// Snapshot readers run concurrently with every writer kind (run with -race).
func TestLifecycleSnapshotConcurrentWriters(t *testing.T) {
	vols, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	res := reopen(t, t.TempDir())
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			v := fmt.Sprintf("v%d", i%4)
			_, _ = vols.CreateVolume(VolumeSpec{VolumeID: v, SizeBytes: 1 << 20, ReplicationFactor: 1})
			_ = vols.DeleteVolume(v)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			v := fmt.Sprintf("r%d", i%4)
			_, _ = res.Admit(v, "d1")
			_, _ = res.Admit(v, "d2")
			_, _ = res.Abort(v, "d1")
			if i%3 == 0 {
				_, _ = res.Commit(v, "d2")
			} else {
				_, _ = res.Abort(v, "d2")
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < 4; i++ {
				_, _ = vols.GetVolumeSnapshot(fmt.Sprintf("v%d", i))
				_ = res.ReservedSnapshot(fmt.Sprintf("r%d", i))
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}
