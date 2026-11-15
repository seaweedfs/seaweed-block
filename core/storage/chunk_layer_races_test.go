package storage

// Deterministic interleavings for races the C1-C11 contract did not pin:
// presence persistence ordering, lazy-fill generations, stale-epoch
// attachment and capture admission versus Close. Each schedule is forced
// with a test-only hook (chunkLayerImpl.test*Hook) or a held remote call.

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

// pauseOnce returns a hook that parks its first caller until release, and
// signals entered when it parks. Later callers pass through.
func pauseOnce(t *testing.T) (hook func(), entered chan struct{}, release func()) {
	t.Helper()
	entered = make(chan struct{}, 1)
	gate := make(chan struct{})
	var first sync.Once
	var rel sync.Once
	release = func() { rel.Do(func() { close(gate) }) }
	t.Cleanup(release)
	onUnblock(t, release)
	hook = func() {
		parked := false
		first.Do(func() { parked = true })
		if !parked {
			return
		}
		entered <- struct{}{}
		<-gate
	}
	return hook, entered, release
}

func waitSignal(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(ctLocalBound):
		t.Fatalf("%s did not happen", what)
	}
}

// An eviction's presence snapshot must not overwrite a newer save. Schedule:
// the eviction snapshots "chunk 2 absent, no local blocks" and pauses; a
// write into chunk 2 is flushed, so its checkpoint records "local LBA 9" and
// recycles the WAL entry; the eviction then resumes. After a crash, LBA 9
// must read back as the local write, not be refetched from the older HEAD.
func TestChunkLayer_PresenceSaveCannotOverwriteNewerSave(t *testing.T) {
	s, path, crash := crashableStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	layer := mustAttach(t, s, f, cfg)
	for lba := uint32(8); lba < 12; lba++ {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(0x80+lba))); err != nil {
			t.Fatal(err)
		}
	}
	mustSyncFlush(t, s)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}

	impl := layer.(*chunkLayerImpl)
	hook, parked, release := pauseOnce(t)
	impl.testPresenceSnapshotHook = hook
	evicted := make(chan error, 1)
	go func() { evicted <- layer.EvictChunk(2) }()
	waitSignal(t, parked, "eviction's presence snapshot")

	newer := makeBlock(ctBlockSize, 0xB9)
	lsn, err := s.Write(9, newer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- s.flusher.flushOnce() }()
	select { // the flush either completes now or waits for the eviction save
	case err := <-flushed:
		flushed <- err
	case <-time.After(300 * time.Millisecond):
	}
	release()
	if err := waitErr(t, evicted, "eviction"); err != nil {
		t.Fatalf("evict: %v", err)
	}
	if err := waitErr(t, flushed, "flush"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	impl.testPresenceSnapshotHook = nil
	if cp := s.CheckpointLSN(); cp < lsn {
		t.Fatalf("checkpoint %d did not pass LSN %d; the WAL still holds the write and the schedule proves nothing", cp, lsn)
	}
	crash()
	s2, _ := reopen(t, path)
	mustAttach(t, s2, f, cfg)
	if got, err := s2.Read(9); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("after restart LBA 9 is not the local write (an older presence snapshot replaced the newer one): %v", err)
	}
}

// A fill fetched for an older presence generation must not install. Schedule:
// fetch A of chunk 2 (from H1) is held; fetch B fills the chunk; a write to
// LBA 10 is flushed and published as H2; the chunk is evicted again; then A
// resumes. LBA 10 must read the H2 bytes.
func TestChunkLayer_StaleFillCannotInstallAfterReEviction(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	for lba := uint32(8); lba < 12; lba++ {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(0x80+lba))); err != nil {
			t.Fatal(err)
		}
	}
	mustSyncFlush(t, s)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if err := layer.EvictChunk(2); err != nil {
		t.Fatal(err)
	}

	held, releaseA := holdNextGet(t, f)
	readA := make(chan error, 1)
	joinedA := make(chan struct{})
	t.Cleanup(func() {
		releaseA()
		select {
		case <-joinedA:
		case <-time.After(ctLocalBound):
			t.Errorf("held read A not joined")
		}
	})
	go func() {
		defer close(joinedA)
		_, err := s.Read(8)
		readA <- err
	}()
	waitKey(t, held, "fetch A")
	if got, err := s.Read(9); err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, 0x89)) { // fetch B fills chunk 2
		t.Fatalf("fill B: %v", err)
	}
	x := makeBlock(ctBlockSize, 0xAA)
	if _, err := s.Write(10, x); err != nil {
		t.Fatal(err)
	}
	mustSyncFlush(t, s)
	if _, err := layer.Capture(ctTimeout(t)); err != nil { // H2 carries X
		t.Fatal(err)
	}
	if err := layer.EvictChunk(2); err != nil { // absent again, local overrides cleared
		t.Fatal(err)
	}
	releaseA()
	if err := waitErr(t, readA, "read A"); err != nil {
		t.Logf("read A returned %v (allowed: its fill is stale)", err)
	}
	if got, err := s.Read(10); err != nil || !bytes.Equal(got, x) {
		t.Fatalf("LBA 10 after the stale fill resumed is not H2's block: %v", err)
	}
}

// A layer must not publish over a HEAD that already belongs to a newer epoch
// or a competing holder, even when it attaches with that HEAD's exact
// version (a stale node reopening after a takeover). A new primary with a
// higher epoch may take over.
func TestChunkLayer_StaleAttachDoesNotPublishOverNewerHolder(t *testing.T) {
	seed := make([]byte, ctNumBlocks*ctBlockSize)
	copy(seed, makeBlock(ctBlockSize, 0x01))
	for _, tc := range []struct {
		name               string
		headEpoch, myEpoch uint64
		headHolder         string
		wantFenced         bool
	}{
		{"newer epoch", 2, 1, "host-b", true},
		{"competing holder, same epoch", 1, 1, "host-b", true},
		{"new primary takes over", 1, 2, "host-b", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeChunkRemote()
			f.seedHead(t, seed, 5, tc.headEpoch)
			f.mu.Lock()
			f.head.Holder = tc.headHolder
			f.heads[len(f.heads)-1].Holder = tc.headHolder
			f.mu.Unlock()
			s, _ := newChunkTestStore(t)
			if _, err := s.Write(3, makeBlock(ctBlockSize, 0x33)); err != nil { // stale local change
				t.Fatal(err)
			}
			layer := mustAttach(t, s, f, ctConfig(tc.myEpoch))
			before := f.stats()
			_, err := layer.Capture(ctTimeout(t))
			after := f.stats()
			if !tc.wantFenced {
				if err != nil || after.head.Epoch != tc.myEpoch || after.head.Holder != "host-a" {
					t.Fatalf("takeover by a higher epoch: err %v, HEAD epoch %d holder %q", err, after.head.Epoch, after.head.Holder)
				}
				return
			}
			if !errors.Is(err, errChunkLayerFenced) || !layer.Fenced() {
				t.Fatalf("capture over a newer holder: err %v fenced %v, want fenced", err, layer.Fenced())
			}
			if after.cas != before.cas || after.head.Gen != before.head.Gen || after.head.Holder != tc.headHolder {
				t.Fatalf("stale holder published: %d CAS calls, HEAD gen %d -> %d holder %q", after.cas-before.cas, before.head.Gen, after.head.Gen, after.head.Holder)
			}
		})
	}
}

// Close must wait for a capture that has been admitted, and concurrent Close
// calls all return only after shutdown completes.
func TestChunkLayer_CloseWaitsForAdmittedCapture(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	if _, err := s.Write(1, makeBlock(ctBlockSize, 0x11)); err != nil {
		t.Fatal(err)
	}
	impl := layer.(*chunkLayerImpl)
	hook, parked, release := pauseOnce(t)
	impl.testAfterAdmitHook = hook
	done := startCapture(t, layer)
	waitSignal(t, parked, "admitted capture")
	closed1, closed2 := make(chan struct{}), make(chan struct{})
	go func() { _ = layer.Close(); close(closed1) }()
	go func() { _ = layer.Close(); close(closed2) }()
	select {
	case <-closed1:
		t.Fatal("Close returned while an admitted capture was still running")
	case <-closed2:
		t.Fatal("a concurrent Close returned while an admitted capture was still running")
	case <-time.After(300 * time.Millisecond):
	}
	release()
	_ = waitErr(t, done, "admitted capture")
	for _, c := range []chan struct{}{closed1, closed2} {
		select {
		case <-c:
		case <-time.After(ctLocalBound):
			t.Fatal("Close did not return after the admitted capture finished")
		}
	}
}
