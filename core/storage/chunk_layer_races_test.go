package storage

// Deterministic interleavings for races the C1-C11 contract did not pin:
// presence persistence ordering, lazy-fill generations, stale-epoch
// attachment and capture admission versus Close. Each schedule is forced
// with a test-only hook (chunkLayerImpl.test*Hook) or a held remote call.

import (
	"bytes"
	"crypto/sha256"
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
			if got := layer.Fenced(); got != tc.wantFenced {
				t.Fatalf("right after attach: fenced %v, want %v (a superseding HEAD must fence at attach)", got, tc.wantFenced)
			}
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

// A presence replacement is not saved until its directory entry is durable.
// With the directory open or fsync failing, the checkpoint that needs the
// presence save must not advance (checkpoint and physical WAL tail
// unchanged); after the fault clears, a retry advances it and the local
// write inside the evicted chunk survives a crash.
func TestChunkLayer_FailedDirSyncLeavesPresenceUnsaved(t *testing.T) {
	for _, fault := range []string{"directory open fails", "directory fsync fails"} {
		t.Run(fault, func(t *testing.T) {
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
			if err := layer.EvictChunk(2); err != nil {
				t.Fatal(err)
			}
			impl := layer.(*chunkLayerImpl)
			impl.syncDirFn = func(string) error { return errors.New("injected: " + fault) }

			newer := makeBlock(ctBlockSize, 0xB9)
			lsn, err := s.Write(9, newer) // a local write inside the evicted chunk
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			cpBefore, tailBefore := s.CheckpointLSN(), walLogicalTail(s)
			if err := flushWithin(t, s, "flush needing a presence save"); err == nil {
				t.Fatal("the flush advanced although the presence replacement's directory entry is not durable")
			}
			if cp, tail := s.CheckpointLSN(), walLogicalTail(s); cp != cpBefore || tail != tailBefore {
				t.Fatalf("failed directory sync still moved checkpoint %d -> %d or WAL tail %d -> %d", cpBefore, cp, tailBefore, tail)
			}

			impl.syncDirFn = syncDir // the fault clears: a retry must succeed
			if err := flushWithin(t, s, "retried flush"); err != nil {
				t.Fatalf("retry after the directory fault cleared: %v", err)
			}
			if cp := s.CheckpointLSN(); cp < lsn {
				t.Fatalf("retried checkpoint %d did not pass LSN %d", cp, lsn)
			}
			crash()
			s2, _ := reopen(t, path)
			mustAttach(t, s2, f, cfg)
			if got, err := s2.Read(9); err != nil || !bytes.Equal(got, newer) {
				t.Fatalf("after restart LBA 9 is not the local write: %v", err)
			}
		})
	}
}

// A fill whose HEAD ref went stale must not authorize reading the unfilled
// extent. Schedule: a fresh host's read of chunk 2 holds its fetch of H1; a
// competing holder publishes H2 with different chunk 2 bytes, and the
// layer's failed CAS adopts H2; the held fetch resumes. The held read
// itself must return H2's block or an error: never the zero extent, never H1.
func TestChunkLayer_StaleFillNeverAuthorizesUnresolvedExtentRead(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := uint32(8); lba < 12; lba++ { // chunk 2 in H1
		copy(src[int(lba)*ctBlockSize:], makeBlock(ctBlockSize, byte(0x80+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 5, 1)
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, f, cfg)

	held, releaseA := holdNextGet(t, f)
	type res struct {
		b   []byte
		err error
	}
	readA := make(chan res, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		releaseA()
		select {
		case <-joined:
		case <-time.After(ctLocalBound):
			t.Errorf("held read not joined")
		}
	})
	go func() {
		defer close(joined)
		b, err := s.Read(9)
		readA <- res{b, err}
	}()
	waitKey(t, held, "fetch of chunk 2 from H1")

	h2 := make([]byte, ctChunkBytes)
	for i := 0; i < ctChunkBlocks; i++ {
		copy(h2[i*ctBlockSize:], makeBlock(ctBlockSize, byte(0xC0+i)))
	}
	if _, err := s.Write(20, makeBlock(ctBlockSize, 0x20)); err != nil { // something for the cut to publish
		t.Fatal(err)
	}
	f.mu.Lock()
	f.beforeCAS = func(f *fakeChunkRemote) { // a competing holder publishes H2 first
		f.objs["h2/chunk2"] = append([]byte(nil), h2...)
		f.everStored["h2/chunk2"] = true
		next := copyChunkHead(f.head)
		next.Epoch, next.Holder = 3, "host-b"
		next.Chunks[2] = chunkRef{Key: "h2/chunk2", SHA256: sha256.Sum256(h2)}
		f.installLocked(next)
	}
	f.mu.Unlock()
	if _, err := layer.Capture(ctTimeout(t)); err == nil {
		t.Fatal("the cut succeeded although a newer holder published first")
	}
	releaseA()
	select {
	case r := <-readA:
		want := h2[1*ctBlockSize : 2*ctBlockSize] // LBA 9 is block 1 of chunk 2
		switch {
		case r.err != nil:
			// allowed: the read refuses instead of guessing
		case bytes.Equal(r.b, make([]byte, ctBlockSize)):
			t.Fatal("the held read returned the zero extent after its fill went stale")
		case bytes.Equal(r.b, blockOf(src, 9)):
			t.Fatal("the held read returned H1's block although HEAD is now H2")
		case !bytes.Equal(r.b, want):
			t.Fatalf("the held read returned neither H2's block nor an error (byte %#x)", r.b[0])
		}
	case <-time.After(ctLocalBound):
		t.Fatal("the held read did not finish after its fetch was released")
	}
	if got, err := s.Read(10); err != nil || !bytes.Equal(got, h2[2*ctBlockSize:3*ctBlockSize]) {
		t.Fatalf("after the stale fill, LBA 10 is not H2's block: %v", err)
	}
}

// A fresh host's first cut runs as a hash compare (no usable bitmap). It
// must not fetch chunks that are absent and unwritten: their content is
// HEAD's by definition. Only the chunk written locally is published; the
// untouched chunks keep their HEAD refs.
func TestChunkLayer_FreshHostHashCutFetchesNoUntouchedChunk(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := uint32(0); lba < 16; lba++ { // chunks 0-3 in HEAD
		copy(src[int(lba)*ctBlockSize:], makeBlock(ctBlockSize, byte(0x70+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 5, 1)
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, f, cfg)
	if _, err := s.Write(20, makeBlock(ctBlockSize, 0x20)); err != nil { // chunk 5, ZERO in HEAD
		t.Fatal(err)
	}
	before := f.stats()
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("first cut on a fresh host: %v", err)
	}
	after := f.stats()
	if after.gets != before.gets {
		t.Fatalf("the first cut fetched %d chunks; untouched absent chunks must not be fetched", after.gets-before.gets)
	}
	if after.puts-before.puts != 1 {
		t.Fatalf("the first cut stored %d chunks, want 1 (chunk 5)", after.puts-before.puts)
	}
	for idx := uint64(0); idx < 4; idx++ {
		if after.head.Chunks[idx] != before.head.Chunks[idx] {
			t.Fatalf("untouched chunk %d lost its HEAD ref", idx)
		}
	}
}
