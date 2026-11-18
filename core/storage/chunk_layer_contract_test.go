package storage

// Contract tests for the chunk layer (chunk_seam.go), invariants C1-C10.
// C11 (persisted dirty bitmap) is in chunk_layer_c11_test.go.
//
// The remote is an asynchronous image published only at quiesced cuts; the
// local WAL and extent stay authoritative. Every test drives the real
// WALStore with the auto flusher stopped, so flush rounds happen exactly
// where a test calls flushOnce. The remote is the in-memory fake in
// chunk_fake_remote_test.go. Gates on the fake are released, background
// captures cancelled and joined, and stores and layers closed with a bound,
// all registered before any worker starts.
//
// Until the layer exists these tests fail at attachChunkLayer with
// errChunkLayerNotImplemented ("RED"). TestChunkFake* pass today.

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// C1 quiesced-cut image: HEAD advances only at quiesced cuts. A Capture
// returns H, the last LSN written before its barrier (synced or not); HEAD
// gets RemoteLSN = H and equals the image at H exactly; HEAD is CAS'd only
// after every captured chunk is stored.
func TestChunkLayer_C1_HeadIsQuiescedCutImage(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	for i, lba := range []uint32{0, 5, 9, 31, 5} { // chunks 0,1,2,7; LBA 5 twice
		w.write(t, s, lba, byte(0x10+i))
	}
	mustSyncFlush(t, s)
	last := w.write(t, s, 13, 0x99) // written before the cut, never synced or flushed

	h, err := layer.Capture(ctTimeout(t))
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if h != last {
		t.Fatalf("Capture returned H=%d, want the last written LSN %d", h, last)
	}
	if r, _, _ := s.Boundaries(); r < h {
		t.Fatalf("after the cut the synced frontier R=%d < H=%d (capture must Sync under its barrier)", r, h)
	}
	st := f.stats()
	if st.head.RemoteLSN != h || layer.RemoteLSN() != h {
		t.Fatalf("remote_lsn HEAD %d layer %d, want H=%d", st.head.RemoteLSN, layer.RemoteLSN(), h)
	}
	if len(st.head.Chunks) != 5 {
		t.Fatalf("HEAD names %d chunks, want 5 (untouched chunks stay ZERO)", len(st.head.Chunks))
	}

	// HEAD must not be CAS'd while a captured chunk is still being stored.
	w.write(t, s, 2, 0x22)
	entered, release := holdPuts(t, f)
	casBefore := f.stats().cas
	done := startCapture(t, layer)
	waitKey(t, entered, "chunk upload")
	if got := f.stats().cas; got != casBefore {
		t.Fatalf("HEAD CAS issued while a chunk upload is in flight (%d new CAS calls)", got-casBefore)
	}
	release()
	if err := waitErr(t, done, "second Capture"); err != nil {
		t.Fatalf("second Capture: %v", err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// C2 L1 unchanged: once a capture's barrier is released, a stalled or failing
// upload gates none of Write, Sync, flush, the checkpoint, WAL recycling or
// R/S/H, and the layer stops its in-flight upload on cancel.
func TestChunkLayer_C2_StalledUploadDoesNotGateLocal(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 1, 0x11)
	mustSyncFlush(t, s)

	entered, release := holdPuts(t, f)
	upCtx, cancel := context.WithCancel(context.Background())
	upDone := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-joined:
		case <-time.After(ctLocalBound):
			t.Errorf("stalled capture not joined within %v", ctLocalBound)
		}
	})
	go func() {
		defer close(joined)
		_, err := layer.Capture(upCtx)
		upDone <- err
	}()
	waitKey(t, entered, "stalled upload") // the barrier is released before any upload

	var lastLSN uint64
	within(t, "local write/sync/flush while the upload is stalled", func() error {
		for i := 0; i < 8; i++ {
			data := makeBlock(ctBlockSize, byte(0x20+i))
			lsn, err := s.Write(uint32(2+i), data)
			if err != nil {
				return err
			}
			w.record(lsn, uint32(2+i), data)
			lastLSN = lsn
			synced, err := s.Sync()
			if err != nil {
				return err
			}
			if synced < lsn {
				return errors.New("Sync returned less than the write it covers")
			}
			if err := s.flusher.flushOnce(); err != nil {
				return err
			}
		}
		return nil
	})
	if cp := s.CheckpointLSN(); cp < lastLSN {
		t.Fatalf("checkpoint %d did not reach %d while the upload stalled", cp, lastLSN)
	}
	if r, tail, h := s.Boundaries(); r < lastLSN || h < lastLSN || tail != s.CheckpointLSN()+1 {
		t.Fatalf("boundaries R=%d S=%d H=%d changed by a stalled upload (checkpoint %d, last %d)", r, tail, h, s.CheckpointLSN(), lastLSN)
	}
	casBefore := f.stats().cas
	cancel()
	if err := waitErr(t, upDone, "cancelled capture"); err == nil {
		t.Fatal("a capture cancelled during its upload reported success")
	}
	release()
	if got := f.stats().cas; got != casBefore {
		t.Fatalf("a cancelled capture issued %d CAS calls", got-casBefore)
	}

	// A failing remote: Capture reports it; local work afterwards is unaffected.
	f.mu.Lock()
	f.failPut = errFakeRemoteDown
	f.mu.Unlock()
	if _, err := layer.Capture(ctTimeout(t)); err == nil {
		t.Fatal("Capture must report a failing remote")
	}
	within(t, "local write/sync/flush after a failed capture", func() error {
		lsn, err := s.Write(20, makeBlock(ctBlockSize, 0x33))
		if err != nil {
			return err
		}
		if synced, err := s.Sync(); err != nil || synced < lsn {
			return errors.New("Sync after a failed capture")
		}
		return s.flusher.flushOnce()
	})
	if got, _ := s.Read(20); !bytes.Equal(got, makeBlock(ctBlockSize, 0x33)) {
		t.Fatal("a failed capture changed local reads")
	}
	if st := f.stats(); st.installed != 0 {
		t.Fatalf("HEAD moved %d times although no capture completed", st.installed)
	}
}

// C3 no torn chunk: chunks are copied under the capture barrier, so every
// HEAD ever installed equals the image at its RemoteLSN even while a writer
// keeps overwriting the same chunk and flushing. Overlap is forced: each
// round holds the upload until the writer has completed rounds after it.
func TestChunkLayer_C3_EveryHeadIsExactUnderConcurrentWrites(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	for lba := uint32(0); lba < ctChunkBlocks; lba++ {
		w.write(t, s, lba, byte(0x40+lba))
	}
	mustSyncFlush(t, s)

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWriter := func() { stopOnce.Do(func() { close(stop) }) }
	progress := make(chan struct{}, 1024)
	werr := make(chan error, 1)
	var rounds atomic.Int64
	t.Cleanup(func() {
		stopWriter()
		select {
		case <-werr:
		case <-time.After(ctLocalBound):
			t.Errorf("writer not joined within %v", ctLocalBound)
		}
	})
	go func() {
		defer close(werr)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			lba := uint32(i % ctChunkBlocks)
			data := makeBlock(ctBlockSize, byte(i))
			lsn, err := s.Write(lba, data)
			if err == nil {
				w.record(lsn, lba, data)
				_, err = s.Sync()
			}
			if err == nil {
				err = s.flusher.flushOnce()
				// A concurrent cut can invalidate a prepared checkpoint map;
				// the production worker retries this explicit stale-proof outcome.
				if errors.Is(err, ErrMapCoverageStale) {
					err = nil
				}
			}
			if err != nil {
				werr <- err
				return
			}
			rounds.Add(1)
			select {
			case progress <- struct{}{}:
			default:
			}
		}
	}()

	for round := 0; round < 5; round++ {
		entered, release := holdPuts(t, f)
		done := startCapture(t, layer)
		waitKey(t, entered, "held upload")
		for len(progress) > 0 {
			<-progress
		}
		for need := 0; need < 3; need++ { // the writer must advance while the upload is held
			select {
			case <-progress:
			case <-time.After(ctLocalBound):
				t.Fatalf("round %d: writer made no progress while an upload was held", round)
			}
		}
		release()
		if err := waitErr(t, done, "held capture"); err != nil {
			t.Fatalf("round %d Capture: %v", round, err)
		}
	}
	for i := 0; i < 20; i++ {
		if _, err := layer.Capture(ctTimeout(t)); err != nil {
			t.Fatalf("free-running Capture %d: %v", i, err)
		}
	}
	stopWriter()
	if err := waitErr(t, werr, "writer"); err != nil {
		t.Fatalf("writer: %v", err)
	}
	if rounds.Load() < 15 {
		t.Fatalf("writer completed only %d rounds; overlap not established", rounds.Load())
	}
	// Each held round installs a HEAD; a free-running capture with no new
	// LSN since the last cut is idle and may skip its CAS.
	if n := len(f.installedHeads()); n < 6 {
		t.Fatalf("%d HEADs installed, want the 5 held rounds plus at least one free-running", n)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// C3 barrier: a write issued while a cut is still copying its chunks does
// not land until the copy is done, so it can never tear the captured image.
// The copy is held deterministically: on a fresh host the cut's copy of a
// dirty chunk must fetch that chunk's absent blocks, and the fetch is held.
func TestChunkLayer_C3_WriteDuringCopyWaitsForTheCut(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := uint32(8); lba < 12; lba++ { // chunk 2
		copy(src[int(lba)*ctBlockSize:], makeBlock(ctBlockSize, byte(0x80+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	s, _ := newChunkTestStore(t)
	func() {
		if err := s.SeedChunkFrontier(10); err != nil {
			t.Fatal(err)
		}
	}() // the log continues from HEAD's remote_lsn (W4b's job in production)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, f, cfg)
	local := makeBlock(ctBlockSize, 0xD8)
	if _, err := s.Write(8, local); err != nil { // chunk 2 dirty; LBA 9-11 still absent
		t.Fatal(err)
	}

	gEntered, gRelease := holdGets(t, f)
	late := makeBlock(ctBlockSize, 0xDA)
	wrote := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		gRelease()
		select {
		case <-joined:
		case <-time.After(ctLocalBound):
			t.Errorf("late writer not joined within %v", ctLocalBound)
		}
	})
	done := startCapture(t, layer)
	waitKey(t, gEntered, "the cut's fetch of an absent block in chunk 2")
	go func() {
		defer close(joined)
		_, err := s.Write(10, late)
		wrote <- err
	}()
	select {
	case err := <-wrote:
		t.Fatalf("a write landed while the cut was still copying (err %v); the barrier was released before the copy", err)
	case <-time.After(200 * time.Millisecond): // the copy is held indefinitely; the write must wait
	}
	gRelease()
	if err := waitErr(t, done, "held cut"); err != nil {
		t.Fatalf("held cut: %v", err)
	}
	if err := waitErr(t, wrote, "write after the cut"); err != nil {
		t.Fatal(err)
	}
	img := f.imageOf(t, f.stats().head)
	if !bytes.Equal(blockOf(img, 8), local) {
		t.Fatal("the cut's image lacks the write made before its barrier")
	}
	if !bytes.Equal(blockOf(img, 10), blockOf(src, 10)) {
		t.Fatal("the cut's image contains a write issued after its barrier")
	}
	if got, _ := s.Read(10); !bytes.Equal(got, late) {
		t.Fatal("the write that waited for the cut is not visible locally")
	}
	assertNoRemoteMisuse(t, f)
}

// C4 no missed dirt: every extent write marks its chunk dirty since HEAD,
// including writes that land during an upload and rebuild BASE writes. A
// capture refused by the owner's gate, or during a BASE install, touches
// neither the remote nor writes.
func TestChunkLayer_C4_WritesDuringUploadAndBaseAreCapturedNext(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	var gate atomic.Value // error or nil
	cfg := ctConfig(1)
	cfg.CaptureAllowed = func() error {
		if e, _ := gate.Load().(error); e != nil {
			return e
		}
		return nil
	}
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 4, 0x44) // chunk 1
	mustSyncFlush(t, s)

	entered, release := holdPuts(t, f)
	done := startCapture(t, layer)
	waitKey(t, entered, "chunk 1 upload")
	var during uint64
	within(t, "write into the chunk being uploaded", func() error {
		data := makeBlock(ctBlockSize, 0x66)
		lsn, err := s.Write(6, data) // same chunk, after the cut
		if err != nil {
			return err
		}
		w.record(lsn, 6, data)
		during = lsn
		if _, err := s.Sync(); err != nil {
			return err
		}
		return s.flusher.flushOnce()
	})
	release()
	if err := waitErr(t, done, "first Capture"); err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	if h := f.stats().head; h.RemoteLSN >= during {
		t.Fatalf("first HEAD remote_lsn %d covers LSN %d written after its cut", h.RemoteLSN, during)
	}
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("second Capture: %v", err)
	}
	if h := f.stats().head; h.RemoteLSN < during {
		t.Fatalf("second HEAD remote_lsn %d still misses the mid-upload write %d", h.RemoteLSN, during)
	}

	// Rebuild through the v3 store handle. The admitted BASE is the full
	// image at the last cut, plus the replacement block, at a new floor.
	image := f.imageOf(t, f.stats().head)
	base := makeBlock(ctBlockSize, 0xBA)
	copy(image[20*ctBlockSize:21*ctBlockSize], base)
	frontier := during + 10
	handle, err := s.BeginBaseInstall(frontier, [16]byte{0x44})
	if err != nil {
		t.Fatal(err)
	}
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		data := image[int(lba)*ctBlockSize : int(lba+1)*ctBlockSize]
		if err := handle.WriteBase(lba, data); err != nil {
			t.Fatal(err)
		}
		w.record(frontier, lba, data)
	}
	if _, err := s.Sync(); !errors.Is(err, ErrBaseInstalling) {
		t.Fatalf("logical Sync during install: %v", err)
	}
	before := f.stats()
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
		t.Fatalf("capture while installing: %v", err)
	}
	if after := f.stats(); after.puts != before.puts || after.cas != before.cas {
		t.Fatal("capture during install made remote effects")
	}
	if err := handle.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	waitReconciledForTest(t, layer) // the BASE is reconciled against HEAD first
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("Capture after BASE: %v", err)
	}
	if got := blockOf(f.imageOf(t, f.stats().head), 20); !bytes.Equal(got, base) {
		t.Fatal("rebuild BASE block (WriteExtentDirect) never reached the remote image")
	}
	assertEveryHeadIsPrefixImage(t, f, w)

	// Owner gate: a refused capture issues no remote call and blocks no writer.
	gate.Store(errors.New("not a healthy primary"))
	w.write(t, s, 7, 0x77)
	before = f.stats()
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
		t.Fatalf("Capture while the owner refuses: got %v, want errChunkCaptureRefused", err)
	}
	after := f.stats()
	if after.puts != before.puts || after.cas != before.cas {
		t.Fatalf("refused capture still made %d puts and %d CAS calls", after.puts-before.puts, after.cas-before.cas)
	}
	within(t, "write after a refused capture", func() error {
		_, err := s.Write(8, makeBlock(ctBlockSize, 0x88))
		return err
	})
}

// C5 immutable chunks: a key is never overwritten, reused or deleted while
// referenced; new content gets a new key; an older HEAD's chunks read back
// whole; an idle capture stores nothing.
func TestChunkLayer_C5_ChunksAreImmutable(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	for lba := uint32(0); lba < ctChunkBlocks; lba++ {
		w.write(t, s, lba, byte(0x01+lba))
	}
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	h1 := f.stats().head
	old := f.imageOf(t, h1)[:ctChunkBytes]
	w.write(t, s, 2, 0xF2)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	h2 := f.stats().head
	if h2.Chunks[0] == h1.Chunks[0] {
		t.Fatalf("changed chunk 0 kept ref %+v", h1.Chunks[0])
	}
	if got := f.imageOf(t, h1)[:ctChunkBytes]; !bytes.Equal(got, old) {
		t.Fatalf("gen %d's chunk 0 no longer reads back whole as published", h1.Gen)
	}
	before := f.stats()
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if after := f.stats(); after.puts != before.puts {
		t.Fatalf("idle capture stored %d chunks", after.puts-before.puts)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// C6 single writer, takeover between captures: a CAS lost to a newer epoch
// fences the layer for good; the stale holder never changes HEAD again.
func TestChunkLayer_C6_FencedOnTakeover(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.takeOverLocked("host-b")
	f.mu.Unlock()
	taken := f.stats().head

	w.write(t, s, 2, 0x23)
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkHeadPrecondition) && !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("stale holder's capture: got %v, want a precondition/fenced error", err)
	}
	if !layer.Fenced() {
		t.Fatal("layer not fenced after its CAS lost to a newer epoch")
	}
	casBefore := f.stats().cas
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("fenced Capture: got %v, want errChunkLayerFenced", err)
	}
	if err := layer.MakeRemoteDurable(ctTimeout(t), 1); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("fenced MakeRemoteDurable: got %v, want errChunkLayerFenced", err)
	}
	after := f.stats()
	if after.cas != casBefore {
		t.Fatalf("fenced layer issued %d more CAS calls", after.cas-casBefore)
	}
	if after.head.Gen != taken.Gen || after.head.Holder != "host-b" {
		t.Fatalf("stale holder changed HEAD: gen %d holder %q", after.head.Gen, after.head.Holder)
	}
}

// casResolution runs one Capture with setup applied to the fake, and returns
// its result plus the remote calls it made from its first CAS on.
func casResolution(t *testing.T, setup func(f *fakeChunkRemote)) (*WALStore, *fakeChunkRemote, chunkLayer, *writeLog, uint64, error, []string) {
	t.Helper()
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 3, 0x31)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 3, 0x32)
	start := len(f.stats().ops)
	f.mu.Lock()
	setup(f)
	f.mu.Unlock()
	h, err := layer.Capture(ctTimeout(t))
	ops := f.stats().ops[start:]
	for i, op := range ops {
		if strings.HasPrefix(op, "cas:") {
			return s, f, layer, w, h, err, ops[i:]
		}
	}
	t.Fatalf("the capture under test issued no CAS (ops %v)", ops)
	return nil, nil, nil, nil, 0, nil, nil
}

// mustRereadAfterEachUncertainCAS: after every CAS whose outcome was
// unknown or lost, a fresh GetHead must come before the next CAS, and the
// last uncertain outcome must be followed by a GetHead (a re-read attempt).
func mustRereadAfterEachUncertainCAS(t *testing.T, ops []string) {
	t.Helper()
	pending := false
	for _, op := range ops {
		switch {
		case op == "cas:unknown" || op == "cas:lost":
			if pending {
				t.Fatalf("blind CAS retry after an uncertain outcome without re-reading HEAD (ops %v)", ops)
			}
			pending = true
		case op == "gethead":
			pending = false
		case strings.HasPrefix(op, "cas:"):
			if pending {
				t.Fatalf("blind CAS retry after an uncertain outcome without re-reading HEAD (ops %v)", ops)
			}
		}
	}
	if pending {
		t.Fatalf("uncertain CAS outcome not followed by a HEAD re-read (ops %v)", ops)
	}
}

func casCount(ops []string) int {
	n := 0
	for _, op := range ops {
		if strings.HasPrefix(op, "cas:") {
			n++
		}
	}
	return n
}

func TestChunkLayer_C6_AppliedResponseLostResolvedByReread(t *testing.T) {
	s, f, layer, w, h, err, ops := casResolution(t, func(f *fakeChunkRemote) { f.casModes = []fakeCASMode{casAppliedLost} })
	mustRereadAfterEachUncertainCAS(t, ops)
	if n := casCount(ops); n != 1 {
		t.Fatalf("an applied CAS was repeated: %d CAS calls (ops %v)", n, ops)
	}
	if err != nil {
		t.Fatalf("our CAS was applied (HEAD re-read shows our proposal), got %v", err)
	}
	head := f.stats().head
	if layer.Fenced() || head.RemoteLSN != h || layer.RemoteLSN() != h || head.Holder != "host-a" || head.Epoch != 1 {
		t.Fatalf("after resolution: fenced %v, HEAD remote_lsn %d holder %q epoch %d, layer remote_lsn %d, H %d", layer.Fenced(), head.RemoteLSN, head.Holder, head.Epoch, layer.RemoteLSN(), h)
	}
	w.write(t, s, 4, 0x41) // the next capture must build on the re-read generation
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("capture after a resolved lost response: %v", err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

func uncertainNeverClaimsSuccess(t *testing.T, modes ...fakeCASMode) {
	t.Helper()
	_, f, layer, _, h, err, ops := casResolution(t, func(f *fakeChunkRemote) { f.casModes = modes })
	mustRereadAfterEachUncertainCAS(t, ops)
	head := f.stats().head
	if err == nil && (head.RemoteLSN != h || layer.RemoteLSN() != h) {
		// Success is allowed only if a re-read showed the CAS unapplied and an informed CAS then landed.
		t.Fatalf("Capture claimed success for H=%d but HEAD remote_lsn is %d (layer %d)", h, head.RemoteLSN, layer.RemoteLSN())
	}
	if err != nil && layer.RemoteLSN() != head.RemoteLSN {
		t.Fatalf("an unresolved CAS moved the layer remote_lsn to %d (HEAD %d)", layer.RemoteLSN(), head.RemoteLSN)
	}
	if layer.Fenced() {
		t.Fatal("an unapplied CAS with an unchanged HEAD fenced the layer")
	}
}

func TestChunkLayer_C6_UnknownNotAppliedNeverClaimsSuccess(t *testing.T) {
	uncertainNeverClaimsSuccess(t, casUnknownNotApplied)
}

// C6: two uncertain outcomes are each resolved by a fresh re-read. The
// layer may return after one uncertainty (the test then drives another
// capture) or retry after an informed re-read; either way two uncertain CAS
// outcomes must actually occur, each followed by a GetHead before the next
// CAS, and no capture may claim success that HEAD does not show.
func TestChunkLayer_C6_TwoUncertainOutcomesEachReread(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 3, 0x31)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 3, 0x32)
	start := len(f.stats().ops)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casUnknownNotApplied, casUnknownNotApplied}
	f.mu.Unlock()
	uncertain := func() int {
		n := 0
		for _, op := range f.stats().ops[start:] {
			if op == "cas:unknown" {
				n++
			}
		}
		return n
	}
	for attempt := 0; attempt < 3 && uncertain() < 2; attempt++ {
		h, err := layer.Capture(ctTimeout(t))
		head := f.stats().head
		if err == nil && (head.RemoteLSN != h || layer.RemoteLSN() != h) {
			t.Fatalf("attempt %d claimed success for H=%d but HEAD remote_lsn is %d (layer %d)", attempt, h, head.RemoteLSN, layer.RemoteLSN())
		}
		if err != nil && layer.RemoteLSN() != head.RemoteLSN {
			t.Fatalf("attempt %d: an unresolved CAS moved the layer remote_lsn to %d (HEAD %d)", attempt, layer.RemoteLSN(), head.RemoteLSN)
		}
		if layer.Fenced() {
			t.Fatalf("attempt %d: an unapplied CAS with an unchanged HEAD fenced the layer", attempt)
		}
	}
	if n := uncertain(); n != 2 {
		t.Fatalf("%d uncertain CAS outcomes observed, want 2", n)
	}
	mustRereadAfterEachUncertainCAS(t, f.stats().ops[start:])
}

func TestChunkLayer_C6_RereadUnavailableStaysUnresolved(t *testing.T) {
	s, f, layer, w, _, err, ops := casResolution(t, func(f *fakeChunkRemote) {
		f.casModes = []fakeCASMode{casAppliedLost}
		f.afterCAS = func(f *fakeChunkRemote) { f.failGet = errFakeRemoteDown }
	})
	prior := f.installedHeads()[0].RemoteLSN
	if err == nil {
		t.Fatal("Capture reported success although its CAS outcome could not be re-read")
	}
	if n := casCount(ops); n != 1 {
		t.Fatalf("CAS repeated while the outcome was unknown: %d CAS calls (ops %v)", n, ops)
	}
	mustRereadAfterEachUncertainCAS(t, ops)
	if layer.RemoteLSN() != prior || layer.Fenced() {
		t.Fatalf("unresolved outcome: layer remote_lsn %d (want %d), fenced %v", layer.RemoteLSN(), prior, layer.Fenced())
	}
	// The remote comes back and HEAD now holds our own proposal. The next
	// capture must re-read and build on it, not fence itself on a stale gen.
	f.mu.Lock()
	f.failGet = nil
	f.mu.Unlock()
	w.write(t, s, 5, 0x55)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("capture after the remote returned: %v", err)
	}
	if layer.Fenced() {
		t.Fatal("layer fenced itself on its own applied proposal")
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

func TestChunkLayer_C6_CompetingHeadDuringUncertainCASFences(t *testing.T) {
	_, f, layer, _, _, err, ops := casResolution(t, func(f *fakeChunkRemote) {
		f.casModes = []fakeCASMode{casUnknownNotApplied}
		f.afterCAS = func(f *fakeChunkRemote) { f.takeOverLocked("host-b") }
	})
	mustRereadAfterEachUncertainCAS(t, ops)
	if err == nil || !layer.Fenced() {
		t.Fatalf("a newer epoch appeared while our CAS was uncertain: err %v fenced %v, want an error and fenced", err, layer.Fenced())
	}
	casBefore := f.stats().cas
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("fenced Capture: got %v", err)
	}
	after := f.stats()
	if after.cas != casBefore || after.head.Holder != "host-b" {
		t.Fatalf("after fencing: %d more CAS calls, HEAD holder %q", after.cas-casBefore, after.head.Holder)
	}
}

// C6 demotion, admission side: a primary demoted while its capture's upload
// is in flight fails the pre-CAS admission check and issues no HEAD CAS.
func TestChunkLayer_C6_DemotedBeforeFinalCheckDoesNotPublish(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	var demoted atomic.Bool
	cfg := ctConfig(1)
	cfg.CaptureAllowed = func() error {
		if demoted.Load() {
			return errors.New("not a healthy primary")
		}
		return nil
	}
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 6, 0x61)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 6, 0x62)
	entered, release := holdPuts(t, f)
	casBefore := f.stats().cas
	done := startCapture(t, layer)
	waitKey(t, entered, "upload of the capture being demoted")
	demoted.Store(true)
	release()
	if err := waitErr(t, done, "demoted capture"); err == nil {
		t.Fatal("a capture completed although its primary was demoted before the final admission check")
	}
	if got := f.stats().cas; got != casBefore {
		t.Fatalf("a demoted primary issued %d HEAD CAS calls", got-casBefore)
	}
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
		t.Fatalf("Capture after demotion: got %v, want errChunkCaptureRefused", err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// C6 demotion, fencing side: when the takeover lands after the final
// admission check (between it and the CAS), the epoch fence is what stops
// the old primary: its CAS fails, it fences, and HEAD stays the new
// holder's. The admission check alone cannot close this window.
func TestChunkLayer_C6_TakeoverAfterFinalCheckIsStoppedByEpoch(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 7, 0x71)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 7, 0x72)
	f.mu.Lock()
	f.beforeCAS = func(f *fakeChunkRemote) { f.takeOverLocked("host-b") }
	f.mu.Unlock()
	if _, err := layer.Capture(ctTimeout(t)); err == nil {
		t.Fatal("old primary's capture succeeded after a takeover landed before its CAS")
	}
	if !layer.Fenced() {
		t.Fatal("old primary not fenced after its CAS lost to the takeover epoch")
	}
	if h := f.stats().head; h.Holder != "host-b" || h.Epoch != 2 {
		t.Fatalf("HEAD after the takeover: holder %q epoch %d, want host-b epoch 2", h.Holder, h.Epoch)
	}
}

// C7 lazy read: a fresh host fetches nothing at attach, reads absent blocks
// exactly as in HEAD and ZERO chunks as zeros without a fetch, and a fill
// held across newer local writes (to the requested block and its siblings,
// in the WAL and in the extent) never replaces them.
func TestChunkLayer_C7_FreshHostReadsLazilyFromHead(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := uint32(0); lba < 16; lba++ { // chunks 0-3 hold data; 4-7 are ZERO
		copy(src[int(lba)*ctBlockSize:], makeBlock(ctBlockSize, byte(0x70+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 100, 1)

	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	mustAttach(t, s, f, cfg)
	if g := f.stats().gets; g != 0 {
		t.Fatalf("attach fetched %d chunks eagerly; reads must be lazy", g)
	}
	if got, err := s.Read(1); err != nil || !bytes.Equal(got, blockOf(src, 1)) {
		t.Fatalf("lazy read LBA 1: %v", err)
	}
	if g := f.stats().gets; g == 0 {
		t.Fatal("read of an absent block fetched nothing")
	}
	getsBefore := f.stats().gets
	if got, err := s.Read(20); err != nil || !bytes.Equal(got, make([]byte, ctBlockSize)) {
		t.Fatalf("ZERO chunk read LBA 20: %v", err)
	}
	if g := f.stats().gets; g != getsBefore {
		t.Fatalf("a ZERO chunk read fetched %d objects", g-getsBefore)
	}

	// Chunk 2 (LBA 8-11) is untouched. Hold its fetch across newer local writes.
	gEntered, gRelease := holdGets(t, f)
	type readRes struct {
		b   []byte
		err error
	}
	readDone := make(chan readRes, 1)
	readJoined := make(chan struct{})
	t.Cleanup(func() {
		gRelease()
		select {
		case <-readJoined:
		case <-time.After(ctLocalBound):
			t.Errorf("held lazy read not joined within %v", ctLocalBound)
		}
	})
	go func() {
		defer close(readJoined)
		b, err := s.Read(9)
		readDone <- readRes{b, err}
	}()
	waitKey(t, gEntered, "chunk 2 fetch")
	requested := makeBlock(ctBlockSize, 0xC9)
	walOnly := makeBlock(ctBlockSize, 0xC1)
	flushed := makeBlock(ctBlockSize, 0xC2)
	within(t, "local writes while a fill of the same chunk is in flight", func() error {
		for lba, b := range map[uint32][]byte{9: requested, 10: walOnly, 11: flushed} {
			if _, err := s.Write(lba, b); err != nil {
				return err
			}
		}
		if _, err := s.Sync(); err != nil {
			return err
		}
		if err := s.flusher.flushOnce(); err != nil { // 9, 10 and 11 reach the extent during the fill
			return err
		}
		_, err := s.Write(10, walOnly) // and 10 again, WAL only
		return err
	})
	gRelease()
	select {
	case r := <-readDone:
		// The overlapping read may return HEAD's block or the newer local one.
		if r.err != nil || (!bytes.Equal(r.b, blockOf(src, 9)) && !bytes.Equal(r.b, requested)) {
			t.Fatalf("held lazy read LBA 9: %v (neither HEAD's nor the newer local block)", r.err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("held lazy read did not finish after its fetch was released")
	}
	for round := 0; round < 2; round++ {
		for lba, want := range map[uint32][]byte{9: requested, 10: walOnly, 11: flushed, 8: blockOf(src, 8)} {
			if got, _ := s.Read(lba); !bytes.Equal(got, want) {
				t.Fatalf("round %d: LBA %d is not the newest version (a fill replaced a newer local block?)", round, lba)
			}
		}
		mustSyncFlush(t, s)
	}
}

// C7 read-miss bound: a fetch that has started but never completes fails the
// read with a deadline error at the configured ReadMissTimeout (not a longer
// fixed timeout, not an unrelated immediate refusal), and local work still
// makes progress.
func TestChunkLayer_C7_ReadMissTimeoutIsBounded(t *testing.T) {
	const timeout = 200 * time.Millisecond
	const tolerance = 800 * time.Millisecond // scheduling slack on a shared host
	src := make([]byte, ctNumBlocks*ctBlockSize)
	copy(src[8*ctBlockSize:], makeBlock(ctBlockSize, 0x88)) // chunk 2
	f := newFakeChunkRemote()
	f.seedHead(t, src, 50, 1)
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	cfg.ReadMissTimeout = timeout
	mustAttach(t, s, f, cfg)
	gEntered, _ := holdGets(t, f) // never released before cleanup
	var elapsed time.Duration
	var readErr error
	within(t, "read of an absent block whose fetch never completes", func() error {
		start := time.Now()
		_, readErr = s.Read(8)
		elapsed = time.Since(start)
		return nil
	})
	select {
	case <-gEntered:
	default:
		t.Fatal("the read failed without ever starting its remote fetch")
	}
	if !errors.Is(readErr, context.DeadlineExceeded) {
		t.Fatalf("read of a never-completing fetch: got %v, want a deadline error", readErr)
	}
	if elapsed < timeout*3/4 || elapsed > timeout+tolerance {
		t.Fatalf("read failed after %v; want about the configured %v (allowed %v..%v)", elapsed, timeout, timeout*3/4, timeout+tolerance)
	}
	within(t, "local write/sync/flush after a timed-out read miss", func() error {
		if _, err := s.Write(20, makeBlock(ctBlockSize, 0x20)); err != nil {
			return err
		}
		if _, err := s.Sync(); err != nil {
			return err
		}
		return s.flusher.flushOnce()
	})
}

// C8 durability point: MakeRemoteDurable(t) returns nil only after the local
// Sync covers t and a HEAD with RemoteLSN >= t is installed; a local Sync
// failure, a remote failure or a never-written t publish no covering HEAD.
func TestChunkLayer_C8_MakeRemoteDurable(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		s, _ := newChunkTestStore(t)
		f := newFakeChunkRemote()
		layer := mustAttach(t, s, f, ctConfig(1))
		w := newWriteLog()
		w.write(t, s, 10, 0xA0)
		target := w.write(t, s, 17, 0xA7) // written, not synced, not flushed
		if err := layer.MakeRemoteDurable(ctTimeout(t), target); err != nil {
			t.Fatalf("MakeRemoteDurable(%d): %v", target, err)
		}
		if r, _, _ := s.Boundaries(); r < target {
			t.Fatalf("returned nil but the local synced frontier R=%d < %d", r, target)
		}
		if h := f.stats().head; h.RemoteLSN < target {
			t.Fatalf("returned nil but HEAD remote_lsn %d < %d", h.RemoteLSN, target)
		}
		assertEveryHeadIsPrefixImage(t, f, w)
	})
	noCoveringHead := func(t *testing.T, f *fakeChunkRemote, target uint64) {
		t.Helper()
		for _, h := range f.installedHeads() {
			if h.RemoteLSN >= target {
				t.Fatalf("HEAD gen %d claims remote_lsn %d >= %d after a failed durability point", h.Gen, h.RemoteLSN, target)
			}
		}
	}
	t.Run("local sync fails", func(t *testing.T) {
		s, _ := newChunkTestStore(t)
		f := newFakeChunkRemote()
		layer := mustAttach(t, s, f, ctConfig(1))
		s.committer.syncFunc = func() error { return errors.New("injected local fsync failure") }
		target, err := s.Write(3, makeBlock(ctBlockSize, 0x03))
		if err != nil {
			t.Fatal(err)
		}
		if err := layer.MakeRemoteDurable(ctTimeout(t), target); err == nil {
			t.Fatal("MakeRemoteDurable returned nil although the local Sync failed")
		}
		noCoveringHead(t, f, target)
	})
	t.Run("remote down", func(t *testing.T) {
		s, _ := newChunkTestStore(t)
		f := newFakeChunkRemote()
		layer := mustAttach(t, s, f, ctConfig(1))
		f.mu.Lock()
		f.failPut = errFakeRemoteDown
		f.mu.Unlock()
		target, err := s.Write(4, makeBlock(ctBlockSize, 0x04))
		if err != nil {
			t.Fatal(err)
		}
		if err := layer.MakeRemoteDurable(ctTimeout(t), target); err == nil {
			t.Fatal("MakeRemoteDurable returned nil with the remote down")
		}
		noCoveringHead(t, f, target)
	})
	t.Run("never written", func(t *testing.T) {
		s, _ := newChunkTestStore(t)
		f := newFakeChunkRemote()
		layer := mustAttach(t, s, f, ctConfig(1))
		last, err := s.Write(5, makeBlock(ctBlockSize, 0x05))
		if err != nil {
			t.Fatal(err)
		}
		within(t, "MakeRemoteDurable of a never-written LSN", func() error {
			if err := layer.MakeRemoteDurable(ctTimeout(t), last+100); err == nil {
				return errors.New("returned nil for an LSN that was never written")
			}
			return nil
		})
		noCoveringHead(t, f, last+100)
	})
}

// C9 eviction is per chunk: allowed only when the chunk is not dirty since
// HEAD; an evicted chunk is fetched again on read, with the same bytes.
func TestChunkLayer_C9_EvictOnlyCleanChunks(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 8, 0x81) // chunk 2
	mustSyncFlush(t, s)
	if err := layer.EvictChunk(2); !errors.Is(err, errChunkEvictRefused) {
		t.Fatalf("evict of a chunk dirty since HEAD (no HEAD yet): got %v, want refused", err)
	}
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if err := layer.EvictChunk(2); err != nil {
		t.Fatalf("evict of a clean chunk: %v", err)
	}
	getsBefore := f.stats().gets
	if got, err := s.Read(8); err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, 0x81)) {
		t.Fatalf("read after eviction: %v", err)
	}
	if f.stats().gets == getsBefore {
		t.Fatal("read after eviction fetched nothing (chunk was not really absent)")
	}
	w.write(t, s, 9, 0x91) // chunk 2 dirty again
	mustSyncFlush(t, s)
	if err := layer.EvictChunk(2); !errors.Is(err, errChunkEvictRefused) {
		t.Fatalf("evict of a chunk written after HEAD: got %v, want refused", err)
	}
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if err := layer.EvictChunk(2); err != nil {
		t.Fatalf("evict after the new write was captured: %v", err)
	}
	for lba, want := range map[uint32]byte{8: 0x81, 9: 0x91} {
		if got, _ := s.Read(lba); !bytes.Equal(got, makeBlock(ctBlockSize, want)) {
			t.Fatalf("LBA %d after the second eviction differs", lba)
		}
	}
}

// C9 across a restart: absence is durable. An evicted chunk of an existing
// local volume still reads back as HEAD publishes it after a crash and
// reopen, and only by fetching: with the remote down the read fails rather
// than returning retained local bytes. A newer local write into an evicted
// chunk survives the restart.
func TestChunkLayer_C9_EvictionSurvivesRestart(t *testing.T) {
	setup := func(t *testing.T) (s *WALStore, path string, crash func(), f *fakeChunkRemote, cfg chunkLayerConfig, layer chunkLayer) {
		s, path, crash = crashableStore(t)
		f = newFakeChunkRemote()
		cfg = ctConfig(1)
		layer = mustAttach(t, s, f, cfg)
		for lba := uint32(8); lba < 12; lba++ { // chunk 2, every block non-zero
			if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(0x80+lba))); err != nil {
				t.Fatal(err)
			}
		}
		mustSyncFlush(t, s)
		if _, err := layer.Capture(ctTimeout(t)); err != nil {
			t.Fatal(err)
		}
		if err := layer.EvictChunk(2); err != nil {
			t.Fatalf("evict: %v", err)
		}
		return s, path, crash, f, cfg, layer
	}

	t.Run("evicted then crashed", func(t *testing.T) {
		_, path, crash, f, cfg, _ := setup(t)
		crash() // nothing read or written since the eviction
		s2, _ := reopen(t, path)
		mustAttach(t, s2, f, cfg) // attach reads HEAD; the remote goes down after
		f.mu.Lock()
		f.failGet = errFakeRemoteDown
		f.mu.Unlock()
		if got, err := s2.Read(10); err == nil {
			t.Fatalf("evicted block read back %#x from retained local bytes with the remote down; absence was lost across the restart", got[0])
		}
		f.mu.Lock()
		f.failGet = nil
		f.mu.Unlock()
		getsBefore := f.stats().gets
		for lba := uint32(8); lba < 12; lba++ {
			if got, err := s2.Read(lba); err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, byte(0x80+lba))) {
				t.Fatalf("after restart evicted LBA %d is not HEAD's block: %v", lba, err)
			}
		}
		if f.stats().gets == getsBefore {
			t.Fatal("evicted chunk was served after restart without a fetch")
		}
	})

	t.Run("newer local write after eviction", func(t *testing.T) {
		s, path, crash, f, cfg, _ := setup(t)
		newer := makeBlock(ctBlockSize, 0xE9)
		if _, err := s.Write(9, newer); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		crash()
		s2, _ := reopen(t, path)
		mustAttach(t, s2, f, cfg)
		for lba, want := range map[uint32][]byte{
			8:  makeBlock(ctBlockSize, 0x88),
			9:  newer,
			10: makeBlock(ctBlockSize, 0x8A),
			11: makeBlock(ctBlockSize, 0x8B),
		} {
			if got, err := s2.Read(lba); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("after restart LBA %d is not the correct version: %v", lba, err)
			}
		}
	})
}

// C10 restart: local recovery is unchanged (WAL replay) and needs no remote;
// after a restart the local volume (FreshFromHead=false) wins over an older
// HEAD, and the next capture publishes it.
func TestChunkLayer_C10_LocalRecoveryWinsOverOlderHead(t *testing.T) {
	s, path, crash := crashableStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1) // the same bitmap store survives the restart
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 9, 0x91)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 9, 0x92)
	w.write(t, s, 11, 0xB1)
	if _, err := s.Sync(); err != nil { // L1 only, never captured
		t.Fatal(err)
	}
	crash()

	f.mu.Lock()
	f.failGet, f.failPut = errFakeRemoteDown, errFakeRemoteDown // remote down during local recovery
	f.mu.Unlock()
	s2, _ := reopen(t, path)
	f.mu.Lock()
	f.failGet, f.failPut = nil, nil
	f.mu.Unlock()
	layer2 := mustAttach(t, s2, f, cfg)
	for lba, want := range map[uint32]byte{9: 0x92, 11: 0xB1} {
		got, err := s2.Read(lba)
		if err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, want)) {
			t.Fatalf("after restart LBA %d is not the locally synced block (HEAD is older): %v", lba, err)
		}
	}
	if _, err := layer2.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("capture after restart: %v", err)
	}
	if got := blockOf(f.imageOf(t, f.stats().head), 9); !bytes.Equal(got, makeBlock(ctBlockSize, 0x92)) {
		t.Fatal("first capture after restart did not publish the recovered local data")
	}
}
