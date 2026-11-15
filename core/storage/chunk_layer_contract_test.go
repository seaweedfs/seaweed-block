package storage

// Contract tests for the chunk layer (chunk_seam.go), invariants C1-C10.
//
// The remote is an asynchronous image of an LSN prefix; the local WAL and
// extent stay authoritative. Every test drives the real WALStore with the
// auto flusher stopped, so flush rounds and checkpoints happen exactly where
// the test calls flushOnce. The remote is an in-memory fake with fault
// injection (stalled or failing Put, lost CAS response).
//
// Until the layer exists these tests fail at attachChunkLayer with
// errChunkLayerNotImplemented ("RED"). TestChunkFakeRemote_* checks the
// fake itself and passes today.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	ctBlockSize   = 4096
	ctChunkBlocks = 4  // small chunks so a test spans several
	ctNumBlocks   = 32 // 8 chunks
)

var errFakeRemoteDown = errors.New("fake remote: unavailable")
var errFakeCASResponseLost = errors.New("fake remote: CAS response lost")

// fakeChunkRemote is an in-memory chunkRemote. It enforces immutable keys
// and the HEAD CAS, records every call, and injects faults on request.
type fakeChunkRemote struct {
	mu   sync.Mutex
	objs map[string][]byte
	head chunkHead // Gen 0 = no HEAD yet

	putCalls, getCalls, casCalls int
	putKeys                      []string
	putBytes                     map[string][]byte // bytes as passed to Put, copied on entry
	overwriteAttempts            int
	heads                        []chunkHead // every HEAD installed by a CAS, in order

	putGate    chan struct{} // non-nil: Put blocks (after recording) until closed
	putEntered chan string   // non-nil: receives each Put key on entry (non-blocking)
	failPut    error
	failGet    error
	failCAS    error
	loseCAS    bool // next successful CAS is applied but its response is lost
}

func newFakeChunkRemote() *fakeChunkRemote {
	return &fakeChunkRemote{objs: map[string][]byte{}, putBytes: map[string][]byte{}}
}

func (f *fakeChunkRemote) Put(ctx context.Context, key string, data []byte) error {
	f.mu.Lock()
	f.putCalls++
	f.putKeys = append(f.putKeys, key)
	f.putBytes[key] = append([]byte(nil), data...)
	gate, entered, fail := f.putGate, f.putEntered, f.failPut
	_, exists := f.objs[key]
	if exists {
		f.overwriteAttempts++
	}
	f.mu.Unlock()
	if entered != nil {
		select {
		case entered <- key:
		default:
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if exists {
		return errChunkKeyExists
	}
	if fail != nil {
		return fail
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, raced := f.objs[key]; raced {
		f.overwriteAttempts++
		return errChunkKeyExists
	}
	f.objs[key] = append([]byte(nil), data...)
	return nil
}

func (f *fakeChunkRemote) Get(ctx context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.failGet != nil {
		return nil, f.failGet
	}
	b, ok := f.objs[key]
	if !ok {
		return nil, fmt.Errorf("fake remote: no key %q", key)
	}
	return append([]byte(nil), b...), nil
}

func (f *fakeChunkRemote) GetHead(ctx context.Context) (chunkHead, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGet != nil {
		return chunkHead{}, f.failGet
	}
	return copyChunkHead(f.head), nil
}

func (f *fakeChunkRemote) CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.casCalls++
	if f.failCAS != nil {
		return 0, f.failCAS
	}
	if expected.Gen != f.head.Gen || expected.Epoch != f.head.Epoch {
		return 0, errChunkHeadPrecondition
	}
	for idx, key := range next.Chunks {
		if _, ok := f.objs[key]; !ok {
			return 0, fmt.Errorf("fake remote: HEAD names chunk %d key %q that was never stored", idx, key)
		}
	}
	installed := copyChunkHead(next)
	installed.Gen = f.head.Gen + 1
	f.head = installed
	f.heads = append(f.heads, copyChunkHead(installed))
	if f.loseCAS {
		f.loseCAS = false
		return 0, errFakeCASResponseLost
	}
	return installed.Gen, nil
}

func (f *fakeChunkRemote) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objs, key)
	return nil
}

func copyChunkHead(h chunkHead) chunkHead {
	out := h
	out.Chunks = make(map[uint64]string, len(h.Chunks))
	for k, v := range h.Chunks {
		out.Chunks[k] = v
	}
	return out
}

func (f *fakeChunkRemote) gets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls
}

func (f *fakeChunkRemote) snapshot() (chunkHead, int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return copyChunkHead(f.head), f.putCalls, f.casCalls, f.overwriteAttempts
}

// seedHead stores chunk bytes and installs a HEAD directly, standing in for
// a previous holder. image is the whole volume; all-zero chunks stay ZERO.
func (f *fakeChunkRemote) seedHead(t *testing.T, image []byte, remoteLSN, epoch uint64) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	chunkBytes := ctChunkBlocks * ctBlockSize
	head := chunkHead{Gen: f.head.Gen + 1, Epoch: epoch, Holder: "seed", RemoteLSN: remoteLSN, Chunks: map[uint64]string{}}
	for idx := 0; idx*chunkBytes < len(image); idx++ {
		c := image[idx*chunkBytes : (idx+1)*chunkBytes]
		if bytes.Equal(c, make([]byte, chunkBytes)) {
			continue
		}
		key := fmt.Sprintf("seed/%d/%d", idx, head.Gen)
		f.objs[key] = append([]byte(nil), c...)
		head.Chunks[uint64(idx)] = key
	}
	f.head = head
}

// remoteImage assembles the volume image HEAD publishes.
func (f *fakeChunkRemote) remoteImage(t *testing.T) []byte {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	chunkBytes := ctChunkBlocks * ctBlockSize
	img := make([]byte, ctNumBlocks*ctBlockSize)
	for idx, key := range f.head.Chunks {
		b, ok := f.objs[key]
		if !ok {
			t.Fatalf("HEAD gen %d names chunk %d key %q that is not stored", f.head.Gen, idx, key)
		}
		if len(b) != chunkBytes {
			t.Fatalf("chunk %d key %q has %d bytes, want %d", idx, key, len(b), chunkBytes)
		}
		copy(img[int(idx)*chunkBytes:], b)
	}
	return img
}

// writeLog records what the test wrote, so the image at any LSN is known.
type writeLog struct {
	lsns map[uint64]struct {
		lba  uint32
		data []byte
	}
}

func newWriteLog() *writeLog {
	return &writeLog{lsns: map[uint64]struct {
		lba  uint32
		data []byte
	}{}}
}

func (w *writeLog) write(t *testing.T, s *WALStore, lba uint32, fill byte) uint64 {
	t.Helper()
	data := makeBlock(ctBlockSize, fill)
	lsn, err := s.Write(lba, data)
	if err != nil {
		t.Fatalf("Write LBA %d: %v", lba, err)
	}
	w.lsns[lsn] = struct {
		lba  uint32
		data []byte
	}{lba, data}
	return lsn
}

// imageAt is the volume image after applying every logged write with LSN <= lsn.
func (w *writeLog) imageAt(lsn uint64) []byte {
	img := make([]byte, ctNumBlocks*ctBlockSize)
	for l := uint64(1); l <= lsn; l++ {
		if e, ok := w.lsns[l]; ok {
			copy(img[int(e.lba)*ctBlockSize:], e.data)
		}
	}
	return img
}

func newChunkTestStore(t *testing.T) (*WALStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vol.bin")
	s, err := CreateWALStore(path, ctNumBlocks, ctBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func ctConfig(epoch uint64) chunkLayerConfig {
	return chunkLayerConfig{ChunkBlocks: ctChunkBlocks, Epoch: epoch, Holder: "host-a"}
}

func mustAttach(t *testing.T, s *WALStore, r chunkRemote, cfg chunkLayerConfig) chunkLayer {
	t.Helper()
	layer, err := attachChunkLayer(s, r, cfg)
	if errors.Is(err, errChunkLayerNotImplemented) {
		t.Fatalf("RED: %v", err)
	}
	if err != nil {
		t.Fatalf("attachChunkLayer: %v", err)
	}
	return layer
}

func mustSyncFlush(t *testing.T, s *WALStore) {
	t.Helper()
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatalf("flushOnce: %v", err)
	}
}

func ctTimeout(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

func blockOf(img []byte, lba uint32) []byte {
	return img[int(lba)*ctBlockSize : int(lba+1)*ctBlockSize]
}

func assertImage(t *testing.T, what string, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		if !bytes.Equal(blockOf(got, lba), blockOf(want, lba)) {
			t.Fatalf("%s: LBA %d first differs: got byte %#x, want %#x", what, lba, blockOf(got, lba)[0], blockOf(want, lba)[0])
		}
	}
}

// ---- the fake itself (passes today) ----

func TestChunkFakeRemote_ImmutableKeysAndCAS(t *testing.T) {
	f := newFakeChunkRemote()
	c := context.Background()
	if err := f.Put(c, "k", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(c, "k", []byte("b")); !errors.Is(err, errChunkKeyExists) {
		t.Fatalf("overwrite: got %v, want errChunkKeyExists", err)
	}
	if b, _ := f.Get(c, "k"); string(b) != "a" {
		t.Fatalf("overwrite replaced bytes: %q", b)
	}
	gen, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1, Chunks: map[uint64]string{0: "k"}})
	if err != nil || gen != 1 {
		t.Fatalf("first CAS: gen %d err %v", gen, err)
	}
	if _, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1}); !errors.Is(err, errChunkHeadPrecondition) {
		t.Fatalf("stale CAS: got %v", err)
	}
	if _, err := f.CASHead(c, chunkHeadVersion{1, 1}, chunkHead{Epoch: 1, Chunks: map[uint64]string{0: "missing"}}); err == nil {
		t.Fatal("CAS naming an unstored key must fail")
	}
	f.loseCAS = true
	if _, err := f.CASHead(c, chunkHeadVersion{1, 1}, chunkHead{Epoch: 1, RemoteLSN: 7}); !errors.Is(err, errFakeCASResponseLost) {
		t.Fatalf("lost response: got %v", err)
	}
	if h, _ := f.GetHead(c); h.Gen != 2 || h.RemoteLSN != 7 {
		t.Fatalf("lost-response CAS must still be applied: gen %d remote_lsn %d", h.Gen, h.RemoteLSN)
	}
}

// ---- C1-C10 ----

// C1 prefix image: HEAD is CAS'd with remote_lsn = C only after every chunk
// holding blocks with LSN <= C is uploaded, so HEAD is always the complete
// image of an LSN prefix.
func TestChunkLayer_C1_HeadIsCompletePrefixImage(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	for i, lba := range []uint32{0, 5, 9, 31, 5} { // chunks 0,1,2,7; LBA 5 twice
		w.write(t, s, lba, byte(0x10+i))
	}
	mustSyncFlush(t, s)
	c := s.CheckpointLSN()

	// One chunk upload stalls: HEAD must not move until every chunk is in.
	f.putGate = make(chan struct{})
	f.putEntered = make(chan string, 16)
	done := make(chan error, 1)
	go func() { _, err := layer.RemoteCheckpoint(ctTimeout(t)); done <- err }()
	select {
	case <-f.putEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("no chunk upload started")
	}
	if h, _, cas, _ := f.snapshot(); cas != 0 || h.Gen != 0 {
		t.Fatalf("HEAD CAS issued while a chunk upload is still in flight (cas calls %d, gen %d)", cas, h.Gen)
	}
	close(f.putGate)
	if err := <-done; err != nil {
		t.Fatalf("RemoteCheckpoint: %v", err)
	}
	h, _, _, _ := f.snapshot()
	if h.RemoteLSN != c {
		t.Fatalf("remote_lsn %d, want the local checkpoint %d", h.RemoteLSN, c)
	}
	assertImage(t, "HEAD image vs image at remote_lsn", f.remoteImage(t), w.imageAt(h.RemoteLSN))
	if len(h.Chunks) != 4 {
		t.Fatalf("HEAD names %d chunks, want 4 (untouched chunks stay ZERO)", len(h.Chunks))
	}

	// A write synced but not yet checkpointed is not part of the image.
	late := w.write(t, s, 13, 0x99)
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatalf("second RemoteCheckpoint: %v", err)
	}
	h, _, _, _ = f.snapshot()
	if h.RemoteLSN >= late {
		t.Fatalf("remote_lsn %d covers LSN %d that never reached the local checkpoint %d", h.RemoteLSN, late, s.CheckpointLSN())
	}
	assertImage(t, "HEAD image after an un-checkpointed write", f.remoteImage(t), w.imageAt(h.RemoteLSN))
}

// C2 L1 unchanged: chunk upload never gates Sync, the checkpoint, WAL
// recycling or R/S/H; a stalled or failing remote changes none of them.
func TestChunkLayer_C2_StalledRemoteDoesNotGateLocal(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 1, 0x11)
	mustSyncFlush(t, s)

	f.putGate = make(chan struct{}) // every upload hangs
	defer close(f.putGate)
	upCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = layer.RemoteCheckpoint(upCtx) }()

	deadline := time.Now().Add(5 * time.Second)
	var lastLSN uint64
	for i := 0; i < 8; i++ {
		lastLSN = w.write(t, s, uint32(2+i), byte(0x20+i))
		synced, err := s.Sync()
		if err != nil {
			t.Fatalf("Sync with a stalled remote: %v", err)
		}
		if synced < lastLSN {
			t.Fatalf("Sync returned %d < written %d with a stalled remote", synced, lastLSN)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatalf("flushOnce with a stalled remote: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("local write/sync/flush slowed by a stalled remote upload")
		}
	}
	if cp := s.CheckpointLSN(); cp < lastLSN {
		t.Fatalf("checkpoint %d did not reach %d while the remote stalled", cp, lastLSN)
	}
	if r, sTail, h := s.Boundaries(); r < lastLSN || h < lastLSN || sTail != s.CheckpointLSN()+1 {
		t.Fatalf("boundaries R=%d S=%d H=%d changed by the chunk layer (checkpoint %d, last %d)", r, sTail, h, s.CheckpointLSN(), lastLSN)
	}

	// A failing remote: the same local results, an error from the remote step only.
	f2 := newFakeChunkRemote()
	f2.failPut = errFakeRemoteDown
	s2, _ := newChunkTestStore(t)
	layer2 := mustAttach(t, s2, f2, ctConfig(1))
	w2 := newWriteLog()
	l := w2.write(t, s2, 3, 0x33)
	mustSyncFlush(t, s2)
	if _, err := layer2.RemoteCheckpoint(ctTimeout(t)); err == nil {
		t.Fatal("RemoteCheckpoint must report a failing remote")
	}
	if got, _ := s2.Read(3); !bytes.Equal(got, makeBlock(ctBlockSize, 0x33)) || s2.CheckpointLSN() < l {
		t.Fatal("a failing remote changed local reads or the checkpoint")
	}
	if h, _, _, _ := f2.snapshot(); h.Gen != 0 {
		t.Fatalf("HEAD moved to gen %d although uploads failed", h.Gen)
	}
}

// C3 no torn chunk: an uploaded chunk equals the extent content at one
// checkpoint boundary, even while the flusher keeps writing into that chunk.
// A writer keeps overwriting chunk 0 and flushing while checkpoints run; every
// HEAD ever installed must publish exactly the image at its remote_lsn.
func TestChunkLayer_C3_UploadedChunkIsOneCheckpointImage(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	for lba := uint32(0); lba < ctChunkBlocks; lba++ {
		w.write(t, s, lba, byte(0x40+lba))
	}
	mustSyncFlush(t, s)

	stop := make(chan struct{})
	werr := make(chan error, 1)
	var wmu sync.Mutex // guards w across the writer goroutine and the final check
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
				wmu.Lock()
				w.lsns[lsn] = struct {
					lba  uint32
					data []byte
				}{lba, data}
				wmu.Unlock()
				_, err = s.Sync()
			}
			if err == nil {
				err = s.flusher.flushOnce()
			}
			if err != nil {
				werr <- err
				return
			}
		}
	}()
	for i := 0; i < 50; i++ {
		if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
			close(stop)
			t.Fatalf("RemoteCheckpoint %d under concurrent flushes: %v", i, err)
		}
	}
	close(stop)
	if err := <-werr; err != nil {
		t.Fatalf("writer: %v", err)
	}
	f.mu.Lock()
	heads := append([]chunkHead(nil), f.heads...)
	objs := make(map[string][]byte, len(f.objs))
	for k, v := range f.objs {
		objs[k] = v
	}
	f.mu.Unlock()
	if len(heads) == 0 {
		t.Fatal("no HEAD was installed")
	}
	chunkBytes := ctChunkBlocks * ctBlockSize
	wmu.Lock()
	defer wmu.Unlock()
	for _, h := range heads {
		want := w.imageAt(h.RemoteLSN)[:chunkBytes]
		got := objs[h.Chunks[0]]
		if !bytes.Equal(got, want) {
			t.Fatalf("HEAD gen %d publishes a chunk 0 that is not the image at its remote_lsn %d (torn by concurrent flushes)", h.Gen, h.RemoteLSN)
		}
	}
}

// C4 no missed dirt: every extent write marks its chunk dirty, including
// writes during an upload and rebuild BASE writes; nothing is dropped.
func TestChunkLayer_C4_WritesDuringUploadAndBaseAreUploadedNext(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 4, 0x44) // chunk 1
	mustSyncFlush(t, s)

	f.putGate = make(chan struct{})
	f.putEntered = make(chan string, 4)
	done := make(chan error, 1)
	go func() { _, err := layer.RemoteCheckpoint(ctTimeout(t)); done <- err }()
	<-f.putEntered
	during := w.write(t, s, 6, 0x66) // same chunk, lands mid-upload
	mustSyncFlush(t, s)
	close(f.putGate)
	if err := <-done; err != nil {
		t.Fatalf("first RemoteCheckpoint: %v", err)
	}
	f.putGate, f.putEntered = nil, nil
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatalf("second RemoteCheckpoint: %v", err)
	}
	h, _, _, _ := f.snapshot()
	if h.RemoteLSN < during {
		t.Fatalf("remote_lsn %d never covered the mid-upload write %d", h.RemoteLSN, during)
	}
	if got := blockOf(f.remoteImage(t), 6); !bytes.Equal(got, makeBlock(ctBlockSize, 0x66)) {
		t.Fatal("write that landed during an upload was dropped from the remote image")
	}

	// Rebuild BASE: WriteExtentDirect plus frontier and Sync must be uploaded too.
	base := makeBlock(ctBlockSize, 0xBA)
	if err := s.WriteExtentDirect(20, base); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(during + 10)
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatalf("RemoteCheckpoint after BASE: %v", err)
	}
	if got := blockOf(f.remoteImage(t), 20); !bytes.Equal(got, base) {
		t.Fatal("rebuild BASE block (WriteExtentDirect) never reached the remote image")
	}
}

// C5 immutable chunks: a key is never overwritten, new content means a new
// key, and an older HEAD's chunks still read back as they were.
func TestChunkLayer_C5_ChunksAreImmutable(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 0, 0x01)
	mustSyncFlush(t, s)
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	h1, _, _, _ := f.snapshot()
	oldKey := h1.Chunks[0]
	w.write(t, s, 0, 0x02)
	mustSyncFlush(t, s)
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	h2, _, _, over := f.snapshot()
	if over != 0 {
		t.Fatalf("%d Put calls tried to overwrite an existing key", over)
	}
	if h2.Chunks[0] == oldKey {
		t.Fatalf("changed chunk 0 kept key %q", oldKey)
	}
	if b, err := f.Get(context.Background(), oldKey); err != nil || !bytes.Equal(b[:ctBlockSize], makeBlock(ctBlockSize, 0x01)) {
		t.Fatalf("gen %d's chunk 0 no longer reads back as published: %v", h1.Gen, err)
	}
	// Repeating a checkpoint with nothing new must not re-put anything either.
	_, putsBefore, _, _ := f.snapshot()
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if _, putsAfter, _, over := f.snapshot(); over != 0 || putsAfter != putsBefore {
		t.Fatalf("idle checkpoint issued %d puts (%d overwrite attempts)", putsAfter-putsBefore, over)
	}
}

// C6 single writer: a CAS precondition failure fences the layer for good; a
// lost CAS response is resolved by re-reading HEAD, never by a blind retry.
func TestChunkLayer_C6_FencedOnEpochChange(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	mustSyncFlush(t, s)
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	// Another holder takes over: epoch 2.
	h, _, _, _ := f.snapshot()
	if _, err := f.CASHead(context.Background(), chunkHeadVersion{h.Gen, h.Epoch}, chunkHead{Epoch: 2, Holder: "host-b", RemoteLSN: h.RemoteLSN, Chunks: h.Chunks}); err != nil {
		t.Fatal(err)
	}
	taken, _, _, _ := f.snapshot()

	w.write(t, s, 2, 0x23)
	mustSyncFlush(t, s)
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); !errors.Is(err, errChunkHeadPrecondition) && !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("stale holder's checkpoint: got %v, want a precondition/fenced error", err)
	}
	if !layer.Fenced() {
		t.Fatal("layer not fenced after its CAS lost to a newer epoch")
	}
	_, _, casBefore, _ := f.snapshot()
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("fenced layer: got %v, want errChunkLayerFenced", err)
	}
	if err := layer.MakeRemoteDurable(ctTimeout(t), s.CheckpointLSN()); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("fenced MakeRemoteDurable: got %v, want errChunkLayerFenced", err)
	}
	after, _, casAfter, _ := f.snapshot()
	if casAfter != casBefore {
		t.Fatalf("fenced layer issued %d more CAS calls", casAfter-casBefore)
	}
	if after.Gen != taken.Gen || after.Epoch != 2 || after.Holder != "host-b" {
		t.Fatalf("stale holder changed HEAD: gen %d epoch %d holder %q", after.Gen, after.Epoch, after.Holder)
	}
}

func TestChunkLayer_C6_LostCASResponseResolvedByReread(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 7, 0x77)
	mustSyncFlush(t, s)
	c := s.CheckpointLSN()
	f.loseCAS = true
	_, _, casBefore, _ := f.snapshot()
	_, err := layer.RemoteCheckpoint(ctTimeout(t))
	h, _, casAfter, _ := f.snapshot()
	if casAfter-casBefore != 1 {
		t.Fatalf("lost CAS response led to %d CAS calls; resolve by re-reading HEAD, not by retrying", casAfter-casBefore)
	}
	if err != nil {
		t.Fatalf("CAS was applied but its response lost; HEAD re-read should show success, got %v", err)
	}
	if layer.Fenced() {
		t.Fatal("a lost response to our own successful CAS fenced the layer")
	}
	if layer.RemoteLSN() != c || h.RemoteLSN != c {
		t.Fatalf("remote_lsn layer %d HEAD %d, want %d", layer.RemoteLSN(), h.RemoteLSN, c)
	}
	// The next checkpoint must use the re-read generation.
	w.write(t, s, 7, 0x78)
	mustSyncFlush(t, s)
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatalf("checkpoint after a resolved lost response: %v", err)
	}
}

// C7 lazy read: an absent block reads back exactly as in HEAD, ZERO chunks
// read as zeros, and a fill never replaces a newer local block.
func TestChunkLayer_C7_FreshHostReadsLazilyFromHead(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := uint32(0); lba < 8; lba++ { // chunks 0-1 hold data; 2-7 are ZERO
		copy(src[int(lba)*ctBlockSize:], makeBlock(ctBlockSize, byte(0x70+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 100, 1)

	s, _ := newChunkTestStore(t) // empty local volume: a fresh host
	layer := mustAttach(t, s, f, ctConfig(2))
	for _, lba := range []uint32{5, 0, 3, 12, 31} {
		got, err := s.Read(lba)
		if err != nil {
			t.Fatalf("Read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(got, blockOf(src, lba)) {
			t.Fatalf("lazy read LBA %d differs from HEAD", lba)
		}
	}
	if f.gets() == 0 {
		t.Fatal("reads of absent blocks never fetched from the remote")
	}
	// A newer local write in a chunk that has not been fetched yet must win over the fill.
	local := makeBlock(ctBlockSize, 0xCC)
	if _, err := s.Write(6, local); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Read(7); !bytes.Equal(got, blockOf(src, 7)) { // triggers chunk 1's fill
		t.Fatal("neighbour read after a local write differs from HEAD")
	}
	mustSyncFlush(t, s)
	if got, _ := s.Read(6); !bytes.Equal(got, local) {
		t.Fatal("lazy fill replaced a newer local block")
	}
	_ = layer
}

// C8 durability point: MakeRemoteDurable(t) = Sync, checkpoint >= t, upload
// until remote_lsn >= t, CAS. Only a nil return makes t remote durable.
func TestChunkLayer_C8_MakeRemoteDurable(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 10, 0xA0)
	target := w.write(t, s, 17, 0xA7) // written, not synced, not flushed
	if err := layer.MakeRemoteDurable(ctTimeout(t), target); err != nil {
		t.Fatalf("MakeRemoteDurable(%d): %v", target, err)
	}
	h, _, _, _ := f.snapshot()
	if h.RemoteLSN < target {
		t.Fatalf("returned nil but remote_lsn %d < target %d", h.RemoteLSN, target)
	}
	assertImage(t, "image after MakeRemoteDurable", f.remoteImage(t), w.imageAt(h.RemoteLSN))

	// With the remote down it must fail, and HEAD must not claim the target.
	target2 := w.write(t, s, 18, 0xA8)
	f.failPut = errFakeRemoteDown
	if err := layer.MakeRemoteDurable(ctTimeout(t), target2); err == nil {
		t.Fatal("MakeRemoteDurable returned nil with the remote down")
	}
	if h, _, _, _ := f.snapshot(); h.RemoteLSN >= target2 {
		t.Fatalf("HEAD claims remote_lsn %d >= %d although uploads failed", h.RemoteLSN, target2)
	}
}

// C9 eviction: a block may be made absent only when HEAD covers it with
// remote_lsn >= its last written LSN and it is not dirty; a later read
// fetches the same bytes back.
func TestChunkLayer_C9_EvictOnlyWhatHeadCovers(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 8, 0x81)
	mustSyncFlush(t, s)
	if err := layer.Evict(8); !errors.Is(err, errChunkEvictRefused) {
		t.Fatalf("evict before any remote checkpoint: got %v, want refused", err)
	}
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 8, 0x82) // newer than remote_lsn, still dirty
	if err := layer.Evict(8); !errors.Is(err, errChunkEvictRefused) {
		t.Fatalf("evict of a dirty block newer than remote_lsn: got %v, want refused", err)
	}
	mustSyncFlush(t, s)
	if err := layer.Evict(8); !errors.Is(err, errChunkEvictRefused) {
		t.Fatalf("evict of a flushed block above remote_lsn: got %v, want refused", err)
	}
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if err := layer.Evict(8); err != nil {
		t.Fatalf("evict of a covered clean block: %v", err)
	}
	getsBefore := f.gets()
	got, err := s.Read(8)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, makeBlock(ctBlockSize, 0x82)) {
		t.Fatal("read after eviction differs from the last written block")
	}
	if f.gets() == getsBefore {
		t.Fatal("read after eviction did not fetch from the remote (block was not really absent)")
	}
}

// C10 restart: local recovery is unchanged (WAL replay) and independent of
// the remote; after restart the local WAL and extent win over an older HEAD.
func TestChunkLayer_C10_LocalRecoveryWinsOverOlderHead(t *testing.T) {
	// Not newChunkTestStore: this store is crashed below, so it must not be
	// closed again by a cleanup.
	path := filepath.Join(t.TempDir(), "vol.bin")
	s, err := CreateWALStore(path, ctNumBlocks, ctBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	crashed := false
	t.Cleanup(func() {
		if !crashed {
			_ = s.Close()
		}
	})
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 9, 0x91)
	mustSyncFlush(t, s)
	if _, err := layer.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 9, 0x92)
	w.write(t, s, 11, 0xB1)
	if _, err := s.Sync(); err != nil { // L1 only, never uploaded
		t.Fatal(err)
	}
	// Crash.
	crashed = true
	s.committer.Stop()
	if err := s.fd.Close(); err != nil {
		t.Fatal(err)
	}

	f.failGet = errFakeRemoteDown // the remote is down during local recovery
	f.failPut = errFakeRemoteDown
	s2, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.DisableAutoFlushForRecoveryTest()
	if _, err := s2.Recover(); err != nil {
		t.Fatalf("local recovery must not need the remote: %v", err)
	}
	f.failGet, f.failPut = nil, nil
	layer2 := mustAttach(t, s2, f, ctConfig(1))
	for lba, want := range map[uint32]byte{9: 0x92, 11: 0xB1} {
		got, err := s2.Read(lba)
		if err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, want)) {
			t.Fatalf("after restart LBA %d is not the locally synced block (HEAD is older): %v", lba, err)
		}
	}
	mustSyncFlush(t, s2)
	if _, err := layer2.RemoteCheckpoint(ctTimeout(t)); err != nil {
		t.Fatalf("checkpoint after restart: %v", err)
	}
	if got := blockOf(f.remoteImage(t), 9); !bytes.Equal(got, makeBlock(ctBlockSize, 0x92)) {
		t.Fatal("first checkpoint after restart did not publish the recovered local data")
	}
}
