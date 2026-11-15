package storage

// In-memory chunkRemote with fault injection, and the shared helpers of the
// chunk layer contract tests (chunk_layer_contract_test.go).

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	ctChunkBytes  = ctChunkBlocks * ctBlockSize
	ctLocalBound  = 5 * time.Second // local work must finish within this while the remote is held
)

var (
	errFakeRemoteDown      = errors.New("fake remote: unavailable")
	errFakeCASUnknown      = errors.New("fake remote: CAS outcome unknown")
	errFakeCASResponseLost = errors.New("fake remote: CAS response lost")
)

type fakeCASMode int

const (
	casNormal            fakeCASMode = iota
	casUnknownNotApplied             // next CAS: not applied, returns errFakeCASUnknown
	casAppliedLost                   // next CAS: applied, returns errFakeCASResponseLost
)

// fakeChunkRemote enforces immutable keys and the HEAD CAS, logs every call
// in order, and injects faults on request. Every field is guarded by mu.
type fakeChunkRemote struct {
	mu   sync.Mutex
	objs map[string][]byte
	head chunkHead // Gen 0 = no HEAD yet

	ops               []string // "put:<key>", "get:<key>", "gethead", "cas", "delete:<key>"
	putCalls          int
	getCalls          int
	getHeadCalls      int
	casCalls          int
	putBytes          map[string][]byte // bytes as presented to Put, copied on entry
	overwriteAttempts int               // Put of a key that exists or once existed
	putMutated        int               // Put buffers changed between entry and store
	deletedReferenced int               // Delete of a key some installed HEAD names
	heads             []chunkHead       // every HEAD installed by a CAS, in order
	everStored        map[string]bool

	putGate    chan struct{} // non-nil: Put blocks after recording, until closed
	putEntered chan string   // non-nil: receives each Put key on entry (non-blocking)
	getGate    chan struct{} // non-nil: Get blocks after recording, until closed
	getEntered chan string
	failPut    error
	failGet    error // Get and GetHead
	casMode    fakeCASMode
	afterCAS   func(f *fakeChunkRemote) // runs once, under mu, after the next CAS is decided
}

func newFakeChunkRemote() *fakeChunkRemote {
	return &fakeChunkRemote{objs: map[string][]byte{}, putBytes: map[string][]byte{}, everStored: map[string]bool{}}
}

func (f *fakeChunkRemote) Put(ctx context.Context, key string, data []byte) error {
	entry := append([]byte(nil), data...)
	f.mu.Lock()
	f.ops = append(f.ops, "put:"+key)
	f.putCalls++
	f.putBytes[key] = entry
	gate, entered, fail := f.putGate, f.putEntered, f.failPut
	reused := f.everStored[key]
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if reused {
		f.overwriteAttempts++
		return errChunkKeyExists
	}
	if fail != nil {
		return fail
	}
	if !bytes.Equal(entry, data) {
		f.putMutated++
	}
	if f.everStored[key] {
		f.overwriteAttempts++
		return errChunkKeyExists
	}
	f.objs[key] = entry
	f.everStored[key] = true
	return nil
}

func (f *fakeChunkRemote) Get(ctx context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	f.ops = append(f.ops, "get:"+key)
	f.getCalls++
	gate, entered := f.getGate, f.getEntered
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
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.ops = append(f.ops, "gethead")
	f.getHeadCalls++
	if f.failGet != nil {
		return chunkHead{}, f.failGet
	}
	return copyChunkHead(f.head), nil
}

func (f *fakeChunkRemote) CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "cas")
	f.casCalls++
	mode := f.casMode
	f.casMode = casNormal
	defer func() {
		if hook := f.afterCAS; hook != nil {
			f.afterCAS = nil
			hook(f)
		}
	}()
	if mode == casUnknownNotApplied {
		return 0, errFakeCASUnknown
	}
	if expected.Gen != f.head.Gen || expected.Epoch != f.head.Epoch {
		return 0, errChunkHeadPrecondition
	}
	for idx, ref := range next.Chunks {
		b, ok := f.objs[ref.Key]
		if !ok {
			return 0, fmt.Errorf("fake remote: HEAD names chunk %d key %q that is not stored", idx, ref.Key)
		}
		if sha256.Sum256(b) != ref.SHA256 {
			return 0, fmt.Errorf("fake remote: HEAD chunk %d key %q carries a SHA-256 that does not match its bytes", idx, ref.Key)
		}
	}
	f.installLocked(next)
	if mode == casAppliedLost {
		return 0, errFakeCASResponseLost
	}
	return f.head.Gen, nil
}

// installLocked installs next as HEAD gen+1. Caller holds mu.
func (f *fakeChunkRemote) installLocked(next chunkHead) {
	installed := copyChunkHead(next)
	installed.Gen = f.head.Gen + 1
	f.head = installed
	f.heads = append(f.heads, copyChunkHead(installed))
}

func (f *fakeChunkRemote) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "delete:"+key)
	for _, h := range f.heads {
		for _, ref := range h.Chunks {
			if ref.Key == key {
				f.deletedReferenced++
			}
		}
	}
	delete(f.objs, key)
	return nil
}

func copyChunkHead(h chunkHead) chunkHead {
	out := h
	out.Chunks = make(map[uint64]chunkRef, len(h.Chunks))
	for k, v := range h.Chunks {
		out.Chunks[k] = v
	}
	return out
}

type fakeStats struct {
	head                           chunkHead
	puts, gets, getHeads, cas      int
	overwrite, mutated, deletedRef int
	installed                      int
	opsSince                       func(int) []string
	opCount                        int
}

func (f *fakeChunkRemote) stats() fakeStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	ops := append([]string(nil), f.ops...)
	return fakeStats{
		head: copyChunkHead(f.head), puts: f.putCalls, gets: f.getCalls, getHeads: f.getHeadCalls, cas: f.casCalls,
		overwrite: f.overwriteAttempts, mutated: f.putMutated, deletedRef: f.deletedReferenced, installed: len(f.heads),
		opsSince: func(i int) []string { return ops[i:] }, opCount: len(ops),
	}
}

// takeOver installs a HEAD from another holder (epoch+1), keeping the image.
func (f *fakeChunkRemote) takeOverLocked(holder string) {
	next := copyChunkHead(f.head)
	next.Epoch = f.head.Epoch + 1
	next.Holder = holder
	f.installLocked(next)
}

// seedHead stores chunk bytes and installs a HEAD directly, standing in for
// a previous holder. image is the whole volume; all-zero chunks stay ZERO.
func (f *fakeChunkRemote) seedHead(t *testing.T, image []byte, remoteLSN, epoch uint64) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	head := chunkHead{Epoch: epoch, Holder: "seed", RemoteLSN: remoteLSN, Chunks: map[uint64]chunkRef{}}
	for idx := 0; idx*ctChunkBytes < len(image); idx++ {
		c := image[idx*ctChunkBytes : (idx+1)*ctChunkBytes]
		if bytes.Equal(c, make([]byte, ctChunkBytes)) {
			continue
		}
		key := fmt.Sprintf("seed/%d", idx)
		f.objs[key] = append([]byte(nil), c...)
		f.everStored[key] = true
		head.Chunks[uint64(idx)] = chunkRef{Key: key, SHA256: sha256.Sum256(c)}
	}
	f.installLocked(head)
}

// imageOf assembles the volume image a HEAD publishes.
func (f *fakeChunkRemote) imageOf(t *testing.T, h chunkHead) []byte {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	img := make([]byte, ctNumBlocks*ctBlockSize)
	for idx, ref := range h.Chunks {
		key := ref.Key
		b, ok := f.objs[key]
		if !ok {
			t.Fatalf("HEAD gen %d names chunk %d key %q that is not stored", h.Gen, idx, key)
		}
		if len(b) != ctChunkBytes {
			t.Fatalf("chunk %d key %q has %d bytes, want %d", idx, key, len(b), ctChunkBytes)
		}
		copy(img[int(idx)*ctChunkBytes:], b)
	}
	return img
}

func (f *fakeChunkRemote) installedHeads() []chunkHead {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]chunkHead, len(f.heads))
	for i, h := range f.heads {
		out[i] = copyChunkHead(h)
	}
	return out
}

// writeLog records what a test wrote, so the image at any LSN is known.
type writeLog struct {
	mu   sync.Mutex
	lsns map[uint64]loggedWrite
}

type loggedWrite struct {
	lba  uint32
	data []byte
}

func newWriteLog() *writeLog { return &writeLog{lsns: map[uint64]loggedWrite{}} }

func (w *writeLog) record(lsn uint64, lba uint32, data []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lsns[lsn] = loggedWrite{lba, data}
}

func (w *writeLog) write(t *testing.T, s *WALStore, lba uint32, fill byte) uint64 {
	t.Helper()
	data := makeBlock(ctBlockSize, fill)
	lsn, err := s.Write(lba, data)
	if err != nil {
		t.Fatalf("Write LBA %d: %v", lba, err)
	}
	w.record(lsn, lba, data)
	return lsn
}

// imageAt is the volume image after every logged write with LSN <= lsn.
func (w *writeLog) imageAt(lsn uint64) []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	img := make([]byte, ctNumBlocks*ctBlockSize)
	for l := uint64(1); l <= lsn; l++ {
		if e, ok := w.lsns[l]; ok {
			copy(img[int(e.lba)*ctBlockSize:], e.data)
		}
	}
	return img
}

// assertEveryHeadIsPrefixImage checks every HEAD ever installed, not only the
// last one: each must publish exactly the image at its remote_lsn.
func assertEveryHeadIsPrefixImage(t *testing.T, f *fakeChunkRemote, w *writeLog) {
	t.Helper()
	heads := f.installedHeads()
	if len(heads) == 0 {
		t.Fatal("no HEAD was installed")
	}
	for _, h := range heads {
		assertImage(t, fmt.Sprintf("HEAD gen %d vs image at its remote_lsn %d", h.Gen, h.RemoteLSN), f.imageOf(t, h), w.imageAt(h.RemoteLSN))
	}
	if st := f.stats(); st.overwrite != 0 || st.mutated != 0 || st.deletedRef != 0 {
		t.Fatalf("remote misuse: %d overwrite attempts, %d Put buffers mutated in flight, %d deletes of referenced keys", st.overwrite, st.mutated, st.deletedRef)
	}
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

// fakeBitmapStore is an in-memory chunkBitmapStore whose state survives a
// simulated restart (the same object is handed to the next attach). onSave
// runs inside Save, before it returns, so a test can check the store's state
// at the moment the dirty set becomes durable.
type fakeBitmapStore struct {
	mu       sync.Mutex
	hdr      chunkBitmapHeader
	dirty    map[uint64]bool
	stored   bool // false = nothing usable persisted (lost)
	saves    int
	deltas   []chunkDelta
	failSave error
	onSave   func(dirty []uint64)
	lostNext bool // next Load reports ok=false
}

func newFakeBitmap() *fakeBitmapStore { return &fakeBitmapStore{dirty: map[uint64]bool{}} }

func (b *fakeBitmapStore) Load() (chunkBitmapHeader, []uint64, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lostNext || !b.stored {
		b.lostNext = false
		return chunkBitmapHeader{}, nil, false, nil
	}
	var out []uint64
	for idx := range b.dirty {
		out = append(out, idx)
	}
	return b.hdr, out, true, nil
}

func (b *fakeBitmapStore) Save(dirty []uint64) error {
	b.mu.Lock()
	hook, fail := b.onSave, b.failSave
	b.mu.Unlock()
	if hook != nil {
		hook(append([]uint64(nil), dirty...))
	}
	if fail != nil {
		return fail
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stored = true
	b.dirty = map[uint64]bool{}
	for _, idx := range dirty {
		b.dirty[idx] = true
	}
	b.saves++
	return nil
}

func (b *fakeBitmapStore) Reset(hdr chunkBitmapHeader, stillDirty []uint64, delta chunkDelta) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hdr, b.stored = hdr, true
	b.dirty = map[uint64]bool{}
	for _, idx := range stillDirty {
		b.dirty[idx] = true
	}
	d := delta
	d.Dirty = append([]uint64(nil), delta.Dirty...)
	d.NewKeys = make(map[uint64]string, len(delta.NewKeys))
	for k, v := range delta.NewKeys {
		d.NewKeys[k] = v
	}
	b.deltas = append(b.deltas, d)
	return nil
}

// persisted reports whether chunk idx is set in the durable bitmap.
func (b *fakeBitmapStore) persisted(idx uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stored && b.dirty[idx]
}

func (b *fakeBitmapStore) snapshot() (chunkBitmapHeader, []chunkDelta, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hdr, append([]chunkDelta(nil), b.deltas...), b.saves
}

func ctConfig(epoch uint64) chunkLayerConfig {
	return chunkLayerConfig{ChunkBlocks: ctChunkBlocks, Epoch: epoch, Holder: "host-a", Bitmap: newFakeBitmap()}
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

// within runs fn and fails the test if it does not return within ctLocalBound
// (a deadlock or a remote-gated local path), instead of hanging the suite.
func within(t *testing.T, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(ctLocalBound):
		t.Fatalf("%s did not finish within %v while the remote was held", what, ctLocalBound)
	}
}

func waitKey(t *testing.T, ch chan string, what string) string {
	t.Helper()
	select {
	case k := <-ch:
		return k
	case <-time.After(ctLocalBound):
		t.Fatalf("%s did not start", what)
		return ""
	}
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
	gen, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1, Chunks: map[uint64]chunkRef{0: {Key: "k", SHA256: sha256.Sum256([]byte("a"))}}})
	if err != nil || gen != 1 {
		t.Fatalf("first CAS: gen %d err %v", gen, err)
	}
	if _, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1}); !errors.Is(err, errChunkHeadPrecondition) {
		t.Fatalf("stale CAS: got %v", err)
	}
	if _, err := f.CASHead(c, chunkHeadVersion{1, 1}, chunkHead{Epoch: 1, Chunks: map[uint64]chunkRef{0: {Key: "missing"}}}); err == nil {
		t.Fatal("CAS naming an unstored key must fail")
	}
	// Deleting a referenced key, then putting it again, are both recorded.
	if err := f.Delete(c, "k"); err != nil {
		t.Fatal(err)
	}
	if err := f.Put(c, "k", []byte("c")); !errors.Is(err, errChunkKeyExists) {
		t.Fatalf("reuse of a deleted key: got %v, want errChunkKeyExists", err)
	}
	if st := f.stats(); st.deletedRef != 1 || st.overwrite != 2 {
		t.Fatalf("misuse counters: deletedRef %d overwrite %d, want 1 and 2", st.deletedRef, st.overwrite)
	}
}

func TestChunkFakeRemote_UncertainCASModes(t *testing.T) {
	c := context.Background()
	f := newFakeChunkRemote()
	_ = f.Put(c, "k", []byte("a"))
	f.casMode = casUnknownNotApplied
	if _, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1, Chunks: map[uint64]chunkRef{0: {Key: "k", SHA256: sha256.Sum256([]byte("a"))}}}); !errors.Is(err, errFakeCASUnknown) {
		t.Fatalf("unknown mode: got %v", err)
	}
	if h, _ := f.GetHead(c); h.Gen != 0 {
		t.Fatalf("unknown-not-applied CAS was applied: gen %d", h.Gen)
	}
	f.casMode = casAppliedLost
	if _, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1, RemoteLSN: 7}); !errors.Is(err, errFakeCASResponseLost) {
		t.Fatalf("lost mode: got %v", err)
	}
	if h, _ := f.GetHead(c); h.Gen != 1 || h.RemoteLSN != 7 {
		t.Fatalf("applied-lost CAS must be applied: gen %d remote_lsn %d", h.Gen, h.RemoteLSN)
	}
	f.afterCAS = func(f *fakeChunkRemote) { f.takeOverLocked("host-b") }
	f.casMode = casUnknownNotApplied
	_, _ = f.CASHead(c, chunkHeadVersion{1, 1}, chunkHead{Epoch: 1})
	if h, _ := f.GetHead(c); h.Epoch != 2 || h.Holder != "host-b" {
		t.Fatalf("afterCAS takeover not installed: epoch %d holder %q", h.Epoch, h.Holder)
	}
	if st := f.stats(); st.getHeads != 3 || st.cas != 3 {
		t.Fatalf("call counters: getHeads %d cas %d, want 3 and 3", st.getHeads, st.cas)
	}
}

func TestChunkFakeRemote_GatesAndMutationDetector(t *testing.T) {
	c := context.Background()
	f := newFakeChunkRemote()
	f.putGate, f.putEntered = make(chan struct{}), make(chan string, 1)
	buf := []byte("orig")
	done := make(chan error, 1)
	go func() { done <- f.Put(c, "m", buf) }()
	waitKey(t, f.putEntered, "gated Put")
	buf[0] = 'X' // the caller mutates its buffer while the Put is in flight
	close(f.putGate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if st := f.stats(); st.mutated != 1 {
		t.Fatalf("in-flight buffer mutation not detected: %d", st.mutated)
	}
	f.getGate, f.getEntered = make(chan struct{}), make(chan string, 1)
	go func() { _, _ = f.Get(c, "m") }()
	if k := waitKey(t, f.getEntered, "gated Get"); k != "m" {
		t.Fatalf("gated Get key %q", k)
	}
	close(f.getGate)
}
