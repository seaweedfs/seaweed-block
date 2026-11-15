package storage

// In-memory chunkRemote and chunkBitmapStore with fault injection, and the
// shared helpers of the chunk layer contract tests.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
	casUnknownNotApplied             // not applied, returns errFakeCASUnknown
	casAppliedLost                   // applied, returns errFakeCASResponseLost
	casDeferred                      // retained server-side, returns errFakeCASUnknown; applied later by releaseDeferredCAS
)

type deferredCAS struct {
	expected chunkHeadVersion
	next     chunkHead
}

// releaseDeferredCAS lets the server finish every retained CAS now: each
// applies only if HEAD is still at its expected version (as a real filer
// would evaluate the precondition when it finally runs). It returns how
// many applied.
func (f *fakeChunkRemote) releaseDeferredCAS() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	applied := 0
	for _, d := range f.deferred {
		if f.head.Gen == d.expected.Gen && f.head.Epoch == d.expected.Epoch {
			f.installLocked(copyChunkHead(d.next))
			applied++
		}
	}
	f.deferred = nil
	return applied
}

// fakeChunkRemote enforces immutable keys, chunk hashes and the HEAD CAS,
// logs every call in order, and injects faults on request. Every field is
// guarded by mu.
type fakeChunkRemote struct {
	mu   sync.Mutex
	objs map[string][]byte
	head chunkHead // Gen 0 = no HEAD yet

	// ops: "put:<key>", "get:<key>", "gethead", "delete:<key>", and one of
	// "cas:ok", "cas:pre" (precondition), "cas:unknown", "cas:lost", "cas:err".
	ops               []string
	putCalls          int
	getCalls          int
	getHeadCalls      int
	casCalls          int
	putBytes          map[string][]byte // bytes as presented to Put, copied on entry
	overwriteAttempts int               // Put of a key that exists or once existed
	putMutated        int               // Put buffers changed while the Put was in flight
	deletedReferenced int               // Delete of a key some installed HEAD names
	heads             []chunkHead       // every HEAD installed by a CAS, in order
	everStored        map[string]bool

	putGate    chan struct{} // non-nil: Put blocks after recording, until closed
	putEntered chan string   // non-nil: receives each Put key on entry (non-blocking)
	getGate    chan struct{} // non-nil: Get blocks after recording, until closed
	getEntered chan string
	getHoldOne chan struct{} // non-nil: only the next Get blocks on it (one-shot)
	getHoldIn  chan string   // receives the key of the one-shot held Get
	failPut    error
	failGet    error                    // Get and GetHead
	casModes   []fakeCASMode            // consumed one per CAS; empty = casNormal
	beforeCAS  func(f *fakeChunkRemote) // runs once, under mu, before the next CAS is decided
	onGetHead  func(f *fakeChunkRemote) // runs under mu inside every GetHead while set (a read that blocks across a change)
	afterCAS   func(f *fakeChunkRemote) // runs once, under mu, after the next CAS is decided
	intents    map[string]chunkIntent
	deferred   []deferredCAS
}

func newFakeChunkRemote() *fakeChunkRemote {
	return &fakeChunkRemote{objs: map[string][]byte{}, putBytes: map[string][]byte{}, everStored: map[string]bool{}, intents: map[string]chunkIntent{}}
}

func (f *fakeChunkRemote) PutIntent(ctx context.Context, in chunkIntent) (string, error) {
	for _, k := range in.Keys {
		if err := validChunkKey(k); err != nil {
			return "", err
		}
	}
	id, err := newChunkIntentID(in.Gen)
	if err != nil {
		return "", err
	}
	in.ID, in.CreatedNs, in.Keys = id, time.Now().UnixNano(), append([]string(nil), in.Keys...)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "putintent:"+id)
	f.intents[id] = in
	return id, nil
}

func (f *fakeChunkRemote) DeleteIntent(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, "deleteintent:"+id)
	delete(f.intents, id)
	return nil
}

func (f *fakeChunkRemote) ListIntents(ctx context.Context) ([]chunkIntent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]chunkIntent, 0, len(f.intents))
	for _, in := range f.intents {
		in.Keys = append([]string(nil), in.Keys...)
		out = append(out, in)
	}
	return out, nil
}

func (f *fakeChunkRemote) Put(ctx context.Context, key string, data []byte) error {
	if err := validChunkKey(key); err != nil {
		return err
	}
	entry := append([]byte(nil), data...)
	f.mu.Lock()
	f.ops = append(f.ops, "put:"+key)
	f.putCalls++
	f.putBytes[key] = entry
	gate, entered := f.putGate, f.putEntered
	f.mu.Unlock()
	// On every exit, a buffer changed since entry means the caller mutated it
	// while the Put was in flight.
	defer func() {
		if !bytes.Equal(entry, data) {
			f.mu.Lock()
			f.putMutated++
			f.mu.Unlock()
		}
	}()
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
	if f.everStored[key] {
		f.overwriteAttempts++
		return errChunkKeyExists
	}
	if f.failPut != nil {
		return f.failPut
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
	holdOne, holdIn := f.getHoldOne, f.getHoldIn
	f.getHoldOne = nil
	f.mu.Unlock()
	if holdOne != nil {
		if holdIn != nil {
			select {
			case holdIn <- key:
			default:
			}
		}
		select {
		case <-holdOne:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
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
	if hook := f.onGetHead; hook != nil {
		hook(f)
	}
	if f.failGet != nil {
		return chunkHead{}, f.failGet
	}
	return copyChunkHead(f.head), nil
}

func (f *fakeChunkRemote) CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.casCalls++
	if hook := f.beforeCAS; hook != nil {
		f.beforeCAS = nil
		hook(f)
	}
	mode := casNormal
	if len(f.casModes) > 0 {
		mode, f.casModes = f.casModes[0], f.casModes[1:]
	}
	outcome := "cas:err"
	defer func() {
		f.ops = append(f.ops, outcome)
		if hook := f.afterCAS; hook != nil {
			f.afterCAS = nil
			hook(f)
		}
	}()
	if mode == casUnknownNotApplied {
		outcome = "cas:unknown"
		return 0, errFakeCASUnknown
	}
	if mode == casDeferred {
		outcome = "cas:deferred"
		f.deferred = append(f.deferred, deferredCAS{expected: expected, next: copyChunkHead(next)})
		return 0, errFakeCASUnknown
	}
	if expected.Gen != f.head.Gen || expected.Epoch != f.head.Epoch {
		outcome = "cas:pre"
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
		outcome = "cas:lost"
		return 0, errFakeCASResponseLost
	}
	outcome = "cas:ok"
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
	ops                            []string
}

func (f *fakeChunkRemote) stats() fakeStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeStats{
		head: copyChunkHead(f.head), puts: f.putCalls, gets: f.getCalls, getHeads: f.getHeadCalls, cas: f.casCalls,
		overwrite: f.overwriteAttempts, mutated: f.putMutated, deletedRef: f.deletedReferenced, installed: len(f.heads),
		ops: append([]string(nil), f.ops...),
	}
}

// takeOverLocked installs a HEAD from another holder (epoch+1), keeping the image.
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
		b, ok := f.objs[ref.Key]
		if !ok {
			t.Fatalf("HEAD gen %d names chunk %d key %q that is not stored", h.Gen, idx, ref.Key)
		}
		if len(b) != ctChunkBytes {
			t.Fatalf("chunk %d key %q has %d bytes, want %d", idx, ref.Key, len(b), ctChunkBytes)
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

// holdPuts makes every Put block until the returned release is called. The
// release is registered as a cleanup before any worker starts, so a failing
// test never leaves an upload parked on the gate.
func holdPuts(t *testing.T, f *fakeChunkRemote) (entered chan string, release func()) {
	t.Helper()
	gate := make(chan struct{})
	entered = make(chan string, 64)
	f.mu.Lock()
	f.putGate, f.putEntered = gate, entered
	f.mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			f.mu.Lock()
			if f.putGate == gate {
				f.putGate, f.putEntered = nil, nil
			}
			f.mu.Unlock()
			close(gate)
		})
	}
	t.Cleanup(release)
	onUnblock(t, release)
	return entered, release
}

// holdGets is holdPuts for Get.
func holdGets(t *testing.T, f *fakeChunkRemote) (entered chan string, release func()) {
	t.Helper()
	gate := make(chan struct{})
	entered = make(chan string, 64)
	f.mu.Lock()
	f.getGate, f.getEntered = gate, entered
	f.mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			f.mu.Lock()
			if f.getGate == gate {
				f.getGate, f.getEntered = nil, nil
			}
			f.mu.Unlock()
			close(gate)
		})
	}
	t.Cleanup(release)
	onUnblock(t, release)
	return entered, release
}

// holdNextGet makes only the next Get block until release (registered as a
// cleanup and unblocker before any worker starts); later Gets proceed.
func holdNextGet(t *testing.T, f *fakeChunkRemote) (entered chan string, release func()) {
	t.Helper()
	gate := make(chan struct{})
	entered = make(chan string, 1)
	f.mu.Lock()
	f.getHoldOne, f.getHoldIn = gate, entered
	f.mu.Unlock()
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	onUnblock(t, release)
	return entered, release
}

// fakeBitmapStore is an in-memory chunkBitmapStore whose state survives a
// simulated restart (the same object is handed to the next attach). onSave
// and onReset run inside Save and Reset, before they return; block, when
// set, parks every call until closed.
type fakeBitmapStore struct {
	mu       sync.Mutex
	hdr      chunkBitmapHeader
	dirty    map[uint64]bool
	stored   bool // false = nothing usable persisted (lost)
	calls    int  // Load + Save + WriteHeader + Reset
	saves    int
	headers  []chunkBitmapHeader // every WriteHeader, in order
	deltas   []chunkDelta
	failSave error
	onSave   func(dirty []uint64)
	onReset  func(hdr chunkBitmapHeader, stillDirty []uint64)
	block    chan struct{}
	entered  chan struct{} // receives one token per call that parks on block
	lostNext bool          // next Load reports ok=false
}

func newFakeBitmap() *fakeBitmapStore {
	return &fakeBitmapStore{dirty: map[uint64]bool{}, entered: make(chan struct{}, 64)}
}

func (b *fakeBitmapStore) enter() {
	b.mu.Lock()
	b.calls++
	block := b.block
	b.mu.Unlock()
	if block != nil {
		select {
		case b.entered <- struct{}{}:
		default:
		}
		<-block
	}
}

func (b *fakeBitmapStore) Load() (chunkBitmapHeader, []uint64, bool, error) {
	b.enter()
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
	b.enter()
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

func (b *fakeBitmapStore) WriteHeader(hdr chunkBitmapHeader) error {
	b.enter()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hdr, b.stored = hdr, true
	b.headers = append(b.headers, hdr)
	return nil
}

func (b *fakeBitmapStore) Reset(hdr chunkBitmapHeader, stillDirty []uint64, delta chunkDelta) error {
	b.enter()
	b.mu.Lock()
	hook := b.onReset
	b.mu.Unlock()
	if hook != nil {
		hook(hdr, append([]uint64(nil), stillDirty...))
	}
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

func (b *fakeBitmapStore) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
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
	assertNoRemoteMisuse(t, f)
}

func assertNoRemoteMisuse(t *testing.T, f *fakeChunkRemote) {
	t.Helper()
	if st := f.stats(); st.overwrite != 0 || st.mutated != 0 || st.deletedRef != 0 {
		t.Fatalf("remote misuse: %d overwrite attempts, %d Put buffers mutated in flight, %d deletes of referenced keys", st.overwrite, st.mutated, st.deletedRef)
	}
}

// closeBounded closes a store without letting a stuck Close hang the suite:
// a Close still blocked after ctLocalBound is reported, and the process-level
// go test -timeout remains the backstop for a genuinely wedged store.
func closeBounded(t *testing.T, s *WALStore) {
	t.Helper()
	done := make(chan struct{})
	go func() { _ = s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(ctLocalBound):
		t.Errorf("WALStore.Close still blocked after %v (a lock held across remote work?)", ctLocalBound)
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
	t.Cleanup(func() { closeBounded(t, s) })
	return s, path
}

// crashableStore is a store a test may crash; it is closed at cleanup only
// if it was not crashed.
func crashableStore(t *testing.T) (*WALStore, string, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vol.bin")
	s, err := CreateWALStore(path, ctNumBlocks, ctBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	crashed := false
	t.Cleanup(func() {
		if !crashed {
			closeBounded(t, s)
		}
	})
	return s, path, func() {
		crashed = true
		s.committer.Stop()
		if err := s.fd.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// reopen opens and recovers a crashed store; it may be crashed again.
func reopen(t *testing.T, path string) (*WALStore, func()) {
	t.Helper()
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	crashed := false
	t.Cleanup(func() {
		if !crashed {
			closeBounded(t, s)
		}
	})
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Recover(); err != nil {
		t.Fatalf("local recovery: %v", err)
	}
	return s, func() {
		crashed = true
		s.committer.Stop()
		if err := s.fd.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func ctConfig(epoch uint64) chunkLayerConfig {
	return chunkLayerConfig{ChunkBlocks: ctChunkBlocks, Epoch: epoch, Holder: "host-a", Bitmap: newFakeBitmap(), ReadMissTimeout: ctLocalBound}
}

// mustAttach attaches a layer and registers its bounded Close.
func mustAttach(t *testing.T, s *WALStore, r chunkRemote, cfg chunkLayerConfig) chunkLayer {
	t.Helper()
	layer, err := attachChunkLayer(s, r, cfg)
	if errors.Is(err, errChunkLayerNotImplemented) {
		t.Fatalf("RED: %v", err)
	}
	if err != nil {
		t.Fatalf("attachChunkLayer: %v", err)
	}
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { _ = layer.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(ctLocalBound):
			t.Errorf("chunk layer Close still blocked after %v (owned workers not joined)", ctLocalBound)
		}
	})
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

// unblockers holds, per test, the functions that release whatever a test
// holds a worker on (remote gates, a held store lock, a parked bitmap
// store). within()'s cleanup runs them before joining its worker.
var unblockers sync.Map // *testing.T -> *unblockList

type unblockList struct {
	mu  sync.Mutex
	fns []func()
}

// onUnblock registers an idempotent release for t's held resources.
func onUnblock(t *testing.T, fn func()) {
	v, _ := unblockers.LoadOrStore(t, &unblockList{})
	l := v.(*unblockList)
	l.mu.Lock()
	l.fns = append(l.fns, fn)
	l.mu.Unlock()
	t.Cleanup(func() { unblockers.Delete(t) })
}

func runUnblock(t *testing.T) {
	v, ok := unblockers.Load(t)
	if !ok {
		return
	}
	l := v.(*unblockList)
	l.mu.Lock()
	fns := append([]func(){}, l.fns...)
	l.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

// within runs fn and fails the test if it has not returned after
// ctLocalBound. A timeout means the work did NOT settle. Settlement is
// registered before the worker starts: at cleanup (which runs before the
// store and layer closes registered earlier) every unblocker runs, then the
// worker is joined with a bound; a worker still running after that is
// reported, and the process-level go test -timeout is the last backstop.
func within(t *testing.T, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		runUnblock(t)
		select {
		case <-joined:
		case <-time.After(ctLocalBound):
			t.Errorf("%s: worker still running %v after its resources were released", what, ctLocalBound)
		}
	})
	go func() {
		defer close(joined)
		done <- fn()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(ctLocalBound):
		t.Fatalf("%s did not return within %v while the remote was held; its worker is joined at cleanup", what, ctLocalBound)
	}
}

// startCapture runs one Capture in the background. Its context is cancelled
// and its goroutine joined (bounded) at cleanup, registered before it starts.
func startCapture(t *testing.T, layer chunkLayer) <-chan error {
	t.Helper()
	c, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	result := make(chan error, 1)
	t.Cleanup(func() {
		runUnblock(t) // release anything the capture may be parked on
		cancel()
		select {
		case <-done:
		case <-time.After(ctLocalBound):
			t.Errorf("a background Capture did not stop within %v after cancel", ctLocalBound)
		}
	})
	go func() {
		defer close(done)
		_, err := layer.Capture(c)
		result <- err
	}()
	return result
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

func waitErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(ctLocalBound):
		t.Fatalf("%s did not return within %v", what, ctLocalBound)
		return nil
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

// ---- the fakes themselves (pass today) ----

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
	ref := chunkRef{Key: "k", SHA256: sha256.Sum256([]byte("a"))}
	gen, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1, Chunks: map[uint64]chunkRef{0: ref}})
	if err != nil || gen != 1 {
		t.Fatalf("first CAS: gen %d err %v", gen, err)
	}
	if _, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1}); !errors.Is(err, errChunkHeadPrecondition) {
		t.Fatalf("stale CAS: got %v", err)
	}
	if _, err := f.CASHead(c, chunkHeadVersion{1, 1}, chunkHead{Epoch: 1, Chunks: map[uint64]chunkRef{0: {Key: "missing"}}}); err == nil {
		t.Fatal("CAS naming an unstored key must fail")
	}
	if _, err := f.CASHead(c, chunkHeadVersion{1, 1}, chunkHead{Epoch: 1, Chunks: map[uint64]chunkRef{0: {Key: "k"}}}); err == nil {
		t.Fatal("CAS with a wrong chunk hash must fail")
	}
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

func TestChunkFakeRemote_UncertainCASModesAndOutcomeLog(t *testing.T) {
	c := context.Background()
	f := newFakeChunkRemote()
	_ = f.Put(c, "k", []byte("a"))
	start := len(f.stats().ops)
	ref := chunkRef{Key: "k", SHA256: sha256.Sum256([]byte("a"))}
	f.casModes = []fakeCASMode{casUnknownNotApplied, casAppliedLost}
	if _, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1, Chunks: map[uint64]chunkRef{0: ref}}); !errors.Is(err, errFakeCASUnknown) {
		t.Fatalf("unknown mode: got %v", err)
	}
	if h, _ := f.GetHead(c); h.Gen != 0 {
		t.Fatalf("unknown-not-applied CAS was applied: gen %d", h.Gen)
	}
	if _, err := f.CASHead(c, chunkHeadVersion{0, 0}, chunkHead{Epoch: 1, RemoteLSN: 7}); !errors.Is(err, errFakeCASResponseLost) {
		t.Fatalf("lost mode: got %v", err)
	}
	if h, _ := f.GetHead(c); h.Gen != 1 || h.RemoteLSN != 7 {
		t.Fatalf("applied-lost CAS must be applied: gen %d remote_lsn %d", h.Gen, h.RemoteLSN)
	}
	f.beforeCAS = func(f *fakeChunkRemote) { f.takeOverLocked("host-b") }
	if _, err := f.CASHead(c, chunkHeadVersion{1, 1}, chunkHead{Epoch: 1}); !errors.Is(err, errChunkHeadPrecondition) {
		t.Fatalf("CAS after a takeover installed by beforeCAS: got %v", err)
	}
	if h, _ := f.GetHead(c); h.Epoch != 2 || h.Holder != "host-b" {
		t.Fatalf("beforeCAS takeover not installed: epoch %d holder %q", h.Epoch, h.Holder)
	}
	want := "cas:unknown gethead cas:lost gethead cas:pre gethead"
	if got := strings.Join(f.stats().ops[start:], " "); got != want {
		t.Fatalf("ops %q, want %q", got, want)
	}
}

func TestChunkFakeRemote_GatesAndMutationDetector(t *testing.T) {
	c := context.Background()
	f := newFakeChunkRemote()

	// Mutation during a Put that completes.
	entered, release := holdPuts(t, f)
	buf := []byte("orig")
	done := make(chan error, 1)
	go func() { done <- f.Put(c, "m", buf) }()
	waitKey(t, entered, "gated Put")
	buf[0] = 'X'
	release()
	if err := waitErr(t, done, "released Put"); err != nil {
		t.Fatal(err)
	}
	// Mutation during a Put that is cancelled while held.
	entered2, _ := holdPuts(t, f)
	cc, cancel := context.WithCancel(c)
	buf2 := []byte("two!")
	done2 := make(chan error, 1)
	go func() { done2 <- f.Put(cc, "n", buf2) }()
	waitKey(t, entered2, "second gated Put")
	buf2[0] = 'Y'
	cancel()
	if err := waitErr(t, done2, "cancelled Put"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Put: got %v", err)
	}
	if st := f.stats(); st.mutated != 2 {
		t.Fatalf("in-flight buffer mutation counted %d times, want 2 (completed and cancelled)", st.mutated)
	}

	// A held Get stays blocked until release, then returns the exact bytes.
	gEntered, gRelease := holdGets(t, f)
	type res struct {
		b   []byte
		err error
	}
	got := make(chan res, 1)
	go func() { b, err := f.Get(c, "m"); got <- res{b, err} }()
	if k := waitKey(t, gEntered, "gated Get"); k != "m" {
		t.Fatalf("gated Get key %q", k)
	}
	select {
	case r := <-got:
		t.Fatalf("gated Get returned before release: %q %v", r.b, r.err)
	case <-time.After(50 * time.Millisecond):
	}
	gRelease()
	select {
	case r := <-got:
		if r.err != nil || string(r.b) != "orig" {
			t.Fatalf("released Get: %q %v, want the bytes stored at Put entry \"orig\"", r.b, r.err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("released Get did not return")
	}
	// A held Get returns on cancel.
	gEntered2, _ := holdGets(t, f)
	gc, gcancel := context.WithCancel(c)
	got2 := make(chan error, 1)
	go func() { _, err := f.Get(gc, "m"); got2 <- err }()
	waitKey(t, gEntered2, "second gated Get")
	gcancel()
	if err := waitErr(t, got2, "cancelled Get"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Get: got %v", err)
	}
}

func TestChunkFakeBitmap_PersistsAcrossRestartAndBlocks(t *testing.T) {
	b := newFakeBitmap()
	if _, _, ok, _ := b.Load(); ok {
		t.Fatal("empty bitmap reported usable state")
	}
	_ = b.Save([]uint64{3, 5})
	_ = b.WriteHeader(chunkBitmapHeader{Base: chunkBaseInstalling})
	hdr, dirty, ok, _ := b.Load()
	if !ok || hdr.Base != chunkBaseInstalling || len(dirty) != 2 {
		t.Fatalf("after Save+WriteHeader: ok %v hdr %+v dirty %v", ok, hdr, dirty)
	}
	_ = b.Reset(chunkBitmapHeader{BaseLSN: 9, HeadGen: 2, Epoch: 1}, []uint64{5}, chunkDelta{ToLSN: 9, Gen: 2, Dirty: []uint64{3}})
	if b.persisted(3) || !b.persisted(5) {
		t.Fatal("Reset must keep only stillDirty")
	}
	b.lostNext = true
	if _, _, ok, _ := b.Load(); ok {
		t.Fatal("lostNext not honoured")
	}
	b.mu.Lock()
	b.block = make(chan struct{})
	block := b.block
	b.mu.Unlock()
	var unblock sync.Once
	release := func() { unblock.Do(func() { close(block) }) }
	saved := make(chan struct{})
	t.Cleanup(func() { // registered before the Save starts: release, then a bounded join
		release()
		select {
		case <-saved:
		case <-time.After(ctLocalBound):
			t.Errorf("parked Save not joined within %v", ctLocalBound)
		}
	})
	go func() { _ = b.Save(nil); close(saved) }()
	select {
	case <-b.entered: // the Save is parked on the block
	case <-time.After(ctLocalBound):
		t.Fatal("Save never reached the blocked bitmap store")
	}
	select {
	case <-saved:
		t.Fatal("Save returned while the bitmap store was blocked")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case <-saved:
	case <-time.After(ctLocalBound):
		t.Fatal("blocked Save did not return after release")
	}
}
