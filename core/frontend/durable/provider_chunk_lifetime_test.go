package durable_test

// walstore-chunk provider lifetime: interrupted fresh initialization,
// publication authority bound to the attach-time lineage, and attach
// settlement against Close and against a concurrent RecoverVolume. Each
// schedule drives the real provider entry points.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// gatedRemote wraps a remote with failure and hold points.
type gatedRemote struct {
	storage.ChunkRemote

	mu          sync.Mutex
	failGetHead int           // fail the next N GetHead calls
	holdGetHead chan struct{} // non-nil: GetHead blocks until closed
	holdPut     chan struct{} // non-nil: Put blocks until closed
	honourCtx   bool          // a held call also returns when its ctx ends
	holdAfter   bool          // GetHead reads HEAD first, then holds
	entered     chan string   // receives "gethead"/"put" when a call is held
	once        sync.Once
}

func newGated(inner storage.ChunkRemote) *gatedRemote {
	return &gatedRemote{ChunkRemote: inner, entered: make(chan string, 16)}
}

func (g *gatedRemote) hold(ctx context.Context, what string, ch chan struct{}) error {
	if ch == nil {
		return nil
	}
	g.entered <- what
	if g.honourCtx {
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ch
	return nil
}

func (g *gatedRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	g.mu.Lock()
	fail := g.failGetHead > 0
	if fail {
		g.failGetHead--
	}
	ch := g.holdGetHead
	g.mu.Unlock()
	if fail {
		return storage.ChunkHead{}, errors.New("injected GetHead failure")
	}
	if g.holdAfter {
		h, err := g.ChunkRemote.GetHead(ctx)
		_ = g.hold(ctx, "gethead", ch)
		return h, err
	}
	if err := g.hold(ctx, "gethead", ch); err != nil {
		return storage.ChunkHead{}, err
	}
	return g.ChunkRemote.GetHead(ctx)
}

func (g *gatedRemote) Put(ctx context.Context, key string, data []byte) error {
	g.mu.Lock()
	ch := g.holdPut
	g.mu.Unlock()
	if err := g.hold(ctx, "put", ch); err != nil {
		return err
	}
	return g.ChunkRemote.Put(ctx, key, data)
}

// release opens every hold once (safe from cleanup and from the test).
func (g *gatedRemote) release() {
	g.once.Do(func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.holdGetHead != nil {
			close(g.holdGetHead)
		}
		if g.holdPut != nil {
			close(g.holdPut)
		}
	})
}

func waitEntered(t *testing.T, g *gatedRemote, what string) {
	t.Helper()
	select {
	case got := <-g.entered:
		if got != what {
			t.Fatalf("held call %q, want %q", got, what)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no %s call was held", what)
	}
}

func recvErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
		return nil
	}
}

// seededRemote returns a remote whose HEAD publishes 0x33 at LBA 33
// (chunk 2) and 0x64 at LBA 100 (chunk 6).
func seededRemote(t *testing.T) *storage.MemChunkRemote {
	t.Helper()
	remote := storage.NewMemChunkRemote()
	p := chunkProvider(t, t.TempDir(), remote, v1View(), 0)
	s := openRecovered(t, p)
	for lba, fill := range map[uint32]byte{33: 0x33, 100: 0x64} {
		if _, err := s.Write(lba, block(fill)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatalf("seed capture: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	return remote
}

func readEq(t *testing.T, s storage.LogicalStorage, lba uint32, want []byte, what string) {
	t.Helper()
	got, err := s.Read(lba)
	if err != nil {
		t.Fatalf("%s: read %d: %v", what, lba, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: LBA %d = %x...; want %x...", what, lba, got[:4], want[:4])
	}
}

// A fresh volume whose first attach fails is still fresh after a normal
// close and reopen: it reads HEAD, never the untouched local zeros.
func TestWALStoreChunk_FailedFreshAttachThenReopenReadsHead(t *testing.T) {
	remote := seededRemote(t)
	root := t.TempDir()
	g := newGated(remote)
	g.failGetHead = 1
	p := chunkProvider(t, root, g, v1View(), 0)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err == nil {
		t.Fatal("RecoverVolume succeeded although the first attach could not read HEAD")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := chunkProvider(t, root, remote, v1View(), 0)
	s := openRecovered(t, p2)
	readEq(t, s, 33, block(0x33), "reopen after a failed fresh attach")
	readEq(t, s, 100, block(0x64), "reopen after a failed fresh attach")
}

// Interrupted before any attach: created, never recovered, closed.
func TestWALStoreChunk_CreatedNeverAttachedThenReopenReadsHead(t *testing.T) {
	remote := seededRemote(t)
	root := t.TempDir()
	p := chunkProvider(t, root, remote, v1View(), 0)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := chunkProvider(t, root, remote, v1View(), 0)
	s := openRecovered(t, p2)
	readEq(t, s, 33, block(0x33), "reopen of a never-attached fresh volume")
}

// Once a fresh attach succeeded the volume is no longer fresh: a restart
// keeps its local writes over HEAD and still reads untouched chunks from
// HEAD.
func TestWALStoreChunk_FreshVolumeKeepsLocalWritesAfterRestart(t *testing.T) {
	remote := seededRemote(t)
	root := t.TempDir()
	p := chunkProvider(t, root, remote, v1View(), 0)
	s := openRecovered(t, p)
	if _, err := s.Write(33, block(0x77)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := chunkProvider(t, root, remote, v1View(), 0)
	s2 := openRecovered(t, p2)
	readEq(t, s2, 33, block(0x77), "restart after a fresh attach")
	readEq(t, s2, 100, block(0x64), "restart after a fresh attach")
}

// Publication authority is the attach-time lineage. A healthy projection
// that moved to another epoch, replica or endpoint version refuses the
// capture; the unchanged lineage still publishes.
func TestWALStoreChunk_LineageDriftRefusesCapture(t *testing.T) {
	cases := map[string]func(p *frontend.Projection){
		"unchanged": nil,
		"epoch":     func(p *frontend.Projection) { p.Epoch++ },
		"replica":   func(p *frontend.Projection) { p.ReplicaID = "r2" },
		"endpoint":  func(p *frontend.Projection) { p.EndpointVersion++ },
	}
	for name, drift := range cases {
		t.Run(name, func(t *testing.T) {
			remote := storage.NewMemChunkRemote()
			view := v1View()
			p := chunkProvider(t, t.TempDir(), remote, view, 0)
			s := openRecovered(t, p)
			if _, err := s.Write(1, block(1)); err != nil {
				t.Fatal(err)
			}
			if drift != nil {
				proj := view.Projection()
				drift(&proj)
				view.set(proj)
			}
			_, err := p.CaptureVolume(context.Background(), "v1")
			head, _ := remote.GetHead(context.Background())
			if drift == nil {
				if err != nil || head.Gen != 1 {
					t.Fatalf("capture under the attach lineage: %v (HEAD gen %d)", err, head.Gen)
				}
				return
			}
			if !errors.Is(err, storage.ErrChunkCaptureRefused) {
				t.Fatalf("capture after %s drift: %v; want refused", name, err)
			}
			if head.Gen != 0 || remote.Puts() != 0 {
				t.Fatalf("capture after %s drift published: HEAD gen %d, %d chunks stored", name, head.Gen, remote.Puts())
			}
		})
	}
}

// The lineage moves while the capture's upload is held, after the first
// admission check: the final admission before the CAS refuses it.
func TestWALStoreChunk_LineageDriftDuringHeldUploadRefusesPublication(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	g := newGated(remote)
	g.holdPut = make(chan struct{})
	t.Cleanup(g.release)
	view := v1View()
	p := chunkProvider(t, t.TempDir(), g, view, 0)
	s := openRecovered(t, p)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { _, err := p.CaptureVolume(context.Background(), "v1"); errc <- err }()
	waitEntered(t, g, "put")
	proj := view.Projection()
	proj.Epoch++
	view.set(proj)
	g.release()
	if err := recvErr(t, errc, "CaptureVolume"); !errors.Is(err, storage.ErrChunkCaptureRefused) {
		t.Fatalf("capture whose lineage moved during upload: %v; want refused", err)
	}
	if head, _ := remote.GetHead(context.Background()); head.Gen != 0 {
		t.Fatalf("stale lineage published HEAD gen %d", head.Gen)
	}
}

// Close waits for an attach that is in flight and the attach never
// publishes after shutdown: RecoverVolume fails and the volume is gone.
func TestWALStoreChunk_CloseSettlesHeldAttach(t *testing.T) {
	g := newGated(storage.NewMemChunkRemote())
	g.holdGetHead = make(chan struct{})
	t.Cleanup(g.release)
	p := chunkProvider(t, t.TempDir(), g, v1View(), 0)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	recErr := make(chan error, 1)
	go func() { _, err := p.RecoverVolume(context.Background(), "v1"); recErr <- err }()
	waitEntered(t, g, "gethead")
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while an attach was still in flight")
	case <-time.After(200 * time.Millisecond):
	}
	g.release()
	if err := recvErr(t, recErr, "RecoverVolume"); err == nil {
		t.Fatal("RecoverVolume succeeded after the provider was closed")
	}
	_ = recvErr(t, closed, "Close")
	if _, err := p.CaptureVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrVolumeNotOpen) {
		t.Fatalf("chunk layer reachable after Close: %v", err)
	}
}

// The attach read HEAD successfully before Close; it completes after
// Close with no remote call left to cancel, and must still publish
// nothing.
func TestWALStoreChunk_AttachFinishingAfterClosePublishesNothing(t *testing.T) {
	g := newGated(storage.NewMemChunkRemote())
	g.holdGetHead = make(chan struct{})
	g.holdAfter = true
	t.Cleanup(g.release)
	p := chunkProvider(t, t.TempDir(), g, v1View(), 0)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	recErr := make(chan error, 1)
	go func() { _, err := p.RecoverVolume(context.Background(), "v1"); recErr <- err }()
	waitEntered(t, g, "gethead")
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	time.Sleep(100 * time.Millisecond) // Close is now waiting for the attach
	g.release()
	if err := recvErr(t, recErr, "RecoverVolume"); err == nil {
		t.Fatal("RecoverVolume succeeded for an attach that finished after Close")
	}
	_ = recvErr(t, closed, "Close")
	if _, err := p.CaptureVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrVolumeNotOpen) {
		t.Fatalf("chunk layer reachable after Close: %v", err)
	}
}

// Close cancels the attach's remote call instead of waiting on the remote.
func TestWALStoreChunk_CloseCancelsAttachRemoteCall(t *testing.T) {
	g := newGated(storage.NewMemChunkRemote())
	g.holdGetHead = make(chan struct{})
	g.honourCtx = true
	t.Cleanup(g.release)
	p := chunkProvider(t, t.TempDir(), g, v1View(), 0)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	recErr := make(chan error, 1)
	go func() { _, err := p.RecoverVolume(context.Background(), "v1"); recErr <- err }()
	waitEntered(t, g, "gethead")
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	_ = recvErr(t, closed, "Close")
	if err := recvErr(t, recErr, "RecoverVolume"); err == nil {
		t.Fatal("RecoverVolume succeeded after the provider was closed")
	}
}

// Two RecoverVolume calls racing on one volume both settle on a single
// attached layer.
func TestWALStoreChunk_ConcurrentRecoverAttachesOnce(t *testing.T) {
	g := newGated(storage.NewMemChunkRemote())
	g.holdGetHead = make(chan struct{})
	t.Cleanup(g.release)
	p := chunkProvider(t, t.TempDir(), g, v1View(), 0)
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := p.RecoverVolume(context.Background(), "v1"); errs <- err }()
	}
	waitEntered(t, g, "gethead")
	time.Sleep(100 * time.Millisecond) // let the second call reach its attach
	g.release()
	for i := 0; i < 2; i++ {
		if err := recvErr(t, errs, "RecoverVolume"); err != nil {
			t.Fatalf("concurrent RecoverVolume: %v", err)
		}
	}
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatalf("capture after concurrent recovery: %v", err)
	}
}

// The caller's recovery deadline governs the attach's first remote call,
// with no Close and no hold release.
func TestWALStoreChunk_RecoverHonoursCallerDeadline(t *testing.T) {
	g := newGated(storage.NewMemChunkRemote())
	g.holdGetHead = make(chan struct{})
	g.honourCtx = true
	t.Cleanup(g.release)
	p := chunkProvider(t, t.TempDir(), g, v1View(), 0)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	recErr := make(chan error, 1)
	go func() { _, err := p.RecoverVolume(ctx, "v1"); recErr <- err }()
	waitEntered(t, g, "gethead")
	if err := recvErr(t, recErr, "RecoverVolume past its caller deadline"); err == nil {
		t.Fatal("RecoverVolume succeeded although its deadline expired during attach")
	}
}

// An attached layer outlives the recovery context: main cancels it right
// after recovery, and captures must keep working.
func TestWALStoreChunk_LayerOutlivesRecoveryContext(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	p := chunkProvider(t, t.TempDir(), remote, v1View(), 0)
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatalf("capture after the recovery context ended: %v", err)
	}
}

// Recovery before the master's assignment is visible does not bind the
// layer to epoch 0: once the projection carries the assignment, the
// volume publishes under it.
func TestWALStoreChunk_AttachWaitsForMasterAuthority(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"}) // pre-assignment
	p := chunkProvider(t, t.TempDir(), remote, view, 0)
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatalf("RecoverVolume before assignment: %v", err)
	}
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	// Healthy but with no master-assigned epoch is not authority either.
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"}))
	time.Sleep(150 * time.Millisecond)
	if _, err := p.CaptureVolume(context.Background(), "v1"); err == nil {
		t.Fatal("capture succeeded under a healthy projection with epoch 0")
	}
	if head, _ := remote.GetHead(context.Background()); head.Gen != 0 {
		t.Fatalf("published under epoch 0: HEAD gen %d epoch %d", head.Gen, head.Epoch)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2}))
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err = p.CaptureVolume(context.Background(), "v1")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture never succeeded under the assigned epoch: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if head, _ := remote.GetHead(context.Background()); head.Epoch != 4 {
		t.Fatalf("HEAD epoch %d, want the assigned 4", head.Epoch)
	}
}

// Close while a volume still waits for authority returns and leaves no
// layer behind.
func TestWALStoreChunk_CloseWhileAwaitingAuthority(t *testing.T) {
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), view, 0)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	_ = recvErr(t, closed, "Close")
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2}))
	if _, err := p.CaptureVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrVolumeNotOpen) {
		t.Fatalf("chunk layer reachable after Close: %v", err)
	}
}

// Close settles a deferred attach that is in flight: the authority wait
// is registered like a recovery.
func TestWALStoreChunk_CloseSettlesDeferredAttach(t *testing.T) {
	g := newGated(storage.NewMemChunkRemote())
	g.holdGetHead = make(chan struct{})
	t.Cleanup(g.release)
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	p := chunkProvider(t, t.TempDir(), g, view, 0)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2}))
	waitEntered(t, g, "gethead") // the deferred attach is reading HEAD
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while a deferred attach was still in flight")
	case <-time.After(200 * time.Millisecond):
	}
	g.release()
	_ = recvErr(t, closed, "Close")
	if _, err := p.CaptureVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrVolumeNotOpen) {
		t.Fatalf("chunk layer reachable after Close: %v", err)
	}
}

// corruptWALWrite finds the WAL record of the one write carrying fill and
// turns it into a CRC-valid write-batch record with no block count, which
// recovery refuses as a WAL integrity fault. It returns a restore func.
func corruptWALWrite(t *testing.T, path string, fill byte) func() {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{fill}, 4096)
	at := bytes.Index(raw, payload)
	if at < 30 || bytes.Index(raw[at+1:], payload) >= 0 {
		t.Fatalf("WAL record for fill %#x not found exactly once", fill)
	}
	const hdr = 30 // LSN 8, Reserved 8, Type 1, Flags 1, LBA 8, Length 4
	start := at - hdr
	rec := append([]byte(nil), raw[start:at+4096+8]...)
	orig := append([]byte(nil), rec...)
	if rec[16] != 0x01 || binary.LittleEndian.Uint64(rec[8:]) != 0 {
		t.Fatalf("unexpected WAL record header % x", rec[:hdr])
	}
	rec[16] = 0x04 // write batch, Reserved stays 0: invalid
	binary.LittleEndian.PutUint32(rec[hdr+4096:], crc32.ChecksumIEEE(rec[:hdr+4096]))
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(rec, int64(start)); err != nil {
		t.Fatal(err)
	}
	return func() {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.WriteAt(orig, int64(start)); err != nil {
			t.Fatal(err)
		}
	}
}

// A deferred attach belongs to the recovery that registered it. Recovery
// A succeeds before authority; recovery B then fails on a real WAL
// integrity fault; when authority arrives, A's wait must neither attach
// nor admit I/O. A later successful recovery converges.
func TestWALStoreChunk_StaleWaiterCannotOverrideFailedRecovery(t *testing.T) {
	root := t.TempDir()
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	remote := storage.NewMemChunkRemote()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: root, BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		WALRecoveryTestDisableFlusher: true,
		ChunkRemoteFor:                func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil { // A: ok, no authority
		t.Fatalf("recovery A: %v", err)
	}
	if _, err := s.Write(5, block(0xA7)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	restore := corruptWALWrite(t, filepath.Join(root, "v1.bin"), 0xA7)
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrWALIntegrityFault) { // B
		t.Fatalf("recovery B over a corrupt WAL record: %v; want a WAL integrity fault", err)
	}

	authority := healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2})
	view.set(authority)
	be, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // many authority-wait ticks
	if _, err := be.Write(ctx, 7*4096, block(1)); err == nil {
		t.Fatal("I/O admitted after the latest recovery failed")
	}
	if _, err := p.CaptureVolume(ctx, "v1"); err == nil {
		t.Fatal("chunk layer attached after the latest recovery failed")
	}

	// Convergence: repair, recover again without authority, then authority.
	restore()
	view.set(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil { // C
		t.Fatalf("recovery C after repair: %v", err)
	}
	view.set(authority)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err = be.Write(ctx, 7*4096, block(1)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("I/O never admitted after a later successful recovery: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := captureWhenReconciled(ctx, p, "v1"); err != nil {
		t.Fatalf("capture after convergence: %v", err)
	}
	readEq(t, s, 5, block(0xA7), "repaired WAL record after convergence")
}

// A failed recovery retires the authority wait of the earlier successful
// one: the volume no longer reports a layer awaiting authority.
func TestWALStoreChunk_FailedRecoveryRetiresAuthorityWait(t *testing.T) {
	root := t.TempDir()
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: root, BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		WALRecoveryTestDisableFlusher: true,
		ChunkRemoteFor:                func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil },
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CaptureVolume(ctx, "v1"); errors.Is(err, durable.ErrNotChunkVolume) || err == nil {
		t.Fatalf("after recovery without authority: %v; want the volume awaiting authority", err)
	}
	if _, err := s.Write(5, block(0xA7)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	corruptWALWrite(t, filepath.Join(root, "v1.bin"), 0xA7)
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrWALIntegrityFault) {
		t.Fatalf("failing recovery: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := p.CaptureVolume(ctx, "v1")
		if errors.Is(err, durable.ErrNotChunkVolume) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after a failed recovery: %v; want no layer and no live authority wait", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// parkingView parks exactly one Projection call once armed, until the test
// releases it with the projection it should return; that projection then
// stays current.
type parkingView struct {
	*stubView
	mu      sync.Mutex
	armed   bool
	parked  chan struct{}
	release chan frontend.Projection
}

func (v *parkingView) arm() {
	v.mu.Lock()
	v.armed, v.parked, v.release = true, make(chan struct{}), make(chan frontend.Projection)
	v.mu.Unlock()
}

func (v *parkingView) Projection() frontend.Projection {
	v.mu.Lock()
	if !v.armed {
		v.mu.Unlock()
		return v.stubView.Projection()
	}
	v.armed = false
	parked, release := v.parked, v.release
	v.mu.Unlock()
	close(parked)
	p := <-release
	v.stubView.set(p)
	return p
}

// countingRemote counts HEAD reads (an attach starts with one).
type countingRemote struct {
	storage.ChunkRemote
	heads atomic.Int64
}

func (c *countingRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	c.heads.Add(1)
	return c.ChunkRemote.GetHead(ctx)
}

// The window the per-tick check cannot cover: wait A has passed its tick
// check and is reading the projection when recovery B takes the volume and
// fails. Only the re-check under the per-volume serialization stops A.
func TestWALStoreChunk_StaleWaiterPastTickCheckCannotAttach(t *testing.T) {
	root := t.TempDir()
	view := &parkingView{stubView: newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})}
	remote := &countingRemote{ChunkRemote: storage.NewMemChunkRemote()}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: root, BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		WALRecoveryTestDisableFlusher: true,
		ChunkRemoteFor:                func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil { // A: ok, no authority
		t.Fatalf("recovery A: %v", err)
	}
	if _, err := s.Write(5, block(0xA7)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	restore := corruptWALWrite(t, filepath.Join(root, "v1.bin"), 0xA7)

	// Park A's wait inside its projection read, after its tick check.
	view.arm()
	select {
	case <-view.parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the authority wait never read the projection")
	}
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrWALIntegrityFault) { // B
		t.Fatalf("recovery B over a corrupt WAL record: %v; want a WAL integrity fault", err)
	}
	authority := healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2})
	view.release <- authority // A resumes and sees authority

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err = p.CaptureVolume(ctx, "v1")
		if !errors.Is(err, durable.ErrChunkAwaitingAuthority) {
			break // A has settled
		}
		if time.Now().After(deadline) {
			t.Fatal("the stale authority wait never settled")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !errors.Is(err, durable.ErrNotChunkVolume) {
		t.Fatalf("stale wait attached a layer after the latest recovery failed: capture %v", err)
	}
	if n := remote.heads.Load(); n != 0 {
		t.Fatalf("stale wait started a remote attach (%d HEAD reads)", n)
	}
	be, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := be.Write(ctx, 7*4096, block(1)); err == nil {
		t.Fatal("stale wait admitted I/O after the latest recovery failed")
	}

	// Convergence: repair, recovery C without authority, then authority.
	restore()
	view.set(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil { // C
		t.Fatalf("recovery C after repair: %v", err)
	}
	view.set(authority)
	deadline = time.Now().Add(10 * time.Second)
	for {
		if _, err = be.Write(ctx, 7*4096, block(1)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("I/O never admitted after a later successful recovery: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := captureWhenReconciled(ctx, p, "v1"); err != nil {
		t.Fatalf("capture after convergence: %v", err)
	}
	readEq(t, s, 5, block(0xA7), "repaired WAL record after convergence")
}

// captureWhenReconciled cuts once the volume's chunk layer has reconciled
// its dirty set: a volume recovered with local writes reconciles first,
// and a cut meanwhile is refused as retryable (ErrChunkReconciling).
func captureWhenReconciled(ctx context.Context, p *durable.DurableProvider, vol string) (uint64, error) {
	for {
		h, err := p.CaptureVolume(ctx, vol)
		if !errors.Is(err, storage.ErrChunkReconciling) || ctx.Err() != nil {
			return h, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}
