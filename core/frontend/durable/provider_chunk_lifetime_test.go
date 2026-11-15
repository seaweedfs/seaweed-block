package durable_test

// walstore-chunk provider lifetime: interrupted fresh initialization,
// publication authority bound to the attach-time lineage, and attach
// settlement against Close and against a concurrent RecoverVolume. Each
// schedule drives the real provider entry points.

import (
	"bytes"
	"context"
	"errors"
	"sync"
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
