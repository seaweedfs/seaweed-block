package durable_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Permanent recovery-fault classification: INV-FE-ERROR-RETRY-CLASS-001, coverage A9.1.
// A real strict-W WAL integrity failure must survive recovery readiness gating
// as a permanent cause. Ordinary awaiting-recovery readiness stays retryable.
func TestRecoveryStoreFaultRemainsPermanentAtGuestIO(t *testing.T) {
	root := t.TempDir()
	remote := storage.NewMemChunkRemote()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: root, BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		WALRecoveryTestDisableFlusher: true,
		ChunkRemoteFor:                func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b := p.Backend("v1")
	if _, err := b.Read(ctx, 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) ||
		frontend.ClassifyBackendError(ctx, err) != frontend.BackendErrorNotReady {
		t.Fatalf("ordinary pre-recovery gate is not retryable not-ready: %v", err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(5, block(0xA7)); err != nil {
		t.Fatal(err)
	}
	ws, ok := s.(*storage.WALStore)
	if !ok {
		t.Fatalf("unexpected store %T", s)
	}
	if err := ws.SetWMode(storage.WModeStrict); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	restore := corruptWALWrite(t, filepath.Join(root, "v1.bin"), 0xA7)
	t.Cleanup(restore)
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrWALIntegrityFault) {
		t.Fatalf("expected real WAL integrity recovery failure: %v", err)
	}
	n, err := b.Read(ctx, 5*4096, make([]byte, 4096))
	if n != 0 || !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("failed recovery guest read: n=%d, want 0 and not-ready: %v", n, err)
	}
	if got := frontend.ClassifyBackendError(ctx, err); got != frontend.BackendErrorPermanent {
		t.Errorf("store-fault recovery became guest retry class %v, want permanent: %v", got, err)
	}
	if !errors.Is(err, storage.ErrWALIntegrityFault) {
		t.Errorf("guest error lost permanent WAL-integrity cause: %v", err)
	}
	// A later successful recovery clears the old cause. A subsequent ordinary
	// readiness pause must not inherit a repaired fault's permanent class.
	restore()
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("recovery after repair: %v", err)
	}
	want := block(0xA7)
	got := make([]byte, len(want))
	n, err = b.Read(ctx, 5*4096, got)
	if err != nil || n != len(want) || !bytes.Equal(got, want) {
		t.Fatalf("repaired acknowledged record through backend: n=%d, want %d; matching bytes=%t: %v", n, len(want), bytes.Equal(got, want), err)
	}
	b.SetOperational(false, "maintenance pause")
	_, err = b.Read(ctx, 5*4096, make([]byte, 4096))
	if !errors.Is(err, frontend.ErrNotReady) || errors.Is(err, storage.ErrWALIntegrityFault) ||
		frontend.ClassifyBackendError(ctx, err) != frontend.BackendErrorNotReady {
		t.Fatalf("ordinary pause retained the repaired recovery fault: %v", err)
	}
}
