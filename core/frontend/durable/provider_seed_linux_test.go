//go:build linux

package durable_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type seedSyncFaultRemote struct {
	storage.ChunkRemote
	once       sync.Once
	beforeSeed func()
}

func (r *seedSyncFaultRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	head, err := r.ChunkRemote.GetHead(ctx)
	if err == nil {
		r.once.Do(r.beforeSeed)
	}
	return head, err
}

// Replace only this fixture's WAL descriptor after Recover has read it and
// before the seed's real os.File.Sync. /dev/full fsync returns EINVAL. Restore
// the original open-file description before retry; no production fault seam.
func failSeedFD(t *testing.T, root string) func() {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	fd := -1
	for _, e := range entries {
		name, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err == nil && strings.HasPrefix(name, root+string(os.PathSeparator)) {
			if fd != -1 {
				t.Fatal("more than one fixture WAL descriptor")
			}
			fd, err = strconv.Atoi(e.Name())
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if fd < 0 {
		t.Fatal("fixture WAL descriptor not found")
	}
	backup, err := syscall.Dup(fd)
	if err != nil {
		t.Fatal(err)
	}
	full, err := os.OpenFile("/dev/full", os.O_RDWR, 0)
	if err != nil {
		syscall.Close(backup)
		t.Fatal(err)
	}
	defer full.Close()
	if err := syscall.Dup2(int(full.Fd()), fd); err != nil {
		syscall.Close(backup)
		t.Fatal(err)
	}
	var once sync.Once
	restore := func() {
		once.Do(func() {
			if err := syscall.Dup2(backup, fd); err != nil {
				t.Error(err)
			}
			_ = syscall.Close(backup)
		})
	}
	t.Cleanup(restore)
	return restore
}

func TestRestoreFailedSeedSyncRetryCaptureAndReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	root := filepath.Join(t.TempDir(), "provider")
	remote := storage.NewMemChunkRemote()
	data := bytes.Repeat([]byte{0xA7}, 4*4096)
	if err := remote.Put(ctx, "seed", data); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 3, Holder: "r1", RemoteLSN: 7,
		Chunks: map[uint64]storage.ChunkRef{0: {Key: "seed", SHA256: sha256.Sum256(data)}}}); err != nil {
		t.Fatal(err)
	}
	var restore func()
	fault := &seedSyncFaultRemote{ChunkRemote: remote, beforeSeed: func() { restore = failSeedFD(t, root) }}
	makeProvider := func(r storage.ChunkRemote) *durable.DurableProvider {
		p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root,
			NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, WALRecoveryTestDisableFlusher: true,
			ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return r, nil }}, v1View())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	p := makeProvider(fault)
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ws := s.(*storage.WALStore)
	if _, err := p.RecoverVolume(ctx, "v1"); err == nil {
		t.Fatal("real seed fsync failure was accepted")
	} else if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("expected real fsync EINVAL, got %v", err)
	} else {
		t.Logf("injected real WAL fsync failure: %v", err)
	}
	if restore == nil {
		t.Fatal("fault not exercised")
	}
	restore()
	blocked, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blocked.Read(ctx, 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("failed recovery exposed backend: %v", err)
	}
	if got := ws.NextLSN(); got != 1 {
		t.Errorf("failed seed published allocator watermark %d, want 1", got)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal("same-handle retry:", err)
	}
	if r, _, h := ws.Boundaries(); r != 7 || h != 7 {
		t.Errorf("retry did not durably seed HEAD: R=%d H=%d", r, h)
	}
	if cut, err := p.CaptureVolume(ctx, "v1"); err != nil || cut != 7 {
		t.Errorf("retry cut=%d err=%v, want 7", cut, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := makeProvider(remote)
	s2, err := p2.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if next := s2.(*storage.WALStore).NextLSN(); next != 8 {
		t.Errorf("reopen nextLSN=%d, want 8", next)
	}
	b, err := p2.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if _, err := b.Read(ctx, 0, got); err != nil || !bytes.Equal(got, data[:4096]) {
		t.Fatal("restored bytes changed:", err)
	}
	if _, err := b.Write(ctx, 4096, bytes.Repeat([]byte{0xB8}, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if cut, err := p2.CaptureVolume(ctx, "v1"); err != nil || cut != 8 {
		t.Fatalf("new cut=%d err=%v, want 8", cut, err)
	}
}
