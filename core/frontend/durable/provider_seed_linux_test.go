//go:build linux

package durable_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

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
		if err == nil && name == filepath.Join(root, "v1.bin") {
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

// A real failed WAL fsync during a B restore must keep the guest closed.
// The store's fsync failure is sticky; reopening the same local root retries
// from the unchanged published B image.
func TestRestoreFailedSeedSyncRetryCaptureAndReopen(t *testing.T) {
	ctx := hangGuard(t)
	c, position, data := seededLayoutForSyncFault(t)
	root := filepath.Join(t.TempDir(), "provider")
	var restore func()
	p := c.provider(t, root, v1View(), "r1", func(cfg *durable.ProviderConfig) {
		cfg.WALRecoveryTestDisableFlusher = true
		next := cfg.ChunkLayoutRemoteFor
		cfg.ChunkLayoutRemoteFor = func(name string, me layout.HolderID) (storage.ChunkRemote, error) {
			restore = failSeedFD(t, root)
			return next(name, me)
		}
	})
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ws := s.(*storage.WALStore)
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("real seed fsync should fail with EINVAL: %v", err)
	}
	if restore == nil {
		t.Fatal("real fsync fault not exercised")
	}
	restore()
	if _, err := p.Backend("v1").Read(ctx, 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("failed recovery exposed guest I/O: %v", err)
	}
	if got := ws.NextLSN(); got != 1 {
		t.Fatalf("failed seed advanced allocator to %d", got)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrFsyncFailed) {
		t.Fatalf("same-handle retry after failed fsync: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p1 := c.provider(t, root, v1View(), "r1", nil)
	if _, err := p1.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p1.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal("restore after reopen:", err)
	}
	if _, err := p1.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := c.provider(t, root, v1View(), "r1", nil)
	s2, err := p2.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if next := s2.(*storage.WALStore).NextLSN(); next <= position {
		t.Fatalf("reopen nextLSN=%d, published position=%d", next, position)
	}
	got := make([]byte, 4096)
	if _, err := p2.Backend("v1").Read(ctx, 0, got); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored bytes changed: %v", err)
	}
	if _, err := p2.Backend("v1").Write(ctx, 4096, bytes.Repeat([]byte{0xB8}, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := p2.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if cut, err := p2.CaptureVolume(ctx, "v1"); err != nil || cut <= position {
		t.Fatalf("new cut=%d err=%v, prior position=%d", cut, err, position)
	}
}

func seededLayoutForSyncFault(t *testing.T) (*layoutCase, uint64, []byte) {
	t.Helper()
	ctx := hangGuard(t)
	c := newLayoutCase(t, 64, 4)
	source := c.provider(t, t.TempDir(), viewAs("old", 1), "old", nil)
	s := openRecovered(t, source)
	data := bytes.Repeat([]byte{0xA7}, 4096)
	if _, err := s.Write(0, data); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	position := c.head(t).Position
	if position == 0 {
		t.Fatal("source did not publish B image")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	return c, position, data
}

// B may claim before local seed. A failed seed may leave that claim in HEAD,
// but it cannot change the published image or admit a guest write.
func TestRestoreFailedSeedNeverServesOrChangesImage(t *testing.T) {
	ctx := hangGuard(t)
	c, position, data := seededLayoutForSyncFault(t)
	before := c.head(t)
	root := filepath.Join(t.TempDir(), "provider")
	var restore func()
	p := c.provider(t, root, v1View(), "r1", func(cfg *durable.ProviderConfig) {
		cfg.WALRecoveryTestDisableFlusher = true
		next := cfg.ChunkLayoutRemoteFor
		cfg.ChunkLayoutRemoteFor = func(name string, me layout.HolderID) (storage.ChunkRemote, error) {
			restore = failSeedFD(t, root)
			return next(name, me)
		}
	})
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("seed sync should fail with EINVAL: %v", err)
	}
	if restore == nil {
		t.Fatal("real fsync fault not exercised")
	}
	restore()
	after := c.head(t)
	if after.Position != position || after.Current == nil || before.Current == nil || *after.Current != *before.Current {
		t.Fatalf("failed seed changed published image: before=%+v after=%+v", before, after)
	}
	if _, err := p.Backend("v1").Read(ctx, 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("failed seed admitted I/O: %v", err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrFsyncFailed) {
		t.Fatalf("same-store retry after failed fsync: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	again := c.provider(t, root, v1View(), "r1", nil)
	if _, err := again.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := again.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal("reopen:", err)
	}
	got := make([]byte, 4096)
	if _, err := again.Backend("v1").Read(ctx, 0, got); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reopen changed B image: %v", err)
	}
}
