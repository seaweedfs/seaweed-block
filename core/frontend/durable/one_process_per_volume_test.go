package durable_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// lockedProvider is a chunk provider whose host volume-lock directory is dir.
func lockedProvider(t *testing.T, root, lockDir string, remote storage.ChunkRemote) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: root,
		HostVolumeLock: true, VolumeLockDirTestOnly: lockDir,
		BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, v1View())
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// V1-SPEC r2.45 (1): one live process per volume per host. A second
// provider for the same volume id on the same host (another storage root,
// the same host lock directory) is refused before its store opens; once the
// first closes, the volume can be opened again.
func TestOneLiveProcessPerVolumePerHost(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	lockDir := t.TempDir()
	a := lockedProvider(t, t.TempDir(), lockDir, remote)
	if _, err := a.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	held, err := os.Stat(filepath.Join(lockDir, "v1.lock"))
	if err != nil {
		t.Fatal(err)
	}
	bRoot := t.TempDir()
	b := lockedProvider(t, bRoot, lockDir, remote)
	if _, err := b.EnsureStorage("v1"); !errors.Is(err, storage.ErrWALStoreLocked) {
		t.Fatalf("a second provider for the same volume on this host opened: %v; want %v", err, storage.ErrWALStoreLocked)
	}
	if left, err := os.ReadDir(bRoot); err != nil || len(left) != 0 {
		t.Fatalf("the refused second provider touched its root: %v %v", left, err)
	}
	other := lockedProvider(t, t.TempDir(), lockDir, remote)
	if _, err := other.EnsureStorage("v2"); err != nil {
		t.Fatalf("another volume was refused by v1's lock: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	c := lockedProvider(t, t.TempDir(), lockDir, remote)
	if _, err := c.EnsureStorage("v1"); err != nil {
		t.Fatalf("the volume could not be opened after its first owner closed: %v", err)
	}
	// Released, never unlinked: the next owner locks the same file.
	if again, err := os.Stat(filepath.Join(lockDir, "v1.lock")); err != nil || !os.SameFile(held, again) {
		t.Fatalf("the lock file was replaced between owners: %v", err)
	}
}

// V1-SPEC r2.45 (1): the provider creates a missing lock directory (0700)
// and refuses, with the operator's cure, only when it cannot create it.
func TestHostVolumeLockDirectoryCreatedOrRefused(t *testing.T) {
	newProvider := func(dir string) (*durable.DurableProvider, error) {
		return durable.NewDurableProvider(durable.ProviderConfig{
			Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks,
			HostVolumeLock: true, VolumeLockDirTestOnly: dir,
		}, v1View())
	}
	missing := filepath.Join(t.TempDir(), "sw-block", "locks")
	p, err := newProvider(missing)
	if err != nil {
		t.Fatalf("a missing lock directory was not created: %v", err)
	}
	_ = p.Close()
	if fi, err := os.Stat(missing); err != nil || !fi.IsDir() {
		t.Fatalf("lock directory %s: %v", missing, err)
	}
	blocked := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocked, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	p, err = newProvider(filepath.Join(blocked, "locks"))
	if err == nil {
		_ = p.Close()
	}
	if !errors.Is(err, durable.ErrVolumeLockDirUnavailable) || !strings.Contains(err.Error(), "install the tmpfiles.d entry") || !strings.Contains(err.Error(), "chown the directory and its lock files") {
		t.Fatalf("an uncreatable lock directory: %v; want the refusal with its cure", err)
	}
}

// A lock file this process cannot write (one another user created, as the
// root ublk unit does) is still taken: the lock opens it read-only.
func TestHostVolumeLockTakesReadOnlyLockFile(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	lockDir := t.TempDir()
	path := filepath.Join(lockDir, "v1.lock")
	if err := os.WriteFile(path, nil, 0o444); err != nil {
		t.Fatal(err)
	}
	a := lockedProvider(t, t.TempDir(), lockDir, remote)
	if _, err := a.EnsureStorage("v1"); err != nil {
		t.Fatalf("a read-only lock file refused the volume: %v", err)
	}
	b := lockedProvider(t, t.TempDir(), lockDir, remote)
	if _, err := b.EnsureStorage("v1"); !errors.Is(err, storage.ErrWALStoreLocked) {
		t.Fatalf("a second provider on a read-only lock file: %v; want %v", err, storage.ErrWALStoreLocked)
	}
}
