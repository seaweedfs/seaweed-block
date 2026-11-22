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
	b := lockedProvider(t, t.TempDir(), lockDir, remote)
	if _, err := b.EnsureStorage("v1"); !errors.Is(err, storage.ErrWALStoreLocked) {
		t.Fatalf("a second provider for the same volume on this host opened: %v; want %v", err, storage.ErrWALStoreLocked)
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
}

// V1-SPEC r2.45 (1): without its host lock directory the provider refuses
// to start, with the operator's cure; it never creates the directory. The
// default path is checked where the host lacks it.
func TestHostVolumeLockRefusesMissingDirectory(t *testing.T) {
	dirs := []string{filepath.Join(t.TempDir(), "locks")}
	if _, err := os.Stat(durable.HostVolumeLockDir); errors.Is(err, os.ErrNotExist) {
		dirs = append(dirs, "") // the default path
	} else {
		t.Logf("this host has %s: default path not exercised", durable.HostVolumeLockDir)
	}
	for _, dir := range dirs {
		p, err := durable.NewDurableProvider(durable.ProviderConfig{
			Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks,
			HostVolumeLock: true, VolumeLockDirTestOnly: dir,
		}, v1View())
		if err == nil {
			_ = p.Close()
		}
		if !errors.Is(err, durable.ErrVolumeLockDirMissing) || !strings.Contains(err.Error(), "install the tmpfiles.d entry or create the directory") {
			t.Fatalf("lock dir %q: a provider without its lock directory started: %v", dir, err)
		}
		want := dir
		if want == "" {
			want = durable.HostVolumeLockDir
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not name %s: %v", want, err)
		}
		if _, statErr := os.Stat(want); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("the provider created its lock directory %s: %v", want, statErr)
		}
	}
}
