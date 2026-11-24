//go:build unix

package durable_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// As the tmpfiles.d entry lays out the host-lock rule: a lock directory
// the provider creates gets a traversable parent (0755) and is itself 0700,
// whatever the umask. A 0700 parent created by root would keep the unit's
// user out even after the locks directory is chowned to it.
func TestHostVolumeLockCreatesParent0755AndLocks0700(t *testing.T) {
	old := syscall.Umask(0o077) // a strict umask must not narrow the parent
	defer syscall.Umask(old)
	parent := filepath.Join(t.TempDir(), "sw-block")
	locks := filepath.Join(parent, "locks")
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks,
		HostVolumeLock: true, VolumeLockDirTestOnly: locks,
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	for path, want := range map[string]os.FileMode{parent: 0o755, locks: 0o700} {
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := fi.Mode().Perm(); got != want {
			t.Fatalf("%s created %v, want %v", path, got, want)
		}
	}
}

// newLockProvider opens a provider whose host lock directory is locks.
func newLockProvider(t *testing.T, locks string) {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks,
		HostVolumeLock: true, VolumeLockDirTestOnly: locks,
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
}

func wantPerm(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != want {
		t.Fatalf("%s is %v, want %v", path, got, want)
	}
}

// Directories the provider did not create keep their modes: an existing
// parent with a missing lock directory, and both existing.
func TestHostVolumeLockKeepsExistingDirectoryModes(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "sw-block")
	if err := os.Mkdir(parent, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o711); err != nil {
		t.Fatal(err)
	}
	locks := filepath.Join(parent, "locks")
	newLockProvider(t, locks)
	wantPerm(t, parent, 0o711)
	wantPerm(t, locks, 0o700)

	if err := os.Chmod(locks, 0o750); err != nil {
		t.Fatal(err)
	}
	newLockProvider(t, locks)
	wantPerm(t, parent, 0o711)
	wantPerm(t, locks, 0o750)
}

