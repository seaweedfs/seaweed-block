//go:build unix

package durable_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// V1-SPEC r2.45 (1), as the tmpfiles.d entry lays it out: a lock directory
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
