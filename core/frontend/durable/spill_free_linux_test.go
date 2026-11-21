//go:build linux

package durable

import (
	"syscall"
	"testing"
)

// The provider's spill free-space reader reports the bytes an unprivileged
// writer has on the storage root's filesystem.
func TestSpillFree_ReadsTheStorageRootFilesystem(t *testing.T) {
	dir := t.TempDir()
	free, err := spillFree(dir)()
	if err != nil {
		t.Fatal(err)
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		t.Fatal(err)
	}
	want := int64(st.Bavail) * int64(st.Bsize)
	if free <= 0 || free > 2*want+1<<30 || want > 2*free+1<<30 {
		t.Fatalf("spill free %d bytes; statfs says %d", free, want)
	}
	if _, err := spillFree(dir + "/missing")(); err == nil {
		t.Fatal("a missing storage root read as free space")
	}
}
