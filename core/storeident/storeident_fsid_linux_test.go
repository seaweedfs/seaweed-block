//go:build linux

package storeident

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// On Linux Of reports the directory's statfs f_fsid: non-zero, the value
// statfs itself gives, and equal for two directories of one file system.
// Skipped only where the file system reports no f_fsid at all.
func TestOfReportsFSIDOnLinux(t *testing.T) {
	root := t.TempDir()
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		t.Fatal(err)
	}
	if st.Fsid.X__val[0] == 0 && st.Fsid.X__val[1] == 0 {
		t.Skipf("the file system of %s reports no f_fsid", root)
	}
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	a := Of(root)
	want := uint64(uint32(st.Fsid.X__val[0]))<<32 | uint64(uint32(st.Fsid.X__val[1]))
	if a.FSID == 0 || a.FSID != want {
		t.Fatalf("Of(%s).FSID = %#x, want statfs f_fsid %#x", root, a.FSID, want)
	}
	if b := Of(sub); b.FSID != a.FSID {
		t.Fatalf("two dirs of one file system: fsid %#x vs %#x", a.FSID, b.FSID)
	}
	if Of(filepath.Join(root, "absent")).FSID != 0 {
		t.Fatal("missing dir reported an fsid")
	}
}
