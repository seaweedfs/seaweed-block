package main_test

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
)

type kzFaultFS struct {
	tree              fstest.MapFS
	denyDir, denyFile string
}

func (s kzFaultFS) Open(name string) (fs.File, error) { return s.tree.Open(name) }

func (s kzFaultFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == s.denyDir {
		return nil, fs.ErrPermission
	}
	return s.tree.ReadDir(name)
}

func (s kzFaultFS) ReadFile(name string) ([]byte, error) {
	if name == s.denyFile {
		return nil, fs.ErrPermission
	}
	return s.tree.ReadFile(name)
}

func TestKnownZeroPartialNamespaceInventoryMustRefuse(t *testing.T) {
	const nqn = "nqn.2026-11.io.seaweedfs:kz-review"
	m := kzTree(func(m fstest.MapFS) {
		kzSub(m, 1, nqn, "nvme1", "tcp", "S1", true, "nvme1n1")
		m["nvme/nvme1/nvme1n2"] = &fstest.MapFile{Mode: fs.ModeDir}
	})
	if _, err := kzResolve(m, nqn, ""); err == nil || !strings.Contains(err.Error(), "2 namespaces") {
		t.Fatalf("complete inventory control must refuse two namespaces: %v", err)
	}
	d, err := kzResolve(kzFaultFS{tree: m, denyDir: "nvme/nvme1"}, nqn, "")
	if err == nil {
		t.Fatalf("partial inventory accepted %s; unreadable controller namespace listing is not an empty listing", d.Path)
	}
}

func TestKnownZeroUnreadableMatchingSubsystemMustRefuse(t *testing.T) {
	const nqn = "nqn.2026-11.io.seaweedfs:kz-review"
	m := kzTree(func(m fstest.MapFS) {
		kzSub(m, 1, nqn, "nvme1", "tcp", "S1", true, "nvme1n1")
		kzSub(m, 2, nqn, "nvme2", "tcp", "S2", true, "nvme2n1")
	})
	if _, err := kzResolve(m, nqn, ""); err == nil || !strings.Contains(err.Error(), "2 subsystems") {
		t.Fatalf("complete inventory control must refuse two subsystems: %v", err)
	}
	d, err := kzResolve(kzFaultFS{tree: m, denyFile: "nvme-subsystem/nvme-subsys2/subsysnqn"}, nqn, "")
	if err == nil {
		t.Fatalf("partial inventory accepted %s; unreadable subsystem identity is not proof of no match", d.Path)
	}
}

func TestKnownZeroGlobSuppressedInventoryErrorMustRefuse(t *testing.T) {
	const nqn = "nqn.2026-11.io.seaweedfs:kz-review"
	m := kzTree(func(m fstest.MapFS) {
		kzSub(m, 1, nqn, "nvme1", "tcp", "S1", true, "nvme1n1")
		kzSub(m, 2, nqn, "nvme2", "tcp", "S2", true, "nvme2n1")
	})
	if _, err := kzResolve(m, nqn, ""); err == nil || !strings.Contains(err.Error(), "2 subsystems") {
		t.Fatalf("complete inventory control must refuse two subsystems: %v", err)
	}
	d, err := kzResolve(kzFaultFS{tree: m, denyDir: "nvme-subsystem/nvme-subsys2"}, nqn, "")
	if err == nil {
		t.Fatalf("partial discovery accepted %s; fs.Glob hid a directory error", d.Path)
	}
}
