package storeident

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Two identities match only when both are fully known and equal: an unknown
// machine id or device (zero value) never matches, not even another unknown.
func TestSameFileSystemTruthTable(t *testing.T) {
	known := Identity{MachineID: "m1", Dev: 7}
	for _, c := range []struct {
		name string
		a, b Identity
		want bool
	}{
		{"same machine same dev", known, Identity{MachineID: "m1", Dev: 7}, true},
		{"same machine other dev", known, Identity{MachineID: "m1", Dev: 8}, false},
		{"other machine same dev", known, Identity{MachineID: "m2", Dev: 7}, false},
		{"both zero", Identity{}, Identity{}, false},
		{"empty machine ids, same dev", Identity{Dev: 7}, Identity{Dev: 7}, false},
		{"same machine, both dev unknown", Identity{MachineID: "m1"}, Identity{MachineID: "m1"}, false},
		{"one side unknown dev", known, Identity{MachineID: "m1"}, false},
		{"one side unknown machine", known, Identity{Dev: 7}, false},
		{"known vs zero", known, Identity{}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.a.SameFileSystem(c.b); got != c.want {
				t.Fatalf("%+v.SameFileSystem(%+v) = %v, want %v", c.a, c.b, got, c.want)
			}
			if got := c.b.SameFileSystem(c.a); got != c.want {
				t.Fatalf("not symmetric: %+v.SameFileSystem(%+v) = %v, want %v", c.b, c.a, got, c.want)
			}
		})
	}
}

// Of on a missing directory leaves the device unknown, so it never matches.
func TestOfMissingDirIsUnknown(t *testing.T) {
	id := Of(filepath.Join(t.TempDir(), "absent"))
	if id.Dev != 0 {
		t.Fatalf("missing dir: Dev = %d, want 0", id.Dev)
	}
	if id.SameFileSystem(id) {
		t.Fatal("an identity with an unknown device matched itself")
	}
}

// On Linux two directories of one file system have the same identity, and
// both parts are known (machine-id present on the test hosts).
func TestOfSameFileSystemOnLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("st_dev / machine-id identity is Linux-only in practice")
	}
	if _, err := os.Stat("/etc/machine-id"); err != nil {
		if _, err := os.Stat("/var/lib/dbus/machine-id"); err != nil {
			t.Skip("no machine-id on this host")
		}
	}
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	a, b := Of(root), Of(sub)
	if !a.Known() {
		t.Fatalf("identity of %s unknown: %+v", root, a)
	}
	if !a.SameFileSystem(b) {
		t.Fatalf("two dirs of one file system differ: %+v vs %+v", a, b)
	}
	if proc := Of("/proc"); proc.Dev != 0 && a.SameFileSystem(proc) {
		t.Fatalf("temp dir and /proc reported as one file system: %+v", a)
	}
}
