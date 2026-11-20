//go:build linux

package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeviceDataSyncPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, cache, want    string
		partition, dm, mixed bool
	}{
		{name: "write-back", cache: "write back\n", want: "fsync"},
		{name: "write-through", cache: "write through\n", want: "fdatasync"},
		{name: "missing", want: "fsync"},
		{name: "unknown", cache: "unknown\n", want: "fsync"},
		{name: "partition", cache: "write through\n", want: "fdatasync", partition: true},
		{name: "dm-through", cache: "write through\n", want: "fdatasync", dm: true},
		{name: "dm-mixed", cache: "write through\n", want: "fsync", dm: true, mixed: true},
		{name: "dm-missing", want: "fsync", dm: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			put := func(path, value string) {
				t.Helper()
				path = filepath.Join(root, path)
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(value), 0644); err != nil {
					t.Fatal(err)
				}
			}
			link := func(target, path string) {
				t.Helper()
				path = filepath.Join(root, path)
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(root, target), path); err != nil {
					t.Fatal(err)
				}
			}
			disk := "devices/pci/block/nvme0n1"
			if tc.cache != "" {
				put(disk+"/queue/write_cache", tc.cache)
			}
			device := disk
			if tc.partition {
				put(disk+"/nvme0n1p1/partition", "1\n")
				device = disk + "/nvme0n1p1"
			}
			if tc.dm {
				device = "devices/virtual/block/dm-0"
				put(device+"/dm/name", "volume")
				// A virtual queue's advertised policy is not a backing-device proof.
				put(device+"/queue/write_cache", "write through\n")
				link(disk, device+"/slaves/nvme0n1")
				if tc.mixed {
					put("devices/pci/block/nvme1n1/queue/write_cache", "write back\n")
					link("devices/pci/block/nvme1n1", device+"/slaves/nvme1n1")
				}
			}
			if tc.cache != "" || tc.dm {
				link(device, "dev/block/8:1")
			}
			mode, reason := deviceDataSyncMode(root, 8, 1)
			if mode != tc.want || reason == "" {
				t.Fatalf("mode=%s reason=%q, want %s", mode, reason, tc.want)
			}
		})
	}
}

func TestExplicitDataSyncPolicyDoesNotInspectDevice(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.Close() // Discovery would fail; explicit policy must not inspect the fd.
	for _, mode := range []string{"fsync", "fdatasync"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("SWBLOCK_SYNC", mode)
			got, reason, err := selectedDataSyncMode(f)
			if err != nil || got != mode || !strings.Contains(reason, "override") {
				t.Fatalf("explicit policy = %s %q %v", got, reason, err)
			}
		})
	}
}

func TestDeviceDataSyncMalformedTopology(t *testing.T) {
	for _, shape := range []string{"empty-dm", "cycle", "unreadable-cache"} {
		t.Run(shape, func(t *testing.T) {
			root := t.TempDir()
			device := filepath.Join(root, "dev/block/8:1")
			if err := os.MkdirAll(device, 0755); err != nil {
				t.Fatal(err)
			}
			switch shape {
			case "empty-dm":
				if err := os.Mkdir(filepath.Join(device, "dm"), 0755); err != nil {
					t.Fatal(err)
				}
			case "cycle":
				if err := os.Mkdir(filepath.Join(device, "slaves"), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(device, filepath.Join(device, "slaves/self")); err != nil {
					t.Fatal(err)
				}
			case "unreadable-cache":
				if err := os.MkdirAll(filepath.Join(device, "queue/write_cache"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			mode, reason := deviceDataSyncMode(root, 8, 1)
			if mode != "fsync" || reason == "" {
				t.Fatalf("unsafe fallback: %s %q", mode, reason)
			}
		})
	}
}

func TestDefaultDataSyncUnknownFileFallsBack(t *testing.T) {
	t.Setenv("SWBLOCK_SYNC", "")
	f, err := os.CreateTemp(t.TempDir(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	mode, reason, err := selectedDataSyncMode(f)
	if err != nil || mode != "fsync" || !strings.Contains(reason, "unavailable") {
		t.Fatalf("fallback: %s %q %v", mode, reason, err)
	}
}
