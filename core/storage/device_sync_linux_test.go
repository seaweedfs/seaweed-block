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
		name, cache, want        string
		partition, dm, md, mixed bool
	}{
		{name: "write-back", cache: "write back\n", want: "fsync"},
		{name: "write-through", cache: "write through\n", want: "fdatasync"},
		{name: "missing", want: "fsync"},
		{name: "unknown", cache: "unknown\n", want: "fsync"},
		{name: "partition", cache: "write through\n", want: "fdatasync", partition: true},
		{name: "dm-through", cache: "write through\n", want: "fdatasync", dm: true},
		{name: "dm-mixed", cache: "write through\n", want: "fsync", dm: true, mixed: true},
		{name: "dm-missing", want: "fsync", dm: true},
		{name: "md-through", cache: "write through\n", want: "fdatasync", md: true},
		{name: "md-mixed", cache: "write through\n", want: "fsync", md: true, mixed: true},
		{name: "md-missing", want: "fsync", md: true},
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
			if tc.dm || tc.md {
				device = "devices/virtual/block/dm-0"
				if tc.md {
					device = "devices/virtual/block/md0"
					put(device+"/md/level", "raid1")
				} else {
					put(device+"/dm/name", "volume")
				}
				// A virtual queue's advertised policy is not a backing-device proof.
				put(device+"/queue/write_cache", "write through\n")
				link(disk, device+"/slaves/nvme0n1")
				if tc.mixed {
					put("devices/pci/block/nvme1n1/queue/write_cache", "write back\n")
					link("devices/pci/block/nvme1n1", device+"/slaves/nvme1n1")
				}
			}
			if tc.cache != "" || tc.dm || tc.md {
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
	for _, shape := range []string{"empty-dm", "empty-md", "cycle", "unreadable-cache"} {
		t.Run(shape, func(t *testing.T) {
			root := t.TempDir()
			device := filepath.Join(root, "dev/block/8:1")
			if err := os.MkdirAll(device, 0755); err != nil {
				t.Fatal(err)
			}
			if shape != "unreadable-cache" {
				if err := os.MkdirAll(filepath.Join(device, "queue"), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(device, "queue/write_cache"), []byte("write through\n"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			switch shape {
			case "empty-dm", "empty-md":
				kind := "dm"
				if shape == "empty-md" {
					kind = "md"
				}
				if err := os.Mkdir(filepath.Join(device, kind), 0755); err != nil {
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
			if strings.HasPrefix(shape, "empty-") && !strings.Contains(reason, "backing devices unavailable") {
				t.Fatalf("guard not witnessed: %q", reason)
			}
			if shape == "cycle" && !strings.Contains(reason, "cycle") {
				t.Fatalf("cycle guard not witnessed: %q", reason)
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

func TestExplicitAutoDataSyncPolicy(t *testing.T) {
	t.Setenv("SWBLOCK_SYNC", "auto")
	f, err := os.CreateTemp(t.TempDir(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	mode, reason, err := selectedDataSyncMode(f)
	if err != nil || mode != "fsync" || !strings.Contains(reason, "unavailable") {
		t.Fatalf("auto fallback: %s %q %v", mode, reason, err)
	}
}

func TestDeviceDataSyncStackNodePolicy(t *testing.T) {
	for _, kind := range []string{"dm", "md"} {
		for _, cache := range []string{"write back", "unknown", "missing", "unreadable"} {
			t.Run(kind+"/"+cache, func(t *testing.T) {
				root := t.TempDir()
				device := filepath.Join(root, "dev/block/8:1")
				disk := filepath.Join(root, "devices/block/disk")
				for _, dir := range []string{filepath.Join(device, kind), filepath.Join(device, "queue"), filepath.Join(device, "slaves"), filepath.Join(disk, "queue")} {
					if err := os.MkdirAll(dir, 0755); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(disk, "queue/write_cache"), []byte("write through\n"), 0644); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(disk, filepath.Join(device, "slaves/disk")); err != nil {
					t.Fatal(err)
				}
				switch cache {
				case "missing":
				case "unreadable":
					if err := os.Mkdir(filepath.Join(device, "queue/write_cache"), 0755); err != nil {
						t.Fatal(err)
					}
				default:
					if err := os.WriteFile(filepath.Join(device, "queue/write_cache"), []byte(cache+"\n"), 0644); err != nil {
						t.Fatal(err)
					}
				}
				mode, reason := deviceDataSyncMode(root, 8, 1)
				if mode != "fsync" || !strings.Contains(reason, "write_cache") {
					t.Fatalf("stack node bypassed: %s %q", mode, reason)
				}
			})
		}
	}
}
