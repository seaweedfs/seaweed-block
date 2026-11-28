package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/geometry"
)

// The cluster spec is read before blockmaster starts serving: a volume of
// exactly MaxNewVolumeBytes is imported, one chunk more refuses the start
// with an error naming the volume and the maximum.
func TestClusterSpecRefusesAVolumeAboveTheMaximum(t *testing.T) {
	for _, c := range []struct {
		name string
		size uint64
		ok   bool
	}{
		{"at the maximum", geometry.MaxNewVolumeBytes, true},
		{"one chunk above", geometry.MaxNewVolumeBytes + 4<<20, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, err := os.ReadFile(writeClusterSpecForTest(t))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "spec.yaml")
			raw := strings.Replace(string(b), "    size_bytes: 1048576\n", "    size_bytes: "+strconv.FormatUint(c.size, 10)+"\n", 1)
			if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			imports, err := loadClusterSpec(path)
			if c.ok {
				if err != nil || len(imports.Volumes) != 1 || imports.Volumes[0].SizeBytes != c.size {
					t.Fatalf("volume of %d bytes: %+v, %v", c.size, imports.Volumes, err)
				}
				return
			}
			if !errors.Is(err, geometry.ErrVolumeTooLarge) || !strings.Contains(err.Error(), `"v1"`) || !strings.Contains(err.Error(), strconv.FormatUint(geometry.MaxNewVolumeBytes, 10)) {
				t.Fatalf("volume of %d bytes: got %v, want ErrVolumeTooLarge naming v1 and the maximum", c.size, err)
			}
		})
	}
}
