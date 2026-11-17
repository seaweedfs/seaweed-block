package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestChunkLSNMapV3_EntryBoundBeforeRootIO(t *testing.T) {
	d := mapDescriptorV3(t)
	d.BlockSize = 1
	d.VolumeSize = uint64(int(^uint(0)>>1))/16 + 1
	f, err := os.Create(filepath.Join(t.TempDir(), "closed-map"))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	// The geometry must be refused before even Stat, without allocating a root.
	_, _, err = readChunkLSNMapV3(f, d, 1)
	if !errors.Is(err, ErrMapInvalid) {
		t.Fatalf("entry bound must precede file I/O: %v", err)
	}
}
