package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWALStoreV3OwnerExcludesOpenAndReadOnly(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	if other, err := openWALStoreV3ForTest(path); !errors.Is(err, ErrWALStoreLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("second writable owner: %v", err)
	}
	if reader, err := openReadOnlyV3ForTest(path, WALStoreOptions{}); !errors.Is(err, ErrWALStoreLocked) {
		if reader != nil {
			_ = reader.Close()
		}
		t.Fatalf("offline reader during a writable open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := openReadOnlyV3ForTest(path, WALStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = reopened.Close()
}

func TestWALStoreV3CreatorKeepsOwnershipThroughPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	var createdFD *os.File
	var stages int
	testCreateOwnerV3 = func(fd *os.File, published bool) {
		createdFD = fd
		stages++
		if other, err := openWALStoreV3ForTest(path); !errors.Is(err, ErrWALStoreLocked) {
			if other != nil {
				_ = other.Close()
			}
			t.Errorf("creation published=%v writable contender: %v", published, err)
		}
		if other, err := openReadOnlyV3ForTest(path, WALStoreOptions{}); !errors.Is(err, ErrWALStoreLocked) {
			if other != nil {
				_ = other.Close()
			}
			t.Errorf("creation published=%v readonly contender: %v", published, err)
		}
	}
	t.Cleanup(func() { testCreateOwnerV3 = nil })
	s, err := createMappedWALStoreV3ForTest(path, 16, 4096, 65536, WModeLagged, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if stages != 2 || s.fd != createdFD {
		t.Errorf("creator must retain the same descriptor: stages=%d samefd=%v", stages, s.fd == createdFD)
	}
	if other, err := openWALStoreV3ForTest(path); !errors.Is(err, ErrWALStoreLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("returned owner lost lock: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = other.Close()
}
