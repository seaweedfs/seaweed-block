package storage

import (
	"errors"
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
