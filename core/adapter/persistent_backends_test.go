package adapter_test

import (
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

// persistentBackend abstracts the two P13-qualified persistent
// backends so every P13 test runs against both. The policy here is
// the same one the storage contract tests follow: whenever we
// exercise a persistent path, prove it on every backend that
// implements LogicalStorage so we catch backend-specific drift.

type persistentBackend struct {
	name       string
	create     func(t *testing.T, path string) storage.LogicalStorage
	reopen     func(t *testing.T, path string) storage.LogicalStorage
	abruptStop func(t *testing.T, s storage.LogicalStorage)
}

var persistentBackends = []persistentBackend{
	{
		name: "walstore",
		create: func(t *testing.T, path string) storage.LogicalStorage {
			t.Helper()
			s, err := storage.CreateWALStore(path, testNumBlocks, testBlockSize)
			if err != nil {
				t.Fatalf("CreateWALStore: %v", err)
			}
			return s
		},
		reopen: func(t *testing.T, path string) storage.LogicalStorage {
			t.Helper()
			s, err := storage.OpenWALStore(path)
			if err != nil {
				t.Fatalf("OpenWALStore: %v", err)
			}
			if _, err := s.Recover(); err != nil {
				t.Fatalf("Recover WALStore: %v", err)
			}
			return s
		},
		abruptStop: func(t *testing.T, s storage.LogicalStorage) {
			t.Helper()
			ws, ok := s.(*storage.WALStore)
			if !ok {
				t.Fatalf("abruptStop: expected *WALStore, got %T", s)
			}
			if err := ws.SimulateAbruptStop(); err != nil {
				t.Fatalf("WALStore SimulateAbruptStop: %v", err)
			}
		},
	},
	{
		name: "smartwal",
		create: func(t *testing.T, path string) storage.LogicalStorage {
			t.Helper()
			s, err := smartwal.CreateStore(path, testNumBlocks, testBlockSize)
			if err != nil {
				t.Fatalf("smartwal CreateStore: %v", err)
			}
			return s
		},
		reopen: func(t *testing.T, path string) storage.LogicalStorage {
			t.Helper()
			s, err := smartwal.OpenStore(path)
			if err != nil {
				t.Fatalf("smartwal OpenStore: %v", err)
			}
			if _, err := s.Recover(); err != nil {
				t.Fatalf("Recover smartwal: %v", err)
			}
			return s
		},
		abruptStop: func(t *testing.T, s storage.LogicalStorage) {
			t.Helper()
			ss, ok := s.(*smartwal.Store)
			if !ok {
				t.Fatalf("abruptStop: expected *smartwal.Store, got %T", s)
			}
			if err := ss.SimulateAbruptStop(); err != nil {
				t.Fatalf("smartwal SimulateAbruptStop: %v", err)
			}
		},
	},
}

// createPersistentStore creates a new persistent store of the given
// backend at a fresh path inside the test's temp dir. Auto-registers
// graceful Close via t.Cleanup (tests that need abrupt stop mark
// closed via abruptStop and the t.Cleanup Close becomes a no-op).
func (b persistentBackend) createStore(t *testing.T, name string) (storage.LogicalStorage, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name+".bin")
	s := b.create(t, path)
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}
