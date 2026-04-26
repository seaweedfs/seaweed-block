package transport

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func newPersistentTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "seaweed-block-transport-persist-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() {
		var lastErr error
		for i := 0; i < 20; i++ {
			lastErr = os.RemoveAll(dir)
			if lastErr == nil || os.IsNotExist(lastErr) {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Logf("best-effort RemoveAll(%s): %v", dir, lastErr)
	})
	return dir
}

func createWALStoreForTest(t *testing.T, dir, name string) *storage.WALStore {
	t.Helper()
	path := filepath.Join(dir, name)
	s, err := storage.CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatalf("CreateWALStore(%s): %v", name, err)
	}
	return s
}

func writePersistentBlocks(t *testing.T, store storage.LogicalStorage, count uint32) uint64 {
	t.Helper()
	for i := uint32(0); i < count; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		data[1] = byte(i + 0xA0)
		if _, err := store.Write(i, data); err != nil {
			t.Fatalf("Write LBA %d: %v", i, err)
		}
	}
	lsn, err := store.Sync()
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	return lsn
}

func assertLogicalStorageMatch(t *testing.T, label string, primary, replica storage.LogicalStorage, count uint32) {
	t.Helper()
	for i := uint32(0); i < count; i++ {
		pd, err := primary.Read(i)
		if err != nil {
			t.Fatalf("%s: primary read LBA %d: %v", label, i, err)
		}
		rd, err := replica.Read(i)
		if err != nil {
			t.Fatalf("%s: replica read LBA %d: %v", label, i, err)
		}
		if !bytes.Equal(pd, rd) {
			t.Fatalf("%s: LBA %d mismatch (primary[0]=%d replica[0]=%d)", label, i, pd[0], rd[0])
		}
	}
}

func TestTransport_CatchUp_WALStore_PersistsAcrossReplicaReopen(t *testing.T) {
	dir := newPersistentTestDir(t)
	primary := createWALStoreForTest(t, dir, "primary-catchup.dat")
	replica := createWALStoreForTest(t, dir, "replica-catchup.dat")

	listener, err := NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()

	pH := writePersistentBlocks(t, primary, 10)

	exec := NewBlockExecutor(primary, listener.Addr())
	startCh := make(chan adapter.SessionStartResult, 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartCatchUp("r1", 41, 1, 1, 1, pH); err != nil {
		t.Fatal(err)
	}

	select {
	case <-startCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for catch-up start")
	}

	select {
	case result := <-closeCh:
		if !result.Success {
			t.Fatalf("catch-up failed: %s", result.FailReason)
		}
		if result.AchievedLSN != pH {
			t.Fatalf("achievedLSN=%d want %d", result.AchievedLSN, pH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for catch-up close")
	}

	listener.Stop()
	if err := replica.Close(); err != nil {
		t.Fatalf("replica Close: %v", err)
	}

	reopened, err := storage.OpenWALStore(filepath.Join(dir, "replica-catchup.dat"))
	if err != nil {
		t.Fatalf("OpenWALStore replica reopen: %v", err)
	}
	defer reopened.Close()
	recovered, err := reopened.Recover()
	if err != nil {
		t.Fatalf("Recover replica reopen: %v", err)
	}
	if recovered != pH {
		t.Fatalf("recovered frontier=%d want %d", recovered, pH)
	}

	assertLogicalStorageMatch(t, "catch-up WALStore reopen", primary, reopened, 10)

	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close: %v", err)
	}
	if err := primary.Close(); err != nil {
		t.Fatalf("primary Close: %v", err)
	}
}

func TestTransport_Rebuild_WALStore_PersistsAcrossReplicaReopen(t *testing.T) {
	dir := newPersistentTestDir(t)
	primary := createWALStoreForTest(t, dir, "primary-rebuild.dat")
	replica := createWALStoreForTest(t, dir, "replica-rebuild.dat")

	listener, err := NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()

	pH := writePersistentBlocks(t, primary, 10)

	exec := NewBlockExecutor(primary, listener.Addr())
	startCh := make(chan adapter.SessionStartResult, 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild("r1", 42, 1, 1, pH); err != nil {
		t.Fatal(err)
	}

	select {
	case <-startCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for rebuild start")
	}

	select {
	case result := <-closeCh:
		if !result.Success {
			t.Fatalf("rebuild failed: %s", result.FailReason)
		}
		if result.AchievedLSN != pH {
			t.Fatalf("achievedLSN=%d want %d", result.AchievedLSN, pH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for rebuild close")
	}

	listener.Stop()
	if err := replica.Close(); err != nil {
		t.Fatalf("replica Close: %v", err)
	}

	reopened, err := storage.OpenWALStore(filepath.Join(dir, "replica-rebuild.dat"))
	if err != nil {
		t.Fatalf("OpenWALStore replica reopen: %v", err)
	}
	defer reopened.Close()
	recovered, err := reopened.Recover()
	if err != nil {
		t.Fatalf("Recover replica reopen: %v", err)
	}
	if recovered != pH {
		t.Fatalf("recovered frontier=%d want %d", recovered, pH)
	}

	assertLogicalStorageMatch(t, "rebuild WALStore reopen", primary, reopened, 10)

	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close: %v", err)
	}
	if err := primary.Close(); err != nil {
		t.Fatalf("primary Close: %v", err)
	}
}
