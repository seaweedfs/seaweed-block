package snapshot

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPhase175RestoreTargetFailsClosedThenRetriesAndActivates(t *testing.T) {
	manager, rec, want := createStreamFixture(t)
	var archive bytes.Buffer
	if _, err := manager.StreamArchive(context.Background(), rec.SnapshotID, &archive); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dataPath := filepath.Join(root, "target.bin")
	markerPath := filepath.Join(root, "restore.json")
	target, err := OpenRestoreTarget(RestoreTargetConfig{
		MarkerPath:      markerPath,
		TargetDataPath:  dataPath,
		SnapshotID:      rec.SnapshotID,
		TargetVolumeID:  "restored-vol",
		TargetReplicaID: "r1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store := storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)
	identified := identifyRestoreStorage(dataPath, "store-a", store)
	failing := &failSecondWrite{LogicalStorage: identified}
	if err := target.BindStorage(failing); err != nil {
		t.Fatal(err)
	}
	activated := false
	if err := target.Activate(func() error { activated = true; return nil }); !errors.Is(err, ErrRestoreNotApplied) || activated {
		t.Fatalf("premature activation err=%v activated=%v", err, activated)
	}
	if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec); err == nil {
		t.Fatal("partial apply succeeded")
	}
	if marker := target.Marker(); marker.State != RestoreStatePending {
		t.Fatalf("partial marker=%+v", marker)
	}
	if activated {
		t.Fatal("partial apply released readiness")
	}

	result, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != RestoreStateApplied || result.RestoredBlocks != rec.RecordCount || result.RestoredBytes != rec.DataBytes || result.AlreadyApplied {
		t.Fatalf("apply result=%+v", result)
	}
	for lba, expected := range want {
		got, err := store.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("LBA %d mismatch", lba)
		}
	}

	reopened := openRestoreTargetForTest(t, markerPath, dataPath, rec, identified)
	idempotent, err := reopened.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec)
	if err != nil || !idempotent.AlreadyApplied || idempotent.State != RestoreStateApplied {
		t.Fatalf("idempotent apply=%+v err=%v", idempotent, err)
	}
	if err := reopened.Activate(func() error {
		activated = reopened.Marker().State == RestoreStateActivated
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !activated || reopened.Marker().State != RestoreStateActivated {
		t.Fatalf("activation marker=%+v activated=%v", reopened.Marker(), activated)
	}

	restarted := openRestoreTargetForTest(t, markerPath, dataPath, rec, identified)
	callbackCount := 0
	if err := restarted.Activate(func() error { callbackCount++; return nil }); err != nil {
		t.Fatal(err)
	}
	if callbackCount != 1 {
		t.Fatalf("activated restart callback count=%d", callbackCount)
	}
}

func TestPhase175RestoreTargetRejectsUnsafeOrConflictingState(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	var archive bytes.Buffer
	if _, err := manager.StreamArchive(context.Background(), rec.SnapshotID, &archive); err != nil {
		t.Fatal(err)
	}
	t.Run("data-without-marker", func(t *testing.T) {
		root := t.TempDir()
		dataPath := filepath.Join(root, "target.bin")
		if err := os.WriteFile(dataPath, []byte("preexisting"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: filepath.Join(root, "restore.json"), TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "v", TargetReplicaID: "r1"})
		if !errors.Is(err, ErrRestoreUnsafe) {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("catalog-conflict", func(t *testing.T) {
		root := t.TempDir()
		dataPath := filepath.Join(root, "target.bin")
		target, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: filepath.Join(root, "restore.json"), TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "v", TargetReplicaID: "r1"})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		store := identifyRestoreStorage(dataPath, "store-b", storage.NewBlockStore(rec.NumBlocks, rec.BlockSize))
		if err := target.BindStorage(store); err != nil {
			t.Fatal(err)
		}
		truncated := archive.Bytes()[:archive.Len()-1]
		if _, err := target.Apply(context.Background(), bytes.NewReader(truncated), rec); !errors.Is(err, ErrArchiveCorrupt) {
			t.Fatalf("bind error=%v", err)
		}
		changed := rec
		changed.ArchiveSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), changed); !errors.Is(err, ErrRestoreConflict) {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("applied-marker-without-data", func(t *testing.T) {
		root := t.TempDir()
		dataPath := filepath.Join(root, "target.bin")
		markerPath := filepath.Join(root, "restore.json")
		target, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: markerPath, TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "v", TargetReplicaID: "r1"})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		store := identifyRestoreStorage(dataPath, "store-c", storage.NewBlockStore(rec.NumBlocks, rec.BlockSize))
		if err := target.BindStorage(store); err != nil {
			t.Fatal(err)
		}
		if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(dataPath); err != nil {
			t.Fatal(err)
		}
		_, err = OpenRestoreTarget(RestoreTargetConfig{MarkerPath: markerPath, TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "v", TargetReplicaID: "r1"})
		if !errors.Is(err, ErrRestoreUnsafe) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestPhase175RestoreTargetCorruptStreamNeverApplies(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	var archive bytes.Buffer
	if _, err := manager.StreamArchive(context.Background(), rec.SnapshotID, &archive); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dataPath := filepath.Join(root, "target.bin")
	target, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: filepath.Join(root, "restore.json"), TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "v", TargetReplicaID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store := identifyRestoreStorage(dataPath, "store-d", storage.NewBlockStore(rec.NumBlocks, rec.BlockSize))
	if err := target.BindStorage(store); err != nil {
		t.Fatal(err)
	}
	bad := corruptCopy(archive.Bytes(), archiveHeaderSize+recordHeaderSize+3)
	if _, err := target.Apply(context.Background(), bytes.NewReader(bad), rec); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("error=%v", err)
	}
	if marker := target.Marker(); marker.State != RestoreStatePending {
		t.Fatalf("marker=%+v", marker)
	}
}

func TestPhase175RestoreTargetVerifiesDigestBeforeApplyingLBA(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	var archive bytes.Buffer
	if _, err := manager.StreamArchive(context.Background(), rec.SnapshotID, &archive); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dataPath := filepath.Join(root, "target.bin")
	target, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: filepath.Join(root, "restore.json"), TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "v", TargetReplicaID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store := storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)
	if err := target.BindStorage(identifyRestoreStorage(dataPath, "digest-first", store)); err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), archive.Bytes()...)
	binary.LittleEndian.PutUint32(bad[archiveHeaderSize:archiveHeaderSize+4], 1)
	if _, err := target.Apply(context.Background(), bytes.NewReader(bad), rec); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("corrupt-LBA error=%v", err)
	}
	zero := make([]byte, rec.BlockSize)
	if got, err := store.Read(1); err != nil || !bytes.Equal(got, zero) {
		t.Fatalf("unverified LBA reached target err=%v", err)
	}
	if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Read(1); err != nil || !bytes.Equal(got, zero) {
		t.Fatalf("retry retained contaminated LBA err=%v", err)
	}
}

func TestPhase175RestoreTargetRejectsDifferentDurableStoreOnReopen(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	var archive bytes.Buffer
	if _, err := manager.StreamArchive(context.Background(), rec.SnapshotID, &archive); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dataPath := filepath.Join(root, "target.bin")
	markerPath := filepath.Join(root, "restore.json")
	target, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: markerPath, TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "v", TargetReplicaID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	original := identifyRestoreStorage(dataPath, "store-original", storage.NewBlockStore(rec.NumBlocks, rec.BlockSize))
	if err := target.BindStorage(original); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: markerPath, TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "v", TargetReplicaID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	replacement := identifyRestoreStorage(dataPath, "store-replacement", storage.NewBlockStore(rec.NumBlocks, rec.BlockSize))
	if err := reopened.BindStorage(replacement); !errors.Is(err, ErrRestoreConflict) {
		t.Fatalf("replacement bind error=%v", err)
	}
}

func openRestoreTargetForTest(t *testing.T, markerPath, dataPath string, rec Record, store storage.LogicalStorage) *RestoreTarget {
	t.Helper()
	target, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: markerPath, TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "restored-vol", TargetReplicaID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.BindStorage(store); err != nil {
		t.Fatal(err)
	}
	return target
}

type identifiedRestoreStorage struct {
	storage.LogicalStorage
	identity storage.DurableStorageIdentity
}

func (s *identifiedRestoreStorage) DurableStorageIdentity() storage.DurableStorageIdentity {
	return s.identity
}

func identifyRestoreStorage(path, id string, store storage.LogicalStorage) storage.LogicalStorage {
	return &identifiedRestoreStorage{LogicalStorage: store, identity: storage.DurableStorageIdentity{Path: path, StoreID: id}}
}
