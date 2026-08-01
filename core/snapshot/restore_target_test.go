package snapshot

import (
	"bytes"
	"context"
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
	activated := false
	if err := target.Activate(func() error { activated = true; return nil }); !errors.Is(err, ErrRestoreNotApplied) || activated {
		t.Fatalf("premature activation err=%v activated=%v", err, activated)
	}

	store := storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)
	failing := &failSecondWrite{LogicalStorage: store}
	if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec, failing); err == nil {
		t.Fatal("partial apply succeeded")
	}
	if marker := target.Marker(); marker.State != RestoreStatePending {
		t.Fatalf("partial marker=%+v", marker)
	}
	if activated {
		t.Fatal("partial apply released readiness")
	}

	result, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec, failing)
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

	reopened := openRestoreTargetForTest(t, markerPath, dataPath, rec)
	idempotent, err := reopened.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec, store)
	if err != nil || !idempotent.AlreadyApplied || idempotent.State != RestoreStateApplied {
		t.Fatalf("idempotent apply=%+v err=%v", idempotent, err)
	}
	if err := reopened.Activate(func() error { activated = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !activated || reopened.Marker().State != RestoreStateActivated {
		t.Fatalf("activation marker=%+v activated=%v", reopened.Marker(), activated)
	}

	restarted := openRestoreTargetForTest(t, markerPath, dataPath, rec)
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
		truncated := archive.Bytes()[:archive.Len()-1]
		if _, err := target.Apply(context.Background(), bytes.NewReader(truncated), rec, storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)); !errors.Is(err, ErrArchiveCorrupt) {
			t.Fatalf("bind error=%v", err)
		}
		changed := rec
		changed.ArchiveSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), changed, storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)); !errors.Is(err, ErrRestoreConflict) {
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
		if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), rec, storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)); err != nil {
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
	bad := corruptCopy(archive.Bytes(), archiveHeaderSize+recordHeaderSize+3)
	if _, err := target.Apply(context.Background(), bytes.NewReader(bad), rec, storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("error=%v", err)
	}
	if marker := target.Marker(); marker.State != RestoreStatePending {
		t.Fatalf("marker=%+v", marker)
	}
}

func openRestoreTargetForTest(t *testing.T, markerPath, dataPath string, rec Record) *RestoreTarget {
	t.Helper()
	target, err := OpenRestoreTarget(RestoreTargetConfig{MarkerPath: markerPath, TargetDataPath: dataPath, SnapshotID: rec.SnapshotID, TargetVolumeID: "restored-vol", TargetReplicaID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	return target
}
