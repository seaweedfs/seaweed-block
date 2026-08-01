package snapshot

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/parallelwal"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

func TestPhase175DiscardRestoreTargetRemovesVerifiedFilesAndRetries(t *testing.T) {
	root := t.TempDir()
	req := restoreDiscardRequestForTest(root)
	createPendingRestoreTargetForDiscardTest(t, req, true)

	result, err := DiscardRestoreTarget(req)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DataRemoved || !result.MarkerRemoved || result.AlreadyDiscarded {
		t.Fatalf("discard result=%+v", result)
	}
	result, err = DiscardRestoreTarget(req)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DataRemoved || !result.MarkerRemoved || !result.AlreadyDiscarded {
		t.Fatalf("retry result=%+v", result)
	}
}

func TestPhase175DiscardRestoreTargetResumesAfterDataRemoval(t *testing.T) {
	root := t.TempDir()
	req := restoreDiscardRequestForTest(root)
	createPendingRestoreTargetForDiscardTest(t, req, false)

	result, err := DiscardRestoreTarget(req)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DataRemoved || !result.MarkerRemoved || result.AlreadyDiscarded {
		t.Fatalf("discard result=%+v", result)
	}
}

func TestPhase175DiscardRestoreTargetResumesAfterMarkerRemoval(t *testing.T) {
	root := t.TempDir()
	req := restoreDiscardRequestForTest(root)
	receiptPath := filepath.Join(root, req.TargetVolumeID+".restore-discard.json")
	if err := persistRestoreDiscardReceipt(receiptPath, restoreDiscardReceipt{
		Version: restoreDiscardReceiptVersion, State: restoreDiscardDataRemoved,
		OperationID: req.OperationID, SnapshotID: req.SnapshotID, TargetVolumeID: req.TargetVolumeID, TargetReplicaID: req.TargetReplicaID,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := DiscardRestoreTarget(req)
	if err != nil {
		t.Fatal(err)
	}
	if !result.AlreadyDiscarded || !result.DataRemoved || !result.MarkerRemoved {
		t.Fatalf("resume result=%+v", result)
	}
	receipt, err := loadRestoreDiscardReceipt(receiptPath)
	if err != nil || receipt.State != restoreDiscardComplete {
		t.Fatalf("receipt=%+v error=%v", receipt, err)
	}
}

func TestPhase175DiscardRestoreTargetRejectsMismatchedReceipt(t *testing.T) {
	root := t.TempDir()
	req := restoreDiscardRequestForTest(root)
	receiptPath := filepath.Join(root, req.TargetVolumeID+".restore-discard.json")
	if err := persistRestoreDiscardReceipt(receiptPath, restoreDiscardReceipt{
		Version: restoreDiscardReceiptVersion, State: restoreDiscardComplete,
		OperationID: "abort-other", SnapshotID: req.SnapshotID, TargetVolumeID: req.TargetVolumeID, TargetReplicaID: req.TargetReplicaID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := DiscardRestoreTarget(req); !errors.Is(err, ErrRestoreUnsafe) {
		t.Fatalf("mismatched receipt error=%v", err)
	}
}

func TestPhase175DiscardRestoreTargetVerifiesEveryDurableBackend(t *testing.T) {
	tests := []struct {
		name   string
		kind   string
		create func(string) (storage.LogicalStorage, error)
	}{
		{name: "walstore", kind: "walstore", create: func(path string) (storage.LogicalStorage, error) {
			return storage.CreateWALStore(path, 4, 4096)
		}},
		{name: "smartwal", kind: "smartwal", create: func(path string) (storage.LogicalStorage, error) {
			return smartwal.CreateStore(path, 4, 4096)
		}},
		{name: "parallelwal", kind: "parallelwal", create: func(path string) (storage.LogicalStorage, error) {
			return parallelwal.CreateStore(path, 4, 4096)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			req := restoreDiscardRequestForTest(root)
			createRestoreTargetForDiscardTest(t, req, tt.kind, tt.create)
			result, err := DiscardRestoreTarget(req)
			if err != nil {
				t.Fatal(err)
			}
			if !result.DataRemoved || !result.MarkerRemoved {
				t.Fatalf("discard result=%+v", result)
			}
		})
	}
}

func TestPhase175DiscardRestoreTargetRecoversCreateBeforeBindCrash(t *testing.T) {
	tests := []struct {
		kind   string
		create func(string) (storage.LogicalStorage, error)
	}{
		{kind: "walstore", create: func(path string) (storage.LogicalStorage, error) { return storage.CreateWALStore(path, 4, 4096) }},
		{kind: "smartwal", create: func(path string) (storage.LogicalStorage, error) { return smartwal.CreateStore(path, 4, 4096) }},
		{kind: "parallelwal", create: func(path string) (storage.LogicalStorage, error) { return parallelwal.CreateStore(path, 4, 4096) }},
	}
	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			root := t.TempDir()
			req := restoreDiscardRequestForTest(root)
			markerPath := filepath.Join(root, req.TargetVolumeID+".restore.json")
			dataPath := filepath.Join(root, req.TargetVolumeID+".bin")
			target, err := OpenRestoreTarget(RestoreTargetConfig{
				MarkerPath: markerPath, TargetDataPath: dataPath, SnapshotID: req.SnapshotID,
				TargetVolumeID: req.TargetVolumeID, TargetReplicaID: req.TargetReplicaID,
			})
			if err != nil {
				t.Fatal(err)
			}
			mustPrepareRestoreStorage(t, target, tt.kind, 4, 4096)
			store, err := tt.create(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenRestoreTarget(RestoreTargetConfig{
				MarkerPath: markerPath, TargetDataPath: dataPath, SnapshotID: req.SnapshotID,
				TargetVolumeID: req.TargetVolumeID, TargetReplicaID: req.TargetReplicaID,
			}); err != nil {
				t.Fatalf("restart after create-before-bind crash: %v", err)
			}
			result, err := DiscardRestoreTarget(req)
			if err != nil || !result.DataRemoved || !result.MarkerRemoved {
				t.Fatalf("discard=%+v error=%v", result, err)
			}
		})
	}
}

func TestPhase175RestoreStorageIntentCannotBeBackfilledAfterDataCreation(t *testing.T) {
	root := t.TempDir()
	req := restoreDiscardRequestForTest(root)
	dataPath := filepath.Join(root, req.TargetVolumeID+".bin")
	target, err := OpenRestoreTarget(RestoreTargetConfig{
		MarkerPath: filepath.Join(root, req.TargetVolumeID+".restore.json"), TargetDataPath: dataPath,
		SnapshotID: req.SnapshotID, TargetVolumeID: req.TargetVolumeID, TargetReplicaID: req.TargetReplicaID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte("unowned"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := target.PrepareStorage("smartwal", 4, 4096); !errors.Is(err, ErrRestoreUnsafe) {
		t.Fatalf("backfilled storage intent error=%v", err)
	}
}

func TestPhase175DiscardActivatedRestoreRequiresExplicitOverride(t *testing.T) {
	manager, record, _ := createStreamFixture(t)
	var archive bytes.Buffer
	if _, err := manager.StreamArchive(context.Background(), record.SnapshotID, &archive); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	req := restoreDiscardRequestForTest(root)
	req.SnapshotID = record.SnapshotID
	dataPath := filepath.Join(root, req.TargetVolumeID+".bin")
	target, err := OpenRestoreTarget(RestoreTargetConfig{
		MarkerPath: filepath.Join(root, req.TargetVolumeID+".restore.json"), TargetDataPath: dataPath,
		SnapshotID: req.SnapshotID, TargetVolumeID: req.TargetVolumeID, TargetReplicaID: req.TargetReplicaID,
	})
	if err != nil {
		t.Fatal(err)
	}
	mustPrepareRestoreStorage(t, target, "smartwal", record.NumBlocks, record.BlockSize)
	store, err := smartwal.CreateStore(dataPath, record.NumBlocks, record.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.BindStorage(store); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), record); err != nil {
		t.Fatal(err)
	}
	if err := target.Activate(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := DiscardRestoreTarget(req); !errors.Is(err, ErrRestoreUnsafe) {
		t.Fatalf("activated target error=%v", err)
	}
	req.AllowActivated = true
	if _, err := DiscardRestoreTarget(req); err != nil {
		t.Fatal(err)
	}
}

func TestPhase175DiscardRestoreTargetFailsClosedWithoutMatchingMarker(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, root string, req RestoreDiscardRequest) string
	}{
		{
			name: "data without marker",
			setup: func(t *testing.T, root string, req RestoreDiscardRequest) string {
				path := filepath.Join(root, req.TargetVolumeID+".bin")
				mustWriteRestoreDiscardFile(t, path)
				return path
			},
		},
		{
			name: "replaced data store",
			setup: func(t *testing.T, root string, req RestoreDiscardRequest) string {
				createPendingRestoreTargetForDiscardTest(t, req, true)
				path := filepath.Join(root, req.TargetVolumeID+".bin")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				replacement, err := smartwal.CreateStore(path, 8, 4096)
				if err != nil {
					t.Fatal(err)
				}
				if err := replacement.Close(); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
		{
			name: "marker identity mismatch",
			setup: func(t *testing.T, root string, req RestoreDiscardRequest) string {
				other := req
				other.SnapshotID = "snap-other"
				createPendingRestoreTargetForDiscardTest(t, other, true)
				return filepath.Join(root, req.TargetVolumeID+".bin")
			},
		},
		{
			name: "data symlink",
			setup: func(t *testing.T, root string, req RestoreDiscardRequest) string {
				createPendingRestoreTargetForDiscardTest(t, req, false)
				outside := filepath.Join(t.TempDir(), "outside.bin")
				mustWriteRestoreDiscardFile(t, outside)
				if err := os.Symlink(outside, filepath.Join(root, req.TargetVolumeID+".bin")); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
				return outside
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			req := restoreDiscardRequestForTest(root)
			protected := tt.setup(t, root, req)
			if _, err := DiscardRestoreTarget(req); !errors.Is(err, ErrRestoreUnsafe) && !errors.Is(err, ErrRestoreConflict) {
				t.Fatalf("discard error=%v", err)
			}
			if _, err := os.Stat(protected); err != nil {
				t.Fatalf("protected file changed: %v", err)
			}
		})
	}
}

func TestPhase175DiscardRestoreTargetRejectsUnsafeIdentityAndRoot(t *testing.T) {
	root := t.TempDir()
	req := restoreDiscardRequestForTest(root)
	req.TargetVolumeID = "../victim"
	if _, err := DiscardRestoreTarget(req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unsafe identity error=%v", err)
	}

	realRoot := t.TempDir()
	linkedRoot := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(realRoot, linkedRoot); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	req = restoreDiscardRequestForTest(linkedRoot)
	if _, err := DiscardRestoreTarget(req); !errors.Is(err, ErrRestoreUnsafe) {
		t.Fatalf("symlink root error=%v", err)
	}
}

func restoreDiscardRequestForTest(root string) RestoreDiscardRequest {
	return RestoreDiscardRequest{
		RootPath:        root,
		OperationID:     "abort-001",
		SnapshotID:      "snap-abc",
		TargetVolumeID:  "restored-a",
		TargetReplicaID: "r1",
	}
}

func createPendingRestoreTargetForDiscardTest(t *testing.T, req RestoreDiscardRequest, withData bool) {
	t.Helper()
	createRestoreTargetForDiscardTest(t, req, "smartwal", func(path string) (storage.LogicalStorage, error) {
		return smartwal.CreateStore(path, 4, 4096)
	})
	if !withData {
		if err := os.Remove(filepath.Join(req.RootPath, req.TargetVolumeID+".bin")); err != nil {
			t.Fatal(err)
		}
	}
}

func createRestoreTargetForDiscardTest(t *testing.T, req RestoreDiscardRequest, kind string, create func(string) (storage.LogicalStorage, error)) {
	t.Helper()
	target, err := OpenRestoreTarget(RestoreTargetConfig{
		MarkerPath:      filepath.Join(req.RootPath, req.TargetVolumeID+".restore.json"),
		TargetDataPath:  filepath.Join(req.RootPath, req.TargetVolumeID+".bin"),
		SnapshotID:      req.SnapshotID,
		TargetVolumeID:  req.TargetVolumeID,
		TargetReplicaID: req.TargetReplicaID,
	})
	if err != nil {
		t.Fatal(err)
	}
	mustPrepareRestoreStorage(t, target, kind, 4, 4096)
	dataPath := filepath.Join(req.RootPath, req.TargetVolumeID+".bin")
	store, err := create(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.BindStorage(store); err != nil {
		t.Fatal(err)
	}
	closer, ok := store.(interface{ Close() error })
	if !ok {
		t.Fatal("durable test store does not implement Close")
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustWriteRestoreDiscardFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("protected restore data"), 0o600); err != nil {
		t.Fatal(err)
	}
}
