package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

func TestPhase175FullBackupExportRelocateImportRestore(t *testing.T) {
	root := t.TempDir()
	sourceManager, err := OpenManager(filepath.Join(root, "source-catalog"))
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(4, 4096)
	atCut := testBlock(0x61)
	afterCut := testBlock(0x62)
	other := testBlock(0x71)
	if _, err := source.Write(0, atCut); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write(3, other); err != nil {
		t.Fatal(err)
	}
	snapshotRecord, err := sourceManager.Create(context.Background(), CreateRequest{Name: "backup-source", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write(0, afterCut); err != nil {
		t.Fatal(err)
	}

	backupRoot := filepath.Join(root, "backup-export")
	backups, err := OpenBackupManager(backupRoot, sourceManager)
	if err != nil {
		t.Fatal(err)
	}
	backupRecord, err := backups.Export(context.Background(), BackupRequest{BackupID: "full-001", SnapshotID: snapshotRecord.SnapshotID})
	if err != nil {
		t.Fatal(err)
	}
	if backupRecord.State != BackupStateReady || backupRecord.SourceSnapshotID != snapshotRecord.SnapshotID || backupRecord.ArchiveSHA256 != snapshotRecord.ArchiveSHA256 || backupRecord.DestinationEvidence != "archives/full-001.sbbackup" {
		t.Fatalf("backup record=%+v", backupRecord)
	}
	if backupRecord.ManifestSHA256 == "" || backupRecord.ManifestSHA256 != backupManifestDigest(backupRecord) {
		t.Fatalf("backup manifest digest=%q", backupRecord.ManifestSHA256)
	}
	retry, err := backups.Export(context.Background(), BackupRequest{BackupID: "full-001", SnapshotID: snapshotRecord.SnapshotID})
	if err != nil || !retry.CreatedAt.Equal(backupRecord.CreatedAt) {
		t.Fatalf("idempotent export=%+v err=%v", retry, err)
	}
	if err := sourceManager.Delete(snapshotRecord.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if _, ok := sourceManager.Get(snapshotRecord.SnapshotID); ok {
		t.Fatal("source catalog still contains deleted snapshot")
	}

	relocatedRoot := filepath.Join(root, "relocated-backup")
	if err := os.Rename(backupRoot, relocatedRoot); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenBackupManager(relocatedRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if listed := reopened.List(); len(listed) != 1 || listed[0].BackupID != "full-001" {
		t.Fatalf("backup list=%+v", listed)
	}
	if _, err := reopened.Export(context.Background(), BackupRequest{BackupID: "another", SnapshotID: snapshotRecord.SnapshotID}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("offline backup manager export error=%v", err)
	}
	targetCatalogRoot := filepath.Join(root, "imported-catalog")
	targetManager, err := OpenManager(targetCatalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := reopened.Import(context.Background(), "full-001", targetManager)
	if err != nil {
		t.Fatal(err)
	}
	if !sameCatalogRecord(imported, snapshotRecord) {
		t.Fatalf("imported=%+v source=%+v", imported, snapshotRecord)
	}
	retryImported, err := reopened.Import(context.Background(), "full-001", targetManager)
	if err != nil || !sameCatalogRecord(retryImported, imported) {
		t.Fatalf("idempotent import=%+v err=%v", retryImported, err)
	}
	targetManager, err = OpenManager(targetCatalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	if recovered, ok := targetManager.Get(imported.SnapshotID); !ok || !sameCatalogRecord(recovered, imported) {
		t.Fatalf("recovered imported catalog=%+v ok=%v", recovered, ok)
	}

	targetPath := filepath.Join(root, "restored.bin")
	if _, err := targetManager.RestoreToNew(context.Background(), imported.SnapshotID, targetPath, func(path string, numBlocks uint32, blockSize int) (storage.LogicalStorage, error) {
		return smartwal.CreateStore(path, numBlocks, blockSize)
	}); err != nil {
		t.Fatal(err)
	}
	restored, err := smartwal.OpenStore(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err := restored.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := restored.Read(0)
	if err != nil || string(got) != string(atCut) {
		t.Fatal("restored backup did not preserve snapshot cut bytes")
	}
	got, err = restored.Read(3)
	if err != nil || string(got) != string(other) {
		t.Fatal("restored backup lost an archived block")
	}
	if _, err := restored.Write(0, testBlock(0x81)); err != nil {
		t.Fatal(err)
	}
	live, err := source.Read(0)
	if err != nil || string(live) != string(afterCut) {
		t.Fatal("restored backup write changed the live source")
	}
}

func TestPhase175FullBackupRejectsCorruptionAndLeavesNoImport(t *testing.T) {
	root := t.TempDir()
	sourceManager, err := OpenManager(filepath.Join(root, "source"))
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(2, 4096)
	if _, err := source.Write(1, testBlock(0x91)); err != nil {
		t.Fatal(err)
	}
	rec, err := sourceManager.Create(context.Background(), CreateRequest{Name: "corrupt-backup", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	backupRoot := filepath.Join(root, "backup")
	backups, err := OpenBackupManager(backupRoot, sourceManager)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backups.Export(context.Background(), BackupRequest{BackupID: "full-corrupt", SnapshotID: rec.SnapshotID}); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(backupRoot, "archives", "full-corrupt.sbbackup")
	raw, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	raw[archiveHeaderSize+recordHeaderSize+3] ^= 0xff
	if err := os.WriteFile(archivePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	target, err := OpenManager(filepath.Join(root, "target"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backups.Import(context.Background(), "full-corrupt", target); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("corrupt backup import error=%v", err)
	}
	if got := target.List(""); len(got) != 0 {
		t.Fatalf("corrupt import published catalog records: %+v", got)
	}
	if _, err := OpenBackupManager(backupRoot, sourceManager); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("corrupt backup reopen error=%v", err)
	}
}

func TestPhase175FullBackupCancelledExportLeavesNoResidue(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenManager(filepath.Join(root, "source"))
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(2, 4096)
	if _, err := source.Write(0, testBlock(0xa1)); err != nil {
		t.Fatal(err)
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "cancel-export", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	backupRoot := filepath.Join(root, "backup")
	backups, err := OpenBackupManager(backupRoot, manager)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := backups.Export(ctx, BackupRequest{BackupID: "cancelled", SnapshotID: rec.SnapshotID}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled export error=%v", err)
	}
	for _, dir := range []string{filepath.Join(backupRoot, "archives"), filepath.Join(backupRoot, "records")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("cancelled export left residue in %s: %v", dir, entries)
		}
	}
}

func TestPhase175FullBackupRejectsTamperedManifestIdentity(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenManager(filepath.Join(root, "source"))
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(1, 4096)
	if _, err := source.Write(0, testBlock(0xb1)); err != nil {
		t.Fatal(err)
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "identity-source", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	backupRoot := filepath.Join(root, "backup")
	backups, err := OpenBackupManager(backupRoot, manager)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backups.Export(context.Background(), BackupRequest{BackupID: "identity", SnapshotID: rec.SnapshotID}); err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(backupRoot, "records", "identity.json")
	raw, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest BackupRecord
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.SourceSnapshotID = "../../outside"
	manifest.Snapshot.SnapshotID = manifest.SourceSnapshotID
	manifest.ManifestSHA256 = backupManifestDigest(manifest)
	raw, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recordPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBackupManager(backupRoot, nil); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("tampered manifest reopen error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "outside.sbsnap")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tampered identity escaped catalog: %v", err)
	}

	metadataRoot := filepath.Join(root, "backup-metadata")
	metadataBackups, err := OpenBackupManager(metadataRoot, manager)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadataBackups.Export(context.Background(), BackupRequest{BackupID: "metadata", SnapshotID: rec.SnapshotID}); err != nil {
		t.Fatal(err)
	}
	metadataPath := filepath.Join(metadataRoot, "records", "metadata.json")
	raw, err = os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Snapshot.SourceVolumeID = "changed-without-manifest-digest"
	raw, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadataPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBackupManager(metadataRoot, nil); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("metadata tamper reopen error=%v", err)
	}
}

func TestPhase175FullBackupRecoveryRemovesOwnedResidue(t *testing.T) {
	root := t.TempDir()
	archives := filepath.Join(root, "archives")
	records := filepath.Join(root, "records")
	for _, dir := range []string{archives, records} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{
		filepath.Join(archives, ".tmp-backup-interrupted"),
		filepath.Join(records, ".tmp-backup-record-interrupted"),
		filepath.Join(archives, "orphan.sbbackup"),
	} {
		if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	unowned := filepath.Join(archives, "keep.txt")
	if err := os.WriteFile(unowned, []byte("not owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err := OpenBackupManager(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := manager.List(); len(got) != 0 {
		t.Fatalf("recovered backup records=%+v", got)
	}
	for _, path := range []string{
		filepath.Join(archives, ".tmp-backup-interrupted"),
		filepath.Join(records, ".tmp-backup-record-interrupted"),
		filepath.Join(archives, "orphan.sbbackup"),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned residue remains %s: %v", path, err)
		}
	}
	if _, err := os.Stat(unowned); err != nil {
		t.Fatalf("unowned file removed: %v", err)
	}
}
