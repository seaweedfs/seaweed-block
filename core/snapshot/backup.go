package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	backupRecordVersion = 1
	BackupStateReady    = "ready"
)

var backupIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type BackupRequest struct {
	BackupID   string
	SnapshotID string
}

// BackupRecord is the durable manifest for one full, immutable snapshot
// export. DestinationEvidence is relative to the backup root so a complete
// backup directory can be moved without rewriting its integrity contract.
type BackupRecord struct {
	Version             int       `json:"version"`
	BackupID            string    `json:"backup_id"`
	SourceSnapshotID    string    `json:"source_snapshot_id"`
	CreatedAt           time.Time `json:"created_at"`
	State               string    `json:"state"`
	ArchiveBytes        int64     `json:"archive_bytes"`
	ArchiveSHA256       string    `json:"archive_sha256"`
	ManifestSHA256      string    `json:"manifest_sha256"`
	DestinationEvidence string    `json:"destination_evidence"`
	Snapshot            Record    `json:"snapshot"`
}

type BackupManager struct {
	mu          sync.Mutex
	root        string
	archivesDir string
	recordsDir  string
	snapshots   *Manager
	byID        map[string]BackupRecord
	now         func() time.Time
}

func OpenBackupManager(root string, snapshots *Manager) (*BackupManager, error) {
	if root == "" {
		return nil, fmt.Errorf("backup: root is required")
	}
	b := &BackupManager{
		root:        root,
		archivesDir: filepath.Join(root, "archives"),
		recordsDir:  filepath.Join(root, "records"),
		snapshots:   snapshots,
		byID:        make(map[string]BackupRecord),
		now:         time.Now,
	}
	for _, dir := range []string{b.root, b.archivesDir, b.recordsDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("backup: mkdir %q: %w", dir, err)
		}
	}
	if err := b.recover(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *BackupManager) Export(ctx context.Context, req BackupRequest) (BackupRecord, error) {
	if !backupIDPattern.MatchString(req.BackupID) || req.SnapshotID == "" {
		return BackupRecord{}, fmt.Errorf("%w: backup id and snapshot id are required", ErrInvalidRequest)
	}
	if b.snapshots == nil {
		return BackupRecord{}, fmt.Errorf("%w: snapshot manager is required for export", ErrInvalidRequest)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.byID[req.BackupID]; ok {
		if existing.SourceSnapshotID != req.SnapshotID {
			return BackupRecord{}, ErrNameConflict
		}
		return existing, nil
	}
	tmp, err := os.CreateTemp(b.archivesDir, ".tmp-backup-"+req.BackupID+"-*")
	if err != nil {
		return BackupRecord{}, fmt.Errorf("backup: create archive temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return BackupRecord{}, fmt.Errorf("backup: chmod archive temp: %w", err)
	}
	snapshotRecord, streamErr := b.snapshots.StreamArchive(ctx, req.SnapshotID, tmp)
	if streamErr != nil {
		_ = tmp.Close()
		return BackupRecord{}, streamErr
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return BackupRecord{}, fmt.Errorf("backup: fsync archive temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return BackupRecord{}, fmt.Errorf("backup: close archive temp: %w", err)
	}
	finalArchive := b.archivePath(req.BackupID)
	if _, err := os.Stat(finalArchive); err == nil {
		return BackupRecord{}, fmt.Errorf("%w: backup archive already exists", ErrRestoreConflict)
	} else if !errors.Is(err, os.ErrNotExist) {
		return BackupRecord{}, fmt.Errorf("backup: stat archive: %w", err)
	}
	if err := os.Rename(tmpPath, finalArchive); err != nil {
		return BackupRecord{}, fmt.Errorf("backup: publish archive: %w", err)
	}
	archivePublished := true
	defer func() {
		if archivePublished {
			_ = os.Remove(finalArchive)
		}
	}()
	if err := syncDir(b.archivesDir); err != nil {
		return BackupRecord{}, err
	}
	rec := BackupRecord{
		Version:             backupRecordVersion,
		BackupID:            req.BackupID,
		SourceSnapshotID:    snapshotRecord.SnapshotID,
		CreatedAt:           b.now().UTC(),
		State:               BackupStateReady,
		ArchiveBytes:        snapshotRecord.ArchiveBytes,
		ArchiveSHA256:       snapshotRecord.ArchiveSHA256,
		DestinationEvidence: filepath.ToSlash(filepath.Join("archives", req.BackupID+".sbbackup")),
		Snapshot:            snapshotRecord,
	}
	rec.ManifestSHA256 = backupManifestDigest(rec)
	if err := b.writeRecord(rec); err != nil {
		_ = os.Remove(b.recordPath(rec.BackupID))
		_ = syncDir(b.recordsDir)
		return BackupRecord{}, err
	}
	archivePublished = false
	b.byID[rec.BackupID] = rec
	return rec, nil
}

func (b *BackupManager) Import(ctx context.Context, backupID string, target *Manager) (Record, error) {
	if target == nil {
		return Record{}, fmt.Errorf("%w: import target is required", ErrInvalidRequest)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.byID[backupID]
	if !ok {
		return Record{}, ErrNotFound
	}
	f, err := os.Open(b.archivePath(backupID))
	if err != nil {
		return Record{}, fmt.Errorf("backup: open archive: %w", err)
	}
	defer f.Close()
	return target.ImportArchive(ctx, rec.Snapshot, f)
}

func (b *BackupManager) Get(backupID string) (BackupRecord, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.byID[backupID]
	return rec, ok
}

func (b *BackupManager) List() []BackupRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]BackupRecord, 0, len(b.byID))
	for _, rec := range b.byID {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].BackupID < out[j].BackupID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (b *BackupManager) recover() error {
	if err := removeTemporaryFiles(b.archivesDir); err != nil {
		return err
	}
	if err := removeTemporaryFiles(b.recordsDir); err != nil {
		return err
	}
	entries, err := os.ReadDir(b.recordsDir)
	if err != nil {
		return fmt.Errorf("backup: read records: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(b.recordsDir, entry.Name()))
		if err != nil {
			return fmt.Errorf("backup: read record %q: %w", entry.Name(), err)
		}
		var rec BackupRecord
		if err := json.Unmarshal(raw, &rec); err != nil || validateBackupRecord(rec) != nil {
			return fmt.Errorf("%w: invalid backup record %q", ErrArchiveCorrupt, entry.Name())
		}
		if entry.Name() != rec.BackupID+".json" {
			return fmt.Errorf("backup: record filename %q does not match id %q", entry.Name(), rec.BackupID)
		}
		if _, exists := b.byID[rec.BackupID]; exists {
			return fmt.Errorf("backup: duplicate id %q", rec.BackupID)
		}
		cut, err := readArchive(context.Background(), b.archivePath(rec.BackupID), rec.ArchiveSHA256, nil)
		if err != nil || cut.Frontier != rec.Snapshot.Frontier || cut.NumBlocks != rec.Snapshot.NumBlocks || cut.BlockSize != rec.Snapshot.BlockSize || cut.BlockCount != rec.Snapshot.RecordCount || cut.DataBytes != rec.Snapshot.DataBytes {
			return fmt.Errorf("%w: backup catalog/archive mismatch for %q", ErrArchiveCorrupt, rec.BackupID)
		}
		info, err := os.Stat(b.archivePath(rec.BackupID))
		if err != nil || info.Size() != rec.ArchiveBytes {
			return fmt.Errorf("%w: backup archive size mismatch for %q", ErrArchiveCorrupt, rec.BackupID)
		}
		b.byID[rec.BackupID] = rec
	}
	return b.removeOrphanArchives()
}

func (b *BackupManager) removeOrphanArchives() error {
	entries, err := os.ReadDir(b.archivesDir)
	if err != nil {
		return fmt.Errorf("backup: read archives: %w", err)
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sbbackup" {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".sbbackup")
		if _, ok := b.byID[id]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(b.archivesDir, entry.Name())); err != nil {
			return fmt.Errorf("backup: remove orphan archive %q: %w", entry.Name(), err)
		}
		removed = true
	}
	if removed {
		return syncDir(b.archivesDir)
	}
	return nil
}

func (b *BackupManager) writeRecord(rec BackupRecord) error {
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("backup: marshal record: %w", err)
	}
	tmp, err := os.CreateTemp(b.recordsDir, ".tmp-backup-record-*")
	if err != nil {
		return fmt.Errorf("backup: create record temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("backup: chmod record temp: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("backup: write record: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("backup: fsync record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("backup: close record: %w", err)
	}
	if err := os.Rename(tmpPath, b.recordPath(rec.BackupID)); err != nil {
		return fmt.Errorf("backup: publish record: %w", err)
	}
	return syncDir(b.recordsDir)
}

func validateBackupRecord(rec BackupRecord) error {
	wantDestination := filepath.ToSlash(filepath.Join("archives", rec.BackupID+".sbbackup"))
	if rec.Version != backupRecordVersion || !backupIDPattern.MatchString(rec.BackupID) || rec.SourceSnapshotID != rec.Snapshot.SnapshotID || rec.CreatedAt.IsZero() || rec.State != BackupStateReady || rec.ArchiveBytes != rec.Snapshot.ArchiveBytes || rec.ArchiveSHA256 != rec.Snapshot.ArchiveSHA256 || rec.ManifestSHA256 != backupManifestDigest(rec) || rec.DestinationEvidence != wantDestination || validateImportedRecord(rec.Snapshot) != nil {
		return fmt.Errorf("invalid ready backup record")
	}
	return nil
}

func backupManifestDigest(rec BackupRecord) string {
	rec.ManifestSHA256 = ""
	raw, _ := json.Marshal(rec)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:])
}

func (b *BackupManager) archivePath(id string) string {
	return filepath.Join(b.archivesDir, id+".sbbackup")
}

func (b *BackupManager) recordPath(id string) string {
	return filepath.Join(b.recordsDir, id+".json")
}
